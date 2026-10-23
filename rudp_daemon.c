#include "rudp_socket.h"
#include <pthread.h>
#include <sys/select.h>

/* The daemon owns every UDP socket. Applications talk to it through the
 * shared socket table (guarded by a System V semaphore). */

union semun {
    int val;
    struct semid_ds *buf;
    unsigned short *array;
};

static double drop_prob = DROP_PROB;

static int64_t now_ms(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (int64_t)ts.tv_sec * 1000 + ts.tv_nsec / 1000000;
}

static void cleanup_ipc(void) {
    if (shm_id >= 0) shmctl(shm_id, IPC_RMID, NULL);
    if (sem_id >= 0) semctl(sem_id, 0, IPC_RMID);
}

static void on_signal(int sig) {
    (void)sig;
    cleanup_ipc();
    _exit(0);
}

static void send_ack(ktp_socket_info *s) {
    ktp_message a;
    memset(&a, 0, sizeof(a));
    a.type = ACK_MSG;
    a.ack_num = s->rcv_base;
    a.window_size = (uint8_t)(MAX_WINDOW_SIZE - s->recv_buffer_count);
    sendto(s->udp_socket, &a, sizeof(a), 0,
           (struct sockaddr *)&s->dest_addr, sizeof(s->dest_addr));
}

/* Move held-back messages that have become in-order into recv_buffer.
 * Returns how many were delivered. */
static int drain_ooo(ktp_socket_info *s) {
    int moved = 0;
    while (s->recv_buffer_count < MAX_WINDOW_SIZE) {
        int found = -1;
        for (int k = 0; k < MAX_WINDOW_SIZE; k++) {
            if (s->ooo_used[k] && s->ooo[k].seq_num == s->rcv_base) {
                found = k;
                break;
            }
        }
        if (found < 0) break;
        s->recv_buffer[s->recv_buffer_count++] = s->ooo[found];
        s->ooo_used[found] = 0;
        s->rcv_base++;
        moved++;
    }
    return moved;
}

static void handle_data(int idx, ktp_socket_info *s, const ktp_message *msg) {
    uint8_t d = (uint8_t)(msg->seq_num - s->rcv_base);

    if (d == 0 && s->recv_buffer_count < MAX_WINDOW_SIZE) {
        /* the expected message: deliver it, then anything it unblocks */
        s->recv_buffer[s->recv_buffer_count++] = *msg;
        s->rcv_base++;
        for (int k = 0; k < MAX_WINDOW_SIZE; k++) {
            if (s->ooo_used[k] && s->ooo[k].seq_num == msg->seq_num) s->ooo_used[k] = 0;
        }
        drain_ooo(s);
    } else if (d > 0 && d < MAX_WINDOW_SIZE) {
        /* ahead of the expected one: hold it back (ignore if already held) */
        int held = 0, free_slot = -1;
        for (int k = 0; k < MAX_WINDOW_SIZE; k++) {
            if (s->ooo_used[k] && s->ooo[k].seq_num == msg->seq_num) held = 1;
            if (!s->ooo_used[k] && free_slot < 0) free_slot = k;
        }
        if (!held && free_slot >= 0) {
            s->ooo[free_slot] = *msg;
            s->ooo_used[free_slot] = 1;
            printf("[socket %d] out-of-order seq %d held (expected %d)\n", idx, msg->seq_num, s->rcv_base);
        }
    } else if (d != 0) {
        printf("[socket %d] duplicate/old seq %d ignored\n", idx, msg->seq_num);
    } else {
        printf("[socket %d] receive buffer full, seq %d dropped\n", idx, msg->seq_num);
    }

    if (s->recv_buffer_count >= MAX_WINDOW_SIZE) s->nospace_flag = 1;

    /* every DATA message is answered with a cumulative ACK + current window */
    send_ack(s);
}

static void handle_ack(ktp_socket_info *s, const ktp_message *msg) {
    uint8_t n = (uint8_t)(msg->ack_num - s->snd_base);
    if (n > s->inflight_count) return;   /* stale or bogus ACK */

    if (n > 0) {
        for (int i = 0; i + n < s->inflight_count; i++) {
            s->inflight[i] = s->inflight[i + n];
            s->inflight_time[i] = s->inflight_time[i + n];
        }
        s->inflight_count -= n;
        s->snd_base = msg->ack_num;
    }
    s->peer_window = msg->window_size;
}

/* Called regularly: finish delivery after the application freed space and
 * send the window update the sender is waiting for. */
static void service_window(ktp_socket_info *s) {
    int advanced = drain_ooo(s);
    int free_space = MAX_WINDOW_SIZE - s->recv_buffer_count;
    if (advanced || (s->nospace_flag && free_space > 0)) {
        send_ack(s);
        if (free_space > 0) s->nospace_flag = 0;
    }
}

static void *receiver_thread(void *arg) {
    (void)arg;
    printf("Receiver thread started\n");

    for (;;) {
        fd_set rfds;
        FD_ZERO(&rfds);
        int maxfd = -1;

        sem_lock();
        for (int i = 0; i < MAX_SOCKETS; i++) {
            int fd = socket_info[i].udp_socket;
            if (socket_info[i].is_active && fd >= 0 && fd < FD_SETSIZE) {
                FD_SET(fd, &rfds);
                if (fd > maxfd) maxfd = fd;
            }
        }
        sem_unlock();

        if (maxfd < 0) {
            usleep(50000);
            continue;
        }

        struct timeval tv = {0, 50000};
        int ready = select(maxfd + 1, &rfds, NULL, NULL, &tv);
        if (ready < 0) {
            if (errno != EINTR) {
                perror("select failed");
                usleep(50000);
            }
            continue;
        }

        sem_lock();
        for (int i = 0; i < MAX_SOCKETS; i++) {
            ktp_socket_info *s = &socket_info[i];
            if (!s->is_active || s->udp_socket < 0) continue;

            if (ready > 0 && s->udp_socket < FD_SETSIZE && FD_ISSET(s->udp_socket, &rfds)) {
                ktp_message msg;
                struct sockaddr_in from;
                socklen_t flen = sizeof(from);
                ssize_t n = recvfrom(s->udp_socket, &msg, sizeof(msg), MSG_DONTWAIT,
                                     (struct sockaddr *)&from, &flen);
                if (n == (ssize_t)sizeof(msg) &&
                    from.sin_addr.s_addr == s->dest_addr.sin_addr.s_addr &&
                    from.sin_port == s->dest_addr.sin_port &&
                    msg.len <= MAX_MSG_SIZE) {
                    if (drop_message(drop_prob)) {
                        printf("[socket %d] simulated loss: dropped %s %d\n", i,
                               msg.type == DATA_MSG ? "DATA seq" : "ACK ack",
                               msg.type == DATA_MSG ? msg.seq_num : msg.ack_num);
                    } else if (msg.type == DATA_MSG) {
                        handle_data(i, s, &msg);
                    } else if (msg.type == ACK_MSG) {
                        handle_ack(s, &msg);
                    }
                }
            }
            service_window(s);
        }
        sem_unlock();
    }
    return NULL;
}

static void transmit(ktp_socket_info *s, const ktp_message *m) {
    sendto(s->udp_socket, m, sizeof(*m), 0,
           (struct sockaddr *)&s->dest_addr, sizeof(s->dest_addr));
}

static void *sender_thread(void *arg) {
    (void)arg;
    printf("Sender thread started\n");

    for (;;) {
        usleep(10000);

        sem_lock();
        int64_t now = now_ms();
        for (int i = 0; i < MAX_SOCKETS; i++) {
            ktp_socket_info *s = &socket_info[i];
            if (!s->is_active || s->udp_socket < 0) continue;

            if (s->inflight_count > 0 && now - s->inflight_time[0] >= RTO_MS) {
                /* timeout: retransmit the whole unacknowledged window (Go-Back-N) */
                printf("[socket %d] timeout, retransmitting %d message(s) from seq %d\n",
                       i, s->inflight_count, s->snd_base);
                for (int k = 0; k < s->inflight_count; k++) {
                    transmit(s, &s->inflight[k]);
                    s->inflight_time[k] = now;
                }
            } else if (s->inflight_count == 0 && s->peer_window == 0 &&
                       s->send_buffer_count > 0 && now - s->last_probe_ms >= RTO_MS) {
                /* receiver advertised a zero window: send one message as a probe */
                s->peer_window = 1;
                s->last_probe_ms = now;
            }

            while (s->send_buffer_count > 0 &&
                   s->inflight_count < MAX_WINDOW_SIZE &&
                   s->inflight_count < s->peer_window) {
                ktp_message *m = &s->inflight[s->inflight_count];
                *m = s->send_buffer[0];
                m->seq_num = s->snd_next++;
                s->inflight_time[s->inflight_count] = now;
                s->inflight_count++;

                for (int k = 0; k + 1 < s->send_buffer_count; k++) {
                    s->send_buffer[k] = s->send_buffer[k + 1];
                }
                s->send_buffer_count--;

                transmit(s, m);
            }
        }
        sem_unlock();
    }
    return NULL;
}

static void serve_request(int idx, ktp_socket_info *s) {
    if (s->req == REQ_CREATE) {
        int fd = socket(AF_INET, SOCK_DGRAM, 0);
        s->req_result = fd < 0 ? -1 : 0;
        s->req_errno = fd < 0 ? errno : 0;
        s->udp_socket = fd;
    } else if (s->req == REQ_BIND) {
        if (bind(s->udp_socket, (struct sockaddr *)&s->src_addr, sizeof(s->src_addr)) < 0) {
            s->req_result = -1;
            s->req_errno = errno;
        } else {
            s->req_result = 0;
            s->req_errno = 0;
            printf("[socket %d] bound to port %d, peer port %d\n", idx,
                   ntohs(s->src_addr.sin_port), ntohs(s->dest_addr.sin_port));
        }
    } else if (s->req == REQ_CLOSE) {
        if (s->udp_socket >= 0) close(s->udp_socket);
        s->udp_socket = -1;
        s->req_result = 0;
        s->req_errno = 0;
        printf("[socket %d] closed\n", idx);
    }
    s->req = REQ_NONE;   /* the library sees this and returns to the application */
}

/* Socket management: serves ksocket/kbind/kclose requests and reclaims
 * sockets that belonged to processes that died. */
static void *manager_thread(void *arg) {
    (void)arg;
    printf("Socket manager thread started\n");

    int ticks = 0;
    for (;;) {
        usleep(10000);
        ticks++;

        sem_lock();
        for (int i = 0; i < MAX_SOCKETS; i++) {
            ktp_socket_info *s = &socket_info[i];
            if (s->is_active && s->req != REQ_NONE) serve_request(i, s);
        }

        if (ticks % 500 == 0) {   /* every ~5 s */
            for (int i = 0; i < MAX_SOCKETS; i++) {
                ktp_socket_info *s = &socket_info[i];
                if (s->is_active && kill(s->pid, 0) == -1 && errno == ESRCH) {
                    if (s->udp_socket >= 0) close(s->udp_socket);
                    s->udp_socket = -1;
                    s->is_active = 0;
                    printf("[socket %d] reclaimed: process %d is gone\n", i, (int)s->pid);
                }
            }
        }
        sem_unlock();
    }
    return NULL;
}

static int count_active_sockets(void) {
    int count = 0;
    sem_lock();
    for (int i = 0; i < MAX_SOCKETS; i++) {
        if (socket_info[i].is_active) count++;
    }
    sem_unlock();
    return count;
}

int main(int argc, char *argv[]) {
    setvbuf(stdout, NULL, _IOLBF, 0);

    if (argc > 2) {
        fprintf(stderr, "Usage: %s [drop_probability]\n", argv[0]);
        return 1;
    }
    if (argc == 2) {
        drop_prob = atof(argv[1]);
        if (drop_prob < 0.0 || drop_prob >= 1.0) {
            fprintf(stderr, "drop_probability must be in [0, 1)\n");
            return 1;
        }
    }
    srand((unsigned)(time(NULL) ^ getpid()));

    /* remove leftovers from an earlier run */
    int old = shmget(SHM_KEY, 0, 0666);
    if (old >= 0) shmctl(old, IPC_RMID, NULL);
    old = semget(SEM_KEY, 1, 0666);
    if (old >= 0) semctl(old, 0, IPC_RMID);

    shm_id = shmget(SHM_KEY, sizeof(ktp_socket_info) * MAX_SOCKETS, IPC_CREAT | 0666);
    if (shm_id < 0) {
        perror("shmget failed");
        return 1;
    }
    socket_info = (ktp_socket_info *)shmat(shm_id, NULL, 0);
    if (socket_info == (void *)-1) {
        perror("shmat failed");
        cleanup_ipc();
        return 1;
    }
    sem_id = semget(SEM_KEY, 1, IPC_CREAT | 0666);
    if (sem_id < 0) {
        perror("semget failed");
        cleanup_ipc();
        return 1;
    }

    memset(socket_info, 0, sizeof(ktp_socket_info) * MAX_SOCKETS);
    for (int i = 0; i < MAX_SOCKETS; i++) socket_info[i].udp_socket = -1;

    union semun arg;
    arg.val = 1;
    if (semctl(sem_id, 0, SETVAL, arg) < 0) {
        perror("semctl failed");
        cleanup_ipc();
        return 1;
    }

    signal(SIGINT, on_signal);
    signal(SIGTERM, on_signal);

    pthread_t r_thread, s_thread, m_thread;
    if (pthread_create(&r_thread, NULL, receiver_thread, NULL) != 0 ||
        pthread_create(&s_thread, NULL, sender_thread, NULL) != 0 ||
        pthread_create(&m_thread, NULL, manager_thread, NULL) != 0) {
        perror("pthread_create failed");
        cleanup_ipc();
        return 1;
    }

    printf("\nRUDP daemon running\n");
    printf("Maximum sockets: %d, window: %d, RTO: %d ms, simulated loss: %.2f\n",
           MAX_SOCKETS, MAX_WINDOW_SIZE, RTO_MS, drop_prob);

    for (;;) {
        sleep(60);
        printf("daemon running (%d active sockets)\n", count_active_sockets());
    }
    return 0;
}
