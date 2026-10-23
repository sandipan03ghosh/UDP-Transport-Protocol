#include "rudp_socket.h"

ktp_socket_info *socket_info = NULL;
int shm_id = -1;
int sem_id = -1;

/* Attach to the segments created by the daemon. Never creates them: a
 * semaphore created here would start at 0 and block every caller. */
static int init_shared_memory(void) {
    if (socket_info != NULL) return 0;

    int id = shmget(SHM_KEY, sizeof(ktp_socket_info) * MAX_SOCKETS, 0666);
    if (id < 0) {
        fprintf(stderr, "cannot attach to shared memory: is the rudp daemon running?\n");
        errno = ENOTCONN;
        return -1;
    }
    void *p = shmat(id, NULL, 0);
    if (p == (void *)-1) {
        perror("shmat failed");
        return -1;
    }
    int sid = semget(SEM_KEY, 1, 0666);
    if (sid < 0) {
        fprintf(stderr, "cannot attach to semaphore: is the rudp daemon running?\n");
        shmdt(p);
        errno = ENOTCONN;
        return -1;
    }
    shm_id = id;
    sem_id = sid;
    socket_info = (ktp_socket_info *)p;
    return 0;
}

/* SEM_UNDO releases the lock if a process dies while holding it. */
void sem_lock(void) {
    struct sembuf sb = {0, -1, SEM_UNDO};
    while (semop(sem_id, &sb, 1) < 0) {
        if (errno == EINTR) continue;
        perror("semop lock failed");
        exit(1);
    }
}

void sem_unlock(void) {
    struct sembuf sb = {0, 1, SEM_UNDO};
    while (semop(sem_id, &sb, 1) < 0) {
        if (errno == EINTR) continue;
        perror("semop unlock failed");
        exit(1);
    }
}

int drop_message(double prob) {
    double r = (double)rand() / (double)RAND_MAX;
    return r < prob;
}

/* Wait until the daemon has served the request posted in slot idx.
 * Returns 0 and sets *result/*err_no, or -1 on timeout. */
static int wait_request(int idx, int *result, int *err_no) {
    for (int waited = 0; waited < REQ_TIMEOUT_MS; waited++) {
        sem_lock();
        if (socket_info[idx].req == REQ_NONE) {
            *result = socket_info[idx].req_result;
            *err_no = socket_info[idx].req_errno;
            sem_unlock();
            return 0;
        }
        sem_unlock();
        usleep(1000);
    }
    errno = ETIMEDOUT;
    return -1;
}

int ksocket(int domain, int type, int protocol) {
    (void)protocol;
    if (type != SOCK_KTP) {
        errno = EINVAL;
        return -1;
    }
    if (domain != AF_INET) {
        errno = EAFNOSUPPORT;
        return -1;
    }
    if (init_shared_memory() < 0) return -1;

    sem_lock();
    int slot = -1;
    for (int i = 0; i < MAX_SOCKETS; i++) {
        if (socket_info[i].is_active == 0) {
            slot = i;
            break;
        }
    }
    if (slot == -1) {
        sem_unlock();
        errno = ENOSPACE;
        return -1;
    }

    ktp_socket_info *s = &socket_info[slot];
    memset(s, 0, sizeof(*s));
    s->is_active = 1;
    s->pid = getpid();
    s->udp_socket = -1;
    s->peer_window = MAX_WINDOW_SIZE;
    s->req = REQ_CREATE;   /* the daemon creates the UDP socket */
    sem_unlock();

    int result = 0, err = 0;
    if (wait_request(slot, &result, &err) < 0 || result < 0) {
        if (result < 0) errno = err;
        sem_lock();
        s->req = REQ_NONE;
        s->is_active = 0;
        sem_unlock();
        return -1;
    }
    return slot;
}

int kbind(int sockfd, const struct sockaddr *src_addr, socklen_t src_addrlen,
          const struct sockaddr *dest_addr, socklen_t dest_addrlen) {
    if (sockfd < 0 || sockfd >= MAX_SOCKETS) {
        errno = EBADF;
        return -1;
    }
    if (src_addrlen < sizeof(struct sockaddr_in) || dest_addrlen < sizeof(struct sockaddr_in)) {
        errno = EINVAL;
        return -1;
    }
    if (init_shared_memory() < 0) return -1;

    sem_lock();
    if (socket_info[sockfd].is_active == 0 || socket_info[sockfd].req != REQ_NONE) {
        sem_unlock();
        errno = EBADF;
        return -1;
    }
    memcpy(&socket_info[sockfd].src_addr, src_addr, sizeof(struct sockaddr_in));
    memcpy(&socket_info[sockfd].dest_addr, dest_addr, sizeof(struct sockaddr_in));
    socket_info[sockfd].req = REQ_BIND;   /* the daemon binds the UDP socket */
    sem_unlock();

    int result, err;
    if (wait_request(sockfd, &result, &err) < 0) return -1;
    if (result < 0) {
        errno = err;
        return -1;
    }
    return 0;
}

int ksendto(int sockfd, const void *buf, size_t len, int flags,
            const struct sockaddr *dest_addr, socklen_t dest_addrlen) {
    (void)flags;
    if (sockfd < 0 || sockfd >= MAX_SOCKETS) {
        errno = EBADF;
        return -1;
    }
    if (dest_addrlen < sizeof(struct sockaddr_in)) {
        errno = EINVAL;
        return -1;
    }
    if (len > MAX_MSG_SIZE) {
        errno = EMSGSIZE;
        return -1;
    }
    if (init_shared_memory() < 0) return -1;

    sem_lock();
    ktp_socket_info *s = &socket_info[sockfd];
    if (s->is_active == 0) {
        sem_unlock();
        errno = EBADF;
        return -1;
    }

    const struct sockaddr_in *dest = (const struct sockaddr_in *)dest_addr;
    if (s->dest_addr.sin_port == 0 ||
        dest->sin_addr.s_addr != s->dest_addr.sin_addr.s_addr ||
        dest->sin_port != s->dest_addr.sin_port) {
        sem_unlock();
        errno = ENOTBOUND;
        return -1;
    }

    if (s->send_buffer_count >= MAX_WINDOW_SIZE) {
        sem_unlock();
        errno = ENOSPACE;
        return -1;
    }

    /* the sequence number is assigned by the daemon when it transmits */
    ktp_message *msg = &s->send_buffer[s->send_buffer_count];
    memset(msg, 0, sizeof(*msg));
    msg->type = DATA_MSG;
    msg->len = (uint16_t)len;
    memcpy(msg->data, buf, len);
    s->send_buffer_count++;

    sem_unlock();
    return (int)len;
}

int krecvfrom(int sockfd, void *buf, size_t len, int flags,
              struct sockaddr *src_addr, socklen_t *src_addrlen) {
    (void)flags;
    if (sockfd < 0 || sockfd >= MAX_SOCKETS) {
        errno = EBADF;
        return -1;
    }
    if (init_shared_memory() < 0) return -1;

    sem_lock();
    ktp_socket_info *s = &socket_info[sockfd];
    if (s->is_active == 0) {
        sem_unlock();
        errno = EBADF;
        return -1;
    }
    if (s->recv_buffer_count == 0) {
        sem_unlock();
        errno = ENOMESSAGE;
        return -1;
    }

    ktp_message *msg = &s->recv_buffer[0];
    size_t n = msg->len < len ? msg->len : len;   /* real length, not the buffer size */
    memcpy(buf, msg->data, n);

    for (int i = 0; i < s->recv_buffer_count - 1; i++) {
        s->recv_buffer[i] = s->recv_buffer[i + 1];
    }
    s->recv_buffer_count--;
    /* the daemon notices the freed space and sends the window update */

    if (src_addr != NULL && src_addrlen != NULL) {
        socklen_t copy = *src_addrlen < sizeof(s->dest_addr) ? *src_addrlen : sizeof(s->dest_addr);
        memcpy(src_addr, &s->dest_addr, copy);   /* the peer's address */
        *src_addrlen = sizeof(s->dest_addr);
    }

    sem_unlock();
    return (int)n;
}

int kclose(int sockfd) {
    if (sockfd < 0 || sockfd >= MAX_SOCKETS) {
        errno = EBADF;
        return -1;
    }
    if (init_shared_memory() < 0) return -1;

    sem_lock();
    if (socket_info[sockfd].is_active == 0) {
        sem_unlock();
        errno = EBADF;
        return -1;
    }
    sem_unlock();

    /* let queued and unacknowledged data finish before closing */
    int drained = 0;
    for (int waited = 0; waited < CLOSE_DRAIN_MS; waited += 10) {
        sem_lock();
        int pending = socket_info[sockfd].send_buffer_count + socket_info[sockfd].inflight_count;
        sem_unlock();
        if (pending == 0) {
            drained = 1;
            break;
        }
        usleep(10000);
    }
    if (!drained) {
        fprintf(stderr, "kclose: gave up waiting for unacknowledged data\n");
    }

    sem_lock();
    socket_info[sockfd].req = REQ_CLOSE;
    sem_unlock();

    int result, err;
    int rc = wait_request(sockfd, &result, &err);

    sem_lock();
    socket_info[sockfd].req = REQ_NONE;
    socket_info[sockfd].is_active = 0;
    sem_unlock();

    return rc < 0 ? -1 : 0;
}
