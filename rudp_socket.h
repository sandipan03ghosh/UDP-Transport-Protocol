#ifndef RUDP_SOCKET_H
#define RUDP_SOCKET_H

#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <unistd.h>
#include <string.h>
#include <signal.h>
#include <errno.h>
#include <time.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/ipc.h>
#include <sys/shm.h>
#include <sys/sem.h>

#define MAX_SOCKETS 10
#define DATA_MSG 1
#define ACK_MSG 2
#define SOCK_KTP 3
#define SHM_KEY 1234
#define SEM_KEY 5678
#define MAX_MSG_SIZE 512
#define MAX_WINDOW_SIZE 10
#define RTO_MS 2000            /* retransmission timeout */
#define REQ_TIMEOUT_MS 5000    /* max wait for the daemon to serve a request */
#define CLOSE_DRAIN_MS 30000   /* max wait in kclose for unacked data */
#define DROP_PROB 0.2          /* default simulated loss probability */

/* custom errno values */
#define ENOSPACE 200    /* no space in buffer */
#define ENOTBOUND 201   /* destination does not match the bound address */
#define ENOMESSAGE 202  /* no message in receive buffer */

/* requests from the library to the daemon (daemon owns the UDP sockets) */
#define REQ_NONE 0
#define REQ_CREATE 1
#define REQ_BIND 2
#define REQ_CLOSE 3

typedef struct {
    uint8_t type;         /* DATA_MSG or ACK_MSG */
    uint8_t seq_num;      /* DATA: sequence number (mod 256) */
    uint8_t ack_num;      /* ACK: next expected sequence number (cumulative) */
    uint8_t window_size;  /* ACK: free slots in the receiver's buffer */
    uint16_t len;         /* DATA: number of valid bytes in data */
    char data[MAX_MSG_SIZE];
} ktp_message;

typedef struct {
    int is_active;
    pid_t pid;
    int udp_socket;       /* valid only inside the daemon process */

    int req;              /* pending request for the daemon */
    int req_result;
    int req_errno;

    struct sockaddr_in src_addr;
    struct sockaddr_in dest_addr;

    /* sender side */
    ktp_message send_buffer[MAX_WINDOW_SIZE];   /* queued by ksendto, not yet transmitted */
    int send_buffer_count;
    ktp_message inflight[MAX_WINDOW_SIZE];      /* transmitted, not yet acknowledged (oldest first) */
    int64_t inflight_time[MAX_WINDOW_SIZE];     /* last transmit time in ms */
    int inflight_count;
    uint8_t snd_base;     /* sequence number of the oldest unacked message */
    uint8_t snd_next;     /* sequence number given to the next transmitted message */
    int peer_window;      /* last window advertised by the receiver */
    int64_t last_probe_ms;

    /* receiver side */
    ktp_message recv_buffer[MAX_WINDOW_SIZE];   /* in-order messages waiting for krecvfrom */
    int recv_buffer_count;
    ktp_message ooo[MAX_WINDOW_SIZE];           /* out-of-order messages held back */
    int ooo_used[MAX_WINDOW_SIZE];
    uint8_t rcv_base;     /* next expected sequence number */
    int nospace_flag;     /* window update owed to the sender */
} ktp_socket_info;

extern ktp_socket_info *socket_info;
extern int shm_id;
extern int sem_id;

int ksocket(int domain, int type, int protocol);
int kbind(int sockfd, const struct sockaddr *src_addr, socklen_t src_addrlen,
          const struct sockaddr *dest_addr, socklen_t dest_addrlen);
int ksendto(int sockfd, const void *buf, size_t len, int flags,
            const struct sockaddr *dest_addr, socklen_t dest_addrlen);
int krecvfrom(int sockfd, void *buf, size_t len, int flags,
              struct sockaddr *src_addr, socklen_t *src_addrlen);
int kclose(int sockfd);

void sem_lock(void);
void sem_unlock(void);
int drop_message(double probability);

#endif /* RUDP_SOCKET_H */
