#include "rudp_socket.h"

#define BUFFER_SIZE 512

int main(int argc, char *argv[]) {
    if (argc != 5) {
        printf("Usage: %s <local_ip> <local_port> <peer_ip> <peer_port>\n", argv[0]);
        exit(1);
    }

    int sockfd = ksocket(AF_INET, SOCK_KTP, 0);
    if (sockfd < 0) {
        perror("ksocket failed");
        return 1;
    }

    struct sockaddr_in local_addr;
    memset(&local_addr, 0, sizeof(local_addr));
    local_addr.sin_family = AF_INET;
    local_addr.sin_addr.s_addr = inet_addr(argv[1]);
    local_addr.sin_port = htons(atoi(argv[2]));

    struct sockaddr_in peer_addr;
    memset(&peer_addr, 0, sizeof(peer_addr));
    peer_addr.sin_family = AF_INET;
    peer_addr.sin_addr.s_addr = inet_addr(argv[3]);
    peer_addr.sin_port = htons(atoi(argv[4]));

    if (kbind(sockfd, (struct sockaddr *)&local_addr, sizeof(local_addr),
              (struct sockaddr *)&peer_addr, sizeof(peer_addr)) < 0) {
        perror("kbind failed");
        kclose(sockfd);
        return 1;
    }

    FILE *file = fopen("testfile.txt", "rb");
    if (file == NULL) {
        perror("Failed to open testfile.txt");
        kclose(sockfd);
        return 1;
    }

    char buffer[BUFFER_SIZE];
    size_t bytes_read;
    long total_sent = 0;

    while ((bytes_read = fread(buffer, 1, BUFFER_SIZE, file)) > 0) {
        /* retry the same chunk until the send buffer has room */
        while (ksendto(sockfd, buffer, bytes_read, 0,
                       (struct sockaddr *)&peer_addr, sizeof(peer_addr)) < 0) {
            if (errno == ENOSPACE) {
                usleep(10000);
                continue;
            }
            perror("ksendto failed");
            fclose(file);
            kclose(sockfd);
            return 1;
        }
        total_sent += (long)bytes_read;
    }
    fclose(file);

    printf("Queued %ld bytes, waiting for them to be acknowledged...\n", total_sent);
    kclose(sockfd);   /* blocks until everything queued has been acknowledged */
    printf("File sending complete. Total bytes sent: %ld\n", total_sent);
    return 0;
}
