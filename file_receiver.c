#include "rudp_socket.h"

#define BUFFER_SIZE 512
#define IDLE_LIMIT 300   /* 100 ms polls without data before giving up (30 s) */

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

    char filename[64];
    snprintf(filename, sizeof(filename), "received_file_port_%s.txt", argv[2]);

    FILE *file = fopen(filename, "wb");
    if (file == NULL) {
        perror("Failed to create output file");
        kclose(sockfd);
        return 1;
    }

    char buffer[BUFFER_SIZE];
    long total_received = 0;
    int idle = 0;
    int got_data = 0;

    while (1) {
        int n = krecvfrom(sockfd, buffer, BUFFER_SIZE, 0, NULL, NULL);
        if (n < 0) {
            if (errno == ENOMESSAGE) {
                usleep(100000);
                /* only start the idle countdown once the transfer has begun */
                if (got_data && ++idle > 50) {   /* ~5 s of silence */
                    printf("No data for 5 seconds, assuming transfer complete\n");
                    break;
                }
                if (!got_data && ++idle > IDLE_LIMIT) {
                    printf("No data arrived, giving up\n");
                    break;
                }
                continue;
            }
            perror("krecvfrom failed");
            break;
        }
        idle = 0;
        got_data = 1;
        fwrite(buffer, 1, (size_t)n, file);
        total_received += n;
    }

    fclose(file);
    printf("File reception complete. Total bytes received: %ld\n", total_received);
    printf("Data saved to: %s\n", filename);

    kclose(sockfd);
    return 0;
}
