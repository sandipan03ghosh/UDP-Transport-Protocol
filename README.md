# KTP: Reliable Transport Protocol over UDP

A reliable, in-order transport protocol built on top of UDP, written in C for Linux. Applications use a socket-style library (`ksocket`, `kbind`, `ksendto`, `krecvfrom`, `kclose`) and a background daemon does the protocol work.

## Features

- Sequence numbers (8-bit, wrap-around safe) and cumulative ACKs
- Sliding-window flow control: the receiver advertises its free buffer space, and sends a window update when a full buffer frees up
- Timeout-based retransmission of the whole unacknowledged window (Go-Back-N style)
- Receiver-side buffering of out-of-order messages so the application always reads data in sequence
- Duplicate and stale messages are ignored and re-ACKed
- Simulated packet loss (default 20%, configurable) to exercise retransmission
- Shared-memory socket table protected by a System V semaphore (up to 10 sockets)
- Multi-threaded daemon (pthreads) that owns all UDP sockets

## Architecture

```
 app process A                     app process B
 (file_sender)                     (file_receiver)
     |  ksocket/kbind/ksendto           |  krecvfrom/kclose
     v                                  v
 +------------------ shared memory socket table ------------------+
 |  per socket: send queue, in-flight window, receive buffer,      |
 |  out-of-order buffer, sequence state     (guarded by semaphore) |
 +------------------------------+----------------------------------+
                                |
                        rudp_daemon (owns the UDP sockets)
                          - receiver thread: DATA/ACK handling, window updates
                          - sender thread:   transmit, retransmit on timeout
                          - manager thread:  create/bind/close requests,
                                             reclaim sockets of dead processes
```

The library never touches a UDP socket. `ksocket`, `kbind` and `kclose` post a request in shared memory and the daemon performs it. This is needed because file descriptors belong to one process.

## Files

| File | Purpose |
|---|---|
| `rudp_socket.h` / `rudp_socket.c` | message format, shared structures, the socket-style API |
| `rudp_daemon.c` | the daemon: protocol logic and threads |
| `file_sender.c` / `file_receiver.c` | demo apps that transfer `testfile.txt` |

## Build and run

Needs Linux (System V IPC, pthreads). On Windows use Docker Desktop or WSL.

```bash
make
./bin/rudp_daemon &                                   # optional argument: drop probability, e.g. 0.3
./bin/file_receiver 127.0.0.1 6001 127.0.0.1 6000 &   # local ip/port, peer ip/port
./bin/file_sender   127.0.0.1 6000 127.0.0.1 6001
cmp testfile.txt received_file_port_6001.txt          # no output means identical
```

Put some text in `testfile.txt` first. With Docker:

```bash
docker build -t ktp .
docker run -it --name ktp ktp
# in the container run the commands above; use `docker exec -it ktp bash` for more shells
```

The daemon and the apps must run in the same container or machine, because they share memory.

Stop the daemon with Ctrl+C (it removes the shared memory and semaphore).

## Limitations

- Fixed 2-second retransmission timeout, no adaptive RTO
- No congestion control
- Whole-window retransmission instead of selective retransmission
- One global lock for the socket table, and a fixed maximum of 10 sockets
- 8-bit sequence numbers with a 10-message window
- Meant for local experiments, not production use
