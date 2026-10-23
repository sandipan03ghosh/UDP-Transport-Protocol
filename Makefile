CC = gcc
CFLAGS = -Wall -Wextra -g
LIBS = -lpthread -lrt

BIN_DIR = bin
LIB_DIR = lib
OBJ_DIR = obj

LIBRARY = $(LIB_DIR)/librudp.a
LIBRARY_OBJS = $(OBJ_DIR)/rudp_socket.o

DAEMON = $(BIN_DIR)/rudp_daemon
SENDER = $(BIN_DIR)/file_sender
RECEIVER = $(BIN_DIR)/file_receiver

all: $(LIBRARY) $(DAEMON) $(SENDER) $(RECEIVER)

$(BIN_DIR) $(LIB_DIR) $(OBJ_DIR):
	mkdir -p $@

$(OBJ_DIR)/%.o: %.c rudp_socket.h | $(OBJ_DIR)
	$(CC) $(CFLAGS) -c $< -o $@

$(LIBRARY): $(LIBRARY_OBJS) | $(LIB_DIR)
	ar rcs $@ $^

$(DAEMON): rudp_daemon.c rudp_socket.h $(LIBRARY) | $(BIN_DIR)
	$(CC) $(CFLAGS) rudp_daemon.c -o $@ -L$(LIB_DIR) -lrudp $(LIBS)

$(SENDER): file_sender.c rudp_socket.h $(LIBRARY) | $(BIN_DIR)
	$(CC) $(CFLAGS) file_sender.c -o $@ -L$(LIB_DIR) -lrudp $(LIBS)

$(RECEIVER): file_receiver.c rudp_socket.h $(LIBRARY) | $(BIN_DIR)
	$(CC) $(CFLAGS) file_receiver.c -o $@ -L$(LIB_DIR) -lrudp $(LIBS)

clean:
	rm -rf $(OBJ_DIR) $(LIB_DIR) $(BIN_DIR)

.PHONY: all clean
