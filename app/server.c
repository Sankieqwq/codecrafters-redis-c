#include <errno.h>
#include <netinet/in.h>
#include <netinet/ip.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <unistd.h>

int parser(char **read, char result[][128], int size_read) {
  int res_idx = 0;
  for (int i = 0; i < size_read; i++) {
    if (read[i][0] == '*')
      continue;
    int len = 0, idx = 0;
    while (read[i][idx] == '\r' || read[i][idx] == '\n')
      idx++;
    while (read[i][idx] >= '0' && read[i][idx] <= '9') {
      len *= 10;
      len += read[i][idx] - '0';
      idx++;
    }
    while (read[i][idx] == '\r' || read[i][idx] == '\n')
      idx++;
    for (int j = idx; j < idx + len; j++) {
      result[res_idx][j - idx] = read[i][j];
    }
    res_idx++;
  }
  return 0;
}

void send_ping(void *client_fd) {
  int client = *(int *)client_fd;
  char buffer[1024];
  while (read(client, buffer, 1024) != 0) {
    char *command[1024];
    int idx = 0;
    command[0] = strtok(buffer, "$");
    while (command[idx] != NULL) {
      idx++;
      command[idx] = strtok(NULL, "$");
    }
    char res[128][128];
    parser(command, res, idx);
    if (strcasecmp(res[0], "PING") == 0) {
    char *message = "+PONG\r\n";
    send(client, message, strlen(message), 0);
    } else if (strcasecmp(res[0], "ECHO") == 0) {
      int len = strlen(res[1]);
      char message[1024] = {0};
      sprintf(message, "$%d\r\n%s\r\n", len, res[1]);
      send(client, message, len + 6, 0);
    }
  }
}
int main() {
  // Disable output buffering
  setbuf(stdout, NULL);

  // You can use print statements as follows for debugging, they'll be visible
  // when running tests.
  printf("Logs from your program will appear here!\n");

  // Uncomment this block to pass the first stage
  //
  int server_fd, client_addr_len;
  struct sockaddr_in client_addr;

  server_fd = socket(AF_INET, SOCK_STREAM, 0);
  if (server_fd == -1) {
    printf("Socket creation failed: %s...\n", strerror(errno));
    return 1;
  }

  // Since the tester restarts your program quite often, setting SO_REUSEADDR
  // ensures that we don't run into 'Address already in use' errors
  int reuse = 1;
  if (setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse)) <
      0) {
    printf("SO_REUSEADDR failed: %s \n", strerror(errno));
    return 1;
  }

  struct sockaddr_in serv_addr = {
      .sin_family = AF_INET,
      .sin_port = htons(6379),
      .sin_addr = {htonl(INADDR_ANY)},
  };

  if (bind(server_fd, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) != 0) {
    printf("Bind failed: %s \n", strerror(errno));
    return 1;
  }

  int connection_backlog = 5;
  if (listen(server_fd, connection_backlog) != 0) {
    printf("Listen failed: %s \n", strerror(errno));
    return 1;
  }

  printf("Waiting for a client to connect...\n");
  client_addr_len = sizeof(client_addr);

  int client_fd;
  while ((client_fd = accept(server_fd, (struct sockaddr *)&client_addr,
                             (socklen_t *)&client_addr_len)) > 0) {
    printf("Client connected\n");

    pthread_t thread;
    pthread_create(&thread, NULL, (void *)send_ping, &client_fd);
    pthread_detach(thread);
  }

  close(server_fd);
  pthread_exit(NULL);

  return 0;
}
