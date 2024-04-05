#include <errno.h>
#include <netinet/in.h>
#include <netinet/ip.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <unistd.h>

typedef struct x {
  char key[128];
  char value[128];
  struct timeval *ttl;
} map;

int compareTimeval(const struct timeval *tv1, const struct timeval *tv2) {
  if (tv1->tv_sec < tv2->tv_sec) {
    return -1;
  } else if (tv1->tv_sec > tv2->tv_sec) {
    return 1;
  } else {
    if (tv1->tv_usec < tv2->tv_usec) {
      return -1;
    } else if (tv1->tv_usec > tv2->tv_usec) {
      return 1;
    } else {
      return 0;
    }
  }
}

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
  map *redis[10000];
  int redis_size = 0;

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
    } else if (strcasecmp(res[0], "SET") == 0) {
      map *new_val = (map *)malloc(sizeof(map));
      strcpy(new_val->key, res[1]);
      strcpy(new_val->value, res[2]);
      if (strcasecmp(res[3], "PX") == 0) {
        struct timeval tv;
        long long millsec = atoi(res[4]);
        gettimeofday(&tv, NULL);
        tv.tv_sec += millsec / 1000LL;
        tv.tv_usec += (millsec % 1000LL) * 1000LL;
        new_val->ttl = &tv;
      } else {
        new_val->ttl = NULL;
      }
      redis[redis_size++] = new_val;
      char *message = "+OK\r\n";
      send(client, message, strlen(message), 0);
    } else if (strcasecmp(res[0], "GET") == 0) {
      map *val = NULL;
      for (int i = 0; i < redis_size; i++) {
        struct timeval tv;
        gettimeofday(&tv, NULL);
        if (redis[i]->ttl != NULL && compareTimeval(redis[i]->ttl, &tv) < 0) {
          redis[i] = redis[--redis_size];
          i--;
          continue;
        }
        if (strcmp(res[1], redis[i]->key) == 0) {
          val = redis[i];
          break;
        }
      }
      char message[1024] = {0};
      if (val == NULL) {
        char *message = "$-1\r\n";
        send(client, message, strlen(message), 0);
      } else {
        int len = strlen(val->value);
        int cnt = 0;
        for (int i = len; i > 0; i /= 10) {
          cnt++;
        }
        printf("%d", cnt);
        sprintf(message, "$%d\r\n%s\r\n", len, val->value);
        send(client, message, len + cnt + 5, 0);
      }
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
