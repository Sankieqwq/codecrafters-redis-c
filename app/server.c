#include <arpa/inet.h>
#include <errno.h>
#include <getopt.h>
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

extern char *optarg;
extern int optind, opterr, optopt;

typedef struct x {
  char key[128];
  char value[128];
  struct timeval *ttl;
} map;

typedef struct y {
  int client_fd;
  int is_master;
} client;

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
  client c = *(client *)client_fd;
  int client_f = c.client_fd;
  char buffer[1024];
  map *redis[10000];
  int redis_size = 0;

  while (read(client_f, buffer, 1024) != 0) {
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
      send(client_f, message, strlen(message), 0);
    } else if (strcasecmp(res[0], "ECHO") == 0) {
      int len = strlen(res[1]);
      char message[1024] = {0};
      sprintf(message, "$%d\r\n%s\r\n", len, res[1]);
      send(client_f, message, len + 6, 0);
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
      send(client_f, message, strlen(message), 0);
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
      if (val == NULL) {
        char *message = "$-1\r\n";
        send(client_f, message, strlen(message), 0);
      } else {
        char message[1024] = {0};
        int len = strlen(val->value);
        int cnt = 0;
        for (int i = len; i > 0; i /= 10) {
          cnt++;
        }
        sprintf(message, "$%d\r\n%s\r\n", len, val->value);
        send(client_f, message, len + cnt + 5, 0);
      }
    } else if (strcasecmp(res[0], "INFO") == 0) {
      if (strcasecmp(res[1], "replication") == 0) {
        char message[1024] = {0};
        int j, siz;
        if (c.is_master) {
          siz = 11 + 54 + 20 + 4;
          j = sprintf(message, "$%d\r\nrole:master\r\n", siz);
        } else {
          siz = 10 + 54 + 20 + 4;
          j = sprintf(message, "$%d\r\nrole:slave\r\n", siz);
        }
        j += sprintf(message + j,
                     "master_replid:"
                     "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb\r\n");
        j += sprintf(message + j, "master_repl_offset:0\r\n");
        send(client_f, message, strlen(message), 0);
      }
    }
  }
}

static struct option long_options[] = {
    {"port", required_argument, NULL, 'p'},
    {"replicaof", required_argument, NULL, 'r'},
};

int main(int argc, char **argv) {
  // Disable output buffering
  setbuf(stdout, NULL);

  // You can use print statements as follows for debugging, they'll be visible
  // when running tests.
  printf("Logs from your program will appear here!\n");
  int index = 0;
  int c = 0;
  int port = 6379;
  int master = 1;
  char *master_host = NULL;
  int master_port = 0;

  while (EOF != (c = getopt_long(argc, argv, "p:r:", long_options, &index))) {
    switch (c) {
    case 'p':
      port = atoi(argv[optind - 1]);
      break;
    case 'r':
      master = 0;
      master_host = argv[optind - 1];
      master_port = atoi(argv[optind]);
      break;
    case '?':
      printf("unknow option:%c\n", optopt);
      break;
    default:
      break;
    }
  }
  // Uncomment this block to pass the first stage
  //
  int server_fd, client_addr_len;
  struct sockaddr_in client_addr;

  if (!master) {
    int master_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (master_fd == -1) {
      printf("Socket creation to master failed: %s...\n", strerror(errno));
      return 1;
    }
    struct sockaddr_in master_addr = {
        .sin_family = AF_INET,
        .sin_port = htons(master_port),
        .sin_addr = {htonl(INADDR_ANY)},
    };
    if(strcmp(master_host, "localhost")==0){
      master_host = "127.0.0.1";
    }
    if (inet_pton(AF_INET, master_host, &master_addr.sin_addr) <= 0) {
      printf("Invalid master address / Address not supported");
      close(master_fd);
      return 1;
    }
    if (connect(master_fd, (const struct sockaddr *)&master_addr,
                sizeof(master_addr)) != 0) {
      printf("Master connection failed");
      close(master_fd);
      return 1;
    }
    send(master_fd, "*1\r\n$4\r\nping\r\n", strlen("*1\r\n$4\r\nping\r\n"),0);
    printf("send");
  }

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
      .sin_port = htons(port),
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

    client c = {client_fd, master};
    pthread_t thread;
    pthread_create(&thread, NULL, (void *)send_ping, &c);
    pthread_detach(thread);
  }

  close(server_fd);
  pthread_exit(NULL);

  return 0;
}
