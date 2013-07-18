/*
 * Copyright (C) dirlt
 */

#include <sys/epoll.h>
#include <signal.h>
#include <pthread.h>
#include "common/utils.h"
#include "common/socket.h"
#include "common/logger_inl.h"

using namespace common;

void* thread_function(void* /*arg*/) {
  SPERM_DEBUG("start client...");
  int conn = create_tcp_socket();
  set_nonblock(conn);
  SPERM_DEBUG("start connect...");
  tcp_connect(conn, "127.0.0.1", 19870);
  set_tcp_nodelay(conn);

  char c = 'x';
  if(write(conn, &c, 1) < 0) {
    SPERM_WARNING("write failed(%s)", SERRNO);
  }
  close(conn);
  return NULL;
}

int main() {
  std::string ip = get_local_ip();
  SPERM_TRACE("local ip='%s'", ip.c_str());
  // --------------------
  signal(SIGPIPE, SIG_IGN);
  SPERM_DEBUG("start server...");
  int server = create_tcp_socket();
  set_ip_reuseaddr(server);
  SPERM_DEBUG("start bind listen...");
  tcp_bind_listen(server, "127.0.0.1", 19870, 10);
  pthread_t tid;
  pthread_create(&tid, NULL, thread_function, NULL);
  int conn = tcp_accept(server);
  SPERM_DEBUG("conn(%d)", conn);
  char c[1];
  read(conn, c, sizeof(c));
  printf("%c\n", c[0]);
  close(conn);
  pthread_join(tid, NULL);
  close(server);
}
