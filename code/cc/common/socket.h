/*
 * Copyright (C) dirlt
 */

#ifndef __SPERM_CC_COMMON_SOCKET_H__
#define __SPERM_CC_COMMON_SOCKET_H__

#include <netdb.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/tcp.h>
#include <arpa/inet.h>
#include <cassert>
#include "common/logger_inl.h"
#include "common/utils.h"

namespace common {

static inline int create_tcp_socket() {
  int fd = socket(AF_INET, SOCK_STREAM, 0);
  if(fd == -1) {
    SPERM_WARNING("socket(AF_INET,SOCKET_STREAM,0) failed(%s)", SERRNO);
  }
  return fd;
}

static inline void set_tcp_nodelay(int fd) {
  int value = 1;
  socklen_t value_len = sizeof(value);
  if(setsockopt(fd, SOL_TCP, TCP_NODELAY, &value, value_len) != 0) {
    SPERM_WARNING("setsockopt(%d,SOL_TCP,TCP_NODELAY) failed(%s)", fd, SERRNO);
  }
}

static inline void set_ip_reuseaddr(int fd) {
  int value = 1;
  socklen_t value_len = sizeof(value);
  if(setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &value, value_len) != 0) {
    SPERM_WARNING("setsockopt(%d,SOL_SOCKET,SO_REUSEADDR) failed(%s)", fd, SERRNO);
  }
}

static inline int tcp_accept(int fd) {
again:
  int sock = accept(fd, NULL, NULL);
  if(sock < 0) {
    if(errno == EINTR ||
        errno == ECONNABORTED) {
      goto again;
    }
    SPERM_WARNING("accept(%d) failed(%s)", fd, SERRNO);
  }
  return sock;
}

static inline int tcp_connect(int fd,
                              const char* ip,
                              int port) {
  struct sockaddr_in addr;
  memset(&addr, 0, sizeof(addr));
  socklen_t addrlen = sizeof(addr);
  addr.sin_family = AF_INET;
  inet_aton(ip, &(addr.sin_addr));
  addr.sin_port = htons(port);
again:
  int ret = connect(fd, reinterpret_cast<sockaddr*>(&addr), addrlen);
  if(ret != 0 && errno != EINPROGRESS) {
    if(errno == EINTR) {
      goto again;
    }
    SPERM_WARNING("connect(%d,%s,%d) failed(%s)", fd, ip, port, SERRNO);
    return -1;
  }
  if(errno == EINPROGRESS) {
    SPERM_DEBUG("connect inprogress");
    return 1;
  }
  return 0;
}

static inline int tcp_bind_listen(int fd,
                                  const char* ip,
                                  int port,
                                  int backlog) {
  struct sockaddr_in addr;
  memset(&addr, 0, sizeof(addr));
  socklen_t addrlen = sizeof(addr);
  addr.sin_family = AF_INET;
  inet_aton(ip, &(addr.sin_addr));
  addr.sin_port = htons(port);
  int ret = bind(fd, reinterpret_cast<sockaddr*>(&addr), addrlen);
  if(ret != 0) {
    SPERM_WARNING("bind(%d,%s,%d) failed(%s)", fd, ip, port, SERRNO);
    return -1;
  }
  ret = listen(fd, backlog);
  if(ret != 0) {
    SPERM_WARNING("listen(%d,%d) failed(%s)", fd, backlog, SERRNO);
    return -1;
  }
  return 0;
}

static inline int get_peer_name(int fd, std::string* ip, int* port) {
  struct sockaddr_in addr;
  socklen_t addrlen = sizeof(addr);
  memset(&addr, 0, sizeof(addr));
  int ret = getpeername(fd, reinterpret_cast<sockaddr*>(&addr), &addrlen);
  if(ret != 0) {
    SPERM_WARNING("getpeername(%d) failed(%s)", fd, SERRNO);
    return -1;
  }
  char ip6[INET6_ADDRSTRLEN];
  inet_ntop(AF_INET, &(addr.sin_addr), ip6, sizeof(ip6));
  *ip = ip6;
  *port = ntohs(addr.sin_port);
  return 0;
}

static inline int get_sock_name(int fd, std::string* ip, int* port) {
  struct sockaddr_in addr;
  socklen_t addrlen = sizeof(addr);
  memset(&addr, 0, sizeof(addr));
  int ret = getsockname(fd, reinterpret_cast<sockaddr*>(&addr), &addrlen);
  if(ret != 0) {
    SPERM_WARNING("getsockname(%d) failed(%s)", fd, SERRNO);
    return -1;
  }
  char ip6[INET6_ADDRSTRLEN];
  inet_ntop(AF_INET, &(addr.sin_addr), ip6, sizeof(ip6));
  *ip = ip6;
  *port = ntohs(addr.sin_port);
  return 0;
}

static inline int get_ip(const char* hostname, std::string* ip) {
  char buf[16 * 1024]; // I think it's enough.
  struct hostent ent;
  struct hostent* entp;
  int herrno = 0;
  gethostbyname2_r(hostname, AF_INET, &ent, buf, sizeof(buf), &entp, &herrno);
  if(!entp) {
    SPERM_WARNING("gethostbyname2_r(%s,AF_INET) failed(%s)", hostname, hstrerror(herrno));
    return -1;
  }
  char ip6[INET6_ADDRSTRLEN];
  inet_ntop(entp->h_addrtype, entp->h_addr_list[0], ip6, sizeof(ip6));
  *ip = ip6;
  return 0;
}

static inline std::string get_local_ip() {
  std::string hostname = get_hostname();
  std::string ip;
  assert(get_ip(hostname.c_str(), &ip) == 0);
  return ip;
}

} // namespace common

#endif // __SPERM_CC_COMMON_SOCKET_H__
