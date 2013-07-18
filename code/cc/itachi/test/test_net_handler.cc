/*
 * Copyright (C) dirlt
 */

#include <signal.h>
#include "itachi/itachi.h"
#include "common/logger_inl.h"
#include "common/utils.h"
#include "common/socket.h"

using namespace itachi;
using namespace common;

// #ifndef DEBUG
// #undef SPERM_TRACE
// #define SPERM_TRACE(fmt,...)
// #endif

class HttpSinker: public AsyncClient {
public:
  class HttpContext : public AsyncContext {
  public:
    std::string rbuf;
    std::string wbuf;
    int wpos;
    HttpContext(AsyncClient* client):
      AsyncContext(client) {
    }
    virtual ~HttpContext() {
      SPERM_TRACE("---------->release object(%p)", this);
      close_fd();
    }
  }; // class HttpContext
  HttpContext* ctx;
  // ------------------------------
  HttpSinker(Itachi* itachi):
    AsyncClient(itachi),
    ctx(NULL) {
  }
  virtual ~HttpSinker() {
    delete ctx;
  }
  enum {
    AA_BIND_LISTEN = AsyncContext::AA_USER + 1,
    AA_READ,
    AA_WRITE,
  };
  void start() {
    ctx = new HttpContext(this);
    int fd = create_tcp_socket();
    set_nonblock(fd);
    set_ip_reuseaddr(fd);
    tcp_bind_listen(fd, "0.0.0.0", 19880, 100);
    ctx->initNetReadEvent(AA_BIND_LISTEN, fd);
    itachi->launch(ctx);
  }
  virtual void onComplete(AsyncContext* ctx_) {
    HttpContext* ctx = static_cast<HttpContext*>(ctx_);
    SPERM_TRACE("onComplete(%p)", ctx);
    if(ctx->action == AA_BIND_LISTEN) {
      SPERM_TRACE("ready to accept...");
      int conn = tcp_accept(ctx->fd());
      set_nonblock(conn);
      HttpContext* ctx2 = new HttpContext(ctx->client);
      SPERM_TRACE("---------->new object(%p)", ctx2);
      ctx2->initNetReadEvent(AA_READ, conn);
      itachi->launch(ctx2);
      ctx->initNetReadEvent(AA_BIND_LISTEN, ctx->fd());
      itachi->launch(ctx);

    } else if(ctx->action == AA_READ) {
      SPERM_TRACE("ready to read...");
      char buf[64 * 1024];
      int size = read(ctx->fd(), buf, sizeof(buf));
      SPERM_TRACE("----->ref count(%p,%d)", ctx, ctx->getRefCount());
      if(size <= 0) {
        ctx->release();
        SPERM_TRACE("peer closed(%s)", SERRNO);
        return ; // peer close. maybe abnormally.
      }
      ctx->rbuf.append(buf, size);
      size = ctx->rbuf.size();
      if(size >= 4 &&
          ctx->rbuf[size - 1] == '\n' &&
          ctx->rbuf[size - 2] == '\r' &&
          ctx->rbuf[size - 3] == '\n' &&
          ctx->rbuf[size - 4] == '\r') {
        // printf("------------------------------\n%s\n", ctx->rbuf.c_str());
        // response.
        ctx->wpos = 0;
        ctx->wbuf.clear();
        ctx->wbuf.append("HTTP/1.1 200 OK\r\n");
        ctx->wbuf.append("Server: itachi::HttpSinker\r\n");
        ctx->wbuf.append("Content-Length: 32\r\n");
        ctx->wbuf.append("\r\n");
        ctx->wbuf.append("********************************");
        ctx->initNetWriteEvent(AA_WRITE, ctx->fd());
        itachi->launch(ctx);
      } else {
        ctx->initNetReadEvent(AA_READ, ctx->fd());
        itachi->launch(ctx);
      }

    } else if(ctx->action == AA_WRITE) {
      SPERM_TRACE("ready to write...");
      const char* data = ctx->wbuf.data();
      int dsize = ctx->wbuf.size();
      int size = write(ctx->fd(), data + ctx->wpos, dsize - ctx->wpos);
      ctx->wpos += size;
      SPERM_TRACE("written %d, whole %d", size, dsize);
      if(ctx->wpos >= dsize) {
        // over. close conn.
        // TODO(dirlt): when I test with ab, I have to call ctx->release() and return immediately
        // otherwise ab will blocks. I really don't know how ab handle it. but httperf works.
        ctx->initNetReadEvent(AA_READ, ctx->fd());
        itachi->launch(ctx);
      } else {
        ctx->initNetWriteEvent(AA_WRITE, ctx->fd());
        itachi->launch(ctx);
      }
    }
  }
};  // class HttpSinker

static volatile bool exit_ = false;
void sig_handler(int /*signo*/) {
  exit_ = true;
}

int main() {
  Itachi itachi;
  signal(SIGINT, sig_handler);
  SPERM_TRACE("itachi.start...");
  itachi.start(4, 4); // specify threads.
  HttpSinker sinker(&itachi);
  sinker.start();
  while(1) {
    if(exit_) {
      itachi.stop();
      break;
    }
    sleep_ms(1000);
  };
  return 0;
}
