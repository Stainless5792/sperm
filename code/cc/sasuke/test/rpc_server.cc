/* coding:utf-8
 * Copyright (C) dirlt
 */

#include <signal.h>
#include "sasuke/rpc.h"
#include "sasuke/proto_rpc.h"
#include "echo.pb.h"

using namespace sample;
using namespace sasuke;

class EchoService:
  public proto_rpc::Service< Echo, Echo > {
public:
  void handleRequest(const Echo* req, Echo* res) {
    res->set_content(req->content());
  }
}; // class EchoService

static volatile bool exit_ = false;
static void sig_handler(int signo) {
  SPERM_TRACE("caught signal %d\n",signo);
  if(signo == SIGINT) {
    SPERM_DEBUG("siganl == SIGINT");
    exit_ = true;
  }
}

//#define ip "tcp://10.26.140.39:19870"
#define ip "tcp://127.0.0.1:19870"

int main() {
  static const int server_threads = 1;
  rpc::Context::init();
  rpc::Server conn;
  signal(SIGINT, sig_handler);
  rpc::ServiceCallBack call_back;
  call_back.registerService("echo", new EchoService());
  conn.set_call_back(&call_back);
  SPERM_TRACE("serve %s", ip);
  if(conn.run(ip, server_threads) < 0) {
    return -1;
  }
  while(1){
    if(exit_){
      SPERM_TRACE("exit...");
      conn.stop();
      break;
    }
    sleep_ms(1000);
  }
  return 0;
}
