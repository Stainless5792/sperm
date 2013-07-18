/*
 * Copyright (C) dirlt
 */

#include <sys/time.h>
#include "common/utils.h"
#include "sasuke/rpc.h"
#include "sasuke/proto_rpc.h"
#include "echo.pb.h"

using namespace sample;
using namespace sasuke;
using namespace common;

//#define ip "tcp://10.26.140.39:19870"
#define ip "tcp://127.0.0.1:19870"

int main() {
  static const int kCounterUnit = 10000;
  static const char* kContent = "love way you live(rihana feat. eminem).";
  rpc::Context::init();
  rpc::Client conn;
  if(conn.connect(ip) < 0) {
    SPERM_WARNING("connect %s failed", ip);
    return -1;
  }
  size_t counter = 0;
  double _whole_times = 0.0;
  double _period_start = 0.0;
  _period_start = gettime_ms();
  while(1) {
    proto_rpc::Request<Echo> req;
    proto_rpc::Response<Echo> res;
    req.set_method("echo");
    req.set_content(kContent);
    // no timeout.
    if(conn.call(&req, &res, 10000) < 0) {
      SPERM_WARNING("call failed, reconnect...");
      conn.reconnect();
      continue;
    }
    counter++;
    if((counter % kCounterUnit) == 0) {
      double _period_end = gettime_ms();
      _whole_times += _period_end - _period_start;
      SPERM_TRACE("last %6d calls cost %.3lf ms, average %.3lf ms, whole %8zu calls cost %.3lf, average %.3lf ms",
                   kCounterUnit, (_period_end - _period_start), (_period_end - _period_start) / kCounterUnit,
                   counter, _whole_times, _whole_times / counter);
      _period_start = _period_end;
    }
  } // while(1)
  conn.close();
  return 0;
}

