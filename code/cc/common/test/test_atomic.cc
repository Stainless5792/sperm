/*
 * Copyright (C) dirlt
 */

#include <pthread.h>
#include <cassert>
#include <cstdio>
#include "common/atomic.h"
#include "common/utils.h"
#include "common/logger_inl.h"
#include "common/lock.h"

using namespace common;

class A: public RefCount {

};

static volatile bool flag;
static volatile int32_t counter;
void* thr_func(void*) {
  while(flag) {
    AtomicInc(counter);
    AtomicDec(counter);
  }
  return NULL;
}

int main() {
  A* a = new A;
  assert(a->getRefCount() == 1);
  assert(a->acquire() == 2);
  assert(a->release() == 1);
  assert(a->release() == 0);

  // ------------------------------------------------------------
  {
    pthread_t tid[2];
    counter = 0;
    flag = true;
    pthread_create(tid, NULL, thr_func, NULL);
    pthread_create(tid + 1, NULL, thr_func, NULL);
    sleep_ms(2000);
    flag = false;
    pthread_join(tid[0], NULL);
    pthread_join(tid[1], NULL);
    SPERM_TRACE("counter=%d(expected 0)", counter);
  }
  return 0;
}
