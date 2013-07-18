/*
 * Copyright (C) dirlt
 */

#include <pthread.h>
#include <cassert>
#include <cstdio>
#include "common/utils.h"

int main() {
  pid_t tid = get_tid();
  printf("%zu\n", static_cast<size_t>(tid));
  return 0;
}
