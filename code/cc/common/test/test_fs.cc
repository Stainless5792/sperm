/* coding:utf-8
 * Copyright (C) dirlt
 */

#include "common/fs.h"
using namespace common;

int main() {
  std::vector< std::string >files;
  list_directory(".", &files);
  printf("--------------------(%s)--------------------\n", ".");
  for(size_t i = 0; i < files.size(); i++) {
    printf("|---%s\n", files[i].c_str());
  }
  return 0;
}
