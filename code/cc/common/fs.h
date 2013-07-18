/* coding:utf-8
 * Copyright (C) dirlt
 */

#ifndef __SPERM_CC_COMMON_FS_H__
#define __SPERM_CC_COMMON_FS_H__

#include <sys/types.h>
#include <dirent.h>
#include <vector>
#include <string>
#include <cstring>
#include "common/logger_inl.h"

namespace common {

static inline int list_directory(const char* path,
                                 std::vector< std::string >* files) {
  DIR* dir = opendir(path);
  if(dir == NULL) {
    SPERM_WARNING("opendir(%s) failed(%s)", path, SERRNO);
    return -1;
  }
  struct dirent* ent = NULL;
  files->clear();
  while(1) {
    ent = readdir(dir);
    if(ent == NULL) {
      break;
    }
    if(strcmp(ent->d_name, ".") == 0 ||
        strcmp(ent->d_name, "..") == 0) {
      continue;
    }
    files->push_back(ent->d_name);
  }
  closedir(dir);
  return 0;
}

}; // namespace common


#endif // __SPERM_CC_COMMON_FS_H__
