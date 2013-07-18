/* coding:utf-8
 * Copyright (C) dirlt
 */

#include <cstdio>
#include "common/logger_inl.h"
#include "common/zkclient.h"

using namespace common::zookeeper;

int main() {
  Client client("127.0.0.1:19871");
  int code = client.connect();
  if(code != 0) {
    SPERM_WARNING("connect failed(%s)", Client::code_to_string(code));
    return -1;
  }
  const char* path = "/dirtysalt/sperm";
  const char* node = "/dirtysalt/sperm/node";
  client.removeTree(path);
  // --------------------
  // createTree
  code = client.createTree(path);
  if(code != 0) {
    SPERM_WARNING("createTree(%s) failed(%s)", path, Client::code_to_string(code));
    return -1;
  }
  SPERM_DEBUG("createTree(%s) OK", path);
  // --------------------
  // write
  code = client.write(path, "hello");
  if(code != 0) {
    SPERM_WARNING("write(%s) failed(%s)", path, Client::code_to_string(code));
    return -1;
  }
  SPERM_DEBUG("write(%s) OK", path);
  // --------------------
  std::string data;
  code = client.read(path, &data);
  if(code != 0) {
    SPERM_WARNING("read(%s) failed(%s)", path, Client::code_to_string(code));
    return -1;
  }
  SPERM_DEBUG("read(%s) OK", path);
  // --------------------
  // create some temporary nodes.
  for(int i = 0; i < 10; i++) {
    std::string vpath;
    code = client.createNode(node, true, &vpath);
    if(code != 0) {
      SPERM_WARNING("createNode(%s,temporary) failed(%s)", node
                    , Client::code_to_string(code));
      return -1;
    }
    SPERM_DEBUG("create temporary node %s", vpath.c_str());
  }
  // --------------------
  std::vector< std::string > children;
  code = client.readChildren(path, &children);
  if(code != 0) {
    SPERM_WARNING("readChildren(%s) failed(%s)", path, Client::code_to_string(code));
    return -1;
  }
  printf("------------------------------------------------------------\n");
  printf("%s\n", path);
  for(size_t i = 0; i < children.size(); i++) {
    printf("|--- %s\n", children[i].c_str());
  }
  printf("\n");
  SPERM_DEBUG("readChildren(%s) OK", path);
  // --------------------
  code = client.removeTree(path);
  if(code != 0) {
    SPERM_WARNING("removeTree(%s) failed(%s)", path, Client::code_to_string(code));
    return -1;
  }
  SPERM_DEBUG("removeTree(%s) OK", path);
  return 0;
}
