/* coding:utf-8
 * Copyright (C) dirlt
 */

#ifndef __SPERM_CC_COMMON_PROTO_DYNAMIC_H__
#define __SPERM_CC_COMMON_PROTO_DYNAMIC_H__

#include <iostream>
#include <google/protobuf/descriptor.h>
#include <google/protobuf/compiler/importer.h>
#include <google/protobuf/dynamic_message.h>

namespace common {

// ------------------------------------------------------------
// simplify usage of dynamic message in protobuf.
class ProtoDynamic:
  public google::protobuf::compiler::MultiFileErrorCollector {
public:
  ProtoDynamic(): importer_(&tree_, this) {
    tree_.MapPath("/", "/"); // absolute path.
    tree_.MapPath("", "."); // default starts with current directory.
  }
  void addMapPath(const char* vpath, const char* path) {
    tree_.MapPath(vpath, path);
  }
  virtual ~ProtoDynamic() {}
  struct ErrorMessage {
    std::string filename;
    int line;
    int column;
    std::string message;
  };
  std::vector< ErrorMessage > error_msg;
  virtual void AddError(const std::string& filename,
                        int line, int column,
                        const std::string& message) {
    ErrorMessage msg;
    msg.filename = filename;
    msg.line = line;
    msg.column = column;
    msg.message = message;
    error_msg.push_back(msg);
  }
  const google::protobuf::FileDescriptor* import(const std::string& filename) {
    return importer_.Import(filename);
  }
  static const google::protobuf::Descriptor* findMessageTypeByName(
    const google::protobuf::FileDescriptor* fd,
    const std::string& name) {
    return fd->pool()->FindMessageTypeByName(name);
  }
  static google::protobuf::MessageFactory* newMessageFactory(
    const google::protobuf::FileDescriptor* fd) {
    return new google::protobuf::DynamicMessageFactory(fd->pool());
  }
  static void deleteMessageFactory(google::protobuf::MessageFactory* factory) {
    delete factory;
  }
private:
  google::protobuf::compiler::DiskSourceTree tree_;
  google::protobuf::compiler::Importer importer_;
}; // class ProtoDynamic

} // namespace common

#endif // __SPERM_CC_COMMON_PROTO_DYNAMIC_H__
