/* coding:utf-8
 * Copyright (C) dirlt
 */

#include <cassert>
#include "common/proto_dynamic.h"
#include "sample.pb.h"

using namespace google::protobuf;
using namespace google::protobuf::compiler;
using namespace common;
using namespace sample;

int main() {
  std::string data;
  {
    // create message dynamicly.
    ProtoDynamic dynamic;
    const FileDescriptor* fd = dynamic.import("sample.proto");
    assert(fd);
    const Descriptor* desc = dynamic.findMessageTypeByName(fd, "sample.Session");
    assert(desc);
    MessageFactory* factory = dynamic.newMessageFactory(fd);
    Message* msg = factory->GetPrototype(desc)->New();
    const Reflection* reflection = msg->GetReflection();
    assert(reflection);

    reflection->SetString(msg, desc->FindFieldByName("user"), "dirlt");
    reflection->SetString(msg, desc->FindFieldByName("passwd"), "fuck");
    msg->SerializeToString(&data);
  }
  {
    Session session;
    session.ParseFromString(data);
    assert(strcmp(session.user().c_str(), "dirlt") == 0);
    assert(strcmp(session.passwd().c_str(), "fuck") == 0);
  }
  return 0;
}
