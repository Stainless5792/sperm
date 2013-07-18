/*
 * Copyright (C) dirlt
 */

#include <cassert>
#include "common/proto_serializer.h"
#include "sample.pb.h"
using namespace common;
using namespace sample;

void case1() {
  WriteableByteArray out_bytes;
  Session session;
  session.set_user("dirlt");
  session.set_passwd("123456");
  assert(proto_serializer::SerializeToByteArray(&session, &out_bytes));
  ByteSize size = 0;
  const Byte* data = out_bytes.data(&size);
  ReadableByteArray in_bytes(data, size);
  session.Clear();
  assert(proto_serializer::DeserizlizeFromByteArray(&session, &in_bytes));
  assert(session.user() == "dirlt");
  assert(session.passwd() == "123456");
}

int main() {
  case1();
  return 0;
}
