/*
 * Copyright (C) dirlt
 */

#ifndef __SPERM_CC_COMMON_PROTO_SERIAZLIZER_H__
#define __SPERM_CC_COMMON_PROTO_SERIAZLIZER_H__

#include <google/protobuf/io/coded_stream.h>
#include "common/serializer.h"
#include "common/logger_inl.h"

namespace common {
namespace proto_serializer {

// format | uint64 | blob |
// ------------------------------------------------------------
// wrapper protobuf serialize and deserialize method.

template <class T>
bool SerializeToByteArray(const T* obj, WriteableByteArray* bytes) {
  if(!bytes) {
    SPERM_WARNING("bytes==0");
    return false;
  }
  size_t size = obj->ByteSize();
  if(!serializer::serialize(static_cast<uint64_t>(size), bytes)) {
    SPERM_WARNING("serialize(%zu, %p) failed", size, bytes);
    return false;
  }
  Byte* buf = bytes->allocate(size);
  if(!buf) {
    SPERM_WARNING("allocate(%zu) failed", size);
    return false;
  }
  if(!obj->SerializeToArray(buf, size)) {
    SPERM_WARNING("SerializeToArray(%p,%zu) failed", buf, size);
    return false;
  }
  return true;
}

template <class T>
bool DeserizlizeFromByteArray(T* obj, ReadableByteArray* bytes) {
  if(!bytes) {
    SPERM_WARNING("bytes==0");
    return false;
  }
  uint64_t _size = 0;
  if(!serializer::deserialize(&_size, bytes)) {
    SPERM_WARNING("deserialize(%zu, %p) failed", static_cast<size_t>(_size), bytes);
    return false;
  }
  size_t size = _size;
  const Byte* data = bytes->remain(NULL);
  google::protobuf::io::CodedInputStream input(data, size);
  if(!obj->ParseFromCodedStream(&input)) {
    SPERM_WARNING("ParseFromCodedStream(%p,%zu) failed", data, size);
    return false;
  }
  bytes->read(NULL, size);
  return true;
}

} // namespace proto_serializer
} // namespace common

#endif // __SPERM_CC_COMMON_PROTO_SERIAZLIZER_H__

