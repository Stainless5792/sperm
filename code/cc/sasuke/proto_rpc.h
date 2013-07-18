/*
 * Copyright (C) dirlt
 */

#ifndef __SPERM_CC_ZMQ_PROTO_RPC_H__
#define __SPERM_CC_ZMQ_PROTO_RPC_H__

#include "common/logger_inl.h"
#include "common/proto_serializer.h"
#include "sasuke/rpc.h"

namespace sasuke {
namespace proto_rpc {

// ------------------------------------------------------------
// T is a protobuf object. wrapper it to be a Request.
template <class T>
class Request : public rpc::Request, public T {
public:
  virtual ~Request() {
  }
  virtual bool serialize(common::WriteableByteArray* bytes) const;
  virtual bool deserialize(common::ReadableByteArray* bytes);
}; // class Request

template <class T>
bool Request<T>::serialize(common::WriteableByteArray* bytes) const {
  return common::proto_serializer::SerializeToByteArray(this, bytes);
}

template <class T>
bool Request<T>::deserialize(common::ReadableByteArray* bytes) {
  return common::proto_serializer::DeserizlizeFromByteArray(this, bytes);
}

// ------------------------------------------------------------
// T is a protobuf object. wrapper it to be a Response.
template <class T>
class Response : public T, public rpc::Response {
public:
  virtual ~Response() {
  }
  virtual bool serialize(common::WriteableByteArray* bytes) const;
  virtual bool deserialize(common::ReadableByteArray* bytes);
}; // class Response

template <class T>
bool Response<T>::serialize(common::WriteableByteArray* bytes) const {
  return common::proto_serializer::SerializeToByteArray(this, bytes);
}

template <class T>
bool Response<T>::deserialize(common::ReadableByteArray* bytes) {
  return common::proto_serializer::DeserizlizeFromByteArray(this, bytes);
}

// ------------------------------------------------------------
// REQUEST and RESPONSE are protobuf objects.
template <class REQUEST, class RESPONSE>
class Service : public rpc::Service {
public:
  virtual rpc::Request* allocateRequest();
  virtual rpc::Response* allocateResponse();
  virtual void run(const rpc::Request* request, rpc::Response* response);
  virtual void handleRequest(const REQUEST* req, RESPONSE* res) = 0;
}; // class Service.

template <class REQUEST, class RESPONSE>
rpc::Request* Service<REQUEST, RESPONSE>::allocateRequest() {
  return new Request<REQUEST>();
}

template <class REQUEST, class RESPONSE>
rpc::Response* Service<REQUEST, RESPONSE>::allocateResponse() {
  return new Response<RESPONSE>();
}

template <class REQUEST, class RESPONSE>
void Service<REQUEST, RESPONSE>::run(const rpc::Request* request, rpc::Response* response) {
  const Request<REQUEST>* req = dynamic_cast<const Request<REQUEST>*>(request);
  Response<RESPONSE>* res = dynamic_cast<Response<RESPONSE>*>(response);
  handleRequest(req, res);
}

} // namespace proto_rpc
} // namespace sasuke

#endif // __SPERM_CC_SASUKE_PROTO_RPC_H__
