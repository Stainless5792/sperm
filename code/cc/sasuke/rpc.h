/*
 * Copyright (C) dirlt
 */

#ifndef __SPERM_CC_SASUKE_RPC_H__
#define __SPERM_CC_SASUKE_RPC_H__

#include <string>
#include <vector>
#include <map>
#include <zmq.h>
#include "common/byte_array.h"
#include "common/serializer.h"

namespace sasuke {
namespace rpc {

// ------------------------------------------------------------
// RPC Context Singleton Pattern.
// !!!singleton is thread unsafe.
// app better to call 'init' at first.
class Context {
public:
  static void init(int io_threads = kDefaultIOThreads);
  static Context* instance();
  void* context() const {
    return zmq_context_;
  }
  uint32_t increaseConnectionNumber();
private:
  Context(int io_threads);
  ~Context();
  static const int kDefaultIOThreads = 4;
  static Context* instance_;
  volatile uint32_t connection_number_;
  void* zmq_context_;
}; // class Context

// ------------------------------------------------------------
// RPC Request and Response Interface.
class Request: public common::serializer::Serializable {
public:
  const std::string& method() const;
  void set_method(const std::string& method);
  Request();
  // here 'virtual' is important',otherwise there is a mem leak
  virtual ~Request() {}
protected:
  std::string method_;
};

class Response: public common::serializer::Serializable {
public:
  Response() {}
  // here 'virtual' is important',otherwise there is a mem leak
  virtual ~Response() {}
};

// ------------------------------------------------------------
// RPC Service. service register them.
class Service {
public:
  virtual ~Service() {}
  virtual Request* allocateRequest() = 0;
  virtual Response* allocateResponse() = 0;
  // default deallocate action is just delete it.
  virtual void deallocateRequest(Request* request) {
    delete request;
  }
  virtual void deallocateResponse(Response* response) {
    delete response;
  }
  virtual void run(const Request* request, Response* response) = 0;
  const std::string& service_name() const {
    return srv_name_;
  }
  void set_service_name(const std::string& srv_name) {
    srv_name_ = srv_name;
  }
public:
  std::string srv_name_;
};

// ------------------------------------------------------------
// RPC Client.
class Client {
public:
  // if Client is lock_step, it must repeats the pattern that 'send-recv'
  Client(bool lock_step = true);
  ~Client();
  // we don't guarantee the server already started
  // if you try to ensure that, you should guarantee on app level.
  // by call a dummy method or something else.
  int connect(const char* uri);
  int reconnect();
  void close();
  // bytes level operation.
  int send(common::ReadableByteArray* bytes);
  int recv(common::WriteableByteArray* bytes, int timeout_ms);
  // object level operation.
  int send(const Request* request);
  int recv(Response* response, int timeout_ms);
  int call(const Request* request, Response* response, int timeout_ms);
  // book the serve and connect uri
  // so if connection is broken, user can connect again.
  // but there is a fancy interface 'reconnect' for that.
  // so in most case user don't need this method.
  const char* uri() const {
    return uri_.c_str();
  }
  // ----------------------------------------
  // advanced usage. much more efficient.
  // send 'data/size' with pointer, call 'free' to deallocate it after.
  int sendPtrInternalFree(void* data, size_t size);
  class Message {
  public:
    Message();
    ~Message();
    void* data(size_t* size);
    void free();
    friend class Client;
  private:
    zmq_msg_t msg_;
  };
  // recv 'Message' and get 'data/size', user can call 'free' to free it manually
  // or let scope call ~Message to free it automatically.
  int recvMessage(Message* message, int timeout_ms);
private:
  // recv message and store it to 'msg'
  int recvInternalMessage(zmq_msg_t* msg, int timeous_ms);
  void* zmq_main_socket_;
  std::string uri_;
  bool lock_step_;
}; // class Client

// ------------------------------------------------------------
// RPC Server.
void device(void* front, void* end, int timeout_ms, volatile bool* stop);
class CallBack {
public:
  virtual ~CallBack() {}
  virtual int handle(common::ReadableByteArray* in_bytes,
                     common::WriteableByteArray* out_bytes) = 0;
};

class ServiceCallBack : public CallBack {
public:
  ServiceCallBack() {}
  virtual ~ServiceCallBack() {}
  void registerService(const std::string& method, Service* service) {
    dispatcher_[method] = service;
  }
  virtual int handle(common::ReadableByteArray* in_bytes,
                     common::WriteableByteArray* out_bytes);
private:
  typedef std::map< std::string, Service* > Dispatcher;
  Dispatcher dispatcher_;
};

class Server {
public:
  Server();
  ~Server();
  void close();
  const char* uri() const {
    return uri_.c_str();
  }
  void set_call_back(CallBack* call_back) {
    call_back_ = call_back;
  }
  int run(const char* uri, int callback_threads);
  void stop();
  void* rpc_thread_function(); // use internally.
  void* device_thread_function(); // use internally.
private:
  static const int kserverPollTimeoutMillSeconds = 500;
  void* zmq_main_socket_;
  void* zmq_back_socket_;
  CallBack* call_back_;
  std::string uri_;
  char inproc_uri_[128];
  pthread_t* tids_;
  int callback_threads_;
  pthread_t devide_tid_;
  volatile bool stop_;
}; // class Server

} // namespace rpc
} // namespace sasuke

#endif // __SPERM_CC_SASUKE_RPC_H__
