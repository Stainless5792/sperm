/*
 * Copyright (C) dirlt
 */

#include <signal.h>
#include "common/atomic.h"
#include "common/serializer.h"
#include "common/logger_inl.h"
#include "sasuke/rpc.h"

#define ZSERRNO (zmq_strerror(errno))
#define ZSERRNO2(n) (zmq_strerror(n))

namespace sasuke {
namespace rpc {

using namespace common;
// zmq_msg_t operation is very brittle, especially zmq_msg_init_data
// is easy to confuse user and cause memory leak or double free.
// and I just try it add as many comments as I can when I operate zmq_msg_t.

// ------------------------------------------------------------
// Context Implementation
Context* Context::instance_ = NULL;

static void ignore_sigpipe(int signo) {
  if(signo == SIGPIPE) {
    SPERM_DEBUG("sigpipe arrived");
  }
}

void Context::init(int io_threads) {
  if(instance_ == NULL) {
    instance_ = new Context(io_threads);
    // always remember to install SIGPIPE handler.
    if(signal(SIGPIPE, ignore_sigpipe) == SIG_ERR) {
      SPERM_FATAL("signal(SIGPIPE) failed(%s)", SERRNO);
    }
  }
}

Context* Context::instance() {
  if(instance_ == NULL) {
    instance_ = new Context(kDefaultIOThreads);
  }
  return instance_;
}

Context::Context(int io_threads) {
  connection_number_ = 0;
  zmq_context_ = zmq_init(io_threads);
  if(zmq_context_ == NULL) {
    SPERM_FATAL("zmq_init(%d) failed(%s)", io_threads, ZSERRNO);
  }
}

Context::~Context() {
again:
  int ret = zmq_term(zmq_context_);
  if(ret < 0) {
    if(ret == EINTR) {
      goto again;
    }
    SPERM_WARNING("zmq_term(%p) failed(%s)", zmq_context_, ZSERRNO);
  }
}

uint32_t Context::increaseConnectionNumber() {
  return AtomicInc(connection_number_);
}

// ------------------------------------------------------------
// RPC Request and Response Implementation
Request::Request(): method_("null") {
}

const std::string& Request::method() const {
  return method_;
}

void Request::set_method(const std::string& method) {
  method_ = method;
}

// ------------------------------------------------------------
// wrapper of zmq
static inline int _zmq_recv(void* socket, zmq_msg_t* msg, int flags) {
again:
  int ret = zmq_recv(socket, msg, flags);
  if(ret < 0) {
    if(errno == EINTR) {
      goto again;
    }
    SPERM_WARNING("zmq_recv(%p,%p,%d) failed(%s)", socket, &msg, flags, ZSERRNO);
  }
  return ret;
}

static inline int _zmq_send(void* socket, zmq_msg_t* msg, int flags) {
again:
  int ret = zmq_send(socket, msg, flags);
  if(ret < 0) {
    if(errno == EINTR) {
      goto again;
    }
    SPERM_WARNING("zmq_send(%p,%p,%d) failed(%s)", socket, &msg, flags, ZSERRNO);
  }
  return ret;
}

static inline int _zmq_poll(zmq_pollitem_t* items, int nitems, int timeout_ms) {
again:
  int ret = zmq_poll(items, nitems, timeout_ms * 1000); // timeout unit is microseconds.
  if(ret < 0) {
    if(errno == EINTR) {
      goto again;
    }
    SPERM_WARNING("zmq_poll(%p,%d,%dms) failed(%s)", items, nitems, timeout_ms, ZSERRNO);
    goto again;
  }
  return ret;
}

static inline int _zmq_getsockopt(void* socket, int option_name, void* option_value, size_t* option_len) {
again:
  int ret = zmq_getsockopt(socket, option_name, option_value, option_len);
  if(ret < 0) {
    if(errno == EINTR) {
      goto again;
    }
    SPERM_WARNING("zmq_getsockopt(%p,%d,%p,%p) faild(%s)", socket, option_name, option_value, option_len, ZSERRNO);
  }
  return ret;
}

// copy the 'data/size' to 'msg'
static inline int _zmq_put_msg(zmq_msg_t* msg, const void* data, size_t size) {
  zmq_msg_close(msg); // discard old content.
  int ret = zmq_msg_init_size(msg, size);
  if(ret < 0) {
    SPERM_WARNING("zmq_msg_init_size(%zu) failed(%s)", size, ZSERRNO);
  } else {
    memcpy(zmq_msg_data(msg), data, size);
  }
  return ret;
}

void internal_free(void* data, void* /*hint*/) {
  // just free it.
  free(data);
}

// put ptr of 'data/size' to 'msg', and when unneed call the 'internal_free' to deallocate 'data/size'
static inline int _zmq_put_msg_ptr_internal_free(zmq_msg_t* msg, void* data, size_t size) {
  zmq_msg_close(msg); // discard old content.
  // always OK.
  zmq_msg_init_data(msg, data, size, internal_free, NULL);
  return 0;
}

static inline void* _zmq_socket(void* context, int type) {
  void* socket = zmq_socket(context, type);
  if(socket == NULL) {
    SPERM_WARNING("zmq_socket(%p,%d) failed(%s)", context, type, ZSERRNO);
  }
  return socket;
}

static inline int _zmq_connect(void* socket, const char* uri) {
  int ret = zmq_connect(socket, uri);
  if(ret < 0) {
    SPERM_WARNING("zmq_connect(%p,%s) failed(%s)", socket, uri, ZSERRNO);
  }
  return ret;
}

static inline int _zmq_bind(void* socket, const char* uri) {
  int ret = zmq_bind(socket, uri);
  if(ret < 0) {
    SPERM_WARNING("zmq_bind(%p,%s) failed(%s)", socket, uri, ZSERRNO);
  }
  return ret;
}

static inline int _zmq_close(void* socket) {
  int ret = zmq_close(socket);
  if(ret < 0) {
    SPERM_WARNING("zmq_close(%p) faild(%s)", socket, ZSERRNO);
  }
  return ret;
}

// ------------------------------------------------------------
// wrapper of serialization and deserilization
// format | method | blob |
static inline bool serialize_request(const Request* request,
                                     WriteableByteArray* bytes) {
  // put method at first.
  if(!serializer::serialize(request->method(), bytes)) {
    return false;
  }
  //SPERM_DEBUG("serialize method(%s)", request->method().c_str());
  // put object then.
  if(!request->serialize(bytes)) {
    return false;
  }
  //SPERM_DEBUG("serialize request OK");
  return true;
}

static inline bool deserialize_method(std::string* method,
                                      ReadableByteArray* bytes) {
  // fetch method at first.
  if(!serializer::deserialize(method, bytes)) {
    return false;
  }
  //SPERM_DEBUG("deserialize method(%s) OK", method->c_str());
  return true;
}

// format | return | blob |
static inline bool serialize_response(const Response* response,
                                      WriteableByteArray* bytes) {
  // put object then.
  if(!response->serialize(bytes)) {
    return false;
  }
  //SPERM_DEBUG("serialize response OK");
  return true;
}

static inline bool deserialize_response(Response* response,
                                        ReadableByteArray* bytes) {
  // get object then.
  if(!response->deserialize(bytes)) {
    return false;
  }
  //SPERM_DEBUG("deserialize response OK");
  return true;
}

// ------------------------------------------------------------
// Client Implementation.
Client::Client(bool lock_step):
  zmq_main_socket_(NULL),
  lock_step_(lock_step) {
}

Client::~Client() {
  close();
}

int Client::connect(const char* uri) {
  uri_ = uri;
  // bind the main socket to send request
  zmq_main_socket_ = _zmq_socket(Context::instance()->context(),
                                 lock_step_ ? ZMQ_REQ : ZMQ_DEALER);
  if(zmq_main_socket_ == NULL) {
    goto fail;
  }
  if(_zmq_connect(zmq_main_socket_, uri) < 0) {
    goto fail;
  }
  return 0;
fail:
  close();
  return -1;
}

int Client::reconnect() {
  close();
  return connect(uri_.c_str());
}

void Client::close() {
  if(zmq_main_socket_) {
    _zmq_close(zmq_main_socket_);
    zmq_main_socket_ = NULL;
  }
}

int Client::sendPtrInternalFree(void* data, size_t size) {
  zmq_msg_t msg;
  // always OK. so we can close anyway.
  _zmq_put_msg_ptr_internal_free(&msg, data, size);
  //SPERM_DEBUG("client send data size:%zu", size);
  // since user don't know when to free the data.
  // so if we fail right here, we have to help user to free it.
  // otherwise user are in the fucking embrassment.
  if(_zmq_send(zmq_main_socket_, &msg, 0) < 0) {
    zmq_msg_close(&msg);
    return -1;
  }
  return 0;
}

// --------------------
// Client Message Implementation
Client::Message::Message() {
  zmq_msg_init(&msg_);
}

Client::Message::~Message() {
  free();
}

void Client::Message::free() {
  zmq_msg_close(&msg_);
}

void* Client::Message::data(size_t* size) {
  if(size) {
    *size = zmq_msg_size(&msg_);
  }
  return zmq_msg_data(&msg_);
}

int Client::recvMessage(Message* message, int timeout_ms) {
  return recvInternalMessage(&(message->msg_), timeout_ms);
}

int Client::recvInternalMessage(zmq_msg_t* msg, int timeout_ms) {
  zmq_pollitem_t items[1];
  items[0].socket = zmq_main_socket_;
  items[0].fd = -1;
  items[0].events = ZMQ_POLLIN;

  int ret = _zmq_poll(items, 1, timeout_ms);
  if(ret < 0) {
    return -1;
  } else if(ret == 0) {
    SPERM_WARNING("timeout(%dms)", timeout_ms);
    return -1;
  }
  if(_zmq_recv(zmq_main_socket_, msg, 0) < 0) {
    return -1;
  }
  return 0;
}

int Client::send(ReadableByteArray* bytes) {
  zmq_msg_t msg;
  zmq_msg_init(&msg);
  ByteSize size = 0;
  const Byte* data = bytes->remain(&size);
  //SPERM_DEBUG("client send data size:%zu", size);
  if(_zmq_put_msg(&msg, data, size) < 0) {
    zmq_msg_close(&msg);
    return -1;
  }
  if(_zmq_send(zmq_main_socket_, &msg, 0) < 0) {
    zmq_msg_close(&msg);
    return -1;
  }
  zmq_msg_close(&msg);
  return 0;
}

int Client::recv(WriteableByteArray* bytes, int timeout_ms) {
  zmq_msg_t msg;
  zmq_msg_init(&msg);
  if(recvInternalMessage(&msg, timeout_ms) < 0) {
    zmq_msg_close(&msg);
    return -1;
  }
  const void* data = zmq_msg_data(&msg);
  size_t size = zmq_msg_size(&msg);
  //SPERM_DEBUG("client recv data size:%zu", size);
  Byte* dst = bytes->allocate(size);
  if(dst == NULL) {
    SPERM_WARNING("allocate(%zu) failed", size);
    zmq_msg_close(&msg);
    return -1;
  }
  memcpy(dst, data, size);
  zmq_msg_close(&msg);
  return 0;
}


int Client::send(const Request* request) {
  WriteableByteArray send_bytes(true);

  if(!serialize_request(request, &send_bytes)) {
    SPERM_WARNING("serialize_request(%p,%p) failed", request, &send_bytes);
    return -1;
  }

  ByteSize size;
  const Byte* data = send_bytes.data(&size);
  if(sendPtrInternalFree(const_cast<Byte*>(data), size) < 0) {
    return -1;
  }
  return 0;
}

int Client::recv(Response* response, int timeout_ms) {
  zmq_msg_t msg;
  zmq_msg_init(&msg);

  if(recvInternalMessage(&msg, timeout_ms) < 0) {
    zmq_msg_close(&msg);
    return -1;
  }

  ReadableByteArray _recv_bytes(reinterpret_cast<const Byte*>(zmq_msg_data(&msg)),
                                zmq_msg_size(&msg));
  if(!deserialize_response(response, &_recv_bytes)) {
    SPERM_WARNING("deserialize_response(%p,%p) failed", response, &_recv_bytes);
    zmq_msg_close(&msg);
    return -1;
  }

  zmq_msg_close(&msg);
  return 0;
}

int Client::call(const Request* request, Response* response, int timeout_ms) {
  if(send(request) < 0) {
    return -1;
  }
  if(recv(response, timeout_ms) < 0) {
    return -1;
  }
  return 0;
}

// ------------------------------------------------------------
// Server Implementation.
Server::Server():
  zmq_main_socket_(NULL),
  zmq_back_socket_(NULL),
  call_back_(NULL),
  tids_(NULL),
  stop_(true) {
}

Server::~Server() {
  close();
}

void Server::close() {
  if(zmq_main_socket_) {
    zmq_close(zmq_main_socket_);
    zmq_main_socket_ = NULL;
  }
  if(zmq_back_socket_) {
    zmq_close(zmq_back_socket_);
    zmq_back_socket_ = NULL;
  }
}
// ------------------------------------------------------------
// thread and dispatch function.
int ServiceCallBack::handle(ReadableByteArray* in_bytes,
                            WriteableByteArray* out_bytes) {
  std::string method;
  int ret = -1;
  Request* request = NULL;
  Response* response = NULL;
  Service* service = NULL;
  Dispatcher::const_iterator it;

  // deserialize method.
  if(!deserialize_method(&method, in_bytes)) {
    SPERM_WARNING("deserialize_method(%p,%p) failed", &method, in_bytes);
    goto fail;
  }
  it = dispatcher_.find(method);
  if(it == dispatcher_.end()) {
    SPERM_WARNING("method(%s) not found", method.c_str());
    goto fail;
  }

  //SPERM_DEBUG("dispatch function with method(%s)", method.c_str());
  service = it->second;

  request = service->allocateRequest();
  if(!request->deserialize(in_bytes)) {
    SPERM_WARNING("request deserialize(%p) failed", in_bytes);
    goto fail;
  }

  response = service->allocateResponse();
  service->run(request, response);
  SPERM_DEBUG("service->run(%s)", method.c_str());
  if(!serialize_response(response, out_bytes)) {
    SPERM_WARNING("response serialize(%p,%p) failed", response, out_bytes);
    goto fail;
  }
  // deallocate request and response.
  if(request) {
    service->deallocateRequest(request);
    request = NULL;
  }
  if(response) {
    service->deallocateResponse(response);
    response = NULL;
  }

  ret = 0;
fail:
  // check again.
  if(request) {
    service->deallocateRequest(request);
    request = NULL;
  }
  if(response) {
    service->deallocateResponse(response);
    response = NULL;
  }
  return ret;
}

static void* proxy_rpc_thread_function(void* arg) {
  Server* conn = static_cast<Server*>(arg);
  return conn->rpc_thread_function();
}

void* Server::rpc_thread_function() {
  void* receiver = NULL;
  int ret = 0;
  zmq_pollitem_t items[1];
  zmq_msg_t msg;
  zmq_msg_init(&msg);

  receiver = _zmq_socket(Context::instance()->context(), ZMQ_REP);
  if(receiver == NULL) {
    goto fail;
  }
  if(_zmq_connect(receiver, inproc_uri_) < 0) {
    goto fail;
  }
  SPERM_DEBUG("rpc thread function connect to '%s' OK", inproc_uri_);

  items[0].socket = receiver;
  items[0].fd = -1;
  items[0].events = ZMQ_POLLIN;

  while(1) {
    // check connection is break.
    if(stop_) {
      break;
    }
    ret = _zmq_poll(items, 1, Server::kserverPollTimeoutMillSeconds);
    if(ret < 0) {
      goto fail;
    } else if(ret == 0) { // timeout.
      continue;
    }
    //SPERM_DEBUG("rpc thread function triggered OK");

    if(_zmq_recv(receiver, &msg, 0) < 0) {
      goto fail;
    }
    //SPERM_DEBUG("rpc thread function recv message OK");

    // handle message
    ReadableByteArray recv_bytes(static_cast<const Byte*>(zmq_msg_data(&msg)),
                                 zmq_msg_size(&msg));
    WriteableByteArray send_bytes(true); // user free.
    //SPERM_DEBUG("server recv data size:%zu", zmq_msg_size(&msg));
    if(!call_back_) {
      SPERM_FATAL("call_back_==NULL");
      continue;
    }
    if(call_back_->handle(&recv_bytes, &send_bytes) < 0) {
      SPERM_WARNING("call_back_->handle(%p,%p) failed", &recv_bytes, &send_bytes);
      continue;
    }
    //SPERM_DEBUG("rpc thread function handle message OK");

    // reply.
    ByteSize data_size;
    const Byte* data = send_bytes.data(&data_size);
    //SPERM_DEBUG("server send data size:%zu", data_size);
    _zmq_put_msg_ptr_internal_free(&msg, const_cast<Byte*>(data), data_size);
    if(_zmq_send(receiver, &msg, 0) < 0) {
      zmq_msg_close(&msg); // like sendPtrInternalFree.
      goto fail;
    }
    // we have to init right here.
    // otherwise 'recv' or we call 'close'
    // will cause double free. be CAUTIOUS!!!.
    zmq_msg_init(&msg);
    //SPERM_DEBUG("rpc thread function reply message OK");
  } // while(1)

fail:
  zmq_msg_close(&msg); //
  if(receiver) {
    zmq_close(receiver);
    receiver = NULL;
  }
  return NULL;
}

static void* proxy_device_thread_function(void* arg) {
  Server* server = static_cast<Server*>(arg);
  return server->device_thread_function();
}

void* Server::device_thread_function() {
  device(zmq_main_socket_, zmq_back_socket_, kserverPollTimeoutMillSeconds, &stop_);
  return NULL;
}

int Server::run(const char* uri, int callback_threads) {
  if(!stop_) {
    SPERM_WARNING("server running");
    return -1;
  }
  if(callback_threads <= 0) {
    SPERM_FATAL("callback_threads(%d)<=0", callback_threads);
  }

  int ret = 0;
  uri_ = uri;
  callback_threads_ = callback_threads;

  // bind the main socket to recv request from client
  zmq_main_socket_ = _zmq_socket(Context::instance()->context(), ZMQ_ROUTER);
  if(zmq_main_socket_ == NULL) {
    goto fail;
  }
  if(_zmq_bind(zmq_main_socket_, uri) < 0) {
    goto fail;
  }
  SPERM_DEBUG("bind main socket to '%s' OK", uri);

  // bind the backend socket to handle the request
  zmq_back_socket_ = _zmq_socket(Context::instance()->context(), ZMQ_DEALER);
  if(zmq_back_socket_ == NULL) {
    goto fail;
  }
  snprintf(inproc_uri_, sizeof(inproc_uri_), "inproc://conn#%u",
           Context::instance()->increaseConnectionNumber());
  if(_zmq_bind(zmq_back_socket_, inproc_uri_) < 0) {
    goto fail;
  }
  SPERM_DEBUG("bind back socket to '%s' OK", inproc_uri_);

  stop_ = false;
  tids_ = new pthread_t[callback_threads];
  // allocate threads.
  for(int i = 0; i < callback_threads; i++) {
    ret = pthread_create(tids_ + i, NULL, proxy_rpc_thread_function, this);
    if(ret < 0) {
      // fatal is much easier. just abort it;
      SPERM_FATAL("pthread_create(%d) failed(%s)", i, ZSERRNO2(ret));
    }
  }
  SPERM_DEBUG("allocate threads OK");
  // allocate device thread.
  ret = pthread_create(&devide_tid_, NULL, proxy_device_thread_function, this);
  if(ret < 0) {
    SPERM_FATAL("pthread_create failed(%s)", ZSERRNO2(ret));
  }
  SPERM_DEBUG("allocate device thread OK");
  return 0;
fail:
  close();
  return -1;
}

void Server::stop() {
  stop_ = true;
  // deallocate thread.
  for(int i = 0; i < callback_threads_; i++) {
    // ignore the return value
    pthread_join(tids_[i], NULL);
  }
  SPERM_DEBUG("deallocate threads OK");
  pthread_join(devide_tid_, NULL);
  SPERM_DEBUG("deallocate device thread OK");
}

void device(void* front, void* end, int timeout_ms, volatile bool* stop) {
  int ret = 0;
  int64_t more;
  size_t moresz = sizeof(more);
  zmq_pollitem_t items[2];
  zmq_msg_t msg;
  zmq_msg_init(&msg);

  // epoll main socket and back socket. also detect stop flag.
  // works as zmq device.
  items[0].socket = front;
  items[0].fd = -1;
  items[0].events = ZMQ_POLLIN;
  items[1].socket = end;
  items[1].fd = -1;
  items[1].events = ZMQ_POLLIN;
  while(1) {
    // check connection is break.
    if(*stop) {
      break;
    }
    ret = _zmq_poll(items, 2, timeout_ms);
    if(ret < 0) {
      goto fail;
    } else if(ret == 0) {
      continue;
    }
    //SPERM_DEBUG("serve poll triggered");

    if(items[0].revents & ZMQ_POLLIN) {
      while(1) {
        if(_zmq_recv(items[0].socket, &msg, 0) < 0) {
          goto fail;
        }
        if(_zmq_getsockopt(items[0].socket, ZMQ_RCVMORE, &more, &moresz) < 0) {
          goto fail;
        }
        if(_zmq_send(items[1].socket, &msg, more ? ZMQ_SNDMORE : 0) < 0) {
          goto fail;
        }
        if(!more) {
          break;
        }
      }
    }

    if(items[1].revents & ZMQ_POLLIN) {
      while(1) {
        if( _zmq_recv(items[1].socket, &msg, 0) < 0) {
          goto fail;
        }
        if(_zmq_getsockopt(items[1].socket, ZMQ_RCVMORE, &more, &moresz) < 0) {
          goto fail;
        }
        if(_zmq_send(items[0].socket, &msg, more ? ZMQ_SNDMORE : 0) < 0) {
          goto fail;
        }
        if(!more) {
          break;
        }
      }
    }
  } // while(1)
fail:
  zmq_msg_close(&msg);
}


} // namespace rpc
} // namespace sasuke

#undef ZSERRNO
#undef ZSERRNO2
