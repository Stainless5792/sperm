/* coding:utf-8
 * Copyright (C) dirlt
 */

#include "sasuke/zhelpers.h"

void* context;
int main() {
  context = zmq_init(4);
  const char* endpoint = "tcp://127.0.0.1:19870";
  {
    void* end = zmq_socket(context, ZMQ_ROUTER);
    zmq_bind(end, endpoint);
    void* front = zmq_socket(context, ZMQ_DEALER);
    zmq_connect(front, endpoint);
    s_send(front, "hello");
    {
      zmq_msg_t message;
      // --------------------recv--------------------
      // uuid.
      zmq_msg_init(&message);
      zmq_recv(end, &message, 0);
      size_t uuid_size = zmq_msg_size(&message);
      char* uuid = new char[uuid_size];
      memcpy(uuid, static_cast<char*>(zmq_msg_data(&message)), uuid_size);
      zmq_msg_close(&message);

      // content.
      zmq_msg_init(&message);
      zmq_recv(end, &message, 0);
      printf("recv...'%s'\n", static_cast<char*>(zmq_msg_data(&message)));
      zmq_msg_close(&message);

      // --------------------send--------------------
      // uuid.
      zmq_msg_init_size(&message, uuid_size);
      memcpy(zmq_msg_data(&message), uuid, uuid_size);
      zmq_send(end, &message, ZMQ_SNDMORE);
      zmq_msg_close(&message);
      s_send(end, "hello.ack");

      s_dump(front);
    }
    s_send(front, "world");
    s_dump(end);

    // --------------------
    void* front2 = zmq_socket(context, ZMQ_DEALER);
    zmq_connect(front2, endpoint);
    s_send(front2, "xyz");
    s_dump(end);
    zmq_close(front);
    zmq_close(front2);
    zmq_close(end);
  }
  zmq_term(context);
  return 0;
}
