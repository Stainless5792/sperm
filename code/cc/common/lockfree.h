/* coding:utf-8
 * Copyright (C) dirlt
 */

#ifndef __SPERM_CC_COMMON_LOCKFREE_H__
#define __SPERM_CC_COMMON_LOCKFREE_H__

#include "atomic.h"

namespace common {
namespace lockfree {

template<typename T, const int number = 1024* 1024>
class SampleMemoryAllcator {
 public:
  struct Entry {   
    T value;
    Entry* next;
    Entry():value(), next(0) {
    }
  };
  static const int kSampleNumber = number;
 private:
  volatile Entry* head_;
  Entry* arena_;
 public:
    
  SampleMemoryAllcator(): head_(0), arena_(0) {
    arena_ = static_cast<Entry*>(calloc(number, sizeof(Entry)));
    for(int i=1;i<number;i++){
      arena_[i-1].next=arena_+i;
    }
    arena_[number-1].next=0;
    head_ = arena_;
  }

  ~SampleMemoryAllcator() {
    free(arena_);
  }

  Entry* alloc() {
    for(;;) {
      Entry* t= const_cast<Entry*>(AtomicGetValue(head_));
      if(t) {
        Entry* n = t->next;
        if(CompareAndSwapPointer(head_, n, t) == t){
          return t;
        }
      }
    }
    return 0;
  }
  
  void free(Entry* e) {
    for(;;) {
      Entry* t=const_cast<Entry*>(AtomicGetValue(head_));
      e->next = t;
      if(CompareAndSwapPointer(head_, e,  t) == t){
        break;      
      }
    }
  }
}; // class SampleMemoryAllcator

} // namespace lockfree
} // namespace common

#endif // __SPERM_CC_COMMON_LOCKFREE_H__
