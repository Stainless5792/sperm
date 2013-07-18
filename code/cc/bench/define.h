/*
 * Copyright (C) dirlt
 */

#ifndef __SPERM_CC_BENCH_DEFINE_H__
#define __SPERM_CC_BENCH_DEFINE_H__

#define TC(func,arg) do {                       \
    for(int i=0;i<kThreadNumber;i++){           \
      pthread_create(&tid[i],NULL,func,arg);    \
    }                                           \
  }while(0)
#define TJ() do {                               \
    for(int i=0;i<kThreadNumber;i++){           \
      pthread_join(tid[i],NULL);                \
    }                                           \
  }while(0)

#define RTC(func,arg) do {                      \
    for(int i=0;i<kReadThreadNumber;i++){       \
      pthread_create(&rtid[i],NULL,func,arg);   \
    }                                           \
  }while(0)
#define WTC(func,arg) do {                        \
    for(int i=0;i<kWriteThreadNumber;i++){        \
      pthread_create(&wtid[i],NULL,func,arg);     \
    }                                             \
  }while(0)
#define RTJ() do {                              \
    for(int i=0;i<kReadThreadNumber;i++){       \
      pthread_join(rtid[i],NULL);               \
    }                                           \
  }while(0)
#define WTJ() do {                              \
    for(int i=0;i<kWriteThreadNumber;i++){      \
      pthread_join(wtid[i],NULL);               \
    }                                           \
  }while(0)

#endif // __SPERM_CC_BENCH_DEFINE_H__
