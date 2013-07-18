/*
 * Copyright (C) dirlt
 */

#ifndef __SPERM_CC_COMMON_LOGGER_INL_H__
#define __SPERM_CC_COMMON_LOGGER_INL_H__

#include <cstdlib>
#include <cstdio>
#include <cerrno>
#include <cstring>

#define SERRNO (strerror(errno))
#define SERRNO2(n) (strerror(n))

#define SPERM_NOTICE(fmt,...) fprintf(stderr,"[NOTICE][%s:%d]"fmt"\n",__FILE__,__LINE__,##__VA_ARGS__)
#define SPERM_TRACE(fmt,...) fprintf(stderr,"[TRACE][%s:%d]"fmt"\n",__FILE__,__LINE__,##__VA_ARGS__)
#ifdef DEBUG
#define SPERM_DEBUG(fmt,...) fprintf(stderr,"[DEBUG][%s:%d]"fmt"\n",__FILE__,__LINE__,##__VA_ARGS__)
#else
#define SPERM_DEBUG(fmt,...)
#endif
#define SPERM_WARNING(fmt,...) fprintf(stderr,"[WARNING][%s:%d]"fmt"\n",__FILE__,__LINE__,##__VA_ARGS__)
#define SPERM_FATAL(fmt,...) fprintf(stderr,"[FATAL][%s:%d]"fmt"\n",__FILE__,__LINE__,##__VA_ARGS__);abort()

#endif // __SPERM_CC_COMMON_LOGGER_INL_H__
