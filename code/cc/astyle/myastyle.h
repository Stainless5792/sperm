/*
 * Copyright (C) dirlt
 */


#ifndef __SPERM_CC_ASTYLE_MYASTYLE_H__
#define __SPERM_CC_ASTYLE_MYASTYLE_H__

#include <string>

namespace astyle {

extern const char* kDefaultOptions;
const std::string beautify(const char* src,
                           const char* options = kDefaultOptions);

} // namespace astyle

#endif // __SPERM_CC_ASTYLE_MYASTYLE_H__
