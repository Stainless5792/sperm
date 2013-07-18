/*
 * Copyright (C) dirlt
 */


#include "astyle/astyle.h"
#include "astyle/myastyle.h"
#include "common/logger_inl.h"

static void AstyleError(int /*number*/, char* mesg) {
  SPERM_FATAL("astyle failed(%s)", mesg);
}

static char* AstyleAlloc(unsigned long size) {
  char* p = new char[size];
  return p;
}

namespace astyle {

const char* kDefaultOptions =
  "--style=java\n"
  "--indent=spaces=2\n"
  "--brackets=attach\n"
  "--indent-switches\n"
  "--indent-labels\n"
  "--indent-preprocessor\n"
  "--indent-col1-comments\n"
  "--convert-tabs\n"
  "--delete-empty-lines\n"
  "--align-pointer=type\n"
  "--pad-oper\n";

const std::string beautify(const char* src, const char* options) {
  char* out = AStyleMain(src, options, AstyleError, AstyleAlloc);
  std::string res(out);
  free(out);
  return res;
}

} // namespace astyle
