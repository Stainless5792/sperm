/*
 * Copyright (C) dirlt
 */

#include <string>
#include "astyle/myastyle.h"
#include "common/logger_inl.h"

static const char* nasty_code = "int main(){\nprintf(\"hello,world\");\nreturn 0;\n}\n";

int main() {
  std::string s = astyle::beautify(nasty_code);
  printf("--------------------\n%s\n--------------------\n", s.c_str());
  return 0;
}
