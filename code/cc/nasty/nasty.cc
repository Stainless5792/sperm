/* coding:utf-8
 * Copyright (C) dirlt
 */

#include <sstream>
#include "common/logger_inl.h"
#include "nasty/nasty.h"
#include "nasty/nasty.y.hh"
#include "nasty/nasty.l.hh"

int yyparse(void* scanner, sperm::nasty::Parser* parser);

namespace sperm {
namespace nasty {

void Parser::free() {
  delete ex_;
  ex_ = 0;
}

Parser::~Parser() {
  free();
}

Expr* Parser::run() {
  yyscan_t scanner;
  yylex_init(&scanner);
  FILE* fin = fopen(f_.c_str(), "rb");
  if(fin == 0) {
    SPERM_WARNING("open(%s) failed(%s)", f_.c_str(), SERRNO);
    return 0;
  }
  yyset_in(fin, scanner);
  if(yyparse(scanner, this) != 0) {
    free();
    fclose(fin);
    return 0;
  }
  return ex_;
}

void Expr::appendAtom(Atom* at) {
  as_.push_back(at);
}

Expr::~Expr() {
  for(size_t i = 0; i < as_.size(); i++) {
    delete as_[i];
  }
}

static inline void repeat(std::ostream& os, int indent, char c) {
  for(int i = 0; i < indent; i++) {
    os << c;
  }
}

std::string Expr::toString() const {
  std::ostringstream oss ;
  int lbrace = 0;
  write(oss, 0, lbrace);
  return oss.str();
}

void Expr::write(std::ostream& os, int indent, int& lbrace) const {
  for(size_t i = 0; i < as_.size(); i++) {
    as_[i]->write(os, indent, lbrace);
    if((i + 1) != as_.size()) {
      os << '\n';
    }
  }
}

Atom::~Atom() {
  delete ex_;
  ex_ = 0;
}

void Atom::write(std::ostream& os, int indent, int& lbrace) const {
  if(type_ == EX) {
    lbrace += 1;
    ex_ -> write(os, indent + 1, lbrace);
    os << ')';
  } else {
    repeat(os, (lbrace != 0) ? indent-1 : indent, ' ');
    repeat(os, lbrace, '(');
    lbrace = 0;
    os << s_;
  }
}

} // namespace nasty
} // namespace sperm
