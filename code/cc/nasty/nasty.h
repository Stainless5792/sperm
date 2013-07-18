/* coding:utf-8
 * Copyright (C) dirlt
 */

#ifndef __SPERM_CC_NASTY_NASTY_H__
#define __SPERM_CC_NASTY_NASTY_H__

#include <string>
#include <vector>
#include <ostream>

namespace sperm {
namespace nasty {

class Expr;
class Atom;

class Parser {
public:
  Parser(const char* f) :
    f_(f), ex_(0) {
  }
  const std::string& f() const {
    return f_;
  }
  Expr* run();
  void setExpr(Expr* ex) {
    ex_ = ex;
  }
  ~Parser();
private:
  void free();
  std::string f_;
  Expr* ex_;
}; // class Parser

class Expr {
public:
  const std::vector< Atom* >& as() const {
    return as_;
  }
  void appendAtom(Atom* at);
  ~Expr();
  std::string toString() const ;
private:
  friend class Atom;
  friend class Parser;
  void write(std::ostream& os, int indent, int& lbrace) const;
  std::vector< Atom* > as_;
}; // class Expr

class Atom {
public:
  enum {
    SR,
    ID,
    DBL,
    INT,
    EX,
  };
  Atom(int type, const char* text, int leng,
       int line, int column):
    type_(type), s_(text, leng),
    line_(line), column_(column),
    ex_(0) {
  }
  Atom(Expr* ex):
    type_(EX), ex_(ex) {
  }
  ~Atom();
private:
  friend class Expr;
  friend class Parser;
  void write(std::ostream& os, int indent, int& lbrace) const;
  int type_;
  std::string s_;
  int line_;
  int column_;
  Expr* ex_;
}; // class Atom

} // namespace nasty
} // namespace sperm

#endif // __SPERM_CC_NASTY_NASTY_H__
