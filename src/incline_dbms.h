#ifndef incline_dbms_h
#define incline_dbms_h

#include <stdexcept>
#include <string>
#include <vector>
#include "getoptpp.h"

class incline_dbms {
public:
  struct factory {
    virtual ~factory() {}
    virtual incline_dbms* create(const std::string& host, unsigned short port) = 0;
    incline_dbms* create() { return create(std::string(), 0); }
    virtual std::string get_hostport() const = 0;
  };
  class value_t {
  protected:
    std::string* s_;
  public:
    value_t() : s_(NULL) {}
    value_t(const char* s) : s_(NULL) { if (s != NULL) s_ = new std::string(s); }
    value_t(const std::string& s) : s_(NULL) { s_ = new std::string(s); }
    value_t(const value_t& x) : s_(NULL) { if (x.s_ != NULL) s_ = new std::string(*x.s_); }
    ~value_t() { delete s_; }
    value_t& operator=(const value_t& x) {
      if (this != &x) {
	std::string* t = new std::string(*x.s_);
	std::swap(s_, t);
	delete t;
      }
      return *this;
    }
    bool is_null() const { return s_ == NULL; }
    const std::string* operator->() const { return s_; }
    std::string* operator->(){ return s_; }
    const std::string& operator*() const { return *s_; }
    std::string& operator*() { return *s_; }
  };
  struct error_t : public std::domain_error {
    error_t(const std::string& s) : std::domain_error(s) {}
  };
  struct deadlock_error_t : public error_t {
    deadlock_error_t(const std::string& s) : error_t(s) {}
  };
  struct timeout_error_t : public error_t {
    timeout_error_t(const std::string& s) : error_t(s) {}
  };
public:
  virtual ~incline_dbms() {}
  virtual std::string escape(const std::string& s) = 0;
  virtual void execute(const std::string& stmt) = 0;
  void query(std::vector<std::vector<value_t> >& rows, const char* fmt, ...) __attribute__((__format__(__printf__, 3, 4)));
  virtual void query(std::vector<std::vector<value_t> >& rows, const std::string& stmt) = 0;
protected:
  incline_dbms() {}
private:
  incline_dbms(const incline_dbms&); // not copyable
  incline_dbms& operator=(const incline_dbms&);
public:
  static getoptpp::opt_str opt_rdbms_;
  static getoptpp::opt_str opt_database_;
  static factory* factory_;
  static bool setup_factory();
};

#endif
