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
    virtual incline_dbms* create(const std::string& host, unsigned short port, const std::string& user, const std::string& password) = 0;
    incline_dbms* create() { return create(std::string(), 0, std::string(), std::string()); }
    std::pair<std::string, unsigned short> get_hostport() const;
    virtual unsigned short default_port() const = 0;
    virtual std::vector<std::string> create_trigger(const std::string& name, const std::string& event, const std::string& time, const std::string& table, const std::string& funcbody) const = 0;
    virtual std::vector<std::string> drop_trigger(const std::string& name, const std::string& table, bool if_exists) const = 0;
    virtual std::string create_queue_table(const std::string& table_name, const std::string& column_defs, bool if_not_exists) const = 0;
    virtual std::string delete_using(const std::string& table_name, const std::vector<std::string>& using_list) const = 0;
    virtual bool has_replace_into() const { return false; }
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
protected:
  std::string host_;
  unsigned short port_;
public:
  virtual ~incline_dbms() {}
  virtual std::string escape(const std::string& s) = 0;
  virtual void execute(const std::string& stmt) = 0;
  void query(std::vector<std::vector<value_t> >& rows, const char* fmt, ...) __attribute__((__format__(__printf__, 3, 4)));
  virtual void query(std::vector<std::vector<value_t> >& rows, const std::string& stmt) = 0;
  virtual std::string get_column_def(const std::string& table_name, const std::string& column_name) = 0;
protected:
  incline_dbms(const std::string& host, unsigned short port) : host_(host), port_(port) {}
private:
  incline_dbms(const incline_dbms&); // not copyable
  incline_dbms& operator=(const incline_dbms&);
public:
  static getoptpp::opt_str opt_rdbms_;
  static getoptpp::opt_str opt_database_;
  static getoptpp::opt_str opt_host_;
  static getoptpp::opt_int opt_port_;
  static getoptpp::opt_str opt_user_;
  static getoptpp::opt_str opt_password_;
  static factory* factory_;
  static bool setup_factory();
};

#endif
