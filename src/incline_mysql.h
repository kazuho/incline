#ifndef incline_mysql_h
#define incline_mysql_h

#include "incline_dbms.h"

namespace tmd {
  struct conn_t;
}

class incline_mysql : public incline_dbms {
public:
  typedef incline_dbms super;
  struct factory : public incline_dbms::factory {
    virtual ~factory() {}
    virtual incline_mysql* create(const std::string& host, unsigned short port);
    virtual std::string get_hostport() const;
  };
protected:
  tmd::conn_t* dbh_;
public:
  virtual ~incline_mysql();
  virtual std::string escape(const std::string& s);
  virtual void execute(const std::string& stmt);
  virtual void query(std::vector<std::vector<value_t> >& rows, const std::string& stmt);
private:
  incline_mysql(const std::string& host, unsigned short port);
public:
  static getoptpp::opt_str opt_mysql_host_;
  static getoptpp::opt_str opt_mysql_user_;
  static getoptpp::opt_str opt_mysql_password_;
  static getoptpp::opt_int opt_mysql_port_;
};

#endif
