#ifndef incline_pgsql_h
#define incline_pgsql_h

#include <libpq-fe.h>
#include "incline_dbms.h"

class incline_pgsql : public incline_dbms {
public:
  typedef incline_dbms super;
  struct factory : public incline_dbms::factory {
    virtual ~factory() {}
    virtual incline_pgsql* create(const std::string& host, unsigned short port);
    virtual std::string get_hostport() const;
  };
protected:
  PGconn* dbh_;
public:
  virtual ~incline_pgsql();
  virtual std::string escape(const std::string& s);
  virtual void execute(const std::string& stmt);
  virtual void query(std::vector<std::vector<value_t> >& rows, const std::string& stmt);
private:
  incline_pgsql(const std::string& host, unsigned short port);
  PGconn* _dbh();
public:
  static getoptpp::opt_str opt_pgsql_host_;
  static getoptpp::opt_str opt_pgsql_user_;
  static getoptpp::opt_str opt_pgsql_password_;
  static getoptpp::opt_int opt_pgsql_port_;
  static incline_pgsql* create(const std::string& host, unsigned short port);
};

#endif
