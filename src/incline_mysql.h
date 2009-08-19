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
    virtual unsigned short default_port() const { return 3306; }
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
};

#endif
