#ifndef incline_pgsql_h
#define incline_pgsql_h

#include <libpq-fe.h>
#include "incline_dbms.h"

class incline_pgsql : public incline_dbms {
public:
  typedef incline_dbms super;
  struct factory : public super::factory {
    typedef super::factory super;
    virtual ~factory() {}
    virtual incline_pgsql* create(const std::string& host, unsigned short port);
    virtual unsigned short default_port() const { return 5432; }
    virtual std::vector<std::string> create_trigger(const std::string& name, const std::string& event, const std::string& time, const std::string& table, const std::string& funcbody) const;
    virtual std::vector<std::string> drop_trigger(const std::string& name, bool if_exists) const;
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
};

#endif
