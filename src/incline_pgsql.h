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
    virtual incline_pgsql* create(const std::string& host, unsigned short port, const std::string& user, const std::string& password);
    virtual unsigned short default_port() const { return 5432; }
    virtual std::vector<std::string> create_trigger(const std::string& name, const std::string& event, const std::string& time, const std::string& table, const std::map<std::string, std::string>& funcvar, const std::string& funcbody) const;
    virtual std::vector<std::string> drop_trigger(const std::string& name, const std::string& table, bool if_exists) const;
    virtual std::string serial_column_type() const { return "BIGSERIAL"; }
    virtual std::string create_table_suffix() const { return std::string(); }
    virtual std::string delete_using(const std::string& table_name, const std::vector<std::string>& using_list) const;
  };
protected:
  PGconn* dbh_;
  std::string conninfo_;
public:
  virtual ~incline_pgsql();
  virtual std::string escape(const std::string& s);
  virtual unsigned long long execute(const std::string& stmt);
  virtual void query(std::vector<std::vector<value_t> >& rows, const std::string& stmt);
 virtual std::string get_column_def(const std::string& table_name, const std::string& column_name);
private:
 incline_pgsql(const std::string& host, unsigned short port, const std::string& user, const std::string& password);
  PGconn* _dbh();
};

#endif
