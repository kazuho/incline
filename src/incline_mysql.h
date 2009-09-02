#ifndef incline_mysql_h
#define incline_mysql_h

#include "incline_dbms.h"

namespace tmd {
  struct conn_t;
}

class incline_mysql : public incline_dbms {
public:
  typedef incline_dbms super;
  struct factory : public super::factory {
    typedef super::factory super;
    virtual ~factory() {}
    virtual incline_mysql* create(const std::string& host, unsigned short port, const std::string& user, const std::string& password);
    virtual unsigned short default_port() const { return 3306; }
    virtual std::vector<std::string> create_trigger(const std::string& name, const std::string& event, const std::string& time, const std::string& table, const std::string& funcbody) const;
    virtual std::vector<std::string> drop_trigger(const std::string& name, const std::string& table, bool if_exists) const;
    virtual std::string create_queue_table(const std::string& table_name, const std::string& column_defs, bool if_not_exists) const;
    virtual std::string delete_using(const std::string& table_name, const std::vector<std::string>& using_list) const;
    virtual bool has_replace_into() const { return true; }
  };
protected:
  tmd::conn_t* dbh_;
public:
  virtual ~incline_mysql();
  virtual std::string escape(const std::string& s);
  virtual void execute(const std::string& stmt);
  virtual void query(std::vector<std::vector<value_t> >& rows, const std::string& stmt);
  virtual std::string get_column_def(const std::string& table_name, const std::string& column_name);
private:
  incline_mysql(const std::string& host, unsigned short port, const std::string& user, const std::string& password);
};

#endif
