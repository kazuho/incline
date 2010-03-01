#ifndef incline_mysql_h
#define incline_mysql_h

#include "incline_dbms.h"

namespace tmd {
  class conn_t;
}

class incline_mysql : public incline_dbms {
public:
  typedef incline_dbms super;
  struct factory : public super::factory {
    virtual ~factory() {}
    virtual incline_mysql* create(const std::string& host, unsigned short port, const std::string& user, const std::string& password);
    virtual unsigned short default_port() const { return 3306; }
    virtual std::vector<std::string> create_trigger(const std::string& name, const std::string& event, const std::string& time, const std::string& table, const std::map<std::string, std::string>& funcvar, const std::string& funcbody) const;
    virtual std::vector<std::string> drop_trigger(const std::string& name, const std::string& table, bool if_exists) const;
    virtual std::string serial_column_type() const { return "BIGINT UNSIGNED NOT NULL AUTO_INCREMENT"; }
    virtual std::string create_table_suffix() const { return " ENGINE=InnoDB DEFAULT CHARSET=utf8"; }
    virtual std::string delete_using(const std::string& table_name, const std::vector<std::string>& using_list) const;
    virtual bool has_replace_into() const { return true; }
  };
protected:
  tmd::conn_t* dbh_;
public:
  virtual ~incline_mysql();
  virtual std::string escape(const std::string& s);
  virtual unsigned long long execute(const std::string& stmt);
  virtual void query(std::vector<std::vector<value_t> >& rows, const std::string& stmt);
  virtual std::string get_column_def(const std::string& table_name, const std::string& column_name);
private:
  incline_mysql(const std::string& host, unsigned short port, const std::string& user, const std::string& password);
};

#endif
