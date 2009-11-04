#ifndef incline_fw_h
#define incline_fw_h

#include <string>
#include <vector>

class incline_dbms;
class incline_def_async_qtable;
class incline_driver_async_qtable;

class incline_fw {
public:
  
  class manager {
  protected:
    const incline_driver_async_qtable* driver_;
    int poll_interval_;
    int log_fd_;
  public:
    manager(const incline_driver_async_qtable* driver, int poll_interval, int log_fd) : driver_(driver), poll_interval_(poll_interval), log_fd_(log_fd) {}
    virtual ~manager() {}
    const incline_driver_async_qtable* driver() const { return driver_; }
    int poll_interval() const { return poll_interval_; }
    void log_sql(const incline_dbms* dbh, const std::string& sql);
  };
  
protected:
  manager* mgr_;
  const incline_def_async_qtable* def_;
  incline_dbms* dbh_;
  std::vector<std::string> dest_pk_columns_;
  std::string fetch_query_base_;
  std::string insert_row_query_base_;
  std::string delete_row_query_base_;
public:
  incline_fw(manager* mgr, const incline_def_async_qtable* def, incline_dbms* dbh);
  virtual ~incline_fw();
  const manager* mgr() const { return mgr_; }
  manager* mgr() { return mgr_; }
  const incline_def_async_qtable* def() const { return def_; }
  virtual void* run();
protected:
  virtual void do_run() = 0;
  void fetch_rows(const std::string& cond, std::vector<std::string>& iq_ids, std::vector<std::vector<std::string> >& delete_pks, std::vector<std::vector<std::string> >& insert_rows);
  // should perform DELETE, then (INSERT|REPLACE)
  void insert_rows(incline_dbms* dbh, const std::vector<std::vector<std::string> >& rows) const;
  void delete_rows(incline_dbms* dbh, const std::vector<std::vector<std::string> >& pk_rows) const;
protected:
  static std::string _build_pk_cond(incline_dbms* dbh, const std::vector<std::string>& colnames, const std::vector<std::string>& rows);
};

#endif
