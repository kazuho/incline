#ifndef incline_driver_async_qtable_h
#define incline_driver_async_qtable_h

#include "incline_driver_async.h"

class incline_def_async_qtable;
class incline_dbms;

class incline_driver_async_qtable : public incline_driver_async {
public:
  virtual incline_def* create_def() const;
  std::vector<std::string> create_table_all(bool if_not_exists, incline_dbms* dbh) const;
  std::vector<std::string> drop_table_all(bool if_exists) const;
  std::string create_table_of(const incline_def* def, bool if_not_exists, incline_dbms* dbh) const;
  std::string drop_table_of(const incline_def* def, bool if_exists) const;
  virtual void run_forwarder(int poll_interval, int log_fd) const;
  virtual bool should_exit_loop() const { return false; }
protected:
  std::string _create_table_of(const incline_def_async_qtable* def, const std::string& table_name, bool if_not_exists, incline_dbms* dbh) const;
  virtual void do_build_enqueue_insert_sql(trigger_body& body, const incline_def* def, const std::string& src_table, action_t action, const std::vector<std::string>* cond) const;
  virtual void do_build_enqueue_delete_sql(trigger_body& body, const incline_def* def, const std::string& src_table, const std::vector<std::string>* cond) const;
};

#endif
