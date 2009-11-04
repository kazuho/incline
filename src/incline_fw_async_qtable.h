#ifndef incline_fw_async_qtable_h
#define incline_fw_async_qtable_h

#include "incline_fw.h"

class incline_fw_async_qtable : public incline_fw {
public:
  typedef incline_fw super;
protected:
  std::string clear_queue_query_base_;
public:
  incline_fw_async_qtable(manager* mgr, const incline_def_async_qtable* def, incline_dbms* dbh);
protected:
  virtual void do_run();
  virtual bool do_update_rows(const std::vector<std::vector<std::string> >& delete_pks, const std::vector<std::vector<std::string> >& insert_rows);
  virtual std::string do_get_extra_cond();
};

#endif
