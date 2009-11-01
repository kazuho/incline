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
  virtual void do_post_commit(const std::vector<std::string>& iq_ids);
};

#endif
