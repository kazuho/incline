#ifndef incline_driver_async_qtable_h
#define incline_driver_async_qtable_h

#include "incline_driver_async.h"

namespace tmd {
  class conn_t;
}

class incline_driver_async_qtable : public incline_driver_async {
public:
  virtual incline_def* create_def() const;
  std::string create_table_of(const incline_def* def, tmd::conn_t& dbh) const;
  std::string drop_table_of(const incline_def* def, bool if_exists) const;
protected:
  virtual std::vector<std::string> do_build_enqueue_sql(const incline_def* def, const std::map<std::string, std::string>& pk_columns, const std::vector<std::string>& tables, const std::vector<std::string>& cond) const;
};

#endif
