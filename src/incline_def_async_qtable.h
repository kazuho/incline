#ifndef incline_def_async_qtable_h
#define incline_def_async_qtable_h

#include "incline_def_async.h"

class incline_def_async_qtable : public incline_def_async {
public:
  typedef incline_def_async super;
protected:
  std::string queue_table_;
public:
  std::string queue_table() const { return queue_table_; }
  virtual std::string parse(const picojson::value& def);
protected:
  virtual std::string do_parse_property(const std::string& name, const picojson::value& value);
};

#endif
