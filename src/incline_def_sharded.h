#ifndef incline_def_sharded_h
#define incline_def_sharded_h

#include "incline_def_async_qtable.h"

class incline_def_sharded : public incline_def_async_qtable {
public:
  typedef incline_def_async_qtable super;
protected:
  std::string shard_file_;
public:
  std::string shard_file() const { return shard_file_; }
  virtual std::string parse(const picojson::value& def);
protected:
  virtual std::string do_parse_property(const std::string& name, const picojson::value& value);
};

#endif
