#ifndef incline_def_sharded_h
#define incline_def_sharded_h

#include "incline_def_async_qtable.h"

class incline_def_sharded : public incline_def_async_qtable {
  typedef incline_def_async_qtable super;
public:
  virtual std::string parse(const picojson::value& def);
protected:
  virtual std::string do_parse_property(const std::string& name, const picojson::value& value);
};

#endif
