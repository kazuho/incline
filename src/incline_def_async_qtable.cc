#include "incline_def_async_qtable.h"

using namespace std;

string
incline_def_async_qtable::parse(const picojson::value& def)
{
  string err = super::parse(def);
  if (! err.empty()) {
    return err;
  }
  // post init
  if (queue_table_.empty()) {
    queue_table_ = "_iq_" + destination();
  }
  return string();
}

string
incline_def_async_qtable::do_parse_property(const string& name,
					    const picojson::value& value)
{
  if (name == "queue_table") {
    queue_table_ = value.to_str();
  } else {
    return super::do_parse_property(name, value);
  }
  return string();
}
