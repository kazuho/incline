#include "incline_def_sharded.h"

using namespace std;

string
incline_def_sharded::parse(const picojson::value& def)
{
  string err = super::parse(def);
  if (! err.empty()) {
    return err;
  }
  // post init
  if (direct_expr_column_.empty()) {
    return "no shard_key defined for table:" + destination();
  }
  return string();
}

string
incline_def_sharded::do_parse_property(const string& name,
				       const picojson::value& value)
{
  if (name == "shard-key") {
    set_direct_expr_column(value.to_str());
  } else {
    return super::do_parse_property(name, value);
  }
  return string();
}
