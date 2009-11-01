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
  if (shard_file_.empty()) {
    return "no shard.file defined for table:" + destination();
  }
  return string();
}

string
incline_def_sharded::do_parse_property(const string& name,
				       const picojson::value& value)
{
  if (name == "shard") {
    if (! value.is<picojson::object>()) {
      return "property \"shard\" of table:" + destination()
	+ " is not an object";
    }
    const picojson::value& key = value.get("key");
    if (! key.is<picojson::null>()) {
      set_direct_expr_column(key.to_str());
    }
    shard_file_ = value.get("file").to_str();
  } else {
    return super::do_parse_property(name, value);
  }
  return string();
}
