#include "incline_def_async.h"

using namespace std;

string
incline_def_async::direct_expr(const string& col_expr) const
{
  string r;
  for (string::const_iterator ei = direct_expr_base_.begin();
       ei != direct_expr_base_.end();
       ++ei) {
    if (*ei == '?') {
      if (ei + 1 != direct_expr_base_.end() && *(ei + 1) == '?') {
	r += '?';
	++ei;
      } else {
	r += col_expr;
      }
    } else {
      r += *ei;
    }
  }
  return r;   
}

string
incline_def_async::parse(const picojson::value& def)
{
  string err = super::parse(def);
  if (! err.empty()) {
    return err;
  }
  // post init
  if (direct_expr_column_.empty() != direct_expr_base_.empty()) {
    return "properties \"direct_expr\" and \"direct_expr_column\" should be used together";
  }
  return string();
}

string
incline_def_async::do_parse_property(const string& name,
				     const picojson::value& value)
{
  if (name == "direct_expr") {
    direct_expr_base_ = value.to_str();
  } else if (name == "direct_expr_column") {
    direct_expr_column_ = value.to_str();
  } else {
    return super::do_parse_property(name, value);
  }
  return string();
}
