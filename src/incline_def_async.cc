#include "incline_def_async.h"

using namespace std;

string
incline_def_async::parse(const picojson::value& def)
{
  string err = super::parse(def);
  if (! err.empty()) {
    return err;
  }
  // post init
  if (! direct_expr_column_.empty()
      && (pk_columns().find(source_column_of(direct_expr_column_, ""))
	  == pk_columns().end())) {
    return "column not exists in pk of destination:" + destination() + '.'
      + direct_expr_column_;
  }
  return string();
}

string
incline_def_async::direct_expr_column(const string& desired_table,
				      const string& table_rewrite_to) const
{
  if (direct_expr_column_.empty()) {
    return string();
  } else if (table_rewrite_to.empty()) {
    return direct_expr_column_;
  }
  
  string src_column = source_column_of(direct_expr_column_);
  if (table_of_column(src_column) == desired_table) {
    return table_rewrite_to + src_column.substr(desired_table.size());
  }
  // TODO currently only follows inner join conds of 1 depth
  for (vector<pair<string, string> >::const_iterator mi = merge_.begin();
       mi != merge_.end();
       ++mi) {
    if (mi->first == src_column
	&& table_of_column(mi->second) == desired_table) {
      return table_rewrite_to + mi->second.substr(desired_table.size());
    }
    if (mi->second == src_column
	&& table_of_column(mi->first) == desired_table) {
      return table_rewrite_to + mi->first.substr(desired_table.size());
    }
  }
  return src_column;
}
