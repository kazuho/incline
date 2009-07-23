#include <assert.h>
#include "incline_def.h"
#include "incline_util.h"

using namespace std;

bool
incline_def::is_master_of(const string& table) const
{
  for (map<string, string>::const_iterator pki = pk_columns_.begin();
       pki != pk_columns_.end();
       ++pki) {
    if (table_of_column(pki->first) == table) {
      return true;
    }
  }
    return false;
}

bool
incline_def::is_dependent_of(const string& table) const
{
  for (vector<string>::const_iterator si = source_.begin();
       si != source_.end();
       ++si) {
    if (*si == table) {
      return true;
    }
  }
  return false;
}

vector<string>
incline_def::build_merge_cond(const string& tbl_rewrite_from,
			      const string& tbl_rewrite_to, bool master_only)
  const
{
  vector<string> cond;
  for (vector<pair<string, string> >::const_iterator mi = merge_.begin();
       mi != merge_.end();
       ++mi) {
    string first_tbl = table_of_column(mi->first),
      second_tbl = table_of_column(mi->second);
    if (master_only &&
	! ((is_master_of(first_tbl) || first_tbl == tbl_rewrite_from)
	   && (is_master_of(second_tbl) || second_tbl == tbl_rewrite_from))) {
      continue;
    }
    if (first_tbl == tbl_rewrite_from) {
      cond.push_back(tbl_rewrite_to
		     + mi->first.substr(first_tbl.size()) + '='
		     + mi->second);
    } else if (second_tbl == tbl_rewrite_from) {
      cond.push_back(mi->first + '=' + tbl_rewrite_to
		     + mi->second.substr(second_tbl.size()));
    } else {
      cond.push_back(mi->first + '=' + mi->second);
    }
  }
  return cond;
}

string
incline_def::parse(const picojson::value& def)
{
  if (! def.is<picojson::object>()) {
    return "definition should be an object";
  }
  // get destination
  if (picojson::value d = def.get("destination")) {
    destination_ = d.to_str();
  } else {
    return "no destination";
  }
  { // get source
    picojson::value src = def.get("source");
    if (src.is<std::string>()) {
      source_.push_back(src.get<std::string>());
    } else if (src.is<picojson::array>()) {
      for (picojson::array::const_iterator si
	     = src.get<picojson::array>().begin();
	   si != src.get<picojson::array>().end();
	   ++si) {
	source_.push_back(si->to_str());
      }
    } else {
      return "no source (of type string or array)";
    }
  }
  { // get pk_columns
    string err;
    if (! (err = _parse_columns(def, "pk_columns", pk_columns_)).empty()) {
      return err;
    }
    // get npk_columns
    if (! def.get("npk_columns").is<picojson::null>()
	&& ! (err = _parse_columns(def, "npk_columns", npk_columns_)).empty()) {
      return err;
    }
  }
  // get merge
  if (picojson::value merge = def.get("merge")) {
    if (! merge.is<picojson::object>()) {
      return "merge should be of type object";
    }
    for (picojson::object::const_iterator mi
	   = merge.get<picojson::object>().begin();
	 mi != merge.get<picojson::object>().end();
	 ++mi) {
      string l(mi->first), r(mi->second.to_str());
      if (! is_dependent_of(table_of_column(l, ""))) {
	return "table of " + l + " not in source";
      } else if (! is_dependent_of(table_of_column(r, ""))) {
	return "table of " + r + " not in source";
      }
      merge_.push_back(make_pair(l, r));
    }
  }
  // build other props
  _rebuild_columns();
  // parse other attributes
  for (picojson::object::const_iterator oi
	 = def.get<picojson::object>().begin();
       oi != def.get<picojson::object>().end();
       ++oi) {
    if (! incline_util::is_one_of("destination\0source\0pk_columns\0"
				  "npk_columns\0merge\0",
				  oi->first)) {
      string err = do_parse_property(oi->first, oi->second);
      if (! err.empty()) {
	return err;
      }
    }
  }
  return string();
}

string
incline_def::do_parse_property(const string& name, const picojson::value&)
{
  return "unknown property:" + name;
}

string
incline_def::_parse_columns(const picojson::value& def,
			    const string& property,
			    map<string, string>& columns)
{
  picojson::value cols = def.get(property);
  if (! cols.is<picojson::object>()) {
    return "no " + property + " (of type object)";
  }
  for (picojson::object::const_iterator ci
	 = cols.get<picojson::object>().begin();
       ci != cols.get<picojson::object>().end();
       ++ci) {
    if (! is_dependent_of(table_of_column(ci->first, ""))) {
      return "table of " + ci->first + " not in source";
    }
    columns[ci->first] = ci->second.to_str();
  }
  return string();
}

string
incline_def::source_column_of(const string& dest_column,
			      const char* ret_on_error) const
{
  map<string, string>::const_iterator ci;
  for (map<string, string>::const_iterator ci = columns_.begin();
       ci != columns_.end();
       ++ci) {
    if (ci->second == dest_column) {
      return ci->first;
    }
  }
  assert(ret_on_error != NULL);
  return ret_on_error;
}

void
incline_def::_rebuild_columns()
{
  columns_.clear();
  for (map<string, string>::const_iterator i = pk_columns_.begin();
       i != pk_columns_.end();
       ++i) {
    columns_[i->first] = i->second;
  }
  for (map<string, string>::const_iterator i = npk_columns_.begin();
       i != npk_columns_.end();
       ++i) {
    columns_[i->first] = i->second;
  }
}

string
incline_def::table_of_column(const string& column, const char* ret_on_error)
{
  string::size_type dot_at = column.find('.', 0);
  if (ret_on_error == NULL) {
    assert(dot_at != string::npos);
  } else if (dot_at == string::npos) {
    return ret_on_error;
  }
  return column.substr(0, dot_at);
}
