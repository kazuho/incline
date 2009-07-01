#include <assert.h>
#include "incline_def.h"

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
      cond.push_back(tbl_rewrite_to + '.'
		     + mi->first.substr(first_tbl.size() + 1) + '='
		     + mi->second);
    } else if (second_tbl == tbl_rewrite_from) {
      cond.push_back(mi->first + '=' + tbl_rewrite_to
		     + mi->second.substr(second_tbl.size() + 1));
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
  // get source
  picojson::value src = def.get("source");
  if (src.is<std::string>()) {
    source_.push_back(src.get<std::string>());
  } else if (src.is<picojson::array>()) {
    for (picojson::array::const_iterator si(src.get<picojson::array>().begin());
	 si != src.get<picojson::array>().end();
	 ++si) {
      source_.push_back(si->to_str());
    }
  } else {
    return "no source (of type string or array)";
  }
  // get pk_columns
  picojson::value pkc = def.get("pk_columns");
  if (! pkc.is<picojson::object>()) {
    return "no pk_columns (of type object)";
  }
  for (picojson::object::const_iterator pi(pkc.get<picojson::object>().begin());
       pi != pkc.get<picojson::object>().end();
       ++pi) {
    pk_columns_[pi->first] = pi->second.to_str();
  }
  // get npk_columns
  picojson::value npc = def.get("npk_columns");
  if (! npc.is<picojson::object>()) {
    return "no pk_columns (of type object)";
  }
  for (picojson::object::const_iterator ni(npc.get<picojson::object>().begin());
       ni != npc.get<picojson::object>().end();
       ++ni) {
    npk_columns_[ni->first] = ni->second.to_str();
  }
  // TODO parse other attributes
  return string();
}

void
incline_def::_rebuild_columns()
{
  columns_.clear();
  dest_columns_.clear();
  for (map<string, string>::const_iterator i = pk_columns_.begin();
       i != pk_columns_.end();
       ++i) {
    columns_[i->first] = i->second;
    dest_columns_[i->second] = i->first;
  }
  for (map<string, string>::const_iterator i = npk_columns_.begin();
       i != npk_columns_.end();
       ++i) {
    columns_[i->first] = i->second;
    dest_columns_[i->second] = i->first;
  }
}

string
incline_def::table_of_column(const string& column)
{
  string::size_type dot_at = column.find('.', 0);
  assert(dot_at != string::npos);
  return column.substr(0, dot_at);
}
