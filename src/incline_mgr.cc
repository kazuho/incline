#include <algorithm>
#include <set>
#include "incline_driver.h"
#include "incline_mgr.h"
#include "incline_util.h"
#include "tmd.h"

using namespace std;

incline_mgr::incline_mgr(incline_driver* d)
  : db_name_("test"), defs_(), driver_(d), trigger_time_("AFTER")
{
  driver_->mgr_ = this;
}

incline_mgr::~incline_mgr() {
  for (vector<incline_def*>::iterator di = defs_.begin();
       di != defs_.end();
       ++di) {
    delete *di;
  }
  delete driver_;
}

string
incline_mgr::parse(const picojson::value& src)
{
  if (! src.is<picojson::array>()) {
    return "definition should be an array of objects";
  }
  for (picojson::array::const_iterator di = src.get<picojson::array>().begin();
       di != src.get<picojson::array>().end();
       ++di) {
    if (! di->is<picojson::object>()) {
      return "definition should be an array of objects";
    }
    incline_def* def = driver_->create_def();
    string err = def->parse(*di);
    if (! err.empty()) {
      delete def;
      return err;
    }
    defs_.push_back(def);
  }
  return string();
}

vector<string>
incline_mgr::get_src_tables() const
{
  set<string> rs;
  for (vector<incline_def*>::const_iterator di = defs_.begin();
       di != defs_.end();
       ++di) {
    for (vector<string>::const_iterator si = (*di)->source().begin();
	 si != (*di)->source().end();
	 ++si) {
      rs.insert(*si);
    }
  }
  vector<string> r;
  copy(rs.begin(), rs.end(), back_inserter(r));
  return r;
}

vector<string>
incline_mgr::create_trigger_all(bool drop_if_exists) const
{
  vector<string> r;
  vector<string> src_tables(get_src_tables());
  
  for (vector<string>::const_iterator sti = src_tables.begin();
       sti != src_tables.end();
       ++sti) {
    if (drop_if_exists) {
      r.push_back(drop_trigger_of(*sti, "INSERT", true));
    }
    incline_util::push_back(r, insert_trigger_of(*sti));
    if (drop_if_exists) {
      r.push_back(drop_trigger_of(*sti, "UPDATE", true));
    }
    incline_util::push_back(r, update_trigger_of(*sti));
    if (drop_if_exists) {
      r.push_back(drop_trigger_of(*sti, "DELETE", true));
    }
    incline_util::push_back(r, delete_trigger_of(*sti));
  }
  
  return r;
}

vector<string>
incline_mgr::drop_trigger_all(bool drop_if_exists) const
{
  vector<string> r;
  vector<string> src_tables(get_src_tables());
  
  for (vector<string>::const_iterator sti = src_tables.begin();
       sti != src_tables.end();
       ++sti) {
    r.push_back(drop_trigger_of(*sti, "INSERT", drop_if_exists));
    r.push_back(drop_trigger_of(*sti, "UPDATE", drop_if_exists));
    r.push_back(drop_trigger_of(*sti, "DELETE", drop_if_exists));
  }
  
  return r;
}

vector<string>
incline_mgr::insert_trigger_of(const string& src_table) const
{
  return driver_->insert_trigger_of(src_table);
}

vector<string>
incline_mgr::update_trigger_of(const string& src_table) const
{
  return driver_->update_trigger_of(src_table);
}

vector<string>
incline_mgr::delete_trigger_of(const string& src_table) const
{
  return driver_->delete_trigger_of(src_table);
}

string
incline_mgr::drop_trigger_of(const string& src_table, const string& event,
			     bool if_exists) const
{
  return string("DROP TRIGGER ") + (if_exists ? "IF EXISTS " : "")
    + _build_trigger_name(src_table, event);
}

vector<string>
incline_mgr::build_trigger_stmt(const string& src_table, const string& event,
				const vector<string>& body) const
{
  if (body.size() == 0) {
    return vector<string>();
  }
  std::string r = "CREATE TRIGGER " + _build_trigger_name(src_table, event)
    + ' ' + trigger_time_ + ' ' + event + " ON " + src_table
    + " FOR EACH ROW BEGIN\n";
  for (vector<string>::const_iterator bi = body.begin();
       bi != body.end();
       ++bi) {
    r += string("  ") + *bi + ";\n";
  }
  r += "END";
  return incline_util::vectorize(r);
}

string
incline_mgr::_build_trigger_name(const string& src_table, const string& event)
  const
{
  return "_INCLINE_" + src_table + "_" + event;
}
