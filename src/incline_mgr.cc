#include <algorithm>
#include <set>
#include "incline_dbms.h"
#include "incline_driver.h"
#include "incline_mgr.h"
#include "incline_util.h"

using namespace std;

incline_mgr::incline_mgr(incline_driver* d)
  : defs_(), driver_(d), trigger_time_("AFTER")
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
    auto_ptr<incline_def> def(driver_->create_def());
    string err = def->parse(*di);
    if (! err.empty()) {
      return err;
    }
    defs_.push_back(def.release());
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
    incline_util::push_back(r, insert_trigger_of(*sti));
    incline_util::push_back(r, update_trigger_of(*sti));
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
    incline_util::push_back(r, drop_trigger_of(*sti, "INSERT", drop_if_exists));
    incline_util::push_back(r, drop_trigger_of(*sti, "UPDATE", drop_if_exists));
    incline_util::push_back(r, drop_trigger_of(*sti, "DELETE", drop_if_exists));
  }
  
  return r;
}

vector<string>
incline_mgr::insert_trigger_of(const string& src_table) const
{
  incline_driver::trigger_body body;
  driver_->insert_trigger_of(body, src_table);
  return build_trigger_stmt(src_table, "INSERT", body);
}

vector<string>
incline_mgr::update_trigger_of(const string& src_table) const
{
  incline_driver::trigger_body body;
  driver_->update_trigger_of(body, src_table);
  return build_trigger_stmt(src_table, "UPDATE", body);
}

vector<string>
incline_mgr::delete_trigger_of(const string& src_table) const
{
  incline_driver::trigger_body body;
  driver_->delete_trigger_of(body, src_table);
  return build_trigger_stmt(src_table, "DELETE", body);
}

vector<string>
incline_mgr::drop_trigger_of(const string& src_table, const string& event,
			     bool if_exists) const
{
  return
    incline_dbms::factory_->drop_trigger(_build_trigger_name(src_table, event),
					 src_table, if_exists);
}

vector<string>
incline_mgr::build_trigger_stmt(const string& src_table, const string& event,
				const incline_driver::trigger_body& body) const
{
  if (body.stmt.empty()) {
    return vector<string>();
  }
  string funcbody;
  string indent = "  ";
  for (vector<string>::const_iterator bi = body.stmt.begin();
       bi != body.stmt.end();
       ++bi) {
    if (! bi->empty() && (*bi)[bi->size() - 1] == '\\') {
      funcbody += indent + bi->substr(0, bi->size() - 1) + "\n";
      indent += "  ";
    } else if (bi->substr(0, 4) == "ELSE") {
      funcbody += indent.substr(2) + *bi + "\n";
    } else {
      if (bi->substr(0, 4) == "END ") {
	indent = indent.substr(2);
      }
      funcbody += indent + *bi + ";\n";
    }
  }
  return
    incline_dbms::factory_->create_trigger(_build_trigger_name(src_table,
							       event),
					   event, trigger_time_, src_table,
					   body.var, funcbody);
}

string
incline_mgr::_build_trigger_name(const string& src_table, const string& event)
  const
{
  return "_INCLINE_" + src_table + "_" + event;
}
