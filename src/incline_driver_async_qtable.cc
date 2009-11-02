extern "C" {
#include <sys/uio.h>
#include <unistd.h>
}
#include "incline_dbms.h"
#include "incline_def_async_qtable.h"
#include "incline_driver_async_qtable.h"
#include "incline_fw_async_qtable.h"
#include "incline_mgr.h"
#include "incline_util.h"
#include "start_thread.h"

using namespace std;

incline_def*
incline_driver_async_qtable::create_def() const
{
  return new incline_def_async_qtable();
}

vector<string>
incline_driver_async_qtable::create_table_all(bool if_not_exists,
					      incline_dbms* dbh) const
{
  vector<string> r;
  for (vector<incline_def*>::const_iterator di = mgr_->defs().begin();
       di != mgr_->defs().end();
       ++di) {
    r.push_back(create_table_of(*di, if_not_exists, dbh));
  }
  return r;
}

vector<string>
incline_driver_async_qtable::drop_table_all(bool if_exists) const
{
  vector<string> r;
  for (vector<incline_def*>::const_iterator di = mgr_->defs().begin();
       di != mgr_->defs().end();
       ++di) {
    r.push_back(drop_table_of(*di, if_exists));
  }
  return r;
}

string
incline_driver_async_qtable::create_table_of(const incline_def* _def,
					     bool if_not_exists,
					     incline_dbms* dbh) const
{
  const incline_def_async_qtable* def
    = dynamic_cast<const incline_def_async_qtable*>(_def);
  assert(def != NULL);
  return _create_table_of(def, def->queue_table(), if_not_exists, dbh);
}

string
incline_driver_async_qtable::drop_table_of(const incline_def* _def,
					   bool if_exists) const
{
  const incline_def_async_qtable* def
    = dynamic_cast<const incline_def_async_qtable*>(_def);
  assert(def != NULL);
  return string("DROP TABLE ") + (if_exists ? "IF EXISTS " : "")
    + def->queue_table();
}

void
incline_driver_async_qtable::run_forwarder(int poll_interval, int log_fd) const
{
  incline_fw_async_qtable::manager manager(this, poll_interval, log_fd);
  vector<pthread_t> threads;
  
  { // create and start forwarders
    const vector<incline_def*>& defs = mgr()->defs();
    for (vector<incline_def*>::const_iterator di = defs.begin();
	 di != defs.end();
	 ++di) {
      const incline_def_async_qtable* def
	= static_cast<const incline_def_async_qtable*>(*di);
      incline_dbms* dbh = incline_dbms::factory_->create();
      assert(dbh != NULL);
      threads.push_back(start_thread(new incline_fw_async_qtable(&manager, def,
								 dbh)));
    }
  }
  
  // loop
  while (! threads.empty()) {
    pthread_join(threads.back(), NULL);
    threads.pop_back();
  }
}

string
incline_driver_async_qtable::_create_table_of(const incline_def_async_qtable*
					      def,
					      const string& table_name,
					      bool if_not_exists,
					      incline_dbms* dbh) const
{
  vector<string> col_defs;
  for (map<string, string>::const_iterator ci = def->columns().begin();
       ci != def->columns().end();
       ++ci) {
    string::size_type dot_at = ci->first.find('.');
    assert(dot_at != string::npos);
    col_defs.push_back(ci->second + ' '
		       + dbh->get_column_def(ci->first.substr(0, dot_at),
					     ci->first.substr(dot_at + 1)));
  }
  return string("CREATE TABLE ") + (if_not_exists ? "IF NOT EXISTS " : "")
    + table_name + " (_iq_id " + incline_dbms::factory_->serial_column_type()
    + ",_iq_action CHAR(1) NOT NULL," + incline_util::join(',', col_defs)
    + ",PRIMARY KEY(_iq_id))" + incline_dbms::factory_->create_table_suffix();
}

void
incline_driver_async_qtable::do_build_enqueue_insert_sql(trigger_body& body,
							 const incline_def*
							 _def,
							 const string&
							 src_table,
							 action_t action,
							 const vector<string>*
							 cond)
  const
{
  const incline_def_async_qtable* def
    = dynamic_cast<const incline_def_async_qtable*>(_def);
  map<string, string> extra_columns;
  extra_columns["_iq_action"] = "'";
  extra_columns["_iq_action"].push_back((char)action);
  extra_columns["_iq_action"].push_back('\'');
  incline_driver_standalone::_build_insert_from_def(body, def,
						    def->queue_table(),
						    src_table, act_insert, cond,
						    &extra_columns);
}

void
incline_driver_async_qtable::do_build_enqueue_delete_sql(trigger_body& body,
							 const incline_def*
							 _def,
							 const string&
							 src_table,
							 const vector<string>*
							 _cond)
  const
{
  const incline_def_async_qtable* def
    = dynamic_cast<const incline_def_async_qtable*>(_def);
  map<string, string> pk_columns;
  for (map<string, string>::const_iterator pi = def->pk_columns().begin();
       pi != def->pk_columns().end();
       ++pi) {
    string src = pi->first;
    if (incline_def::table_of_column(pi->first) == src_table) {
      src = "OLD" + src.substr(src_table.size());
    }
    pk_columns[src] = pi->second;
  }
  vector<string> tables;
  for (vector<string>::const_iterator si = def->source().begin();
       si != def->source().end();
       ++si) {
    if (*si != src_table && def->is_master_of(*si)) {
      tables.push_back(*si);
    }
  }
  vector<string> cond = def->build_merge_cond(src_table, "OLD", true);
  if (_cond != NULL) {
    incline_util::push_back(cond, *_cond);
  }
  string sql = "INSERT INTO " + def->queue_table() + " ("
    + incline_util::join(',', incline_util::filter("%2", pk_columns))
    + ",_iq_action) SELECT "
    + incline_util::join(',', incline_util::filter("%1", pk_columns))
    + ",'D'";
  if (! tables.empty()) {
    sql += " FROM " + incline_util::join(" INNER JOIN ", tables);
  }
  if (! cond.empty()){
    sql += " WHERE " + incline_util::join(" AND ", cond);
  }
  body.stmt.push_back(sql);
}
