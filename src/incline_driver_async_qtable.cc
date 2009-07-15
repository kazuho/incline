#include "start_thread.h"
#include "tmd.h"
#include "incline_def_async_qtable.h"
#include "incline_driver_async_qtable.h"
#include "incline_mgr.h"
#include "incline_util.h"

using namespace std;

incline_def*
incline_driver_async_qtable::create_def() const
{
  return new incline_def_async_qtable();
}

vector<string>
incline_driver_async_qtable::create_table_all(bool if_not_exists,
					      tmd::conn_t& dbh) const
{
  vector<string> r;
  for (std::vector<incline_def*>::const_iterator di = mgr_->defs().begin();
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
  for (std::vector<incline_def*>::const_iterator di = mgr_->defs().begin();
       di != mgr_->defs().end();
       ++di) {
    r.push_back(drop_table_of(*di, if_exists));
  }
  return r;
}

string
incline_driver_async_qtable::create_table_of(const incline_def* _def,
					     bool if_not_exists,
					     tmd::conn_t& dbh) const
{
  const incline_def_async_qtable* def
    = dynamic_cast<const incline_def_async_qtable*>(_def);
  assert(def != NULL);
  return _create_table_of(def, def->queue_table(), false, true, dbh);
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

string
incline_driver_async_qtable::_create_table_of(const incline_def_async_qtable*
					      def,
					      const std::string& table_name,
					      bool temporary,
					      bool if_not_exists,
					      tmd::conn_t& dbh) const
{
  vector<string> col_defs, pk_cols;
  for (map<string, string>::const_iterator pi = def->pk_columns().begin();
       pi != def->pk_columns().end();
       ++pi) {
    string table_name = incline_def::table_of_column(pi->first);
    string column_name = pi->first.substr(table_name.size() + 1);
    tmd::query_t res(dbh,
		     "SELECT COLUMN_TYPE,CHARACTER_SET_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA='%s' AND TABLE_NAME='%s' AND COLUMN_NAME='%s'",
		     tmd::escape(dbh, mgr_->db_name()).c_str(),
		     tmd::escape(dbh, table_name).c_str(),
		     tmd::escape(dbh, column_name).c_str());
    if (res.fetch().eof()) {
      // TODO throw an exception instead
      cerr << "failed to obtain column definition of: " << pi->first << endl;
      exit(4);
    }
    col_defs.push_back(pi->second + ' ' + res.field(0));
    if (res.field(1) != NULL) {
      col_defs.back() += string(" CHARSET ") + res.field(1);
    }
    pk_cols.push_back(pi->second);
  }
  return string("CREATE ") + (temporary ? "TEMPORARY " : "") + "TABLE "
    + (if_not_exists ? "IF NOT EXISTS " : "") + table_name + " ("
    + incline_util::join(',', col_defs.begin(), col_defs.end())
    + ",_iq_version INT UNSIGNED NOT NULL DEFAULT 0,PRIMARY KEY("
    + incline_util::join(',', pk_cols.begin(), pk_cols.end())
    + ")) ENGINE=" + (temporary ? "MEMORY" : "InnoDB");
}

vector<string>
incline_driver_async_qtable::do_build_enqueue_sql(const incline_def* _def,
						  const map<string, string>&
						  pk_columns,
						  const vector<string>& tables,
						  const vector<string>& cond)
  const
{
  const incline_def_async_qtable* def
    = dynamic_cast<const incline_def_async_qtable*>(_def);
  vector<string> src_cols, dest_cols;
  for (map<string, string>::const_iterator pi = pk_columns.begin();
       pi != pk_columns.end();
       ++pi) {
    src_cols.push_back(pi->first);
    dest_cols.push_back(pi->second);
  }
  string sql = "INSERT INTO " + def->queue_table() + " ("
    + incline_util::join(',', dest_cols.begin(), dest_cols.end()) + ") SELECT "
    + incline_util::join(',', src_cols.begin(), src_cols.end());
  if (! tables.empty()) {
    sql += " FROM "
      + incline_util::join(" INNER JOIN ", tables.begin(), tables.end());
  }
  if (! cond.empty()) {
    sql += " WHERE "
      + incline_util::join(" AND ", cond.begin(), cond.end());
  }
  sql += " ON DUPLICATE KEY UPDATE _iq_version=_iq_version+1";
  return incline_util::vectorize(sql);
}

incline_driver_async_qtable::forwarder::forwarder(forwarder_mgr* mgr,
						  const
						  incline_def_async_qtable* def,
						  tmd::conn_t* dbh,
						  int poll_interval)
  : mgr_(mgr), def_(def), dbh_(dbh), poll_interval_(poll_interval)
{
  vector<string> dest_cols, src_cols,
    fetch_src_cond(def->build_merge_cond("", "")), del_queue_cond;
  string temp_table = "_qt_" + def->queue_table();
  // build column names
  for (map<string, string>::const_iterator pi = def->pk_columns().begin();
       pi != def->pk_columns().end();
       ++pi) {
    dest_pk_columns_.push_back(pi->second);
    dest_cols.push_back(pi->second);
    src_cols.push_back(pi->first);
    fetch_src_cond.push_back(pi->first + '=' + temp_table + '.' + pi->second);
    del_queue_cond.push_back(def->queue_table() + '.' + pi->second + '='
			     + temp_table + '.' + pi->second);
    
  }
  for (map<string, string>::const_iterator ni = def->npk_columns().begin();
       ni != def->npk_columns().end();
       ++ni) {
    dest_cols.push_back(ni->second);
    src_cols.push_back(ni->first);
  }
  // create temporary table
  tmd::execute(*dbh_,
	       mgr_->driver()->_create_table_of(def, temp_table, true, false,
						*dbh_));
  // build read queries
  copy_to_temp_query_base_ =
    "INSERT INTO " + temp_table + " SELECT * FROM " + def->queue_table();
  fetch_pk_query_ =
    "SELECT "
    + incline_util::join(',', dest_pk_columns_.begin(), dest_pk_columns_.end())
    + " FROM " + temp_table;
  fetch_src_query_ =
    "SELECT "
    + incline_util::join(',', src_cols.begin(), src_cols.end())
    + " FROM " + temp_table + " INNER JOIN "
    + incline_util::join(" INNER JOIN ", def->source().begin(),
			 def->source().end())
    + " WHERE "
    + incline_util::join(" AND ", fetch_src_cond.begin(), fetch_src_cond.end());
  delete_queue_query_ =
    "DELETE FROM " + def->queue_table() + " WHERE EXISTS (SELECT * FROM "
    + temp_table + " WHERE "
    + incline_util::join(" AND ", del_queue_cond.begin(), del_queue_cond.end())
    + " AND " + def->queue_table() + "._iq_version=" + temp_table
    + "._iq_version)";
  delete_temp_query_ = "DELETE FROM " + temp_table;
  // build write queries
  replace_row_query_base_ =
    "REPLACE INTO " + def->destination() + " ("
    + incline_util::join(',', dest_cols.begin(), dest_cols.end())
    + ") VALUES ";
  delete_row_query_base_ = "DELETE FROM " + def->destination() + " WHERE ";
}

incline_driver_async_qtable::forwarder::~forwarder()
{
  delete dbh_;
}

void* incline_driver_async_qtable::forwarder::run()
{
  while (1) {
    vector<string> pk_values;
    { // poll the queue table
      string extra_cond = do_get_extra_cond();
      string query = copy_to_temp_query_base_;
      if (! extra_cond.empty()) {
	query += " WHERE " + extra_cond;
      }
      query += " LIMIT 1";
      tmd::execute(*dbh_, query);
      if (tmd::affected_rows(*dbh_) == 0) {
	sleep(poll_interval_);
	continue;
      }
    }
    { // fetch the pks
      tmd::query_t res(*dbh_, fetch_pk_query_);
      assert(! res.fetch().eof());
      for (size_t i = 0; i < res.num_fields(); ++i) {
	pk_values.push_back(res.field(i));
      }
    }
    // we have a row in queue, handle it
    tmd::query_t res(*dbh_, fetch_src_query_);
    bool success;
    if (! res.fetch().eof()) {
      success = do_replace_row(res);
    } else {
      success = do_delete_row(pk_values);
    }
    // remove from queue
    if (success) {
      tmd::execute(*dbh_, delete_queue_query_);
    }
    tmd::execute(*dbh_, delete_temp_query_);
  }
  
  return NULL;
}

bool
incline_driver_async_qtable::forwarder::do_replace_row(tmd::query_t& res)
{
  replace_row(*dbh_, res);
  return true;
}

bool
incline_driver_async_qtable::forwarder::do_delete_row(const vector<string>&
						      pk_values)
{
  delete_row(*dbh_, pk_values);
  return true;
}

string
incline_driver_async_qtable::forwarder::do_get_extra_cond()
{
  return string();
}

void
incline_driver_async_qtable::forwarder::replace_row(tmd::conn_t& dbh,
						    tmd::query_t& res) const
{
  vector<string> values;
  for (size_t i = 0; i < res.num_fields(); ++i) {
    values.push_back("'" + tmd::escape(dbh, res.field(i)) + '\'');
  }
  tmd::execute(dbh, 
	       replace_row_query_base_ + '('
	       + incline_util::join(',', values.begin(), values.end())
	       + ')');
}

void
incline_driver_async_qtable::forwarder::delete_row(tmd::conn_t& dbh,
						   const vector<string>&
						   pk_values) const
{
  vector<string> dcond;
  for (size_t i = 0; i < dest_pk_columns_.size(); ++i) {
    dcond.push_back(dest_pk_columns_[i] + "='"
		    + tmd::escape(dbh, pk_values[i]) + '\'');
  }
  tmd::execute(dbh,
	       delete_row_query_base_
	       + incline_util::join(" AND ", dcond.begin(), dcond.end()));
}

void*
incline_driver_async_qtable::forwarder_mgr::run()
{
  vector<pthread_t> threads;
  
  { // create and start forwarders
    const vector<incline_def*>& defs = driver()->mgr()->defs();
    for (vector<incline_def*>::const_iterator di = defs.begin();
	 di != defs.end();
	 ++di) {
      const incline_def_async_qtable* def
	= dynamic_cast<const incline_def_async_qtable*>(*di);
      assert(def != NULL);
      threads.push_back(start_thread(do_create_forwarder(def)));
    }
  }
  
  // loop
  while (! threads.empty()) {
    pthread_join(threads.back(), NULL);
    threads.pop_back();
  }
  
  return NULL;
}

incline_driver_async_qtable::forwarder*
incline_driver_async_qtable::forwarder_mgr::do_create_forwarder(const incline_def_async_qtable* def)
{
  tmd::conn_t* dbh = (*connect_)(src_host_.c_str(), src_port_);
  assert(dbh != NULL);
  return new forwarder(this, def, dbh, poll_interval_);
}
