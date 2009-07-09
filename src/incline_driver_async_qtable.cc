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


incline_driver_async_qtable::forwarder*
incline_driver_async_qtable::create_forwarder(incline_def* _def,
					      tmd::conn_t* dbh,
					      int poll_interval) const
{
  const incline_def_async_qtable* def
    = dynamic_cast<const incline_def_async_qtable*>(_def);
  assert(def != NULL);
  return new forwarder(this, def, dbh, poll_interval);
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
    + ")) ENGINE=InnoDB";
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
  string sql = string("INSERT INTO ") + def->queue_table() + " ("
    + incline_util::join(',', dest_cols.begin(), dest_cols.end()) + ") SELECT "
    + incline_util::join(',', src_cols.begin(), src_cols.end());
  if (! tables.empty()) {
    sql += string(" FROM ")
      + incline_util::join(" INNER JOIN ", tables.begin(), tables.end());
  }
  if (! cond.empty()) {
    sql += string(" WHERE ")
      + incline_util::join(" AND ", cond.begin(), cond.end());
  }
  sql += " ON DUPLICATE KEY UPDATE _iq_version=_iq_version+1";
  return incline_util::vectorize(sql);
}

incline_driver_async_qtable::forwarder::~forwarder()
{
  delete dbh_;
}

incline_driver_async_qtable::forwarder::forwarder(const
						  incline_driver_async_qtable*
						  driver,
						  const
						  incline_def_async_qtable* def,
						  tmd::conn_t* dbh,
						  int poll_interval)
  : dbh_(dbh), dest_table_(def->destination()),
    queue_table_(def->queue_table()),
    temp_table_(string("_qt_") + def->queue_table()),
    src_tables_(def->source()), merge_cond_(def->build_merge_cond("", "")),
    poll_interval_(poll_interval)
{
  // build column names
  for (map<string, string>::const_iterator pi = def->pk_columns().begin();
       pi != def->pk_columns().end();
       ++pi) {
    dest_pk_columns_.push_back(pi->second);
    dest_columns_.push_back(pi->second);
    src_pk_columns_.push_back(pi->first);
    src_columns_.push_back(pi->first);
  }
  for (map<string, string>::const_iterator ni = def->npk_columns().begin();
       ni != def->npk_columns().end();
       ++ni) {
    dest_columns_.push_back(ni->second);
    src_columns_.push_back(ni->first);
  }
  // create temporary table
  tmd::execute(*dbh_,
	       driver->_create_table_of(def, temp_table_, true, false, *dbh_));
}

void incline_driver_async_qtable::forwarder::_run()
{
  while (1) {
    vector<string> pk_values;
    { // poll the queue table
      tmd::execute(*dbh_,
		   string("INSERT INTO ") + temp_table_ + " SELECT * FROM "
		   + queue_table_ + " LIMIT 1");
      string extra_cond = do_get_extra_cond();
      tmd::query_t res(*dbh_,
		       string("SELECT ") 
		       + incline_util::join(',', dest_pk_columns_.begin(),
					    dest_pk_columns_.end())
		       + " FROM " + temp_table_
		       + (extra_cond.empty()
			  ? string()
			  : string(" WHERE ") + extra_cond));
      if (res.fetch().eof()) {
	sleep(poll_interval_);
	continue;
      }
      for (size_t i = 0; i < dest_pk_columns_.size(); ++i) {
	pk_values.push_back(res.field(i));
      }
    }
    // we have a row in queue, handle it
    vector<string> scond(merge_cond_);
    for (size_t i = 0; i < src_pk_columns_.size(); ++i) {
      scond.push_back(src_pk_columns_[i] + "='"
		      + tmd::escape(*dbh_, pk_values[i]) + '\'');
    }
    tmd::query_t res(*dbh_,
		     string("SELECT ")
		     + incline_util::join(',', src_columns_.begin(),
					  src_columns_.end())
		     + " FROM "
		     + incline_util::join(" INNER JOIN ", src_tables_.begin(), 
					  src_tables_.end())
		     + " WHERE "
		     + incline_util::join(" AND ", scond.begin(), scond.end()));
    if (! res.fetch().eof()) {
      do_replace_row(res);
    } else {
      do_delete_row(pk_values);
    }
    { // remove from queue
      vector<string> dcond;
      for (vector<string>::const_iterator pi = dest_pk_columns_.begin();
	   pi != dest_pk_columns_.end();
	   ++pi) {
	dcond.push_back(queue_table_ + '.' + *pi + '=' + temp_table_ + '.'
			+ *pi);
      }
      tmd::execute(*dbh_,
		   string("DELETE FROM ") + queue_table_
		   + " WHERE EXISTS (SELECT * FROM " + temp_table_ + " WHERE "
		   + incline_util::join(" AND ", dcond.begin(), dcond.end())
		   + ')');
      tmd::execute(*dbh_, string("DELETE FROM ") + temp_table_);
    }
  }
}

void
incline_driver_async_qtable::forwarder::do_replace_row(tmd::query_t& res)
{
  replace_row(*dbh_, res);
}

void
incline_driver_async_qtable::forwarder::do_delete_row(const vector<string>&
						      pk_values)
{
  delete_row(*dbh_, pk_values);
}

string
incline_driver_async_qtable::forwarder::do_get_extra_cond()
{
  return string();
}

void*
incline_driver_async_qtable::forwarder::run(void* _fw)
{
  forwarder* fw = static_cast<forwarder*>(_fw);
  fw->_run();
  return NULL;
}

void
incline_driver_async_qtable::forwarder::replace_row(tmd::conn_t& dbh,
						    tmd::query_t& res)
{
  vector<string> values;
  for (size_t i = 0; i < src_columns_.size(); ++i) {
    values.push_back(string("'") + tmd::escape(dbh, res.field(i)) + '\'');
  }
  tmd::execute(dbh,
	       string("REPLACE INTO ") + dest_table_ + " ("
	       + incline_util::join(',', dest_columns_.begin(),
				    dest_columns_.end())
	       + ") VALUES ("
	       + incline_util::join(',', values.begin(), values.end())
	       + ')');
}

void
incline_driver_async_qtable::forwarder::delete_row(tmd::conn_t& dbh,
						   const vector<string>&
						   pk_values)
{
  vector<string> dcond;
  for (size_t i = 0; i < dest_pk_columns_.size(); ++i) {
    dcond.push_back(dest_pk_columns_[i] + "='"
		    + tmd::escape(dbh, pk_values[i]) + '\'');
  }
  tmd::execute(dbh,
	       string("DELETE FROM ") + dest_table_ + " WHERE "
	       + incline_util::join(" AND ", dcond.begin(), dcond.end()));
}
