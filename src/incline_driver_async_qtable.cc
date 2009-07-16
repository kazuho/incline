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
    + incline_util::join(',', col_defs)
    + ",_iq_version INT UNSIGNED NOT NULL DEFAULT 0,PRIMARY KEY("
    + incline_util::join(',', pk_cols) + ")) ENGINE="
    + (temporary ? "MEMORY" : "InnoDB");
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
  string sql = "INSERT INTO " + def->queue_table() + " ("
    + incline_util::join(',', incline_util::filter("%2", pk_columns))
    + ") SELECT "
    + incline_util::join(',', incline_util::filter("%1", pk_columns));
  if (! tables.empty()) {
    sql += " FROM " + incline_util::join(" INNER JOIN ", tables);
  }
  if (! cond.empty()) {
    sql += " WHERE " + incline_util::join(" AND ", cond);
  }
  sql += " ON DUPLICATE KEY UPDATE _iq_version=_iq_version+1";
  return incline_util::vectorize(sql);
}

incline_driver_async_qtable::forwarder::forwarder(forwarder_mgr* mgr,
						  const
						  incline_def_async_qtable* def,
						  tmd::conn_t* dbh,
						  int poll_interval)
  : mgr_(mgr), def_(def), dbh_(dbh), poll_interval_(poll_interval),
    dest_pk_columns_(incline_util::filter("%2", def->pk_columns()))
{
  string temp_table = "_qt_" + def->queue_table();
  // create temporary table
  tmd::execute(*dbh_,
	       mgr_->driver()->_create_table_of(def, temp_table, true, false,
						*dbh_));
  // build read queries
  copy_to_temp_query_base_ =
    "INSERT INTO " + temp_table + " SELECT * FROM " + def->queue_table();
  fetch_pk_query_ =
    "SELECT " + incline_util::join(',', dest_pk_columns_) + " FROM "
    + temp_table;
  {
    vector<string> src_cols(incline_util::filter("%1", def->pk_columns()));
    incline_util::push_back(src_cols,
			    incline_util::filter("%1", def->npk_columns()));
    vector<string> cond(def->build_merge_cond("", ""));
    incline_util::push_back(cond, 
			    incline_util::filter(("%1=" + temp_table + ".%2")
						 .c_str(),
						 def->pk_columns()));
    fetch_src_query_ =
      "SELECT "
      + incline_util::join(',', src_cols) + " FROM " + temp_table +
      " INNER JOIN " + incline_util::join(" INNER JOIN ", def->source())
      + " WHERE " + incline_util::join(" AND ", cond);
  }
  delete_queue_query_ =
    "DELETE FROM " + def->queue_table() + " WHERE EXISTS (SELECT * FROM "
    + temp_table + " WHERE "
    + incline_util::join(" AND ",
			 incline_util::filter((def->queue_table() + ".%2="
					       + temp_table + ".%2").c_str(),
					      def->pk_columns()))
    + " AND " + def->queue_table() + "._iq_version=" + temp_table
    + "._iq_version)";
  delete_temp_query_ = "DELETE FROM " + temp_table;
  // build write queries
  {
    vector<string> dest_cols(incline_util::filter("%2", def->pk_columns()));
    incline_util::push_back(dest_cols,
			    incline_util::filter("%2", def->npk_columns()));
    replace_row_query_base_ =
      "REPLACE INTO " + def->destination() + " ("
      + incline_util::join(',', dest_cols) + ") VALUES ";
  }
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
    vector<vector<string> > replace_rows;
    size_t num_rows;
    { // poll the queue table
      string extra_cond = do_get_extra_cond();
      string query = copy_to_temp_query_base_;
      if (! extra_cond.empty()) {
	query += " WHERE " + extra_cond;
      }
      query += " LIMIT 10";
      tmd::execute(*dbh_, query);
      num_rows = tmd::affected_rows(*dbh_);
      if (num_rows == 0) {
	sleep(poll_interval_);
	continue;
      }
    }
    // fetch rows to replace
    for (tmd::query_t res(*dbh_, fetch_src_query_);
	 ! res.fetch().eof();
	 ) {
      replace_rows.push_back(vector<string>());
      for (size_t i = 0; i < res.num_fields(); ++i) {
	replace_rows.back().push_back(res.field(i));
      }
    }
    if (! replace_rows.empty()) {
      if (! do_replace_rows(replace_rows)) {
	goto FAIL;
      }
    }
    // remove rows if we need to
    if (replace_rows.size() != num_rows) {
      vector<vector<string> > delete_pks;
      for (tmd::query_t res(*dbh_, fetch_pk_query_);
	   ! res.fetch().eof();
	   ) {
	for (vector<vector<string> >::const_iterator ri = replace_rows.begin();
	     ri != replace_rows.end();
	     ++ri) {
	  for (size_t i = 0; i < res.num_fields(); ++i) {
	    if ((*ri)[i] != res.field(i)) {
	      goto ROW_NOT_EQUAL;
	    }
	  }
	  goto NEXT_ROW;
	ROW_NOT_EQUAL:
	  ;
	}
	delete_pks.push_back(vector<string>());
	for (size_t i = 0; i < res.num_fields(); ++i) {
	  delete_pks.back().push_back(res.field(i));
	}
      NEXT_ROW:
	;
      }
      if (! do_delete_rows(delete_pks)) {
	goto FAIL;
      }
    }
    // remove from queue
    tmd::execute(*dbh_, delete_queue_query_);
  FAIL:
    tmd::execute(*dbh_, delete_temp_query_);
  }
  
  return NULL;
}

bool
incline_driver_async_qtable::forwarder::do_replace_rows(const vector<vector<string> >& rows)
{
  replace_rows(*dbh_, to_ptr_rows(rows));
  return true;
}

bool
incline_driver_async_qtable::forwarder::do_delete_rows(const vector<vector<string> >& pk_rows)
{
  delete_rows(*dbh_, to_ptr_rows(pk_rows));
  return true;
}

string
incline_driver_async_qtable::forwarder::do_get_extra_cond()
{
  return string();
}

void
incline_driver_async_qtable::forwarder::replace_rows(tmd::conn_t& dbh,
						     const vector<const vector<string>*>& rows) const
{
  string sql = replace_row_query_base_ + '(';
  for (vector<const vector<string>*>::const_iterator ri = rows.begin();
       ri != rows.end();
       ++ri) {
    for (vector<string>::const_iterator ci = (*ri)->begin();
	 ci != (*ri)->end();
	 ++ci) {
      sql.push_back('\'');
      sql += tmd::escape(dbh, *ci);
      sql += "',";
    }
    sql.erase(sql.size() - 1);
    sql += "),(";
  }
  sql.erase(sql.size() - 2);
  tmd::execute(dbh,  sql);
}

void
incline_driver_async_qtable::forwarder::delete_rows(tmd::conn_t& dbh,
						    const vector<const vector<string>*>& pk_rows) const
{
  string sql = delete_row_query_base_;
  for (vector<const vector<string>*>::const_iterator ri = pk_rows.begin();
       ri != pk_rows.end();
       ++ri) {
    vector<string> dcond;
    for (size_t i = 0; i < (*ri)->size(); ++i) {
      dcond.push_back(dest_pk_columns_[i] + "='"
		      + tmd::escape(dbh, (**ri)[i]) + '\'');
    }
    if (ri != pk_rows.begin()) {
      sql += " OR ";
    }
    sql.push_back('(');
    sql += incline_util::join(" AND ", dcond);
    sql.push_back(')');
  }
  tmd::execute(dbh, sql);
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
