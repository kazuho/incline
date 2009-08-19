extern "C" {
#include <sys/uio.h>
#include <unistd.h>
}
#include "start_thread.h"
#include "incline_dbms.h"
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
					      incline_dbms* dbh) const
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

string
incline_driver_async_qtable::_create_table_of(const incline_def_async_qtable*
					      def,
					      const std::string& table_name,
					      bool if_not_exists,
					      incline_dbms* dbh) const
{
  vector<string> col_defs;
  for (map<string, string>::const_iterator ci = def->columns().begin();
       ci != def->columns().end();
       ++ci) {
    vector<vector<incline_dbms::value_t> > rows;
    dbh->query(rows,
	       "SELECT UPPER(COLUMN_TYPE),CHARACTER_SET_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA='%s' AND TABLE_NAME='%s' AND COLUMN_NAME='%s'",
	       dbh->escape(*incline_dbms::opt_database_).c_str(),
	       dbh->escape(def->destination()).c_str(),
	       dbh->escape(ci->second).c_str());
    if (rows.empty()) {
      // TODO throw an exception instead
      cerr << "failed to obtain column definition of: " << ci->first << endl;
      exit(4);
    }
    col_defs.push_back(ci->second + ' ' + *rows[0][0]);
    if (! rows[0][1].is_null()) {
      col_defs.back() += string(" CHARSET ") + *rows[0][1];
    }
    col_defs.back() += " NOT NULL";
  }
  return string("CREATE TABLE ") + (if_not_exists ? "IF NOT EXISTS " : "")
    + table_name
    + (" (_iq_id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,"
       " _iq_action CHAR(1) CHARACTER SET latin1 NOT NULL,")
    + incline_util::join(',', col_defs)
    + ",PRIMARY KEY (_iq_id)) ENGINE InnoDB";
}

vector<string>
incline_driver_async_qtable::do_build_enqueue_insert_sql(const incline_def*
							 _def,
							 const string&
							 src_table,
							 const string& command,
							 const vector<string>*
							 cond)
  const
{
  const incline_def_async_qtable* def
    = dynamic_cast<const incline_def_async_qtable*>(_def);
  map<string, string> extra_columns;
  extra_columns["_iq_action"] = "'R'";
  string sql
    = incline_driver_standalone::_build_insert_from_def(def, def->queue_table(),
							src_table, command,
							cond, &extra_columns);
  return incline_util::vectorize(sql);
}

vector<string>
incline_driver_async_qtable::do_build_enqueue_delete_sql(const incline_def*
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
  return incline_util::vectorize(sql);
}

incline_driver_async_qtable::forwarder::forwarder(forwarder_mgr* mgr,
						  const
						  incline_def_async_qtable* def,
						  incline_dbms* dbh,
						  int poll_interval)
  : mgr_(mgr), def_(def), dbh_(dbh), poll_interval_(poll_interval),
    dest_pk_columns_(incline_util::filter("%2", def->pk_columns()))
{
  fetch_query_base_ = "SELECT _iq_id,_iq_action,"
    + incline_util::join(',',
			 incline_util::filter("%2", def_->pk_columns()));
  if (! def_->npk_columns().empty()) {
    fetch_query_base_ += ','
      + incline_util::join(',',
			   incline_util::filter("%2",
						def_->npk_columns()));
  }
  fetch_query_base_ += " FROM " + def_->queue_table() + ' ';
  clear_queue_query_base_ = "DELETE FROM " + def_->queue_table()
    + " WHERE _iq_id IN ";
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
  string extra_cond, last_id;
  
  while (1) {
    try {
      vector<string> iq_ids;
      vector<pair<char, vector<string> > > rows;
      { // update fetch state
	string new_extra_cond = do_get_extra_cond();
	if (extra_cond != new_extra_cond) {
	  extra_cond = new_extra_cond;
	  last_id.clear();
	}
      }
      { // fetch data
	string query = fetch_query_base_;
	if (! extra_cond.empty()) {
	  // TODO create and use index shard_key,_iq_id
	  query += " WHERE " + extra_cond;
	  if (! last_id.empty()) {
	    query += " AND _iq_id>" + last_id;
	  }
	}
	query += " ORDER BY _iq_id LIMIT 50";
	// load rows
	vector<vector<incline_dbms::value_t> > res;
	dbh_->query(res, query);
	for (vector<vector<incline_dbms::value_t> >::const_iterator ri
	       = res.begin();
	     ri != res.end();
	     ++ri) {
	  iq_ids.push_back(*(*ri)[0]);
	  char action = (*(*ri)[1])[0];
	  rows.push_back(make_pair(action, vector<string>()));
	  for (size_t i = 0;
	       i < (action == 'R'
		    ? def_->columns().size() : def_->pk_columns().size());
	       ++i) {
	    rows.back().second.push_back(*(*ri)[i + 2]);
	  }
	}
      }
      // sleep and retry if no data
      if (rows.empty()) {
	sleep(poll_interval_);
	continue;
      }
      if (! extra_cond.empty()) {
	last_id = iq_ids.back();
      }
      vector<const vector<string>*> replace_rows, delete_pks;
      // fill replace_rows and delete_rows
      // compile error gcc 4.0.1 (mac) when using const_reverse_iter
      for (vector<pair<char, vector<string> > >::reverse_iterator ri
	     = rows.rbegin();
	   ri != rows.rend();
	   ++ri) {
	for (vector<pair<char, vector<string> > >::reverse_iterator ci
	       = rows.rbegin();
	     ci != ri;
	     ++ci) {
	  for (size_t i = 0; i < def_->pk_columns().size(); ++i) {
	    if (ri->second[i] != ci->second[i]) {
	      goto ROW_NOT_EQUAL;
	    }
	  }
	  goto EQ_ROW_FOUND;
	ROW_NOT_EQUAL:
	  ;
	}
	// row with same pk not exists, register it
	switch (ri->first) {
	case 'R': // replace
	  replace_rows.push_back(&ri->second);
	  break;
	case 'D': // delete
	  delete_pks.push_back(&ri->second);
	  break;
	default:
	  assert(0);
	}
      EQ_ROW_FOUND:
	;
      }
      // update and remove from queue if successful
      if (do_update_rows(replace_rows, delete_pks)) {
	dbh_->execute(clear_queue_query_base_ + '('
		      + incline_util::join(',', iq_ids) + ')');
      }
    } catch (incline_dbms::deadlock_error_t&) {
      // just retry
    } catch (incline_dbms::timeout_error_t&) {
      // just retry
    }
  }
  
  return NULL;
}

bool
incline_driver_async_qtable::forwarder::do_update_rows(const vector<const vector<string>*>& replace_rows, const vector<const vector<string>*>& delete_rows)
{
  if (! replace_rows.empty()) {
    this->replace_rows(dbh_, replace_rows);
  }
  if (! delete_rows.empty()) {
    this->delete_rows(dbh_, delete_rows);
  }
  return true;
}

string
incline_driver_async_qtable::forwarder::do_get_extra_cond()
{
  return string();
}

void
incline_driver_async_qtable::forwarder::replace_rows(incline_dbms* dbh,
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
      sql += dbh->escape(*ci);
      sql += "',";
    }
    sql.erase(sql.size() - 1);
    sql += "),(";
  }
  sql.erase(sql.size() - 2);
  mgr_->log_sql(sql);
  dbh->execute(sql);
}

void
incline_driver_async_qtable::forwarder::delete_rows(incline_dbms* dbh,
						    const vector<const vector<string>*>& pk_rows) const
{
  vector<string> conds;
  for (vector<const vector<string>*>::const_iterator pi = pk_rows.begin();
       pi != pk_rows.end();
       ++pi) {
    conds.push_back("(" + _build_pk_cond(dbh, dest_pk_columns_, **pi) + ')');
  }
  string sql = delete_row_query_base_ + incline_util::join(" OR ", conds);
  mgr_->log_sql(sql);
  dbh->execute(sql);
}

string
incline_driver_async_qtable::forwarder::_build_pk_cond(incline_dbms* dbh,
						       const vector<string>&
						       colnames,
						       const vector<string>&
						       rows)
{
  assert(colnames.size() == rows.size());
  vector<string> cond;
  for (size_t i = 0; i < rows.size(); ++i) {
    cond.push_back(colnames[i] + "='" + dbh->escape(rows[i]) + '\'');
  }
  return incline_util::join(" AND ", cond);
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

void
incline_driver_async_qtable::forwarder_mgr::log_sql(const string& sql)
{
  if (log_fd_ != -1) {
    struct iovec vec[2];
    vec[0].iov_base = const_cast<char*>(sql.c_str());
    vec[0].iov_len = sql.size();
    vec[1].iov_base = const_cast<char*>(";\n");
    vec[1].iov_len = 2;
    writev(log_fd_, vec, 2);
  }
}

incline_driver_async_qtable::forwarder*
incline_driver_async_qtable::forwarder_mgr::do_create_forwarder(const incline_def_async_qtable* def)
{
  incline_dbms* dbh = incline_dbms::factory_->create();
  assert(dbh != NULL);
  return new forwarder(this, def, dbh, poll_interval_);
}
