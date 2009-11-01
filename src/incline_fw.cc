extern "C" {
#include <sys/uio.h>
#include <unistd.h>
}
#include "incline_dbms.h"
#include "incline_def_async_qtable.h"
#include "incline_driver_async_qtable.h"
#include "incline_fw.h"
#include "incline_util.h"

using namespace std;

void
incline_fw::manager::log_sql(const incline_dbms* dbms, const string& sql)
{
  if (log_fd_ != -1) {
    struct iovec vec[3];
    char buf[1024];
    snprintf(buf, sizeof(buf), "%s:%hu:", dbms->host().c_str(), dbms->port());
    vec[0].iov_base = buf;
    vec[0].iov_len = strlen(buf);
    vec[1].iov_base = const_cast<char*>(sql.c_str());
    vec[1].iov_len = sql.size();
    vec[2].iov_base = const_cast<char*>(";\n");
    vec[2].iov_len = 2;
    writev(log_fd_, vec, sizeof(vec) / sizeof(vec[0]));
  }
}

incline_fw::incline_fw(manager* mgr, const incline_def_async_qtable* def,
		       incline_dbms* dbh)
  : mgr_(mgr), def_(def), dbh_(dbh),
    dest_pk_columns_(incline_util::filter("%2", def->pk_columns()))
{
  // fetch query
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
  { // insert / update query
    vector<string> dest_cols(incline_util::filter("%2", def->pk_columns()));
    incline_util::push_back(dest_cols,
			    incline_util::filter("%2", def->npk_columns()));
    insert_row_query_base_ =
      (incline_dbms::factory_->has_replace_into()
       ? "REPLACE INTO " : "INSERT INTO ")
      + def->destination() + " ("
      + incline_util::join(',', dest_cols) + ") VALUES ";
  }
  // delete query
  delete_row_query_base_ = "DELETE FROM " + def->destination() + " WHERE ";
}

incline_fw::~incline_fw()
{
  delete dbh_;
}

void*
incline_fw::run()
{
  string extra_cond, last_id;
  
  while (! mgr_->driver()->should_exit_loop()) {
    try {
      vector<string> iq_ids;
      vector<vector<string> > delete_pks, insert_rows;
      { // fetch rows
	string cond = do_get_extra_cond();
	if (cond != extra_cond) {
	  extra_cond = cond;
	  last_id.clear();
	}
	if (! last_id.empty()) {
	  if (! cond.empty()) {
	    cond += " AND ";
	  }
	  cond += "_iq_id>" + last_id;
	}
	fetch_rows(cond, iq_ids, delete_pks, insert_rows);
      }
      // sleep and retry if no data
      if (iq_ids.empty()) {
	sleep(mgr()->poll_interval());
	continue;
      }
      if (! extra_cond.empty()) {
	last_id = iq_ids.back();
      }
      // update and remove from queue if successful
      if (do_update_rows(delete_pks, insert_rows)) {
	do_post_commit(iq_ids);
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
incline_fw::do_update_rows(const vector<vector<string> >& delete_pks,
			   const vector<vector<string> >& insert_rows)
{
  // the order: DELETE -> INSERT is a requirement, see above
  if (! delete_pks.empty()) {
    this->delete_rows(dbh_, delete_pks);
  }
  if (! insert_rows.empty()) {
    this->insert_rows(dbh_, insert_rows);
  }
  return true;
}

string
incline_fw::do_get_extra_cond()
{
  return string();
}

void
incline_fw::do_post_commit(const vector<string>& iq_ids)
{
}

// TODO should use vector<shared_ptr<vector<string> > >
void
incline_fw::fetch_rows(const string& cond, vector<string>& iq_ids, vector<vector<string> >& delete_pks, vector<vector<string> >& insert_rows)
{
  vector<pair<char, vector<string> > > rows;
  
  iq_ids.clear();
  delete_pks.clear();
  insert_rows.clear();
  
  { // load rows
    string query = fetch_query_base_;
    if (! cond.empty()) {
      query += " WHERE " + cond;
    }
    query += " ORDER BY _iq_id LIMIT 50";
    vector<vector<incline_dbms::value_t> > res;
    dbh_->query(res, query);
    for (vector<vector<incline_dbms::value_t> >::const_iterator ri
	   = res.begin();
	 ri != res.end();
	 ++ri) {
      iq_ids.push_back(*(*ri)[0]);
      incline_driver::action_t action
	= (incline_driver::action_t)(*(*ri)[1])[0];
      rows.push_back(make_pair(action, vector<string>()));
      for (size_t i = 0;
	   i < (action != incline_driver::act_delete
		? def_->columns().size() : def_->pk_columns().size());
	   ++i) {
	rows.back().second.push_back(*(*ri)[i + 2]);
      }
    }
  }
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
      // modif. of same pk exists
      goto ROW_CHECK_END;
    ROW_NOT_EQUAL:
      ;
    }
    // row with same pk not exists, register it
    switch (ri->first) {
    case incline_driver::act_insert:
    case incline_driver::act_update:
      insert_rows.push_back(ri->second);
      break;
    case incline_driver::act_delete:
      delete_pks.push_back(ri->second);
      break;
    default:
      assert(0);
    }
  ROW_CHECK_END:
    ;
  }
  // add pks of insert_rows to delete_pks, if no support for replace into
  // it works since delete is performed before insert (in a single transaction)
  if (! incline_dbms::factory_->has_replace_into()) {
    for (vector<vector<string> >::const_iterator ri
	   = insert_rows.begin();
	 ri != insert_rows.end();
	 ++ri) {
      delete_pks.push_back(*ri);
    }
  }
}

void
incline_fw::insert_rows(incline_dbms* dbh, const vector<vector<string> >& rows)
  const
{
  string sql = insert_row_query_base_ + '(';
  for (vector<vector<string> >::const_iterator ri = rows.begin();
       ri != rows.end();
       ++ri) {
    for (vector<string>::const_iterator ci = ri->begin();
	 ci != ri->end();
	 ++ci) {
      sql.push_back('\'');
      sql += dbh->escape(*ci);
      sql += "',";
    }
    sql.erase(sql.size() - 1);
    sql += "),(";
  }
  sql.erase(sql.size() - 2);
  mgr_->log_sql(dbh, sql);
  dbh->execute(sql);
}

void
incline_fw::delete_rows(incline_dbms* dbh,
			const vector<vector<string> >& pk_rows) const
{
  vector<string> conds;
  for (vector<vector<string> >::const_iterator pi = pk_rows.begin();
       pi != pk_rows.end();
       ++pi) {
    conds.push_back("(" + _build_pk_cond(dbh, dest_pk_columns_, *pi) + ')');
  }
  string sql = delete_row_query_base_ + incline_util::join(" OR ", conds);
  mgr_->log_sql(dbh, sql);
  dbh->execute(sql);
}

string
incline_fw::_build_pk_cond(incline_dbms* dbh, const vector<string>& colnames,
			   const vector<string>& rows)
{
  assert(colnames.size() <= rows.size());
  vector<string> cond;
  for (size_t i = 0; i < colnames.size(); ++i) {
    cond.push_back(colnames[i] + "='" + dbh->escape(rows[i]) + '\'');
  }
  return incline_util::join(" AND ", cond);
}
