#include "tmd.h"
#include "incline.h"
#include "incline_def_async_qtable.h"
#include "incline_driver_async_qtable.h"
#include "incline_mgr.h"

using namespace std;

string
incline_driver_async_qtable::create_table_of(const incline_def* _def,
					     tmd::conn_t& dbh) const
{
  const incline_def_async_qtable* def
    = dynamic_cast<const incline_def_async_qtable*>(_def);
  assert(def != NULL);
  
  vector<string> col_defs, pk_cols;
  for (map<string, string>::const_iterator pi = def->pk_columns().begin();
       pi != def->pk_columns().end();
       ++pi) {
    tmd::query_t res(dbh,
		     "SELECT COLUMN_TYPE,CHARACTER_SET_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA='%s' AND CONCAT(TABLE_NAME,'.',COLUMN_NAME)='%s'",
		     tmd::escape(dbh, mgr_->db_name()).c_str(),
		     tmd::escape(dbh, pi->first).c_str());
    if (res.fetch().eof()) {
      // TODO display the error
      assert(0);
    }
    col_defs.push_back(pi->second + ' ' + res.field(0));
    if (res.field(1) != NULL) {
      col_defs.back() += string(" CHARSET ") + res.field(1);
    }
    pk_cols.push_back(pi->second);
  }
  return "CREATE TABLE " + def->queue_table() + " ("
    + incline::join(',', col_defs.begin(), col_defs.end())
    + ",cq_version INT UNSIGNED NOT NULL,PRIMARY KEY("
    + incline::join(',', pk_cols.begin(), pk_cols.end())
    + ")) ENGINE=InnoDB";
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

vector<string>
incline_driver_async_qtable::do_build_enqueue_sql(const incline_def* def,
						  const map<string, string>&
						  pk_columns,
						  const vector<string>& tables,
						  const vector<string>& cond)
  const
{
  vector<string> src_cols, dest_cols;
  for (map<string, string>::const_iterator pi = pk_columns.begin();
       pi != pk_columns.end();
       ++pi) {
    src_cols.push_back(pi->first);
    dest_cols.push_back(pi->second);
  }
  string sql = string("INSERT INTO ") + def->destination() + " ("
    + incline::join(',', dest_cols.begin(), dest_cols.end()) + ") SELECT "
    + incline::join(',', src_cols.begin(), src_cols.end());
  if (! tables.empty()) {
    sql += string(" FROM ")
      + incline::join(" INNER JOIN ", tables.begin(), tables.end());
  }
  if (! cond.empty()) {
    sql += string(" WHERE ")
      + incline::join(" AND ", cond.begin(), cond.end());
  }
  sql += " ON DUPLICATE KEY UPDATE _cq_version=_cq_version+1";
  return incline::vectorize(sql);
}
