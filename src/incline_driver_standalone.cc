#include "incline_mgr.h"
#include "incline_driver_standalone.h"
#include "incline_util.h"

using namespace std;

vector<string>
incline_driver_standalone::insert_trigger_of(const string& src_table) const
{
  vector<string> body;
  for (vector<incline_def*>::const_iterator di = mgr_->defs().begin();
       di != mgr_->defs().end();
       ++di) {
    const incline_def* def = *di;
    if (def->is_master_of(src_table)) {
      incline_util::push_back(body,
			      _build_insert_from_def(def, src_table, "INSERT"));
    }
  }
  return mgr_->build_trigger_stmt(src_table, "INSERT", body);
}

vector<string>
incline_driver_standalone::update_trigger_of(const string& src_table) const
{
  vector<string> body;
  for (vector<incline_def*>::const_iterator di = mgr_->defs().begin();
       di != mgr_->defs().end();
       ++di) {
    const incline_def* def = *di;
    if (def->is_dependent_of(src_table)) {
      if (def->is_master_of(src_table)) {
	incline_util::push_back(body,
				_build_insert_from_def(def, src_table,
						       "REPLACE"));
      } else {
	incline_util::push_back(body,
				_build_update_merge_from_def(def, src_table));
      }
    }
  }
  return mgr_->build_trigger_stmt(src_table, "UPDATE", body);
}

vector<string>
incline_driver_standalone::delete_trigger_of(const string& src_table) const
{
  vector<string> body;
  for (vector<incline_def*>::const_iterator di = mgr_->defs().begin();
       di != mgr_->defs().end();
       ++di) {
    const incline_def* def = *di;
    if (def->is_master_of(src_table)) {
      incline_util::push_back(body, _build_delete_from_def(def, src_table));
    }
  }
  return mgr_->build_trigger_stmt(src_table, "DELETE", body);
}

vector<string>
incline_driver_standalone::_build_insert_from_def(const incline_def* def,
						  const string& src_table,
						  const string& command,
						  const vector<string>& _cond)
  const
{
  vector<string> cond(_cond);
  vector<string> src_cols, dest_cols;
  for (map<string, string>::const_iterator ci = def->columns().begin();
       ci != def->columns().end();
       ++ci) {
    src_cols.push_back(incline_def::table_of_column(ci->first) == src_table
		       ? string("NEW") + ci->first.substr(src_table.size())
		       : ci->first);
    dest_cols.push_back(ci->second);
  }
  string sql = command + " INTO " + def->destination() + "(" +
    incline_util::join(',', dest_cols.begin(), dest_cols.end()) + ") SELECT " +
    incline_util::join(',', src_cols.begin(), src_cols.end());
  if (def->source().size() > 1) {
    vector<string> join_tables;
    for (vector<string>::const_iterator si = def->source().begin();
	 si != def->source().end();
	 ++si) {
      if (*si != src_table) {
	join_tables.push_back(*si);
      }
    }
    sql += string(" FROM ")
      + incline_util::join(" INNER JOIN ", join_tables.begin(),
			   join_tables.end());
    incline_util::push_back(cond, def->build_merge_cond(src_table, "NEW"));
  }
  if (! cond.empty()) {
    sql += string(" WHERE ")
      + incline_util::join(" AND ", cond.begin(), cond.end());
  }
  return incline_util::vectorize(sql);
}

vector<string>
incline_driver_standalone::_build_delete_from_def(const incline_def* def,
						  const string& src_table,
						  const vector<string>& _cond)
  const 
{
  vector<string> cond(_cond);
  for (map<string, string>::const_iterator pi = def->pk_columns().begin();
       pi != def->pk_columns().end();
       ++pi) {
    if (incline_def::table_of_column(pi->first) == src_table) {
      cond.push_back(pi->second + "=OLD."
		     + pi->first.substr(src_table.size() + 1));
    }
  }
  string sql = string("DELETE FROM ") + def->destination() + " WHERE "
    + incline_util::join(" AND ", cond.begin(), cond.end());
  return incline_util::vectorize(sql);
}

vector<string>
incline_driver_standalone::_build_update_merge_from_def(const incline_def* def,
							const string& src_table,
							const vector<string>&
						       _cond) const
{
  vector<string> set_expr, cond(_cond);
  for (map<string, string>::const_iterator ci = def->columns().begin();
       ci != def->columns().end();
       ++ci) {
    if (incline_def::table_of_column(ci->first) == src_table) {
      set_expr.push_back(ci->second + "=NEW."
		     + ci->first.substr(src_table.size() + 1));
    }
  }
  incline_util::push_back(cond, _merge_cond_of(def, src_table));
  string sql = string("UPDATE ") + def->destination() + " SET "
    + incline_util::join(',', set_expr.begin(), set_expr.end()) + " WHERE "
    + incline_util::join(" AND ", cond.begin(), cond.end());
  return incline_util::vectorize(sql);
}

vector<string>
incline_driver_standalone::_merge_cond_of(const incline_def* def,
					  const string& src_table) const
{
  vector<string> cond;
  for (vector<pair<string, string> >::const_iterator mi = def->merge().begin();
       mi != def->merge().end();
       ++mi) {
    string src_col, cmp_col;
    if (incline_def::table_of_column(mi->first) == src_table) {
      src_col = mi->first;
      cmp_col = mi->second;
    } else if (incline_def::table_of_column(mi->second) == src_table) {
      src_col = mi->second;
      cmp_col = mi->first;
    } else {
      // do not know how to handle multiple dependent tables; sorry
      assert(0);
    }
    assert(def->columns().find(cmp_col) != def->columns().end());
    cond.push_back(def->destination() + '.'
		   + def->columns().find(cmp_col)->second + "=NEW."
		   + src_col.substr(src_table.size() + 1));
  }
  return cond;
}
