#include "incline_mgr.h"
#include "incline_dbms.h"
#include "incline_driver_standalone.h"
#include "incline_util.h"

using namespace std;

void
incline_driver_standalone::insert_trigger_of(trigger_body& body,
					     const string& src_table) const
{
  for (vector<incline_def*>::const_iterator di = mgr_->defs().begin();
       di != mgr_->defs().end();
       ++di) {
    const incline_def* def = *di;
    if (def->is_master_of(src_table)) {
      _build_insert_from_def(body, def, src_table, act_insert);
    }
  }
}

void
incline_driver_standalone::update_trigger_of(trigger_body& body,
					     const string& src_table) const
{
  for (vector<incline_def*>::const_iterator di = mgr_->defs().begin();
       di != mgr_->defs().end();
       ++di) {
    const incline_def* def = *di;
    if (def->is_dependent_of(src_table)) {
      if (def->is_master_of(src_table)) {
	if (! def->npk_columns().empty()) {
	  _build_insert_from_def(body, def, src_table, act_update);
	}
      } else {
	_build_insert_from_def(body, def, src_table, act_update);
      }
    }
  }
}

void
incline_driver_standalone::delete_trigger_of(trigger_body& body,
					     const string& src_table) const
{
  for (vector<incline_def*>::const_iterator di = mgr_->defs().begin();
       di != mgr_->defs().end();
       ++di) {
    const incline_def* def = *di;
    if (def->is_master_of(src_table)) {
      _build_delete_from_def(body, def, src_table);
    }
  }
}

void
incline_driver_standalone::_build_insert_from_def(trigger_body& body,
						  const incline_def* def,
						  const string& src_table,
						  action_t action,
						  const vector<string>* _cond)
  const
{
  _build_insert_from_def(body, def, def->destination(), src_table, action,
			 _cond, NULL);
}

void
incline_driver_standalone::_build_delete_from_def(trigger_body& body,
						  const incline_def* def,
						  const string& src_table,
						  const vector<string>& _cond)
  const 
{
  vector<string> cond(_cond);
  string sql;
  bool use_join = false;
  
  for (vector<pair<string, string> >::const_iterator mi = def->merge().begin();
       mi != def->merge().end();
       ++mi) {
    if (def->pk_columns().find(mi->first) == def->pk_columns().end()
	&& def->pk_columns().find(mi->second) == def->pk_columns().end()) {
      use_join = true;
      break;
    }
  }
  
  for (map<string, string>::const_iterator pi = def->pk_columns().begin();
       pi != def->pk_columns().end();
       ++pi) {
    if (incline_def::table_of_column(pi->first) == src_table) {
      cond.push_back(def->destination() + '.' + pi->second + "=OLD."
		     + pi->first.substr(src_table.size() + 1));
    }
  }
  if (use_join) {
    vector<string> using_list;
    for (vector<string>::const_iterator si = def->source().begin();
	 si != def->source().end();
	 ++si) {
      if (*si != src_table && def->is_master_of(*si)) {
	using_list.push_back(*si);
	for (map<string, string>::const_iterator pi = def->pk_columns().begin();
	     pi != def->pk_columns().end();
	     ++pi) {
	  if (incline_def::table_of_column(pi->first) == *si) {
	    cond.push_back(def->destination() + '.' + pi->second + '='
			   + pi->first);
	  }
	}
      }
    }
    incline_util::push_back(cond,
			    def->build_merge_cond(src_table, "OLD", true));
    sql = incline_dbms::factory_->delete_using(def->destination(), using_list)
      + " WHERE " + incline_util::join(" AND ", cond);
  } else {
    for (vector<pair<string, string> >::const_iterator mi
	   = def->merge().begin();
	 mi != def->merge().end();
	 ++mi) {
      string old_col, alias_col;
      if (incline_def::table_of_column(mi->first) == src_table) {
	old_col = mi->first;
	alias_col = mi->second;
      } else if (incline_def::table_of_column(mi->second) == src_table) {
	old_col = mi->second;
	alias_col = mi->first;
      }
      if (! old_col.empty()
	  && def->pk_columns().find(old_col) == def->pk_columns().end()) {
	map<string, string>::const_iterator ci
	  = def->pk_columns().find(alias_col);
	assert(ci != def->pk_columns().end());
	cond.push_back(ci->second + "=OLD."
		       + old_col.substr(src_table.size() + 1));
      }
    }
    sql = "DELETE FROM " + def->destination() + " WHERE "
      + incline_util::join(" AND ", cond);
  }
  
  body.stmt.push_back(sql);
}

void
incline_driver_standalone::_build_update_merge_from_def(trigger_body& body,
							const incline_def* def,
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
  string sql = "UPDATE " + def->destination() + " SET "
    + incline_util::join(',', set_expr) + " WHERE "
    + incline_util::join(" AND ", cond);
  body.stmt.push_back(sql);
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

void
incline_driver_standalone::_build_insert_from_def(trigger_body& body,
						  const incline_def *def,
						  const string& dest_table,
						  const string& src_table,
						  action_t action,
						  const vector<string>* _cond,
						  const map<string, string>*
						  extra_columns)
{
  if (action == act_update && def->npk_columns().empty()) {
    // nothing to do (pks should not be altered in an UPDATE statement)
    return;
  }
  
  vector<string> cond;
  if (_cond != NULL) {
    incline_util::push_back(cond, *_cond);
  }
  
  bool use_update = action == act_update
    && ! incline_dbms::factory_->has_replace_into();
  
  if (use_update && action == act_update && def->source().size() == 1) {
    // use: UPDATE ... WHERE ...
    string sql = "UPDATE " + dest_table + " SET "
      + incline_util::join(",",
			   incline_util::filter("%2=%1",
						incline_util::filter(incline_util::rewrite_prefix(def->source()[0] + '.', "NEW."), NULL, def->npk_columns())))
      + " WHERE "
      + incline_util::join(" AND ",
			   incline_util::filter("%2=%1",
						incline_util::filter(incline_util::rewrite_prefix(def->source()[0] + '.', "NEW."), NULL, def->pk_columns())));
    body.stmt.push_back(sql);
    return;
  }
  
  // use: SELECT (with FOR UPDATE if necessary)
  string query;
  
  { // build query
    vector<string> src_cols;
    for (map<string, string>::const_iterator ci = def->columns().begin();
	 ci != def->columns().end();
	 ++ci) {
      src_cols.push_back(incline_util::rewrite_prefix(src_table + '.', "NEW.")
			 (ci->first));
      if (use_update) {
	src_cols.back() += " AS _" + ci->second;
      }
    }
    query = "SELECT " + incline_util::join(',', src_cols);
    if (! use_update && extra_columns != NULL) {
      query += ","
	+ incline_util::join(',', incline_util::filter("%2", *extra_columns));
    }
    if (def->source().size() > 1) {
      vector<string> join_tables;
      for (vector<string>::const_iterator si = def->source().begin();
	   si != def->source().end();
	   ++si) {
	if (*si != src_table) {
	  join_tables.push_back(*si);
	}
      }
      query += " FROM " + incline_util::join(",", join_tables);
      incline_util::push_back(cond, def->build_merge_cond(src_table, "NEW"));
    }
    if (! cond.empty()) {
      query += " WHERE " + incline_util::join(" AND ", cond);
    }
    if (def->source().size() > 1) {
      query += " FOR UPDATE";
    }
  }

  if (! use_update) {
    // mysql
    string sql = (action == act_insert ? "INSERT INTO " : "REPLACE INTO ") + dest_table
      + " ("
      + incline_util::join(',', incline_util::filter("%2", def->columns()));
    if (extra_columns != NULL) {
      sql += ","
	+ incline_util::join(',', incline_util::filter("%1", *extra_columns));
    }
    sql += ") " + query;
    body.stmt.push_back(sql);
  } else {
    // postgresql
    string sql = "UPDATE " + dest_table + " SET "
      + incline_util::join(',', incline_util::filter("%2=srow._%2", def->npk_columns()));
    if (extra_columns != NULL) {
      sql += ","
	+ incline_util::join(',',
			     incline_util::filter("%1=%2", *extra_columns));
    }
    sql += " WHERE "
      + incline_util::join(" AND ",
			   incline_util::filter("%2=srow._%2",
						def->pk_columns()));
    body.var.insert(pair<string, string>("srow", "RECORD"));
    body.stmt.push_back("FOR srow IN " + query + " LOOP\\");
    body.stmt.push_back(sql);
    body.stmt.push_back("END LOOP");
  }
}
