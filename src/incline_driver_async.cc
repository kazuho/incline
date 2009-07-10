#include <assert.h>
#include "incline_def_async.h"
#include "incline_mgr.h"
#include "incline_driver_async.h"
#include "incline_util.h"

using namespace std;

incline_def*
incline_driver_async::create_def() const
{
  return new incline_def_async();
}

vector<string>
incline_driver_async::_build_insert_from_def(const incline_def* _def,
					     const string& src_table,
					     const string& command,
					     const vector<string>& cond) const
{
  const incline_def_async* def = dynamic_cast<const incline_def_async*>(_def);
  assert(def != NULL);
  vector<string> r;
  string de_col = def->direct_expr_column(src_table, "NEW");
  if (! de_col.empty()) {
    if (strncmp(de_col.c_str(), "NEW.", 4) == 0) {
      r.push_back("IF (" + do_build_direct_expr(de_col) + ") THEN\\");
      incline_util::push_back(r,
			      super::_build_insert_from_def(def, src_table,
							    command, cond),
			      "  ");
      r.push_back("ELSE\\");
      incline_util::push_back(r, _build_enqueue_sql(def, src_table, "NEW"),
			      "  ");
      r.push_back("END IF");
    } else {
      string direct_expr = do_build_direct_expr(de_col);
      vector<string> cond_and_dexpr(cond);
      cond_and_dexpr.push_back(direct_expr);
      incline_util::push_back(r,
			      super::_build_insert_from_def(def, src_table,
							    command,
							    cond_and_dexpr));
      cond_and_dexpr.pop_back();
      cond_and_dexpr.push_back("! (" +  direct_expr + ")");
      incline_util::push_back(r, 
			      _build_enqueue_sql(def, src_table, "NEW",
						 cond_and_dexpr));
    }
  } else {
    incline_util::push_back(r, _build_enqueue_sql(def, src_table, "NEW", cond));
  }
  return r;
}

vector<string>
incline_driver_async::_build_delete_from_def(const incline_def* _def,
					     const string& src_table,
					     const vector<string>& cond) const
{
  const incline_def_async* def = dynamic_cast<const incline_def_async*>(_def);
  assert(def != NULL);
  vector<string> r;
  string de_col = def->direct_expr_column(src_table, "OLD");
  if (! de_col.empty()) {
    if (strncmp(de_col.c_str(), "OLD.", 4) == 0) {
      r.push_back("IF (" + do_build_direct_expr(de_col) + ") THEN\\");
      incline_util::push_back(r,
			      super::_build_delete_from_def(def, src_table,
							    cond),
			      "  ");
      r.push_back("ELSE\\");
      incline_util::push_back(r,
			      _build_enqueue_sql(def, src_table, "OLD"),
			      "   ");
      r.push_back("END IF");
    } else {
      incline_util::push_back(r,
			      super::_build_delete_from_def(def, src_table,
							    cond));
      vector<string> cond_and_dexpr(cond);
      cond_and_dexpr.push_back("! (" + do_build_direct_expr(de_col) + ')');
      incline_util::push_back(r,
			      _build_enqueue_sql(def, src_table, "OLD",
						 cond_and_dexpr));
    }
  } else {
    incline_util::push_back(r, _build_enqueue_sql(def, src_table, "OLD", cond));
  }
  return r;
}

vector<string>
incline_driver_async::_build_update_merge_from_def(const incline_def* _def,
						   const string& src_table,
						   const vector<string>& cond)
  const
{
  const incline_def_async* def = dynamic_cast<const incline_def_async*>(_def);
  assert(def != NULL);
  vector<string> r;
  string de_col = def->direct_expr_column(src_table);
  if (! de_col.empty()) {
    vector<string> cond_and_dexpr(cond);
    cond_and_dexpr.push_back(do_build_direct_expr(de_col));
    incline_util::push_back(r, 
			    super::_build_update_merge_from_def(def, src_table,
								cond_and_dexpr)
			    );
    cond_and_dexpr.pop_back();
    de_col = def->direct_expr_column(src_table, src_table);
    
    cond_and_dexpr.push_back("! ("
			     + do_build_direct_expr(def->direct_expr_column(src_table, src_table))
			     + ')');
    incline_util::push_back(r, 
			    _build_enqueue_sql(def, src_table, "NEW",
					       cond_and_dexpr));
  } else {
    incline_util::push_back(r, _build_enqueue_sql(def, src_table, "NEW"));
  }
  return r;
}

vector<string>
incline_driver_async::_build_enqueue_sql(const incline_def_async* def,
					 const string& src_table,
					 const string& alias,
					 const vector<string>& _cond) const
{
  map<string, string> pk_columns;
  for (map<string, string>::const_iterator pi = def->pk_columns().begin();
       pi != def->pk_columns().end();
       ++pi) {
    string src = pi->first;
    if (incline_def::table_of_column(pi->first) == src_table) {
      src = alias + src.substr(src_table.size());
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
  vector<string> cond(_cond);
  incline_util::push_back(cond, def->build_merge_cond(src_table, alias, true));
  return do_build_enqueue_sql(def, pk_columns, tables, cond);
}

string
incline_driver_async::do_build_direct_expr(const string& column_expr)
  const
{
  // does not support mixed-mode unless overridden
  assert(0);
}
