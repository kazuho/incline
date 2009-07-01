#include <assert.h>
#include "incline.h"
#include "incline_def_async.h"
#include "incline_mgr.h"
#include "incline_driver_async.h"

using namespace std;

vector<string>
incline_driver_async::_build_insert_from_def(const incline_def* _def,
					     const string& src_table,
					     const string& command,
					     const vector<string>& cond) const
{
  const incline_def_async* def = dynamic_cast<const incline_def_async*>(_def);
  assert(def != NULL);
  vector<string> r;
  if (! def->direct_expr_column().empty()) {
    if (incline_def::table_of_column(def->direct_expr_column()) == src_table) {
      std::string direct_expr
	= def->direct_expr("NEW" +
			   def->direct_expr_column().substr(src_table.size()));
      r.push_back("IF (" + direct_expr + ") THEN");
      incline::push_back(r,
			 super::_build_insert_from_def(def, src_table, command,
						       cond),
			 "  ");
      r.push_back("ELSE");
      incline::push_back(r, _build_enqueue_sql(def, src_table, "NEW"), "  ");
      r.push_back("END IF");
    } else {
      std::string direct_expr = def->direct_expr(def->direct_expr_column());
      vector<string> cond_and_dexpr(cond);
      cond_and_dexpr.push_back(direct_expr);
      incline::push_back(r, 
			 super::_build_insert_from_def(def, src_table, command,
						       cond_and_dexpr));
      cond_and_dexpr.pop_back();
      cond_and_dexpr.push_back("! (" +  direct_expr + ")");
      incline::push_back(r, 
			 _build_enqueue_sql(def, src_table, "NEW",
					    cond_and_dexpr));
    }
  } else {
    incline::push_back(r, _build_enqueue_sql(def, src_table, "NEW", cond));
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
  if (! def->direct_expr_column().empty()) {
    if (incline_def::table_of_column(def->direct_expr_column()) == src_table) {
      std::string direct_expr
	= def->direct_expr("OLD" +
			   def->direct_expr_column().substr(src_table.size()));
      r.push_back("IF (" + direct_expr + ") THEN");
      incline::push_back(r,
			 super::_build_delete_from_def(def, src_table, cond),
			 "  ");
      r.push_back("ELSE");
      incline::push_back(r,
			 _build_enqueue_sql(def, src_table, "OLD"),
			 "   ");
      r.push_back("END IF");
    } else {
      std::string direct_expr = def->direct_expr(def->direct_expr_column());
      vector<string> cond_and_dexpr(cond);
      cond_and_dexpr.push_back(direct_expr);
      incline::push_back(r,
			 super::_build_delete_from_def(def, src_table,
						       cond_and_dexpr));
      cond_and_dexpr.pop_back();
      cond_and_dexpr.push_back(string("! (") +  direct_expr + ")");
      incline::push_back(r,
			 _build_enqueue_sql(def, src_table, "OLD",
					    cond_and_dexpr));
    }
  } else {
    incline::push_back(r, _build_enqueue_sql(def, src_table, "OLD", cond));
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
  if (! def->direct_expr_column().empty()) {
    assert(def->pk_columns().find(def->direct_expr_column())
	   != def->pk_columns().end());
    string dest_direct_expr
      = def->direct_expr(def->pk_columns().find(def->direct_expr_column())
			 ->second);
    vector<string> cond_and_dexpr(cond);
    cond_and_dexpr.push_back(dest_direct_expr);
    incline::push_back(r, 
		       super::_build_update_merge_from_def(def, src_table,
							   cond_and_dexpr));
    cond_and_dexpr.pop_back();
    cond_and_dexpr.push_back(string("! (") + dest_direct_expr + ')');
    incline::push_back(r, 
		       _build_enqueue_sql(def, src_table, "NEW",
					  cond_and_dexpr));
  } else {
    incline::push_back(r, _build_enqueue_sql(def, src_table, "NEW"));
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
  incline::push_back(cond, def->build_merge_cond(src_table, alias, true));
  return do_build_enqueue_sql(def, pk_columns, tables, cond);
}
