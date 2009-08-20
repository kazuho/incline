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
					     action_t action,
					     const vector<string>* cond)
  const
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
							    action, cond),
			      "  ");
      r.push_back("ELSE\\");
      incline_util::push_back(r,
			      do_build_enqueue_insert_sql(def, src_table,
							  action, cond),
			      "  ");
      r.push_back("END IF");
    } else {
      string direct_expr = do_build_direct_expr(de_col);
      vector<string> cond_and_dexpr;
      if (cond != NULL) {
	incline_util::push_back(cond_and_dexpr, *cond);
      }
      cond_and_dexpr.push_back(direct_expr);
      incline_util::push_back(r,
			      super::_build_insert_from_def(def, src_table,
							    action,
							    &cond_and_dexpr));
      cond_and_dexpr.pop_back();
      cond_and_dexpr.push_back("NOT (" +  direct_expr + ")");
      incline_util::push_back(r,
			      do_build_enqueue_insert_sql(def, src_table,
							  action,
							  &cond_and_dexpr));
    }
  } else {
    incline_util::push_back(r,
			    do_build_enqueue_insert_sql(def, src_table, action,
							cond));
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
			      do_build_enqueue_delete_sql(def, src_table, NULL),
			      "   ");
      r.push_back("END IF");
    } else {
      incline_util::push_back(r,
			      super::_build_delete_from_def(def, src_table,
							    cond));
      vector<string> cond_and_dexpr(cond);
      cond_and_dexpr.push_back("NOT (" + do_build_direct_expr(de_col) + ')');
      incline_util::push_back(r,
			      do_build_enqueue_delete_sql(def, src_table,
							  &cond_and_dexpr));
    }
  } else {
    incline_util::push_back(r,
			    do_build_enqueue_delete_sql(def, src_table, &cond));
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
    
    cond_and_dexpr.push_back("NOT ("
			     + do_build_direct_expr(def->direct_expr_column(src_table, src_table))
			     + ')');
    incline_util::push_back(r, 
			    do_build_enqueue_insert_sql(def, src_table,
							act_update,
							&cond_and_dexpr));
  } else {
    incline_util::push_back(r,
			    do_build_enqueue_insert_sql(def, src_table,
							act_update, &cond));
  }
  return r;
}

string
incline_driver_async::do_build_direct_expr(const string& column_expr)
  const
{
  // does not support mixed-mode unless overridden
  assert(0);
}
