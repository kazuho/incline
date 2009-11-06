#include "incline_dbms.h"
#include "incline_def_async_qtable.h"
#include "incline_driver_async_qtable.h"
#include "incline_fw_async_qtable.h"
#include "incline_util.h"
#ifdef WIN32
#  include "incline_win32.h"
#endif

using namespace std;

incline_fw_async_qtable::incline_fw_async_qtable(manager* mgr,
						 const incline_def_async_qtable*
						 def,
						 incline_dbms* dbh)
  : super(mgr, def, dbh),
    clear_queue_query_base_("DELETE FROM " + def->queue_table()
			    + " WHERE _iq_id IN ")
{
}

void
incline_fw_async_qtable::do_run()
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
		dbh_->execute(clear_queue_query_base_ + '('
		      + incline_util::join(',', iq_ids) + ')');
      }
    } catch (incline_dbms::deadlock_error_t&) {
      // just retry
    } catch (incline_dbms::timeout_error_t&) {
      // just retry
    }
  }
}

bool
incline_fw_async_qtable::do_update_rows(const vector<vector<string> >&
					delete_pks,
					const vector<vector<string> >&
					insert_rows)
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
incline_fw_async_qtable::do_get_extra_cond()
{
  return string();
}
