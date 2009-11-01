#include "incline_dbms.h"
#include "incline_def_async_qtable.h"
#include "incline_fw_async_qtable.h"
#include "incline_util.h"

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
incline_fw_async_qtable::do_post_commit(const vector<string>& iq_ids)
{
  dbh_->execute(clear_queue_query_base_ + '('
		+ incline_util::join(',', iq_ids) + ')');
}
