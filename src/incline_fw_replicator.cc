#include "incline_dbms.h"
#include "incline_def_sharded.h"
#include "incline_driver_sharded.h"
#include "incline_fw_replicator.h"
#include "incline_mgr.h"
#include "start_thread.h"
#ifdef WIN32
#  include "incline_win32.h"
#endif

using namespace std;

void
incline_fw_replicator::manager::start(vector<pthread_t>& threads)
{
  const vector<incline_def*>& defs = driver()->mgr()->defs();
  pair<string, unsigned short> cur_hostport(driver()->get_hostport());
  
  for (vector<incline_def*>::const_iterator di = defs.begin();
       di != defs.end();
       ++di) {
    const incline_def_sharded* def
      = static_cast<const incline_def_sharded*>(*di);
    const incline_driver_sharded::rule* rl
      = driver()->rule_of(def->shard_file());
    assert(rl != NULL);
    const incline_driver_sharded::replicator_rule* rrl
      = dynamic_cast<const incline_driver_sharded::replicator_rule*>(rl);
    if (rrl != NULL) {
      if (rrl->source().host == cur_hostport.first
          && rrl->source().port == cur_hostport.second) {
        const vector<incline_driver_sharded::connect_params>& dest
	  = rrl->destination();
	for (vector<incline_driver_sharded::connect_params>::const_iterator ddi
	       = dest.begin();
	     ddi != dest.end();
	     ++ddi) {
	  incline_dbms* dbh = incline_dbms::factory_->create();
	  assert(dbh != NULL);
	  threads.push_back(start_thread(new incline_fw_replicator(this, def,
								   dbh, *ddi)));
	}
      }
    }
  }
}

void
incline_fw_replicator::do_run()
{
  incline_dbms* dest_dbh = NULL;
  string last_id;
  
  while (! mgr()->driver()->should_exit_loop()) {
    
    vector<string> iq_ids;
    vector<vector<string> > delete_pks, insert_rows;
    
    // setup
    try {
      if (dest_dbh == NULL) {
	dest_dbh = dest_cp_.connect();
	vector<vector<incline_dbms::value_t> > res;
	dest_dbh->query(res,
		    "SELECT last_id FROM _iq_repl WHERE tbl_name='"
		    + dest_dbh->escape(def()->destination()) + '\'');
	last_id = res.empty() ? string("0") : *res[0][0];
      }
    } catch (incline_dbms::error_t& err) {
      cerr << err.what() << endl;
      goto ON_DEST_ERROR;
    }
    
    // fetch rows
    fetch_rows("_iq_id>" + last_id, iq_ids, delete_pks, insert_rows);
    
    // sleep and retry if no data
    if (iq_ids.empty()) {
      sleep(mgr()->poll_interval());
      goto ON_NEXT;
    }
    last_id = iq_ids.back();
    
    // write to target database
    try {
      dest_dbh->execute("BEGIN");
      if (! delete_pks.empty()) {
	this->delete_rows(dest_dbh, delete_pks);
      }
      if (! insert_rows.empty()) {
	this->insert_rows(dest_dbh, insert_rows);
      }
      string sql("UPDATE _iq_repl SET last_id=" + last_id + " WHERE tbl_name='"
		 + dest_dbh->escape(def()->destination()) + '\'');
      mgr_->log_sql(dest_dbh, sql);
      if (dest_dbh->execute(sql) == 0) {
	sql = "INSERT INTO _iq_repl (tbl_name,last_id) VALUES ('"
	  + dest_dbh->escape(def()->destination()) + "'," + last_id + ')';
	mgr_->log_sql(dest_dbh, sql);
	dest_dbh->execute(sql);
      }
      dest_dbh->execute("COMMIT");
    } catch (incline_dbms::error_t& err) {
      cerr << err.what() << endl;
      goto ON_DEST_ERROR;
    }
    // done
    goto ON_NEXT;
    
  ON_DEST_ERROR:
    delete dest_dbh;
    dest_dbh = NULL;
    { // check connection to source database, forwarder should exit immediately
      // if it is down
      vector<vector<incline_dbms::value_t> > res;
      dbh_->query(res, "SELECT 1");
    }
    sleep(mgr()->poll_interval());
    
  ON_NEXT:
    ;
  }
  
  delete dest_dbh;
}
