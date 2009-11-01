#include "incline_dbms.h"
#include "incline_def_sharded.h"
#include "incline_driver_sharded.h"
#include "incline_fw_sharded.h"
#include "incline_mgr.h"
#include "incline_util.h"
#include "start_thread.h"

using namespace std;

incline_fw_sharded::writer::writer(manager* mgr, const incline_driver_sharded::connect_params& cp) : mgr_(mgr), connect_params_(cp), retry_at_(0)
{
  thr_ = start_thread(this, 0);
}

void
incline_fw_sharded::writer::terminate()
{
  super::terminate();
  pthread_join(thr_, NULL);
}

void*
incline_fw_sharded::writer::do_handle_calls(int)
{
  incline_dbms* dbh = NULL;
  
  while (! terminate_requested()) {
    // get data to handle
    slot_t& slot = get_slot();
    if (slot.empty()) {
      continue;
    }
    // connect to db if necessary
    if (dbh == NULL && (retry_at_ == 0 || retry_at_ <= time(NULL))) {
      try {
	dbh = connect_params_.connect();
	retry_at_ = 0;
      } catch (incline_dbms::error_t& err) {
	// failed to (re)connect
	cerr << err.what() << endl;
	retry_at_ = time(NULL) + 10;
      }
    }
    if (dbh == NULL) {
      // not connected, return immediately
      continue;
    }
    // commit data
    bool use_transaction = true;
    if (slot.size() == 1) {
      writer_call_t* req = slot.front()->request();
      if (req->insert_rows_->empty() || req->delete_rows_->empty()) {
	use_transaction = false;
      }
    }
    try {
      if (use_transaction) {
	dbh->execute("BEGIN");
      }
      for (slot_t::iterator si = slot.begin(); si != slot.end(); ++si) {
	writer_call_t* req = (*si)->request();
	// the order: DELETE -> INSERT is requpired by driver_async_qtable
	if (! req->delete_rows_->empty()) {
	  req->fw_->delete_rows(dbh, *req->delete_rows_);
	}
	if (! req->insert_rows_->empty()) {
	  req->fw_->insert_rows(dbh, *req->insert_rows_);
	}
      }
      if (use_transaction) {
	dbh->execute("COMMIT");
      }
      for (slot_t::iterator si = slot.begin(); si != slot.end(); ++si) {
	writer_call_t* req = (*si)->request();
	req->success_ = true;
      }
      retry_at_ = 0; // reset so that other threads will reconnect immediately
    } catch (incline_dbms::error_t& err) {
      // on error, log error, disconnect
      cerr << err.what() << endl;
      delete dbh;
      dbh = NULL;
      retry_at_ = time(NULL) + 10;
    }
  }
  
  delete dbh;
  return NULL;
}

incline_fw_sharded::manager::~manager()
{
  for (vector<pair<incline_driver_sharded::connect_params, writer*> >::iterator
	 wi = writers_.begin();
       wi != writers_.end();
       ++wi) {
    wi->second->terminate();
    delete wi->second;
  }
}

void
incline_fw_sharded::manager::start(vector<pthread_t>& threads)
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
    if (dynamic_cast<const incline_driver_sharded::shard_rule*>(rl) != NULL) {
      vector<incline_driver_sharded::connect_params>
	cp(rl->get_all_connect_params());
      for (vector<incline_driver_sharded::connect_params>::const_iterator ci
	     = cp.begin();
	   ci != cp.end();
	   ++ci) {
	if (ci->host == cur_hostport.first && ci->port == cur_hostport.second) {
	  goto FOUND;
	}
      }
      goto NOT_FOUND;
    FOUND:
      for (vector<incline_driver_sharded::connect_params>::const_iterator ci
	     = cp.begin();
	   ci != cp.end();
	   ++ci) {
	if (ci->host == cur_hostport.first && ci->port == cur_hostport.second) {
	  // start forwarder after all writer
	} else {
	  writers_.push_back(make_pair(*ci, new writer(this, *ci)));
	}
      }
      {
	// TODO call destructor on termination
	incline_dbms* dbh = incline_dbms::factory_->create();
	assert(dbh != NULL);
	threads.push_back(start_thread(new incline_fw_sharded(this, def, dbh)));
      }
    NOT_FOUND:
      ;
    }
  }
}

incline_fw_sharded::writer*
incline_fw_sharded::manager::get_writer_for(const incline_def_sharded* def,
					    const string& key) const
{
  // FIXME O(N)
  const incline_driver_sharded::rule* rl = driver()->rule_of(def->shard_file());
  assert(rl != NULL);
  incline_driver_sharded::connect_params cp = rl->get_connect_params_for(key);
  for (vector<pair<incline_driver_sharded::connect_params, writer*> >::const_iterator wi = writers_.begin();
       wi != writers_.end();
       ++wi) {
    if (wi->first.host == cp.host && wi->first.port == cp.port) {
      return wi->second;
    }
  }
  assert(0);
}

incline_fw_sharded::incline_fw_sharded(manager* mgr,
				       const incline_def_sharded* def,
				       incline_dbms* dbh)
  : super(mgr, def, dbh)
{
  size_t i = 0;
  for (map<string, string>::const_iterator pi = def->pk_columns().begin();
       pi != def->pk_columns().end();
       ++pi, ++i) {
    if (pi->second == def->direct_expr_column()) {
      shard_col_index_ = i;
      goto FOUND;
    }
  }
  assert(0);
 FOUND:
  ;
}

bool
incline_fw_sharded::do_update_rows(const vector<vector<string> >& delete_rows,
				   const vector<vector<string> >& insert_rows)
{
  map<writer*, writer_call_t*> calls;
  _setup_calls(calls, insert_rows, &writer_call_t::insert_rows_);
  _setup_calls(calls, delete_rows, &writer_call_t::delete_rows_);
  writer::call(calls.begin(), calls.end());
  bool r = true;
  for (map<writer*, writer_call_t*>::iterator ci = calls.begin();
       ci != calls.end();
       ++ci) {
    if (! ci->second->success_) {
      r = false;
    }
    delete ci->second->insert_rows_;
    delete ci->second->delete_rows_;
    delete ci->second;
  }
  return r;
}

string
incline_fw_sharded::do_get_extra_cond()
{
  vector<string> cond;
  const vector<pair<incline_driver_sharded::connect_params, writer*> >&
    writers(mgr()->writers());
  bool has_inactive = false;
  for (vector<pair<incline_driver_sharded::connect_params, writer*> >::const_iterator wi = writers.begin();
       wi != writers.end();
       ++wi) {
    if (wi->second->is_active()) {
      const incline_driver_sharded::rule* rl
	= mgr()->driver()->rule_of(def()->shard_file());
      assert(rl != NULL);
      cond.push_back(rl->build_expr_for(def()->direct_expr_column(),
					wi->first.host, wi->first.port));
    } else {
      has_inactive = true;
    }
  }
  return has_inactive
    ? (cond.empty() ? string("0") : incline_util::join(" OR ", cond))
    : string();
}

void
incline_fw_sharded::_setup_calls(map<writer*, writer_call_t*>& calls,
				 const vector<vector<string> >& rows,
				 vector<vector<string> >*
				 writer_call_t::*target_rows)
{
  for (vector<vector<string> >::const_iterator ri = rows.begin();
       ri != rows.end();
       ++ri) {
    writer* wr = mgr()->get_writer_for(def(), (*ri)[shard_col_index_]);
    map<writer*, writer_call_t*>::iterator ci = calls.lower_bound(wr);
    if (ci != calls.end() && ci->first == wr) {
      (ci->second->*target_rows)->push_back(*ri);
    } else {
      writer_call_t* call
	= new writer_call_t(this, new vector<vector<string> >(),
			    new vector<vector<string> >());
      (call->*target_rows)->push_back(*ri);
      calls.insert(ci, make_pair(wr, call));
    }
  }
}
