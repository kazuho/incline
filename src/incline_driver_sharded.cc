#include <cassert>
#include <set>
#include "start_thread.h"
#include "tmd.h"
#include "incline_def_sharded.h"
#include "incline_driver_sharded.h"
#include "incline_mgr.h"
#include "incline_util.h"

using namespace std;

namespace incline_driver_sharded_ns {
  
  template <typename KEYTYPE> struct str_to_key_type {
    // operator() intentionally not defined to generate compile error
  };
  
  template <> struct str_to_key_type<long long> {
    long long operator()(const string& s) const {
      long long v;
      int r = sscanf(s.c_str(), "%lld", &v);
      assert(r == 1);
      return v;
    }
  };
  
  template <> struct str_to_key_type<string> {
    string operator()(const string& s) const {
      return s;
    }
  };
  
  template <typename KEYTYPE> struct key_type_to_str {
    // operator() intentionally not defined to generate compile error
  };
  
  template <> struct key_type_to_str<long long> {
    string operator()(long long v) {
      char buf[sizeof("-9223372036854775808")];
      sprintf(buf, "%lld", v);
      return buf;
    }
  };
  
  template <> struct key_type_to_str<string> {
    string operator()(const string& s) const {
      return s;
    }
  };
  
  template <typename KEYTYPE> class range_rule
    : public incline_driver_sharded::rule {
  protected:
    map<KEYTYPE, string> lb_hostport_; // lower_bound => hostport
  public:
    virtual string parse(const picojson::value& def) {
      const picojson::value& map = def.get("map");
      if (! map.is<picojson::object>()) {
	return "map is not of type object";
      }
      for (picojson::object::const_iterator mi
	     = map.get<picojson::object>().begin();
	   mi != map.get<picojson::object>().end();
	   ++mi) {
    	lb_hostport_[str_to_key_type<KEYTYPE>()(mi->first)]
	  = mi->second.to_str();
      }
      return string();
    }
    virtual vector<string> get_all_hostport() const {
      set<string> rs;
      for (typename map<KEYTYPE, string>::const_iterator i
	     = lb_hostport_.begin();
	   i != lb_hostport_.end();
	   ++i) {
	rs.insert(i->second);
      }
      vector<string> r;
      copy(rs.begin(), rs.end(), back_inserter(r));
      return r;
    }
    virtual string get_hostport_for(const string& key) const {
      typename map<KEYTYPE, string>::const_iterator i
	= lb_hostport_.upper_bound(str_to_key_type<KEYTYPE>()(key));
      return i == lb_hostport_.begin() ? string() : (--i)->second;
    }
    virtual string build_expr_for(const string& column_expr,
				  const string& hostport) const {
      typename map<KEYTYPE, string>::const_iterator i;
      vector<string> cond;
      for (i = lb_hostport_.begin(); i != lb_hostport_.end(); ++i) {
	if (i->second == hostport) {
	  cond.push_back('(' + key_type_to_str<KEYTYPE>()(i->first) + "<="
			 + column_expr);
	  ++i;
	  if (i != lb_hostport_.end()) {
	    cond.back() += " AND " + column_expr + '<'
	      + key_type_to_str<KEYTYPE>()(i->first);
	  }
	  cond.back() += ')';
	  --i;
	}
      }
      assert(! cond.empty()); // hostport not found
      return '(' + incline_util::join(" OR ", cond) + ')';
    }
  };
  
  class hash_int_rule : public incline_driver_sharded::rule {
  protected:
    vector<string> hostport_; // hostport_[key % hostport_.size()]
  public:
    virtual string parse(const picojson::value& def) {
      if (! def.get("num").is<double>()) {
	return "``num'' field does not exist in hash partitioning rule";
      }
      const picojson::value& nodes = def.get("nodes");
      if (! nodes.is<picojson::array>()) {
	return "``nodes'' array not defined in hash partition rule";
      }
      for (picojson::array::const_iterator ni
	     = nodes.get<picojson::array>().begin();
	   ni != nodes.get<picojson::array>().end();
	   ++ni) {
	hostport_.push_back(ni->to_str());
      }
      if (def.get("num").get<double>() != hostport_.size()) {
	return "number of nodes does not match the value specified in ``num'' field";
      }
      return string();
    }
    virtual vector<string> get_all_hostport() const {
      set<string> rs;
      for (vector<string>::const_iterator hi = hostport_.begin();
	   hi != hostport_.end();
	   ++hi) {
	rs.insert(*hi);
      }
      vector<string> r;
      copy(rs.begin(), rs.end(), back_inserter(r));
      return r;
    }
    virtual string get_hostport_for(const string& key) const {
      return hostport_[str_to_key_type<long long>()(key) % hostport_.size()];
    }
    virtual string build_expr_for(const string& column_expr,
				  const string& hostport) const {
      vector<string> cond;
      for (unsigned i = 0; i < hostport_.size(); ++i) {
	if (hostport_[i] == hostport) {
	  char modulo_str[sizeof("%-9223372036854775808=-2147483648)")];
	  sprintf(modulo_str, "%%%lld=%u)", (long long)hostport_.size(), i);
	  cond.push_back('(' + column_expr + modulo_str);
	}
      }
      assert(! cond.empty()); // hostport not found
      return '(' + incline_util::join(" OR ", cond) + ')';
    }
  };
  
}

using namespace incline_driver_sharded_ns;

incline_def*
incline_driver_sharded::create_def() const
{
  return new incline_def_sharded();
}

string
incline_driver_sharded::parse_shard_def(const picojson::value& def)
{
  if (! def.is<picojson::object>()) {
    return "definition should be ant object";
  }
  // get algorithm and build the rule
  string algo = def.get("algorithm").to_str();
#define RANGE_ALGO(id, type) \
  if (algo == "range-" id) rule_ = new range_rule<type>()
  RANGE_ALGO("int", long long);
  RANGE_ALGO("str-case-sensitive", string);
#undef RANGE_ALGO
  if (algo == "hash-int") rule_ = new hash_int_rule();
  if (rule_ == NULL) {
    return "unknown sharding algorithm: " + algo;
  }
  // build the rule
  return rule_->parse(def);
}

string
incline_driver_sharded::set_hostport(const string& hostport)
{
  vector<string> all_hp = rule_->get_all_hostport();
  for (vector<string>::const_iterator i = all_hp.begin();
       i != all_hp.end();
       ++i) {
    if (*i == hostport) {
      cur_hostport_ = hostport;
      return string();
    }
  }
  // not found
  return "specified database does not exist in shard definition:" + hostport;
}

string
incline_driver_sharded::do_build_direct_expr(const string& column_expr) const
{
  return rule_->build_expr_for(column_expr, cur_hostport_);
}

void*
incline_driver_sharded::fw_writer::do_handle_calls(int)
{
  tmd::conn_t* dbh = NULL;
  
  while (! terminate_requested()) {
    // get data to handle
    slot_t& slot = get_slot();
    if (slot.empty()) {
      continue;
    }
    // connect to db if necessary
    if (dbh == NULL && (retry_at_ == 0 || retry_at_ <= time(NULL))) {
      dbh = mgr_->connect(hostport_);
      retry_at_ = 0;
    }
    if (dbh == NULL) {
      // not connected, return immediately
      continue;
    }
    // commit data
    bool use_transaction = true;
    if (slot.size() == 1) {
      fw_writer_call_t* req = slot.front()->request();
      if (req->replace_rows_->empty() || req->delete_rows_->empty()) {
	use_transaction = false;
      }
    }
    try {
      if (use_transaction) {
	tmd::execute(*dbh, "BEGIN");
      }
      for (slot_t::iterator si = slot.begin(); si != slot.end(); ++si) {
	fw_writer_call_t* req = (*si)->request();
	if (! req->replace_rows_->empty()) {
	  req->forwarder_->replace_rows(*dbh, *req->replace_rows_);
	}
	if (! req->delete_rows_->empty()) {
	  req->forwarder_->delete_rows(*dbh, *req->delete_rows_);
	}
      }
      if (use_transaction) {
	tmd::execute(*dbh, "COMMIT");
      }
      for (slot_t::iterator si = slot.begin(); si != slot.end(); ++si) {
	fw_writer_call_t* req = (*si)->request();
	req->success_ = true;
      }
      retry_at_ = 0; // reset so that other threads will reconnect immediately
    } catch (tmd::error_t err) {
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

incline_driver_sharded::forwarder::forwarder(forwarder_mgr* mgr,
					     const incline_def_sharded* def,
					     tmd::conn_t* dbh,
					     int poll_interval)
  : super(mgr, def, dbh, poll_interval)
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
incline_driver_sharded::forwarder::do_update_rows(const vector<vector<string> >& replace_rows, const vector<vector<string> >& delete_rows)
{
  map<fw_writer*, fw_writer_call_t*> calls;
  _setup_calls(calls, replace_rows, &fw_writer_call_t::replace_rows_);
  _setup_calls(calls, delete_rows, &fw_writer_call_t::delete_rows_);
  fw_writer::call(calls.begin(), calls.end());
  bool r = true;
  for (map<fw_writer*, fw_writer_call_t*>::iterator ci = calls.begin();
       ci != calls.end();
       ++ci) {
    if (! ci->second->success_) {
      r = false;
    }
    delete ci->second->replace_rows_;
    delete ci->second->delete_rows_;
    delete ci->second;
  }
  return r;
}

string
incline_driver_sharded::forwarder::do_get_extra_cond()
{
  vector<string> cond;
  const map<string, fw_writer*>& writers(mgr()->writers());
  bool has_inactive = false;
  for (map<string, fw_writer*>::const_iterator wi = writers.begin();
       wi != writers.end();
       ++wi) {
    if (wi->second->is_active()) {
      cond.push_back(mgr()->driver()->rule()
		     ->build_expr_for(def()->direct_expr_column(), wi->first));
    } else {
      has_inactive = true;
    }
  }
  return has_inactive
    ? (cond.empty() ? string("0") : incline_util::join(" OR ", cond))
    : string();
}

void
incline_driver_sharded::forwarder::_setup_calls(map<fw_writer*, fw_writer_call_t*>& calls, const vector<vector<string> >& rows, vector<const vector<string>*>* fw_writer_call_t::*target_rows)
{
  for (vector<vector<string> >::const_iterator ri = rows.begin();
       ri != rows.end();
       ++ri) {
    fw_writer* writer = mgr()->get_writer_for((*ri)[shard_col_index_]);
    map<fw_writer*, fw_writer_call_t*>::iterator ci = calls.lower_bound(writer);
    if (ci != calls.end() && ci->first == writer) {
      (ci->second->*target_rows)->push_back(&*ri);
    } else {
      fw_writer_call_t* call
	= new fw_writer_call_t(this, new vector<const vector<string>*>(),
			       new vector<const vector<string>*>());
      (call->*target_rows)->push_back(&*ri);
      calls.insert(ci, make_pair(writer, call));
    }
  }
}

void*
incline_driver_sharded::forwarder_mgr::run()
{
  vector<pthread_t> threads;
  
  { // create writers and start
    vector<string> all_hostport(driver()->rule()->get_all_hostport());
    for (vector<string>::const_iterator hi = all_hostport.begin();
	 hi != all_hostport.end();
	 ++hi) {
      fw_writer* writer = new fw_writer(this, *hi);
      threads.push_back(start_thread(writer, 0));
      writers_[*hi] = writer;
    }
  }

  // supre
  super::run();
  
  // loop
  while (! threads.empty()) {
    pthread_join(threads.back(), NULL);
    threads.pop_back();
  }
  
  return NULL;
}

tmd::conn_t*
incline_driver_sharded::forwarder_mgr::connect(const string& hostport)
{
  unsigned short port = 0;
  string::size_type colon_at = hostport.find(':');
  assert(colon_at != string::npos);
  sscanf(hostport.c_str() + colon_at + 1, "%hu", &port);
  return (*connect_)(hostport.substr(0, colon_at).c_str(), port);
}

incline_driver_sharded::forwarder*
incline_driver_sharded::forwarder_mgr::do_create_forwarder(const incline_def_async_qtable* _def)
{
  const incline_def_sharded* def
    = dynamic_cast<const incline_def_sharded*>(_def);
  assert(def != NULL);
  tmd::conn_t* dbh = (*connect_)(src_host_.c_str(), src_port_);
  assert(dbh != NULL);
  return new forwarder(this, def, dbh, poll_interval_);
}
