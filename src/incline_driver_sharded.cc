extern "C" {
#include <sys/stat.h>
}
#include <cassert>
#include <fstream>
#include <sstream>
#include "start_thread.h"
#include "incline_dbms.h"
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
    // lower_bound => hostport
    map<KEYTYPE, incline_driver_sharded::connect_params> lb_connect_params_;
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
	incline_driver_sharded::connect_params params;
	string err = params.parse(mi->second);
	if (! err.empty()) {
	  return err;
	}
	lb_connect_params_[str_to_key_type<KEYTYPE>()(mi->first)] = params;
      }
      return string();
    }
    virtual vector<incline_driver_sharded::connect_params>
    get_all_connect_params() const {
      vector<incline_driver_sharded::connect_params> r;
      for (typename map<KEYTYPE, incline_driver_sharded::connect_params>
	     ::const_iterator i = lb_connect_params_.begin();
	   i != lb_connect_params_.end();
	   ++i) {
	for (vector<incline_driver_sharded::connect_params>::const_iterator ri
	       = r.begin();
	     ri != r.end();
	     ++ri) {
	  if (ri->host == i->second.host && ri->port == i->second.port) {
	    goto ON_FOUND;
	  }
	}
	r.push_back(i->second);
      ON_FOUND:
	;
      }
      return r;
    }
    virtual incline_driver_sharded::connect_params
    get_connect_params_for(const string& key) const {
      typename map<KEYTYPE, incline_driver_sharded::connect_params>
	::const_iterator i
	= lb_connect_params_.upper_bound(str_to_key_type<KEYTYPE>()(key));
      return i == lb_connect_params_.begin()
	? incline_driver_sharded::connect_params() : (--i)->second;
    }
    virtual string build_expr_for(const string& column_expr, const string& host,
				  unsigned short port) const {
      vector<string> cond;
      for (typename map<KEYTYPE, incline_driver_sharded::connect_params>
	     ::const_iterator i = lb_connect_params_.begin();
	   i != lb_connect_params_.end();
	   ++i) {
	if (i->second.host == host && i->second.port == port) {
	  cond.push_back('(' + key_type_to_str<KEYTYPE>()(i->first) + "<="
			 + column_expr);
	  ++i;
	  if (i != lb_connect_params_.end()) {
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
    // connect_params_[key % connect_params_.size()]
    vector<incline_driver_sharded::connect_params> connect_params_;
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
	incline_driver_sharded::connect_params params;
	string err = params.parse(*ni);
	if (! err.empty()) {
	  return err;
	}
	connect_params_.push_back(params);
      }
      if (def.get("num").get<double>() != connect_params_.size()) {
	return "number of nodes does not match the value specified in ``num'' field";
      }
      return string();
    }
    virtual vector<incline_driver_sharded::connect_params>
    get_all_connect_params() const {
      vector<incline_driver_sharded::connect_params> r;
      for (vector<incline_driver_sharded::connect_params>::const_iterator hi
	     = connect_params_.begin();
	   hi != connect_params_.end();
	   ++hi) {
	for (vector<incline_driver_sharded::connect_params>::const_iterator ri
	       = r.begin();
	     ri != r.end();
	     ++ri) {
	  if (ri->host == hi->host && ri->port == hi->port) {
	    goto ON_FOUND;
	  }
	}
	r.push_back(*hi);
      ON_FOUND:
	;
      }
      return r;
    }
    virtual incline_driver_sharded::connect_params
    get_connect_params_for(const string& key) const {
      return connect_params_[str_to_key_type<long long>()(key)
			     % connect_params_.size()];
    }
    virtual string build_expr_for(const string& column_expr, const string& host,
				  unsigned short port) const {
      vector<string> cond;
      for (unsigned i = 0; i < connect_params_.size(); ++i) {
	if (connect_params_[i].host == host
	    && connect_params_[i].port == port) {
	  char modulo_str[sizeof("%-9223372036854775808=-2147483648)")];
	  sprintf(modulo_str, "%%%lld=%u)", (long long)connect_params_.size(),
		  i);
	  cond.push_back('(' + column_expr + modulo_str);
	}
      }
      assert(! cond.empty()); // hostport not found
      return '(' + incline_util::join(" OR ", cond) + ')';
    }
  };
  
}

using namespace incline_driver_sharded_ns;

string
incline_driver_sharded::connect_params::parse(const picojson::value& _def)
{
  const picojson::value& def(_def.is<picojson::array>() ? _def.get(0) : _def);
  if (! def.is<picojson::object>()) {
    return "connection information is not an object";
  }
  const picojson::value::object& defobj(def.get<picojson::value::object>());
  picojson::value::object::const_iterator vi;
#define COPY_IF(val)				  \
  if ((vi = defobj.find(#val)) != defobj.end()) { \
    if (! vi->second.is<string>()) {		  \
      return #val " is not a string";		  \
    }						  \
    val = vi->second.get<string>();	  \
  }
  COPY_IF(host);
  if ((vi = defobj.find("port")) != defobj.end()) {
      if (vi->second.is<double>()) {
	port = (unsigned short)vi->second.get<double>();
      } else if (vi->second.is<string>()) {
	if (sscanf(vi->second.get<string>().c_str(), "%hu", &port) != 1) {
	  return "port should be a double or a strig";
	}
      } else {
	return "port is not a double nor a string";
      }
  }
  COPY_IF(username);
  COPY_IF(password);
#undef COPY_IF_TYPE
  return string();
}

incline_def*
incline_driver_sharded::create_def() const
{
  return new incline_def_sharded();
}

bool
incline_driver_sharded::should_exit_loop() const
{
  if (mtime_of_shard_def_file_ != _get_mtime_of_shard_def_file()) {
    cerr << "detected update of shard definition file, exitting..." << endl;
    return true;
  }
  return false;
}

string
incline_driver_sharded::parse_shard_def(const string& shard_def_file)
{
  picojson::value shard_def;
  
  // read file
  if (shard_def_file == "-") {
    string err = picojson::parse(shard_def, cin);
    if (! err.empty()) {
      return err;
    }
  } else {
    shard_def_file_ = shard_def_file;
    if ((mtime_of_shard_def_file_ = _get_mtime_of_shard_def_file()) == 0) {
      return "failed to obtain information of file:" + shard_def_file_;
    }
    ifstream fin;
    fin.open(shard_def_file.c_str(), ios::in);
    if (! fin.is_open()) {
      return "failed to open file:" + shard_def_file;
    }
    string err = picojson::parse(shard_def, fin);
    if (! err.empty()) {
      return err;
    }
    fin.close();
  }
  // parse json
  if (! shard_def.is<picojson::object>()) {
    return "definition should be ant object";
  }
  // get algorithm and build the rule
  string algo = shard_def.get("algorithm").to_str();
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
  return rule_->parse(shard_def);
}

string
incline_driver_sharded::set_hostport(const string& host, unsigned short port)
{
  vector<connect_params> all_cp = rule_->get_all_connect_params();
  for (vector<connect_params>::const_iterator i = all_cp.begin();
       i != all_cp.end();
       ++i) {
    if (i->host == host && i->port == port) {
      cur_host_ = host;
      cur_port_ = port;
      return string();
    }
  }
  // not found
  stringstream ss;
  ss << "specified database does not exist in shard definition:" << host << ':'
     << port;
  return ss.str();
}

string
incline_driver_sharded::do_build_direct_expr(const string& column_expr) const
{
  return rule_->build_expr_for(column_expr, cur_host_, cur_port_);
}

void*
incline_driver_sharded::fw_writer::do_handle_calls(int)
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
	dbh = mgr_->connect(connect_params_);
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
      fw_writer_call_t* req = slot.front()->request();
      if (req->insert_rows_->empty() || req->delete_rows_->empty()) {
	use_transaction = false;
      }
    }
    try {
      if (use_transaction) {
	dbh->execute("BEGIN");
      }
      for (slot_t::iterator si = slot.begin(); si != slot.end(); ++si) {
	fw_writer_call_t* req = (*si)->request();
	// the order: DELETE -> INSERT is requpired by driver_async_qtable
	if (! req->delete_rows_->empty()) {
	  req->forwarder_->delete_rows(dbh, *req->delete_rows_);
	}
	if (! req->insert_rows_->empty()) {
	  req->forwarder_->insert_rows(dbh, *req->insert_rows_);
	}
      }
      if (use_transaction) {
	dbh->execute("COMMIT");
      }
      for (slot_t::iterator si = slot.begin(); si != slot.end(); ++si) {
	fw_writer_call_t* req = (*si)->request();
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

incline_driver_sharded::forwarder::forwarder(forwarder_mgr* mgr,
					     const incline_def_sharded* def,
					     incline_dbms* dbh,
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
incline_driver_sharded::forwarder::do_update_rows(const vector<const vector<string>*>& delete_rows, const vector<const vector<string>*>& insert_rows)
{
  map<fw_writer*, fw_writer_call_t*> calls;
  _setup_calls(calls, insert_rows, &fw_writer_call_t::insert_rows_);
  _setup_calls(calls, delete_rows, &fw_writer_call_t::delete_rows_);
  fw_writer::call(calls.begin(), calls.end());
  bool r = true;
  for (map<fw_writer*, fw_writer_call_t*>::iterator ci = calls.begin();
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
incline_driver_sharded::forwarder::do_get_extra_cond()
{
  vector<string> cond;
  const vector<pair<connect_params, fw_writer*> >& writers(mgr()->writers());
  bool has_inactive = false;
  for (vector<pair<connect_params, fw_writer*> >::const_iterator wi
	 = writers.begin();
       wi != writers.end();
       ++wi) {
    if (wi->second->is_active()) {
      cond.push_back(mgr()->driver()->rule()
		     ->build_expr_for(def()->direct_expr_column(),
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
incline_driver_sharded::forwarder::_setup_calls(map<fw_writer*, fw_writer_call_t*>& calls, const vector<const vector<string>*>& rows, vector<const vector<string>*>* fw_writer_call_t::*target_rows)
{
  for (vector<const vector<string>*>::const_iterator ri = rows.begin();
       ri != rows.end();
       ++ri) {
    fw_writer* writer = mgr()->get_writer_for((**ri)[shard_col_index_]);
    map<fw_writer*, fw_writer_call_t*>::iterator ci = calls.lower_bound(writer);
    if (ci != calls.end() && ci->first == writer) {
      (ci->second->*target_rows)->push_back(*ri);
    } else {
      fw_writer_call_t* call
	= new fw_writer_call_t(this, new vector<const vector<string>*>(),
			       new vector<const vector<string>*>(),
			       new vector<const vector<string>*>());
      (call->*target_rows)->push_back(*ri);
      calls.insert(ci, make_pair(writer, call));
    }
  }
}

void*
incline_driver_sharded::forwarder_mgr::run()
{
  vector<pthread_t> threads;
  
  { // create writers and start
    vector<connect_params>
      all_hostport(driver()->rule()->get_all_connect_params());
    for (vector<connect_params>::const_iterator hi = all_hostport.begin();
	 hi != all_hostport.end();
	 ++hi) {
      fw_writer* writer = new fw_writer(this, *hi);
      threads.push_back(start_thread(writer, 0));
      writers_.push_back(make_pair(*hi, writer));
    }
  }

  // super
  super::run();
  
  // stop writers and exit
  for (vector<pair<connect_params, fw_writer*> >::iterator wi
	 = writers_.begin();
       wi != writers_.end();
       ++wi) {
    wi->second->terminate();
  }
  while (! threads.empty()) {
    pthread_join(threads.back(), NULL);
    threads.pop_back();
  }
  
  return NULL;
}

incline_dbms*
incline_driver_sharded::forwarder_mgr::connect(const connect_params& cp)
{
  return incline_dbms::factory_->create(cp.host, cp.port, cp.username,
					cp.password);
}

incline_driver_sharded::forwarder*
incline_driver_sharded::forwarder_mgr::do_create_forwarder(const incline_def_async_qtable* _def)
{
  const incline_def_sharded* def
    = dynamic_cast<const incline_def_sharded*>(_def);
  assert(def != NULL);
#if 1
  // use username and password supplied in command line, since pacific may
  // disable write access under credentials defined by the client, but we still
  // want to continue transferring the modifications until the queue tables
  // become empty
  incline_dbms* dbh = incline_dbms::factory_->create();
#else
  vector<connect_params> all_cp = driver()->rule()->get_all_connect_params();
  pair<string, unsigned short> cur_hostport = driver()->get_hostport();
  incline_dbms* dbh = NULL;
  for (vector<connect_params>::const_iterator i = all_cp.begin();
       i != all_cp.end();
       ++i) {
    if (i->host == cur_hostport.first && i->port == cur_hostport.second) {
      dbh = connect(*i);
      break;
    }
  }
#endif
  assert(dbh != NULL);
  return new forwarder(this, def, dbh, poll_interval_);
}

time_t
incline_driver_sharded::_get_mtime_of_shard_def_file() const
{
  struct stat st;
  if (shard_def_file_.empty()
      || lstat(shard_def_file_.c_str(), &st) != 0) {
    return 0;
  }
  assert(st.st_mtime != 0); // we use mtime==0 to indicate error, there's no way
  return st.st_mtime;
}
