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
#include "incline_fw_sharded.h"
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
    : public incline_driver_sharded::shard_rule {
  protected:
    // lower_bound => hostport
    map<KEYTYPE, incline_driver_sharded::connect_params> lb_connect_params_;
  public:
    range_rule(const string& file) : shard_rule(file) {}
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
	? incline_driver_sharded::connect_params(file()) : (--i)->second;
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
  protected:
    virtual string parse(const picojson::value& def) {
      const picojson::value& map = def.get("map");
      if (! map.is<picojson::object>()) {
	return "map is not of type object";
      }
      for (picojson::object::const_iterator mi
	     = map.get<picojson::object>().begin();
	   mi != map.get<picojson::object>().end();
	   ++mi) {
	incline_driver_sharded::connect_params params(file());
	string err = params.parse(mi->second);
	if (! err.empty()) {
	  return err;
	}
	if (! (lb_connect_params_
	       .insert(make_pair(str_to_key_type<KEYTYPE>()(mi->first),
				 params))
	       .second)) {
	  return "key collision found in file: " + file();
	}
      }
      return string();
    }
  };
  
  class hash_int_rule : public incline_driver_sharded::shard_rule {
  protected:
    // connect_params_[key % connect_params_.size()]
    vector<incline_driver_sharded::connect_params> connect_params_;
  public:
    hash_int_rule(const string& file) : shard_rule(file) {}
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
  protected:
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
	incline_driver_sharded::connect_params params(file());
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
    val = vi->second.get<string>();		  \
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

incline_dbms*
incline_driver_sharded::connect_params::connect()
{
  return incline_dbms::factory_->create(host, port, username, password);
}

incline_driver_sharded::rule*
incline_driver_sharded::rule::parse(const string& file, string& err)
{
  picojson::value def;
  
  { // read file
    ifstream fin;
    fin.open(file.c_str(), ios::in);
    if (! fin.is_open()) {
      err = "failed to open file:" + file;
      return NULL;
    }
    if (! (err = picojson::parse(def, fin)).empty()) {
      return NULL;
    }
    fin.close();
  }
  // check type
  if (! def.is<picojson::object>()) {
    err = "shard definition is not an object in file:" + file;
    return NULL;
  }
  // get algorithm and build the rule
  string algo = def.get("algorithm").to_str();
  rule* rule = NULL;
#define RANGE_ALGO(id, type) \
  if (algo == "range-" id) rule = new range_rule<type>(file)
  RANGE_ALGO("int", long long);
  RANGE_ALGO("str-case-sensitive", string);
#undef RANGE_ALGO
  if (algo == "hash-int") rule = new hash_int_rule(file);
  if (rule == NULL) {
    err = "unknown sharding algorithm: " + algo;
    return NULL;
  }
  // build the rule
  if (! (err = rule->parse(def)).empty()) {
    delete rule;
    return NULL;
  }
  
  return rule;
}

bool
incline_driver_sharded::rule::should_exit_loop() const
{
  return _get_file_mtime() != file_mtime_;
}

time_t
incline_driver_sharded::rule::_get_file_mtime() const
{
  struct stat st;
  if (file_.empty() || lstat(file_.c_str(), &st) != 0) {
    return 0;
  }
  assert(st.st_mtime != 0); // we use mtime==0 to indicate error
  return st.st_mtime;
}

void
incline_driver_sharded::run_forwarder(int poll_interval, int log_fd) const
{
  incline_fw_sharded::manager shard_mgr(this, poll_interval, log_fd);
  vector<pthread_t> threads;
  
  // create shard forwarders and writers
  shard_mgr.start(threads);
  // TODO create replicators
  // wait until all forwarder threads exit
  while (! threads.empty()) {
    pthread_join(threads.back(), NULL);
    threads.pop_back();
  }
}

incline_driver_sharded::~incline_driver_sharded()
{
  for (vector<const rule*>::iterator ri = rules_.begin(); ri != rules_.end(); ++ri) {
    delete *ri;
  }
}

string
incline_driver_sharded::init(const string& host, unsigned short port)
{
  string err;
  
  // load all shard defs
  for (vector<incline_def*>::const_iterator di = mgr()->defs().begin();
       di != mgr()->defs().end();
       ++di) {
    incline_def_sharded* def = static_cast<incline_def_sharded*>(*di);
    string shard_file = def->shard_file();
    const rule* rl = rule_of(shard_file);
    if (rl == NULL) {
      if ((rl = rule::parse(shard_file, err)) == NULL) {
	return err;
      }
      rules_.push_back(rl);
    }
    if (def->direct_expr_column().empty()) {
      if (dynamic_cast<const replication_rule*>(rl) == NULL) {
	return "TODO write error message";
      }
    } else {
      if (dynamic_cast<const shard_rule*>(rl) == NULL) {
	return "TODO write error message";
      }
      // TODO check if given host:port exists in list
    }
  }
  
  // build list of all destinations (as well as checking collisions)
  vector<connect_params> all_cp;
  if (! (err = get_all_connect_params(all_cp)).empty()) {
    return err;
  }
  
  cur_host_ = host;
  cur_port_ = port;
  return string();
}

incline_def*
incline_driver_sharded::create_def() const
{
  return new incline_def_sharded();
}

string
incline_driver_sharded::get_all_connect_params(vector<connect_params>& all_cp) const
{
  for (vector<const rule*>::const_iterator ri = rules_.begin();
       ri != rules_.end();
       ++ri) {
    vector<connect_params> partial = (*ri)->get_all_connect_params();
    for (vector<connect_params>::const_iterator pi = partial.begin();
	 pi != partial.end();
	 ++pi) {
      for (vector<connect_params>::const_iterator ai = all_cp.begin();
	   ai != all_cp.end();
	   ++ai) {
	if (pi->host == ai->host && pi->port == ai->port) {
	  // same instance must not reappear (since it will be difficult to
	  // divide)
	  stringstream ss;
	  ss << "collision found for " << pi->host << ':' << pi->port
	     << " in files:" << ai->file << " and " << pi->file;
	  return ss.str();
	}
      }
      all_cp.push_back(*pi);
    }
  }
  return string();
}

bool
incline_driver_sharded::should_exit_loop() const
{
  for (vector<const rule*>::const_iterator ri = rules_.begin();
       ri != rules_.end();
       ++ri) {
    if ((*ri)->should_exit_loop()) {
      return true;
    }
  }
  return false;
}

const incline_driver_sharded::rule*
incline_driver_sharded::rule_of(const string& file) const
{  
  // FIXME O(N)
  for (vector<const rule*>::const_iterator ri = rules_.begin();
       ri != rules_.end();
       ++ri) {
    const rule* r = *ri;
    if (r->file() == file) {
      return r;
    }
  }
  return NULL;
}

void
incline_driver_sharded::_build_insert_from_def(trigger_body& body,
					       const incline_def* _def,
					       const string& src_table,
					       action_t action,
					       const vector<string>* cond) const
{
  const incline_def_sharded* def
    = dynamic_cast<const incline_def_sharded*>(_def);
  assert(def != NULL);
  if (_is_source_host_of(def)) {
    super::_build_insert_from_def(body, def, src_table, action, cond);
  }
}

void
incline_driver_sharded::_build_delete_from_def(trigger_body& body,
					       const incline_def* _def,
					       const string& src_table,
					       const vector<string>& cond) const
{
  const incline_def_sharded* def
    = dynamic_cast<const incline_def_sharded*>(_def);
  assert(def != NULL);
  if (_is_source_host_of(def)) {
    super::_build_delete_from_def(body, def, src_table, cond);
  }
}

void
incline_driver_sharded::_build_update_merge_from_def(trigger_body& body,
						     const incline_def* _def,
						     const string& src_table,
						     const vector<string>& cond)
  const
{
  const incline_def_sharded* def
    = dynamic_cast<const incline_def_sharded*>(_def);
  assert(def != NULL);
  if (_is_source_host_of(def)) {
    super::_build_update_merge_from_def(body, def, src_table, cond);
  }
}

string
incline_driver_sharded::do_build_direct_expr(const incline_def_async* _def,
					     const string& column_expr) const
{
  const incline_def_sharded* def
    = dynamic_cast<const incline_def_sharded*>(_def);
  assert(def != NULL);
  const rule* rl = rule_of(def->shard_file());
  assert(rl != NULL);
  return rl->build_expr_for(column_expr, cur_host_, cur_port_);
}

bool
incline_driver_sharded::_is_source_host_of(const incline_def_sharded* def) const
{
  const rule* rule = rule_of(def->shard_file());
  assert(rule != NULL);
  vector<connect_params> cp = rule->get_all_connect_params();
  for (vector<connect_params>::const_iterator ci = cp.begin();
       ci != cp.end();
       ++ci) {
    if (dynamic_cast<const shard_rule*>(rule) != NULL) {
      if (ci->host == cur_host_ && ci->port == cur_port_) {
	return true;
      }
    } else if (dynamic_cast<const replication_rule*>(rule) != NULL) {
      // TODO check if I am the source host
    } else {
      assert(0);
    }
  }
  return false;
}
