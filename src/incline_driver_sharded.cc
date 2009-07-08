extern "C" {
#include <stdint.h>
}
#include <cassert>
#include <sstream>
#include "incline_def_sharded.h"
#include "incline_driver_sharded.h"
#include "incline_util.h"

using namespace std;

namespace incline_driver_sharded_ns {
  
  template <typename KEYTYPE> struct str_to_key_type {
    KEYTYPE operator()(const string& s) const {
      stringstream ss(s);
      KEYTYPE k;
      ss >> k;
      return k;
    }
  };
  
  template <> struct str_to_key_type<string> {
    string operator()(const string& s) const {
      return s;
    }
  };
  
  template <typename KEYTYPE> struct key_type_to_str {
    string operator()(const KEYTYPE& k) const {
      stringstream ss;
      ss << k;
      return ss.str();
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
      vector<string> r;
      for (typename map<KEYTYPE, string>::const_iterator i
	     = lb_hostport_.begin();
	   i != lb_hostport_.end();
	   ++i) {
	r.push_back(i->second);
      }
      return r;
    }
    virtual string get_hostport_for(const string& key) const {
      typename map<KEYTYPE, string>::const_iterator i
	= lb_hostport_.upper_bound(str_to_key_type<KEYTYPE>()(key));
      return i == lb_hostport_.begin() ? string() : (--i)->second;
    }
    virtual string build_direct_expr(const string& column_expr, const string& cur_hostport) {
      typename map<KEYTYPE, string>::const_iterator i;
      for (i = lb_hostport_.begin(); i != lb_hostport_.end(); ++i) {
	if (i->second == cur_hostport) {
	  break;
	}
      }
      assert(i != lb_hostport_.end());
      string cond;
      cond += key_type_to_str<KEYTYPE>()(i->first) + "<=" + column_expr;
      ++i;
      if (i != lb_hostport_.end()) {
	cond += string(" AND ") + column_expr + '<'
	  + key_type_to_str<KEYTYPE>()(i->first);
      }
      return cond;
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
incline_driver_sharded::parse_sharded_def(const picojson::value& def)
{
  if (! def.is<picojson::object>()) {
    return "definition should be ant object";
  }
  // get algorithm and build the rule
  string algo = def.get("algorithm").to_str();
#define RANGE_ALGO(id, type) \
  if (algo == "range-" id) rule_ = new range_rule<type>()
  RANGE_ALGO("int32", int32_t);
  RANGE_ALGO("uint32", uint32_t);
  RANGE_ALGO("int64", int64_t);
  RANGE_ALGO("uint64", uint64_t);
  RANGE_ALGO("string", string);
#undef RANGE_ALGO
  if (rule_ == NULL) {
    return string("unknown sharding algorithm: ") + algo;
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
  return rule_->build_direct_expr(column_expr, cur_hostport_);
}
