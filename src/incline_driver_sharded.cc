extern "C" {
#include <stdint.h>
}
#include <cassert>
#include <sstream>
#include "tmd.h"
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
    virtual string build_range_expr_for(const string& column_expr,
					const string& hostport) {
      typename map<KEYTYPE, string>::const_iterator i;
      for (i = lb_hostport_.begin(); i != lb_hostport_.end(); ++i) {
	if (i->second == hostport) {
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
  return rule_->build_range_expr_for(column_expr, cur_hostport_);
}

incline_driver_sharded::fw_writer::fw_writer(forwarder_mgr* mgr,
					     const string& hostport)
  : mgr_(mgr), hostport_(hostport), to_writer_(NULL), retry_at_(0)
{
  *to_writer_.unsafe_ref() = to_writer_base_;
  pthread_cond_init(&to_writer_cond_, NULL);
}

incline_driver_sharded::fw_writer::~fw_writer()
{
  pthread_cond_destroy(&to_writer_cond_);
}

bool
incline_driver_sharded::fw_writer::is_active() const
{
  return retry_at_ == 0 || retry_at_ <= time(NULL);
}

bool
incline_driver_sharded::fw_writer::replace_row(forwarder* forwarder,
					       tmd::query_t& res)
{
  to_writer_data_t<tmd::query_t> info(forwarder, &res);
  cac_mutex_t<to_writer_t*>::lockref to_writer(to_writer_);
  (*to_writer)->replace_rows_.push_back(&info);
  pthread_cond_signal(&to_writer_cond_);
  pthread_cond_t* wait_cond = &(*to_writer)->from_writer_cond_;
  while (info.result_ == -1) {
    pthread_cond_wait(wait_cond, to_writer_.mutex());
  }
  return info.result_ == 1;
}

bool
incline_driver_sharded::fw_writer::delete_row(forwarder* forwarder,
					      const vector<string>& pk_values)
{
  to_writer_data_t<const vector<string> > info(forwarder, &pk_values);
  cac_mutex_t<to_writer_t*>::lockref to_writer(to_writer_);
  (*to_writer)->delete_rows_.push_back(&info);
  pthread_cond_signal(&to_writer_cond_);
  pthread_cond_t* wait_cond = &(*to_writer)->from_writer_cond_;
  while (info.result_ == -1) {
    pthread_cond_wait(wait_cond, to_writer_.mutex());
  }
  return info.result_ == 1;
}

void
incline_driver_sharded::fw_writer::_run()
{
  tmd::conn_t* dbh = NULL;
  
  while (1) {
    // fetch request
    to_writer_t* to_writer = _wait_and_swap();
    // connect to db if necessary
    if (dbh == NULL && retry_at_ != 0 && retry_at_ <= time(NULL)) {
      dbh = mgr_->connect(hostport_);
      retry_at_ = 0;
    }
    // if connected, try commiting data
    if (dbh != NULL && _commit(*dbh, *to_writer)) {
      _set_result(*to_writer, true);
    } else {
      _set_result(*to_writer, false);
      if (dbh != NULL) {
	delete dbh;
	dbh = NULL;
      }
      retry_at_ = time(NULL) + 10;
    }
    // notify forwarders
    pthread_cond_broadcast(&to_writer->from_writer_cond_);
    // clear to_writer
    to_writer->replace_rows_.clear();
    to_writer->delete_rows_.clear();
  }
}

incline_driver_sharded::fw_writer::to_writer_t*
incline_driver_sharded::fw_writer::_wait_and_swap()
{
  cac_mutex_t<to_writer_t*>::lockref to_writer(to_writer_);
  while ((*to_writer)->replace_rows_.empty()
	 && (*to_writer)->delete_rows_.empty()) {
    pthread_cond_wait(&to_writer_cond_, to_writer_.mutex());
  }
  to_writer_t* r = *to_writer;
  *to_writer = to_writer_base_ + (r == to_writer_base_);
  return r;
}

bool
incline_driver_sharded::fw_writer::_commit(tmd::conn_t& dbh,
					   to_writer_t& to_writer)
{
  try {
    tmd::execute(dbh, "BEGIN TRANSACTION");
    for (vector<to_writer_data_t<tmd::query_t>*>::iterator ri
	   = to_writer.replace_rows_.begin();
	 ri != to_writer.replace_rows_.end();
	 ++ri) {
      (*ri)->forwarder_->replace_row(dbh, *(*ri)->payload_);
    }
    for (vector<to_writer_data_t<const vector<string> >*>::iterator di
	   = to_writer.delete_rows_.begin();
	 di != to_writer.delete_rows_.end();
	 ++di) {
      (*di)->forwarder_->delete_row(dbh, *(*di)->payload_);
      (*di)->result_ = 1;
    }
    tmd::execute(dbh, "COMMIT");
    return true;
  } catch (tmd::error_t err) {
    cerr << err.what() << endl;
    return false;
  }
}

void
incline_driver_sharded::fw_writer::_set_result(to_writer_t& to_writer,
					       bool result)
{
  for (vector<to_writer_data_t<tmd::query_t>*>::iterator ri
	 = to_writer.replace_rows_.begin();
       ri != to_writer.replace_rows_.end();
       ++ri) {
    (*ri)->result_ = result;
  }
  for (vector<to_writer_data_t<const vector<string> >*>::iterator di
	 = to_writer.delete_rows_.begin();
       di != to_writer.delete_rows_.end();
       ++di) {
    (*di)->result_ = result;
  }
}

incline_driver_sharded::forwarder::forwarder(forwarder_mgr* mgr,
					     const incline_def_sharded* def,
					     tmd::conn_t* dbh,
					     int poll_interval)
  : super(mgr->get_driver(), def, dbh, poll_interval), mgr_(mgr),
    shard_index_in_replace_(UINT_MAX), shard_index_in_delete_(0)
{
  // setup shard_index_in_replace_
  for (size_t i = 0; i < dest_columns_.size(); ++i) {
    if (dest_columns_[i] == def->direct_expr_column()) {
      shard_index_in_replace_ = i;
      break;
    }
  }
  assert(shard_index_in_replace_ != UINT_MAX);
  // setup shard_index_in_delete_
  for (map<string, string>::const_iterator pi = def->pk_columns().begin();
       pi != def->pk_columns().end();
       ++pi, ++shard_index_in_delete_) {
    if (pi->second == def->direct_expr_column()) {
      break;
    }
  }
  assert(shard_index_in_delete_ != def->pk_columns().size());
}

bool
incline_driver_sharded::forwarder::do_replace_row(tmd::query_t& res)
{
  return mgr_->get_writer_for(res.field(shard_index_in_replace_))
    ->replace_row(this, res);
}

bool
incline_driver_sharded::forwarder::do_delete_row(const vector<string>&
						 pk_values)
{
  return mgr_->get_writer_for(pk_values[shard_index_in_delete_])
    ->delete_row(this, pk_values);
}

incline_driver_sharded::forwarder_mgr::forwarder_mgr(incline_driver_sharded* driver, tmd::conn_t* (*connect)(const std::string&))
  : driver_(driver), connect_(connect)
{
}

incline_driver_sharded::forwarder_mgr::~forwarder_mgr()
{
  // TODO
}

incline_driver_sharded::forwarder*
incline_driver_sharded::forwarder_mgr::create_forwarder(incline_def* _def,
							tmd::conn_t* dbh,
							int poll_interval)
{
  const incline_def_sharded* def
    = dynamic_cast<const incline_def_sharded*>(_def);
  assert(def != NULL);
  return new forwarder(this, def, dbh, poll_interval);
}
