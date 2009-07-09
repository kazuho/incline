#ifndef incline_driver_sharded_h
#define incline_driver_sharded_h

#include "cac/cac_mutex.h"
#include "incline_driver_async_qtable.h"

class incline_driver_sharded : public incline_driver_async_qtable {
public:
  typedef incline_driver_async_qtable super;
  
  struct rule {
    virtual ~rule() {}
    virtual std::string parse(const picojson::value& def) = 0;
    virtual std::vector<std::string> get_all_hostport() const = 0;
    virtual std::string get_hostport_for(const std::string& key) const = 0;
    virtual std::string build_range_expr_for(const std::string& column_expr, const std::string& hostport) = 0;
  };
  
  class forwarder;
  class forwarder_mgr;
  
  class fw_writer {
  protected:
    template <typename T> struct to_writer_data_t {
      forwarder* forwarder_;
      T* payload_;
      int result_; // -1 if not ready, 0 if failed, 1 if true
      to_writer_data_t(forwarder* f, T* p) : forwarder_(f), payload_(p), result_(-1) {}
    };
    struct to_writer_t {
      pthread_cond_t from_writer_cond_;
      std::vector<to_writer_data_t<tmd::query_t>*> replace_rows_;
      std::vector<to_writer_data_t<const std::vector<std::string> >*> delete_rows_;
      to_writer_t() {
	pthread_cond_init(&from_writer_cond_, NULL);
      }
      ~to_writer_t() {
	pthread_cond_destroy(&from_writer_cond_);
      }
    };
    forwarder_mgr* mgr_;
    std::string hostport_;
    cac_mutex_t<to_writer_t*> to_writer_;
    to_writer_t to_writer_base_[2];
    pthread_cond_t to_writer_cond_;
    time_t retry_at_;
  public:
    fw_writer(forwarder_mgr* mgr, const std::string& hostport);
    ~fw_writer();
    bool is_active() const;
    bool replace_row(forwarder* forwarder, tmd::query_t& res);
    bool delete_row(forwarder* forwarder, const std::vector<std::string>& pk_values);
    void* run();
  protected:
    to_writer_t* _wait_and_swap();
    bool _commit(tmd::conn_t& dbh, to_writer_t& to_writer);
    void _set_result(to_writer_t& to_writer, bool result);
  };

  class forwarder : public incline_driver_async_qtable::forwarder {
    friend class fw_writer;
  public:
    typedef incline_driver_async_qtable::forwarder super;
  protected:
    forwarder_mgr* mgr_;
    size_t shard_index_in_replace_, shard_index_in_delete_;
  public:
    forwarder(forwarder_mgr* mgr, const incline_def_sharded* def, tmd::conn_t* dbh, int poll_interval);
    virtual bool do_replace_row(tmd::query_t& res);
    virtual bool do_delete_row(const std::vector<std::string>& pk_values);
  };
  
  class forwarder_mgr : public incline_driver_async_qtable::forwarder_mgr {
  public:
    typedef incline_driver_async_qtable::forwarder_mgr super;
  protected:
    std::map<std::string, fw_writer*> writers_;
  public:
    forwarder_mgr(incline_driver_sharded* driver, tmd::conn_t* (*connect)(const char* host, unsigned short port), const std::string& src_host, unsigned short src_port, int poll_interval) : super(driver, connect, src_host, src_port, poll_interval) {}
    ~forwarder_mgr() {}
    const incline_driver_sharded* get_driver() const {
      return static_cast<const incline_driver_sharded*>(driver_);
    }
    fw_writer* get_writer_for(const std::string& key) const {
      std::string hostport = get_driver()->get_rule()->get_hostport_for(key);
      std::map<std::string, fw_writer*>::const_iterator wi
	= writers_.find(hostport);
      assert(wi != writers_.end());
      return wi->second;
    }
    virtual void* run();
    tmd::conn_t* connect(const std::string& hostport);
  };
  
protected:
  rule* rule_;
  std::string cur_hostport_;
public:
  incline_driver_sharded() : rule_(NULL), cur_hostport_() {}
  virtual ~incline_driver_sharded() {
    delete rule_;
  }
  virtual incline_def* create_def() const;
  std::string parse_sharded_def(const picojson::value& def);
  const rule* get_rule() const { return rule_; }
  std::string set_hostport(const std::string& hostport);
protected:
  virtual std::string do_build_direct_expr(const std::string& column_expr) const;
};

#endif
