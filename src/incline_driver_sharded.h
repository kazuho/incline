#ifndef incline_driver_sharded_h
#define incline_driver_sharded_h

#include "interthr_call.h"
#include "incline_driver_async_qtable.h"

class incline_driver_sharded : public incline_driver_async_qtable {
public:
  typedef incline_driver_async_qtable super;
  
  struct connect_params {
    std::string host;
    unsigned short port;
    std::string username;
    std::string password;
    connect_params() : host(), port(), username(), password() {}
    std::string parse(const picojson::value& def);
  };
  
  struct rule {
    virtual ~rule() {}
    virtual std::string parse(const picojson::value& def) = 0;
    virtual std::vector<connect_params> get_all_connect_params() const = 0;
    virtual connect_params get_connect_params_for(const std::string& key) const = 0;
    virtual std::string build_expr_for(const std::string& column_expr, const std::string& host, unsigned short port) const = 0;
  };
  
  class forwarder;
  class forwarder_mgr;
  
  struct fw_writer_call_t {
    forwarder* forwarder_;
    std::vector<const std::vector<std::string>*>* insert_rows_, * delete_rows_;
    bool success_;
    fw_writer_call_t(forwarder* f, std::vector<const std::vector<std::string>*>* insert_rows, std::vector<const std::vector<std::string>*>* update_rows, std::vector<const std::vector<std::string>*>* delete_rows) : forwarder_(f), insert_rows_(insert_rows), delete_rows_(delete_rows), success_(false) {}
  };
  
  class fw_writer : public interthr_call_t<fw_writer, fw_writer_call_t> {
  protected:
    forwarder_mgr* mgr_;
    connect_params connect_params_;
    time_t retry_at_;
  public:
  fw_writer(forwarder_mgr* mgr, const connect_params& cp) : mgr_(mgr), connect_params_(cp), retry_at_(0) {}
    bool is_active() const {
      return retry_at_ == 0 || retry_at_ <= time(NULL);
    }
    void* do_handle_calls(int);
  };

  class forwarder : public incline_driver_async_qtable::forwarder {
    friend class fw_writer;
  public:
    typedef incline_driver_async_qtable::forwarder super;
  protected:
    size_t shard_col_index_; // used for replace and delete, they are the same
  public:
    forwarder(forwarder_mgr* mgr, const incline_def_sharded* def, incline_dbms* dbh, int poll_interval);
    const forwarder_mgr* mgr() const {
      return static_cast<const forwarder_mgr*>(super::mgr());
    }
    forwarder_mgr* mgr() { return static_cast<forwarder_mgr*>(super::mgr()); }
    const incline_def_sharded* def() const {
      return static_cast<const incline_def_sharded*>(super::def());
    }
    virtual bool do_update_rows(const std::vector<const std::vector<std::string>*>& delete_rows, const std::vector<const std::vector<std::string>*>& insert_rows);
    virtual std::string do_get_extra_cond();
  protected:
    void _setup_calls(std::map<fw_writer*, fw_writer_call_t*>& calls, const std::vector<const std::vector<std::string>*>& rows, std::vector<const std::vector<std::string>*>* fw_writer_call_t::*target_rows);
  };
  
  class forwarder_mgr : public incline_driver_async_qtable::forwarder_mgr {
  public:
    typedef incline_driver_async_qtable::forwarder_mgr super;
  protected:
    std::vector<std::pair<connect_params, fw_writer*> > writers_;
  public:
    forwarder_mgr(incline_driver_sharded* driver, int poll_interval, int log_fd) : super(driver, poll_interval, log_fd), writers_() {}
    const std::vector<std::pair<connect_params, fw_writer*> > writers() const {
      return writers_;
    }
    const incline_driver_sharded* driver() const {
      return static_cast<const incline_driver_sharded*>(super::driver());
    }
    fw_writer* get_writer_for(const std::string& key) const {
      connect_params cp = driver()->rule()->get_connect_params_for(key);
      for (std::vector<std::pair<connect_params, fw_writer*> >::const_iterator
	     wi = writers_.begin();
	   wi != writers_.end();
	   ++wi) {
	if (wi->first.host == cp.host && wi->first.port == cp.port) {
	  return wi->second;
	}
      }
      assert(0);
    }
    virtual void* run();
    incline_dbms* connect(const connect_params& cp);
  protected:
    virtual forwarder* do_create_forwarder(const incline_def_async_qtable* def);
  };
  
protected:
  rule* rule_;
  std::string cur_host_;
  unsigned short cur_port_;
  std::string shard_def_file_;
  time_t mtime_of_shard_def_file_;
public:
  incline_driver_sharded() : rule_(NULL), cur_host_(), cur_port_(), shard_def_file_(), mtime_of_shard_def_file_(0) {}
  virtual ~incline_driver_sharded() {
    delete rule_;
  }
  virtual incline_def* create_def() const;
  virtual forwarder_mgr* create_forwarder_mgr(int poll_interval, int log_fd) {
    return new forwarder_mgr(this, poll_interval, log_fd);
  }
  virtual bool should_exit_loop() const;
  std::string parse_shard_def(const std::string& shard_def_file);
  const rule* rule() const { return rule_; }
  std::pair<std::string, unsigned short> get_hostport() const {
    return make_pair(cur_host_, cur_port_);
  }
  std::string set_hostport(const std::string& host, unsigned short port);
protected:
  virtual std::string do_build_direct_expr(const std::string& column_expr) const;
  time_t _get_mtime_of_shard_def_file() const;
};

#endif
