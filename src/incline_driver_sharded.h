#ifndef incline_driver_sharded_h
#define incline_driver_sharded_h

#include "interthr_call.h"
#include "incline_driver_async_qtable.h"

class incline_driver_sharded : public incline_driver_async_qtable {
public:
  typedef incline_driver_async_qtable super;
  
  struct rule {
    virtual ~rule() {}
    virtual std::string parse(const picojson::value& def) = 0;
    virtual std::vector<std::string> get_all_hostport() const = 0;
    virtual std::string get_hostport_for(const std::string& key) const = 0;
    virtual std::string build_expr_for(const std::string& column_expr, const std::string& hostport) const = 0;
  };
  
  class forwarder;
  class forwarder_mgr;
  
  struct fw_writer_call_t {
    forwarder* forwarder_;
    const std::vector<const std::vector<std::string>*>* replace_rows_,
      * delete_rows_;
    bool success_;
    fw_writer_call_t(forwarder* f, const std::vector<const std::vector<std::string>*>* replace_rows, const std::vector<const std::vector<std::string>*>* delete_rows) : forwarder_(f), replace_rows_(replace_rows), delete_rows_(delete_rows), success_(false) {}
  };
  
  class fw_writer : public interthr_call_t<fw_writer, fw_writer_call_t> {
  protected:
    forwarder_mgr* mgr_;
    std::string hostport_;
    time_t retry_at_;
  public:
    fw_writer(forwarder_mgr* mgr, const std::string& hostport) : mgr_(mgr), hostport_(hostport), retry_at_(0) {}
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
    forwarder(forwarder_mgr* mgr, const incline_def_sharded* def, tmd::conn_t* dbh, int poll_interval);
    const forwarder_mgr* mgr() const {
      return static_cast<const forwarder_mgr*>(super::mgr());
    }
    forwarder_mgr* mgr() { return static_cast<forwarder_mgr*>(super::mgr()); }
    const incline_def_sharded* def() const {
      return static_cast<const incline_def_sharded*>(super::def());
    }
    virtual bool do_update_rows(const std::vector<std::vector<std::string> >& update_rows, const std::vector<std::vector<std::string> >& delete_rows);
    void map_rows_to_writers(std::map<fw_writer*, std::vector<const std::vector<std::string>*> >& writer_rows, const std::vector<std::vector<std::string> >& rows);
    virtual std::string do_get_extra_cond();
  };
  
  class forwarder_mgr : public incline_driver_async_qtable::forwarder_mgr {
  public:
    typedef incline_driver_async_qtable::forwarder_mgr super;
  protected:
    std::map<std::string, fw_writer*> writers_;
  public:
    forwarder_mgr(incline_driver_sharded* driver, tmd::conn_t* (*connect)(const char* host, unsigned short port), const std::string& src_host, unsigned short src_port, int poll_interval, int log_fd) : super(driver, connect, src_host, src_port, poll_interval, log_fd) {}
    const incline_driver_sharded* driver() const {
      return static_cast<const incline_driver_sharded*>(super::driver());
    }
    const std::map<std::string, fw_writer*> writers() { return writers_; }
    fw_writer* get_writer_for(const std::string& key) const {
      std::string hostport = driver()->rule()->get_hostport_for(key);
      std::map<std::string, fw_writer*>::const_iterator wi
	= writers_.find(hostport);
      assert(wi != writers_.end());
      return wi->second;
    }
    virtual void* run();
    tmd::conn_t* connect(const std::string& hostport);
  protected:
    virtual forwarder* do_create_forwarder(const incline_def_async_qtable* def);
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
  virtual forwarder_mgr* create_forwarder_mgr(tmd::conn_t* (*connect)(const char*, unsigned short), const std::string& src_host, unsigned short src_port, int poll_interval, int log_fd) {
    return new forwarder_mgr(this, connect, src_host, src_port, poll_interval,
			     log_fd);
  }
  std::string parse_shard_def(const picojson::value& def);
  const rule* rule() const { return rule_; }
  std::string set_hostport(const std::string& hostport);
protected:
  virtual std::string do_build_direct_expr(const std::string& column_expr) const;
};

#endif
