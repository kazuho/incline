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
    std::vector<const std::vector<std::string>*>* insert_rows_, *update_rows_,
      * delete_rows_;
    bool success_;
    fw_writer_call_t(forwarder* f, std::vector<const std::vector<std::string>*>* insert_rows, std::vector<const std::vector<std::string>*>* update_rows, std::vector<const std::vector<std::string>*>* delete_rows) : forwarder_(f), insert_rows_(insert_rows), update_rows_(update_rows), delete_rows_(delete_rows), success_(false) {}
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
    forwarder(forwarder_mgr* mgr, const incline_def_sharded* def, incline_dbms* dbh, int poll_interval);
    const forwarder_mgr* mgr() const {
      return static_cast<const forwarder_mgr*>(super::mgr());
    }
    forwarder_mgr* mgr() { return static_cast<forwarder_mgr*>(super::mgr()); }
    const incline_def_sharded* def() const {
      return static_cast<const incline_def_sharded*>(super::def());
    }
    virtual bool do_update_rows(const std::vector<const std::vector<std::string>*>& insert_rows, const std::vector<const std::vector<std::string>*>& update_rows, const std::vector<const std::vector<std::string>*>& delete_rows);
    virtual std::string do_get_extra_cond();
  protected:
    void _setup_calls(std::map<fw_writer*, fw_writer_call_t*>& calls, const std::vector<const std::vector<std::string>*>& rows, std::vector<const std::vector<std::string>*>* fw_writer_call_t::*target_rows);
  };
  
  class forwarder_mgr : public incline_driver_async_qtable::forwarder_mgr {
  public:
    typedef incline_driver_async_qtable::forwarder_mgr super;
  protected:
    std::map<std::string, fw_writer*> writers_;
  public:
    forwarder_mgr(incline_driver_sharded* driver, int poll_interval, int log_fd) : super(driver, poll_interval, log_fd) {}
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
    incline_dbms* connect(const std::string& hostport);
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
  virtual forwarder_mgr* create_forwarder_mgr(int poll_interval, int log_fd) {
    return new forwarder_mgr(this, poll_interval, log_fd);
  }
  std::string parse_shard_def(const picojson::value& def);
  const rule* rule() const { return rule_; }
  std::string set_hostport(const std::string& hostport);
protected:
  virtual std::string do_build_direct_expr(const std::string& column_expr) const;
};

#endif
