#ifndef incline_driver_async_qtable_h
#define incline_driver_async_qtable_h

#include "incline_driver_async.h"

class incline_def_async_qtable;
namespace tmd {
  class conn_t;
  class query_t;
}

class incline_driver_async_qtable : public incline_driver_async {
public:
  
  class forwarder_mgr;
  class forwarder {
    friend class incline_driver_async_qtable;
  protected:
    forwarder_mgr* mgr_;
    const incline_def_async_qtable* def_;
    tmd::conn_t* dbh_;
    int poll_interval_;
    std::vector<std::string> dest_pk_columns_;
    std::string copy_to_temp_query_base_;
    std::string fetch_pk_query_;
    std::string fetch_src_query_;
    std::string delete_queue_query_;
    std::string delete_temp_query_;
    std::string replace_row_query_base_;
    std::string delete_row_query_base_;
  public:
    forwarder(forwarder_mgr* mgr, const incline_def_async_qtable* def, tmd::conn_t* dbh, int poll_interval);
    virtual ~forwarder();
    const forwarder_mgr* mgr() const { return mgr_; }
    forwarder_mgr* mgr() { return mgr_; }
    const incline_def_async_qtable* def() const { return def_; }
    void* run();
  protected:
    virtual bool do_replace_row(tmd::query_t& res);
    virtual bool do_delete_row(const std::vector<std::string>& pk_values);
    virtual std::string do_get_extra_cond();
  protected:
    void replace_row(tmd::conn_t& dbh, tmd::query_t& res) const;
    void delete_row(tmd::conn_t& dbh, const std::vector<std::string>& pk_values) const;
  };
  
  class forwarder_mgr {
  protected:
    const incline_driver_async_qtable* driver_;
    tmd::conn_t* (*connect_)(const char* host, unsigned short port);
    const std::string src_host_;
    unsigned short src_port_;
    int poll_interval_;
  public:
    forwarder_mgr(incline_driver_async_qtable* driver, tmd::conn_t* (*connect)(const char*, unsigned short), const std::string& src_host, unsigned short src_port, int poll_interval) : driver_(driver), connect_(connect), src_host_(src_host), src_port_(src_port), poll_interval_(poll_interval) {}
    virtual ~forwarder_mgr() {}
    const incline_driver_async_qtable* driver() const { return driver_; }
    virtual void* run();
  protected:
    virtual forwarder* do_create_forwarder(const incline_def_async_qtable* def);
  };
  
public:
  virtual incline_def* create_def() const;
  std::vector<std::string> create_table_all(bool if_not_exists, tmd::conn_t& dbh) const;
  std::vector<std::string> drop_table_all(bool if_exists) const;
  std::string create_table_of(const incline_def* def, bool if_not_exists, tmd::conn_t& dbh) const;
  std::string drop_table_of(const incline_def* def, bool if_exists) const;
  virtual forwarder_mgr* create_forwarder_mgr(tmd::conn_t* (*connect)(const char*, unsigned short), const std::string& src_host, unsigned short src_port, int poll_interval) {
    return new forwarder_mgr(this, connect, src_host, src_port, poll_interval);
  }
protected:
  std::string _create_table_of(const incline_def_async_qtable* def, const std::string& table_name, bool temporary, bool if_not_exists, tmd::conn_t& dbh) const;
  virtual std::vector<std::string> do_build_enqueue_sql(const incline_def* def, const std::map<std::string, std::string>& pk_columns, const std::vector<std::string>& tables, const std::vector<std::string>& cond) const;
};

#endif
