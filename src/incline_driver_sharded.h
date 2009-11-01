#ifndef incline_driver_sharded_h
#define incline_driver_sharded_h

#include "interthr_call.h"
#include "incline_driver_async_qtable.h"

class incline_def_sharded;

class incline_driver_sharded : public incline_driver_async_qtable {
public:
  typedef incline_driver_async_qtable super;
  
  struct connect_params {
    std::string file;
    std::string host;
    unsigned short port;
    std::string username;
    std::string password;
    connect_params(const std::string& f) : file(f), host(), port(), username(), password() {}
    std::string parse(const picojson::value& def);
    incline_dbms* connect();
  };
  
  class rule {
  protected:
    std::string file_;
    time_t file_mtime_;
  public:
    rule(const std::string& file) : file_(file), file_mtime_(_get_file_mtime()) {}
    virtual ~rule() {}
    std::string file() const { return file_; }
    bool should_exit_loop() const;
    virtual std::vector<connect_params> get_all_connect_params() const = 0;
    virtual connect_params get_connect_params_for(const std::string& key) const = 0;
    virtual std::string build_expr_for(const std::string& column_expr, const std::string& host, unsigned short port) const = 0;
  protected:
    virtual std::string parse(const picojson::value& def) = 0;
    time_t _get_file_mtime() const;
  public:
    static rule* parse(const std::string& file, std::string& err);
  };
  
  class shard_rule : public rule {
  public:
    shard_rule(const std::string& file) : rule(file) {}
  };
  
  class replication_rule : public rule {
  public:
    replication_rule(const std::string& file) : rule(file) {}
  };
  
protected:
  std::vector<const rule*> rules_;
  std::string cur_host_;
  unsigned short cur_port_;
public:
  incline_driver_sharded() : rules_(), cur_host_(), cur_port_() {}
  virtual ~incline_driver_sharded();
  std::string init(const std::string& host, unsigned short port);
  virtual incline_def* create_def() const;
  virtual void run_forwarder(int poll_interval, int log_fd) const;
  std::string get_all_connect_params(std::vector<connect_params>& all_cp) const;
  virtual bool should_exit_loop() const;
  const rule* rule_of(const std::string& file) const;
  std::pair<std::string, unsigned short> get_hostport() const {
    return make_pair(cur_host_, cur_port_);
  }
protected:
  virtual void _build_insert_from_def(trigger_body& body, const incline_def* def, const std::string& src_table, action_t action, const std::vector<std::string>* cond) const;
  virtual void _build_delete_from_def(trigger_body& body, const incline_def* def, const std::string& src_table, const std::vector<std::string>& cond) const;
  virtual void _build_update_merge_from_def(trigger_body& body, const incline_def* def, const std::string& src_table, const std::vector<std::string>& cond) const;
  virtual std::string do_build_direct_expr(const incline_def_async* def, const std::string& column_expr) const;
  bool _is_source_host_of(const incline_def_sharded* def) const;
};

#endif
