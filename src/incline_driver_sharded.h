#ifndef incline_driver_sharded_h
#define incline_driver_sharded_h

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
  public:
    static const connect_params* find(const std::vector<connect_params>& cp, const std::string& host, unsigned short port);
  };
  
  class rule {
  protected:
    std::string file_;
    unsigned long long file_mtime_;
  public:
    rule(const std::string& file) : file_(file), file_mtime_(_get_file_mtime()) {}
    virtual ~rule() {}
    std::string file() const { return file_; }
    bool should_exit_loop() const;
  protected:
    virtual std::string parse(const picojson::value& def) = 0;
    unsigned long long _get_file_mtime() const;
  public:
    static rule* parse(const std::string& file, std::string& err);
  };
  
  class shard_rule : public rule {
  public:
    shard_rule(const std::string& file) : rule(file) {}
    virtual std::vector<connect_params> get_all_connect_params() const = 0;
    virtual connect_params get_connect_params_for(const std::string& key) const = 0;
    virtual std::string build_expr_for(const std::string& column_expr, const std::string& host, unsigned short port) const = 0;
  };
  
  class replicator_rule : public rule {
  protected:
    connect_params src_cp_;
    std::vector<connect_params> dest_cp_;
  public:
    replicator_rule(const std::string& file) : rule(file), src_cp_(file), dest_cp_() {}
    const connect_params& source() const { return src_cp_; }
    const std::vector<connect_params>& destination() const { return dest_cp_; }
  protected:
    virtual std::string parse(const picojson::value& def);
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
  virtual std::vector<std::string> create_table_all(bool if_not_exists, incline_dbms* dbh) const;
  virtual std::vector<std::string> drop_table_all(bool if_exists) const;
  virtual void run_forwarder(int poll_interval, FILE* log_fh);
  virtual bool should_exit_loop() const;
  const rule* rule_of(const std::string& file) const;
  std::pair<std::string, unsigned short> get_hostport() const {
    return make_pair(cur_host_, cur_port_);
  }
  bool is_src_host_of(const incline_def_sharded* def) const;
  bool is_replicator_dest_host_of(const incline_def_sharded* def) const;
protected:
  virtual void _build_insert_from_def(trigger_body& body, const incline_def* def, const std::string& src_table, action_t action, const std::vector<std::string>* cond) const;
  virtual void _build_delete_from_def(trigger_body& body, const incline_def* def, const std::string& src_table, const std::vector<std::string>& cond) const;
  virtual void _build_update_merge_from_def(trigger_body& body, const incline_def* def, const std::string& src_table, const std::vector<std::string>& cond) const;
  virtual std::string do_build_direct_expr(const incline_def_async* def, const std::string& column_expr) const;
};

#endif
