#ifndef incline_driver_sharded_h
#define incline_driver_sharded_h

#include "incline_driver_async_qtable.h"

class incline_driver_sharded : public incline_driver_async_qtable {
public:
  typedef incline_driver_async_qtable super;
  struct rule {
    virtual ~rule() {}
    virtual std::string parse(const picojson::value& def) = 0;
    virtual std::vector<std::string> get_all_hostport() const = 0;
    virtual std::string get_hostport_for(const std::string& key) const = 0;
    virtual std::string build_direct_expr(const std::string& column_expr, const std::string& cur_hostport) = 0;
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
  void set_hostport(const std::string& hostport) { cur_hostport_ = hostport; }
protected:
  virtual std::string do_build_direct_expr(const std::string& column_expr);
};

#endif
