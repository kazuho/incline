#ifndef incline_mgr_h
#define incline_mgr_h

#include "incline_def.h"

class incline_def;
class incline_driver;

class incline_mgr {
protected:
  std::vector<incline_def*> defs_;
  incline_driver* driver_;
  std::string trigger_time_;
public:
  incline_mgr(incline_driver* d);
  virtual ~incline_mgr();
  const std::vector<incline_def*>& defs() const { return defs_; }
  incline_driver* driver() { return driver_; }
  std::string trigger_time() const { return trigger_time_; }
  std::string parse(const picojson::value& src);
  std::vector<std::string> get_src_tables() const;
  std::vector<std::string> create_trigger_all(bool drop_if_exists) const;
  std::vector<std::string> drop_trigger_all(bool drop_if_exists) const;
  std::vector<std::string> insert_trigger_of(const std::string& src_table) const;
  std::vector<std::string> update_trigger_of(const std::string& src_table) const;
  std::vector<std::string> delete_trigger_of(const std::string& src_table) const;
  std::vector<std::string> drop_trigger_of(const std::string& src_table, const std::string& event, bool if_exists) const;
  std::vector<std::string> build_trigger_stmt(const std::string& src_table, const std::string& event, const std::vector<std::string>& body) const;
protected:
  std::string _build_trigger_name(const std::string& src_table, const std::string& event) const;
private:
  incline_mgr(const incline_mgr&); // not copyable
  incline_mgr& operator=(const incline_mgr&);
};

#endif
