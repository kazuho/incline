#ifndef incline_driver_standalone_h
#define incline_driver_standalone_h

#include <vector>
#include "incline_driver.h"

class incline_def;

class incline_driver_standalone : public incline_driver {
public:
  virtual std::string insert_trigger_of(const std::string& src_table) const;
  virtual std::string update_trigger_of(const std::string& src_table) const;
  virtual std::string delete_trigger_of(const std::string& src_table) const;
protected:
  virtual std::vector<std::string> _build_insert_from_def(const incline_def* def, const std::string& src_table, const std::string& command, const std::vector<std::string>& cond = std::vector<std::string>()) const;
  virtual std::vector<std::string> _build_delete_from_def(const incline_def* def, const std::string& src_table, const std::vector<std::string>& cond = std::vector<std::string>()) const;
  virtual std::vector<std::string> _build_update_merge_from_def(const incline_def* def, const std::string& src_table, const std::vector<std::string>& cond = std::vector<std::string>()) const;
  std::vector<std::string> _merge_cond_of(const incline_def* def, const std::string& src_table) const;
};

#endif
