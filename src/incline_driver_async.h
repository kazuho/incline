#ifndef incline_driver_async_h
#define incline_driver_async_h

#include <map>
#include "incline_driver_standalone.h"

class incline_driver_async : public incline_driver_standalone {
public:
  typedef incline_driver_standalone super;
  virtual incline_def* create_def() const;
protected:
  virtual std::vector<std::string> _build_insert_from_def(const incline_def* def, const std::string& src_table, const std::string& command, const std::vector<std::string>& cond = std::vector<std::string>()) const;
  virtual std::vector<std::string> _build_delete_from_def(const incline_def* def, const std::string& src_table, const std::vector<std::string>& cond = std::vector<std::string>()) const;
  virtual std::vector<std::string> _build_update_merge_from_def(const incline_def* def, const std::string& src_table, const std::vector<std::string>& cond = std::vector<std::string>()) const;
  std::vector<std::string> _build_enqueue_sql(const incline_def_async* def, const std::string& src_table, const std::string& alias, const std::vector<std::string>& cond = std::vector<std::string>()) const;
  virtual std::vector<std::string> do_build_enqueue_sql(const incline_def* def, const std::map<std::string, std::string>& pk_columns, const std::vector<std::string>& tables, const std::vector<std::string>& cond) const = 0;
  virtual std::string do_build_direct_expr(const std::string& direct_expr_column) const;
};

#endif
