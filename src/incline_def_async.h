#ifndef incline_def_async_h
#define incline_def_async_h

#include "incline_def.h"

class incline_def_async : public incline_def {
public:
  typedef incline_def super;
protected:
  std::string direct_expr_column_;
public:
  std::string direct_expr_column() const { return direct_expr_column_; }
  std::string direct_expr_column(const std::string& desired_table, const std::string& table_rewrite_to = std::string()) const;
  virtual std::string parse(const picojson::value& def);
protected:
  void set_direct_expr_column(const std::string& col) {
    direct_expr_column_ = col;
  }
};

#endif
