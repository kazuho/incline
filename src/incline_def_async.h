#ifndef incline_def_async_h
#define incline_def_async_h

#include "incline_def.h"

class incline_def_async : public incline_def {
public:
  typedef incline_def super;
protected:
  std::string direct_expr_column_;
  std::string direct_expr_base_;
public:
  std::string direct_expr_column() const { return direct_expr_column_; }
  std::string direct_expr(const std::string& col_expr) const;
  virtual std::string parse(const picojson::value& def);
protected:
  virtual std::string do_parse_property(const std::string& name, const picojson::value& value);
};

#endif
