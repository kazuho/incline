#ifndef incline_def_async_h
#define incline_def_async_h

#include "incline_def.h"

class incline_def_async : public incline_def {
protected:
  std::string direct_expr_column_;
  std::string direct_expr_base_;
public:
  std::string direct_expr_column() const { return direct_expr_column_; }
  std::string direct_expr(const std::string& col_expr) const;
};

#endif
