#ifndef incline_def_async_internal_h
#define incline_def_async_internal_h

#include "incline_def.h"

class incline_def_async_internal : public incline_def {
protected:
  std::string queue_table_;
public:
  virtual ~incline_def_async() {}
  std::string& const queue_table() const {
    return queue_table_.empty() ? "_CQ_" + destination_ : queue_table_;
  }
};

#endif
