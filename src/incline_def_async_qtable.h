#ifndef incline_def_async_qtable_h
#define incline_def_async_qtable_h

#include "incline_def_async.h"

class incline_def_async_qtable : public incline_def_async {
protected:
  std::string queue_table_;
public:
  std::string queue_table() const {
    return queue_table_.empty() ? "_CQ_" + destination_ : queue_table_;
  }
};

#endif
