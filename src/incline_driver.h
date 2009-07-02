#ifndef incline_driver_h
#define incline_driver_h

#include <string>

class incline_def;
class incline_driver;
class incline_mgr;
namespace picojson {
  class value;
}

class incline_driver {
protected:
  friend class incline_mgr;
  incline_mgr* mgr_;
public:
  incline_driver() : mgr_(NULL) {}
  virtual ~incline_driver() {}
  virtual std::string insert_trigger_of(const std::string& src_table) const = 0;
  virtual std::string update_trigger_of(const std::string& src_table) const = 0;
  virtual std::string delete_trigger_of(const std::string& src_table) const = 0;
  virtual incline_def* create_def() const;
};

#endif
