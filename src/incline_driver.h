#ifndef incline_driver_h
#define incline_driver_h

#include <string>
#include <vector>
#include <map>

class incline_def;
class incline_driver;
class incline_mgr;
namespace picojson {
  class value;
}

class incline_driver {
public:
  enum action_t {
    act_insert = 'I',
    act_update = 'U',
    act_delete = 'D'
  };
  struct trigger_body {
    std::map<std::string, std::string> var; // varname => type
    std::vector<std::string> stmt;
  };
protected:
  friend class incline_mgr;
  incline_mgr* mgr_;
public:
  incline_driver() : mgr_(NULL) {}
  virtual ~incline_driver() {}
  const incline_mgr* mgr() const { return mgr_; }
  incline_mgr* mgr() { return mgr_; }
  virtual void insert_trigger_of(trigger_body& body, const std::string& src_table) const = 0;
  virtual void update_trigger_of(trigger_body& body, const std::string& src_table) const = 0;
  virtual void delete_trigger_of(trigger_body& body, const std::string& src_table) const = 0;
  virtual incline_def* create_def() const;
};

#endif
