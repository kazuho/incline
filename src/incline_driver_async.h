#ifndef incline_driver_async_h
#define incline_driver_async_h

#include "incline_driver_standalone.h"

class incline_def_async;

class incline_driver_async : public incline_driver_standalone {
public:
  typedef incline_driver_standalone super;
  virtual incline_def* create_def() const;
protected:
  virtual void _build_insert_from_def(trigger_body& body, const incline_def* def, const std::string& src_table, action_t action, const std::vector<std::string>* cond = NULL) const;
  virtual void _build_delete_from_def(trigger_body& body, const incline_def* def, const std::string& src_table, const std::vector<std::string>& cond = std::vector<std::string>()) const;
  virtual void _build_update_merge_from_def(trigger_body& body, const incline_def* def, const std::string& src_table, const std::vector<std::string>& cond = std::vector<std::string>()) const;
  virtual void do_build_enqueue_insert_sql(trigger_body& body, const incline_def* def, const std::string& src_table, action_t action, const std::vector<std::string>* cond) const = 0;
  virtual void do_build_enqueue_delete_sql(trigger_body& body, const incline_def* def, const std::string& src_table, const std::vector<std::string>* cond) const = 0;
  virtual std::string do_build_direct_expr(const incline_def_async* def, const std::string& column_expr) const;
};

#endif
