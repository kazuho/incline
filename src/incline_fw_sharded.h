#ifndef incline_fw_sharded_h
#define incline_fw_sharded_h

#include "incline_driver_sharded.h"
#include "incline_fw_async_qtable.h"

class incline_fw_sharded : public incline_fw_async_qtable {
public:
  typedef incline_fw_async_qtable super;
  class manager;
  
  struct writer_call_t {
    incline_fw_sharded* fw_;
    std::vector<std::vector<std::string> >* insert_rows_, * delete_rows_;
    bool success_;
    writer_call_t(incline_fw_sharded* fw, std::vector<std::vector<std::string> >* insert_rows, std::vector<std::vector<std::string> >* delete_rows) : fw_(fw), insert_rows_(insert_rows), delete_rows_(delete_rows), success_(false) {}
  };
  
  class writer : public interthr_call_t<writer, writer_call_t> {
  public:
    typedef interthr_call_t<writer, writer_call_t> super;
  protected:
    manager* mgr_;
    incline_driver_sharded::connect_params connect_params_;
    time_t retry_at_;
    pthread_t thr_;
  public:
    writer(manager* mgr, const incline_driver_sharded::connect_params& cp);
    void terminate();
    bool is_active() const {
      return retry_at_ == 0 || retry_at_ <= time(NULL);
    }
    void* do_handle_calls(int);
  };
  
  class manager : public incline_fw_async_qtable::manager {
  public:
    typedef incline_fw_async_qtable::manager super;
  protected:
    std::vector<std::pair<incline_driver_sharded::connect_params, writer*> > writers_;
  public:
    manager(incline_driver_sharded* driver, int poll_interval, FILE* log_fh) : super(driver, poll_interval, log_fh), writers_() {}
    virtual ~manager();
    const incline_driver_sharded* driver() const { return static_cast<const incline_driver_sharded*>(super::driver()); }
    incline_driver_sharded* driver() { return static_cast<incline_driver_sharded*>(super::driver()); }
    const incline_driver_sharded::shard_rule* rule_of(const incline_def_sharded* def) const;
    const std::vector<std::pair<incline_driver_sharded::connect_params, writer*> >& writers() const { return writers_; }
    void start(std::vector<pthread_t>& threads);
    writer* get_writer_for(const incline_def_sharded* def, const std::string& key) const;
  };
  
protected:
  size_t shard_col_index_; // used for replace and delete, they are the same
public:
  incline_fw_sharded(manager* mgr, const incline_def_sharded* def, incline_dbms* dbh);
  const manager* mgr() const { return static_cast<const manager*>(super::mgr()); }
  manager* mgr() { return static_cast<manager*>(super::mgr()); }
  const incline_def_sharded* def() const { return static_cast<const incline_def_sharded*>(super::def()); }
protected:
  virtual bool do_update_rows(const std::vector<std::vector<std::string> >& delete_rows, const std::vector<std::vector<std::string> >& insert_rows);
  virtual std::string do_get_extra_cond();
  void _setup_calls(std::map<writer*, writer_call_t*>& calls, const std::vector<std::vector<std::string> >& rows, std::vector<std::vector<std::string> >* writer_call_t::*target_rows);  
};

#endif
