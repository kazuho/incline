#ifndef incline_fw_replicator_h
#define incline_fw_replicator_h

#include "incline_driver_sharded.h"
#include "incline_fw.h"

class incline_fw_replicator : public incline_fw {
public:
  typedef incline_fw super;
  
  class manager : public super::manager {
  public:
    manager(incline_driver_sharded* driver, int poll_interval, FILE* log_fh) : super::manager(driver, poll_interval, log_fh) {}
    const incline_driver_sharded* driver() const { return static_cast<const incline_driver_sharded*>(super::manager::driver()); }
    incline_driver_sharded* driver() { return static_cast<incline_driver_sharded*>(super::manager::driver()); }
    void start(std::vector<pthread_t>& threads);
  };
  
protected:
  incline_driver_sharded::connect_params dest_cp_;
public:
  incline_fw_replicator(manager* mgr, const incline_def_sharded* def, incline_dbms* dbh, const incline_driver_sharded::connect_params& dest_cp) : super(mgr, def, dbh), dest_cp_(dest_cp) {}
  const manager* mgr() const { return static_cast<const manager*>(super::mgr()); }
  manager* mgr() { return static_cast<manager*>(super::mgr()); }
  const incline_def_sharded* def() const { return static_cast<const incline_def_sharded*>(super::def()); }
protected:
  virtual void do_run();
};

#endif
