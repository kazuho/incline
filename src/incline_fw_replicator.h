#ifndef incline_fw_replicator_h
#define incline_fw_replicator_h

#include "incline_driver_sharded.h"
#include "incline_fw.h"

class incline_fw_replicator : public incline_fw {
public:
  typedef incline_fw super;
  
  class manager : public super::manager {
  public:
    typedef super::manager super;
  public:
    manager(const incline_driver_sharded* driver, int poll_interval, int log_fd) : super(driver, poll_interval, log_fd) {}
    const incline_driver_sharded* driver() const { return static_cast<const incline_driver_sharded*>(super::driver()); }
    void start(std::vector<pthread_t>& threads);
  };
  
protected:
  incline_driver_sharded::connect_params dest_cp_;
public:
  incline_fw_replicator(manager* mgr, const incline_def_sharded* def, incline_dbms* dbh, const incline_driver_sharded::connect_params& dest_cp) : super(mgr, def, dbh), dest_cp_(dest_cp) {}
  const manager* mgr() const { return static_cast<const manager*>(super::mgr()); }
  manager* mgr() { return static_cast<manager*>(super::mgr()); }
  const incline_def_sharded* def() const { return static_cast<const incline_def_sharded*>(super::def()); }
  virtual void* run();
};

#endif
