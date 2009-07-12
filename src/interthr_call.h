#ifndef interthr_call_h
#define interthr_call_h

extern "C" {
#include <pthread.h>
}
#include <algorithm>
#include <cassert>
#include <vector>
#include "cac/cac_mutex.h"

template <typename Request> struct interthr_call_t {
public:
  class call_info_t {
    friend class interthr_call_t<Request>;
  protected:
    Request* req_;
    pthread_cond_t ret_cond_;
    bool ready_;
  public:
    call_info_t(Request* req) : req_(req), ready_(false) {
      pthread_cond_init(&ret_cond_, NULL);
    }
    ~call_info_t() {
      pthread_cond_destroy(&ret_cond_);
    }
    Request* request() { return req_; }
  };
  typedef std::vector<call_info_t*> slot_t;
protected:
  struct info_t {
    slot_t* active_slot_;
    pthread_cond_t to_worker_cond_;
    bool terminate_;
    info_t() : active_slot_(new slot_t()), terminate_(false) {
      pthread_cond_init(&to_worker_cond_, NULL);
    }
    ~info_t() {
      pthread_cond_destroy(&to_worker_cond_);
      delete active_slot_;
    }
  };
  cac_mutex_t<info_t> info_;
public:
  interthr_call_t() : info_(NULL) {}
  virtual ~interthr_call_t() {
    assert(info_.unsafe_ref()->active_slot_->empty());
  }
  void terminate() {
    typename cac_mutex_t<info_t>::lockref info(info_);
    info->terminate_ = true;
    pthread_cond_broadcast(&info->to_worker_cond_);
  }
  void* run() {
    slot_t* handle_slot = new slot_t();
    while (1) {
      {
	typename cac_mutex_t<info_t>::lockref info(info_);
	for (typename slot_t::iterator i = handle_slot->begin();
	     i != handle_slot->end();
	     ++i) {
	  (*i)->ready_ = true;
	  pthread_cond_signal(&(*i)->ret_cond_);
	}
	handle_slot->clear();
	while (info->active_slot_->empty()) {
	  if (info->terminate_) {
	    goto EXIT;
	  }
	  pthread_cond_wait(&info->to_worker_cond_, info_.mutex());
	}
	std::swap(handle_slot, info->active_slot_);
      }
      do_handle_calls(*handle_slot);
    }
  EXIT:
    delete handle_slot;
    return NULL;
  }
  void call(Request& req) {
    call_info_t ci(&req);
    typename cac_mutex_t<info_t>::lockref info(info_);
    info->active_slot_->push_back(&ci);
    pthread_cond_signal(&info->to_worker_cond_);
    while (! ci.ready_) {
      pthread_cond_wait(&ci.ret_cond_, info_.mutex());
    }
  }
protected:
  virtual void do_handle_calls(slot_t& slot) = 0;
};

#endif
