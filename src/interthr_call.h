/* Copyright 2009 Cybozu Labs, Inc. All rights reserved.
 * 
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 * 
 * 1. Redistributions of source code must retain the above copyright notice,
 *    this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright notice,
 * this list of conditions and the following disclaimer in the documentation
 * and/or other materials provided with the distribution.
 * 
 * THIS SOFTWARE IS PROVIDED BY CYBOZU LABS, INC. ``AS IS'' AND ANY EXPRESS OR
 * IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO
 * EVENT SHALL CYBOZU LABS, INC. OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT,
 * INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
 * (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF
 * THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 * 
 * The views and conclusions contained in the software and documentation are
 * those of the authors and should not be interpreted as representing official
 * policies, either expressed or implied, of Cybozu Labs, Inc.
 */

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

#ifdef TEST_INTERTHR_CALL

#include <iostream>
#include "start_thread.h"

struct request {
  int arg_;
  int ret_;
  request(int arg) : arg_(arg), ret_(0) {}
};

class handler : public interthr_call_t<request> {
protected:
  virtual void do_handle_calls(slot_t& slot) {
    for (slot_t::iterator si = slot.begin(); si != slot.end(); ++si) {
      request* req((*si)->request());
      std::cout << "handler(" << req->arg_ << ") called" << std::endl;
      req->ret_ = req->arg_;
    }
  }
};

int main(void)
{
  // create handler and run two threads
  handler hdr;
  std::vector<pthread_t> hdr_thrs;
  hdr_thrs.push_back(start_thread(&hdr));
  hdr_thrs.push_back(start_thread(&hdr));
  
  // call handler (actually call them from multiple threads)
  for (int i = 0; i < 10; i++) {
    request r(i);
    hdr.call(r);
    assert(r.ret_ == i);
  }
  
  // terminate handler threads
  hdr.terminate();
  while (! hdr_thrs.empty()) {
    pthread_join(hdr_thrs.back(), NULL);
    hdr_thrs.pop_back();
  }
  
  return 0;
}

#endif
#endif
