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
#include <alloca.h>
#include <pthread.h>
}
#include <algorithm>
#include <cassert>
#include <cstdlib>
#include <vector>
#include <map>
#include "cac/cac_mutex.h"

template <typename Handler, typename Request>
struct interthr_call_t {
public:
  class call_info_t {
    friend class interthr_call_t<Handler, Request>;
  protected:
    Request* req_;
    pthread_cond_t* ret_cond_; // set to NULL upon completion
    size_t* remaining_calls_; // may be null
  public:
    call_info_t(Request* req, pthread_cond_t* ret_cond, size_t* remaining_calls) : req_(req), ret_cond_(ret_cond), remaining_calls_(remaining_calls) {}
    Request* request() { return req_; }
  };
  typedef std::vector<call_info_t*> slot_t;
protected:
  struct info_t {
    pthread_cond_t to_worker_cond_;
    slot_t* active_slot_;
    bool terminate_;
    info_t() : active_slot_(NULL), terminate_(false) {
      pthread_cond_init(&to_worker_cond_, NULL);
      active_slot_ = new slot_t();
    }
    ~info_t() {
      delete active_slot_;
      pthread_cond_destroy(&to_worker_cond_);
    }
  };
  cac_mutex_t<info_t> info_;
  pthread_key_t handle_slot_key_;
public:
  interthr_call_t() : info_(NULL) {
    pthread_key_create(&handle_slot_key_, NULL);
  }
  ~interthr_call_t() {
    assert(info_.unsafe_ref()->active_slot_->empty());
    pthread_key_delete(handle_slot_key_);
  }
  bool terminate_requested() const {
    typename cac_mutex_t<info_t>::const_lockref info(info_);
    return info->terminate_;
  }
  void terminate() {
    typename cac_mutex_t<info_t>::lockref info(info_);
    info->terminate_ = true;
    pthread_cond_broadcast(&info->to_worker_cond_);
  }
  template <typename Arg> void* run(Arg& arg) {
    slot_t* handle_slot = new slot_t();
    pthread_setspecific(handle_slot_key_, &handle_slot);
    try {
      void *ret = static_cast<Handler*>(this)->do_handle_calls(arg);
      delete handle_slot;
      return ret;
    } catch (...) {
      delete handle_slot;
      throw;
    }
  }
  void call(Request& req) {
    pthread_cond_t ret_cond;
    pthread_cond_init(&ret_cond, NULL);
    call_info_t ci(&req, &ret_cond, NULL);
    {
      typename cac_mutex_t<info_t>::lockref info(info_);
      info->active_slot_->push_back(&ci);
      pthread_cond_signal(&info->to_worker_cond_);
      while (ci.ret_cond_ != NULL) {
	pthread_cond_wait(&ret_cond, info_.mutex());
      }
    }
    pthread_cond_destroy(&ret_cond);
  }
protected:
  slot_t& get_slot() {
    slot_t*& handle_slot =
      *reinterpret_cast<slot_t**>(pthread_getspecific(handle_slot_key_));
    typename cac_mutex_t<info_t>::lockref info(info_);
    bool multi_mutex_is_locked = false;
    for (typename slot_t::iterator i = handle_slot->begin();
	 i != handle_slot->end();
	 ++i) {
      if ((*i)->remaining_calls_ == NULL) {
	// single-call
	pthread_cond_signal((*i)->ret_cond_);
      } else {
	// multi-call
	if (! multi_mutex_is_locked) {
	  multi_mutex_is_locked = true;
	  pthread_mutex_lock(&multi_mutex_);
	}
	if (--*(*i)->remaining_calls_ == 0) {
	  pthread_cond_signal((*i)->ret_cond_);
	}
      }
      (*i)->ret_cond_ = NULL;
    }
    if (multi_mutex_is_locked) {
      pthread_mutex_unlock(&multi_mutex_);
    }
    handle_slot->clear();
    while (info->active_slot_->empty()) {
      if (info->terminate_) {
	return *handle_slot;
      }
      pthread_cond_wait(&info->to_worker_cond_, info_.mutex());
    }
    std::swap(handle_slot, info->active_slot_);
    return *handle_slot;
  }
protected:
  static pthread_mutex_t multi_mutex_;
public:
  template <typename Iter> static void call(const Iter& first, const Iter& last) {
    size_t remain = std::distance(first, last);
    if (remain == 0) {
      return;
    } else if (remain == 1) {
      Handler* handler = first->first;
      Request* request = first->second;
      handler->call(*request);
      return;
    }
    call_info_t* ci
      = static_cast<call_info_t*>(alloca(sizeof(call_info_t) * remain));
    pthread_cond_t ret_cond;
    pthread_cond_init(&ret_cond, NULL);
    for (Iter it = first; it != last; ++it, ++ci) {
      Handler* handler = it->first;
      Request* request = it->second;
      new (ci) call_info_t(request, &ret_cond, &remain);
      typename cac_mutex_t<info_t>::lockref info(handler->info_);
      info->active_slot_->push_back(ci);
      pthread_cond_signal(&info->to_worker_cond_);
    }
    pthread_mutex_lock(&multi_mutex_);
    while (remain != 0) {
      pthread_cond_wait(&ret_cond, &multi_mutex_);
    }
    pthread_mutex_unlock(&multi_mutex_);
    pthread_cond_destroy(&ret_cond);
  }
};

template<typename Handler, typename Request>
pthread_mutex_t interthr_call_t<Handler, Request>::multi_mutex_ = PTHREAD_MUTEX_INITIALIZER;

#ifdef TEST_INTERTHR_CALL

#include <iostream>
#include "start_thread.h"

struct request {
  int arg_;
  int ret_;
  request(int arg) : arg_(arg), ret_(0) {}
};

struct handler : public interthr_call_t<handler, request> {
  void* do_handle_calls(int thr_id) {
    while (! terminate_requested()) {
      slot_t& slot = get_slot();
      for (slot_t::iterator si = slot.begin(); si != slot.end(); ++si) {
	request* req((*si)->request());
	std::cout << "handler(" << req->arg_ << ") called, thr_id:" << thr_id
		  << std::endl;
	req->ret_ = req->arg_;
      }
    }
    return NULL;
  }
};

int main(void)
{
  // create handler and run two threads
  handler hdr;
  std::vector<pthread_t> hdr_thrs;
  hdr_thrs.push_back(start_thread(&hdr, 1));
  hdr_thrs.push_back(start_thread(&hdr, 2));
  
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
