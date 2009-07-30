#ifndef start_thread_h
#define start_thread_h

extern "C" {
#include <pthread.h>
}
#include <memory>

template <typename T> struct start_thread_t {
  pthread_t thr_;
  start_thread_t(T* obj) {
    pthread_create(&thr_, NULL, _run, obj);
  }
  static void* _run(void* obj) {
    return static_cast<T*>(obj)->run();
  }
};

template <typename T> pthread_t start_thread(T* obj)
{
  return start_thread_t<T>(obj).thr_;
}

template <typename T, typename A> struct start_thread_with_arg_t {
  pthread_t thr_;
  struct run_arg_t {
    T* obj_;
    A arg_;
    run_arg_t(T* obj, const A& arg) : obj_(obj), arg_(arg) {}
  };
  start_thread_with_arg_t(T* obj, const A& arg) {
    pthread_create(&thr_, NULL, _run, new run_arg_t(obj, arg));
  }
  static void* _run(void* _run_arg) {
    std::auto_ptr<run_arg_t> run_arg(reinterpret_cast<run_arg_t*>(_run_arg));
    void* ret = static_cast<T*>(run_arg->obj_)->run(run_arg->arg_);
    return ret;
  }
};

template <typename T, typename A> pthread_t start_thread(T* obj, const A& arg)
{
  return start_thread_with_arg_t<T, A>(obj, arg).thr_;
}

#endif
