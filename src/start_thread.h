#ifndef start_thread_h
#define start_thread_h

extern "C" {
#include <pthread.h>
}

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

#endif
