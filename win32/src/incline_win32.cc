extern "C" {
#include <assert.h>
}
#include "incline_win32.h"

void iw32_sleep(unsigned int seconds)
{
  // bare minimum :-p
  Sleep(seconds * 1000);
}

struct iw32_thread_start {
  void* (*start_routine)(void*);
  void* arg;
  iw32_thread_start(void* (*r)(void*), void* a) : start_routine(r), arg(a) {}
};

static unsigned __stdcall start_cb(void* _ts)
{
  iw32_thread_start* tsp = static_cast<iw32_thread_start*>(_ts);
  iw32_thread_start ts(*tsp);
  delete tsp;
  ts.start_routine(ts.arg);
  return 0;
}

int iw32_pthread_create(HANDLE* thread, const void* _unused, void* (*start_routine)(void*), void* arg)
{
  assert(_unused == NULL);
  iw32_thread_start* ts = new iw32_thread_start(start_routine, arg);
  *thread = (HANDLE)_beginthreadex(NULL, 0, start_cb, ts, 0, NULL);
  if (*thread == INVALID_HANDLE_VALUE) {
    delete ts;
    return -1;
  }
  return 0;
}

int iw32_pthread_join(HANDLE thread, void** _unused)
{
  assert(_unused == NULL);
  WaitForSingleObject(thread, INFINITE);
  return 0;
}

int iw32_pthread_key_create(pthread_key_t* key, void (*_unused)(void*))
{
  assert(_unused == NULL);
  *key = TlsAlloc();
  assert(*key != 0xffffffff);
  return 0;
}

int iw32_pthread_key_delete(pthread_key_t key)
{
  TlsFree(key);
  return 0;
}

int iw32_pthread_mutex_init(pthread_mutex_t* mutex, const void* _unused)
{
  assert(_unused == NULL);
  InitializeCriticalSection(mutex);
  return 0;
}

int iw32_pthread_cond_init(pthread_cond_t* cond, const void* _unused)
{
  assert(_unused = NULL);
  InitializeCriticalSection(&cond->lock);
  cond->ev[0] = CreateEvent(NULL, TRUE, FALSE, NULL);
  assert(cond->ev[0] != INVALID_HANDLE_VALUE);
  cond->ev[1] = CreateEvent(NULL, FALSE, FALSE, NULL);
  assert(cond->ev[1] != INVALID_HANDLE_VALUE);
  cond->nr_waiting = 0;
  return 0;
}

int iw32_pthread_cond_destroy(pthread_cond_t* cond)
{
  assert(cond->nr_waiting == 0);
  CloseHandle(cond->ev[1]);
  CloseHandle(cond->ev[0]);
  DeleteCriticalSection(&cond->lock);
  return 0;
}

int iw32_pthread_cond_wait(pthread_cond_t* cond, pthread_mutex_t* mutex)
{
  // incr nr_waiting
  EnterCriticalSection(&cond->lock);
  cond->nr_waiting++;
  LeaveCriticalSection(&cond->lock);
  
  pthread_mutex_unlock(mutex);

  int r = WaitForMultipleObjects(2, cond->ev, FALSE, INFINITE);

  EnterCriticalSection(&cond->lock);
  --cond->nr_waiting;
  if (r == WAIT_OBJECT_0 && cond->nr_waiting == 0) {
    ResetEvent(cond->ev[0]);
  }
  LeaveCriticalSection(&cond->lock);

  pthread_mutex_lock(mutex);
  return 0;
}

int iw32_pthread_cond_signal(pthread_cond_t* cond)
{
  EnterCriticalSection(&cond->lock);
  if (cond->nr_waiting != 0) {
    SetEvent(cond->ev[1]);
  }
  LeaveCriticalSection(&cond->lock);
  return 0;
}

int iw32_pthread_cond_broadcast(pthread_cond_t* cond)
{
  EnterCriticalSection(&cond->lock);
  if (cond->nr_waiting != 0) {
    SetEvent(cond->ev[0]);
  }
  LeaveCriticalSection(&cond->lock);
  return 0;
}
