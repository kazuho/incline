#ifndef incline_win32_h
#define incline_win32_h

#ifdef __cplusplus
extern "C" {
#endif

#include <windows.h>
#include <process.h>

#ifdef _MSC_VER
#  pragma warning (disable : 4996)
#  define snprintf _snprintf_s
#endif

#define sleep iw32_sleep
void iw32_sleep(unsigned int seconds);

#define pthread_t HANDLE
#define pthread_create iw32_pthread_create
#define pthread_join iw32_pthread_join
int iw32_pthread_create(pthread_t* thread, const void* _unused, void* (*start_routine)(void*), void* arg);
int iw32_pthread_join(pthread_t thread, void** _unused);

#define pthread_key_t DWORD
#define pthread_key_create iw32_pthread_key_create
#define pthread_key_delete iw32_pthread_key_delete
#define pthread_getspecific TlsGetValue
#define pthread_setspecific TlsSetValue
int iw32_pthread_key_create(pthread_key_t* key, void (*_unused)(void*));
int iw32_pthread_key_delete(pthread_key_t key);

#define pthread_mutex_t CRITICAL_SECTION
#define pthread_mutexattr_t void
#define pthread_mutex_init iw32_pthread_mutex_init
#define pthread_mutex_destroy DeleteCriticalSection
#define pthread_mutex_lock EnterCriticalSection
#define pthread_mutex_unlock LeaveCriticalSection
int iw32_pthread_mutex_init(pthread_mutex_t* mutex, const void* _unused);
int iw32_pthread_mutex_destroy(pthread_mutex_t* mutex);
int iw32_pthread_mutex_lock(pthread_mutex_t* mutex);
int iw32_pthread_mutex_unlock(pthread_mutex_t* mutex);

#define pthread_cond_t iw32_pthread_cond_t
#define pthread_condattr_t void
#define pthread_cond_init iw32_pthread_cond_init
#define pthread_cond_destroy iw32_pthread_cond_destroy
#define pthread_cond_wait iw32_pthread_cond_wait
#define pthread_cond_signal iw32_pthread_cond_signal
#define pthread_cond_broadcast iw32_pthread_cond_broadcast
typedef struct iw32_pthread_cond_t {
  CRITICAL_SECTION lock;
  HANDLE ev[2]; /* 0 == broadcast, 1 == signal */
  size_t nr_waiting;
} iw32_pthread_cond_t;
int iw32_pthread_cond_init(pthread_cond_t* cond, const void* _unused);
int iw32_pthread_cond_destroy(pthread_cond_t* cond);
int iw32_pthread_cond_wait(pthread_cond_t* cond, pthread_mutex_t* mutex);
int iw32_pthread_cond_signal(pthread_cond_t* cond);
int iw32_pthread_cond_broadcast(pthread_cond_t* cond);

#ifdef __cplusplus
}
#endif

#endif
