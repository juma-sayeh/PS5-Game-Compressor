/*
 * Game Compressor - shared PFSC job timing helpers.
 */

#pragma once

#include <limits.h>
#include <pthread.h>
#include <stdatomic.h>
#include <stdint.h>
#include <time.h>

static inline uint64_t
monotonic_us(void) {
  struct timespec ts;
#if defined(CLOCK_MONOTONIC)
  clock_gettime(CLOCK_MONOTONIC, &ts);
#else
  clock_gettime(CLOCK_REALTIME, &ts);
#endif
  return (uint64_t)ts.tv_sec * 1000000ULL + (uint64_t)ts.tv_nsec / 1000ULL;
}

static inline void
cancel_poll_cond_wait(pthread_cond_t *cond, pthread_mutex_t *lock) {
  struct timespec ts;
  clock_gettime(CLOCK_REALTIME, &ts);
  ts.tv_nsec += 100000000L;
  if(ts.tv_nsec >= 1000000000L) {
    ts.tv_sec += ts.tv_nsec / 1000000000L;
    ts.tv_nsec %= 1000000000L;
  }
  (void)pthread_cond_timedwait(cond, lock, &ts);
}

static inline void
job_add_wait_us(atomic_long *counter, uint64_t started_at) {
  uint64_t now = monotonic_us();
  if(now <= started_at) return;
  uint64_t delta = now - started_at;
  atomic_fetch_add(counter, delta > (uint64_t)LONG_MAX ? LONG_MAX : (long)delta);
}
