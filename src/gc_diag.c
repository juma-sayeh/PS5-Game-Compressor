/*
 * Game Compressor - lightweight runtime diagnostics.
 */

#include "gc_diag.h"

#include <errno.h>
#include <fcntl.h>
#include <pthread.h>
#include <signal.h>
#include <stdarg.h>
#include <stdio.h>
#include <string.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>

#define GC_DATA_ROOT "/data"
#define GC_DATA_DIR "/data/GameCompressor"
#define GC_LOG_DIR GC_DATA_DIR "/logs"
#define GC_LOG_PATH GC_LOG_DIR "/log.txt"
#define GC_CRASH_LOG_PATH GC_LOG_DIR "/crash.log"

static pthread_mutex_t g_diag_lock = PTHREAD_MUTEX_INITIALIZER;
static char g_checkpoint[128] = "not-started";
static volatile sig_atomic_t g_handling_signal = 0;

static int
mkdir_if_needed(const char *path) {
  if(mkdir(path, 0755) == 0) return 0;
  return errno == EEXIST ? 0 : -1;
}

static void
ensure_diag_dir(void) {
  mkdir_if_needed(GC_DATA_ROOT);
  mkdir_if_needed(GC_DATA_DIR);
  mkdir_if_needed(GC_LOG_DIR);
}

static const char *
signal_name(int sig) {
  switch(sig) {
  case SIGSEGV: return "SIGSEGV";
  case SIGABRT: return "SIGABRT";
  case SIGBUS: return "SIGBUS";
  case SIGILL: return "SIGILL";
#ifdef SIGTRAP
  case SIGTRAP: return "SIGTRAP";
#endif
#ifdef SIGSYS
  case SIGSYS: return "SIGSYS";
#endif
  case SIGFPE: return "SIGFPE";
  default: return "UNKNOWN";
  }
}

static void
fatal_signal_handler(int sig) {
  char line[512];
  int saved_errno = errno;

  if(g_handling_signal) _exit(128 + sig);
  g_handling_signal = 1;

  int n = snprintf(line, sizeof(line),
                   "%ld fatal signal=%s(%d) checkpoint=%s errno=%d\n",
                   (long)time(NULL), signal_name(sig), sig,
                   g_checkpoint, saved_errno);
  if(n < 0) n = snprintf(line, sizeof(line),
                         "fatal signal=%d checkpoint=%s errno=%d\n",
                         sig, g_checkpoint, saved_errno);
  if(n < 0) _exit(128 + sig);
  if((size_t)n >= sizeof(line)) n = (int)sizeof(line) - 1;

  write(STDOUT_FILENO, line, (size_t)n);
  ensure_diag_dir();
  int fd = open(GC_CRASH_LOG_PATH, O_WRONLY | O_CREAT | O_APPEND, 0600);
  if(fd >= 0) {
    write(fd, line, (size_t)n);
    close(fd);
  }
  _exit(128 + sig);
}

void
gc_diag_init(void) {
  ensure_diag_dir();
}

void
gc_diag_install_signal_handlers(void) {
  signal(SIGSEGV, fatal_signal_handler);
  signal(SIGABRT, fatal_signal_handler);
  signal(SIGBUS, fatal_signal_handler);
  signal(SIGILL, fatal_signal_handler);
#ifdef SIGTRAP
  signal(SIGTRAP, fatal_signal_handler);
#endif
#ifdef SIGSYS
  signal(SIGSYS, fatal_signal_handler);
#endif
  signal(SIGFPE, fatal_signal_handler);
}

void
gc_log(const char *fmt, ...) {
  char body[1024];
  char line[1400];
  va_list ap;
  int saved_errno = errno;

  va_start(ap, fmt);
  int body_n = vsnprintf(body, sizeof(body), fmt ? fmt : "", ap);
  va_end(ap);
  if(body_n < 0) {
    snprintf(body, sizeof(body), "log format failed");
  } else if((size_t)body_n >= sizeof(body)) {
    body[sizeof(body) - 1] = 0;
  }

  pthread_mutex_lock(&g_diag_lock);
  int line_n = snprintf(line, sizeof(line), "%ld pid=%ld checkpoint=%s %s\n",
                        (long)time(NULL), (long)getpid(), g_checkpoint, body);
  if(line_n < 0) {
    pthread_mutex_unlock(&g_diag_lock);
    errno = saved_errno;
    return;
  }
  if((size_t)line_n >= sizeof(line)) line_n = (int)sizeof(line) - 1;

  fputs(line, stdout);
  fflush(stdout);

  ensure_diag_dir();
  FILE *file = fopen(GC_LOG_PATH, "a");
  if(file) {
    fwrite(line, 1, (size_t)line_n, file);
    fclose(file);
  }
  pthread_mutex_unlock(&g_diag_lock);
  errno = saved_errno;
}

void
gc_checkpoint(const char *checkpoint) {
  pthread_mutex_lock(&g_diag_lock);
  snprintf(g_checkpoint, sizeof(g_checkpoint), "%s",
           checkpoint ? checkpoint : "unknown");
  pthread_mutex_unlock(&g_diag_lock);
  gc_log("checkpoint=%s", checkpoint ? checkpoint : "unknown");
}
