/*
 * Game Compressor - shared job and utility functions expected by PFS code.
 */

#include <dirent.h>
#include <errno.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <sys/stat.h>
#include <unistd.h>

#include "transfer_internal.h"

struct job_state g_job = {
  .lock = PTHREAD_MUTEX_INITIALIZER,
  .busy = 0,
  .cancel = 0,
};

int
json_grow(json_buf_t *b, size_t add) {
  if(!b) return -1;
  size_t need = b->len + add + 1;
  if(need <= b->cap) return 0;
  size_t cap = b->cap ? b->cap : 256;
  while(cap < need) cap *= 2;
  char *p = realloc(b->data, cap);
  if(!p) return -1;
  b->data = p;
  b->cap = cap;
  return 0;
}

int
json_append(json_buf_t *b, const char *s) {
  size_t n = s ? strlen(s) : 0;
  if(json_grow(b, n) != 0) return -1;
  if(n) memcpy(b->data + b->len, s, n);
  b->len += n;
  b->data[b->len] = 0;
  return 0;
}

int
json_appendf(json_buf_t *b, const char *fmt, ...) {
  va_list ap;
  va_start(ap, fmt);
  va_list cp;
  va_copy(cp, ap);
  int n = vsnprintf(NULL, 0, fmt, cp);
  va_end(cp);
  if(n < 0) {
    va_end(ap);
    return -1;
  }
  if(json_grow(b, (size_t)n) != 0) {
    va_end(ap);
    return -1;
  }
  vsnprintf(b->data + b->len, b->cap - b->len, fmt, ap);
  va_end(ap);
  b->len += (size_t)n;
  return 0;
}

int
json_string(json_buf_t *b, const char *s) {
  if(json_append(b, "\"") != 0) return -1;
  for(const unsigned char *p = (const unsigned char *)(s ? s : ""); *p; p++) {
    char tmp[8];
    if(*p == '"' || *p == '\\') {
      tmp[0] = '\\';
      tmp[1] = (char)*p;
      tmp[2] = 0;
      if(json_append(b, tmp) != 0) return -1;
    } else if(*p == '\n') {
      if(json_append(b, "\\n") != 0) return -1;
    } else if(*p == '\r') {
      if(json_append(b, "\\r") != 0) return -1;
    } else if(*p == '\t') {
      if(json_append(b, "\\t") != 0) return -1;
    } else if(*p < 0x20) {
      snprintf(tmp, sizeof(tmp), "\\u%04x", *p);
      if(json_append(b, tmp) != 0) return -1;
    } else {
      tmp[0] = (char)*p;
      tmp[1] = 0;
      if(json_append(b, tmp) != 0) return -1;
    }
  }
  return json_append(b, "\"");
}

int
serve_owned(const http_request_t *req, int status, char *data, size_t size) {
  int rc = websrv_send(req->fd, status, "application/json", data, size);
  free(data);
  return rc;
}

int
serve_error(const http_request_t *req, int status, const char *msg) {
  json_buf_t b = {0};
  if(json_append(&b, "{\"ok\":false,\"error\":") != 0 ||
     json_string(&b, msg ? msg : "error") != 0 ||
     json_append(&b, "}") != 0) {
    free(b.data);
    return -1;
  }
  return serve_owned(req, status, b.data, b.len);
}

int
path_is_safe(const char *p) {
  if(!p || p[0] != '/') return 0;
  if(strstr(p, "/../") || strstr(p, "/./") || strstr(p, "//")) return 0;
  size_t len = strlen(p);
  if(len >= 1024) return 0;
  if(len >= 3 && (!strcmp(p + len - 3, "/..") || !strcmp(p + len - 2, "/."))) {
    return 0;
  }
  for(const unsigned char *c = (const unsigned char *)p; *c; c++) {
    if(*c < 0x20 || *c >= 0x7f || *c == '\\') return 0;
  }
  return 1;
}

int
mkdirs(const char *path) {
  char tmp[1024];
  int n = snprintf(tmp, sizeof(tmp), "%s", path ? path : "");
  if(n < 0 || (size_t)n >= sizeof(tmp) || tmp[0] != '/') return -1;
  size_t len = strlen(tmp);
  if(len > 1 && tmp[len - 1] == '/') tmp[len - 1] = 0;
  for(char *p = tmp + 1; *p; p++) {
    if(*p != '/') continue;
    *p = 0;
    if(mkdir(tmp, 0777) != 0 && errno != EEXIST) {
      *p = '/';
      return -1;
    }
    *p = '/';
  }
  if(mkdir(tmp, 0777) != 0 && errno != EEXIST) return -1;
  return 0;
}

int
upload_segment_safe(const char *seg) {
  if(!seg || !*seg || !strcmp(seg, ".") || !strcmp(seg, "..")) return 0;
  for(const unsigned char *p = (const unsigned char *)seg; *p; p++) {
    if(*p < 0x20 || *p >= 0x7f || *p == '/' || *p == '\\') return 0;
  }
  return 1;
}

int
operation_error_is_cancelled(const char *err) {
  return err && strcasestr(err, "cancel") != NULL;
}

void
join_path(char *out, size_t out_sz, const char *dir, const char *name) {
  if(!out || out_sz == 0) return;
  if(!dir || !*dir) dir = "/";
  if(!name) name = "";
  size_t len = strlen(dir);
  snprintf(out, out_sz, "%s%s%s", dir, len > 1 && dir[len - 1] != "/"[0] ? "/" : "",
           name);
}

const char *
path_basename(const char *path) {
  const char *slash = strrchr(path ? path : "", '/');
  return slash ? slash + 1 : (path ? path : "");
}

int
write_all_fd(int fd, const void *data, size_t size) {
  const char *p = data;
  while(size > 0) {
    ssize_t n = write(fd, p, size);
    if(n < 0) {
      if(errno == EINTR) continue;
      return -1;
    }
    if(n == 0) return -1;
    p += n;
    size -= (size_t)n;
  }
  return 0;
}

static int
du_walk_should_stop(du_state_t *du, int honor_cancel) {
  if(!du) return 1;
  if(du->cancelled) return 1;
  if(honor_cancel && job_cancelled()) {
    du->cancelled = 1;
    errno = EINTR;
    return 1;
  }
  return 0;
}

static void
du_walk_inner(const char *path, du_state_t *du, int honor_cancel) {
  struct stat st;
  if(du_walk_should_stop(du, honor_cancel)) return;
  if(lstat(path, &st) != 0) return;
  if(du_walk_should_stop(du, honor_cancel)) return;
  du->entries++;
  if(S_ISREG(st.st_mode)) {
    du->files++;
    if(st.st_size > 0) du->bytes += (uint64_t)st.st_size;
    return;
  }
  if(!S_ISDIR(st.st_mode)) return;
  du->dirs++;
  DIR *d = opendir(path);
  if(!d) return;
  if(du_walk_should_stop(du, honor_cancel)) {
    closedir(d);
    return;
  }
  struct dirent *ent;
  while(!du_walk_should_stop(du, honor_cancel) && (ent = readdir(d))) {
    if(du_walk_should_stop(du, honor_cancel)) break;
    if(!strcmp(ent->d_name, ".") || !strcmp(ent->d_name, "..")) continue;
    char child[1024];
    join_path(child, sizeof(child), path, ent->d_name);
    du_walk_inner(child, du, honor_cancel);
  }
  closedir(d);
}

void
du_walk(const char *path, du_state_t *du) {
  if(!du) return;
  memset(du, 0, sizeof(*du));
  du_walk_inner(path, du, 0);
}

void
du_walk_cancelable(const char *path, du_state_t *du) {
  if(!du) return;
  memset(du, 0, sizeof(*du));
  du_walk_inner(path, du, 1);
}

void
job_set_current(const char *path) {
  pthread_mutex_lock(&g_job.lock);
  snprintf(g_job.current, sizeof(g_job.current), "%s", path ? path : "");
  pthread_mutex_unlock(&g_job.lock);
}

void
job_set_phase(const char *phase, long step, long count, const char *current) {
  const char *next_phase = phase ? phase : "";
  long prev_step;
  long prev_count;
  int reset_phase_timer;
  pthread_mutex_lock(&g_job.lock);
  prev_step = atomic_load(&g_job.phase_step);
  prev_count = atomic_load(&g_job.phase_count);
  reset_phase_timer = strcmp(g_job.phase, next_phase) != 0 ||
      (count > 0 && prev_count <= 0) ||
      (count > 0 && step < prev_step);
  if(reset_phase_timer) {
    g_job.phase_started_at = time(NULL);
  }
  snprintf(g_job.phase, sizeof(g_job.phase), "%s", next_phase);
  snprintf(g_job.current, sizeof(g_job.current), "%s", current ? current : "");
  atomic_store(&g_job.phase_step, step);
  atomic_store(&g_job.phase_count, count);
  pthread_mutex_unlock(&g_job.lock);
}

void
job_set_target(const char *path) {
  pthread_mutex_lock(&g_job.lock);
  snprintf(g_job.target, sizeof(g_job.target), "%s", path ? path : "");
  pthread_mutex_unlock(&g_job.lock);
}

int
job_cancelled(void) {
  return atomic_load(&g_job.cancel) != 0;
}

int
job_begin(const char *verb) {
  int expected = 0;
  if(!atomic_compare_exchange_strong(&g_job.busy, &expected, 1)) return 0;
  pthread_mutex_lock(&g_job.lock);
  atomic_store(&g_job.cancel, 0);
  atomic_store(&g_job.total_bytes, 0);
  atomic_store(&g_job.copied_bytes, 0);
  atomic_store(&g_job.compressed_output_bytes, 0);
  atomic_store(&g_job.raw_blocks, 0);
  atomic_store(&g_job.compressed_blocks, 0);
  atomic_store(&g_job.skipped_zlib_blocks, 0);
  atomic_store(&g_job.total_blocks, 0);
  atomic_store(&g_job.bad_blocks_found, 0);
  atomic_store(&g_job.repaired_blocks, 0);
  atomic_store(&g_job.hash_checked_blocks, 0);
  atomic_store(&g_job.hash_matched_blocks, 0);
  atomic_store(&g_job.hash_mismatched_blocks, 0);
  atomic_store(&g_job.software_compared_blocks, 0);
  atomic_store(&g_job.phase_step, 0);
  atomic_store(&g_job.phase_count, 0);
  atomic_store(&g_job.writer_wait_us, 0);
  atomic_store(&g_job.worker_wait_us, 0);
  atomic_store(&g_job.scan_bytes, 0);
  atomic_store(&g_job.scan_files, 0);
  atomic_store(&g_job.scan_dirs, 0);
  atomic_store(&g_job.scan_entries, 0);
  atomic_store(&g_job.scan_elapsed_ms, 0);
  atomic_store(&g_job.scan_workers, 0);
  atomic_store(&g_job.repair_read_bytes, 0);
  atomic_store(&g_job.repair_written_bytes, 0);
  atomic_store(&g_job.repair_copy_bytes, 0);
  atomic_store(&g_job.stream_min_free_bytes, 0);
  atomic_store(&g_job.stream_budget_bytes, 0);
  atomic_store(&g_job.stream_current_credit_bytes, 0);
  atomic_store(&g_job.stream_deleted_bytes, 0);
  atomic_store(&g_job.stream_reverse_temp_bytes, 0);
  atomic_store(&g_job.stream_forward_files, 0);
  atomic_store(&g_job.stream_reverse_files, 0);
  atomic_store(&g_job.total_files, 0);
  atomic_store(&g_job.done_files, 0);
  atomic_store(&g_job.failed_files, 0);
  atomic_store(&g_job.cancel_disabled, 0);
  atomic_store(&g_job.destructive_stream_active, 0);
  atomic_store(&g_job.rollback_requested, 0);
  snprintf(g_job.verb, sizeof(g_job.verb), "%s", verb ? verb : "");
  g_job.current[0] = 0;
  g_job.phase[0] = 0;
  g_job.target[0] = 0;
  g_job.log_path[0] = 0;
  g_job.error[0] = 0;
  g_job.cancel_disabled_reason[0] = 0;
  g_job.started_at = time(NULL);
  g_job.phase_started_at = g_job.started_at;
  g_job.ended_at = 0;
  pthread_mutex_unlock(&g_job.lock);
  return 1;
}

void
job_end(int rc, const char *err) {
  pthread_mutex_lock(&g_job.lock);
  g_job.ended_at = time(NULL);
  if(rc != 0) snprintf(g_job.error, sizeof(g_job.error), "%s", err ? err : "");
  atomic_store(&g_job.cancel_disabled, 0);
  g_job.cancel_disabled_reason[0] = 0;
  pthread_mutex_unlock(&g_job.lock);
  atomic_store(&g_job.busy, 0);
}
