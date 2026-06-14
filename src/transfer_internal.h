/*
 * File Explorer - private file-manager interfaces shared by transfer modules.
 */

#pragma once

#include <pthread.h>
#include <stdatomic.h>
#include <stddef.h>
#include <stdint.h>
#include <sys/types.h>
#include <time.h>

#include "websrv.h"

#define ACTIVITY_ID_MAX 64
#define ACTIVITY_CLIENT_MAX 96
#define ACTIVITY_KIND_MAX 16

typedef struct json_buf {
  char  *data;
  size_t len;
  size_t cap;
} json_buf_t;

typedef struct activity_request_ctx {
  int      queued;
  uint64_t seq;
  char     queue_id[ACTIVITY_ID_MAX];
  char     lease_token[ACTIVITY_ID_MAX];
  char     kind[ACTIVITY_KIND_MAX];
} activity_request_ctx_t;

typedef struct du_state {
  dev_t     root_dev;
  uint64_t  entries;
  uint64_t  files;
  uint64_t  dirs;
  uint64_t  bytes;
  int       cancelled;
} du_state_t;

struct job_state {
  pthread_mutex_t lock;
  atomic_int      busy;
  atomic_int      cancel;
  atomic_long     total_bytes;
  atomic_long     copied_bytes;
  atomic_long     compressed_output_bytes;
  atomic_long     raw_blocks;
  atomic_long     compressed_blocks;
  atomic_long     skipped_zlib_blocks;
  atomic_long     total_blocks;
  atomic_long     bad_blocks_found;
  atomic_long     repaired_blocks;
  atomic_long     hash_checked_blocks;
  atomic_long     hash_matched_blocks;
  atomic_long     hash_mismatched_blocks;
  atomic_long     software_compared_blocks;
  atomic_long     phase_step;
  atomic_long     phase_count;
  atomic_long     writer_wait_us;
  atomic_long     worker_wait_us;
  atomic_long     scan_bytes;
  atomic_long     scan_files;
  atomic_long     scan_dirs;
  atomic_long     scan_entries;
  atomic_long     scan_elapsed_ms;
  atomic_long     scan_workers;
  atomic_long     repair_read_bytes;
  atomic_long     repair_written_bytes;
  atomic_long     repair_copy_bytes;
  atomic_long     stream_min_free_bytes;
  atomic_long     stream_budget_bytes;
  atomic_long     stream_current_credit_bytes;
  atomic_long     stream_deleted_bytes;
  atomic_long     stream_reverse_temp_bytes;
  atomic_long     stream_forward_files;
  atomic_long     stream_reverse_files;
  atomic_int      total_files;
  atomic_int      done_files;
  atomic_int      failed_files;
  atomic_int      destructive_stream_active;
  atomic_int      rollback_requested;
  char            current[512];
  char            phase[32];
  char            verb[16];
  char            target[1024];
  char            log_path[1024];
  char            error[256];
  time_t          started_at;
  time_t          ended_at;
};

extern struct job_state g_job;

int json_grow(json_buf_t *b, size_t add);
int json_append(json_buf_t *b, const char *s);
int json_appendf(json_buf_t *b, const char *fmt, ...);
int json_string(json_buf_t *b, const char *s);

int serve_owned(const http_request_t *req, int status, char *data,
                size_t size);
int serve_error(const http_request_t *req, int status, const char *msg);

int path_is_safe(const char *p);
int mkdirs(const char *path);
int upload_segment_safe(const char *seg);
int operation_error_is_cancelled(const char *err);
void join_path(char *out, size_t out_sz, const char *dir, const char *name);
const char *path_basename(const char *path);
int write_all_fd(int fd, const void *data, size_t size);
void drain_body(int fd, size_t already_read, size_t content_size);
int parse_upload_size_arg(const http_request_t *req, const char *name,
                          uint64_t *out);
void du_walk(const char *path, du_state_t *du);
void du_walk_cancelable(const char *path, du_state_t *du);

void job_set_current(const char *path);
void job_set_phase(const char *phase, long step, long count,
                   const char *current);
void job_clear_countable_progress(void);
void job_set_target(const char *path);
int job_cancelled(void);
int job_begin(const char *verb);
void job_end(int rc, const char *err);
void job_log_name(char *out, size_t out_size);

int activity_validate_lease(const http_request_t *req, const char *kind,
                            activity_request_ctx_t *out, char *err,
                            size_t err_size);
void activity_finish_queue(const char *queue_id, int rc, const char *err,
                           const char *target, long copied_bytes,
                           int done_items, int failed_items,
                           const char *log_name);
void activity_defer_queue_success(const char *queue_id, const char *target,
                                  long copied_bytes, int done_items,
                                  const char *log_name);
