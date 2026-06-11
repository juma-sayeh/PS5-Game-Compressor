/*
 * File Explorer - streamed PFS/PFSC app compression.
 */

#include <ctype.h>
#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <pthread.h>
#include <stdarg.h>
#include <stdatomic.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <time.h>
#include <unistd.h>

#include <zlib.h>
#include "miniz.h"
#if defined(GAME_COMPRESSOR_VERSION)
#include "gc_diag.h"
#define PFSC_WINDOW_LOG(...) gc_log(__VA_ARGS__)
#else
#define PFSC_WINDOW_LOG(...) do { } while(0)
#endif
#include "pfs_compress.h"
#include "pfs_validate_hash.h"
#include "transfer_internal.h"
#include "exfat_upcase_standard.inc"

#define PFS_BLOCK_SIZE 65536ULL
#define PFS_INODE_SIZE 0xA8
#define PFS_VERSION_PS5 2LL
#define PFS_MAGIC 20130315LL
#define PFS_MODE_CASE_INSENSITIVE 0x8

#define PFS_INODE_MODE_O_READ 0x001
#define PFS_INODE_MODE_O_WRITE 0x002
#define PFS_INODE_MODE_O_EXEC 0x004
#define PFS_INODE_MODE_G_READ 0x008
#define PFS_INODE_MODE_G_WRITE 0x010
#define PFS_INODE_MODE_G_EXEC 0x020
#define PFS_INODE_MODE_U_READ 0x040
#define PFS_INODE_MODE_U_WRITE 0x080
#define PFS_INODE_MODE_U_EXEC 0x100
#define PFS_INODE_MODE_DIR 0x4000
#define PFS_INODE_MODE_FILE 0x8000
#define PFS_INODE_RX_ONLY \
  (PFS_INODE_MODE_O_READ | PFS_INODE_MODE_O_EXEC | \
   PFS_INODE_MODE_G_READ | PFS_INODE_MODE_G_EXEC | \
   PFS_INODE_MODE_U_READ | PFS_INODE_MODE_U_EXEC)
#define PFS_INODE_RWX_ALL \
  (PFS_INODE_RX_ONLY | PFS_INODE_MODE_O_WRITE | \
   PFS_INODE_MODE_G_WRITE | PFS_INODE_MODE_U_WRITE)

#define PFS_INODE_FLAG_COMPRESSED 0x1
#define PFS_INODE_FLAG_READONLY 0x10
#define PFS_INODE_FLAG_INTERNAL 0x20000

#define PFS_DIRENT_TYPE_FILE 2
#define PFS_DIRENT_TYPE_DIRECTORY 3
#define PFS_DIRENT_TYPE_DOT 4
#define PFS_DIRENT_TYPE_DOTDOT 5

#define PFSC_MAGIC 0x43534650U
#define PFSC_UNK4 0
#define PFSC_UNK8 6
#define PFSC_HEADER_SIZE 0x30
#define PFSC_BLOCK_OFFSETS_OFFSET 0x400
#define PFSC_INITIAL_DATA_OFFSET 0x10000ULL
#define PFSC_OFFSET_ENTRY_SIZE 8
#define PFSC_FAST_ZLIB_LEVEL MZ_BEST_SPEED
#define PFSC_SLOTS_PER_WORKER 32
#define PFSC_OUTPUT_BUFFER_SIZE (16U * 1024U * 1024U)
#define PFSC_OUTPUT_BUFFER_MIN_SIZE (64U * 1024U)
#define PFS_READ_CACHE_SIZE (16U * 1024U * 1024U)
#define PFS_READ_CACHE_MIN_SIZE (64U * 1024U)
#define PFS_READ_DIRECT_THRESHOLD (1024U * 1024U)
#define PFS_STREAM_REVERSE_CHUNK_MAX_SIZE (128U * 1024U * 1024U)
#define PFS_STREAM_REVERSE_CHUNK_MIN_SIZE (8U * 1024U * 1024U)
#define PFS_STREAM_REVERSE_WORKERS 10
#define PFS_STREAM_REVERSE_LOOKAHEAD_BYTES \
  ((uint64_t)PFS_STREAM_REVERSE_WORKERS * PFS_STREAM_REVERSE_CHUNK_MAX_SIZE)
#define PFS_STREAM_COMPRESS_WORKERS 4
#define PFS_STREAM_TRUNCATE_GRANULARITY (512ULL * 1024ULL * 1024ULL)
#define PFS_STREAM_JOURNAL_MAGIC "GCSTRM2"
#define PFS_STREAM_JOURNAL_VERSION 4U
#define PFS_STREAM_DEFAULT_RESERVE_BYTES (128ULL * 1024ULL * 1024ULL)
#define PFSC_WINDOW_RC_FALLBACK 1
#define PFSC_WINDOW_MAX_WORKERS 16
#define PFSC_WINDOW_MIN_WORKERS 3
#define PFSC_WINDOW_MIN_COMP_WORKERS 2
#define PFSC_WINDOW_READ_PERMITS 1
#define PFSC_WINDOW_WRITE_PERMITS 1
#define PFSC_WINDOW_COMP_PERMITS 14
#define PFSC_WINDOW_LANES 1
#define PFSC_WINDOW_SLOTS 2
#define PFSC_WINDOW_LANE_SIZE (128ULL * 1024ULL * 1024ULL)
#define PFSC_WINDOW_BLOCKS_PER_LANE (PFSC_WINDOW_LANE_SIZE / PFS_BLOCK_SIZE)
#define PFSC_WINDOW_BLOCKS \
  (PFSC_WINDOW_BLOCKS_PER_LANE * (uint64_t)PFSC_WINDOW_LANES)
#define PFSC_WINDOW_COMP_CHUNK_BLOCKS 1U
#define PFSC_INCOMPRESSIBLE_SAMPLE_BYTES 1024U
#define PFSC_COMPRESSIBLE_UNIQUE_MAX 24U
#define PFSC_COMPRESSIBLE_FREQ_MIN 384U

#define EXFAT_SECTOR_SIZE 512ULL
#define EXFAT_SECTORS_PER_CLUSTER 128ULL
#define EXFAT_CLUSTER_SIZE (EXFAT_SECTOR_SIZE * EXFAT_SECTORS_PER_CLUSTER)
#define EXFAT_BOOT_REGION_SECTORS 24ULL
#define EXFAT_BOOT_CHECKSUM_SECTOR 11ULL
#define EXFAT_FAT_OFFSET_SECTORS 128ULL
#define EXFAT_END_OF_CHAIN 0xffffffffU
#define EXFAT_ROOT_SLACK_CLUSTERS 1024ULL
#define EXFAT_TRAILING_PAD_SECTORS EXFAT_BOOT_REGION_SECTORS

typedef struct byte_buf {
  unsigned char *data;
  size_t len;
  size_t cap;
} byte_buf_t;

typedef struct scan_file {
  char rel[1024];
  char abs[1024];
  uint64_t size;
  uint64_t stream_predicted_stored;
  uint32_t stream_schedule_order;
  uint32_t stream_predicted_gain_permille;
  int stream_reverse_required;
  int stream_passthrough_delete;
} scan_file_t;

typedef struct scan_list {
  scan_file_t *items;
  size_t count;
  size_t cap;
} scan_list_t;

typedef struct int_list {
  int *items;
  size_t count;
  size_t cap;
} int_list_t;

typedef struct pfs_dir_node {
  char rel[1024];
  char name[256];
  int parent;
  int inode;
  int nlink;
  int_list_t child_dirs;
  int_list_t child_files;
  unsigned char *blob;
  size_t blob_size;
  uint64_t block_start;
  uint64_t blocks;
} pfs_dir_node_t;

typedef struct pfs_file_node {
  char rel[1024];
  char abs[1024];
  char name[256];
  int parent;
  int inode;
  uint64_t raw_size;
  uint64_t block_start;
  uint64_t blocks;
  int source_deleted;
  uint64_t stream_predicted_stored;
  uint32_t stream_schedule_order;
  uint32_t stream_predicted_gain_permille;
  int stream_reverse_required;
  int stream_passthrough_delete;
} pfs_file_node_t;

typedef enum pfs_segment_type {
  PFS_SEG_MEM = 1,
  PFS_SEG_FILE = 2,
} pfs_segment_type_t;

typedef struct pfs_segment {
  uint64_t offset;
  uint64_t size;
  pfs_segment_type_t type;
  const unsigned char *mem;
  char path[1024];
} pfs_segment_t;

typedef struct destructive_stream_ctx destructive_stream_ctx_t;

typedef struct pfs_layout {
  pfs_dir_node_t *dirs;
  size_t dir_count;
  size_t dir_cap;
  pfs_file_node_t *files;
  size_t file_count;
  size_t file_cap;
  pfs_segment_t *segments;
  size_t segment_count;
  size_t segment_cap;
  unsigned char *header_blob;
  unsigned char *inode_blob;
  size_t inode_blob_size;
  unsigned char *superroot_blob;
  size_t superroot_blob_size;
  unsigned char *fpt_blob;
  size_t fpt_blob_size;
  unsigned char *collision_blob;
  size_t collision_blob_size;
  unsigned char *exfat_boot_blob;
  size_t exfat_boot_blob_size;
  unsigned char *exfat_fat_blob;
  size_t exfat_fat_blob_size;
  unsigned char *exfat_bitmap_blob;
  size_t exfat_bitmap_blob_size;
  unsigned char *exfat_upcase_blob;
  size_t exfat_upcase_blob_size;
  int has_fpt_collision;
  uint64_t inode_count;
  uint64_t inode_block_count;
  uint64_t final_ndblock;
  uint64_t image_size;
} pfs_layout_t;

typedef struct fpt_entry {
  uint32_t hash;
  uint32_t value;
  uint32_t seq;
  uint32_t index;
  int is_dir;
} fpt_entry_t;

typedef struct virtual_reader {
  size_t seg_index;
  ssize_t open_seg;
  int fd;
  unsigned char *cache;
  size_t cache_cap;
  size_t cache_len;
  uint64_t cache_offset;
  ssize_t cache_seg;
  destructive_stream_ctx_t *stream;
} virtual_reader_t;

typedef enum pfsc_slot_state {
  PFSC_SLOT_FREE = 0,
  PFSC_SLOT_FILLING,
  PFSC_SLOT_READY,
  PFSC_SLOT_BUSY,
  PFSC_SLOT_DONE,
} pfsc_slot_state_t;

typedef struct pfsc_slot {
  uint64_t index;
  size_t raw_len;
  size_t comp_len;
  int force_raw;
  unsigned char hash[PFS_VHASH_HASH_SIZE];
  unsigned char *raw;
  unsigned char *comp;
  pfsc_slot_state_t state;
} pfsc_slot_t;

#ifndef GC_PFSC_ZLIB_LEVEL
#define GC_PFSC_ZLIB_LEVEL 7
#endif
#ifndef GC_PFSC_THRESHOLD_GAIN
#define GC_PFSC_THRESHOLD_GAIN 5
#endif
#ifndef GC_PFSC_FORCE_RAW_EXEC
#define GC_PFSC_FORCE_RAW_EXEC 0
#endif
typedef tdefl_compressor pfsc_comp_state_t;

typedef struct pfsc_comp_config {
  int profile;
  int zlib_level;
  int threshold_gain;
  int force_raw_exec;
  int entropy_skip;
  int fast_deflate;
  int raw_only;
  mz_uint miniz_flags;
} pfsc_comp_config_t;

typedef struct pfsc_pool {
  pthread_mutex_t lock;
  pthread_cond_t cond;
  pfsc_slot_t *slots;
  int slot_count;
  int stop;
  int error;
  pfsc_comp_config_t comp_config;
} pfsc_pool_t;

typedef struct pfsc_output_buffer {
  unsigned char *data;
  size_t len;
  size_t cap;
  uint64_t offset;
} pfsc_output_buffer_t;

typedef enum pfsc_window_phase {
  PFSC_WINDOW_PHASE_IDLE = 0,
  PFSC_WINDOW_PHASE_READ,
  PFSC_WINDOW_PHASE_WRITE,
} pfsc_window_phase_t;

typedef enum pfsc_window_task_type {
  PFSC_WINDOW_TASK_READ = 1,
  PFSC_WINDOW_TASK_COMPRESS,
  PFSC_WINDOW_TASK_WRITE,
} pfsc_window_task_type_t;

typedef struct pfsc_window_block {
  uint32_t stored_len;
  uint8_t compressed;
  uint8_t force_raw;
  uint8_t ready;
  uint8_t written;
  unsigned char hash[PFS_VHASH_HASH_SIZE];
} pfsc_window_block_t;

struct pfsc_window_pool;

typedef struct pfsc_window_lane {
  uint64_t start_block;
  uint64_t block_count;
  uint64_t logical_offset;
  uint64_t raw_size;
  uint64_t packed_len;
  uint64_t output_offset;
  int read_done;
  int comp_started;
  int comp_chunks_total;
  int comp_chunks_done;
  int packed;
  int write_queued;
  int write_done;
  unsigned char *raw;
  unsigned char *out;
  unsigned char *write_data;
  pfsc_window_block_t *blocks;
} pfsc_window_lane_t;

typedef struct pfsc_window {
  uint64_t index;
  uint64_t start_block;
  uint64_t block_count;
  int lane_count;
  int read_tasks_remaining;
  int comp_tasks_remaining;
  int write_tasks_remaining;
  int compression_started;
  pfsc_window_lane_t lanes[PFSC_WINDOW_LANES];
} pfsc_window_t;

typedef struct pfsc_window_task {
  pfsc_window_task_type_t type;
  pfsc_window_t *window;
  int lane_index;
  uint32_t first_block;
  uint32_t block_count;
  uint64_t output_offset;
  struct pfsc_window_task *next;
} pfsc_window_task_t;

typedef struct pfsc_window_tuning {
  int max_workers;
  int read_permits;
  int write_permits;
} pfsc_window_tuning_t;

typedef struct pfsc_comp_config pfsc_comp_config_t;

typedef struct pfsc_window_pool {
  pthread_mutex_t lock;
  pthread_cond_t cond;
  pfsc_window_task_t *read_head;
  pfsc_window_task_t *read_tail;
  pfsc_window_task_t *comp_head;
  pfsc_window_task_t *comp_tail;
  pfsc_window_task_t *write_head;
  pfsc_window_task_t *write_tail;
  pthread_t *threads;
  int thread_count;
  int workers_started;
  int phase;
  int stop;
  int error;
  char error_msg[256];
  int read_permits;
  int comp_permits;
  int write_permits;
  int running_read;
  int running_comp;
  int running_write;
  pfs_layout_t *nested;
  int fd;
  uint64_t file_start;
  uint64_t header_size;
  pfs_vhash_writer_t *vhash;
  const pfsc_comp_config_t *comp_config;
} pfsc_window_pool_t;

typedef struct destructive_stream_file {
  char rel[1024];
  char abs[1024];
  uint64_t original_size;
  uint64_t virtual_offset;
  uint64_t committed;
  uint64_t reverse_pos;
  uint64_t reverse_claim;
  atomic_int reversed;
  int deleted;
  int reverse_committing;
  int passthrough_delete;
  int reverse_required;
  uint32_t schedule_order;
  uint32_t predicted_gain_permille;
  uint64_t predicted_stored;
} destructive_stream_file_t;

typedef struct destructive_stream_header_disk {
  char magic[8];
  uint32_t version;
  uint32_t header_size;
  uint32_t file_record_size;
  uint32_t file_count;
  uint32_t format;
  uint32_t nested_type;
  uint32_t rollback_requested;
  uint32_t mutation_started;
  uint32_t compression_complete;
  uint32_t output_finalized;
  uint64_t block_count;
  uint64_t logical_size;
  uint64_t nested_size;
  uint64_t pfsc_header_size;
  uint64_t completed_blocks;
  uint64_t data_pos;
  uint64_t budget_bytes;
  uint64_t reserve_bytes;
  uint64_t current_credit;
  uint64_t actual_output_bytes;
  uint64_t actual_deleted_bytes;
  uint64_t actual_reverse_temp_bytes;
  uint64_t forward_file_count;
  uint64_t reverse_file_count;
  char source_path[1024];
  char output_path[1024];
  char tmp_path[1024];
  char vhash_tmp_path[1024];
  char vhash_output_path[1024];
  char nested_name[256];
} destructive_stream_header_disk_t;

typedef struct destructive_stream_file_disk {
  char rel[1024];
  uint64_t original_size;
  uint64_t virtual_offset;
  uint64_t committed;
  uint64_t reverse_pos;
  uint64_t predicted_stored;
  uint32_t schedule_order;
  uint32_t predicted_gain_permille;
  uint32_t reverse_required;
  uint32_t passthrough_delete;
  uint32_t reversed;
  uint32_t deleted;
} destructive_stream_file_disk_t;

struct destructive_stream_ctx {
  int journal_fd;
  char journal_path[1024];
  char source_path[1024];
  char output_path[1024];
  char tmp_path[1024];
  char vhash_tmp_path[1024];
  char vhash_output_path[1024];
  char nested_name[256];
  int format;
  int nested_type;
  int rollback_requested;
  int mutation_started;
  int compression_complete;
  int output_finalized;
  uint64_t block_count;
  uint64_t logical_size;
  uint64_t nested_size;
  uint64_t pfsc_header_size;
  uint64_t completed_blocks;
  uint64_t journaled_blocks;
  uint64_t data_pos;
  uint64_t budget_bytes;
  uint64_t reserve_bytes;
  uint64_t current_credit;
  uint64_t actual_output_bytes;
  uint64_t actual_deleted_bytes;
  uint64_t actual_reverse_temp_bytes;
  uint64_t forward_file_count;
  uint64_t reverse_file_count;
  destructive_stream_file_t *files;
  size_t file_count;
  uint64_t *offsets;
  int owns_offsets;
  size_t reverse_cap;
  pthread_mutex_t reverse_ahead_lock;
  pthread_cond_t reverse_ahead_cond;
  pthread_t reverse_ahead_threads[PFS_STREAM_REVERSE_WORKERS];
  int reverse_ahead_thread_count;
  int reverse_ahead_sync_initialized;
  int reverse_ahead_started;
  int reverse_ahead_stop;
  int reverse_ahead_done;
  int reverse_ahead_error;
  int reverse_ahead_active;
  int reverse_ahead_inflight;
  size_t reverse_ahead_demand_file;
  uint64_t reverse_ahead_demand_pos;
  size_t reverse_ahead_next_file;
  char reverse_ahead_err[256];
};

static void destructive_stream_reverse_ahead_stop(destructive_stream_ctx_t *ctx);
static int
destructive_stream_file_passthrough_delete(const destructive_stream_file_t *f);

static int
destructive_stream_file_reversed(const destructive_stream_file_t *f) {
  return f && atomic_load_explicit(&f->reversed, memory_order_acquire) != 0;
}

static void
destructive_stream_file_set_reversed(destructive_stream_file_t *f, int reversed) {
  if(f) {
    atomic_store_explicit(&f->reversed, reversed ? 1 : 0, memory_order_release);
  }
}

static void
destructive_stream_recompute_budget(destructive_stream_ctx_t *ctx) {
  if(!ctx) return;
  uint64_t deleted_or_committed = 0;
  uint64_t reverse_temp = 0;
  for(size_t i = 0; i < ctx->file_count; i++) {
    destructive_stream_file_t *f = &ctx->files[i];
    uint64_t committed = f->committed > f->original_size ?
                         f->original_size : f->committed;
    if(UINT64_MAX - deleted_or_committed < committed) {
      deleted_or_committed = UINT64_MAX;
    } else {
      deleted_or_committed += committed;
    }
    if(!f->deleted && !destructive_stream_file_reversed(f) &&
       !destructive_stream_file_passthrough_delete(f)) {
      if(UINT64_MAX - reverse_temp < f->reverse_pos) reverse_temp = UINT64_MAX;
      else reverse_temp += f->reverse_pos;
    }
  }
  uint64_t output = ctx->data_pos > ctx->pfsc_header_size ?
                    ctx->data_pos - ctx->pfsc_header_size : 0;
  uint64_t base = ctx->budget_bytes > ctx->reserve_bytes ?
                  ctx->budget_bytes - ctx->reserve_bytes : ctx->budget_bytes;
  uint64_t credit = base;
  if(UINT64_MAX - credit < deleted_or_committed) credit = UINT64_MAX;
  else credit += deleted_or_committed;
  uint64_t used = output;
  if(UINT64_MAX - used < reverse_temp) used = UINT64_MAX;
  else used += reverse_temp;
  ctx->actual_output_bytes = output;
  ctx->actual_deleted_bytes = deleted_or_committed;
  ctx->actual_reverse_temp_bytes = reverse_temp;
  ctx->current_credit = credit > used ? credit - used : 0;
}

static void
destructive_stream_publish_budget(const destructive_stream_ctx_t *ctx) {
  if(!ctx) return;
  atomic_store(&g_job.stream_budget_bytes,
               ctx->budget_bytes > (uint64_t)LONG_MAX ? LONG_MAX :
               (long)ctx->budget_bytes);
  atomic_store(&g_job.stream_current_credit_bytes,
               ctx->current_credit > (uint64_t)LONG_MAX ? LONG_MAX :
               (long)ctx->current_credit);
  atomic_store(&g_job.stream_deleted_bytes,
               ctx->actual_deleted_bytes > (uint64_t)LONG_MAX ? LONG_MAX :
               (long)ctx->actual_deleted_bytes);
  atomic_store(&g_job.stream_reverse_temp_bytes,
               ctx->actual_reverse_temp_bytes > (uint64_t)LONG_MAX ? LONG_MAX :
               (long)ctx->actual_reverse_temp_bytes);
  atomic_store(&g_job.stream_forward_files,
               ctx->forward_file_count > (uint64_t)LONG_MAX ? LONG_MAX :
               (long)ctx->forward_file_count);
  atomic_store(&g_job.stream_reverse_files,
               ctx->reverse_file_count > (uint64_t)LONG_MAX ? LONG_MAX :
               (long)ctx->reverse_file_count);
}

static void
set_err(char *err, size_t err_size, const char *fmt, ...) {
  if(!err || err_size == 0 || err[0]) return;
  va_list ap;
  va_start(ap, fmt);
  vsnprintf(err, err_size, fmt, ap);
  va_end(ap);
}

static uint64_t
ceil_div_u64(uint64_t a, uint64_t b) {
  return b ? ((a + b - 1) / b) : 0;
}

static int
ends_with_ci(const char *s, const char *suffix) {
  size_t slen = s ? strlen(s) : 0;
  size_t suffix_len = suffix ? strlen(suffix) : 0;
  if(slen < suffix_len) return 0;
  return strcasecmp(s + slen - suffix_len, suffix) == 0;
}

static int
starts_with_ci(const char *s, const char *prefix) {
  size_t prefix_len = prefix ? strlen(prefix) : 0;
  if(!s || strlen(s) < prefix_len) return 0;
  return strncasecmp(s, prefix, prefix_len) == 0;
}

static const char *
path_basename_const(const char *path) {
  const char *slash;
  if(!path || !path[0]) return "";
  slash = strrchr(path, '/');
  return slash ? slash + 1 : path;
}

static int
path_is_executable_payload(const char *path) {
  const char *name = path_basename_const(path);
  if(starts_with_ci(name, "eboot") && ends_with_ci(name, ".bin")) return 1;
  if(ends_with_ci(name, ".prx") || ends_with_ci(name, ".sprx")) return 1;
  return 0;
}

static int
clamp_worker_count(int workers) {
  if(workers <= 0) return PFS_COMPRESS_DEFAULT_WORKERS;
  if(workers > PFS_COMPRESS_MAX_WORKERS) return PFS_COMPRESS_MAX_WORKERS;
  return workers;
}

static int
ascii_tolower(int ch) {
  return ch >= 'A' && ch <= 'Z' ? ch + 32 : ch;
}

static int
ascii_toupper(int ch) {
  return ch >= 'a' && ch <= 'z' ? ch - 32 : ch;
}

static int
ascii_casecmp(const char *a, const char *b) {
  const unsigned char *pa = (const unsigned char *)(a ? a : "");
  const unsigned char *pb = (const unsigned char *)(b ? b : "");
  while(*pa || *pb) {
    int ca = ascii_tolower(*pa);
    int cb = ascii_tolower(*pb);
    if(ca != cb) return ca - cb;
    if(*pa) pa++;
    if(*pb) pb++;
  }
  return 0;
}

static int
path_segment_supported(const char *name) {
  if(!name || !*name || !strcmp(name, ".") || !strcmp(name, "..")) return 0;
  if(strlen(name) >= 256) return 0;
  for(const unsigned char *p = (const unsigned char *)name; *p; p++) {
    if(*p < 0x20 || *p >= 0x7f || *p == '/' || *p == '\\') return 0;
  }
  return 1;
}

static int
title_id_safe(const char *title) {
  if(!title || !*title || strlen(title) >= 64) return 0;
  for(const unsigned char *p = (const unsigned char *)title; *p; p++) {
    if(!isalnum(*p) && *p != '_' && *p != '-') return 0;
  }
  return 1;
}

static int
normalize_app_path(const char *path, char *out, size_t out_size) {
  if(!path || path[0] != '/' || strstr(path, "..")) {
    errno = EINVAL;
    return -1;
  }
  size_t n = strlen(path);
  while(n > 1 && path[n - 1] == '/') n--;
  if(n == 0 || n >= out_size) {
    errno = ENAMETOOLONG;
    return -1;
  }
  memcpy(out, path, n);
  out[n] = 0;
  return 0;
}

static int
path_parent_base(const char *path, char *parent, size_t parent_size,
                 char *base, size_t base_size) {
  const char *slash = strrchr(path ? path : "", '/');
  if(!slash || !slash[1]) {
    errno = EINVAL;
    return -1;
  }
  size_t parent_len = slash == path ? 1 : (size_t)(slash - path);
  if(parent_len >= parent_size || strlen(slash + 1) >= base_size) {
    errno = ENAMETOOLONG;
    return -1;
  }
  memcpy(parent, path, parent_len);
  parent[parent_len] = 0;
  snprintf(base, base_size, "%s", slash + 1);
  return 0;
}

static int
join_rel(char *out, size_t out_size, const char *parent, const char *name) {
  int n;
  if(!parent || !parent[0]) {
    n = snprintf(out, out_size, "%s", name);
  } else {
    n = snprintf(out, out_size, "%s/%s", parent, name);
  }
  if(n < 0 || (size_t)n >= out_size) {
    errno = ENAMETOOLONG;
    return -1;
  }
  return 0;
}

static int
join_abs(char *out, size_t out_size, const char *dir, const char *name) {
  size_t n = strlen(dir);
  int rc = snprintf(out, out_size, "%s%s%s", dir,
                    (n > 1 && dir[n - 1] != '/') ? "/" : "", name);
  if(rc < 0 || (size_t)rc >= out_size) {
    errno = ENAMETOOLONG;
    return -1;
  }
  return 0;
}

static int
join_abs_rel(char *out, size_t out_size, const char *root, const char *rel) {
  size_t n = strlen(root ? root : "");
  int rc;
  if(!root || !root[0] || !rel) {
    errno = EINVAL;
    return -1;
  }
  rc = snprintf(out, out_size, "%s%s%s", root,
                (n > 1 && root[n - 1] != '/') ? "/" : "", rel);
  if(rc < 0 || (size_t)rc >= out_size) {
    errno = ENAMETOOLONG;
    return -1;
  }
  return 0;
}

int
pfs_compress_temp_output_path(const char *output_path, char *out,
                              size_t out_size) {
  char parent[1024];
  char base[256];
  if(!output_path || !output_path[0] || !out || out_size == 0) {
    errno = EINVAL;
    return -1;
  }
  if(path_parent_base(output_path, parent, sizeof(parent),
                      base, sizeof(base)) != 0) {
    return -1;
  }
  int n = snprintf(out, out_size, "%s%s.%s.gc-compress.tmp",
                   parent, parent[1] ? "/" : "", base);
  if(n < 0 || (size_t)n >= out_size) {
    errno = ENAMETOOLONG;
    return -1;
  }
  return 0;
}

static int
remove_tree_local(const char *path) {
  struct stat st;
  if(lstat(path, &st) != 0) {
    if(errno == ENOENT) return 0;
    return -1;
  }
  if(S_ISDIR(st.st_mode)) {
    DIR *d = opendir(path);
    if(!d) return -1;
    int rc = 0;
    struct dirent *ent;
    while((ent = readdir(d))) {
      if(!strcmp(ent->d_name, ".") || !strcmp(ent->d_name, "..")) continue;
      char child[1024];
      if(join_abs(child, sizeof(child), path, ent->d_name) != 0 ||
         remove_tree_local(child) != 0) {
        rc = -1;
        break;
      }
    }
    closedir(d);
    if(rc != 0) return -1;
    return rmdir(path);
  }
  return unlink(path);
}

static int
buf_reserve(byte_buf_t *b, size_t add) {
  if(b->len + add <= b->cap) return 0;
  size_t next = b->cap ? b->cap : 256;
  while(next < b->len + add) next *= 2;
  unsigned char *p = realloc(b->data, next);
  if(!p) return -1;
  b->data = p;
  b->cap = next;
  return 0;
}

static int
buf_append_zero(byte_buf_t *b, size_t n) {
  if(buf_reserve(b, n) != 0) return -1;
  memset(b->data + b->len, 0, n);
  b->len += n;
  return 0;
}

static int
buf_append(byte_buf_t *b, const void *data, size_t n) {
  if(buf_reserve(b, n) != 0) return -1;
  memcpy(b->data + b->len, data, n);
  b->len += n;
  return 0;
}

static void
le16(unsigned char *p, uint16_t v) {
  p[0] = (unsigned char)(v & 0xff);
  p[1] = (unsigned char)((v >> 8) & 0xff);
}

static void
le32(unsigned char *p, uint32_t v) {
  p[0] = (unsigned char)(v & 0xff);
  p[1] = (unsigned char)((v >> 8) & 0xff);
  p[2] = (unsigned char)((v >> 16) & 0xff);
  p[3] = (unsigned char)((v >> 24) & 0xff);
}

static void
le64(unsigned char *p, uint64_t v) {
  for(int i = 0; i < 8; i++) p[i] = (unsigned char)((v >> (i * 8)) & 0xff);
}

static uint32_t
rd32(const unsigned char *p) {
  return (uint32_t)p[0] |
         ((uint32_t)p[1] << 8) |
         ((uint32_t)p[2] << 16) |
         ((uint32_t)p[3] << 24);
}

static uint64_t
rd64(const unsigned char *p) {
  uint64_t v = 0;
  for(int i = 7; i >= 0; i--) v = (v << 8) | p[i];
  return v;
}

static int
pwrite_all_local(int fd, const void *data, size_t size, off_t offset) {
  const unsigned char *p = data;
  while(size > 0) {
    ssize_t n = pwrite(fd, p, size, offset);
    if(n < 0) {
      if(errno == EINTR) continue;
      return -1;
    }
    if(n == 0) {
      errno = EIO;
      return -1;
    }
    p += n;
    size -= (size_t)n;
    offset += n;
  }
  return 0;
}

static int
write_all_local(int fd, const void *data, size_t size) {
  const unsigned char *p = data;
  while(size > 0) {
    ssize_t n = write(fd, p, size);
    if(n < 0) {
      if(errno == EINTR) continue;
      return -1;
    }
    if(n == 0) {
      errno = EIO;
      return -1;
    }
    p += n;
    size -= (size_t)n;
  }
  return 0;
}

static uint64_t monotonic_us(void);
static void virtual_reader_close_file(virtual_reader_t *vr);
static int virtual_reader_open_path(virtual_reader_t *vr, const char *path,
                                    ssize_t key, char *err,
                                    size_t err_size);
static int virtual_reader_file_read(virtual_reader_t *vr, ssize_t seg_index,
                                    uint64_t file_size,
                                    uint64_t file_offset,
                                    unsigned char *out, size_t size);

static void
pfsc_output_buffer_init(pfsc_output_buffer_t *b) {
  size_t cap = PFSC_OUTPUT_BUFFER_SIZE;
  memset(b, 0, sizeof(*b));
  while(cap >= PFSC_OUTPUT_BUFFER_MIN_SIZE) {
    b->data = malloc(cap);
    if(b->data) {
      b->cap = cap;
      return;
    }
    cap /= 2;
  }
}

static int
pfsc_output_buffer_flush(int fd, uint64_t file_start, pfsc_output_buffer_t *b,
                         char *err, size_t err_size) {
  if(!b || b->len == 0) return 0;
  if(pwrite_all_local(fd, b->data, b->len,
                      (off_t)(file_start + b->offset)) != 0) {
    set_err(err, err_size, "write compressed payload: %s", strerror(errno));
    return -1;
  }
  b->len = 0;
  return 0;
}

static int
pfsc_output_buffer_write(int fd, uint64_t file_start, pfsc_output_buffer_t *b,
                         uint64_t offset, const void *data, size_t size,
                         char *err, size_t err_size) {
  if(size == 0) return 0;
  if(!b || !b->data || b->cap == 0) {
    if(pwrite_all_local(fd, data, size, (off_t)(file_start + offset)) != 0) {
      set_err(err, err_size, "write compressed payload: %s", strerror(errno));
      return -1;
    }
    return 0;
  }

  if(b->len > 0 && b->offset + b->len != offset) {
    if(pfsc_output_buffer_flush(fd, file_start, b, err, err_size) != 0) return -1;
  }
  if(size > b->cap) {
    if(pfsc_output_buffer_flush(fd, file_start, b, err, err_size) != 0) return -1;
    if(pwrite_all_local(fd, data, size, (off_t)(file_start + offset)) != 0) {
      set_err(err, err_size, "write compressed payload: %s", strerror(errno));
      return -1;
    }
    return 0;
  }
  if(b->len == 0) b->offset = offset;
  if(b->len + size > b->cap) {
    if(pfsc_output_buffer_flush(fd, file_start, b, err, err_size) != 0) return -1;
    b->offset = offset;
  }
  memcpy(b->data + b->len, data, size);
  b->len += size;
  return 0;
}

static void
pfsc_output_buffer_free(pfsc_output_buffer_t *b) {
  if(!b) return;
  free(b->data);
  memset(b, 0, sizeof(*b));
}

static int
path_is_child_of_root(const char *root, const char *path) {
  size_t root_len;
  if(!root || !path) return 0;
  root_len = strlen(root);
  if(root_len == 0 || strcmp(root, "/") == 0) return 0;
  return strncmp(path, root, root_len) == 0 && path[root_len] == '/';
}

static void
try_remove_empty_parent_dirs(const char *root, const char *path) {
  char dir[1024];
  char *slash;
  size_t root_len;

  if(!root || !path || !path_is_child_of_root(root, path)) return;
  snprintf(dir, sizeof(dir), "%s", path);
  slash = strrchr(dir, '/');
  if(!slash) return;
  *slash = 0;
  root_len = strlen(root);

  while(strlen(dir) > root_len && path_is_child_of_root(root, dir)) {
    if(rmdir(dir) != 0) {
      if(errno == ENOENT) {
        /* Treat already-removed parents like empty parents and keep walking. */
      } else {
        break;
      }
    }
    slash = strrchr(dir, '/');
    if(!slash) break;
    *slash = 0;
  }
}

static int
read_exact_at(int fd, void *data, size_t size, off_t offset) {
  unsigned char *p = data;
  while(size > 0) {
    ssize_t n = pread(fd, p, size, offset);
    if(n < 0) {
      if(errno == EINTR) continue;
      return -1;
    }
    if(n == 0) {
      errno = EIO;
      return -1;
    }
    p += n;
    size -= (size_t)n;
    offset += n;
  }
  return 0;
}

static void
reverse_bytes(unsigned char *data, size_t size) {
  if(!data) return;
  for(size_t i = 0, j = size ? size - 1 : 0; i < j; i++, j--) {
    unsigned char tmp = data[i];
    data[i] = data[j];
    data[j] = tmp;
  }
}

static uint64_t
destructive_stream_file_record_offset(size_t index) {
  return (uint64_t)sizeof(destructive_stream_header_disk_t) +
         (uint64_t)index * (uint64_t)sizeof(destructive_stream_file_disk_t);
}

static uint64_t
destructive_stream_offsets_offset(size_t file_count) {
  return destructive_stream_file_record_offset(file_count);
}

static int
destructive_stream_journal_path_from_tmp(const char *tmp_path, char *out,
                                         size_t out_size) {
  int n = snprintf(out, out_size, "%s.stream-journal", tmp_path ? tmp_path : "");
  if(n < 0 || (size_t)n >= out_size || !tmp_path || !tmp_path[0]) {
    errno = ENAMETOOLONG;
    return -1;
  }
  return 0;
}

int
pfs_compress_stream_journal_path(const char *output_path, char *out,
                                 size_t out_size) {
  char tmp_path[1024];
  if(pfs_compress_temp_output_path(output_path, tmp_path,
                                   sizeof(tmp_path)) != 0) {
    return -1;
  }
  return destructive_stream_journal_path_from_tmp(tmp_path, out, out_size);
}

static void
destructive_stream_init(destructive_stream_ctx_t *ctx) {
  if(!ctx) return;
  memset(ctx, 0, sizeof(*ctx));
  ctx->journal_fd = -1;
  ctx->reverse_cap = PFS_STREAM_REVERSE_CHUNK_MAX_SIZE;
  ctx->reverse_ahead_demand_file = SIZE_MAX;
}

static void
destructive_stream_close(destructive_stream_ctx_t *ctx) {
  if(!ctx) return;
  if(ctx->journal_fd >= 0) {
    close(ctx->journal_fd);
    ctx->journal_fd = -1;
  }
}

static void
destructive_stream_free(destructive_stream_ctx_t *ctx) {
  if(!ctx) return;
  destructive_stream_reverse_ahead_stop(ctx);
  destructive_stream_close(ctx);
  if(ctx->reverse_ahead_sync_initialized) {
    pthread_cond_destroy(&ctx->reverse_ahead_cond);
    pthread_mutex_destroy(&ctx->reverse_ahead_lock);
    ctx->reverse_ahead_sync_initialized = 0;
  }
  free(ctx->files);
  if(ctx->owns_offsets) free(ctx->offsets);
  destructive_stream_init(ctx);
}

static void
destructive_stream_fill_header(const destructive_stream_ctx_t *ctx,
                               destructive_stream_header_disk_t *h) {
  memset(h, 0, sizeof(*h));
  memcpy(h->magic, PFS_STREAM_JOURNAL_MAGIC, strlen(PFS_STREAM_JOURNAL_MAGIC));
  h->version = PFS_STREAM_JOURNAL_VERSION;
  h->header_size = (uint32_t)sizeof(*h);
  h->file_record_size = (uint32_t)sizeof(destructive_stream_file_disk_t);
  h->file_count = ctx->file_count > UINT32_MAX ? UINT32_MAX :
                  (uint32_t)ctx->file_count;
  h->format = (uint32_t)ctx->format;
  h->nested_type = (uint32_t)ctx->nested_type;
  h->rollback_requested = 0;
  h->mutation_started = ctx->mutation_started ? 1U : 0U;
  h->compression_complete = ctx->compression_complete ? 1U : 0U;
  h->output_finalized = ctx->output_finalized ? 1U : 0U;
  h->block_count = ctx->block_count;
  h->logical_size = ctx->logical_size;
  h->nested_size = ctx->nested_size;
  h->pfsc_header_size = ctx->pfsc_header_size;
  h->completed_blocks = ctx->completed_blocks;
  h->data_pos = ctx->data_pos;
  h->budget_bytes = ctx->budget_bytes;
  h->reserve_bytes = ctx->reserve_bytes;
  h->current_credit = ctx->current_credit;
  h->actual_output_bytes = ctx->actual_output_bytes;
  h->actual_deleted_bytes = ctx->actual_deleted_bytes;
  h->actual_reverse_temp_bytes = ctx->actual_reverse_temp_bytes;
  h->forward_file_count = ctx->forward_file_count;
  h->reverse_file_count = ctx->reverse_file_count;
  snprintf(h->source_path, sizeof(h->source_path), "%s", ctx->source_path);
  snprintf(h->output_path, sizeof(h->output_path), "%s", ctx->output_path);
  snprintf(h->tmp_path, sizeof(h->tmp_path), "%s", ctx->tmp_path);
  snprintf(h->vhash_tmp_path, sizeof(h->vhash_tmp_path), "%s",
           ctx->vhash_tmp_path);
  snprintf(h->vhash_output_path, sizeof(h->vhash_output_path), "%s",
           ctx->vhash_output_path);
  snprintf(h->nested_name, sizeof(h->nested_name), "%s", ctx->nested_name);
}

static int
destructive_stream_write_header(destructive_stream_ctx_t *ctx,
                                char *err, size_t err_size) {
  destructive_stream_header_disk_t h;
  if(!ctx || ctx->journal_fd < 0) return 0;
  destructive_stream_recompute_budget(ctx);
  destructive_stream_publish_budget(ctx);
  destructive_stream_fill_header(ctx, &h);
  if(pwrite_all_local(ctx->journal_fd, &h, sizeof(h), 0) != 0) {
    set_err(err, err_size, "write stream journal: %s", strerror(errno));
    return -1;
  }
  return 0;
}

static void
destructive_stream_fill_file_record(const destructive_stream_file_t *src,
                                    destructive_stream_file_disk_t *dst) {
  memset(dst, 0, sizeof(*dst));
  snprintf(dst->rel, sizeof(dst->rel), "%s", src->rel);
  dst->original_size = src->original_size;
  dst->virtual_offset = src->virtual_offset;
  dst->committed = src->committed;
  dst->reverse_pos = src->reverse_pos;
  dst->predicted_stored = src->predicted_stored;
  dst->schedule_order = src->schedule_order;
  dst->predicted_gain_permille = src->predicted_gain_permille;
  dst->reverse_required = src->reverse_required ? 1U : 0U;
  dst->passthrough_delete = src->passthrough_delete ? 1U : 0U;
  dst->reversed = destructive_stream_file_reversed(src) ? 1U : 0U;
  dst->deleted = src->deleted ? 1U : 0U;
}

static int
destructive_stream_write_file_record(destructive_stream_ctx_t *ctx,
                                     size_t index,
                                     char *err, size_t err_size) {
  destructive_stream_file_disk_t rec;
  if(!ctx || ctx->journal_fd < 0 || index >= ctx->file_count) return 0;
  destructive_stream_fill_file_record(&ctx->files[index], &rec);
  if(pwrite_all_local(ctx->journal_fd, &rec, sizeof(rec),
                      (off_t)destructive_stream_file_record_offset(index)) != 0) {
    set_err(err, err_size, "write stream journal file: %s", strerror(errno));
    return -1;
  }
  return 0;
}

static int
destructive_stream_write_offsets(destructive_stream_ctx_t *ctx, uint64_t first,
                                 uint64_t count, char *err, size_t err_size) {
  unsigned char raw[PFSC_OFFSET_ENTRY_SIZE * 256U];
  uint64_t offset_base;
  if(!ctx || ctx->journal_fd < 0 || !ctx->offsets || count == 0) return 0;
  if(first + count > ctx->block_count + 1ULL) {
    set_err(err, err_size, "bad stream journal offset range");
    errno = EINVAL;
    return -1;
  }
  offset_base = destructive_stream_offsets_offset(ctx->file_count);
  while(count > 0) {
    uint64_t n64 = count > 256U ? 256U : count;
    size_t n = (size_t)n64;
    for(size_t i = 0; i < n; i++) le64(raw + i * 8U, ctx->offsets[first + i]);
    if(pwrite_all_local(ctx->journal_fd, raw, n * 8U,
                        (off_t)(offset_base + first * 8ULL)) != 0) {
      set_err(err, err_size, "write stream journal offsets: %s",
              strerror(errno));
      return -1;
    }
    first += n64;
    count -= n64;
  }
  return 0;
}

static int
destructive_stream_sync(destructive_stream_ctx_t *ctx,
                        char *err, size_t err_size) {
  if(!ctx || ctx->journal_fd < 0) return 0;
  if(fsync(ctx->journal_fd) != 0) {
    set_err(err, err_size, "sync stream journal: %s", strerror(errno));
    return -1;
  }
  return 0;
}

static unsigned char *
destructive_stream_alloc_reverse_buffer(const destructive_stream_ctx_t *ctx,
                                        size_t *cap_out,
                                        char *err, size_t err_size) {
  size_t cap = ctx && ctx->reverse_cap ? ctx->reverse_cap :
               PFS_STREAM_REVERSE_CHUNK_MAX_SIZE;
  while(cap >= PFS_STREAM_REVERSE_CHUNK_MIN_SIZE) {
    unsigned char *buf = malloc(cap);
    if(buf) {
      if(cap_out) *cap_out = cap;
      return buf;
    }
    cap /= 2;
  }
  set_err(err, err_size, "out of memory");
  return NULL;
}

static int
destructive_stream_reverse_dir_path(const destructive_stream_ctx_t *ctx,
                                    char *out, size_t out_size) {
  int n = snprintf(out, out_size, "%s.reverse", ctx ? ctx->tmp_path : "");
  if(n < 0 || (size_t)n >= out_size || !ctx || !ctx->tmp_path[0]) {
    errno = ENAMETOOLONG;
    return -1;
  }
  return 0;
}

static int
destructive_stream_reverse_tmp_path(const destructive_stream_ctx_t *ctx,
                                    size_t index, char *out,
                                    size_t out_size) {
  char dir[1024];
  if(destructive_stream_reverse_dir_path(ctx, dir, sizeof(dir)) != 0) return -1;
  int n = snprintf(out, out_size, "%s/%08zu.rev", dir, index);
  if(n < 0 || (size_t)n >= out_size) {
    errno = ENAMETOOLONG;
    return -1;
  }
  return 0;
}

static int
destructive_stream_ensure_reverse_dir(destructive_stream_ctx_t *ctx,
                                      char *err, size_t err_size) {
  char dir[1024];
  if(destructive_stream_reverse_dir_path(ctx, dir, sizeof(dir)) != 0) {
    set_err(err, err_size, "stream reverse directory path too long");
    return -1;
  }
  if(mkdir(dir, 0777) != 0 && errno != EEXIST) {
    set_err(err, err_size, "create stream reverse directory: %s",
            strerror(errno));
    return -1;
  }
  return 0;
}

static void
destructive_stream_fsync_parent_best_effort(const char *path) {
  char parent[1024], base[256];
  if(path_parent_base(path, parent, sizeof(parent), base, sizeof(base)) != 0) {
    return;
  }
#ifdef O_DIRECTORY
  int fd = open(parent, O_RDONLY | O_DIRECTORY);
#else
  int fd = open(parent, O_RDONLY);
#endif
  if(fd >= 0) {
    fsync(fd);
    close(fd);
  }
}

static int
sync_completed_output_file(int fd, char *err, size_t err_size) {
  if(fd < 0) {
    errno = EINVAL;
    set_err(err, err_size, "sync output: bad file descriptor");
    return -1;
  }
  if(fsync(fd) != 0) {
    set_err(err, err_size, "sync output: %s", strerror(errno));
    return -1;
  }
  return 0;
}

static int
destructive_stream_stat_size(const char *path, int *exists,
                             uint64_t *size_out, char *err,
                             size_t err_size) {
  struct stat st;
  if(exists) *exists = 0;
  if(size_out) *size_out = 0;
  if(stat(path, &st) != 0) {
    if(errno == ENOENT) return 0;
    set_err(err, err_size, "stat stream reverse file: %s", strerror(errno));
    return -1;
  }
  if(!S_ISREG(st.st_mode)) {
    set_err(err, err_size, "stream reverse path is not a file");
    errno = EINVAL;
    return -1;
  }
  if(exists) *exists = 1;
  if(size_out) *size_out = st.st_size < 0 ? 0 : (uint64_t)st.st_size;
  return 0;
}

static int
destructive_stream_truncate_path(const char *path, uint64_t size,
                                 char *err, size_t err_size) {
  int fd = open(path, O_RDWR);
  if(fd < 0) {
    set_err(err, err_size, "open stream reverse file: %s", strerror(errno));
    return -1;
  }
  int rc = ftruncate(fd, (off_t)size);
  if(rc == 0 && fsync(fd) != 0) rc = -1;
  int saved = errno;
  close(fd);
  if(rc != 0) {
    errno = saved;
    set_err(err, err_size, "truncate stream reverse file: %s",
            strerror(errno));
    return -1;
  }
  return 0;
}

static int
destructive_stream_finalize_reversed_file(destructive_stream_ctx_t *ctx,
                                          destructive_stream_file_t *f,
                                          size_t file_index,
                                          const char *tmp_path,
                                          char *err, size_t err_size) {
  if(!ctx || !f || !tmp_path) return -1;
  if(!path_is_child_of_root(ctx->source_path, f->abs)) {
    set_err(err, err_size, "refusing to replace source outside app folder");
    errno = EINVAL;
    return -1;
  }
  if(rename(tmp_path, f->abs) != 0) {
    set_err(err, err_size, "install reversed source file: %s",
            strerror(errno));
    return -1;
  }
  destructive_stream_fsync_parent_best_effort(f->abs);
  f->reverse_pos = f->original_size;
  destructive_stream_file_set_reversed(f, 1);
  if(destructive_stream_write_file_record(ctx, file_index, err, err_size) != 0 ||
     destructive_stream_sync(ctx, err, err_size) != 0) {
    return -1;
  }
  destructive_stream_fsync_parent_best_effort(tmp_path);
  return 0;
}

static int
destructive_stream_reconcile_reverse_file(destructive_stream_ctx_t *ctx,
                                          destructive_stream_file_t *f,
                                          size_t file_index,
                                          char *err, size_t err_size) {
  char tmp_path[1024];
  int src_exists = 0, tmp_exists = 0;
  uint64_t src_size = 0, tmp_size = 0;
  if(!ctx || !f) return -1;
  if(destructive_stream_reverse_tmp_path(ctx, file_index, tmp_path,
                                         sizeof(tmp_path)) != 0) {
    set_err(err, err_size, "stream reverse temp path too long");
    return -1;
  }
  if(f->deleted) {
    unlink(tmp_path);
    return 0;
  }
  if(destructive_stream_file_reversed(f)) {
    unlink(tmp_path);
    return 0;
  }
  if(destructive_stream_stat_size(f->abs, &src_exists, &src_size,
                                  err, err_size) != 0 ||
     destructive_stream_stat_size(tmp_path, &tmp_exists, &tmp_size,
                                  err, err_size) != 0) {
    return -1;
  }
  if(f->original_size <= 1) {
    f->reverse_pos = f->original_size;
    destructive_stream_file_set_reversed(f, 1);
    unlink(tmp_path);
    return destructive_stream_write_file_record(ctx, file_index, err,
                                                err_size);
  }

  if(src_exists && tmp_exists) {
    if(src_size > f->original_size || tmp_size > f->original_size) {
      set_err(err, err_size, "bad stream reverse sizes");
      errno = EIO;
      return -1;
    }
    uint64_t expected_tmp = f->original_size - src_size;
    if(tmp_size > expected_tmp) {
      if(destructive_stream_truncate_path(tmp_path, expected_tmp,
                                          err, err_size) != 0) {
        return -1;
      }
      tmp_size = expected_tmp;
    }
    if(tmp_size < expected_tmp) {
      set_err(err, err_size, "stream reverse temp is missing committed data");
      errno = EIO;
      return -1;
    }
    f->reverse_pos = expected_tmp;
    if(expected_tmp == f->original_size) {
      return destructive_stream_finalize_reversed_file(ctx, f, file_index,
                                                       tmp_path, err, err_size);
    }
    return destructive_stream_write_file_record(ctx, file_index, err, err_size);
  }

  if(src_exists && !tmp_exists) {
    if(src_size == f->original_size) {
      if(f->reverse_pos >= f->original_size) {
        f->reverse_pos = f->original_size;
        destructive_stream_file_set_reversed(f, 1);
      } else {
        f->reverse_pos = 0;
      }
      return destructive_stream_write_file_record(ctx, file_index, err,
                                                  err_size);
    }
    set_err(err, err_size, "stream reverse temp missing after source truncation");
    errno = EIO;
    return -1;
  }

  if(!src_exists && tmp_exists) {
    if(tmp_size == f->original_size) {
      return destructive_stream_finalize_reversed_file(ctx, f, file_index,
                                                       tmp_path, err, err_size);
    }
    set_err(err, err_size, "stream source missing during partial reverse");
    errno = EIO;
    return -1;
  }

  if(f->original_size == 0) {
    f->reverse_pos = 0;
    destructive_stream_file_set_reversed(f, 1);
    return destructive_stream_write_file_record(ctx, file_index, err, err_size);
  }
  set_err(err, err_size, "stream source and reverse temp are missing");
  errno = ENOENT;
  return -1;
}

static int
destructive_stream_reconcile_all_reverse_files(destructive_stream_ctx_t *ctx,
                                               char *err, size_t err_size) {
  if(!ctx) return 0;
  for(size_t i = 0; i < ctx->file_count; i++) {
    destructive_stream_file_t *f = &ctx->files[i];
    if(f->deleted || destructive_stream_file_reversed(f)) continue;
    if(f->reverse_pos == 0 && f->original_size > 1) continue;
    if(destructive_stream_reconcile_reverse_file(ctx, &ctx->files[i], i,
                                                err, err_size) != 0) {
      return -1;
    }
  }
  return destructive_stream_sync(ctx, err, err_size);
}

static int
destructive_stream_reconcile_committed_files(destructive_stream_ctx_t *ctx,
                                             char *err, size_t err_size) {
  if(!ctx) return 0;
  for(size_t i = 0; i < ctx->file_count; i++) {
    destructive_stream_file_t *f = &ctx->files[i];
    int exists = 0;
    uint64_t size = 0;
    if(f->deleted && f->committed < f->original_size) {
      set_err(err, err_size, "stream journal deleted a partial source file");
      errno = EIO;
      return -1;
    }
    if(f->committed == 0 && !f->deleted) continue;
    if(f->committed > f->original_size) {
      set_err(err, err_size, "bad stream committed size");
      errno = EIO;
      return -1;
    }
    if(!path_is_child_of_root(ctx->source_path, f->abs)) {
      set_err(err, err_size, "refusing to recover source outside app folder");
      errno = EINVAL;
      return -1;
    }
    if(destructive_stream_stat_size(f->abs, &exists, &size, err, err_size) != 0) {
      return -1;
    }
    if(f->committed >= f->original_size || f->deleted) {
      if(exists && unlink(f->abs) != 0 && errno != ENOENT) {
        set_err(err, err_size, "delete committed source file: %s",
                strerror(errno));
        return -1;
      }
      f->deleted = 1;
      if(exists) destructive_stream_fsync_parent_best_effort(f->abs);
      try_remove_empty_parent_dirs(ctx->source_path, f->abs);
      if(destructive_stream_write_file_record(ctx, i, err, err_size) != 0) {
        return -1;
      }
      continue;
    }
    if(!destructive_stream_file_reversed(f)) {
      set_err(err, err_size, "stream committed source is not reversed");
      errno = EIO;
      return -1;
    }
    uint64_t expected = f->original_size - f->committed;
    if(!exists) {
      set_err(err, err_size, "stream committed source is missing");
      errno = ENOENT;
      return -1;
    }
    if(size < expected) {
      set_err(err, err_size, "stream committed source is shorter than journal");
      errno = EIO;
      return -1;
    }
    if(size > expected) {
      if(destructive_stream_truncate_path(f->abs, expected, err, err_size) != 0) {
        return -1;
      }
      if(destructive_stream_write_file_record(ctx, i, err, err_size) != 0) {
        return -1;
      }
    }
  }
  return destructive_stream_sync(ctx, err, err_size);
}

static void
destructive_stream_publish_resume_progress(const destructive_stream_ctx_t *ctx) {
  if(!ctx) return;
  destructive_stream_publish_budget(ctx);
  uint64_t copied = ctx->compression_complete
      ? ctx->nested_size
      : ctx->completed_blocks * PFS_BLOCK_SIZE;
  if(copied > ctx->nested_size) copied = ctx->nested_size;
  atomic_store(&g_job.total_bytes,
               ctx->nested_size > (uint64_t)LONG_MAX ? LONG_MAX :
               (long)ctx->nested_size);
  atomic_store(&g_job.copied_bytes,
               copied > (uint64_t)LONG_MAX ? LONG_MAX : (long)copied);
  uint64_t output_pos = ctx->data_pos;
  if(ctx->offsets && ctx->completed_blocks <= ctx->block_count &&
     ctx->offsets[ctx->completed_blocks] != 0) {
    output_pos = ctx->offsets[ctx->completed_blocks];
  }
  uint64_t compressed_output =
      output_pos > ctx->pfsc_header_size ? output_pos - ctx->pfsc_header_size : 0;
  atomic_store(&g_job.compressed_output_bytes,
               compressed_output > (uint64_t)LONG_MAX ? LONG_MAX :
               (long)compressed_output);
  atomic_store(&g_job.total_blocks,
               ctx->block_count > (uint64_t)LONG_MAX ? LONG_MAX :
               (long)ctx->block_count);
  atomic_store(&g_job.done_files,
               ctx->completed_blocks > (uint64_t)INT_MAX ? INT_MAX :
               (int)ctx->completed_blocks);
  atomic_store(&g_job.total_files,
               ctx->block_count > (uint64_t)INT_MAX ? INT_MAX :
               (int)ctx->block_count);
  atomic_store(&g_job.destructive_stream_active,
               ctx->mutation_started ? 1 : 0);
}

static void
destructive_stream_remove_reverse_dir(destructive_stream_ctx_t *ctx) {
  char dir[1024];
  if(destructive_stream_reverse_dir_path(ctx, dir, sizeof(dir)) == 0) {
    remove_tree_local(dir);
  }
}

static int
destructive_stream_load(const char *journal_path, destructive_stream_ctx_t *ctx,
                        char *err, size_t err_size) {
  destructive_stream_header_disk_t h;
  int fd = -1;
  destructive_stream_init(ctx);
  fd = open(journal_path, O_RDWR);
  if(fd < 0) {
    set_err(err, err_size, "open stream journal: %s", strerror(errno));
    return -1;
  }
  ctx->journal_fd = fd;
  snprintf(ctx->journal_path, sizeof(ctx->journal_path), "%s", journal_path);
  if(read_exact_at(fd, &h, sizeof(h), 0) != 0) {
    set_err(err, err_size, "read stream journal: %s", strerror(errno));
    goto fail;
  }
  if(memcmp(h.magic, PFS_STREAM_JOURNAL_MAGIC,
            strlen(PFS_STREAM_JOURNAL_MAGIC)) != 0 ||
     h.version != PFS_STREAM_JOURNAL_VERSION ||
     h.header_size != sizeof(h) ||
     h.file_record_size != sizeof(destructive_stream_file_disk_t) ||
     h.file_count == 0 || h.block_count == 0) {
    set_err(err, err_size, "invalid stream journal");
    errno = EINVAL;
    goto fail;
  }

  ctx->file_count = h.file_count;
  ctx->files = calloc(ctx->file_count, sizeof(*ctx->files));
  ctx->offsets = calloc((size_t)(h.block_count + 1ULL), sizeof(*ctx->offsets));
  ctx->owns_offsets = 1;
  if(!ctx->files || !ctx->offsets) {
    set_err(err, err_size, "out of memory");
    goto fail;
  }
  ctx->format = (int)h.format;
  ctx->nested_type = (int)h.nested_type;
  ctx->rollback_requested = 0;
  ctx->mutation_started = h.mutation_started != 0;
  ctx->compression_complete = h.compression_complete != 0;
  ctx->output_finalized = h.output_finalized != 0;
  ctx->block_count = h.block_count;
  ctx->logical_size = h.logical_size;
  ctx->nested_size = h.nested_size;
  ctx->pfsc_header_size = h.pfsc_header_size;
  ctx->completed_blocks = h.completed_blocks;
  ctx->journaled_blocks = h.completed_blocks;
  ctx->data_pos = h.data_pos;
  ctx->budget_bytes = h.budget_bytes;
  ctx->reserve_bytes = h.reserve_bytes;
  ctx->current_credit = h.current_credit;
  ctx->actual_output_bytes = h.actual_output_bytes;
  ctx->actual_deleted_bytes = h.actual_deleted_bytes;
  ctx->actual_reverse_temp_bytes = h.actual_reverse_temp_bytes;
  ctx->forward_file_count = h.forward_file_count;
  ctx->reverse_file_count = h.reverse_file_count;
  snprintf(ctx->source_path, sizeof(ctx->source_path), "%s", h.source_path);
  snprintf(ctx->output_path, sizeof(ctx->output_path), "%s", h.output_path);
  snprintf(ctx->tmp_path, sizeof(ctx->tmp_path), "%s", h.tmp_path);
  snprintf(ctx->vhash_tmp_path, sizeof(ctx->vhash_tmp_path), "%s",
           h.vhash_tmp_path);
  snprintf(ctx->vhash_output_path, sizeof(ctx->vhash_output_path), "%s",
           h.vhash_output_path);
  snprintf(ctx->nested_name, sizeof(ctx->nested_name), "%s", h.nested_name);

  for(size_t i = 0; i < ctx->file_count; i++) {
    destructive_stream_file_disk_t rec;
    if(read_exact_at(fd, &rec, sizeof(rec),
                     (off_t)destructive_stream_file_record_offset(i)) != 0) {
      set_err(err, err_size, "read stream journal file: %s", strerror(errno));
      goto fail;
    }
    snprintf(ctx->files[i].rel, sizeof(ctx->files[i].rel), "%s", rec.rel);
    if(join_abs_rel(ctx->files[i].abs, sizeof(ctx->files[i].abs),
                    ctx->source_path, rec.rel) != 0) {
      set_err(err, err_size, "bad stream journal source path");
      goto fail;
    }
    ctx->files[i].original_size = rec.original_size;
    ctx->files[i].virtual_offset = rec.virtual_offset;
    ctx->files[i].committed = rec.committed;
    ctx->files[i].reverse_pos = rec.reverse_pos;
    ctx->files[i].reverse_claim = rec.reverse_pos;
    ctx->files[i].predicted_stored = rec.predicted_stored;
    ctx->files[i].schedule_order = rec.schedule_order;
    ctx->files[i].predicted_gain_permille = rec.predicted_gain_permille;
    ctx->files[i].reverse_required = rec.reverse_required != 0;
    ctx->files[i].passthrough_delete = rec.passthrough_delete != 0;
    destructive_stream_file_set_reversed(&ctx->files[i], rec.reversed != 0);
    ctx->files[i].deleted = rec.deleted != 0;
  }

  uint64_t offsets_base = destructive_stream_offsets_offset(ctx->file_count);
  unsigned char raw[PFSC_OFFSET_ENTRY_SIZE * 256U];
  uint64_t index = 0;
  while(index <= ctx->block_count) {
    uint64_t remaining = ctx->block_count + 1ULL - index;
    uint64_t n64 = remaining > 256U ? 256U : remaining;
    size_t n = (size_t)n64;
    if(read_exact_at(fd, raw, n * 8U,
                     (off_t)(offsets_base + index * 8ULL)) != 0) {
      set_err(err, err_size, "read stream journal offsets: %s", strerror(errno));
      goto fail;
    }
    for(size_t i = 0; i < n; i++) ctx->offsets[index + i] = rd64(raw + i * 8U);
    index += n64;
  }

  destructive_stream_publish_resume_progress(ctx);
  job_set_current("Checking stream source files");
  if(destructive_stream_reconcile_all_reverse_files(ctx, err, err_size) != 0) {
    goto fail;
  }
  if(destructive_stream_reconcile_committed_files(ctx, err, err_size) != 0) {
    goto fail;
  }
  return 0;

fail:
  destructive_stream_free(ctx);
  return -1;
}

static int
destructive_stream_create(const char *journal_path, const pfs_app_info_t *info,
                          const char *tmp_path, const char *vhash_tmp_path,
                          const char *vhash_output_path, int format,
                          const pfs_layout_t *nested, uint64_t block_count,
                          uint64_t logical_size, uint64_t pfsc_header_size,
                          uint64_t *offsets, destructive_stream_ctx_t *ctx,
                          char *err, size_t err_size) {
  destructive_stream_init(ctx);
  snprintf(ctx->journal_path, sizeof(ctx->journal_path), "%s", journal_path);
  snprintf(ctx->source_path, sizeof(ctx->source_path), "%s", info->source_path);
  snprintf(ctx->output_path, sizeof(ctx->output_path), "%s", info->output_path);
  snprintf(ctx->tmp_path, sizeof(ctx->tmp_path), "%s", tmp_path);
  snprintf(ctx->vhash_tmp_path, sizeof(ctx->vhash_tmp_path), "%s",
           vhash_tmp_path);
  snprintf(ctx->vhash_output_path, sizeof(ctx->vhash_output_path), "%s",
           vhash_output_path);
  snprintf(ctx->nested_name, sizeof(ctx->nested_name), "%s", info->nested_name);
  ctx->format = format;
  ctx->nested_type = info->nested_type;
  ctx->block_count = block_count;
  ctx->logical_size = logical_size;
  ctx->nested_size = nested->image_size;
  ctx->pfsc_header_size = pfsc_header_size;
  ctx->completed_blocks = 0;
  ctx->journaled_blocks = 0;
  ctx->data_pos = pfsc_header_size;
  ctx->budget_bytes = info->stream_budget_bytes ?
      info->stream_budget_bytes : PFS_STREAM_DEFAULT_BUDGET_BYTES;
  ctx->reserve_bytes = info->stream_reserve_bytes;
  if(ctx->reserve_bytes >= ctx->budget_bytes) ctx->reserve_bytes = 0;
  ctx->current_credit = ctx->budget_bytes - ctx->reserve_bytes;
  ctx->actual_output_bytes = 0;
  ctx->actual_deleted_bytes = 0;
  ctx->actual_reverse_temp_bytes = 0;
  ctx->forward_file_count = info->stream_forward_files;
  ctx->reverse_file_count = info->stream_reverse_files;
  ctx->file_count = nested->file_count;
  ctx->offsets = offsets;
  ctx->owns_offsets = 0;
  ctx->files = calloc(ctx->file_count, sizeof(*ctx->files));
  if(!ctx->files) {
    set_err(err, err_size, "out of memory");
    return -1;
  }
  for(size_t i = 0; i < nested->file_count; i++) {
    destructive_stream_file_t *dst = &ctx->files[i];
    const pfs_file_node_t *src = &nested->files[i];
    snprintf(dst->rel, sizeof(dst->rel), "%s", src->rel);
    snprintf(dst->abs, sizeof(dst->abs), "%s", src->abs);
    dst->original_size = src->raw_size;
    dst->virtual_offset = src->block_start * PFS_BLOCK_SIZE;
    dst->reverse_claim = 0;
    dst->predicted_stored = src->stream_predicted_stored;
    dst->schedule_order = src->stream_schedule_order;
    dst->predicted_gain_permille = src->stream_predicted_gain_permille;
    dst->reverse_required = src->stream_reverse_required;
    dst->passthrough_delete = src->stream_passthrough_delete;
    destructive_stream_file_set_reversed(dst, src->raw_size <= 1);
  }

  ctx->journal_fd = open(journal_path, O_RDWR | O_CREAT | O_TRUNC, 0666);
  if(ctx->journal_fd < 0) {
    set_err(err, err_size, "create stream journal: %s", strerror(errno));
    return -1;
  }
  uint64_t final_size = destructive_stream_offsets_offset(ctx->file_count) +
                        (ctx->block_count + 1ULL) * PFSC_OFFSET_ENTRY_SIZE;
  if(ftruncate(ctx->journal_fd, (off_t)final_size) != 0) {
    set_err(err, err_size, "size stream journal: %s", strerror(errno));
    return -1;
  }
  if(destructive_stream_write_header(ctx, err, err_size) != 0) return -1;
  for(size_t i = 0; i < ctx->file_count; i++) {
    if(destructive_stream_write_file_record(ctx, i, err, err_size) != 0) {
      return -1;
    }
  }
  if(destructive_stream_write_offsets(ctx, 0, ctx->block_count + 1ULL,
                                      err, err_size) != 0 ||
     destructive_stream_sync(ctx, err, err_size) != 0) {
    return -1;
  }
  return 0;
}

static destructive_stream_file_t *
destructive_stream_find_file(destructive_stream_ctx_t *ctx, const char *path,
                             size_t *index_out) {
  if(!ctx || !path) return NULL;
  for(size_t i = 0; i < ctx->file_count; i++) {
    if(!strcmp(ctx->files[i].abs, path)) {
      if(index_out) *index_out = i;
      return &ctx->files[i];
    }
  }
  return NULL;
}

static int
destructive_stream_mark_mutation_started(destructive_stream_ctx_t *ctx,
                                         char *err, size_t err_size) {
  if(!ctx || ctx->mutation_started) return 0;
  ctx->mutation_started = 1;
  atomic_store(&g_job.destructive_stream_active, 1);
  atomic_store(&g_job.cancel, 0);
  atomic_store(&g_job.rollback_requested, 0);
  if(destructive_stream_write_header(ctx, err, err_size) != 0 ||
     destructive_stream_sync(ctx, err, err_size) != 0) {
    return -1;
  }
  return 0;
}

typedef enum reverse_chunk_state {
  REVERSE_CHUNK_FREE = 0,
  REVERSE_CHUNK_FILLING,
  REVERSE_CHUNK_READY,
} reverse_chunk_state_t;

typedef struct reverse_chunk_slot {
  unsigned char *buf;
  size_t cap;
  size_t len;
  uint64_t out_off;
  reverse_chunk_state_t state;
} reverse_chunk_slot_t;

typedef struct reverse_chunk_pool {
  int src_fd;
  uint64_t original_size;
  uint64_t next_out;
  size_t chunk_size;
  reverse_chunk_slot_t *slots;
  int slot_count;
  pthread_mutex_t lock;
  pthread_cond_t cond;
  int stop;
  int error;
  char err[256];
} reverse_chunk_pool_t;

static void
reverse_chunk_pool_set_error(reverse_chunk_pool_t *pool, int saved,
                             const char *fmt, ...) {
  if(!pool) return;
  pthread_mutex_lock(&pool->lock);
  if(!pool->error) {
    pool->error = saved ? saved : EIO;
    if(fmt && fmt[0]) {
      va_list ap;
      va_start(ap, fmt);
      vsnprintf(pool->err, sizeof(pool->err), fmt, ap);
      va_end(ap);
    } else {
      snprintf(pool->err, sizeof(pool->err), "%s", strerror(pool->error));
    }
  }
  pthread_cond_broadcast(&pool->cond);
  pthread_mutex_unlock(&pool->lock);
}

static void *
destructive_stream_reverse_chunk_worker(void *arg) {
  reverse_chunk_pool_t *pool = (reverse_chunk_pool_t *)arg;
  if(!pool) return NULL;
  for(;;) {
    reverse_chunk_slot_t *slot = NULL;
    pthread_mutex_lock(&pool->lock);
    while(!pool->stop && !pool->error && pool->next_out < pool->original_size) {
      for(int i = 0; i < pool->slot_count; i++) {
        if(pool->slots[i].state == REVERSE_CHUNK_FREE) {
          slot = &pool->slots[i];
          break;
        }
      }
      if(slot) break;
      pthread_cond_wait(&pool->cond, &pool->lock);
    }
    if(pool->stop || pool->error || pool->next_out >= pool->original_size) {
      pthread_mutex_unlock(&pool->lock);
      break;
    }
    uint64_t remaining = pool->original_size - pool->next_out;
    size_t n = remaining > (uint64_t)pool->chunk_size ?
               pool->chunk_size : (size_t)remaining;
    slot->out_off = pool->next_out;
    slot->len = n;
    slot->state = REVERSE_CHUNK_FILLING;
    pool->next_out += (uint64_t)n;
    pthread_mutex_unlock(&pool->lock);

    uint64_t read_off = pool->original_size - slot->out_off - (uint64_t)n;
    if(read_exact_at(pool->src_fd, slot->buf, n, (off_t)read_off) != 0) {
      int saved = errno ? errno : EIO;
      reverse_chunk_pool_set_error(pool, saved, "read source for reverse: %s",
                                   strerror(saved));
      break;
    }
    reverse_bytes(slot->buf, n);

    pthread_mutex_lock(&pool->lock);
    slot->state = REVERSE_CHUNK_READY;
    pthread_cond_broadcast(&pool->cond);
    pthread_mutex_unlock(&pool->lock);
  }
  return NULL;
}

static int
destructive_stream_reverse_file(destructive_stream_ctx_t *ctx,
                                destructive_stream_file_t *f,
                                size_t file_index,
                                char *err, size_t err_size) {
  if(!ctx || !f || destructive_stream_file_reversed(f) || f->original_size <= 1) {
    if(f && !destructive_stream_file_reversed(f)) {
      f->reverse_pos = f->original_size;
      destructive_stream_file_set_reversed(f, 1);
      return destructive_stream_write_file_record(ctx, file_index, err,
                                                  err_size);
    }
    return 0;
  }
  if(destructive_stream_mark_mutation_started(ctx, err, err_size) != 0) {
    return -1;
  }
  if(destructive_stream_ensure_reverse_dir(ctx, err, err_size) != 0 ||
     destructive_stream_reconcile_reverse_file(ctx, f, file_index,
                                               err, err_size) != 0) {
    return -1;
  }
  if(destructive_stream_file_reversed(f)) return 0;

  char tmp_path[1024];
  if(destructive_stream_reverse_tmp_path(ctx, file_index, tmp_path,
                                         sizeof(tmp_path)) != 0) {
    set_err(err, err_size, "stream reverse temp path too long");
    return -1;
  }

  int src_fd = open(f->abs, O_RDWR);
  if(src_fd < 0) {
    set_err(err, err_size, "open source file: %s", strerror(errno));
    return -1;
  }
  int tmp_fd = open(tmp_path, O_RDWR | O_CREAT, 0666);
  if(tmp_fd < 0) {
    int saved = errno;
    close(src_fd);
    errno = saved;
    set_err(err, err_size, "open stream reverse temp: %s", strerror(errno));
    return -1;
  }
  struct stat src_meta;
  if(fstat(src_fd, &src_meta) == 0) {
    fchmod(tmp_fd, src_meta.st_mode & 07777);
  }

  reverse_chunk_pool_t pool;
  memset(&pool, 0, sizeof(pool));
  pool.src_fd = src_fd;
  pool.original_size = f->original_size;
  pool.next_out = f->reverse_pos;
  int worker_count = PFS_STREAM_REVERSE_WORKERS;
  uint64_t remaining_chunks = f->original_size > f->reverse_pos ?
      ceil_div_u64(f->original_size - f->reverse_pos,
                   PFS_STREAM_REVERSE_CHUNK_MIN_SIZE) : 0;
  if(remaining_chunks > 0 && remaining_chunks < (uint64_t)worker_count) {
    worker_count = (int)remaining_chunks;
  }
  if(worker_count < 1) worker_count = 1;
  pool.slot_count = worker_count;
  pool.slots = calloc((size_t)pool.slot_count, sizeof(*pool.slots));
  pthread_t *threads = calloc((size_t)worker_count, sizeof(*threads));
  int workers_started = 0;
  int pool_sync_initialized = 0;
  int rc = -1;
  if(!pool.slots || !threads) {
    set_err(err, err_size, "out of memory");
    free(pool.slots);
    free(threads);
    close(src_fd);
    close(tmp_fd);
    return -1;
  }
  for(int i = 0; i < pool.slot_count; i++) {
    pool.slots[i].buf = destructive_stream_alloc_reverse_buffer(
        ctx, &pool.slots[i].cap, err, err_size);
    if(!pool.slots[i].buf) {
      for(int j = 0; j < i; j++) free(pool.slots[j].buf);
      free(pool.slots);
      free(threads);
      close(src_fd);
      close(tmp_fd);
      return -1;
    }
    if(i == 0 || pool.slots[i].cap < pool.chunk_size) {
      pool.chunk_size = pool.slots[i].cap;
    }
  }
  if(pool.chunk_size == 0) pool.chunk_size = PFS_STREAM_REVERSE_CHUNK_MIN_SIZE;
  if(pthread_mutex_init(&pool.lock, NULL) != 0 ||
     pthread_cond_init(&pool.cond, NULL) != 0) {
    set_err(err, err_size, "init reverse chunk workers failed");
    goto done;
  }
  pool_sync_initialized = 1;

  if(!ctx->reverse_ahead_started) job_set_current(f->rel[0] ? f->rel : f->abs);

  struct stat st;
  if(fstat(src_fd, &st) != 0 || st.st_size < 0) {
    set_err(err, err_size, "stat source for reverse: %s", strerror(errno));
    goto done;
  }
  uint64_t src_size = (uint64_t)st.st_size;
  if(src_size > f->original_size) {
    set_err(err, err_size, "bad source size during reverse");
    errno = EIO;
    goto done;
  }
  uint64_t moved = f->original_size - src_size;
  if(moved != f->reverse_pos) {
    f->reverse_pos = moved;
    pool.next_out = moved;
    if(destructive_stream_write_file_record(ctx, file_index, err,
                                            err_size) != 0 ||
       destructive_stream_sync(ctx, err, err_size) != 0) {
      goto done;
    }
  }

  for(int i = 0; i < worker_count; i++) {
    int trc = pthread_create(&threads[i], NULL,
                             destructive_stream_reverse_chunk_worker, &pool);
    if(trc != 0) {
      set_err(err, err_size, "start reverse chunk worker: %s", strerror(trc));
      goto done;
    }
    workers_started++;
  }

  while(f->reverse_pos < f->original_size) {
    if(job_cancelled()) atomic_store(&g_job.cancel, 0);

    reverse_chunk_slot_t *slot = NULL;
    pthread_mutex_lock(&pool.lock);
    while(!pool.error) {
      for(int i = 0; i < pool.slot_count; i++) {
        if(pool.slots[i].state == REVERSE_CHUNK_READY &&
           pool.slots[i].out_off == f->reverse_pos) {
          slot = &pool.slots[i];
          break;
        }
      }
      if(slot) break;
      pthread_cond_wait(&pool.cond, &pool.lock);
    }
    int pool_error = pool.error;
    char pool_err[256];
    snprintf(pool_err, sizeof(pool_err), "%s", pool.err);
    pthread_mutex_unlock(&pool.lock);
    if(pool_error) {
      errno = pool_error;
      set_err(err, err_size, "%s", pool_err[0] ? pool_err : strerror(pool_error));
      goto done;
    }

    uint64_t out_off = slot->out_off;
    size_t n = slot->len;
    if(out_off != f->reverse_pos || n == 0 ||
       out_off + (uint64_t)n > f->original_size) {
      set_err(err, err_size, "bad reverse chunk order");
      errno = EIO;
      goto done;
    }
    if(pwrite_all_local(tmp_fd, slot->buf, n, (off_t)out_off) != 0 ||
       fsync(tmp_fd) != 0) {
      set_err(err, err_size, "write stream reverse temp: %s", strerror(errno));
      goto done;
    }
    uint64_t new_src_size = f->original_size - out_off - (uint64_t)n;
    if(ftruncate(src_fd, (off_t)new_src_size) != 0 || fsync(src_fd) != 0) {
      set_err(err, err_size, "truncate source after reverse: %s",
              strerror(errno));
      goto done;
    }

    f->reverse_pos += (uint64_t)n;
    if(destructive_stream_write_file_record(ctx, file_index, err, err_size) != 0 ||
       destructive_stream_sync(ctx, err, err_size) != 0) {
      goto done;
    }
    if(ctx->reverse_ahead_sync_initialized) {
      pthread_mutex_lock(&ctx->reverse_ahead_lock);
      pthread_cond_broadcast(&ctx->reverse_ahead_cond);
      pthread_mutex_unlock(&ctx->reverse_ahead_lock);
    }
    pthread_mutex_lock(&pool.lock);
    slot->state = REVERSE_CHUNK_FREE;
    slot->len = 0;
    pthread_cond_broadcast(&pool.cond);
    pthread_mutex_unlock(&pool.lock);
  }
  pthread_mutex_lock(&pool.lock);
  pool.stop = 1;
  pthread_cond_broadcast(&pool.cond);
  pthread_mutex_unlock(&pool.lock);
  for(int i = 0; i < workers_started; i++) pthread_join(threads[i], NULL);
  workers_started = 0;
  close(src_fd);
  src_fd = -1;
  close(tmp_fd);
  tmp_fd = -1;

  if(f->reverse_pos >= f->original_size) {
    if(destructive_stream_finalize_reversed_file(ctx, f, file_index, tmp_path,
                                                 err, err_size) != 0) {
      goto done;
    }
  }
  rc = 0;

done:
  if(pool_sync_initialized) {
    pthread_mutex_lock(&pool.lock);
    pool.stop = 1;
    pthread_cond_broadcast(&pool.cond);
    pthread_mutex_unlock(&pool.lock);
  }
  for(int i = 0; i < workers_started; i++) pthread_join(threads[i], NULL);
  if(pool_sync_initialized) {
    pthread_cond_destroy(&pool.cond);
    pthread_mutex_destroy(&pool.lock);
  }
  for(int i = 0; i < pool.slot_count; i++) free(pool.slots[i].buf);
  free(pool.slots);
  free(threads);
  if(src_fd >= 0) close(src_fd);
  if(tmp_fd >= 0) close(tmp_fd);
  return rc;
}

typedef struct reverse_ahead_claim {
  size_t file_index;
  uint64_t out_off;
  size_t len;
  uint64_t original_size;
  char source_path[1024];
  char tmp_path[1024];
} reverse_ahead_claim_t;

static uint64_t
destructive_stream_file_virtual_end(const destructive_stream_file_t *f);
static size_t
destructive_stream_focus_file_index(const destructive_stream_ctx_t *ctx,
                                    uint64_t focus_bytes);
static int
destructive_stream_file_passthrough_delete(const destructive_stream_file_t *f);

static int
destructive_stream_prepare_reverse_ahead(destructive_stream_ctx_t *ctx,
                                         char *err, size_t err_size) {
  if(!ctx) return 0;
  if(destructive_stream_mark_mutation_started(ctx, err, err_size) != 0 ||
     destructive_stream_ensure_reverse_dir(ctx, err, err_size) != 0) {
    return -1;
  }
  for(size_t i = 0; i < ctx->file_count; i++) {
    destructive_stream_file_t *f = &ctx->files[i];
    if(f->deleted || destructive_stream_file_reversed(f)) {
      f->reverse_claim = f->reverse_pos;
      continue;
    }
    if(destructive_stream_file_passthrough_delete(f)) {
      int exists = 0;
      uint64_t size = 0;
      if(destructive_stream_stat_size(f->abs, &exists, &size,
                                      err, err_size) != 0) {
        return -1;
      }
      if(!exists || size != f->original_size) {
        set_err(err, err_size,
                "stream passthrough source changed before compression");
        errno = EIO;
        return -1;
      }
      char tmp_path[1024];
      if(destructive_stream_reverse_tmp_path(ctx, i, tmp_path,
                                             sizeof(tmp_path)) == 0) {
        unlink(tmp_path);
      }
      f->reverse_claim = f->reverse_pos;
      continue;
    }
    if(f->original_size <= 1) {
      f->reverse_pos = f->original_size;
      f->reverse_claim = f->original_size;
      destructive_stream_file_set_reversed(f, 1);
      if(destructive_stream_write_file_record(ctx, i, err, err_size) != 0) {
        return -1;
      }
      continue;
    }
    if(destructive_stream_reconcile_reverse_file(ctx, f, i, err,
                                                 err_size) != 0) {
      return -1;
    }
    f->reverse_claim = f->reverse_pos;
  }
  return destructive_stream_sync(ctx, err, err_size);
}

static int
destructive_stream_reverse_ahead_claim(destructive_stream_ctx_t *ctx,
                                       reverse_ahead_claim_t *claim,
                                       size_t max_len) {
  if(!ctx || !claim) return 0;
  memset(claim, 0, sizeof(*claim));
  if(max_len > PFS_STREAM_REVERSE_CHUNK_MAX_SIZE) {
    max_len = PFS_STREAM_REVERSE_CHUNK_MAX_SIZE;
  }
  if(max_len < PFS_STREAM_REVERSE_CHUNK_MIN_SIZE) {
    max_len = PFS_STREAM_REVERSE_CHUNK_MIN_SIZE;
  }
  pthread_mutex_lock(&ctx->reverse_ahead_lock);
  for(;;) {
    if(ctx->reverse_ahead_stop || ctx->reverse_ahead_error) {
      pthread_mutex_unlock(&ctx->reverse_ahead_lock);
      return 0;
    }

    if(ctx->reverse_ahead_demand_file < ctx->file_count) {
      size_t i = ctx->reverse_ahead_demand_file;
      destructive_stream_file_t *f = &ctx->files[i];
      if(f->deleted || destructive_stream_file_reversed(f) ||
         destructive_stream_file_passthrough_delete(f) ||
         f->reverse_pos >= ctx->reverse_ahead_demand_pos) {
        ctx->reverse_ahead_demand_file = SIZE_MAX;
        ctx->reverse_ahead_demand_pos = 0;
      } else {
        if(f->reverse_claim < f->reverse_pos) f->reverse_claim = f->reverse_pos;
        if(f->reverse_claim < f->original_size &&
           f->reverse_claim < ctx->reverse_ahead_demand_pos) {
          uint64_t out_off = f->reverse_claim;
          uint64_t remaining = f->original_size - out_off;
          size_t len = remaining > (uint64_t)max_len ? max_len :
                       (size_t)remaining;
          if(out_off + (uint64_t)len > ctx->reverse_ahead_demand_pos) {
            uint64_t demand_remaining =
                ctx->reverse_ahead_demand_pos - out_off;
            if(demand_remaining < (uint64_t)len) len = (size_t)demand_remaining;
          }
          if(len > 0) {
            f->reverse_claim += (uint64_t)len;
            ctx->reverse_ahead_inflight++;
            claim->file_index = i;
            claim->out_off = out_off;
            claim->len = len;
            claim->original_size = f->original_size;
            snprintf(claim->source_path, sizeof(claim->source_path), "%s",
                     f->abs);
            if(destructive_stream_reverse_tmp_path(ctx, i, claim->tmp_path,
                                                   sizeof(claim->tmp_path)) != 0) {
              ctx->reverse_ahead_inflight--;
              ctx->reverse_ahead_error = ENAMETOOLONG;
              snprintf(ctx->reverse_ahead_err, sizeof(ctx->reverse_ahead_err),
                       "%s", "stream reverse temp path too long");
              pthread_cond_broadcast(&ctx->reverse_ahead_cond);
              pthread_mutex_unlock(&ctx->reverse_ahead_lock);
              return 0;
            }
            pthread_mutex_unlock(&ctx->reverse_ahead_lock);
            return 1;
          }
        }
      }
    }

    uint64_t focus_bytes = ctx->completed_blocks * PFS_BLOCK_SIZE;
    if(focus_bytes > ctx->nested_size) focus_bytes = ctx->nested_size;
    uint64_t window_end = UINT64_MAX - focus_bytes <
                          PFS_STREAM_REVERSE_LOOKAHEAD_BYTES ?
                          UINT64_MAX :
                          focus_bytes + PFS_STREAM_REVERSE_LOOKAHEAD_BYTES;
    size_t start = destructive_stream_focus_file_index(ctx, focus_bytes);
    int expanded = 0;
retry_scan:
    for(size_t n = 0; n < ctx->file_count; n++) {
      size_t i = start + n;
      if(i >= ctx->file_count) i -= ctx->file_count;
      destructive_stream_file_t *f = &ctx->files[i];
      if(f->deleted || destructive_stream_file_reversed(f)) continue;
      if(destructive_stream_file_passthrough_delete(f)) continue;
      if(!expanded && f->virtual_offset >= window_end) continue;
      if(f->reverse_claim < f->reverse_pos) f->reverse_claim = f->reverse_pos;
      if(f->reverse_claim >= f->original_size) continue;

      uint64_t out_off = f->reverse_claim;
      uint64_t remaining = f->original_size - out_off;
      size_t len = remaining > (uint64_t)max_len ? max_len :
                   (size_t)remaining;
      if(len == 0) continue;

      f->reverse_claim += (uint64_t)len;
      ctx->reverse_ahead_next_file =
          f->reverse_claim < f->original_size ? i : i + 1;
      ctx->reverse_ahead_inflight++;
      claim->file_index = i;
      claim->out_off = out_off;
      claim->len = len;
      claim->original_size = f->original_size;
      snprintf(claim->source_path, sizeof(claim->source_path), "%s", f->abs);
      if(destructive_stream_reverse_tmp_path(ctx, i, claim->tmp_path,
                                             sizeof(claim->tmp_path)) != 0) {
        ctx->reverse_ahead_inflight--;
        ctx->reverse_ahead_error = ENAMETOOLONG;
        snprintf(ctx->reverse_ahead_err, sizeof(ctx->reverse_ahead_err),
                 "%s", "stream reverse temp path too long");
        pthread_cond_broadcast(&ctx->reverse_ahead_cond);
        pthread_mutex_unlock(&ctx->reverse_ahead_lock);
        return 0;
      }
      pthread_mutex_unlock(&ctx->reverse_ahead_lock);
      return 1;
    }

    if(!expanded && ctx->reverse_ahead_inflight == 0) {
      expanded = 1;
      goto retry_scan;
    }
    if(ctx->reverse_ahead_inflight == 0) {
      pthread_mutex_unlock(&ctx->reverse_ahead_lock);
      return 0;
    }
    pthread_cond_wait(&ctx->reverse_ahead_cond, &ctx->reverse_ahead_lock);
  }
}

static int
destructive_stream_reverse_ahead_finish_claim(destructive_stream_ctx_t *ctx,
                                              size_t file_index, int ok,
                                              int saved,
                                              const char *msg) {
  if(!ctx) return -1;
  pthread_mutex_lock(&ctx->reverse_ahead_lock);
  if(ctx->reverse_ahead_inflight > 0) ctx->reverse_ahead_inflight--;
  if(!ok && !ctx->reverse_ahead_error) {
    ctx->reverse_ahead_error = saved ? saved : EIO;
    snprintf(ctx->reverse_ahead_err, sizeof(ctx->reverse_ahead_err), "%s",
             msg && msg[0] ? msg : strerror(ctx->reverse_ahead_error));
    if(file_index < ctx->file_count) {
      destructive_stream_file_t *f = &ctx->files[file_index];
      if(f->reverse_claim > f->reverse_pos) f->reverse_claim = f->reverse_pos;
    }
  }
  pthread_cond_broadcast(&ctx->reverse_ahead_cond);
  pthread_mutex_unlock(&ctx->reverse_ahead_lock);
  return ok ? 0 : -1;
}

static uint64_t
destructive_stream_file_virtual_end(const destructive_stream_file_t *f) {
  if(!f) return 0;
  if(UINT64_MAX - f->virtual_offset < f->original_size) return UINT64_MAX;
  return f->virtual_offset + f->original_size;
}

static int
destructive_stream_file_passthrough_delete(const destructive_stream_file_t *f) {
  return f && !f->deleted &&
         !destructive_stream_file_reversed(f) &&
         f->passthrough_delete;
}

static size_t
destructive_stream_focus_file_index(const destructive_stream_ctx_t *ctx,
                                    uint64_t focus_bytes) {
  if(!ctx || ctx->file_count == 0) return 0;
  for(size_t i = 0; i < ctx->file_count; i++) {
    const destructive_stream_file_t *f = &ctx->files[i];
    if(f->deleted || destructive_stream_file_reversed(f)) continue;
    if(destructive_stream_file_virtual_end(f) > focus_bytes) return i;
  }
  return 0;
}

static int
destructive_stream_commit_reverse_chunk(destructive_stream_ctx_t *ctx,
                                        const reverse_ahead_claim_t *claim,
                                        char *err, size_t err_size) {
  destructive_stream_file_t *f;
  int tmp_fd = -1;
  int src_fd = -1;
  int rc = -1;
  if(!ctx || !claim || claim->file_index >= ctx->file_count) {
    set_err(err, err_size, "bad reverse chunk claim");
    errno = EINVAL;
    return -1;
  }

  f = &ctx->files[claim->file_index];
  pthread_mutex_lock(&ctx->reverse_ahead_lock);
  while(!ctx->reverse_ahead_stop && !ctx->reverse_ahead_error &&
        f->reverse_pos != claim->out_off) {
    pthread_cond_wait(&ctx->reverse_ahead_cond, &ctx->reverse_ahead_lock);
  }
  if(ctx->reverse_ahead_stop || ctx->reverse_ahead_error) {
    int saved = ctx->reverse_ahead_error ? ctx->reverse_ahead_error : EINTR;
    pthread_mutex_unlock(&ctx->reverse_ahead_lock);
    errno = saved;
    set_err(err, err_size, "%s",
            ctx->reverse_ahead_err[0] ? ctx->reverse_ahead_err :
            strerror(saved));
    return -1;
  }
  f->reverse_committing = 1;
  pthread_mutex_unlock(&ctx->reverse_ahead_lock);

  tmp_fd = open(claim->tmp_path, O_RDWR);
  if(tmp_fd < 0) {
    set_err(err, err_size, "open stream reverse temp: %s", strerror(errno));
    goto done;
  }
  if(fsync(tmp_fd) != 0) {
    set_err(err, err_size, "sync stream reverse temp: %s", strerror(errno));
    goto done;
  }
  src_fd = open(claim->source_path, O_RDWR);
  if(src_fd < 0) {
    set_err(err, err_size, "open source file for truncate: %s",
            strerror(errno));
    goto done;
  }
  uint64_t new_src_size =
      claim->original_size - claim->out_off - (uint64_t)claim->len;
  if(ftruncate(src_fd, (off_t)new_src_size) != 0 || fsync(src_fd) != 0) {
    set_err(err, err_size, "truncate source after reverse: %s",
            strerror(errno));
    goto done;
  }
  close(src_fd);
  src_fd = -1;
  close(tmp_fd);
  tmp_fd = -1;

  pthread_mutex_lock(&ctx->reverse_ahead_lock);
  f->reverse_pos += (uint64_t)claim->len;
  f->reverse_committing = 0;
  pthread_cond_broadcast(&ctx->reverse_ahead_cond);
  pthread_mutex_unlock(&ctx->reverse_ahead_lock);

  if(destructive_stream_write_file_record(ctx, claim->file_index,
                                          err, err_size) != 0 ||
     destructive_stream_sync(ctx, err, err_size) != 0) {
    goto done;
  }
  if(f->reverse_pos >= f->original_size) {
    if(destructive_stream_finalize_reversed_file(ctx, f, claim->file_index,
                                                 claim->tmp_path,
                                                 err, err_size) != 0) {
      goto done;
    }
  }
  rc = 0;

done:
  if(src_fd >= 0) close(src_fd);
  if(tmp_fd >= 0) close(tmp_fd);
  pthread_mutex_lock(&ctx->reverse_ahead_lock);
  f->reverse_committing = 0;
  pthread_cond_broadcast(&ctx->reverse_ahead_cond);
  pthread_mutex_unlock(&ctx->reverse_ahead_lock);
  return rc;
}

static void *
destructive_stream_reverse_ahead_main(void *arg) {
  destructive_stream_ctx_t *ctx = (destructive_stream_ctx_t *)arg;
  if(!ctx) return NULL;
  char local_err[256] = {0};
  size_t cap = 0;
  unsigned char *buf = destructive_stream_alloc_reverse_buffer(
      ctx, &cap, local_err, sizeof(local_err));
  if(!buf) {
    destructive_stream_reverse_ahead_finish_claim(
        ctx, 0, 0, errno ? errno : ENOMEM,
        local_err[0] ? local_err : "out of memory");
    goto finished;
  }
  for(;;) {
    reverse_ahead_claim_t claim;
    local_err[0] = '\0';
    if(!destructive_stream_reverse_ahead_claim(ctx, &claim, cap)) break;

    int src_fd = open(claim.source_path, O_RDONLY);
    if(src_fd < 0) {
      snprintf(local_err, sizeof(local_err), "open source file: %s",
               strerror(errno));
      int saved = errno ? errno : EIO;
      destructive_stream_reverse_ahead_finish_claim(
          ctx, claim.file_index, 0, saved, local_err);
      break;
    }
    struct stat src_meta;
    mode_t src_mode = 0666;
    if(fstat(src_fd, &src_meta) == 0) src_mode = src_meta.st_mode & 07777;

    int tmp_fd = open(claim.tmp_path, O_RDWR | O_CREAT, 0666);
    if(tmp_fd < 0) {
      int saved = errno ? errno : EIO;
      snprintf(local_err, sizeof(local_err), "open stream reverse temp: %s",
               strerror(saved));
      close(src_fd);
      destructive_stream_reverse_ahead_finish_claim(
          ctx, claim.file_index, 0, saved, local_err);
      break;
    }
    fchmod(tmp_fd, src_mode);

    uint64_t done = 0;
    int claim_failed = 0;
    while(done < (uint64_t)claim.len) {
      uint64_t left = (uint64_t)claim.len - done;
      size_t n = left > (uint64_t)cap ? cap : (size_t)left;
      uint64_t out_off = claim.out_off + done;
      uint64_t read_off = claim.original_size - out_off - (uint64_t)n;
      if(read_exact_at(src_fd, buf, n, (off_t)read_off) != 0) {
        int saved = errno ? errno : EIO;
        snprintf(local_err, sizeof(local_err), "read source for reverse: %s",
                 strerror(saved));
        close(src_fd);
        src_fd = -1;
        close(tmp_fd);
        tmp_fd = -1;
        destructive_stream_reverse_ahead_finish_claim(
            ctx, claim.file_index, 0, saved, local_err);
        claim_failed = 1;
        break;
      }
      reverse_bytes(buf, n);
      if(pwrite_all_local(tmp_fd, buf, n, (off_t)out_off) != 0) {
        int saved = errno ? errno : EIO;
        snprintf(local_err, sizeof(local_err), "write stream reverse temp: %s",
                 strerror(saved));
        close(src_fd);
        src_fd = -1;
        close(tmp_fd);
        tmp_fd = -1;
        destructive_stream_reverse_ahead_finish_claim(
            ctx, claim.file_index, 0, saved, local_err);
        claim_failed = 1;
        break;
      }
      done += (uint64_t)n;
    }
    if(src_fd >= 0) close(src_fd);
    if(tmp_fd >= 0) close(tmp_fd);
    if(claim_failed) break;

    if(done != (uint64_t)claim.len) {
      int saved = errno ? errno : EIO;
      destructive_stream_reverse_ahead_finish_claim(
          ctx, claim.file_index, 0, saved, local_err);
      break;
    }

    if(destructive_stream_commit_reverse_chunk(ctx, &claim, local_err,
                                               sizeof(local_err)) != 0) {
      int saved = errno ? errno : EIO;
      destructive_stream_reverse_ahead_finish_claim(
          ctx, claim.file_index, 0, saved,
          local_err[0] ? local_err : strerror(saved));
      break;
    }
    destructive_stream_reverse_ahead_finish_claim(ctx, claim.file_index, 1,
                                                  0, NULL);
  }
  free(buf);

finished:
  pthread_mutex_lock(&ctx->reverse_ahead_lock);
  if(ctx->reverse_ahead_active > 0) ctx->reverse_ahead_active--;
  if(ctx->reverse_ahead_active == 0) ctx->reverse_ahead_done = 1;
  pthread_cond_broadcast(&ctx->reverse_ahead_cond);
  pthread_mutex_unlock(&ctx->reverse_ahead_lock);
  return NULL;
}

static int
destructive_stream_reverse_ahead_worker_count(const destructive_stream_ctx_t *ctx) {
  if(!ctx || ctx->file_count == 0) return 0;
  for(size_t i = 0; i < ctx->file_count; i++) {
    const destructive_stream_file_t *f = &ctx->files[i];
    if(!f->deleted && !destructive_stream_file_reversed(f) &&
       !destructive_stream_file_passthrough_delete(f)) {
      return PFS_STREAM_REVERSE_WORKERS;
    }
  }
  return 0;
}

static int
destructive_stream_reverse_ahead_start(destructive_stream_ctx_t *ctx,
                                       char *err, size_t err_size) {
  if(!ctx || ctx->reverse_ahead_started || ctx->file_count == 0) return 0;
  if(!ctx->reverse_ahead_sync_initialized) {
    int rc = pthread_mutex_init(&ctx->reverse_ahead_lock, NULL);
    if(rc != 0) {
      set_err(err, err_size, "init reverse-ahead lock: %s", strerror(rc));
      errno = rc;
      return -1;
    }
    rc = pthread_cond_init(&ctx->reverse_ahead_cond, NULL);
    if(rc != 0) {
      pthread_mutex_destroy(&ctx->reverse_ahead_lock);
      set_err(err, err_size, "init reverse-ahead condition: %s", strerror(rc));
      errno = rc;
      return -1;
    }
    ctx->reverse_ahead_sync_initialized = 1;
  }
  ctx->reverse_ahead_stop = 0;
  ctx->reverse_ahead_done = 0;
  ctx->reverse_ahead_error = 0;
  ctx->reverse_ahead_err[0] = '\0';
  ctx->reverse_ahead_next_file = 0;
  ctx->reverse_ahead_thread_count = 0;
  ctx->reverse_ahead_active = 0;
  ctx->reverse_ahead_inflight = 0;
  if(destructive_stream_prepare_reverse_ahead(ctx, err, err_size) != 0) {
    return -1;
  }
  ctx->reverse_ahead_started = 1;
  int target_workers = destructive_stream_reverse_ahead_worker_count(ctx);
  for(int i = 0; i < target_workers; i++) {
    pthread_mutex_lock(&ctx->reverse_ahead_lock);
    ctx->reverse_ahead_active++;
    pthread_mutex_unlock(&ctx->reverse_ahead_lock);
    int rc = pthread_create(&ctx->reverse_ahead_threads[i], NULL,
                            destructive_stream_reverse_ahead_main, ctx);
    if(rc != 0) {
      pthread_mutex_lock(&ctx->reverse_ahead_lock);
      ctx->reverse_ahead_active--;
      ctx->reverse_ahead_stop = 1;
      pthread_cond_broadcast(&ctx->reverse_ahead_cond);
      pthread_mutex_unlock(&ctx->reverse_ahead_lock);
      for(int j = 0; j < ctx->reverse_ahead_thread_count; j++) {
        pthread_join(ctx->reverse_ahead_threads[j], NULL);
      }
      ctx->reverse_ahead_thread_count = 0;
      ctx->reverse_ahead_active = 0;
      ctx->reverse_ahead_done = 1;
      ctx->reverse_ahead_started = 0;
      set_err(err, err_size, "start reverse-ahead worker: %s", strerror(rc));
      errno = rc;
      return -1;
    }
    ctx->reverse_ahead_thread_count++;
  }
  if(ctx->reverse_ahead_thread_count == 0) {
    ctx->reverse_ahead_done = 1;
    ctx->reverse_ahead_started = 0;
  }
  return 0;
}

static void
destructive_stream_reverse_ahead_stop(destructive_stream_ctx_t *ctx) {
  if(!ctx || !ctx->reverse_ahead_started) return;
  if(ctx->reverse_ahead_sync_initialized) {
    pthread_mutex_lock(&ctx->reverse_ahead_lock);
    ctx->reverse_ahead_stop = 1;
    pthread_cond_broadcast(&ctx->reverse_ahead_cond);
    pthread_mutex_unlock(&ctx->reverse_ahead_lock);
  }
  for(int i = 0; i < ctx->reverse_ahead_thread_count; i++) {
    pthread_join(ctx->reverse_ahead_threads[i], NULL);
  }
  ctx->reverse_ahead_thread_count = 0;
  ctx->reverse_ahead_active = 0;
  ctx->reverse_ahead_started = 0;
}

static int
destructive_stream_wait_reverse_pos(destructive_stream_ctx_t *ctx,
                                    destructive_stream_file_t *f,
                                    size_t file_index,
                                    uint64_t needed,
                                    char *err, size_t err_size) {
  if(!ctx || !f) {
    set_err(err, err_size, "bad stream reverse state");
    errno = EINVAL;
    return -1;
  }
  if(needed > f->original_size) needed = f->original_size;
  if(destructive_stream_file_reversed(f) || f->reverse_pos >= needed) return 0;
  if(!ctx->reverse_ahead_started) {
    return destructive_stream_reverse_file(ctx, f, file_index, err, err_size);
  }
  pthread_mutex_lock(&ctx->reverse_ahead_lock);
  if(!destructive_stream_file_reversed(f) && f->reverse_pos < needed) {
    ctx->reverse_ahead_demand_file = file_index;
    if(ctx->reverse_ahead_demand_pos < needed) {
      ctx->reverse_ahead_demand_pos = needed;
    }
    pthread_cond_broadcast(&ctx->reverse_ahead_cond);
  }
  while(!destructive_stream_file_reversed(f) && f->reverse_pos < needed &&
        !ctx->reverse_ahead_error && !ctx->reverse_ahead_done) {
    pthread_cond_wait(&ctx->reverse_ahead_cond, &ctx->reverse_ahead_lock);
  }
  int reverse_error = ctx->reverse_ahead_error;
  char reverse_err[256];
  snprintf(reverse_err, sizeof(reverse_err), "%s", ctx->reverse_ahead_err);
  int ready = destructive_stream_file_reversed(f) || f->reverse_pos >= needed;
  if(ready && ctx->reverse_ahead_demand_file == file_index &&
     f->reverse_pos >= ctx->reverse_ahead_demand_pos) {
    ctx->reverse_ahead_demand_file = SIZE_MAX;
    ctx->reverse_ahead_demand_pos = 0;
  }
  pthread_mutex_unlock(&ctx->reverse_ahead_lock);
  if(ready) return 0;
  if(reverse_error) {
    errno = reverse_error;
    set_err(err, err_size, "%s",
            reverse_err[0] ? reverse_err : strerror(reverse_error));
  } else {
    errno = EIO;
    set_err(err, err_size, "reverse-ahead finished before source range was ready");
  }
  return -1;
}

static int
destructive_stream_read_reversed(destructive_stream_ctx_t *ctx,
                                 virtual_reader_t *vr, const char *path,
                                 uint64_t file_offset,
                                 unsigned char *out, size_t size,
                                 char *err, size_t err_size) {
  size_t index = 0;
  destructive_stream_file_t *f = destructive_stream_find_file(ctx, path, &index);
  if(!f) {
    set_err(err, err_size, "source file is missing from stream journal");
    errno = EINVAL;
    return -1;
  }
  if(size == 0) return 0;
  if(file_offset < f->committed ||
     file_offset > f->original_size ||
     (uint64_t)size > f->original_size - file_offset) {
    set_err(err, err_size, "read source outside committed stream window");
    errno = EIO;
    return -1;
  }
  while(size > 0) {
    if(destructive_stream_file_reversed(f)) {
      uint64_t disk_start = f->original_size - file_offset - (uint64_t)size;
      ssize_t key = -((ssize_t)index * 3 + 3);
      if(virtual_reader_open_path(vr, f->abs, key, err, err_size) != 0) {
        return -1;
      }
      if(read_exact_at(vr->fd, out, size, (off_t)disk_start) != 0) {
        set_err(err, err_size, "read reversed source file: %s", strerror(errno));
        return -1;
      }
      reverse_bytes(out, size);
      return 0;
    }

    uint64_t reversed_bytes = f->reverse_pos;
    uint64_t source_end = f->original_size > reversed_bytes ?
                          f->original_size - reversed_bytes : 0;
    if(file_offset < source_end) {
      size_t n = size;
      uint64_t available = source_end - file_offset;
      if((uint64_t)n > available) n = (size_t)available;
      ssize_t key = -((ssize_t)index * 3 + 1);
      if(virtual_reader_open_path(vr, f->abs, key, err, err_size) != 0) {
        return -1;
      }
      if(virtual_reader_file_read(vr, key, source_end, file_offset,
                                  out, n) != 0) {
        uint64_t latest_reversed = f->reverse_pos;
        uint64_t latest_source_end = f->original_size > latest_reversed ?
                                     f->original_size - latest_reversed : 0;
        if((errno == EIO || errno == ENOENT) && file_offset >= latest_source_end) {
          virtual_reader_close_file(vr);
          continue;
        }
        set_err(err, err_size, "read source file: %s", strerror(errno));
        return -1;
      }
      out += n;
      file_offset += (uint64_t)n;
      size -= n;
      continue;
    }

    char tmp_path[1024];
    if(destructive_stream_reverse_tmp_path(ctx, index, tmp_path,
                                           sizeof(tmp_path)) != 0) {
      set_err(err, err_size, "stream reverse temp path too long");
      return -1;
    }
    size_t n = size;
    uint64_t disk_start = f->original_size - file_offset - (uint64_t)n;
    if(disk_start + (uint64_t)n > reversed_bytes) {
      uint64_t available = reversed_bytes > disk_start ?
                           reversed_bytes - disk_start : 0;
      if(available == 0) {
        uint64_t needed = disk_start + (uint64_t)n;
        if(destructive_stream_wait_reverse_pos(ctx, f, index, needed,
                                               err, err_size) != 0) {
          return -1;
        }
        continue;
      }
      n = (size_t)available;
    }
    if(access(tmp_path, F_OK) != 0) {
      if(errno == ENOENT &&
         destructive_stream_wait_reverse_pos(ctx, f, index,
                                             disk_start + (uint64_t)n,
                                             err, err_size) == 0) {
        continue;
      }
      set_err(err, err_size, "open stream reverse temp: %s", strerror(errno));
      return -1;
    }
    ssize_t key = -((ssize_t)index * 3 + 2);
    if(virtual_reader_open_path(vr, tmp_path, key, err, err_size) != 0) {
      return -1;
    }
    if(read_exact_at(vr->fd, out, n, (off_t)disk_start) != 0) {
      set_err(err, err_size, "read stream reverse temp: %s", strerror(errno));
      return -1;
    }
    reverse_bytes(out, n);
    out += n;
    file_offset += (uint64_t)n;
    size -= n;
  }
  return 0;
}

static uint64_t
destructive_stream_target_commit(const destructive_stream_file_t *f,
                                 uint64_t completed_bytes) {
  if(!f || completed_bytes <= f->virtual_offset) return 0;
  uint64_t rel = completed_bytes - f->virtual_offset;
  return rel > f->original_size ? f->original_size : rel;
}

static int
destructive_stream_truncate_committed(int fd, uint64_t file_start,
                                      destructive_stream_ctx_t *ctx,
                                      virtual_reader_t *vr,
                                      pfsc_output_buffer_t *outbuf,
                                      pfs_vhash_writer_t *vhash,
                                      uint64_t completed_blocks,
                                      uint64_t data_pos,
                                      int force,
                                      char *err, size_t err_size) {
  if(!ctx) return 0;
  if(job_cancelled()) {
    atomic_store(&g_job.cancel, 0);
  }
  if(completed_blocks > ctx->block_count) completed_blocks = ctx->block_count;
  uint64_t completed_bytes = completed_blocks * PFS_BLOCK_SIZE;
  if(completed_bytes > ctx->nested_size) completed_bytes = ctx->nested_size;

  int should_commit = force || completed_blocks == ctx->block_count;
  uint64_t journal_block_step = PFS_STREAM_TRUNCATE_GRANULARITY / PFS_BLOCK_SIZE;
  if(journal_block_step == 0) journal_block_step = 1;
  if(!should_commit && completed_blocks > ctx->journaled_blocks &&
     completed_blocks - ctx->journaled_blocks >= journal_block_step) {
    should_commit = 1;
  }
  for(size_t i = 0; !should_commit && i < ctx->file_count; i++) {
    destructive_stream_file_t *f = &ctx->files[i];
    int pass_through = destructive_stream_file_passthrough_delete(f);
    if(!destructive_stream_file_reversed(f) && !pass_through) continue;
    uint64_t target = destructive_stream_target_commit(f, completed_bytes);
    if(pass_through && target < f->original_size) continue;
    if(target > f->committed &&
       (target == f->original_size ||
        target - f->committed >= PFS_STREAM_TRUNCATE_GRANULARITY)) {
      should_commit = 1;
      break;
    }
  }
  if(!should_commit) return 0;

  if(pfsc_output_buffer_flush(fd, file_start, outbuf, err, err_size) != 0) {
    return -1;
  }
  if(fsync(fd) != 0) {
    set_err(err, err_size, "sync compressed payload: %s", strerror(errno));
    return -1;
  }
  if(vhash && vhash->fd >= 0 && fsync(vhash->fd) != 0) {
    set_err(err, err_size, "sync validation hash: %s", strerror(errno));
    return -1;
  }

  uint64_t prev_journaled = ctx->journaled_blocks;
  ctx->completed_blocks = completed_blocks;
  ctx->data_pos = data_pos;
  if(destructive_stream_write_offsets(
         ctx, prev_journaled + 1ULL,
         completed_blocks > prev_journaled ? completed_blocks - prev_journaled : 0,
         err, err_size) != 0) {
    return -1;
  }
  ctx->journaled_blocks = completed_blocks;

  for(size_t i = 0; i < ctx->file_count; i++) {
    destructive_stream_file_t *f = &ctx->files[i];
    int pass_through = destructive_stream_file_passthrough_delete(f);
    if(!destructive_stream_file_reversed(f) && !pass_through) continue;
    uint64_t target = destructive_stream_target_commit(f, completed_bytes);
    if(pass_through && target < f->original_size) continue;
    if(target > f->committed) {
      f->committed = target;
      if(destructive_stream_write_file_record(ctx, i, err, err_size) != 0) {
        return -1;
      }
    }
  }
  if(destructive_stream_write_header(ctx, err, err_size) != 0 ||
     destructive_stream_sync(ctx, err, err_size) != 0) {
    return -1;
  }

  if(vr) virtual_reader_close_file(vr);
  for(size_t i = 0; i < ctx->file_count; i++) {
    destructive_stream_file_t *f = &ctx->files[i];
    if(f->deleted || f->committed == 0) continue;
    if(!path_is_child_of_root(ctx->source_path, f->abs)) {
      set_err(err, err_size, "refusing to truncate source outside app folder");
      errno = EINVAL;
      return -1;
    }
    if(f->committed >= f->original_size) {
      if(unlink(f->abs) != 0 && errno != ENOENT) {
        set_err(err, err_size, "delete committed source file: %s",
                strerror(errno));
        return -1;
      }
      f->deleted = 1;
      try_remove_empty_parent_dirs(ctx->source_path, f->abs);
      if(destructive_stream_write_file_record(ctx, i, err, err_size) != 0) {
        return -1;
      }
    } else {
      uint64_t new_size = f->original_size - f->committed;
      if(destructive_stream_truncate_path(f->abs, new_size, err, err_size) != 0) {
        return -1;
      }
    }
  }
  if(destructive_stream_sync(ctx, err, err_size) != 0) return -1;
  return 0;
}

static uint64_t
monotonic_us(void) {
  struct timespec ts;
#if defined(CLOCK_MONOTONIC)
  clock_gettime(CLOCK_MONOTONIC, &ts);
#else
  clock_gettime(CLOCK_REALTIME, &ts);
#endif
  return (uint64_t)ts.tv_sec * 1000000ULL + (uint64_t)ts.tv_nsec / 1000ULL;
}

static void
job_add_wait_us(atomic_long *counter, uint64_t started_at) {
  uint64_t now = monotonic_us();
  if(now <= started_at) return;
  uint64_t delta = now - started_at;
  atomic_fetch_add(counter, delta > (uint64_t)LONG_MAX ? LONG_MAX : (long)delta);
}

static int
scan_push(scan_list_t *list, const char *abs, const char *rel, uint64_t size) {
  if(list->count == list->cap) {
    size_t next = list->cap ? list->cap * 2 : 128;
    scan_file_t *p = realloc(list->items, next * sizeof(*p));
    if(!p) return -1;
    list->items = p;
    list->cap = next;
  }
  scan_file_t *it = &list->items[list->count++];
  memset(it, 0, sizeof(*it));
  snprintf(it->abs, sizeof(it->abs), "%s", abs);
  snprintf(it->rel, sizeof(it->rel), "%s", rel);
  it->size = size;
  return 0;
}

static int
scan_collect(const char *root, const char *rel, scan_list_t *files,
             char *err, size_t err_size) {
  char dir_path[1024];
  if(rel && rel[0]) join_abs(dir_path, sizeof(dir_path), root, rel);
  else snprintf(dir_path, sizeof(dir_path), "%s", root);

  DIR *d = opendir(dir_path);
  if(!d) {
    set_err(err, err_size, "scan: %s", strerror(errno));
    return -1;
  }

  int rc = 0;
  struct dirent *ent;
  while((ent = readdir(d))) {
    char child_abs[1024];
    char child_rel[1024];
    struct stat st;

    if(job_cancelled()) {
      set_err(err, err_size, "cancelled");
      errno = EINTR;
      rc = -1;
      break;
    }
    if(!strcmp(ent->d_name, ".") || !strcmp(ent->d_name, "..")) continue;
    if(!path_segment_supported(ent->d_name)) {
      set_err(err, err_size, "unsupported path name: %s", ent->d_name);
      rc = -1;
      break;
    }
    if(join_abs(child_abs, sizeof(child_abs), dir_path, ent->d_name) != 0 ||
       join_rel(child_rel, sizeof(child_rel), rel ? rel : "", ent->d_name) != 0) {
      set_err(err, err_size, "path too long");
      rc = -1;
      break;
    }
    if(lstat(child_abs, &st) != 0) {
      set_err(err, err_size, "stat: %s", strerror(errno));
      rc = -1;
      break;
    }
    if(S_ISLNK(st.st_mode)) {
      set_err(err, err_size, "symlinks are not supported");
      rc = -1;
      break;
    }
    if(S_ISDIR(st.st_mode)) {
      if(scan_collect(root, child_rel, files, err, err_size) != 0) {
        rc = -1;
        break;
      }
    } else if(S_ISREG(st.st_mode)) {
      if(scan_push(files, child_abs, child_rel,
                   st.st_size > 0 ? (uint64_t)st.st_size : 0) != 0) {
        set_err(err, err_size, "out of memory");
        rc = -1;
        break;
      }
    } else {
      set_err(err, err_size, "unsupported filesystem node: %s", child_rel);
      rc = -1;
      break;
    }
  }

  closedir(d);
  return rc;
}

static int
scan_file_cmp(const void *a, const void *b) {
  const scan_file_t *fa = a;
  const scan_file_t *fb = b;
  return ascii_casecmp(fa->rel, fb->rel);
}

static int
stream_ext_matches(const char *rel, const char *ext) {
  size_t rel_len = strlen(rel ? rel : "");
  size_t ext_len = strlen(ext ? ext : "");
  if(ext_len == 0 || rel_len < ext_len) return 0;
  return !strcasecmp(rel + rel_len - ext_len, ext);
}

static uint32_t
stream_predicted_ratio_permille(const char *rel) {
  static const char *const high_entropy[] = {
    ".pkg", ".pak", ".ucas", ".utoc", ".mp4", ".m4v", ".bik", ".bk2",
    ".wem", ".opus", ".ogg", ".mp3", ".jpg", ".jpeg", ".png", ".zip",
    ".rar", ".7z", ".gz", ".zst", ".lz4", ".xz"
  };
  static const char *const text_like[] = {
    ".json", ".xml", ".txt", ".ini", ".cfg", ".lua", ".js", ".css",
    ".ush", ".usf", ".shader", ".hlsl", ".glsl", ".csv", ".tsv"
  };
  static const char *const executable_like[] = {
    ".bin", ".prx", ".sprx", ".self", ".elf", ".dll", ".so"
  };
  for(size_t i = 0; i < sizeof(high_entropy) / sizeof(high_entropy[0]); i++) {
    if(stream_ext_matches(rel, high_entropy[i])) return 950;
  }
  for(size_t i = 0; i < sizeof(text_like) / sizeof(text_like[0]); i++) {
    if(stream_ext_matches(rel, text_like[i])) return 500;
  }
  for(size_t i = 0; i < sizeof(executable_like) / sizeof(executable_like[0]); i++) {
    if(stream_ext_matches(rel, executable_like[i])) return 850;
  }
  return 700;
}

static uint64_t
stream_predicted_stored_bytes(const scan_file_t *f) {
  if(!f || f->size == 0) return 0;
  uint64_t ratio = stream_predicted_ratio_permille(f->rel);
  if(f->size > UINT64_MAX / ratio) return f->size;
  uint64_t predicted = (f->size * ratio + 999ULL) / 1000ULL;
  if(predicted > f->size) predicted = f->size;
  if(predicted == 0) predicted = 1;
  return predicted;
}

static int
stream_schedule_better_fit(const scan_file_t *items, size_t a, size_t b) {
  const scan_file_t *fa = &items[a];
  const scan_file_t *fb = &items[b];
  uint64_t gain_a = fa->size > fa->stream_predicted_stored ?
                    fa->size - fa->stream_predicted_stored : 0;
  uint64_t gain_b = fb->size > fb->stream_predicted_stored ?
                    fb->size - fb->stream_predicted_stored : 0;
  int useful_a = gain_a > 0;
  int useful_b = gain_b > 0;
  if(useful_a != useful_b) return useful_a;
  if(fa->size != fb->size) return fa->size > fb->size;
  if(gain_a != gain_b) return gain_a > gain_b;
  return ascii_casecmp(fa->rel, fb->rel) < 0;
}

static int
stream_schedule_better_oversize(const scan_file_t *items, size_t a, size_t b) {
  const scan_file_t *fa = &items[a];
  const scan_file_t *fb = &items[b];
  if(fa->size != fb->size) return fa->size > fb->size;
  return ascii_casecmp(fa->rel, fb->rel) < 0;
}

static uint64_t
stream_default_reserve_for_budget(uint64_t budget_bytes) {
  if(budget_bytes == 0) return 0;
  if(budget_bytes <= PFS_STREAM_DEFAULT_RESERVE_BYTES * 2ULL) {
    return budget_bytes / 8ULL;
  }
  return PFS_STREAM_DEFAULT_RESERVE_BYTES;
}

static void
stream_options_normalize(const pfs_stream_options_t *in,
                         pfs_stream_options_t *out) {
  memset(out, 0, sizeof(*out));
  out->budget_bytes = in && in->budget_bytes ?
      in->budget_bytes : PFS_STREAM_DEFAULT_BUDGET_BYTES;
  out->reserve_bytes = in && in->reserve_bytes ?
      in->reserve_bytes : stream_default_reserve_for_budget(out->budget_bytes);
  if(out->reserve_bytes >= out->budget_bytes) out->reserve_bytes = 0;
  out->order = in ? in->order : PFS_STREAM_ORDER_BUDGETED_GAIN;
  if(out->order != PFS_STREAM_ORDER_PATH &&
     out->order != PFS_STREAM_ORDER_BUDGETED_GAIN) {
    out->order = PFS_STREAM_ORDER_BUDGETED_GAIN;
  }
}

static int
stream_schedule_budgeted_gain(scan_list_t *scans,
                              const pfs_stream_options_t *opts,
                              uint64_t *forward_files,
                              uint64_t *reverse_files) {
  if(forward_files) *forward_files = 0;
  if(reverse_files) *reverse_files = 0;
  if(!scans || scans->count == 0) return 0;
  pfs_stream_options_t normalized;
  stream_options_normalize(opts, &normalized);
  if(normalized.order == PFS_STREAM_ORDER_PATH) {
    uint64_t credit = normalized.budget_bytes - normalized.reserve_bytes;
    for(size_t i = 0; i < scans->count; i++) {
      scan_file_t *f = &scans->items[i];
      f->stream_predicted_stored = stream_predicted_stored_bytes(f);
      f->stream_schedule_order = (uint32_t)i;
      f->stream_predicted_gain_permille = f->size > 0 ?
          (uint32_t)(((f->size - f->stream_predicted_stored) * 1000ULL) /
                     f->size) : 0;
      f->stream_reverse_required = f->stream_predicted_stored > credit;
      f->stream_passthrough_delete = !f->stream_reverse_required;
      if(f->stream_reverse_required) {
        if(reverse_files) (*reverse_files)++;
      } else if(forward_files) {
        (*forward_files)++;
      }
      uint64_t gain = f->size > f->stream_predicted_stored ?
                      f->size - f->stream_predicted_stored : 0;
      if(UINT64_MAX - credit < gain) credit = UINT64_MAX;
      else credit += gain;
    }
    return 0;
  }

  scan_file_t *ordered = calloc(scans->count, sizeof(*ordered));
  unsigned char *used = calloc(scans->count, 1);
  if(!ordered || !used) {
    free(ordered);
    free(used);
    return -1;
  }
  for(size_t i = 0; i < scans->count; i++) {
    scans->items[i].stream_predicted_stored =
        stream_predicted_stored_bytes(&scans->items[i]);
    scans->items[i].stream_predicted_gain_permille = scans->items[i].size > 0 ?
        (uint32_t)(((scans->items[i].size -
                     scans->items[i].stream_predicted_stored) * 1000ULL) /
                   scans->items[i].size) : 0;
  }

  uint64_t credit = normalized.budget_bytes - normalized.reserve_bytes;
  for(size_t out = 0; out < scans->count; out++) {
    size_t best = SIZE_MAX;
    for(size_t i = 0; i < scans->count; i++) {
      if(used[i]) continue;
      if(scans->items[i].stream_predicted_stored > credit) continue;
      if(best == SIZE_MAX ||
         stream_schedule_better_fit(scans->items, i, best)) {
        best = i;
      }
    }
    int reverse_required = 0;
    if(best == SIZE_MAX) {
      reverse_required = 1;
      for(size_t i = 0; i < scans->count; i++) {
        if(used[i]) continue;
        if(best == SIZE_MAX ||
           stream_schedule_better_oversize(scans->items, i, best)) {
          best = i;
        }
      }
    }
    if(best == SIZE_MAX) break;
    used[best] = 1;
    ordered[out] = scans->items[best];
    ordered[out].stream_schedule_order = (uint32_t)out;
    ordered[out].stream_reverse_required = reverse_required;
    ordered[out].stream_passthrough_delete = !reverse_required;
    if(reverse_required) {
      if(reverse_files) (*reverse_files)++;
    } else if(forward_files) {
      (*forward_files)++;
    }
    uint64_t gain = ordered[out].size > ordered[out].stream_predicted_stored ?
                    ordered[out].size - ordered[out].stream_predicted_stored : 0;
    if(UINT64_MAX - credit < gain) credit = UINT64_MAX;
    else credit += gain;
  }
  memcpy(scans->items, ordered, scans->count * sizeof(*ordered));
  free(ordered);
  free(used);
  return 0;
}

static int
int_list_push(int_list_t *list, int value) {
  if(list->count == list->cap) {
    size_t next = list->cap ? list->cap * 2 : 8;
    int *p = realloc(list->items, next * sizeof(*p));
    if(!p) return -1;
    list->items = p;
    list->cap = next;
  }
  list->items[list->count++] = value;
  return 0;
}

static int
layout_add_dir(pfs_layout_t *l, const char *rel, const char *name,
               int parent) {
  if(l->dir_count == l->dir_cap) {
    size_t next = l->dir_cap ? l->dir_cap * 2 : 64;
    pfs_dir_node_t *p = realloc(l->dirs, next * sizeof(*p));
    if(!p) return -1;
    l->dirs = p;
    l->dir_cap = next;
  }
  int idx = (int)l->dir_count++;
  memset(&l->dirs[idx], 0, sizeof(l->dirs[idx]));
  snprintf(l->dirs[idx].rel, sizeof(l->dirs[idx].rel), "%s", rel ? rel : "");
  snprintf(l->dirs[idx].name, sizeof(l->dirs[idx].name), "%s", name ? name : "");
  l->dirs[idx].parent = parent;
  return idx;
}

static int
layout_find_dir(const pfs_layout_t *l, const char *rel) {
  for(size_t i = 0; i < l->dir_count; i++) {
    if(!strcmp(l->dirs[i].rel, rel ? rel : "")) return (int)i;
  }
  return -1;
}

static int
layout_add_file(pfs_layout_t *l, const scan_file_t *src, int parent) {
  if(l->file_count == l->file_cap) {
    size_t next = l->file_cap ? l->file_cap * 2 : 128;
    pfs_file_node_t *p = realloc(l->files, next * sizeof(*p));
    if(!p) return -1;
    l->files = p;
    l->file_cap = next;
  }
  int idx = (int)l->file_count++;
  memset(&l->files[idx], 0, sizeof(l->files[idx]));
  snprintf(l->files[idx].rel, sizeof(l->files[idx].rel), "%s", src->rel);
  snprintf(l->files[idx].abs, sizeof(l->files[idx].abs), "%s", src->abs);
  const char *base = strrchr(src->rel, '/');
  snprintf(l->files[idx].name, sizeof(l->files[idx].name), "%s",
           base ? base + 1 : src->rel);
  l->files[idx].parent = parent;
  l->files[idx].raw_size = src->size;
  l->files[idx].stream_predicted_stored = src->stream_predicted_stored;
  l->files[idx].stream_schedule_order = src->stream_schedule_order;
  l->files[idx].stream_predicted_gain_permille =
      src->stream_predicted_gain_permille;
  l->files[idx].stream_reverse_required = src->stream_reverse_required;
  l->files[idx].stream_passthrough_delete = src->stream_passthrough_delete;
  return idx;
}

static int
ensure_parent_dirs(pfs_layout_t *l, const char *file_rel, int *parent_out) {
  char path[1024] = {0};
  char rel[1024];
  snprintf(rel, sizeof(rel), "%s", file_rel);
  char *slash = strrchr(rel, '/');
  if(!slash) {
    *parent_out = 0;
    return 0;
  }
  *slash = 0;

  int parent = 0;
  char *seg = rel;
  while(seg && *seg) {
    char *next_slash = strchr(seg, '/');
    if(next_slash) *next_slash = 0;
    char next_rel[1024];
    if(join_rel(next_rel, sizeof(next_rel), path, seg) != 0) return -1;
    int idx = layout_find_dir(l, next_rel);
    if(idx < 0) {
      idx = layout_add_dir(l, next_rel, seg, parent);
      if(idx < 0 || int_list_push(&l->dirs[parent].child_dirs, idx) != 0) {
        return -1;
      }
    }
    snprintf(path, sizeof(path), "%s", next_rel);
    parent = idx;
    if(!next_slash) break;
    seg = next_slash + 1;
  }
  *parent_out = parent;
  return 0;
}

static int
layout_child_dir_cmp(const pfs_layout_t *l, int a, int b) {
  const char *ra = (a >= 0 && (size_t)a < l->dir_count) ? l->dirs[a].rel : "";
  const char *rb = (b >= 0 && (size_t)b < l->dir_count) ? l->dirs[b].rel : "";
  return ascii_casecmp(ra, rb);
}

static int
layout_child_file_cmp(const pfs_layout_t *l, int a, int b) {
  const char *ra = (a >= 0 && (size_t)a < l->file_count) ? l->files[a].rel : "";
  const char *rb = (b >= 0 && (size_t)b < l->file_count) ? l->files[b].rel : "";
  return ascii_casecmp(ra, rb);
}

static void
layout_sort_child_lists(pfs_layout_t *l) {
  if(!l) return;
  for(size_t d = 0; d < l->dir_count; d++) {
    int_list_t *dirs = &l->dirs[d].child_dirs;
    for(size_t i = 1; i < dirs->count; i++) {
      int value = dirs->items[i];
      size_t j = i;
      while(j > 0 && layout_child_dir_cmp(l, value, dirs->items[j - 1]) < 0) {
        dirs->items[j] = dirs->items[j - 1];
        j--;
      }
      dirs->items[j] = value;
    }
    int_list_t *files = &l->dirs[d].child_files;
    for(size_t i = 1; i < files->count; i++) {
      int value = files->items[i];
      size_t j = i;
      while(j > 0 && layout_child_file_cmp(l, value, files->items[j - 1]) < 0) {
        files->items[j] = files->items[j - 1];
        j--;
      }
      files->items[j] = value;
    }
  }
}

static uint32_t
pfs_hash_path(const char *path) {
  uint32_t h = 0;
  for(const unsigned char *p = (const unsigned char *)path; *p; p++) {
    h = (uint32_t)(ascii_toupper(*p) + (31U * h));
  }
  return h;
}

static int
append_dirent(byte_buf_t *b, uint32_t inode, int type, const char *name) {
  size_t name_len = strlen(name);
  size_t ent_size = name_len + 17;
  size_t rem = ent_size % 8;
  if(rem) ent_size += 8 - rem;
  unsigned char hdr[16];
  le32(hdr + 0, inode);
  le32(hdr + 4, (uint32_t)type);
  le32(hdr + 8, (uint32_t)name_len);
  le32(hdr + 12, (uint32_t)ent_size);
  if(buf_append(b, hdr, sizeof(hdr)) != 0 ||
     buf_append(b, name, name_len) != 0) {
    return -1;
  }
  if(ent_size > 16 + name_len) {
    return buf_append_zero(b, ent_size - 16 - name_len);
  }
  return 0;
}

static int
build_dir_blob(pfs_layout_t *l, int dir_index) {
  pfs_dir_node_t *d = &l->dirs[dir_index];
  byte_buf_t b = {0};
  int parent_inode = dir_index == 0 ? d->inode : l->dirs[d->parent].inode;

  if(append_dirent(&b, (uint32_t)d->inode, PFS_DIRENT_TYPE_DOT, ".") != 0 ||
     append_dirent(&b, (uint32_t)parent_inode, PFS_DIRENT_TYPE_DOTDOT, "..") != 0) {
    free(b.data);
    return -1;
  }
  for(size_t i = 0; i < d->child_dirs.count; i++) {
    pfs_dir_node_t *child = &l->dirs[d->child_dirs.items[i]];
    if(append_dirent(&b, (uint32_t)child->inode, PFS_DIRENT_TYPE_DIRECTORY,
                     child->name) != 0) {
      free(b.data);
      return -1;
    }
  }
  for(size_t i = 0; i < d->child_files.count; i++) {
    pfs_file_node_t *child = &l->files[d->child_files.items[i]];
    if(append_dirent(&b, (uint32_t)child->inode, PFS_DIRENT_TYPE_FILE,
                     child->name) != 0) {
      free(b.data);
      return -1;
    }
  }
  d->blob = b.data;
  d->blob_size = b.len;
  return 0;
}

static int
fpt_entry_cmp(const void *a, const void *b) {
  const fpt_entry_t *ea = a;
  const fpt_entry_t *eb = b;
  if(ea->hash < eb->hash) return -1;
  if(ea->hash > eb->hash) return 1;
  if(ea->seq < eb->seq) return -1;
  if(ea->seq > eb->seq) return 1;
  return 0;
}

static int
uint32_cmp(const void *a, const void *b) {
  uint32_t aa = *(const uint32_t *)a;
  uint32_t bb = *(const uint32_t *)b;
  if(aa < bb) return -1;
  if(aa > bb) return 1;
  return 0;
}

static int
layout_detect_fpt_collision(const pfs_layout_t *l, char *err, size_t err_size) {
  size_t count = (l->dir_count > 0 ? l->dir_count - 1 : 0) + l->file_count;
  uint32_t *hashes = calloc(count ? count : 1, sizeof(*hashes));
  if(!hashes) {
    set_err(err, err_size, "out of memory");
    return -1;
  }

  size_t pos = 0;
  for(size_t i = 1; i < l->dir_count; i++) {
    char full[1024];
    int n = snprintf(full, sizeof(full), "/%s", l->dirs[i].rel);
    if(n < 0 || (size_t)n >= sizeof(full)) {
      free(hashes);
      set_err(err, err_size, "path too long");
      return -1;
    }
    hashes[pos++] = pfs_hash_path(full);
  }
  for(size_t i = 0; i < l->file_count; i++) {
    char full[1024];
    int n = snprintf(full, sizeof(full), "/%s", l->files[i].rel);
    if(n < 0 || (size_t)n >= sizeof(full)) {
      free(hashes);
      set_err(err, err_size, "path too long");
      return -1;
    }
    hashes[pos++] = pfs_hash_path(full);
  }

  qsort(hashes, count, sizeof(*hashes), uint32_cmp);
  for(size_t i = 1; i < count; i++) {
    if(hashes[i - 1] == hashes[i]) {
      free(hashes);
      return 1;
    }
  }
  free(hashes);
  return 0;
}

static int
fpt_entry_full_path(const pfs_layout_t *l, const fpt_entry_t *entry,
                    char *out, size_t out_size) {
  const char *rel = entry->is_dir ? l->dirs[entry->index].rel
                                  : l->files[entry->index].rel;
  int n = snprintf(out, out_size, "/%s", rel);
  if(n < 0 || (size_t)n >= out_size) {
    errno = ENAMETOOLONG;
    return -1;
  }
  return 0;
}

static int
build_fpt_blob(pfs_layout_t *l, char *err, size_t err_size) {
  size_t count = (l->dir_count > 0 ? l->dir_count - 1 : 0) + l->file_count;
  fpt_entry_t *entries = calloc(count ? count : 1, sizeof(*entries));
  if(!entries) {
    set_err(err, err_size, "out of memory");
    return -1;
  }

  size_t pos = 0;
  for(size_t i = 1; i < l->dir_count; i++) {
    char full[1024];
    int n = snprintf(full, sizeof(full), "/%s", l->dirs[i].rel);
    if(n < 0 || (size_t)n >= sizeof(full)) {
      free(entries);
      set_err(err, err_size, "path too long");
      return -1;
    }
    entries[pos].hash = pfs_hash_path(full);
    entries[pos].value = (uint32_t)l->dirs[i].inode | 0x20000000U;
    entries[pos].seq = (uint32_t)pos;
    entries[pos].index = (uint32_t)i;
    entries[pos].is_dir = 1;
    pos++;
  }
  for(size_t i = 0; i < l->file_count; i++) {
    char full[1024];
    int n = snprintf(full, sizeof(full), "/%s", l->files[i].rel);
    if(n < 0 || (size_t)n >= sizeof(full)) {
      free(entries);
      set_err(err, err_size, "path too long");
      return -1;
    }
    entries[pos].hash = pfs_hash_path(full);
    entries[pos].value = (uint32_t)l->files[i].inode;
    entries[pos].seq = (uint32_t)pos;
    entries[pos].index = (uint32_t)i;
    entries[pos].is_dir = 0;
    pos++;
  }
  qsort(entries, count, sizeof(*entries), fpt_entry_cmp);

  size_t unique_count = 0;
  for(size_t i = 0; i < count;) {
    size_t group_end = i + 1;
    while(group_end < count && entries[group_end].hash == entries[i].hash) {
      group_end++;
    }
    if(group_end - i > 1) {
      byte_buf_t collision = {0};
      if(l->collision_blob) {
        collision.data = l->collision_blob;
        collision.len = l->collision_blob_size;
        collision.cap = l->collision_blob_size;
        l->collision_blob = NULL;
        l->collision_blob_size = 0;
      }
      if(collision.len > 0x7fffffffU) {
        free(collision.data);
        free(entries);
        set_err(err, err_size, "flat path table collision resolver is too large");
        return -1;
      }
      uint32_t offset = (uint32_t)collision.len;
      for(size_t j = i; j < group_end; j++) {
        char full[1024];
        uint32_t inode = entries[j].is_dir
          ? (uint32_t)l->dirs[entries[j].index].inode
          : (uint32_t)l->files[entries[j].index].inode;
        int type = entries[j].is_dir
          ? PFS_DIRENT_TYPE_DIRECTORY
          : PFS_DIRENT_TYPE_FILE;
        if(fpt_entry_full_path(l, &entries[j], full, sizeof(full)) != 0 ||
           append_dirent(&collision, inode, type, full) != 0) {
          free(collision.data);
          free(entries);
          set_err(err, err_size, "out of memory");
          return -1;
        }
      }
      if(buf_append_zero(&collision, 0x18) != 0) {
        free(collision.data);
        free(entries);
        set_err(err, err_size, "out of memory");
        return -1;
      }
      l->collision_blob = collision.data;
      l->collision_blob_size = collision.len;
      entries[i].value = 0x80000000U | offset;
    }
    unique_count++;
    i = group_end;
  }

  l->fpt_blob_size = unique_count * 8;
  l->fpt_blob = calloc(1, l->fpt_blob_size ? l->fpt_blob_size : 1);
  if(!l->fpt_blob) {
    free(entries);
    set_err(err, err_size, "out of memory");
    return -1;
  }
  size_t out = 0;
  for(size_t i = 0; i < count;) {
    size_t group_end = i + 1;
    while(group_end < count && entries[group_end].hash == entries[i].hash) {
      group_end++;
    }
    le32(l->fpt_blob + out * 8, entries[i].hash);
    le32(l->fpt_blob + out * 8 + 4, entries[i].value);
    out++;
    i = group_end;
  }
  free(entries);
  return 0;
}

static int
build_superroot_blob(pfs_layout_t *l) {
  byte_buf_t b = {0};
  if(append_dirent(&b, 1, PFS_DIRENT_TYPE_FILE, "flat_path_table") != 0) {
    free(b.data);
    return -1;
  }
  if(l->has_fpt_collision &&
     append_dirent(&b, 2, PFS_DIRENT_TYPE_FILE, "collision_resolver") != 0) {
    free(b.data);
    return -1;
  }
  if(append_dirent(&b, (uint32_t)l->dirs[0].inode,
                   PFS_DIRENT_TYPE_DIRECTORY, "uroot") != 0) {
    free(b.data);
    return -1;
  }
  l->superroot_blob = b.data;
  l->superroot_blob_size = b.len;
  return 0;
}

static void
write_inode(unsigned char *out, uint16_t mode, uint16_t nlink, uint32_t flags,
            uint64_t size, uint64_t size_comp, uint32_t blocks,
            int32_t db0, int fill_rest_minus_one, time_t now) {
  memset(out, 0, PFS_INODE_SIZE);
  le16(out + 0x00, mode);
  le16(out + 0x02, nlink);
  le32(out + 0x04, flags);
  le64(out + 0x08, size);
  le64(out + 0x10, size_comp);
  le64(out + 0x18, (uint64_t)now);
  le64(out + 0x20, (uint64_t)now);
  le64(out + 0x28, (uint64_t)now);
  le64(out + 0x30, (uint64_t)now);
  le32(out + 0x60, blocks);
  le32(out + 0x64, (uint32_t)db0);
  if(fill_rest_minus_one) {
    for(int i = 1; i < 12; i++) le32(out + 0x64 + i * 4, 0xffffffffU);
    for(int i = 0; i < 5; i++) le32(out + 0x94 + i * 4, 0xffffffffU);
  }
}

static void
write_header_blob(unsigned char *hdr, uint64_t inode_count,
                  uint64_t inode_block_count, uint64_t final_ndblock,
                  time_t now) {
  memset(hdr, 0, PFS_BLOCK_SIZE);
  le64(hdr + 0x00, PFS_VERSION_PS5);
  le64(hdr + 0x08, PFS_MAGIC);
  le64(hdr + 0x10, 0);
  hdr[0x18] = 0;
  hdr[0x19] = 0;
  hdr[0x1a] = 1;
  hdr[0x1b] = 0;
  le16(hdr + 0x1c, PFS_MODE_CASE_INSENSITIVE);
  le16(hdr + 0x1e, 0);
  le32(hdr + 0x20, (uint32_t)PFS_BLOCK_SIZE);
  le32(hdr + 0x24, 0);
  le64(hdr + 0x28, 1);
  le64(hdr + 0x30, inode_count);
  le64(hdr + 0x38, final_ndblock);
  le64(hdr + 0x40, inode_block_count);

  unsigned char *sig = hdr + 0x50;
  uint64_t inode_bytes = inode_block_count * PFS_BLOCK_SIZE;
  le16(sig + 0x00, 0);
  le16(sig + 0x02, 1);
  le32(sig + 0x04, PFS_INODE_FLAG_READONLY);
  le64(sig + 0x08, inode_bytes);
  le64(sig + 0x10, inode_bytes);
  le64(sig + 0x18, (uint64_t)now);
  le64(sig + 0x20, (uint64_t)now);
  le64(sig + 0x28, (uint64_t)now);
  le64(sig + 0x30, (uint64_t)now);
  le32(sig + 0x60, (uint32_t)inode_block_count);
  le64(sig + 0x68 + 32, 1);
  le32(hdr + 0x368, 1);
}

static unsigned char *
inode_slot(pfs_layout_t *l, int inode) {
  uint64_t per_block = PFS_BLOCK_SIZE / PFS_INODE_SIZE;
  uint64_t block = (uint64_t)inode / per_block;
  uint64_t index = (uint64_t)inode % per_block;
  return l->inode_blob + block * PFS_BLOCK_SIZE + index * PFS_INODE_SIZE;
}

static int
layout_add_segment(pfs_layout_t *l, uint64_t offset, uint64_t size,
                   pfs_segment_type_t type, const unsigned char *mem,
                   const char *path) {
  if(size == 0) return 0;
  if(l->segment_count == l->segment_cap) {
    size_t next = l->segment_cap ? l->segment_cap * 2 : 128;
    pfs_segment_t *p = realloc(l->segments, next * sizeof(*p));
    if(!p) return -1;
    l->segments = p;
    l->segment_cap = next;
  }
  pfs_segment_t *s = &l->segments[l->segment_count++];
  memset(s, 0, sizeof(*s));
  s->offset = offset;
  s->size = size;
  s->type = type;
  s->mem = mem;
  if(path) snprintf(s->path, sizeof(s->path), "%s", path);
  return 0;
}

static int
build_pfs_layout_from_scans(const scan_list_t *scans, pfs_layout_t *l,
                            char *err, size_t err_size) {
  int rc = -1;
  time_t now = time(NULL);

  if(!scans || scans->count == 0) {
    set_err(err, err_size, "app folder contains no files");
    return -1;
  }

  if(layout_add_dir(l, "", "uroot", -1) != 0) {
    set_err(err, err_size, "out of memory");
    goto done;
  }

  for(size_t i = 0; i < scans->count; i++) {
    int parent = 0;
    if(ensure_parent_dirs(l, scans->items[i].rel, &parent) != 0) {
      set_err(err, err_size, "path too long");
      goto done;
    }
    int file_idx = layout_add_file(l, &scans->items[i], parent);
    if(file_idx < 0 || int_list_push(&l->dirs[parent].child_files,
                                     file_idx) != 0) {
      set_err(err, err_size, "out of memory");
      goto done;
    }
  }
  layout_sort_child_lists(l);

  int collision = layout_detect_fpt_collision(l, err, err_size);
  if(collision < 0) goto done;
  l->has_fpt_collision = collision;

  l->dirs[0].inode = l->has_fpt_collision ? 3 : 2;
  int next_inode = l->has_fpt_collision ? 4 : 3;
  for(size_t i = 1; i < l->dir_count; i++) l->dirs[i].inode = next_inode++;
  for(size_t i = 0; i < l->file_count; i++) l->files[i].inode = next_inode++;
  l->inode_count = (uint64_t)next_inode;

  for(size_t i = 0; i < l->dir_count; i++) {
    l->dirs[i].nlink = i == 0 ? 3 : 2;
    for(size_t j = 0; j < l->dirs[i].child_dirs.count; j++) {
      l->dirs[i].nlink++;
    }
    if(build_dir_blob(l, (int)i) != 0) {
      set_err(err, err_size, "out of memory");
      goto done;
    }
  }
  if(build_fpt_blob(l, err, err_size) != 0) goto done;
  if(build_superroot_blob(l) != 0) {
    set_err(err, err_size, "out of memory");
    goto done;
  }

  uint64_t inodes_per_block = PFS_BLOCK_SIZE / PFS_INODE_SIZE;
  l->inode_block_count = ceil_div_u64(l->inode_count, inodes_per_block);
  l->inode_blob_size = (size_t)(l->inode_block_count * PFS_BLOCK_SIZE);
  l->inode_blob = calloc(1, l->inode_blob_size);
  l->header_blob = calloc(1, PFS_BLOCK_SIZE);
  if(!l->inode_blob || !l->header_blob) {
    set_err(err, err_size, "out of memory");
    goto done;
  }

  uint64_t nd = 1 + l->inode_block_count;
  uint64_t superroot_block = nd++;
  uint64_t fpt_blocks = ceil_div_u64(l->fpt_blob_size, PFS_BLOCK_SIZE);
  if(fpt_blocks == 0) fpt_blocks = 1;
  uint64_t fpt_block = nd;
  nd += fpt_blocks;
  uint64_t collision_blocks = 1;
  uint64_t collision_block = nd;
  if(l->has_fpt_collision) {
    collision_blocks = ceil_div_u64(l->collision_blob_size, PFS_BLOCK_SIZE);
    if(collision_blocks == 0) collision_blocks = 1;
  }
  nd += collision_blocks;

  for(size_t i = 0; i < l->dir_count; i++) {
    l->dirs[i].blocks = ceil_div_u64(l->dirs[i].blob_size, PFS_BLOCK_SIZE);
    if(l->dirs[i].blocks == 0) l->dirs[i].blocks = 1;
    l->dirs[i].block_start = nd;
    nd += l->dirs[i].blocks;
  }
  for(size_t i = 0; i < l->file_count; i++) {
    l->files[i].blocks = ceil_div_u64(l->files[i].raw_size, PFS_BLOCK_SIZE);
    if(l->files[i].blocks == 0) l->files[i].blocks = 1;
    l->files[i].block_start = nd;
    nd += l->files[i].blocks;
  }
  l->final_ndblock = nd;
  l->image_size = nd * PFS_BLOCK_SIZE;

  write_header_blob(l->header_blob, l->inode_count, l->inode_block_count,
                    l->final_ndblock, now);
  write_inode(inode_slot(l, 0), PFS_INODE_MODE_DIR | PFS_INODE_RWX_ALL, 1,
              PFS_INODE_FLAG_INTERNAL | PFS_INODE_FLAG_READONLY,
              PFS_BLOCK_SIZE, PFS_BLOCK_SIZE, 1, (int32_t)superroot_block, 0,
              now);
  write_inode(inode_slot(l, 1), PFS_INODE_MODE_FILE | PFS_INODE_RWX_ALL, 1,
              PFS_INODE_FLAG_INTERNAL | PFS_INODE_FLAG_READONLY,
              l->fpt_blob_size, l->fpt_blob_size, (uint32_t)fpt_blocks,
              (int32_t)fpt_block, 1, now);
  if(l->has_fpt_collision) {
    write_inode(inode_slot(l, 2), PFS_INODE_MODE_FILE | PFS_INODE_RWX_ALL, 1,
                PFS_INODE_FLAG_INTERNAL | PFS_INODE_FLAG_READONLY,
                l->collision_blob_size, l->collision_blob_size,
                (uint32_t)collision_blocks, (int32_t)collision_block, 1,
                now);
  }
  for(size_t i = 0; i < l->dir_count; i++) {
    pfs_dir_node_t *d = &l->dirs[i];
    write_inode(inode_slot(l, d->inode),
                PFS_INODE_MODE_DIR | PFS_INODE_RWX_ALL,
                (uint16_t)d->nlink, PFS_INODE_FLAG_READONLY,
                d->blocks * PFS_BLOCK_SIZE, d->blocks * PFS_BLOCK_SIZE,
                (uint32_t)d->blocks, (int32_t)d->block_start, 1, now);
  }
  for(size_t i = 0; i < l->file_count; i++) {
    pfs_file_node_t *f = &l->files[i];
    write_inode(inode_slot(l, f->inode),
                PFS_INODE_MODE_FILE | PFS_INODE_RWX_ALL, 1,
                PFS_INODE_FLAG_READONLY,
                f->raw_size, f->raw_size, (uint32_t)f->blocks,
                (int32_t)f->block_start, 1, now);
  }

  if(layout_add_segment(l, 0, PFS_BLOCK_SIZE, PFS_SEG_MEM,
                        l->header_blob, NULL) != 0 ||
     layout_add_segment(l, PFS_BLOCK_SIZE, l->inode_blob_size, PFS_SEG_MEM,
                        l->inode_blob, NULL) != 0 ||
     layout_add_segment(l, superroot_block * PFS_BLOCK_SIZE,
                        l->superroot_blob_size, PFS_SEG_MEM,
                        l->superroot_blob, NULL) != 0 ||
     layout_add_segment(l, fpt_block * PFS_BLOCK_SIZE,
                        l->fpt_blob_size, PFS_SEG_MEM,
                        l->fpt_blob, NULL) != 0) {
    set_err(err, err_size, "out of memory");
    goto done;
  }
  if(l->has_fpt_collision &&
     layout_add_segment(l, collision_block * PFS_BLOCK_SIZE,
                        l->collision_blob_size, PFS_SEG_MEM,
                        l->collision_blob, NULL) != 0) {
    set_err(err, err_size, "out of memory");
    goto done;
  }
  for(size_t i = 0; i < l->dir_count; i++) {
    if(layout_add_segment(l, l->dirs[i].block_start * PFS_BLOCK_SIZE,
                          l->dirs[i].blob_size, PFS_SEG_MEM,
                          l->dirs[i].blob, NULL) != 0) {
      set_err(err, err_size, "out of memory");
      goto done;
    }
  }
  for(size_t i = 0; i < l->file_count; i++) {
    if(layout_add_segment(l, l->files[i].block_start * PFS_BLOCK_SIZE,
                          l->files[i].raw_size, PFS_SEG_FILE,
                          NULL, l->files[i].abs) != 0) {
      set_err(err, err_size, "out of memory");
      goto done;
    }
  }

  rc = 0;

done:
  return rc;
}

static int
build_layout_from_files(const char *root, pfs_layout_t *l,
                        char *err, size_t err_size) {
  scan_list_t scans = {0};
  int rc = -1;

  job_set_current("Scanning app folder");
  if(scan_collect(root, "", &scans, err, err_size) != 0) goto done;
  if(scans.count == 0) {
    set_err(err, err_size, "app folder contains no files");
    goto done;
  }
  qsort(scans.items, scans.count, sizeof(scans.items[0]), scan_file_cmp);
  rc = build_pfs_layout_from_scans(&scans, l, err, err_size);

done:
  free(scans.items);
  return rc;
}

static int
build_layout_from_files_stream(const char *root, pfs_layout_t *l,
                               const pfs_stream_options_t *stream_opts,
                               pfs_app_info_t *info,
                               char *err, size_t err_size) {
  scan_list_t scans = {0};
  int rc = -1;
  uint64_t forward_files = 0;
  uint64_t reverse_files = 0;
  pfs_stream_options_t normalized;

  stream_options_normalize(stream_opts, &normalized);
  job_set_current("Scheduling budgeted stream");
  if(scan_collect(root, "", &scans, err, err_size) != 0) goto done;
  if(scans.count == 0) {
    set_err(err, err_size, "app folder contains no files");
    goto done;
  }
  qsort(scans.items, scans.count, sizeof(scans.items[0]), scan_file_cmp);
  if(stream_schedule_budgeted_gain(&scans, &normalized,
                                   &forward_files, &reverse_files) != 0) {
    set_err(err, err_size, "out of memory");
    goto done;
  }
  if(info) {
    info->stream_budget_bytes = normalized.budget_bytes;
    info->stream_reserve_bytes = normalized.reserve_bytes;
    info->stream_forward_files = forward_files;
    info->stream_reverse_files = reverse_files;
  }
  rc = build_pfs_layout_from_scans(&scans, l, err, err_size);

done:
  free(scans.items);
  return rc;
}

typedef struct exfat_allocation {
  uint32_t first_cluster;
  uint32_t cluster_count;
} exfat_allocation_t;

static uint16_t
exfat_upcase_char(uint16_t ch) {
  if(ch >= 'a' && ch <= 'z') return (uint16_t)(ch - 32);
  return ch;
}

static uint16_t
exfat_rotate16(uint16_t value) {
  return (uint16_t)(((value & 1U) ? 0x8000U : 0U) + (value >> 1));
}

static uint32_t
exfat_rotate32(uint32_t value) {
  return ((value & 1U) ? 0x80000000U : 0U) + (value >> 1);
}

static uint32_t
exfat_table_checksum(const unsigned char *data, size_t size) {
  uint32_t sum = 0;
  for(size_t i = 0; i < size; i++) sum = exfat_rotate32(sum) + data[i];
  return sum;
}

static uint32_t
exfat_boot_checksum(const unsigned char *boot, size_t sector_size) {
  uint32_t sum = 0;
  size_t limit = (size_t)EXFAT_BOOT_CHECKSUM_SECTOR * sector_size;
  for(size_t i = 0; i < limit; i++) {
    if(i == 106 || i == 107 || i == 112) continue;
    sum = exfat_rotate32(sum) + boot[i];
  }
  return sum;
}

static uint16_t
exfat_name_hash(const char *name) {
  uint16_t hash = 0;
  for(const unsigned char *p = (const unsigned char *)name; *p; p++) {
    uint16_t ch = exfat_upcase_char(*p);
    hash = (uint16_t)(exfat_rotate16(hash) + (ch & 0xff));
    hash = (uint16_t)(exfat_rotate16(hash) + ((ch >> 8) & 0xff));
  }
  return hash;
}

static uint16_t
exfat_entry_set_checksum(const unsigned char *entries, size_t entry_count) {
  uint16_t sum = 0;
  size_t size = entry_count * 32;
  for(size_t i = 0; i < size; i++) {
    if(i == 2 || i == 3) continue;
    sum = (uint16_t)(exfat_rotate16(sum) + entries[i]);
  }
  return sum;
}

static uint64_t
exfat_cluster_offset(uint32_t cluster_heap_offset, uint32_t cluster) {
  return ((uint64_t)cluster_heap_offset * EXFAT_SECTOR_SIZE) +
         ((uint64_t)cluster - 2ULL) * EXFAT_CLUSTER_SIZE;
}

static size_t
exfat_name_entry_count(const char *name) {
  size_t len = strlen(name);
  return len ? ceil_div_u64(len, 15) : 1;
}

static size_t
exfat_dir_entry_set_count(const char *name) {
  return 2 + exfat_name_entry_count(name);
}

static size_t
exfat_estimate_dir_entries(const pfs_layout_t *l, int dir_index) {
  const pfs_dir_node_t *d = &l->dirs[dir_index];
  size_t entries = dir_index == 0 ? 3 : 0; /* volume label + bitmap + upcase */
  for(size_t i = 0; i < d->child_dirs.count; i++) {
    entries += exfat_dir_entry_set_count(l->dirs[d->child_dirs.items[i]].name);
  }
  for(size_t i = 0; i < d->child_files.count; i++) {
    entries += exfat_dir_entry_set_count(l->files[d->child_files.items[i]].name);
  }
  return entries + 1; /* terminating unused entry */
}

static int
exfat_append_file_entry(byte_buf_t *b, const char *name, int is_dir,
                        uint32_t first_cluster, uint64_t data_len) {
  size_t name_len = strlen(name);
  size_t name_entries = exfat_name_entry_count(name);
  size_t entry_count = 2 + name_entries;
  unsigned char entries[32 * (2 + 17)];
  if(name_len == 0 || name_len > 255 || name_entries > 17) return -1;
  memset(entries, 0, sizeof(entries));

  entries[0] = 0x85;
  entries[1] = (unsigned char)(entry_count - 1);
  le16(entries + 4, (uint16_t)(is_dir ? 0x10 : 0x20));

  unsigned char *stream = entries + 32;
  stream[0] = 0xC0;
  stream[1] = first_cluster ? 0x03 : 0x00;
  stream[3] = (unsigned char)name_len;
  le16(stream + 4, exfat_name_hash(name));
  le64(stream + 8, data_len);
  le32(stream + 20, first_cluster);
  le64(stream + 24, data_len);

  size_t consumed = 0;
  for(size_t e = 0; e < name_entries; e++) {
    unsigned char *fn = entries + 32 * (2 + e);
    fn[0] = 0xC1;
    fn[1] = 0x00;
    for(size_t i = 0; i < 15; i++) {
      uint16_t ch = 0;
      if(consumed < name_len) ch = (unsigned char)name[consumed++];
      le16(fn + 2 + i * 2, ch);
    }
  }

  uint16_t checksum = exfat_entry_set_checksum(entries, entry_count);
  le16(entries + 2, checksum);
  return buf_append(b, entries, entry_count * 32);
}

static int
exfat_build_dir_blob(pfs_layout_t *l, int dir_index,
                     const exfat_allocation_t *dir_allocs,
                     const exfat_allocation_t *file_allocs,
                     uint32_t bitmap_cluster, uint64_t bitmap_size,
                     uint32_t upcase_cluster, uint64_t upcase_size,
                     uint32_t upcase_checksum,
                     char *err, size_t err_size) {
  pfs_dir_node_t *d = &l->dirs[dir_index];
  byte_buf_t b = {0};

  if(dir_index == 0) {
    unsigned char volume_label[32] = {0};
    volume_label[0] = 0x83;

    unsigned char bitmap[32] = {0};
    bitmap[0] = 0x81;
    le32(bitmap + 20, bitmap_cluster);
    le64(bitmap + 24, bitmap_size);

    unsigned char upcase[32] = {0};
    upcase[0] = 0x82;
    le32(upcase + 4, upcase_checksum);
    le32(upcase + 20, upcase_cluster);
    le64(upcase + 24, upcase_size);
    if(buf_append(&b, volume_label, sizeof(volume_label)) != 0 ||
       buf_append(&b, bitmap, sizeof(bitmap)) != 0 ||
       buf_append(&b, upcase, sizeof(upcase)) != 0) {
      set_err(err, err_size, "out of memory");
      free(b.data);
      return -1;
    }
  }

  for(size_t i = 0; i < d->child_dirs.count; i++) {
    int child_idx = d->child_dirs.items[i];
    const pfs_dir_node_t *child = &l->dirs[child_idx];
    uint64_t data_len = (uint64_t)dir_allocs[child_idx].cluster_count *
                        EXFAT_CLUSTER_SIZE;
    if(exfat_append_file_entry(&b, child->name, 1,
                               dir_allocs[child_idx].first_cluster,
                               data_len) != 0) {
      set_err(err, err_size, "out of memory");
      free(b.data);
      return -1;
    }
  }
  for(size_t i = 0; i < d->child_files.count; i++) {
    int child_idx = d->child_files.items[i];
    const pfs_file_node_t *child = &l->files[child_idx];
    if(exfat_append_file_entry(&b, child->name, 0,
                               file_allocs[child_idx].first_cluster,
                               child->raw_size) != 0) {
      set_err(err, err_size, "out of memory");
      free(b.data);
      return -1;
    }
  }
  if(buf_append_zero(&b, 32) != 0) {
    set_err(err, err_size, "out of memory");
    free(b.data);
    return -1;
  }

  uint64_t dir_bytes = (uint64_t)dir_allocs[dir_index].cluster_count *
                       EXFAT_CLUSTER_SIZE;
  if(b.len > dir_bytes || buf_append_zero(&b, (size_t)(dir_bytes - b.len)) != 0) {
    set_err(err, err_size, "out of memory");
    free(b.data);
    return -1;
  }
  d->blob = b.data;
  d->blob_size = b.len;
  return 0;
}

static void
exfat_mark_cluster_range(unsigned char *bitmap, uint32_t first,
                         uint32_t count) {
  if(first < 2 || count == 0) return;
  for(uint32_t c = first; c < first + count; c++) {
    uint32_t bit = c - 2;
    bitmap[bit / 8] |= (unsigned char)(1U << (bit % 8));
  }
}

static void
exfat_set_fat_range(unsigned char *fat, uint32_t first, uint32_t count) {
  if(first < 2 || count == 0) return;
  for(uint32_t i = 0; i < count; i++) {
    uint32_t cluster = first + i;
    uint32_t value = i + 1 < count ? cluster + 1 : EXFAT_END_OF_CHAIN;
    le32(fat + (size_t)cluster * 4, value);
  }
}

static int
exfat_make_upcase_blob(pfs_layout_t *l, uint32_t *checksum,
                       char *err, size_t err_size) {
  l->exfat_upcase_blob = malloc(sizeof(k_exfat_standard_upcase));
  if(!l->exfat_upcase_blob) {
    set_err(err, err_size, "out of memory");
    return -1;
  }
  memcpy(l->exfat_upcase_blob, k_exfat_standard_upcase,
         sizeof(k_exfat_standard_upcase));
  l->exfat_upcase_blob_size = sizeof(k_exfat_standard_upcase);
  *checksum = exfat_table_checksum(l->exfat_upcase_blob,
                                   l->exfat_upcase_blob_size);
  return 0;
}

static int
exfat_make_boot_blob(pfs_layout_t *l, uint64_t volume_sectors,
                     uint32_t fat_offset, uint32_t fat_length,
                     uint32_t cluster_heap_offset, uint32_t cluster_count,
                     uint32_t root_cluster, uint8_t percent_in_use,
                     char *err, size_t err_size) {
  size_t size = (size_t)EXFAT_BOOT_REGION_SECTORS * (size_t)EXFAT_SECTOR_SIZE;
  l->exfat_boot_blob = calloc(1, size);
  if(!l->exfat_boot_blob) {
    set_err(err, err_size, "out of memory");
    return -1;
  }
  l->exfat_boot_blob_size = size;

  for(int copy = 0; copy < 2; copy++) {
    unsigned char *base = l->exfat_boot_blob +
      copy * 12U * (size_t)EXFAT_SECTOR_SIZE;
    unsigned char *boot = base;
    boot[0] = 0xEB;
    boot[1] = 0x76;
    boot[2] = 0x90;
    memcpy(boot + 3, "EXFAT   ", 8);
    le64(boot + 0x40, 0);
    le64(boot + 0x48, volume_sectors);
    le32(boot + 0x50, fat_offset);
    le32(boot + 0x54, fat_length);
    le32(boot + 0x58, cluster_heap_offset);
    le32(boot + 0x5c, cluster_count);
    le32(boot + 0x60, root_cluster);
    le32(boot + 0x64, (uint32_t)time(NULL));
    le16(boot + 0x68, 0x0100);
    le16(boot + 0x6a, 0);
    boot[0x6c] = 9;  /* 512-byte sectors */
    boot[0x6d] = 7;  /* 64 KiB clusters */
    boot[0x6e] = 1;  /* one FAT */
    boot[0x6f] = 0x80;
    boot[0x70] = percent_in_use;
    boot[510] = 0x55;
    boot[511] = 0xAA;
    for(int s = 1; s <= 8; s++) {
      unsigned char *ext = base + (size_t)s * (size_t)EXFAT_SECTOR_SIZE;
      ext[510] = 0x55;
      ext[511] = 0xAA;
    }
    uint32_t checksum = exfat_boot_checksum(base, (size_t)EXFAT_SECTOR_SIZE);
    unsigned char *check = base + 11U * (size_t)EXFAT_SECTOR_SIZE;
    for(size_t off = 0; off < EXFAT_SECTOR_SIZE; off += 4) {
      le32(check + off, checksum);
    }
  }
  return 0;
}

static int
build_exfat_layout_from_scans(const scan_list_t *scans, const char *title_id,
                              pfs_layout_t *l,
                              char *err, size_t err_size) {
  exfat_allocation_t *dir_allocs = NULL;
  exfat_allocation_t *file_allocs = NULL;
  int rc = -1;
  uint32_t upcase_checksum = 0;

  if(!scans || scans->count == 0) {
    set_err(err, err_size, "app folder contains no files");
    return -1;
  }

  if(layout_add_dir(l, "", title_id ? title_id : "root", -1) != 0) {
    set_err(err, err_size, "out of memory");
    goto done;
  }
  for(size_t i = 0; i < scans->count; i++) {
    int parent = 0;
    if(ensure_parent_dirs(l, scans->items[i].rel, &parent) != 0) {
      set_err(err, err_size, "path too long");
      goto done;
    }
    int file_idx = layout_add_file(l, &scans->items[i], parent);
    if(file_idx < 0 ||
       int_list_push(&l->dirs[parent].child_files, file_idx) != 0) {
      set_err(err, err_size, "out of memory");
      goto done;
    }
  }
  layout_sort_child_lists(l);

  dir_allocs = calloc(l->dir_count ? l->dir_count : 1, sizeof(*dir_allocs));
  file_allocs = calloc(l->file_count ? l->file_count : 1, sizeof(*file_allocs));
  if(!dir_allocs || !file_allocs) {
    set_err(err, err_size, "out of memory");
    goto done;
  }
  if(exfat_make_upcase_blob(l, &upcase_checksum, err, err_size) != 0) goto done;

  uint64_t upcase_clusters = ceil_div_u64(l->exfat_upcase_blob_size,
                                          EXFAT_CLUSTER_SIZE);
  uint64_t dir_clusters_total = 0;
  for(size_t i = 0; i < l->dir_count; i++) {
    uint64_t entries = exfat_estimate_dir_entries(l, (int)i);
    uint64_t clusters = ceil_div_u64(entries * 32ULL, EXFAT_CLUSTER_SIZE);
    if(clusters == 0) clusters = 1;
    dir_allocs[i].cluster_count = (uint32_t)clusters;
    dir_clusters_total += clusters;
  }
  uint64_t file_clusters_total = 0;
  for(size_t i = 0; i < l->file_count; i++) {
    uint64_t clusters = ceil_div_u64(l->files[i].raw_size, EXFAT_CLUSTER_SIZE);
    if(clusters > UINT32_MAX) {
      set_err(err, err_size, "file is too large for generated exFAT image");
      errno = EFBIG;
      goto done;
    }
    file_allocs[i].cluster_count = (uint32_t)clusters;
    file_clusters_total += clusters;
  }

  uint64_t bitmap_clusters = 1;
  for(;;) {
    uint64_t cluster_count64 = bitmap_clusters + upcase_clusters +
      dir_clusters_total + file_clusters_total + EXFAT_ROOT_SLACK_CLUSTERS;
    uint64_t bitmap_bytes = ceil_div_u64(cluster_count64, 8);
    uint64_t needed = ceil_div_u64(bitmap_bytes, EXFAT_CLUSTER_SIZE);
    if(needed == bitmap_clusters) break;
    bitmap_clusters = needed;
  }
  uint64_t cluster_count64 = bitmap_clusters + upcase_clusters +
    dir_clusters_total + file_clusters_total + EXFAT_ROOT_SLACK_CLUSTERS;
  if(cluster_count64 == 0 || cluster_count64 > 0xffffff00ULL) {
    set_err(err, err_size, "generated exFAT image is too large");
    errno = EFBIG;
    goto done;
  }
  uint32_t cluster_count = (uint32_t)cluster_count64;
  uint64_t bitmap_size = ceil_div_u64(cluster_count64, 8);
  if(bitmap_size > SIZE_MAX ||
     l->exfat_upcase_blob_size > SIZE_MAX ||
     bitmap_clusters > UINT32_MAX ||
     upcase_clusters > UINT32_MAX) {
    set_err(err, err_size, "generated exFAT image is too large");
    errno = EFBIG;
    goto done;
  }

  l->exfat_bitmap_blob_size = (size_t)bitmap_clusters *
                              (size_t)EXFAT_CLUSTER_SIZE;
  l->exfat_bitmap_blob = calloc(1, l->exfat_bitmap_blob_size);
  if(!l->exfat_bitmap_blob) {
    set_err(err, err_size, "out of memory");
    goto done;
  }

  uint32_t next_cluster = 2;
  uint32_t bitmap_cluster = next_cluster;
  next_cluster += (uint32_t)bitmap_clusters;
  uint32_t upcase_cluster = next_cluster;
  next_cluster += (uint32_t)upcase_clusters;
  for(size_t i = 0; i < l->dir_count; i++) {
    dir_allocs[i].first_cluster = next_cluster;
    next_cluster += dir_allocs[i].cluster_count;
  }
  for(size_t i = 0; i < l->file_count; i++) {
    if(file_allocs[i].cluster_count > 0) {
      file_allocs[i].first_cluster = next_cluster;
      next_cluster += file_allocs[i].cluster_count;
    }
  }

  exfat_mark_cluster_range(l->exfat_bitmap_blob, bitmap_cluster,
                           (uint32_t)bitmap_clusters);
  exfat_mark_cluster_range(l->exfat_bitmap_blob, upcase_cluster,
                           (uint32_t)upcase_clusters);
  for(size_t i = 0; i < l->dir_count; i++) {
    exfat_mark_cluster_range(l->exfat_bitmap_blob, dir_allocs[i].first_cluster,
                             dir_allocs[i].cluster_count);
  }
  for(size_t i = 0; i < l->file_count; i++) {
    exfat_mark_cluster_range(l->exfat_bitmap_blob,
                             file_allocs[i].first_cluster,
                             file_allocs[i].cluster_count);
  }

  for(size_t i = 0; i < l->dir_count; i++) {
    if(exfat_build_dir_blob(l, (int)i, dir_allocs, file_allocs,
                            bitmap_cluster, bitmap_size, upcase_cluster,
                            l->exfat_upcase_blob_size, upcase_checksum,
                            err, err_size) != 0) {
      goto done;
    }
  }

  uint64_t fat_entries = (uint64_t)cluster_count + 2ULL;
  uint64_t fat_bytes = fat_entries * 4ULL;
  uint64_t fat_sectors = ceil_div_u64(fat_bytes, EXFAT_SECTOR_SIZE);
  uint64_t fat_sectors_aligned = ceil_div_u64(fat_sectors,
                                              EXFAT_SECTORS_PER_CLUSTER) *
                                 EXFAT_SECTORS_PER_CLUSTER;
  uint32_t fat_offset = (uint32_t)EXFAT_FAT_OFFSET_SECTORS;
  uint32_t fat_length = (uint32_t)fat_sectors_aligned;
  uint32_t cluster_heap_offset = fat_offset + fat_length;
  uint64_t data_end_sectors = (uint64_t)cluster_heap_offset +
                              (uint64_t)cluster_count *
                              EXFAT_SECTORS_PER_CLUSTER;
  /*
   * ShadowMountPlus/PS5 exfatfs rejects the PPSA14251 nested image when the
   * device ends exactly at the cluster heap. Keep one boot-region-sized tail
   * pad, matching the previously mountable generated images.
   */
  uint64_t volume_sectors = data_end_sectors + EXFAT_TRAILING_PAD_SECTORS;
  if(volume_sectors > UINT32_MAX) {
    set_err(err, err_size, "generated exFAT image is too large");
    errno = EFBIG;
    goto done;
  }

  l->exfat_fat_blob_size = (size_t)fat_sectors_aligned *
                           (size_t)EXFAT_SECTOR_SIZE;
  l->exfat_fat_blob = calloc(1, l->exfat_fat_blob_size);
  if(!l->exfat_fat_blob) {
    set_err(err, err_size, "out of memory");
    goto done;
  }
  le32(l->exfat_fat_blob + 0, 0xfffffff8U);
  le32(l->exfat_fat_blob + 4, EXFAT_END_OF_CHAIN);
  exfat_set_fat_range(l->exfat_fat_blob, bitmap_cluster,
                      (uint32_t)bitmap_clusters);
  exfat_set_fat_range(l->exfat_fat_blob, upcase_cluster,
                      (uint32_t)upcase_clusters);
  for(size_t i = 0; i < l->dir_count; i++) {
    exfat_set_fat_range(l->exfat_fat_blob, dir_allocs[i].first_cluster,
                        dir_allocs[i].cluster_count);
  }
  for(size_t i = 0; i < l->file_count; i++) {
    exfat_set_fat_range(l->exfat_fat_blob, file_allocs[i].first_cluster,
                        file_allocs[i].cluster_count);
  }

  uint64_t allocated_clusters = bitmap_clusters + upcase_clusters +
    dir_clusters_total + file_clusters_total;
  uint8_t percent_in_use = cluster_count ?
    (uint8_t)((allocated_clusters * 100ULL + cluster_count - 1ULL) /
              cluster_count) : 0;
  if(percent_in_use > 100) percent_in_use = 100;
  if(exfat_make_boot_blob(l, volume_sectors, fat_offset, fat_length,
                          cluster_heap_offset, cluster_count,
                          dir_allocs[0].first_cluster, percent_in_use,
                          err, err_size) != 0) {
    goto done;
  }

  l->image_size = volume_sectors * EXFAT_SECTOR_SIZE;
  if(layout_add_segment(l, 0, l->exfat_boot_blob_size, PFS_SEG_MEM,
                        l->exfat_boot_blob, NULL) != 0 ||
     layout_add_segment(l, (uint64_t)fat_offset * EXFAT_SECTOR_SIZE,
                        l->exfat_fat_blob_size, PFS_SEG_MEM,
                        l->exfat_fat_blob, NULL) != 0 ||
     layout_add_segment(l, exfat_cluster_offset(cluster_heap_offset,
                                                bitmap_cluster),
                        l->exfat_bitmap_blob_size, PFS_SEG_MEM,
                        l->exfat_bitmap_blob, NULL) != 0 ||
     layout_add_segment(l, exfat_cluster_offset(cluster_heap_offset,
                                                upcase_cluster),
                        l->exfat_upcase_blob_size, PFS_SEG_MEM,
                        l->exfat_upcase_blob, NULL) != 0) {
    set_err(err, err_size, "out of memory");
    goto done;
  }
  for(size_t i = 0; i < l->dir_count; i++) {
    if(layout_add_segment(l, exfat_cluster_offset(cluster_heap_offset,
                                                  dir_allocs[i].first_cluster),
                          l->dirs[i].blob_size, PFS_SEG_MEM,
                          l->dirs[i].blob, NULL) != 0) {
      set_err(err, err_size, "out of memory");
      goto done;
    }
  }
  for(size_t i = 0; i < l->file_count; i++) {
    uint64_t file_offset;
    if(file_allocs[i].cluster_count == 0) {
      l->files[i].block_start = 0;
      l->files[i].blocks = 0;
      continue;
    }
    file_offset = exfat_cluster_offset(cluster_heap_offset,
                                       file_allocs[i].first_cluster);
    l->files[i].block_start = file_offset / PFS_BLOCK_SIZE;
    l->files[i].blocks = ceil_div_u64(l->files[i].raw_size, PFS_BLOCK_SIZE);
    if(layout_add_segment(l, file_offset, l->files[i].raw_size, PFS_SEG_FILE,
                          NULL, l->files[i].abs) != 0) {
      set_err(err, err_size, "out of memory");
      goto done;
    }
  }

  rc = 0;

done:
  free(dir_allocs);
  free(file_allocs);
  return rc;
}

static int
build_exfat_layout_from_files(const char *root, const char *title_id,
                              pfs_layout_t *l,
                              char *err, size_t err_size) {
  scan_list_t scans = {0};
  int rc = -1;

  job_set_current("Scanning app folder");
  if(scan_collect(root, "", &scans, err, err_size) != 0) goto done;
  if(scans.count == 0) {
    set_err(err, err_size, "app folder contains no files");
    goto done;
  }
  qsort(scans.items, scans.count, sizeof(scans.items[0]), scan_file_cmp);
  rc = build_exfat_layout_from_scans(&scans, title_id, l, err, err_size);

done:
  free(scans.items);
  return rc;
}

static int
build_exfat_layout_from_files_stream(const char *root, const char *title_id,
                                     pfs_layout_t *l,
                                     const pfs_stream_options_t *stream_opts,
                                     pfs_app_info_t *info,
                                     char *err, size_t err_size) {
  scan_list_t scans = {0};
  int rc = -1;
  uint64_t forward_files = 0;
  uint64_t reverse_files = 0;
  pfs_stream_options_t normalized;

  stream_options_normalize(stream_opts, &normalized);
  job_set_current("Scheduling budgeted stream");
  if(scan_collect(root, "", &scans, err, err_size) != 0) goto done;
  if(scans.count == 0) {
    set_err(err, err_size, "app folder contains no files");
    goto done;
  }
  qsort(scans.items, scans.count, sizeof(scans.items[0]), scan_file_cmp);
  if(stream_schedule_budgeted_gain(&scans, &normalized,
                                   &forward_files, &reverse_files) != 0) {
    set_err(err, err_size, "out of memory");
    goto done;
  }
  if(info) {
    info->stream_budget_bytes = normalized.budget_bytes;
    info->stream_reserve_bytes = normalized.reserve_bytes;
    info->stream_forward_files = forward_files;
    info->stream_reverse_files = reverse_files;
  }
  rc = build_exfat_layout_from_scans(&scans, title_id, l, err, err_size);

done:
  free(scans.items);
  return rc;
}

static int
destructive_stream_build_scan_list(const destructive_stream_ctx_t *ctx,
                                   scan_list_t *scans,
                                   char *err, size_t err_size) {
  memset(scans, 0, sizeof(*scans));
  for(size_t i = 0; i < ctx->file_count; i++) {
    if(scan_push(scans, ctx->files[i].abs, ctx->files[i].rel,
                 ctx->files[i].original_size) != 0) {
      set_err(err, err_size, "out of memory");
      return -1;
    }
    scans->items[i].stream_predicted_stored = ctx->files[i].predicted_stored;
    scans->items[i].stream_schedule_order = ctx->files[i].schedule_order;
    scans->items[i].stream_predicted_gain_permille =
        ctx->files[i].predicted_gain_permille;
    scans->items[i].stream_reverse_required =
        ctx->files[i].reverse_required;
    scans->items[i].stream_passthrough_delete =
        ctx->files[i].passthrough_delete;
  }
  return 0;
}

static int
destructive_stream_build_layout(const destructive_stream_ctx_t *ctx,
                                pfs_layout_t *nested,
                                char *err, size_t err_size) {
  scan_list_t scans = {0};
  int rc = -1;
  if(destructive_stream_build_scan_list(ctx, &scans, err, err_size) != 0) {
    goto done;
  }
  if(ctx->format == PFS_COMPRESS_FORMAT_EXFAT) {
    rc = build_exfat_layout_from_scans(&scans, ctx->nested_name, nested,
                                       err, err_size);
  } else {
    rc = build_pfs_layout_from_scans(&scans, nested, err, err_size);
  }
done:
  free(scans.items);
  return rc;
}

static void
layout_free(pfs_layout_t *l) {
  if(!l) return;
  for(size_t i = 0; i < l->dir_count; i++) {
    free(l->dirs[i].child_dirs.items);
    free(l->dirs[i].child_files.items);
    free(l->dirs[i].blob);
  }
  free(l->dirs);
  free(l->files);
  free(l->segments);
  free(l->header_blob);
  free(l->inode_blob);
  free(l->superroot_blob);
  free(l->fpt_blob);
  free(l->collision_blob);
  free(l->exfat_boot_blob);
  free(l->exfat_fat_blob);
  free(l->exfat_bitmap_blob);
  free(l->exfat_upcase_blob);
  memset(l, 0, sizeof(*l));
}

static void
virtual_reader_init(virtual_reader_t *vr) {
  size_t cap = PFS_READ_CACHE_SIZE;
  memset(vr, 0, sizeof(*vr));
  vr->fd = -1;
  vr->open_seg = -1;
  vr->cache_seg = -1;
  while(cap >= PFS_READ_CACHE_MIN_SIZE) {
    vr->cache = malloc(cap);
    if(vr->cache) {
      vr->cache_cap = cap;
      return;
    }
    cap /= 2;
  }
}

static void
virtual_reader_close_file(virtual_reader_t *vr) {
  if(vr && vr->fd >= 0) close(vr->fd);
  if(vr) {
    vr->fd = -1;
    vr->open_seg = -1;
    vr->cache_len = 0;
    vr->cache_offset = 0;
    vr->cache_seg = -1;
  }
}

static void
virtual_reader_free(virtual_reader_t *vr) {
  if(!vr) return;
  virtual_reader_close_file(vr);
  free(vr->cache);
  memset(vr, 0, sizeof(*vr));
  vr->fd = -1;
  vr->open_seg = -1;
  vr->cache_seg = -1;
}

static int
virtual_reader_open_path(virtual_reader_t *vr, const char *path,
                         ssize_t key, char *err, size_t err_size) {
  if(!vr || !path) {
    set_err(err, err_size, "bad source reader");
    errno = EINVAL;
    return -1;
  }
  if(vr->fd >= 0 && vr->open_seg == key) return 0;
  virtual_reader_close_file(vr);
  vr->fd = open(path, O_RDONLY);
  if(vr->fd < 0) {
    set_err(err, err_size, "open source file: %s", strerror(errno));
    return -1;
  }
  vr->open_seg = key;
  return 0;
}

static int
virtual_reader_file_read(virtual_reader_t *vr, ssize_t seg_index,
                         uint64_t file_size, uint64_t file_offset,
                         unsigned char *out, size_t size) {
  if(size >= (size_t)PFS_READ_DIRECT_THRESHOLD) {
    vr->cache_len = 0;
    vr->cache_seg = -1;
    return read_exact_at(vr->fd, out, size, (off_t)file_offset);
  }
  if(!vr->cache || vr->cache_cap == 0) {
    return read_exact_at(vr->fd, out, size, (off_t)file_offset);
  }

  while(size > 0) {
    uint64_t cache_end = vr->cache_offset + vr->cache_len;
    if(vr->cache_seg != seg_index ||
       file_offset < vr->cache_offset ||
       file_offset >= cache_end) {
      uint64_t remaining = file_size > file_offset ? file_size - file_offset : 0;
      size_t fill = remaining > vr->cache_cap ? vr->cache_cap : (size_t)remaining;
      if(fill == 0) {
        errno = EIO;
        return -1;
      }
      if(read_exact_at(vr->fd, vr->cache, fill, (off_t)file_offset) != 0) {
        return -1;
      }
      vr->cache_seg = seg_index;
      vr->cache_offset = file_offset;
      vr->cache_len = fill;
      cache_end = vr->cache_offset + vr->cache_len;
    }

    size_t cache_off = (size_t)(file_offset - vr->cache_offset);
    size_t avail = (size_t)(cache_end - file_offset);
    size_t n = avail < size ? avail : size;
    memcpy(out, vr->cache + cache_off, n);
    out += n;
    file_offset += n;
    size -= n;
  }
  return 0;
}

static int
virtual_reader_read(pfs_layout_t *l, virtual_reader_t *vr, uint64_t offset,
                    unsigned char *out, size_t size,
                    char *err, size_t err_size) {
  if(offset >= l->image_size) {
    memset(out, 0, size);
    return 0;
  }

  uint64_t end = offset + size;
  if(end < offset) end = UINT64_MAX;
  uint64_t pos = offset;
  while(vr->seg_index < l->segment_count &&
        l->segments[vr->seg_index].offset + l->segments[vr->seg_index].size <= offset) {
    vr->seg_index++;
  }

  for(size_t i = vr->seg_index; i < l->segment_count; i++) {
    pfs_segment_t *s = &l->segments[i];
    uint64_t seg_end = s->offset + s->size;
    if(s->offset >= end) break;
    if(seg_end <= offset) continue;

    uint64_t copy_start = s->offset > offset ? s->offset : offset;
    uint64_t copy_end = seg_end < end ? seg_end : end;
    size_t n = (size_t)(copy_end - copy_start);
    size_t dst_off = (size_t)(copy_start - offset);
    size_t src_off = (size_t)(copy_start - s->offset);
    if(copy_start > pos) {
      memset(out + (size_t)(pos - offset), 0, (size_t)(copy_start - pos));
    }

    if(s->type == PFS_SEG_MEM) {
      memcpy(out + dst_off, s->mem + src_off, n);
    } else if(s->type == PFS_SEG_FILE) {
      if(vr->stream) {
        if(destructive_stream_read_reversed(vr->stream, vr, s->path, src_off,
                                            out + dst_off, n,
                                            err, err_size) != 0) {
          return -1;
        }
        if(copy_end > pos) pos = copy_end;
        continue;
      }
      if(vr->open_seg != (ssize_t)i) {
        if(virtual_reader_open_path(vr, s->path, (ssize_t)i,
                                    err, err_size) != 0) {
          return -1;
        }
      }
      if(virtual_reader_file_read(vr, (ssize_t)i, s->size, src_off,
                                  out + dst_off, n) != 0) {
        set_err(err, err_size, "read source file: %s", strerror(errno));
        return -1;
      }
    }
    if(copy_end > pos) pos = copy_end;
  }
  if(pos < end) {
    memset(out + (size_t)(pos - offset), 0, (size_t)(end - pos));
  }
  return 0;
}

static int
layout_block_overlaps_executable_file(const pfs_layout_t *l, uint64_t offset) {
  uint64_t end = offset + PFS_BLOCK_SIZE;
  if(!l) return 0;
  for(size_t i = 0; i < l->segment_count; i++) {
    const pfs_segment_t *s = &l->segments[i];
    uint64_t seg_end = s->offset + s->size;
    if(s->offset >= end) break;
    if(seg_end <= offset || s->type != PFS_SEG_FILE) continue;
    if(path_is_executable_payload(s->path)) return 1;
  }
  return 0;
}

static uint64_t
pfsc_header_span(uint64_t block_count) {
  uint64_t pointer_table_size = (block_count + 1) * PFSC_OFFSET_ENTRY_SIZE;
  uint64_t initial_capacity = PFSC_INITIAL_DATA_OFFSET - PFSC_BLOCK_OFFSETS_OFFSET;
  uint64_t extra = pointer_table_size > initial_capacity
                       ? pointer_table_size - initial_capacity
                       : 0;
  return PFSC_INITIAL_DATA_OFFSET + ceil_div_u64(extra, PFS_BLOCK_SIZE) * PFS_BLOCK_SIZE;
}

static int
write_pfsc_header(int fd, uint64_t file_start, uint64_t header_size,
                  uint64_t logical_size, const uint64_t *offsets,
                  uint64_t block_count, char *err, size_t err_size) {
  unsigned char *header = calloc(1, (size_t)header_size);
  if(!header) {
    set_err(err, err_size, "out of memory");
    return -1;
  }
  le32(header + 0x00, PFSC_MAGIC);
  le32(header + 0x04, PFSC_UNK4);
  le32(header + 0x08, PFSC_UNK8);
  le32(header + 0x0c, (uint32_t)PFS_BLOCK_SIZE);
  le64(header + 0x10, PFS_BLOCK_SIZE);
  le64(header + 0x18, PFSC_BLOCK_OFFSETS_OFFSET);
  le64(header + 0x20, header_size);
  le64(header + 0x28, logical_size);
  for(uint64_t i = 0; i <= block_count; i++) {
    le64(header + PFSC_BLOCK_OFFSETS_OFFSET + i * 8, offsets[i]);
  }
  int rc = pwrite_all_local(fd, header, (size_t)header_size, (off_t)file_start);
  if(rc != 0) set_err(err, err_size, "write PFSC header: %s", strerror(errno));
  free(header);
  return rc;
}

static int
pfs_vhash_writer_resume_local(pfs_vhash_writer_t *w, const char *path,
                              uint64_t block_count,
                              char *err, size_t err_size) {
  unsigned char hdr[PFS_VHASH_HEADER_SIZE];
  if(!w || !path || !path[0]) {
    set_err(err, err_size, "bad validation hash path");
    errno = EINVAL;
    return -1;
  }
  memset(w, 0, sizeof(*w));
  w->fd = -1;
  int fd = open(path, O_RDWR);
  if(fd < 0) {
    set_err(err, err_size, "open validation hash: %s", strerror(errno));
    return -1;
  }
  if(read_exact_at(fd, hdr, sizeof(hdr), 0) != 0 ||
     memcmp(hdr, "PFSCVHS1", 8) != 0 ||
     rd64(hdr + 40) != block_count) {
    close(fd);
    set_err(err, err_size, "validation hash cannot resume");
    errno = EINVAL;
    return -1;
  }
  w->fd = fd;
  w->block_count = block_count;
  snprintf(w->path, sizeof(w->path), "%s", path);
  return 0;
}

static int
pfs_compress_profile_normalize(int profile) {
  return profile == PFS_COMPRESS_PROFILE_FAST ?
      PFS_COMPRESS_PROFILE_FAST : PFS_COMPRESS_PROFILE_SPACE;
}

static int
pfsc_compressed_block_meets_gain(size_t comp_len, int threshold_gain) {
  if(comp_len >= (size_t)PFS_BLOCK_SIZE) return 0;
  if(threshold_gain <= 0) return 1;
  return ((uint64_t)((size_t)PFS_BLOCK_SIZE - comp_len) * 100ULL) >=
         ((uint64_t)PFS_BLOCK_SIZE * (uint64_t)threshold_gain);
}

static int
pfsc_block_likely_compressible(const unsigned char *raw) {
  uint16_t hist[256];
  memset(hist, 0, sizeof(hist));
  const size_t samples = PFSC_INCOMPRESSIBLE_SAMPLE_BYTES;
  const size_t step = (size_t)PFS_BLOCK_SIZE / samples;
  unsigned unique = 0;
  unsigned max_freq = 0;
  for(size_t i = 0; i < samples; i++) {
    unsigned v = raw[i * step];
    uint16_t n = ++hist[v];
    if(n == 1) unique++;
    if(n > max_freq) max_freq = n;
  }
  return unique <= PFSC_COMPRESSIBLE_UNIQUE_MAX ||
         max_freq >= PFSC_COMPRESSIBLE_FREQ_MIN;
}

static size_t
pfsc_compress_block_smaller(const unsigned char *raw, unsigned char *out,
                            const pfsc_comp_config_t *config,
                            pfsc_comp_state_t *comp) {
  if(config && (config->profile == PFS_COMPRESS_PROFILE_FAST ||
                config->fast_deflate)) {
    size_t in_size = (size_t)PFS_BLOCK_SIZE;
    size_t out_size = (size_t)PFS_BLOCK_SIZE - 1;
    if(!comp ||
       tdefl_init(comp, NULL, NULL, (int)config->miniz_flags) !=
           TDEFL_STATUS_OKAY) {
      return 0;
    }
    tdefl_status st = tdefl_compress(comp, raw, &in_size, out, &out_size,
                                     TDEFL_FINISH);
    if(st == TDEFL_STATUS_DONE &&
       in_size == (size_t)PFS_BLOCK_SIZE &&
       pfsc_compressed_block_meets_gain(out_size, config->threshold_gain)) {
      return out_size;
    }
    return 0;
  }

  size_t out_size = (size_t)PFS_BLOCK_SIZE - 1;
  uLongf z_out_size = (uLongf)out_size;
  int zlib_level = config ? config->zlib_level : GC_PFSC_ZLIB_LEVEL;
  int threshold_gain = config ? config->threshold_gain : GC_PFSC_THRESHOLD_GAIN;
  int zrc = compress2(out, &z_out_size, raw, (uLong)PFS_BLOCK_SIZE,
                      zlib_level);
  if(zrc == Z_OK &&
     pfsc_compressed_block_meets_gain((size_t)z_out_size, threshold_gain)) {
    return (size_t)z_out_size;
  }
  return 0;
}

static int
pfsc_find_ready_slot(pfsc_pool_t *pool) {
  for(int i = 0; i < pool->slot_count; i++) {
    if(pool->slots[i].state == PFSC_SLOT_READY) return i;
  }
  return -1;
}

static void
pfsc_pool_set_error(pfsc_pool_t *pool, int err) {
  pthread_mutex_lock(&pool->lock);
  if(!pool->error) pool->error = err ? err : EIO;
  pool->stop = 1;
  pthread_cond_broadcast(&pool->cond);
  pthread_mutex_unlock(&pool->lock);
}

static void *
pfsc_worker_main(void *arg) {
  pfsc_pool_t *pool = arg;
  pfsc_comp_state_t *comp = NULL;
  if(pool->comp_config.profile == PFS_COMPRESS_PROFILE_FAST ||
     pool->comp_config.fast_deflate) {
    comp = malloc(sizeof(*comp));
    if(!comp) {
      pfsc_pool_set_error(pool, ENOMEM);
      return NULL;
    }
  }

  for(;;) {
    pthread_mutex_lock(&pool->lock);
    int slot_index;
    while(!pool->stop && (slot_index = pfsc_find_ready_slot(pool)) < 0) {
      uint64_t wait_started = monotonic_us();
      pthread_cond_wait(&pool->cond, &pool->lock);
      job_add_wait_us(&g_job.worker_wait_us, wait_started);
    }
    if(pool->stop) {
      pthread_mutex_unlock(&pool->lock);
      break;
    }
    pfsc_slot_t *slot = &pool->slots[slot_index];
    slot->state = PFSC_SLOT_BUSY;
    pthread_mutex_unlock(&pool->lock);

    if(slot->force_raw) {
      slot->comp_len = 0;
      atomic_fetch_add(&g_job.skipped_zlib_blocks, 1);
    } else if(pool->comp_config.raw_only) {
      slot->comp_len = 0;
      atomic_fetch_add(&g_job.skipped_zlib_blocks, 1);
    } else if(pool->comp_config.entropy_skip &&
              !pfsc_block_likely_compressible(slot->raw)) {
      slot->comp_len = 0;
      atomic_fetch_add(&g_job.skipped_zlib_blocks, 1);
    } else {
      slot->comp_len = pfsc_compress_block_smaller(slot->raw, slot->comp,
                                                   &pool->comp_config, comp);
    }
    pfs_sha256(slot->raw, slot->raw_len, slot->hash);

    pthread_mutex_lock(&pool->lock);
    slot->state = PFSC_SLOT_DONE;
    pthread_cond_broadcast(&pool->cond);
    pthread_mutex_unlock(&pool->lock);
  }

  free(comp);
  return NULL;
}

static void
pfsc_pool_stop(pfsc_pool_t *pool) {
  pthread_mutex_lock(&pool->lock);
  pool->stop = 1;
  pthread_cond_broadcast(&pool->cond);
  pthread_mutex_unlock(&pool->lock);
}

static void
pfsc_free_slots(pfsc_slot_t *slots, int slot_count) {
  if(!slots) return;
  for(int i = 0; i < slot_count; i++) {
    free(slots[i].raw);
    free(slots[i].comp);
  }
  free(slots);
}

static int
pfsc_window_clamp_int(int v, int lo, int hi) {
  if(v < lo) return lo;
  if(v > hi) return hi;
  return v;
}

static int
pfsc_window_worker_cap(int requested_workers) {
  if(requested_workers <= PFS_COMPRESS_DEFAULT_WORKERS) {
    return PFSC_WINDOW_MAX_WORKERS;
  }
  return pfsc_window_clamp_int(requested_workers, PFSC_WINDOW_MIN_WORKERS,
                               PFSC_WINDOW_MAX_WORKERS);
}

static int
pfsc_window_comp_permits(const pfsc_window_tuning_t *tuning,
                         int stage_permits) {
  int permits = PFSC_WINDOW_COMP_PERMITS;
  int active_room = tuning->max_workers - stage_permits;
  if(permits > active_room) permits = active_room;
  if(permits < PFSC_WINDOW_MIN_COMP_WORKERS) {
    permits = PFSC_WINDOW_MIN_COMP_WORKERS;
  }
  if(permits > tuning->max_workers) permits = tuning->max_workers;
  return permits;
}

static void
pfsc_window_tuning_init(pfsc_window_tuning_t *tuning, int requested_workers) {
  memset(tuning, 0, sizeof(*tuning));
  tuning->max_workers = pfsc_window_worker_cap(requested_workers);
  tuning->read_permits = PFSC_WINDOW_READ_PERMITS;
  tuning->write_permits = PFSC_WINDOW_WRITE_PERMITS;
}

static void
job_store_long_max(atomic_long *counter, uint64_t value) {
  long wanted = value > (uint64_t)LONG_MAX ? LONG_MAX : (long)value;
  long cur = atomic_load(counter);
  while(cur < wanted &&
        !atomic_compare_exchange_weak(counter, &cur, wanted)) {
  }
}

static void
job_store_int_max(atomic_int *counter, uint64_t value) {
  int wanted = value > (uint64_t)INT_MAX ? INT_MAX : (int)value;
  int cur = atomic_load(counter);
  while(cur < wanted &&
        !atomic_compare_exchange_weak(counter, &cur, wanted)) {
  }
}

static void
pfsc_window_push_task(pfsc_window_task_t **head, pfsc_window_task_t **tail,
                      pfsc_window_task_t *task) {
  task->next = NULL;
  if(*tail) (*tail)->next = task;
  else *head = task;
  *tail = task;
}

static pfsc_window_task_t *
pfsc_window_pop_task(pfsc_window_task_t **head, pfsc_window_task_t **tail) {
  pfsc_window_task_t *task = *head;
  if(!task) return NULL;
  *head = task->next;
  if(!*head) *tail = NULL;
  task->next = NULL;
  return task;
}

static void
pfsc_window_pool_set_error(pfsc_window_pool_t *pool, int err,
                           const char *msg) {
  pthread_mutex_lock(&pool->lock);
  if(!pool->error) {
    pool->error = err ? err : EIO;
    snprintf(pool->error_msg, sizeof(pool->error_msg), "%s",
             msg && msg[0] ? msg : strerror(pool->error));
  }
  pool->stop = 1;
  pthread_cond_broadcast(&pool->cond);
  pthread_mutex_unlock(&pool->lock);
}

static int
pfsc_window_pool_enqueue_locked(pfsc_window_pool_t *pool,
                                pfsc_window_task_type_t type,
                                pfsc_window_t *window, int lane_index,
                                uint32_t first_block,
                                uint32_t block_count,
                                uint64_t output_offset) {
  pfsc_window_task_t *task = malloc(sizeof(*task));
  if(!task) {
    if(!pool->error) {
      pool->error = ENOMEM;
      snprintf(pool->error_msg, sizeof(pool->error_msg), "%s",
               "out of memory");
      pool->stop = 1;
    }
    pthread_cond_broadcast(&pool->cond);
    return -1;
  }
  memset(task, 0, sizeof(*task));
  task->type = type;
  task->window = window;
  task->lane_index = lane_index;
  task->first_block = first_block;
  task->block_count = block_count;
  task->output_offset = output_offset;
  switch(type) {
    case PFSC_WINDOW_TASK_READ:
      window->read_tasks_remaining++;
      pfsc_window_push_task(&pool->read_head, &pool->read_tail, task);
      break;
    case PFSC_WINDOW_TASK_COMPRESS:
      window->comp_tasks_remaining++;
      pfsc_window_push_task(&pool->comp_head, &pool->comp_tail, task);
      break;
    case PFSC_WINDOW_TASK_WRITE:
      window->write_tasks_remaining++;
      pfsc_window_push_task(&pool->write_head, &pool->write_tail, task);
      break;
  }
  pthread_cond_broadcast(&pool->cond);
  return 0;
}

static void
pfsc_window_pool_set_phase(pfsc_window_pool_t *pool, int phase,
                           int read_permits, int comp_permits,
                           int write_permits) {
  pthread_mutex_lock(&pool->lock);
  pool->phase = phase;
  pool->read_permits = read_permits;
  pool->comp_permits = comp_permits;
  pool->write_permits = write_permits;
  pthread_cond_broadcast(&pool->cond);
  pthread_mutex_unlock(&pool->lock);
}

static pfsc_window_task_t *
pfsc_window_pool_take_task_locked(pfsc_window_pool_t *pool) {
  if(pool->phase == PFSC_WINDOW_PHASE_READ) {
    if(pool->read_head && pool->running_read < pool->read_permits) {
      pool->running_read++;
      return pfsc_window_pop_task(&pool->read_head, &pool->read_tail);
    }
    if(pool->comp_head && pool->running_comp < pool->comp_permits) {
      pool->running_comp++;
      return pfsc_window_pop_task(&pool->comp_head, &pool->comp_tail);
    }
  } else if(pool->phase == PFSC_WINDOW_PHASE_WRITE) {
    if(pool->write_head && pool->running_write < pool->write_permits) {
      pool->running_write++;
      return pfsc_window_pop_task(&pool->write_head, &pool->write_tail);
    }
    if(pool->comp_head && pool->running_comp < pool->comp_permits) {
      pool->running_comp++;
      return pfsc_window_pop_task(&pool->comp_head, &pool->comp_tail);
    }
  } else if(pool->comp_head && pool->running_comp < pool->comp_permits) {
    pool->running_comp++;
    return pfsc_window_pop_task(&pool->comp_head, &pool->comp_tail);
  }
  return NULL;
}

static void
pfsc_window_pool_finish_task_locked(pfsc_window_pool_t *pool,
                                    const pfsc_window_task_t *task) {
  switch(task->type) {
    case PFSC_WINDOW_TASK_READ:
      if(pool->running_read > 0) pool->running_read--;
      if(task->window->read_tasks_remaining > 0) {
        task->window->read_tasks_remaining--;
      }
      break;
    case PFSC_WINDOW_TASK_COMPRESS:
      if(pool->running_comp > 0) pool->running_comp--;
      if(task->window->comp_tasks_remaining > 0) {
        task->window->comp_tasks_remaining--;
      }
      break;
    case PFSC_WINDOW_TASK_WRITE:
      if(pool->running_write > 0) pool->running_write--;
      if(task->window->write_tasks_remaining > 0) {
        task->window->write_tasks_remaining--;
      }
      break;
  }
  pthread_cond_broadcast(&pool->cond);
}

static void
pfsc_window_pack_lane(pfsc_window_lane_t *lane) {
  int all_raw = 1;
  for(uint64_t i = 0; i < lane->block_count; i++) {
    if(lane->blocks[i].compressed) {
      all_raw = 0;
      break;
    }
  }
  if(all_raw) {
    lane->write_data = lane->raw;
    lane->packed_len = lane->raw_size;
    return;
  }

  lane->write_data = lane->out;
  uint64_t dst = 0;
  for(uint64_t i = 0; i < lane->block_count; i++) {
    uint64_t src = i * PFS_BLOCK_SIZE;
    size_t n = lane->blocks[i].stored_len;
    if(n == 0) continue;
    if(lane->blocks[i].compressed) {
      if(dst != src) memmove(lane->out + dst, lane->out + src, n);
    } else {
      memcpy(lane->out + dst, lane->raw + src, n);
    }
    dst += n;
  }
  lane->packed_len = dst;
}

static int
pfsc_window_lane_ready_locked(const pfsc_window_lane_t *lane) {
  for(uint64_t i = 0; i < lane->block_count; i++) {
    if(!lane->blocks[i].ready) return 0;
  }
  return 1;
}

static void
pfsc_window_read_task(pfsc_window_pool_t *pool, pfsc_window_task_t *task) {
  pfsc_window_lane_t *lane = &task->window->lanes[task->lane_index];
  virtual_reader_t vr;
  char local_err[256] = {0};
  int saved_errno = 0;
  virtual_reader_init(&vr);
  if(job_cancelled()) {
    saved_errno = EINTR;
    snprintf(local_err, sizeof(local_err), "%s", "cancelled");
  } else if(virtual_reader_read(pool->nested, &vr, lane->logical_offset,
                                lane->raw, (size_t)lane->raw_size,
                                local_err, sizeof(local_err)) != 0) {
    saved_errno = errno ? errno : EIO;
  }
  virtual_reader_free(&vr);
  if(saved_errno) {
    pfsc_window_pool_set_error(pool, saved_errno, local_err);
    return;
  }
  uint64_t copied = (lane->start_block + lane->block_count) * PFS_BLOCK_SIZE;
  if(copied > pool->nested->image_size) copied = pool->nested->image_size;
  job_store_long_max(&g_job.copied_bytes, copied);
  pthread_mutex_lock(&pool->lock);
  lane->read_done = 1;
  pthread_cond_broadcast(&pool->cond);
  pthread_mutex_unlock(&pool->lock);
}

static void
pfsc_window_compress_task(pfsc_window_pool_t *pool, pfsc_window_task_t *task,
                          pfsc_comp_state_t *comp) {
  pfsc_window_lane_t *lane = &task->window->lanes[task->lane_index];
  const pfsc_comp_config_t *config = pool->comp_config;

  for(uint32_t i = 0; i < task->block_count; i++) {
    if(job_cancelled()) {
      pfsc_window_pool_set_error(pool, EINTR, "cancelled");
      return;
    }
    uint64_t local_block = (uint64_t)task->first_block + i;
    uint64_t global_block = lane->start_block + local_block;
    unsigned char *raw = lane->raw + local_block * PFS_BLOCK_SIZE;
    unsigned char *out = lane->out + local_block * PFS_BLOCK_SIZE;
    pfsc_window_block_t *result = &lane->blocks[local_block];
    size_t hash_len = (size_t)PFS_BLOCK_SIZE;
    if(global_block * PFS_BLOCK_SIZE < pool->nested->image_size) {
      uint64_t remaining = pool->nested->image_size -
                           global_block * PFS_BLOCK_SIZE;
      if(remaining < PFS_BLOCK_SIZE) hash_len = (size_t)remaining;
    }
    result->force_raw = config && config->force_raw_exec ?
        layout_block_overlaps_executable_file(pool->nested,
                                              global_block * PFS_BLOCK_SIZE) :
        0;
    pfs_sha256(raw, hash_len, result->hash);
    if(result->force_raw) {
      result->stored_len = (uint32_t)PFS_BLOCK_SIZE;
      result->compressed = 0;
      atomic_fetch_add(&g_job.skipped_zlib_blocks, 1);
    } else if(config && config->raw_only) {
      result->stored_len = (uint32_t)PFS_BLOCK_SIZE;
      result->compressed = 0;
      atomic_fetch_add(&g_job.skipped_zlib_blocks, 1);
    } else if(config && config->entropy_skip &&
              !pfsc_block_likely_compressible(raw)) {
      result->stored_len = (uint32_t)PFS_BLOCK_SIZE;
      result->compressed = 0;
      atomic_fetch_add(&g_job.skipped_zlib_blocks, 1);
    } else {
      size_t comp_len = pfsc_compress_block_smaller(raw, out, config, comp);
      if(comp_len > 0 && comp_len < (size_t)PFS_BLOCK_SIZE) {
        result->stored_len = (uint32_t)comp_len;
        result->compressed = 1;
      } else {
        result->stored_len = (uint32_t)PFS_BLOCK_SIZE;
        result->compressed = 0;
      }
    }

    pthread_mutex_lock(&pool->lock);
    result->ready = 1;
    pthread_cond_broadcast(&pool->cond);
    pthread_mutex_unlock(&pool->lock);
  }
}

static void
pfsc_window_write_task(pfsc_window_pool_t *pool, pfsc_window_task_t *task) {
  pfsc_window_lane_t *lane = &task->window->lanes[task->lane_index];
  char local_err[256] = {0};
  int saved_errno = 0;
  uint64_t copied = (lane->start_block + lane->block_count) * PFS_BLOCK_SIZE;
  unsigned char *payload = lane->write_data ? lane->write_data : lane->out;
  if(copied > pool->nested->image_size) copied = pool->nested->image_size;

  if(job_cancelled()) {
    saved_errno = EINTR;
    snprintf(local_err, sizeof(local_err), "%s", "cancelled");
  } else if(pwrite_all_local(pool->fd, payload, (size_t)lane->packed_len,
                             (off_t)(pool->file_start + lane->output_offset)) != 0) {
    saved_errno = errno ? errno : EIO;
    snprintf(local_err, sizeof(local_err), "write compressed payload: %s",
             strerror(saved_errno));
  }
  if(!saved_errno) {
    long raw_blocks = 0;
    long compressed_blocks = 0;
    unsigned char *hashes = malloc((size_t)lane->block_count *
                                   (size_t)PFS_VHASH_HASH_SIZE);
    if(!hashes) {
      saved_errno = ENOMEM;
      snprintf(local_err, sizeof(local_err), "%s", "out of memory");
    }
    for(uint64_t i = 0; i < lane->block_count; i++) {
      if(!saved_errno) {
        memcpy(hashes + i * PFS_VHASH_HASH_SIZE, lane->blocks[i].hash,
               PFS_VHASH_HASH_SIZE);
      }
      if(lane->blocks[i].compressed) compressed_blocks++;
      else raw_blocks++;
    }
    if(!saved_errno) {
      uint64_t off = PFS_VHASH_HEADER_SIZE +
          lane->start_block * (uint64_t)PFS_VHASH_HASH_SIZE;
      size_t bytes = (size_t)lane->block_count * (size_t)PFS_VHASH_HASH_SIZE;
      if(pwrite_all_local(pool->vhash->fd, hashes, bytes, (off_t)off) != 0) {
        saved_errno = errno ? errno : EIO;
        snprintf(local_err, sizeof(local_err), "write validation hash: %s",
                 strerror(saved_errno));
      }
    }
    free(hashes);
    if(raw_blocks) atomic_fetch_add(&g_job.raw_blocks, raw_blocks);
    if(compressed_blocks) {
      atomic_fetch_add(&g_job.compressed_blocks, compressed_blocks);
    }
  }
  if(saved_errno) {
    pfsc_window_pool_set_error(pool, saved_errno, local_err);
    return;
  }
  job_store_long_max(&g_job.copied_bytes, copied);
  job_store_int_max(&g_job.done_files, lane->start_block + lane->block_count);
  uint64_t output_end = lane->output_offset + lane->packed_len;
  job_store_long_max(&g_job.compressed_output_bytes,
                     output_end > pool->header_size ?
                         output_end - pool->header_size : 0);

  pthread_mutex_lock(&pool->lock);
  lane->write_done = 1;
  pthread_cond_broadcast(&pool->cond);
  pthread_mutex_unlock(&pool->lock);
}

static int
pfsc_window_write_lane_hashes_ordered(pfs_vhash_writer_t *vhash,
                                      pfsc_window_lane_t *lane,
                                      char *err, size_t err_size) {
  if(!vhash || vhash->fd < 0 || !lane) {
    errno = EINVAL;
    set_err(err, err_size, "bad ordered validation hash write");
    return -1;
  }
  uint64_t expected = PFS_VHASH_HEADER_SIZE +
      lane->start_block * (uint64_t)PFS_VHASH_HASH_SIZE;
  off_t pos = lseek(vhash->fd, 0, SEEK_CUR);
  if(pos < 0) {
    set_err(err, err_size, "seek validation hash: %s", strerror(errno));
    return -1;
  }
  if((uint64_t)pos != expected) {
    errno = EIO;
    set_err(err, err_size,
            "validation hash out of order: got %llu expected %llu",
            (unsigned long long)(uint64_t)pos,
            (unsigned long long)expected);
    return -1;
  }

  size_t bytes = (size_t)lane->block_count * (size_t)PFS_VHASH_HASH_SIZE;
  unsigned char *hashes = malloc(bytes);
  if(!hashes) {
    errno = ENOMEM;
    set_err(err, err_size, "out of memory");
    return -1;
  }

  long raw_blocks = 0;
  long compressed_blocks = 0;
  for(uint64_t i = 0; i < lane->block_count; i++) {
    memcpy(hashes + i * PFS_VHASH_HASH_SIZE, lane->blocks[i].hash,
           PFS_VHASH_HASH_SIZE);
    if(lane->blocks[i].compressed) compressed_blocks++;
    else raw_blocks++;
  }

  if(write_all_local(vhash->fd, hashes, bytes) != 0) {
    set_err(err, err_size, "write validation hash: %s", strerror(errno));
    free(hashes);
    return -1;
  }
  free(hashes);

  if(raw_blocks) atomic_fetch_add(&g_job.raw_blocks, raw_blocks);
  if(compressed_blocks) {
    atomic_fetch_add(&g_job.compressed_blocks, compressed_blocks);
  }
  return 0;
}

static int
pfsc_window_commit_lane_ordered(pfsc_window_pool_t *pool,
                                const pfsc_window_t *window,
                                pfsc_window_lane_t *lane, int lane_index,
                                uint64_t *data_pos, uint64_t *offsets,
                                char *err, size_t err_size) {
  if(job_cancelled()) {
    errno = EINTR;
    set_err(err, err_size, "cancelled");
    return -1;
  }

  pfsc_window_pack_lane(lane);
  lane->packed = 1;
  lane->output_offset = *data_pos;

  uint64_t packed_sum = 0;
  for(uint64_t i = 0; i < lane->block_count; i++) {
    if(!lane->blocks[i].ready || lane->blocks[i].stored_len == 0 ||
       lane->blocks[i].stored_len > (uint32_t)PFS_BLOCK_SIZE) {
      errno = EIO;
      set_err(err, err_size, "bad ordered window block state");
      return -1;
    }
    packed_sum += lane->blocks[i].stored_len;
  }
  if(packed_sum != lane->packed_len) {
    errno = EIO;
    set_err(err, err_size,
            "ordered window packed size mismatch: got %llu expected %llu",
            (unsigned long long)lane->packed_len,
            (unsigned long long)packed_sum);
    return -1;
  }

  uint64_t expected = pool->file_start + lane->output_offset;
  off_t pos = lseek(pool->fd, 0, SEEK_CUR);
  if(pos < 0) {
    set_err(err, err_size, "seek compressed payload: %s", strerror(errno));
    return -1;
  }
  if((uint64_t)pos != expected) {
    errno = EIO;
    set_err(err, err_size,
            "compressed payload out of order: got %llu expected %llu",
            (unsigned long long)(uint64_t)pos,
            (unsigned long long)expected);
    return -1;
  }

  unsigned char *payload = lane->write_data ? lane->write_data : lane->out;
  PFSC_WINDOW_LOG("pfsc window commit index=%llu offset=%llu bytes=%llu lane=%d",
                  (unsigned long long)window->index,
                  (unsigned long long)lane->output_offset,
                  (unsigned long long)lane->packed_len, lane_index);
  if(write_all_local(pool->fd, payload, (size_t)lane->packed_len) != 0) {
    set_err(err, err_size, "write compressed payload: %s", strerror(errno));
    return -1;
  }
  if(pfsc_window_write_lane_hashes_ordered(pool->vhash, lane,
                                           err, err_size) != 0) {
    return -1;
  }

  for(uint64_t i = 0; i < lane->block_count; i++) {
    uint64_t block = lane->start_block + i;
    *data_pos += lane->blocks[i].stored_len;
    offsets[block + 1ULL] = *data_pos;
    lane->blocks[i].written = 1;
  }

  uint64_t copied = (lane->start_block + lane->block_count) * PFS_BLOCK_SIZE;
  if(copied > pool->nested->image_size) copied = pool->nested->image_size;
  job_store_long_max(&g_job.copied_bytes, copied);
  job_store_int_max(&g_job.done_files, lane->start_block + lane->block_count);
  uint64_t output_end = lane->output_offset + lane->packed_len;
  job_store_long_max(&g_job.compressed_output_bytes,
                     output_end > pool->header_size ?
                         output_end - pool->header_size : 0);
  return 0;
}

static void *
pfsc_window_worker_main(void *arg) {
  pfsc_window_pool_t *pool = arg;
  pfsc_comp_state_t *comp = NULL;
  for(;;) {
    pthread_mutex_lock(&pool->lock);
    pfsc_window_task_t *task = NULL;
    while(!pool->stop && !(task = pfsc_window_pool_take_task_locked(pool))) {
      pthread_cond_wait(&pool->cond, &pool->lock);
    }
    if(pool->stop && !task) {
      pthread_mutex_unlock(&pool->lock);
      break;
    }
    pthread_mutex_unlock(&pool->lock);

    switch(task->type) {
      case PFSC_WINDOW_TASK_READ:
        pfsc_window_read_task(pool, task);
        break;
      case PFSC_WINDOW_TASK_COMPRESS:
        if(!comp && pool->comp_config &&
           (pool->comp_config->profile == PFS_COMPRESS_PROFILE_FAST ||
            pool->comp_config->fast_deflate)) {
          comp = malloc(sizeof(*comp));
          if(!comp) {
            pfsc_window_pool_set_error(pool, ENOMEM, "out of memory");
            break;
          }
        }
        pfsc_window_compress_task(pool, task, comp);
        break;
      case PFSC_WINDOW_TASK_WRITE:
        pfsc_window_write_task(pool, task);
        break;
    }

    pthread_mutex_lock(&pool->lock);
    pfsc_window_pool_finish_task_locked(pool, task);
    pthread_mutex_unlock(&pool->lock);
    free(task);
  }
  free(comp);
  return NULL;
}

static void
pfsc_window_free_task_list(pfsc_window_task_t *task) {
  while(task) {
    pfsc_window_task_t *next = task->next;
    free(task);
    task = next;
  }
}

static void
pfsc_window_pool_stop(pfsc_window_pool_t *pool) {
  pthread_mutex_lock(&pool->lock);
  pool->stop = 1;
  pthread_cond_broadcast(&pool->cond);
  pthread_mutex_unlock(&pool->lock);
}

static int
pfsc_window_pool_start(pfsc_window_pool_t *pool, int worker_count) {
  memset(pool, 0, sizeof(*pool));
  pool->thread_count = worker_count;
  pool->threads = calloc((size_t)worker_count, sizeof(*pool->threads));
  if(!pool->threads) return -1;
  if(pthread_mutex_init(&pool->lock, NULL) != 0 ||
     pthread_cond_init(&pool->cond, NULL) != 0) {
    free(pool->threads);
    pool->threads = NULL;
    return -1;
  }
  for(int i = 0; i < worker_count; i++) {
    int trc = pthread_create(&pool->threads[i], NULL,
                             pfsc_window_worker_main, pool);
    if(trc != 0) {
      pool->error = trc;
      snprintf(pool->error_msg, sizeof(pool->error_msg),
               "start window worker: %s", strerror(trc));
      pfsc_window_pool_stop(pool);
      for(int j = 0; j < pool->workers_started; j++) {
        pthread_join(pool->threads[j], NULL);
      }
      pthread_cond_destroy(&pool->cond);
      pthread_mutex_destroy(&pool->lock);
      free(pool->threads);
      pool->threads = NULL;
      return -1;
    }
    pool->workers_started++;
  }
  return 0;
}

static void
pfsc_window_pool_destroy(pfsc_window_pool_t *pool) {
  if(!pool) return;
  if(pool->threads) {
    pfsc_window_pool_stop(pool);
    for(int i = 0; i < pool->workers_started; i++) {
      pthread_join(pool->threads[i], NULL);
    }
    free(pool->threads);
  }
  pfsc_window_free_task_list(pool->read_head);
  pfsc_window_free_task_list(pool->comp_head);
  pfsc_window_free_task_list(pool->write_head);
  pthread_cond_destroy(&pool->cond);
  pthread_mutex_destroy(&pool->lock);
  memset(pool, 0, sizeof(*pool));
}

static int
pfsc_window_pool_check_error_locked(pfsc_window_pool_t *pool,
                                    char *err, size_t err_size) {
  if(!pool->error) return 0;
  set_err(err, err_size, "%s",
          pool->error_msg[0] ? pool->error_msg : strerror(pool->error));
  errno = pool->error;
  return -1;
}

static int
pfsc_window_alloc_windows(pfsc_window_t *windows, char *err, size_t err_size) {
  memset(windows, 0, sizeof(pfsc_window_t) * PFSC_WINDOW_SLOTS);
  for(int w = 0; w < PFSC_WINDOW_SLOTS; w++) {
    for(int l = 0; l < PFSC_WINDOW_LANES; l++) {
      windows[w].lanes[l].raw = malloc((size_t)PFSC_WINDOW_LANE_SIZE);
      windows[w].lanes[l].out = malloc((size_t)PFSC_WINDOW_LANE_SIZE);
      windows[w].lanes[l].blocks = calloc((size_t)PFSC_WINDOW_BLOCKS_PER_LANE,
                                          sizeof(*windows[w].lanes[l].blocks));
      if(!windows[w].lanes[l].raw || !windows[w].lanes[l].out ||
         !windows[w].lanes[l].blocks) {
        set_err(err, err_size, "window buffers unavailable");
        return -1;
      }
    }
  }
  return 0;
}

static void
pfsc_window_free_windows(pfsc_window_t *windows) {
  if(!windows) return;
  for(int w = 0; w < PFSC_WINDOW_SLOTS; w++) {
    for(int l = 0; l < PFSC_WINDOW_LANES; l++) {
      free(windows[w].lanes[l].raw);
      free(windows[w].lanes[l].out);
      free(windows[w].lanes[l].blocks);
    }
  }
}

static int
pfsc_window_prepare(pfsc_window_t *window, uint64_t window_index,
                    uint64_t start_block, uint64_t block_count) {
  unsigned char *raw[PFSC_WINDOW_LANES];
  unsigned char *out[PFSC_WINDOW_LANES];
  pfsc_window_block_t *blocks[PFSC_WINDOW_LANES];
  for(int i = 0; i < PFSC_WINDOW_LANES; i++) {
    raw[i] = window->lanes[i].raw;
    out[i] = window->lanes[i].out;
    blocks[i] = window->lanes[i].blocks;
  }
  memset(window, 0, sizeof(*window));
  for(int i = 0; i < PFSC_WINDOW_LANES; i++) {
    window->lanes[i].raw = raw[i];
    window->lanes[i].out = out[i];
    window->lanes[i].blocks = blocks[i];
  }
  window->index = window_index;
  window->start_block = start_block;
  window->block_count = block_count;
  uint64_t remaining = block_count;
  for(int i = 0; i < PFSC_WINDOW_LANES && remaining > 0; i++) {
    uint64_t lane_blocks = remaining > PFSC_WINDOW_BLOCKS_PER_LANE ?
        PFSC_WINDOW_BLOCKS_PER_LANE : remaining;
    pfsc_window_lane_t *lane = &window->lanes[i];
    lane->start_block = start_block +
        (uint64_t)i * PFSC_WINDOW_BLOCKS_PER_LANE;
    lane->block_count = lane_blocks;
    lane->logical_offset = lane->start_block * PFS_BLOCK_SIZE;
    lane->raw_size = lane_blocks * PFS_BLOCK_SIZE;
    lane->write_data = lane->out;
    memset(lane->blocks, 0, (size_t)PFSC_WINDOW_BLOCKS_PER_LANE *
           sizeof(*lane->blocks));
    window->lane_count++;
    remaining -= lane_blocks;
  }
  return window->lane_count > 0 ? 0 : -1;
}

static int
pfsc_window_read_window(pfsc_window_pool_t *pool, pfsc_window_t *window,
                        const pfsc_window_tuning_t *tuning,
                        char *err, size_t err_size) {
  int comp_permits = pfsc_window_comp_permits(tuning, tuning->read_permits);
  uint64_t start = monotonic_us();
  pfsc_window_pool_set_phase(pool, PFSC_WINDOW_PHASE_READ,
                             tuning->read_permits, comp_permits, 0);
  pthread_mutex_lock(&pool->lock);
  for(int i = 0; i < window->lane_count; i++) {
    if(pfsc_window_pool_enqueue_locked(pool, PFSC_WINDOW_TASK_READ, window,
                                       i, 0, 0, 0) != 0) {
      pthread_mutex_unlock(&pool->lock);
      return -1;
    }
  }
  while(window->read_tasks_remaining > 0 ||
        pool->running_read > 0) {
    if(pfsc_window_pool_check_error_locked(pool, err, err_size) != 0) {
      pthread_mutex_unlock(&pool->lock);
      return -1;
    }
    pthread_cond_wait(&pool->cond, &pool->lock);
  }
  if(pfsc_window_pool_check_error_locked(pool, err, err_size) != 0) {
    pthread_mutex_unlock(&pool->lock);
    return -1;
  }
  pthread_mutex_unlock(&pool->lock);
  uint64_t elapsed = monotonic_us() - start;
  (void)elapsed;
  PFSC_WINDOW_LOG("pfsc window read index=%llu lanes=%d readPermits=%d compPermits=%d elapsedUs=%llu",
                  (unsigned long long)window->index, window->lane_count,
                  tuning->read_permits, comp_permits,
                  (unsigned long long)elapsed);
  return 0;
}

static int
pfsc_window_start_compression(pfsc_window_pool_t *pool,
                              pfsc_window_t *window,
                              char *err, size_t err_size) {
  if(window->compression_started) return 0;
  pthread_mutex_lock(&pool->lock);
  window->compression_started = 1;
  for(int i = 0; i < window->lane_count; i++) {
    pfsc_window_lane_t *lane = &window->lanes[i];
    lane->comp_started = 1;
    lane->comp_chunks_total = (int)ceil_div_u64(
        lane->block_count, PFSC_WINDOW_COMP_CHUNK_BLOCKS);
    if(lane->comp_chunks_total < 1) lane->comp_chunks_total = 1;
    for(uint32_t first = 0; first < lane->block_count;
        first += PFSC_WINDOW_COMP_CHUNK_BLOCKS) {
      uint64_t left = lane->block_count - first;
      uint32_t count = left > PFSC_WINDOW_COMP_CHUNK_BLOCKS ?
          PFSC_WINDOW_COMP_CHUNK_BLOCKS : (uint32_t)left;
      if(pfsc_window_pool_enqueue_locked(pool, PFSC_WINDOW_TASK_COMPRESS,
                                         window, i, first, count, 0) != 0) {
        pthread_mutex_unlock(&pool->lock);
        set_err(err, err_size, "out of memory");
        return -1;
      }
    }
  }
  pthread_mutex_unlock(&pool->lock);
  return 0;
}

static int
pfsc_window_write_window(pfsc_window_pool_t *pool, pfsc_window_t *window,
                         pfsc_window_tuning_t *tuning, uint64_t *data_pos,
                         uint64_t *offsets, char *err, size_t err_size) {
  int comp_permits = pfsc_window_comp_permits(tuning, tuning->write_permits);
  uint64_t start = monotonic_us();
  uint64_t wait_us = 0;
  int next_lane = 0;
  pfsc_window_pool_set_phase(pool, PFSC_WINDOW_PHASE_WRITE,
                             0, comp_permits, 0);

  pthread_mutex_lock(&pool->lock);
  while(next_lane < window->lane_count) {
    if(pfsc_window_pool_check_error_locked(pool, err, err_size) != 0) {
      pthread_mutex_unlock(&pool->lock);
      return -1;
    }
    pfsc_window_lane_t *lane = &window->lanes[next_lane];
    if(pfsc_window_lane_ready_locked(lane)) {
      pthread_mutex_unlock(&pool->lock);
      int commit_rc = pfsc_window_commit_lane_ordered(
          pool, window, lane, next_lane, data_pos, offsets, err, err_size);
      int saved_errno = errno ? errno : EIO;
      if(commit_rc != 0) {
        pfsc_window_pool_set_error(pool, saved_errno,
                                   err && err[0] ? err : strerror(saved_errno));
        errno = saved_errno;
        return -1;
      }
      pthread_mutex_lock(&pool->lock);
      lane->write_done = 1;
      pthread_cond_broadcast(&pool->cond);
      next_lane++;
      continue;
    }
    uint64_t wait_start = monotonic_us();
    pthread_cond_wait(&pool->cond, &pool->lock);
    uint64_t now = monotonic_us();
    if(now > wait_start) wait_us += now - wait_start;
    job_add_wait_us(&g_job.writer_wait_us, wait_start);
  }
  if(pfsc_window_pool_check_error_locked(pool, err, err_size) != 0) {
    pthread_mutex_unlock(&pool->lock);
    return -1;
  }
  pthread_mutex_unlock(&pool->lock);

  uint64_t elapsed = monotonic_us() - start;
  PFSC_WINDOW_LOG("pfsc window write index=%llu lanes=%d writePermits=0 orderedCommit=1 requestedWritePermits=%d compPermits=%d waitUs=%llu elapsedUs=%llu",
                  (unsigned long long)window->index, window->lane_count,
                  tuning->write_permits, comp_permits,
                  (unsigned long long)wait_us, (unsigned long long)elapsed);
  return 0;
}

static int
compress_nested_to_pfsc_windowed(int fd, uint64_t file_start,
                                 pfs_layout_t *nested,
                                 int requested_workers,
                                 const char *nested_name,
                                 int nested_type,
                                 const char *vhash_path,
                                 uint64_t *stored_size,
                                 int compression_profile,
                                 char *err, size_t err_size) {
  uint64_t block_count = ceil_div_u64(nested->image_size, PFS_BLOCK_SIZE);
  uint64_t logical_size = block_count * PFS_BLOCK_SIZE;
  uint64_t header_size = pfsc_header_span(block_count);
  uint64_t *offsets = NULL;
  pfsc_window_t *windows = NULL;
  pfsc_window_pool_t pool;
  pfs_vhash_writer_t vhash;
  pfsc_window_tuning_t tuning;
  int pool_started = 0;
  int vhash_open = 0;
  int rc = -1;
  uint64_t data_pos = header_size;
  int profile = pfs_compress_profile_normalize(compression_profile);
  pfsc_comp_config_t comp_config = {
    .profile = profile,
    .zlib_level = GC_PFSC_ZLIB_LEVEL,
    .threshold_gain = profile == PFS_COMPRESS_PROFILE_FAST ?
        0 : GC_PFSC_THRESHOLD_GAIN,
    .force_raw_exec = profile == PFS_COMPRESS_PROFILE_FAST ?
        1 : GC_PFSC_FORCE_RAW_EXEC,
    .entropy_skip = profile == PFS_COMPRESS_PROFILE_FAST,
    .fast_deflate = profile == PFS_COMPRESS_PROFILE_FAST,
    .raw_only = 0,
    .miniz_flags = tdefl_create_comp_flags_from_zip_params(
        PFSC_FAST_ZLIB_LEVEL, 15, MZ_DEFAULT_STRATEGY),
  };

  memset(&pool, 0, sizeof(pool));
  memset(&vhash, 0, sizeof(vhash));
  vhash.fd = -1;
  if(block_count == 0) return PFSC_WINDOW_RC_FALLBACK;

  offsets = calloc((size_t)(block_count + 1ULL), sizeof(*offsets));
  windows = calloc(PFSC_WINDOW_SLOTS, sizeof(*windows));
  if(!offsets || !windows ||
     pfsc_window_alloc_windows(windows, err, err_size) != 0) {
    PFSC_WINDOW_LOG("pfsc window unavailable: %s",
                    err && err[0] ? err : "allocation failed");
    rc = PFSC_WINDOW_RC_FALLBACK;
    goto done;
  }

  pfsc_window_tuning_init(&tuning, requested_workers);
  if(pfsc_window_pool_start(&pool, tuning.max_workers) != 0) {
    PFSC_WINDOW_LOG("pfsc window unavailable: start worker pool failed");
    rc = PFSC_WINDOW_RC_FALLBACK;
    goto done;
  }
  pool_started = 1;
  pool.nested = nested;
  pool.fd = fd;
  pool.file_start = file_start;
  pool.header_size = header_size;
  pool.vhash = &vhash;
  pool.comp_config = &comp_config;
  pool.comp_permits = pfsc_window_comp_permits(&tuning, tuning.read_permits);

  if(pfs_vhash_writer_open(&vhash, vhash_path, logical_size,
                           nested->image_size, block_count, nested_name,
                           nested_type, err, err_size) != 0) {
    goto done;
  }
  vhash_open = 1;
  if(lseek(fd, (off_t)(file_start + header_size), SEEK_SET) < 0) {
    set_err(err, err_size, "seek compressed payload: %s", strerror(errno));
    goto done;
  }
  if(lseek(vhash.fd, (off_t)PFS_VHASH_HEADER_SIZE, SEEK_SET) < 0) {
    set_err(err, err_size, "seek validation hash: %s", strerror(errno));
    goto done;
  }

  offsets[0] = header_size;
  atomic_store(&g_job.total_bytes,
               nested->image_size > (uint64_t)LONG_MAX ? LONG_MAX :
               (long)nested->image_size);
  atomic_store(&g_job.copied_bytes, 0);
  atomic_store(&g_job.compressed_output_bytes, 0);
  atomic_store(&g_job.raw_blocks, 0);
  atomic_store(&g_job.compressed_blocks, 0);
  atomic_store(&g_job.skipped_zlib_blocks, 0);
  atomic_store(&g_job.total_blocks,
               block_count > (uint64_t)LONG_MAX ? LONG_MAX :
               (long)block_count);
  atomic_store(&g_job.writer_wait_us, 0);
  atomic_store(&g_job.worker_wait_us, 0);
  atomic_store(&g_job.total_files,
               block_count > (uint64_t)INT_MAX ? INT_MAX : (int)block_count);
  atomic_store(&g_job.done_files, 0);
  char label[320];
  snprintf(label, sizeof(label), "Compressing %s",
           nested_name && nested_name[0] ? nested_name : "nested image");
  job_set_current(label);
  PFSC_WINDOW_LOG("pfsc window start blocks=%llu lanes=%d laneBytes=%llu workers=%d readPermits=%d writePermits=%d profile=%d zlibLevel=%d thresholdGain=%d fastDeflate=%d rawOnly=%d entropySkip=%d",
                  (unsigned long long)block_count, PFSC_WINDOW_LANES,
                  (unsigned long long)PFSC_WINDOW_LANE_SIZE,
                  tuning.max_workers, tuning.read_permits,
                  tuning.write_permits, comp_config.profile,
                  comp_config.zlib_level, comp_config.threshold_gain,
                  comp_config.fast_deflate,
                  comp_config.raw_only, comp_config.entropy_skip);
  PFSC_WINDOW_LOG("pfsc window mode=ordered-commit");

  uint64_t next_block = 0;
  uint64_t next_window_index = 0;
  pfsc_window_t *current = &windows[0];
  int flushed_first_raw_window = 0;
  uint64_t first_blocks = block_count > PFSC_WINDOW_BLOCKS ?
      PFSC_WINDOW_BLOCKS : block_count;
  if(pfsc_window_prepare(current, next_window_index++, next_block,
                         first_blocks) != 0) {
    set_err(err, err_size, "bad window state");
    goto done;
  }
  next_block += first_blocks;
  if(pfsc_window_read_window(&pool, current, &tuning, err, err_size) != 0) {
    goto done;
  }

  while(current) {
    if(pfsc_window_start_compression(&pool, current, err, err_size) != 0) {
      goto done;
    }

    if(comp_config.raw_only && !flushed_first_raw_window) {
      PFSC_WINDOW_LOG("pfsc window raw first flush index=%llu",
                      (unsigned long long)current->index);
      if(pfsc_window_write_window(&pool, current, &tuning, &data_pos,
                                  offsets, err, err_size) != 0) {
        goto done;
      }
      flushed_first_raw_window = 1;
      if(next_block >= block_count) {
        current = NULL;
        continue;
      }

      pfsc_window_t *next = current == &windows[0] ? &windows[1] : &windows[0];
      uint64_t nblocks = block_count - next_block;
      if(nblocks > PFSC_WINDOW_BLOCKS) nblocks = PFSC_WINDOW_BLOCKS;
      if(pfsc_window_prepare(next, next_window_index++, next_block,
                             nblocks) != 0) {
        set_err(err, err_size, "bad window state");
        goto done;
      }
      next_block += nblocks;
      if(pfsc_window_read_window(&pool, next, &tuning, err, err_size) != 0) {
        goto done;
      }
      current = next;
      continue;
    }

    pfsc_window_t *next = NULL;
    if(next_block < block_count) {
      next = current == &windows[0] ? &windows[1] : &windows[0];
      uint64_t nblocks = block_count - next_block;
      if(nblocks > PFSC_WINDOW_BLOCKS) nblocks = PFSC_WINDOW_BLOCKS;
      if(pfsc_window_prepare(next, next_window_index++, next_block,
                             nblocks) != 0) {
        set_err(err, err_size, "bad window state");
        goto done;
      }
      next_block += nblocks;
      if(pfsc_window_read_window(&pool, next, &tuning, err, err_size) != 0) {
        goto done;
      }
      if(pfsc_window_start_compression(&pool, next, err, err_size) != 0) {
        goto done;
      }
    }

    if(pfsc_window_write_window(&pool, current, &tuning, &data_pos,
                                offsets, err, err_size) != 0) {
      goto done;
    }
    current = next;
  }

  pfsc_window_pool_set_phase(&pool, PFSC_WINDOW_PHASE_IDLE, 0,
                             pfsc_window_comp_permits(&tuning, 0), 0);
  job_set_current("Writing PFSC header");
  if(write_pfsc_header(fd, file_start, header_size, logical_size, offsets,
                       block_count, err, err_size) != 0) {
    goto done;
  }
  if(pfs_vhash_writer_close(&vhash, err, err_size) != 0) {
    goto done;
  }
  vhash_open = 0;
  *stored_size = data_pos;
  PFSC_WINDOW_LOG("pfsc window done stored=%llu logical=%llu",
                  (unsigned long long)data_pos,
                  (unsigned long long)logical_size);
  rc = 0;

done:
  if(pool_started) pfsc_window_pool_destroy(&pool);
  if(vhash_open) pfs_vhash_writer_abort(&vhash);
  pfsc_window_free_windows(windows);
  free(windows);
  free(offsets);
  return rc;
}

static int
compress_nested_to_pfsc(int fd, uint64_t file_start, pfs_layout_t *nested,
                        int requested_workers, int delete_stream,
                        const char *nested_name,
                        int nested_type, const char *vhash_path,
                        const char *vhash_output_path, const char *tmp_path,
                        const pfs_app_info_t *stream_info, int format,
                        destructive_stream_ctx_t *stream,
                        const char *source_root, int *delete_started,
                        uint64_t *stored_size, int compression_profile,
                        char *err, size_t err_size) {
  uint64_t block_count = ceil_div_u64(nested->image_size, PFS_BLOCK_SIZE);
  uint64_t logical_size = block_count * PFS_BLOCK_SIZE;
  uint64_t header_size = pfsc_header_span(block_count);
  uint64_t *offsets = NULL;
  int worker_count = clamp_worker_count(requested_workers);
  int slot_count = 0;
  pfsc_slot_t *slots = NULL;
  pthread_t *threads = NULL;
  int workers_started = 0;
  int pool_initialized = 0;
  pfsc_pool_t pool;
  pfsc_output_buffer_t outbuf;
  pfs_vhash_writer_t vhash;
  virtual_reader_t vr;
  int vhash_open = 0;
  int rc = -1;
  uint64_t data_pos = header_size;
  int offsets_owned = 1;
  int profile = pfs_compress_profile_normalize(compression_profile);
  if(delete_stream) profile = PFS_COMPRESS_PROFILE_FAST;
  pfsc_comp_config_t comp_config = {
    .profile = profile,
    .zlib_level = GC_PFSC_ZLIB_LEVEL,
    .threshold_gain = profile == PFS_COMPRESS_PROFILE_FAST ?
        0 : GC_PFSC_THRESHOLD_GAIN,
    .force_raw_exec = profile == PFS_COMPRESS_PROFILE_FAST ?
        1 : GC_PFSC_FORCE_RAW_EXEC,
    .entropy_skip = !delete_stream && profile == PFS_COMPRESS_PROFILE_FAST,
    .fast_deflate = profile == PFS_COMPRESS_PROFILE_FAST,
    .miniz_flags = tdefl_create_comp_flags_from_zip_params(
        PFSC_FAST_ZLIB_LEVEL, 15, MZ_DEFAULT_STRATEGY),
  };

  memset(&pool, 0, sizeof(pool));
  memset(&outbuf, 0, sizeof(outbuf));
  memset(&vr, 0, sizeof(vr));
  memset(&vhash, 0, sizeof(vhash));
  vr.fd = -1;
  vr.open_seg = -1;
  vr.cache_seg = -1;
  vhash.fd = -1;

  if(delete_stream && !stream) {
    set_err(err, err_size, "stream delete requires destructive stream journal");
    errno = EINVAL;
    goto done;
  }
  if(delete_stream && worker_count > PFS_STREAM_COMPRESS_WORKERS) {
    worker_count = PFS_STREAM_COMPRESS_WORKERS;
  }
  if(!delete_stream) {
    int window_rc = compress_nested_to_pfsc_windowed(
        fd, file_start, nested, requested_workers, nested_name, nested_type,
        vhash_path, stored_size, compression_profile, err, err_size);
    if(window_rc == 0) {
      rc = 0;
      goto done;
    }
    if(window_rc != PFSC_WINDOW_RC_FALLBACK) {
      goto done;
    }
    if(err && err_size) err[0] = 0;
    PFSC_WINDOW_LOG("pfsc window falling back to legacy compressor");
  }
  pfsc_output_buffer_init(&outbuf);
  virtual_reader_init(&vr);

  if(block_count < (uint64_t)worker_count) worker_count = (int)block_count;
  if(worker_count < 1) worker_count = 1;
  slot_count = worker_count * PFSC_SLOTS_PER_WORKER;
  if(block_count < (uint64_t)slot_count) slot_count = (int)block_count;
  if(slot_count < 1) slot_count = 1;

  offsets = calloc((size_t)(block_count + 1), sizeof(*offsets));
  slots = calloc((size_t)slot_count, sizeof(*slots));
  threads = calloc((size_t)worker_count, sizeof(*threads));
  if(!offsets || !slots || !threads) {
    set_err(err, err_size, "out of memory");
    goto done;
  }
  if(stream && stream->offsets && stream->completed_blocks > 0) {
    if(stream->block_count != block_count ||
       stream->pfsc_header_size != header_size ||
       stream->nested_size != nested->image_size) {
      set_err(err, err_size, "stream journal does not match layout");
      goto done;
    }
    memcpy(offsets, stream->offsets,
           (size_t)(block_count + 1ULL) * sizeof(*offsets));
    if(stream->owns_offsets) {
      free(stream->offsets);
      stream->owns_offsets = 0;
    }
    stream->offsets = offsets;
    data_pos = offsets[stream->completed_blocks] ?
               offsets[stream->completed_blocks] : stream->data_pos;
  }
  for(int i = 0; i < slot_count; i++) {
    slots[i].raw = malloc((size_t)PFS_BLOCK_SIZE);
    slots[i].comp = malloc((size_t)PFS_BLOCK_SIZE);
    if(!slots[i].raw || !slots[i].comp) {
      set_err(err, err_size, "out of memory");
      goto done;
    }
  }
  if(pthread_mutex_init(&pool.lock, NULL) != 0 ||
     pthread_cond_init(&pool.cond, NULL) != 0) {
    set_err(err, err_size, "init compression tasks failed");
    goto done;
  }
  pool_initialized = 1;
  pool.slots = slots;
  pool.slot_count = slot_count;
  pool.comp_config = comp_config;

  for(int i = 0; i < worker_count; i++) {
    int trc = pthread_create(&threads[i], NULL, pfsc_worker_main, &pool);
    if(trc != 0) {
      set_err(err, err_size, "start compression task: %s", strerror(trc));
      goto done;
    }
    workers_started++;
  }

  offsets[0] = header_size;
  if(stream && stream->journal_fd < 0) {
    char journal_path[1024];
    if(!stream_info || !tmp_path || !vhash_output_path ||
       destructive_stream_journal_path_from_tmp(tmp_path, journal_path,
                                                sizeof(journal_path)) != 0 ||
       destructive_stream_create(journal_path, stream_info, tmp_path,
                                 vhash_path, vhash_output_path, format,
                                 nested, block_count, logical_size,
                                 header_size, offsets, stream,
                                 err, err_size) != 0) {
      goto done;
    }
  }
  if(stream) {
    vr.stream = stream;
  }
  if(stream && stream->completed_blocks > 0) {
    if(pfs_vhash_writer_resume_local(&vhash, vhash_path, block_count,
                                     err, err_size) != 0) {
      goto done;
    }
  } else {
    if(pfs_vhash_writer_open(&vhash, vhash_path, logical_size,
                             nested->image_size, block_count, nested_name,
                             nested_type, err, err_size) != 0) {
      goto done;
    }
  }
  vhash_open = 1;
  atomic_store(&g_job.total_bytes,
               nested->image_size > (uint64_t)LONG_MAX ? LONG_MAX :
               (long)nested->image_size);
  uint64_t start_block = stream ? stream->completed_blocks : 0;
  uint64_t start_copied = start_block * PFS_BLOCK_SIZE;
  if(start_copied > nested->image_size) start_copied = nested->image_size;
  atomic_store(&g_job.copied_bytes,
               start_copied > (uint64_t)LONG_MAX ? LONG_MAX :
               (long)start_copied);
  uint64_t start_compressed_output =
      data_pos > header_size ? data_pos - header_size : 0;
  atomic_store(&g_job.compressed_output_bytes,
               start_compressed_output > (uint64_t)LONG_MAX ? LONG_MAX :
               (long)start_compressed_output);
  atomic_store(&g_job.raw_blocks, 0);
  atomic_store(&g_job.compressed_blocks, 0);
  atomic_store(&g_job.skipped_zlib_blocks, 0);
  atomic_store(&g_job.total_blocks,
               block_count > (uint64_t)LONG_MAX ? LONG_MAX :
               (long)block_count);
  atomic_store(&g_job.writer_wait_us, 0);
  atomic_store(&g_job.worker_wait_us, 0);
  atomic_store(&g_job.total_files,
               block_count > (uint64_t)INT_MAX ? INT_MAX : (int)block_count);
  atomic_store(&g_job.done_files,
               start_block > (uint64_t)INT_MAX ? INT_MAX : (int)start_block);
  char label[320];
  snprintf(label, sizeof(label), "Compressing %s",
           nested_name && nested_name[0] ? nested_name : "nested image");
  job_set_current(label);
  if(stream && job_cancelled() && !stream->mutation_started) {
    set_err(err, err_size, "cancelled");
    errno = EINTR;
    goto done;
  }
  if(stream && destructive_stream_mark_mutation_started(stream, err, err_size) != 0) {
    goto done;
  }
  if(stream && destructive_stream_reverse_ahead_start(stream, err, err_size) != 0) {
    goto done;
  }

  uint64_t next_read = start_block;
  uint64_t next_write = start_block;
  while(next_write < block_count) {
    while(next_read < block_count &&
          next_read - next_write < (uint64_t)slot_count) {
      pfsc_slot_t *slot = &slots[next_read % (uint64_t)slot_count];

      pthread_mutex_lock(&pool.lock);
      while(slot->state != PFSC_SLOT_FREE && !pool.error) {
        pthread_cond_wait(&pool.cond, &pool.lock);
      }
      if(pool.error) {
        int saved = pool.error;
        pthread_mutex_unlock(&pool.lock);
        errno = saved;
        set_err(err, err_size, "compression task failed: %s", strerror(saved));
        goto done;
      }
      slot->state = PFSC_SLOT_FILLING;
      slot->index = next_read;
      slot->raw_len = (size_t)PFS_BLOCK_SIZE;
      if(next_read * PFS_BLOCK_SIZE < nested->image_size) {
        uint64_t remaining = nested->image_size - next_read * PFS_BLOCK_SIZE;
        if(remaining < PFS_BLOCK_SIZE) slot->raw_len = (size_t)remaining;
      }
      slot->comp_len = 0;
      slot->force_raw = 0;
      pthread_mutex_unlock(&pool.lock);

      if(job_cancelled() && (!stream || !stream->mutation_started)) {
        pthread_mutex_lock(&pool.lock);
        slot->state = PFSC_SLOT_FREE;
        pthread_cond_broadcast(&pool.cond);
        pthread_mutex_unlock(&pool.lock);
        set_err(err, err_size, "cancelled");
        errno = EINTR;
        goto done;
      } else if(job_cancelled() && stream) {
        atomic_store(&g_job.cancel, 0);
      }

      if(virtual_reader_read(nested, &vr, next_read * PFS_BLOCK_SIZE,
                             slot->raw, (size_t)PFS_BLOCK_SIZE,
                             err, err_size) != 0) {
        pthread_mutex_lock(&pool.lock);
        slot->state = PFSC_SLOT_FREE;
        pthread_cond_broadcast(&pool.cond);
        pthread_mutex_unlock(&pool.lock);
        goto done;
      }
      slot->force_raw = comp_config.force_raw_exec ?
          layout_block_overlaps_executable_file(nested,
                                                next_read * PFS_BLOCK_SIZE) :
          0;
      pthread_mutex_lock(&pool.lock);
      slot->state = PFSC_SLOT_READY;
      pthread_cond_broadcast(&pool.cond);
      pthread_mutex_unlock(&pool.lock);
      next_read++;
    }

    pfsc_slot_t *slot = &slots[next_write % (uint64_t)slot_count];
    pthread_mutex_lock(&pool.lock);
    while((slot->state != PFSC_SLOT_DONE || slot->index != next_write) &&
          !pool.error) {
      uint64_t wait_started = monotonic_us();
      pthread_cond_wait(&pool.cond, &pool.lock);
      job_add_wait_us(&g_job.writer_wait_us, wait_started);
    }
    if(pool.error) {
      int saved = pool.error;
      pthread_mutex_unlock(&pool.lock);
      errno = saved;
      set_err(err, err_size, "compression task failed: %s", strerror(saved));
      goto done;
    }
    pthread_mutex_unlock(&pool.lock);

    const void *chosen = slot->raw;
    size_t chosen_len = (size_t)PFS_BLOCK_SIZE;
    int block_compressed = 0;
    if(slot->comp_len > 0 && slot->comp_len < (size_t)PFS_BLOCK_SIZE) {
      chosen = slot->comp;
      chosen_len = slot->comp_len;
      block_compressed = 1;
    }
    if(pfsc_output_buffer_write(fd, file_start, &outbuf, data_pos,
                                chosen, chosen_len, err, err_size) != 0) {
      goto done;
    }
    if(pfs_vhash_writer_write_hash(&vhash, next_write, slot->hash,
                                   err, err_size) != 0) {
      goto done;
    }
    data_pos += chosen_len;
    offsets[next_write + 1] = data_pos;
    uint64_t compressed_output = data_pos > header_size ? data_pos - header_size : 0;
    atomic_store(&g_job.compressed_output_bytes,
                 compressed_output > (uint64_t)LONG_MAX ? LONG_MAX :
                 (long)compressed_output);
    if(block_compressed) atomic_fetch_add(&g_job.compressed_blocks, 1);
    else atomic_fetch_add(&g_job.raw_blocks, 1);

    pthread_mutex_lock(&pool.lock);
    slot->state = PFSC_SLOT_FREE;
    pthread_cond_broadcast(&pool.cond);
    pthread_mutex_unlock(&pool.lock);

    uint64_t copied = (next_write + 1) * PFS_BLOCK_SIZE;
    if(copied > nested->image_size) copied = nested->image_size;
    atomic_store(&g_job.copied_bytes,
                 copied > (uint64_t)LONG_MAX ? LONG_MAX : (long)copied);
    atomic_store(&g_job.done_files,
                 next_write + 1 > (uint64_t)INT_MAX ? INT_MAX :
                 (int)(next_write + 1));
    if(stream) {
      stream->completed_blocks = next_write + 1;
      if(destructive_stream_truncate_committed(fd, file_start, stream, &vr,
                                               &outbuf, &vhash,
                                               next_write + 1, data_pos, 0,
                                               err, err_size) != 0) {
        goto done;
      }
      if(delete_started && stream->mutation_started) *delete_started = 1;
    }
    next_write++;
  }

  pfsc_pool_stop(&pool);
  for(int i = 0; i < workers_started; i++) pthread_join(threads[i], NULL);
  workers_started = 0;

  job_set_current("Writing PFSC header");
  if(stream &&
     destructive_stream_truncate_committed(fd, file_start, stream, &vr,
                                          &outbuf, &vhash, block_count,
                                          data_pos, 1, err, err_size) != 0) {
    goto done;
  }
  if(pfsc_output_buffer_flush(fd, file_start, &outbuf, err, err_size) != 0) {
    goto done;
  }
  if(write_pfsc_header(fd, file_start, header_size, logical_size, offsets,
                       block_count, err, err_size) != 0) {
    goto done;
  }
  if(pfs_vhash_writer_close(&vhash, err, err_size) != 0) {
    goto done;
  }
  vhash_open = 0;
  if(stream) {
    stream->compression_complete = 1;
    stream->data_pos = data_pos;
    if(destructive_stream_write_header(stream, err, err_size) != 0 ||
       destructive_stream_sync(stream, err, err_size) != 0) {
      goto done;
    }
  }
  *stored_size = data_pos;
  rc = 0;

done:
  if(stream) destructive_stream_reverse_ahead_stop(stream);
  if(vhash_open) {
    if(stream && stream->mutation_started) {
      close(vhash.fd);
      vhash.fd = -1;
    } else {
      pfs_vhash_writer_abort(&vhash);
    }
  }
  if(pool_initialized) pfsc_pool_stop(&pool);
  for(int i = 0; i < workers_started; i++) pthread_join(threads[i], NULL);
  if(pool_initialized) {
    pthread_cond_destroy(&pool.cond);
    pthread_mutex_destroy(&pool.lock);
  }
  virtual_reader_free(&vr);
  pfsc_output_buffer_free(&outbuf);
  if(offsets_owned) free(offsets);
  free(threads);
  pfsc_free_slots(slots, slot_count);
  return rc;
}

static int
write_blob_padded(int fd, uint64_t block, const unsigned char *blob,
                  size_t blob_size, char *err, size_t err_size) {
  if(blob_size == 0) return 0;
  if(pwrite_all_local(fd, blob, blob_size, (off_t)(block * PFS_BLOCK_SIZE)) != 0) {
    set_err(err, err_size, "write PFS metadata: %s", strerror(errno));
    return -1;
  }
  return 0;
}

static int
write_outer_pfs_metadata(int fd, uint64_t nested_size, uint64_t stored_size,
                         const char *nested_name,
                         char *err, size_t err_size) {
  const uint64_t inode_count = 4;
  const uint64_t inode_block_count = 1;
  const uint64_t superroot_block = 2;
  const uint64_t fpt_block = 3;
  const uint64_t uroot_block = 5;
  const uint64_t file_block = 6;
  uint64_t file_blocks = ceil_div_u64(stored_size, PFS_BLOCK_SIZE);
  if(file_blocks == 0) file_blocks = 1;
  uint64_t final_ndblock = file_block + file_blocks;
  time_t now = time(NULL);
  unsigned char *header = calloc(1, (size_t)PFS_BLOCK_SIZE);
  unsigned char *inode_block = calloc(1, (size_t)PFS_BLOCK_SIZE);
  byte_buf_t superroot = {0};
  byte_buf_t root = {0};
  unsigned char fpt[8];
  int rc = -1;

  if(!header || !inode_block) {
    set_err(err, err_size, "out of memory");
    goto done;
  }
  if(ftruncate(fd, (off_t)(final_ndblock * PFS_BLOCK_SIZE)) != 0) {
    set_err(err, err_size, "truncate output: %s", strerror(errno));
    goto done;
  }

  write_header_blob(header, inode_count, inode_block_count, final_ndblock, now);
  write_inode(inode_block + 0 * PFS_INODE_SIZE,
              PFS_INODE_MODE_DIR | PFS_INODE_RWX_ALL, 1,
              PFS_INODE_FLAG_INTERNAL | PFS_INODE_FLAG_READONLY,
              PFS_BLOCK_SIZE, PFS_BLOCK_SIZE, 1, (int32_t)superroot_block, 0,
              now);
  write_inode(inode_block + 1 * PFS_INODE_SIZE,
              PFS_INODE_MODE_FILE | PFS_INODE_RWX_ALL, 1,
              PFS_INODE_FLAG_INTERNAL | PFS_INODE_FLAG_READONLY,
              sizeof(fpt), sizeof(fpt), 1, (int32_t)fpt_block, 1, now);
  write_inode(inode_block + 2 * PFS_INODE_SIZE,
              PFS_INODE_MODE_DIR | PFS_INODE_RWX_ALL, 3,
              PFS_INODE_FLAG_READONLY,
              PFS_BLOCK_SIZE, PFS_BLOCK_SIZE, 1, (int32_t)uroot_block, 1, now);
  write_inode(inode_block + 3 * PFS_INODE_SIZE,
              PFS_INODE_MODE_FILE | PFS_INODE_RWX_ALL, 1,
              PFS_INODE_FLAG_READONLY | PFS_INODE_FLAG_COMPRESSED,
              stored_size, nested_size, (uint32_t)file_blocks,
              (int32_t)file_block, 1, now);

  char nested_path[320];
  int path_len = snprintf(nested_path, sizeof(nested_path), "/%s",
                          nested_name && nested_name[0]
                            ? nested_name
                            : "pfs_image.dat");
  if(path_len < 0 || (size_t)path_len >= sizeof(nested_path)) {
    set_err(err, err_size, "nested image name too long");
    goto done;
  }
  uint32_t hash = pfs_hash_path(nested_path);
  le32(fpt + 0, hash);
  le32(fpt + 4, 3);

  if(append_dirent(&superroot, 1, PFS_DIRENT_TYPE_FILE,
                   "flat_path_table") != 0 ||
     append_dirent(&superroot, 2, PFS_DIRENT_TYPE_DIRECTORY,
                   "uroot") != 0 ||
     append_dirent(&root, 2, PFS_DIRENT_TYPE_DOT, ".") != 0 ||
     append_dirent(&root, 2, PFS_DIRENT_TYPE_DOTDOT, "..") != 0 ||
     append_dirent(&root, 3, PFS_DIRENT_TYPE_FILE,
                   nested_name && nested_name[0]
                     ? nested_name
                     : "pfs_image.dat") != 0) {
    set_err(err, err_size, "out of memory");
    goto done;
  }

  if(pwrite_all_local(fd, header, (size_t)PFS_BLOCK_SIZE, 0) != 0 ||
     pwrite_all_local(fd, inode_block, (size_t)PFS_BLOCK_SIZE,
                      (off_t)PFS_BLOCK_SIZE) != 0 ||
     write_blob_padded(fd, superroot_block, superroot.data, superroot.len,
                       err, err_size) != 0 ||
     write_blob_padded(fd, fpt_block, fpt, sizeof(fpt),
                       err, err_size) != 0 ||
     write_blob_padded(fd, uroot_block, root.data, root.len,
                       err, err_size) != 0) {
    if(!err[0]) set_err(err, err_size, "write PFS metadata: %s", strerror(errno));
    goto done;
  }
  rc = 0;

done:
  free(header);
  free(inode_block);
  free(superroot.data);
  free(root.data);
  return rc;
}

static int
read_file_limited(const char *path, char **out, size_t *out_size,
                  size_t max_size) {
  FILE *f = fopen(path, "rb");
  if(!f) return -1;
  char *buf = malloc(max_size + 1);
  if(!buf) {
    fclose(f);
    errno = ENOMEM;
    return -1;
  }
  size_t n = fread(buf, 1, max_size, f);
  int ferr = ferror(f);
  fclose(f);
  if(ferr) {
    free(buf);
    errno = EIO;
    return -1;
  }
  buf[n] = 0;
  *out = buf;
  *out_size = n;
  return 0;
}

static int
json_find_string_value(const char *json, const char *key,
                       char *out, size_t out_size) {
  char needle[80];
  snprintf(needle, sizeof(needle), "\"%s\"", key);
  const char *p = strstr(json, needle);
  if(!p) return 0;
  p += strlen(needle);
  while(*p && isspace((unsigned char)*p)) p++;
  if(*p != ':') return 0;
  p++;
  while(*p && isspace((unsigned char)*p)) p++;
  if(*p != '"') return 0;
  p++;
  size_t pos = 0;
  while(*p && *p != '"' && pos + 1 < out_size) {
    if(*p == '\\') return 0;
    out[pos++] = *p++;
  }
  if(*p != '"' || pos == 0) return 0;
  out[pos] = 0;
  return 1;
}

static int
read_title_id(const char *app_path, char *title, size_t title_size,
              char *err, size_t err_size) {
  char param[1024];
  char sce_sys[1024];
  char *json = NULL;
  size_t json_size = 0;
  if(join_abs(sce_sys, sizeof(sce_sys), app_path, "sce_sys") != 0 ||
     join_abs(param, sizeof(param), sce_sys, "param.json") != 0) {
    set_err(err, err_size, "param path too long");
    return -1;
  }
  if(read_file_limited(param, &json, &json_size, 128 * 1024) != 0) {
    set_err(err, err_size, "missing sce_sys/param.json");
    return -1;
  }
  (void)json_size;
  int ok = json_find_string_value(json, "titleId", title, title_size) ||
           json_find_string_value(json, "title_id", title, title_size);
  free(json);
  if(!ok || !title_id_safe(title)) {
    set_err(err, err_size, "param.json is missing a valid titleId");
    return -1;
  }
  return 0;
}

static int
has_root_boot_file(const char *app_path) {
  char p[1024];
  struct stat st;
  if(join_abs(p, sizeof(p), app_path, "eboot.bin") == 0 &&
     stat(p, &st) == 0 && S_ISREG(st.st_mode)) {
    return 1;
  }
  if(join_abs(p, sizeof(p), app_path, "iboot.bin") == 0 &&
     stat(p, &st) == 0 && S_ISREG(st.st_mode)) {
    return 1;
  }
  return 0;
}

static int
find_title_id_in_basename(const char *base, char *title, size_t title_size) {
  if(!base || !title || title_size < 10) return 0;
  size_t len = strlen(base);
  for(size_t i = 0; i + 9 <= len; i++) {
    if(ascii_toupper((unsigned char)base[i]) != 'P' ||
       ascii_toupper((unsigned char)base[i + 1]) != 'P' ||
       ascii_toupper((unsigned char)base[i + 2]) != 'S' ||
       ascii_toupper((unsigned char)base[i + 3]) != 'A') {
      continue;
    }
    int ok = 1;
    for(size_t j = 4; j < 9; j++) {
      if(!isdigit((unsigned char)base[i + j])) {
        ok = 0;
        break;
      }
    }
    if(!ok) continue;
    snprintf(title, title_size, "PPSA%c%c%c%c%c",
             base[i + 4], base[i + 5], base[i + 6],
             base[i + 7], base[i + 8]);
    return 1;
  }
  return 0;
}

static int
image_extension_type(const char *base, size_t *ext_len) {
  if(ends_with_ci(base, ".exfat")) {
    if(ext_len) *ext_len = strlen(".exfat");
    return PFS_NESTED_EXFAT;
  }
  if(ends_with_ci(base, ".ffpfs")) {
    if(ext_len) *ext_len = strlen(".ffpfs");
    return PFS_NESTED_PFS;
  }
  if(ends_with_ci(base, ".pfs")) {
    if(ext_len) *ext_len = strlen(".pfs");
    return PFS_NESTED_PFS;
  }
  if(ends_with_ci(base, ".dat")) {
    if(ext_len) *ext_len = strlen(".dat");
    return PFS_NESTED_PFS;
  }
  if(ext_len) *ext_len = 0;
  return PFS_NESTED_UNKNOWN;
}

static int
validate_pfs_image_file(const char *path, uint64_t file_size,
                        char *err, size_t err_size) {
  unsigned char header[0x48];
  int fd = -1;
  int rc = -1;
  if(file_size < PFS_BLOCK_SIZE || (file_size % PFS_BLOCK_SIZE) != 0) {
    set_err(err, err_size, "PFS image size is not 64 KiB aligned");
    errno = EINVAL;
    return -1;
  }
  fd = open(path, O_RDONLY);
  if(fd < 0) {
    set_err(err, err_size, "open image: %s", strerror(errno));
    return -1;
  }
  if(read_exact_at(fd, header, sizeof(header), 0) != 0) {
    set_err(err, err_size, "read PFS image header: %s", strerror(errno));
    goto done;
  }
  uint64_t final_ndblock = rd64(header + 0x38);
  if(rd64(header + 0x00) != (uint64_t)PFS_VERSION_PS5 ||
     rd64(header + 0x08) != (uint64_t)PFS_MAGIC ||
     rd32(header + 0x20) != (uint32_t)PFS_BLOCK_SIZE ||
     final_ndblock == 0 ||
     final_ndblock > UINT64_MAX / PFS_BLOCK_SIZE ||
     final_ndblock * PFS_BLOCK_SIZE > file_size) {
    set_err(err, err_size, "not a valid PS5 PFS image");
    errno = EINVAL;
    goto done;
  }
  rc = 0;

done:
  close(fd);
  return rc;
}

static int
validate_exfat_image_file(const char *path, uint64_t file_size,
                          char *err, size_t err_size) {
  unsigned char boot[512];
  int fd = -1;
  int rc = -1;
  if(file_size < sizeof(boot)) {
    set_err(err, err_size, "exFAT image is too small");
    errno = EINVAL;
    return -1;
  }
  fd = open(path, O_RDONLY);
  if(fd < 0) {
    set_err(err, err_size, "open image: %s", strerror(errno));
    return -1;
  }
  if(read_exact_at(fd, boot, sizeof(boot), 0) != 0) {
    set_err(err, err_size, "read exFAT boot sector: %s", strerror(errno));
    goto done;
  }
  uint8_t sector_shift = boot[108];
  uint8_t cluster_shift = boot[109];
  uint8_t fat_count = boot[110];
  uint64_t volume_length = rd64(boot + 72);
  uint32_t fat_offset = rd32(boot + 80);
  uint32_t fat_length = rd32(boot + 84);
  uint32_t heap_offset = rd32(boot + 88);
  uint32_t cluster_count = rd32(boot + 92);
  uint32_t root_cluster = rd32(boot + 96);
  if(memcmp(boot + 3, "EXFAT   ", 8) != 0 ||
     boot[510] != 0x55 || boot[511] != 0xaa ||
     sector_shift < 9 || sector_shift > 12 ||
     cluster_shift > 25 || sector_shift + cluster_shift > 32 ||
     fat_count == 0 || fat_count > 2 ||
     volume_length == 0 || fat_offset == 0 || fat_length == 0 ||
     heap_offset == 0 || cluster_count == 0 || root_cluster < 2) {
    set_err(err, err_size, "not a valid exFAT image");
    errno = EINVAL;
    goto done;
  }
  uint64_t sector_size = 1ULL << sector_shift;
  if(volume_length > file_size / sector_size) {
    set_err(err, err_size, "exFAT volume length exceeds image size");
    errno = EINVAL;
    goto done;
  }
  rc = 0;

done:
  close(fd);
  return rc;
}

int
pfs_app_probe(const char *path, pfs_app_info_t *info,
              char *err, size_t err_size) {
  char clean[1024], parent[1024], base[256];
  struct stat st;
  pfs_app_info_t local;
  if(!info) info = &local;
  memset(info, 0, sizeof(*info));

  if(normalize_app_path(path, clean, sizeof(clean)) != 0) {
    set_err(err, err_size, "bad path");
    return -1;
  }
  if(stat(clean, &st) != 0 || !S_ISDIR(st.st_mode)) {
    set_err(err, err_size, "not a folder");
    return -1;
  }
  if(path_parent_base(clean, parent, sizeof(parent), base, sizeof(base)) != 0) {
    set_err(err, err_size, "bad app path");
    return -1;
  }
  if(read_title_id(clean, info->title_id, sizeof(info->title_id),
                   err, err_size) != 0) {
    return -1;
  }
  if(!has_root_boot_file(clean)) {
    set_err(err, err_size, "missing root eboot.bin or iboot.bin");
    return -1;
  }

  snprintf(info->source_path, sizeof(info->source_path), "%s", clean);
  info->source_type = PFS_COMPRESS_SOURCE_APP;
  info->nested_type = PFS_NESTED_PFS;
  int n = snprintf(info->output_path, sizeof(info->output_path), "%s%s%s.ffpfsc",
                   parent, parent[1] ? "/" : "", info->title_id);
  if(n < 0 || (size_t)n >= sizeof(info->output_path)) {
    set_err(err, err_size, "output path too long");
    return -1;
  }
  info->output_exists = stat(info->output_path, &st) == 0;
  return 0;
}

int
pfs_image_probe(const char *path, pfs_app_info_t *info,
                char *err, size_t err_size) {
  char clean[1024], parent[1024], base[256], stem[256];
  struct stat st;
  pfs_app_info_t local;
  size_t ext_len = 0;
  int nested_type;
  if(!info) info = &local;
  memset(info, 0, sizeof(*info));

  if(normalize_app_path(path, clean, sizeof(clean)) != 0) {
    set_err(err, err_size, "bad path");
    return -1;
  }
  if(stat(clean, &st) != 0 || !S_ISREG(st.st_mode) || st.st_size <= 0) {
    set_err(err, err_size, "not an image file");
    return -1;
  }
  if(path_parent_base(clean, parent, sizeof(parent), base, sizeof(base)) != 0 ||
     !path_segment_supported(base)) {
    set_err(err, err_size, "bad image path");
    return -1;
  }
  nested_type = image_extension_type(base, &ext_len);
  if(nested_type == PFS_NESTED_UNKNOWN || ext_len == 0 ||
     strlen(base) <= ext_len || strlen(base) - ext_len >= sizeof(stem)) {
    set_err(err, err_size, "unsupported image extension");
    return -1;
  }
  if(!find_title_id_in_basename(base, info->title_id,
                                sizeof(info->title_id))) {
    set_err(err, err_size, "image name is missing PPSA title id");
    return -1;
  }

  uint64_t file_size = (uint64_t)st.st_size;
  if(nested_type == PFS_NESTED_EXFAT) {
    if(validate_exfat_image_file(clean, file_size, err, err_size) != 0) {
      return -1;
    }
  } else if(validate_pfs_image_file(clean, file_size, err, err_size) != 0) {
    return -1;
  }

  size_t stem_len = strlen(base) - ext_len;
  memcpy(stem, base, stem_len);
  stem[stem_len] = 0;
  snprintf(info->source_path, sizeof(info->source_path), "%s", clean);
  snprintf(info->nested_name, sizeof(info->nested_name), "%s",
           nested_type == PFS_NESTED_PFS ? "pfs_image.dat" : base);
  info->nested_size = file_size;
  info->nested_type = nested_type;
  info->format = nested_type == PFS_NESTED_EXFAT ?
                 PFS_COMPRESS_FORMAT_EXFAT : PFS_COMPRESS_FORMAT_PFS;
  info->source_type = PFS_COMPRESS_SOURCE_IMAGE;
  int n = snprintf(info->output_path, sizeof(info->output_path),
                   "%s%s%s.ffpfsc", parent, parent[1] ? "/" : "", stem);
  if(n < 0 || (size_t)n >= sizeof(info->output_path)) {
    set_err(err, err_size, "output path too long");
    return -1;
  }
  info->output_exists = stat(info->output_path, &st) == 0;
  return 0;
}

int
pfs_compress_probe(const char *path, pfs_app_info_t *info,
                   char *err, size_t err_size) {
  char clean[1024];
  struct stat st;
  if(normalize_app_path(path, clean, sizeof(clean)) != 0) {
    set_err(err, err_size, "bad path");
    return -1;
  }
  if(stat(clean, &st) != 0) {
    set_err(err, err_size, "%s", strerror(errno));
    return -1;
  }
  if(S_ISDIR(st.st_mode)) {
    return pfs_app_probe(clean, info, err, err_size);
  }
  if(S_ISREG(st.st_mode)) {
    return pfs_image_probe(clean, info, err, err_size);
  }
  set_err(err, err_size, "not a compressible app folder or image file");
  errno = EINVAL;
  return -1;
}

static int
build_layout_from_image_file(const pfs_app_info_t *info, pfs_layout_t *l,
                             char *err, size_t err_size) {
  if(!info || !info->source_path[0] || info->nested_size == 0) {
    set_err(err, err_size, "bad source image");
    errno = EINVAL;
    return -1;
  }
  l->image_size = info->nested_size;
  if(l->image_size == 0) {
    set_err(err, err_size, "bad source image size");
    errno = EINVAL;
    return -1;
  }
  if(layout_add_segment(l, 0, info->nested_size, PFS_SEG_FILE,
                        NULL, info->source_path) != 0) {
    set_err(err, err_size, "out of memory");
    errno = ENOMEM;
    return -1;
  }
  return 0;
}

static int
pfs_compress_probed_to_ffpfsc_opts(pfs_app_info_t *info, int overwrite,
                                   int workers, int format,
                                   int delete_policy,
                                   int compression_profile,
                                   const pfs_stream_options_t *stream_opts,
                                   char *err, size_t err_size) {
  pfs_layout_t nested = {0};
  char tmp_path[1024];
  char vhash_tmp_path[1024];
  char vhash_output_path[1024];
  char legacy_tmp_path[1024];
  char legacy_vhash_tmp_path[1024];
  int fd = -1;
  int rc = -1;
  int delete_started = 0;
  uint64_t stored_size = 0;
  const uint64_t outer_file_start = 6 * PFS_BLOCK_SIZE;
  int worker_count = clamp_worker_count(workers);
  destructive_stream_ctx_t stream_ctx;
  int stream_ctx_initialized = 0;
  int destructive_stream = 0;

  if(!info || !info->source_path[0]) {
    set_err(err, err_size, "bad compression source");
    errno = EINVAL;
    return -1;
  }
  if(info->source_type != PFS_COMPRESS_SOURCE_APP &&
     info->source_type != PFS_COMPRESS_SOURCE_IMAGE) {
    set_err(err, err_size, "unsupported compression source");
    errno = EINVAL;
    return -1;
  }
  if(info->source_type == PFS_COMPRESS_SOURCE_APP &&
     format != PFS_COMPRESS_FORMAT_PFS &&
     format != PFS_COMPRESS_FORMAT_EXFAT) {
    set_err(err, err_size, "unsupported compression format");
    errno = EINVAL;
    return -1;
  }
  if(info->source_type == PFS_COMPRESS_SOURCE_IMAGE) {
    format = info->format;
    if(delete_policy == PFS_DELETE_STREAM) {
      set_err(err, err_size, "image stream delete is not supported");
      errno = EINVAL;
      return -1;
    }
  }
  if(delete_policy != PFS_DELETE_KEEP &&
     delete_policy != PFS_DELETE_AFTER &&
     delete_policy != PFS_DELETE_STREAM) {
    set_err(err, err_size, "unsupported delete policy");
    errno = EINVAL;
    return -1;
  }
  info->format = format;
  info->delete_policy = delete_policy;
  info->compression_profile =
      pfs_compress_profile_normalize(compression_profile);
  if(info->source_type == PFS_COMPRESS_SOURCE_APP &&
     delete_policy == PFS_DELETE_STREAM) {
    info->compression_profile = PFS_COMPRESS_PROFILE_FAST;
    pfs_stream_options_t normalized;
    stream_options_normalize(stream_opts, &normalized);
    info->stream_budget_bytes = normalized.budget_bytes;
    info->stream_reserve_bytes = normalized.reserve_bytes;
  }
  if(info->source_type == PFS_COMPRESS_SOURCE_APP) {
    info->nested_type = format == PFS_COMPRESS_FORMAT_EXFAT ?
                        PFS_NESTED_EXFAT : PFS_NESTED_PFS;
    snprintf(info->nested_name, sizeof(info->nested_name), "%s",
             format == PFS_COMPRESS_FORMAT_EXFAT ? info->title_id : "pfs_image.dat");
    if(format == PFS_COMPRESS_FORMAT_EXFAT) {
      size_t used = strlen(info->nested_name);
      if(used + strlen(".exfat") >= sizeof(info->nested_name)) {
        set_err(err, err_size, "nested image name too long");
        errno = ENAMETOOLONG;
        return -1;
      }
      snprintf(info->nested_name + used, sizeof(info->nested_name) - used,
               ".exfat");
    }
  }
  if(info->output_exists && !overwrite) {
    set_err(err, err_size, "output exists");
    errno = EEXIST;
    return -2;
  }

  if(pfs_compress_temp_output_path(info->output_path, tmp_path,
                                   sizeof(tmp_path)) != 0) {
    set_err(err, err_size, "temporary output path too long");
    return -1;
  }
  if(snprintf(legacy_tmp_path, sizeof(legacy_tmp_path), "%s.tmp",
              info->output_path) >= (int)sizeof(legacy_tmp_path)) {
    set_err(err, err_size, "legacy temporary output path too long");
    return -1;
  }
  if(pfs_vhash_sidecar_path(tmp_path, vhash_tmp_path,
                            sizeof(vhash_tmp_path)) != 0 ||
     pfs_vhash_sidecar_path(info->output_path, vhash_output_path,
                            sizeof(vhash_output_path)) != 0 ||
     pfs_vhash_sidecar_path(legacy_tmp_path, legacy_vhash_tmp_path,
                            sizeof(legacy_vhash_tmp_path)) != 0) {
    set_err(err, err_size, "validation hash path too long");
    return -1;
  }
  unlink(legacy_tmp_path);
  unlink(legacy_vhash_tmp_path);

  if(info->source_type == PFS_COMPRESS_SOURCE_IMAGE) {
    job_set_current("Opening existing nested image");
    if(build_layout_from_image_file(info, &nested, err, err_size) != 0) {
      goto done;
    }
  } else if(format == PFS_COMPRESS_FORMAT_EXFAT) {
    job_set_current("Building nested exFAT layout");
    if(delete_policy == PFS_DELETE_STREAM) {
      if(build_exfat_layout_from_files_stream(
             info->source_path, info->title_id, &nested, stream_opts, info,
             err, err_size) != 0) {
        goto done;
      }
    } else if(build_exfat_layout_from_files(info->source_path, info->title_id,
                                            &nested, err, err_size) != 0) {
      goto done;
    }
  } else {
    job_set_current("Building nested PFS layout");
    if(delete_policy == PFS_DELETE_STREAM) {
      if(build_layout_from_files_stream(info->source_path, &nested,
                                        stream_opts, info,
                                        err, err_size) != 0) {
        goto done;
      }
    } else if(build_layout_from_files(info->source_path, &nested,
                                      err, err_size) != 0) {
      goto done;
    }
  }
  destructive_stream = info->source_type == PFS_COMPRESS_SOURCE_APP &&
                       delete_policy == PFS_DELETE_STREAM;
  if(destructive_stream) {
    destructive_stream_init(&stream_ctx);
    stream_ctx_initialized = 1;
  }

  fd = open(tmp_path, O_WRONLY | O_CREAT | O_TRUNC, 0666);
  if(fd < 0) {
    set_err(err, err_size, "open output: %s", strerror(errno));
    goto done;
  }
  if(ftruncate(fd, (off_t)outer_file_start) != 0) {
    set_err(err, err_size, "reserve output: %s", strerror(errno));
    goto done;
  }

  if(compress_nested_to_pfsc(fd, outer_file_start, &nested, worker_count,
                             destructive_stream,
                             info->nested_name, info->nested_type,
                             vhash_tmp_path, vhash_output_path, tmp_path,
                             info, format,
                             destructive_stream ? &stream_ctx : NULL,
                             info->source_path,
                             &delete_started, &stored_size,
                             info->compression_profile,
                             err, err_size) != 0) {
    goto done;
  }
  info->nested_size = nested.image_size;
  info->stored_size = stored_size;

  if(job_cancelled() && (!destructive_stream || !stream_ctx.mutation_started)) {
    set_err(err, err_size, "cancelled");
    errno = EINTR;
    goto done;
  } else if(job_cancelled() && destructive_stream) {
    atomic_store(&g_job.cancel, 0);
  }

  job_set_current("Finalizing .ffpfsc");
  if(write_outer_pfs_metadata(fd, nested.image_size, stored_size,
                              info->nested_name, err, err_size) != 0) {
    goto done;
  }
  if(sync_completed_output_file(fd, err, err_size) != 0) {
    goto done;
  }
  if(close(fd) != 0) {
    set_err(err, err_size, "close output: %s", strerror(errno));
    fd = -1;
    goto done;
  }
  fd = -1;

  if(info->source_type == PFS_COMPRESS_SOURCE_APP &&
     delete_policy != PFS_DELETE_KEEP) {
    job_set_current("Removing source app folder");
    if(remove_tree_local(info->source_path) != 0 && errno != ENOENT) {
      set_err(err, err_size, "remove source app folder: %s", strerror(errno));
      goto done;
    }
  } else if(info->source_type == PFS_COMPRESS_SOURCE_IMAGE &&
            delete_policy == PFS_DELETE_AFTER) {
    job_set_current("Removing source image");
    if(unlink(info->source_path) != 0 && errno != ENOENT) {
      set_err(err, err_size, "remove source image: %s", strerror(errno));
      goto done;
    }
  }

  if(rename(tmp_path, info->output_path) != 0) {
    set_err(err, err_size, "rename output: %s", strerror(errno));
    goto done;
  }
  destructive_stream_fsync_parent_best_effort(info->output_path);
  info->output_exists = 1;
  if(rename(vhash_tmp_path, vhash_output_path) != 0) {
    set_err(err, err_size, "rename validation hash: %s", strerror(errno));
    unlink(vhash_output_path);
    goto done;
  }
  destructive_stream_fsync_parent_best_effort(vhash_output_path);
  if(destructive_stream) {
    stream_ctx.output_finalized = 1;
    if(destructive_stream_write_header(&stream_ctx, err, err_size) != 0 ||
       destructive_stream_sync(&stream_ctx, err, err_size) != 0) {
      goto done;
    }
  }

  if(destructive_stream && stream_ctx.journal_path[0]) {
    destructive_stream_remove_reverse_dir(&stream_ctx);
    unlink(stream_ctx.journal_path);
  }
  rc = 0;

done:
  if(fd >= 0) close(fd);
  int destructive_started = stream_ctx_initialized && stream_ctx.mutation_started;
  if(rc != 0 &&
     (delete_policy != PFS_DELETE_STREAM ||
      (!delete_started && !destructive_started))) {
    unlink(tmp_path);
    unlink(vhash_tmp_path);
    unlink(legacy_tmp_path);
    unlink(legacy_vhash_tmp_path);
    if(stream_ctx_initialized && stream_ctx.journal_path[0]) {
      destructive_stream_remove_reverse_dir(&stream_ctx);
      unlink(stream_ctx.journal_path);
    }
  }
  if(stream_ctx_initialized) destructive_stream_free(&stream_ctx);
  layout_free(&nested);
  return rc;
}

int
pfs_compress_resume_stream_journal_profile(const char *journal_path,
                                           int workers,
                                           int compression_profile,
                                           pfs_app_info_t *info,
                                           char *err, size_t err_size) {
  destructive_stream_ctx_t stream_ctx;
  pfs_layout_t nested = {0};
  int fd = -1;
  int rc = -1;
  int delete_started = 1;
  uint64_t stored_size = 0;
  const uint64_t outer_file_start = 6 * PFS_BLOCK_SIZE;
  int worker_count = clamp_worker_count(workers);
  pfs_app_info_t local_info;
  if(!info) info = &local_info;
  memset(info, 0, sizeof(*info));
  destructive_stream_init(&stream_ctx);

  job_set_current("Loading stream journal");
  if(destructive_stream_load(journal_path, &stream_ctx, err, err_size) != 0) {
    return -1;
  }
  atomic_store(&g_job.destructive_stream_active,
               stream_ctx.mutation_started ? 1 : 0);
  atomic_store(&g_job.rollback_requested, 0);
  snprintf(info->source_path, sizeof(info->source_path), "%s",
           stream_ctx.source_path);
  snprintf(info->output_path, sizeof(info->output_path), "%s",
           stream_ctx.output_path);
  snprintf(info->nested_name, sizeof(info->nested_name), "%s",
           stream_ctx.nested_name);
  info->format = stream_ctx.format;
  info->nested_type = stream_ctx.nested_type;
  info->source_type = PFS_COMPRESS_SOURCE_APP;
  info->delete_policy = PFS_DELETE_STREAM;
  info->compression_profile = PFS_COMPRESS_PROFILE_FAST;

  job_set_current("Rebuilding stream layout");
  if(destructive_stream_build_layout(&stream_ctx, &nested, err, err_size) != 0) {
    goto done;
  }
  info->nested_size = nested.image_size;
  if(stream_ctx.completed_blocks > 0 || stream_ctx.compression_complete) {
    uint64_t copied = stream_ctx.compression_complete
        ? nested.image_size
        : stream_ctx.completed_blocks * PFS_BLOCK_SIZE;
    if(copied > nested.image_size) copied = nested.image_size;
    atomic_store(&g_job.total_bytes,
                 nested.image_size > (uint64_t)LONG_MAX ? LONG_MAX :
                 (long)nested.image_size);
    atomic_store(&g_job.copied_bytes,
                 copied > (uint64_t)LONG_MAX ? LONG_MAX : (long)copied);
  }

  if(!stream_ctx.compression_complete) {
    job_set_current("Restarting stream compression");
    fd = open(stream_ctx.tmp_path, O_RDWR);
    if(fd < 0) {
      set_err(err, err_size, "open stream temp output: %s", strerror(errno));
      goto done;
    }
    if(compress_nested_to_pfsc(fd, outer_file_start, &nested, worker_count,
                               1, stream_ctx.nested_name,
                               stream_ctx.nested_type,
                               stream_ctx.vhash_tmp_path,
                               stream_ctx.vhash_output_path,
                               stream_ctx.tmp_path, info,
                               stream_ctx.format, &stream_ctx,
                               stream_ctx.source_path, &delete_started,
                               &stored_size, info->compression_profile,
                               err, err_size) != 0) {
      goto done;
    }
  } else {
    stored_size = stream_ctx.data_pos;
    fd = open(stream_ctx.tmp_path, O_RDWR);
    if(fd < 0) {
      int open_errno = errno;
      if(!stream_ctx.output_finalized) {
        if(open_errno == ENOENT &&
           access(stream_ctx.output_path, F_OK) == 0) {
          fd = -1;
        } else {
          set_err(err, err_size, "open stream temp output: %s",
                  strerror(open_errno));
          goto done;
        }
      }
    }
  }
  info->stored_size = stored_size;

  if(!stream_ctx.output_finalized) {
    job_set_current("Finalizing .ffpfsc");
    if(fd >= 0) {
      if(write_outer_pfs_metadata(fd, nested.image_size, stored_size,
                                  stream_ctx.nested_name, err,
                                  err_size) != 0) {
        goto done;
      }
      if(sync_completed_output_file(fd, err, err_size) != 0) {
        goto done;
      }
      if(close(fd) != 0) {
        set_err(err, err_size, "close output: %s", strerror(errno));
        fd = -1;
        goto done;
      }
      fd = -1;
      if(rename(stream_ctx.tmp_path, stream_ctx.output_path) != 0) {
        int rename_errno = errno;
        if(rename_errno != ENOENT ||
           access(stream_ctx.output_path, F_OK) != 0) {
          set_err(err, err_size, "rename output: %s",
                  strerror(rename_errno));
          goto done;
        }
      }
      destructive_stream_fsync_parent_best_effort(stream_ctx.output_path);
    } else if(access(stream_ctx.output_path, F_OK) != 0) {
      set_err(err, err_size, "stream final output is missing");
      goto done;
    }
    if(rename(stream_ctx.vhash_tmp_path, stream_ctx.vhash_output_path) != 0) {
      int rename_errno = errno;
      if(rename_errno != ENOENT ||
         access(stream_ctx.vhash_output_path, F_OK) != 0) {
        set_err(err, err_size, "rename validation hash: %s",
                strerror(rename_errno));
        goto done;
      }
    }
    destructive_stream_fsync_parent_best_effort(stream_ctx.vhash_output_path);
    stream_ctx.output_finalized = 1;
    if(destructive_stream_write_header(&stream_ctx, err, err_size) != 0 ||
       destructive_stream_sync(&stream_ctx, err, err_size) != 0) {
      goto done;
    }
  } else if(fd >= 0) {
    close(fd);
    fd = -1;
  }

  job_set_current("Removing source app folder");
  if(remove_tree_local(stream_ctx.source_path) != 0 && errno != ENOENT) {
    set_err(err, err_size, "remove source app folder: %s", strerror(errno));
    goto done;
  }
  destructive_stream_remove_reverse_dir(&stream_ctx);
  unlink(stream_ctx.journal_path);
  rc = 0;

done:
  if(fd >= 0) close(fd);
  layout_free(&nested);
  destructive_stream_free(&stream_ctx);
  return rc;
}

int
pfs_compress_resume_stream_journal(const char *journal_path, int workers,
                                   pfs_app_info_t *info,
                                   char *err, size_t err_size) {
  return pfs_compress_resume_stream_journal_profile(
      journal_path, workers, PFS_COMPRESS_PROFILE_SPACE, info, err, err_size);
}

int
pfs_compress_app_to_ffpfsc_opts(const char *path, int overwrite,
                                int workers, int format,
                                int delete_policy,
                                pfs_app_info_t *info,
                                char *err, size_t err_size) {
  pfs_app_info_t local_info;
  if(!info) info = &local_info;
  if(pfs_app_probe(path, info, err, err_size) != 0) return -1;
  return pfs_compress_probed_to_ffpfsc_opts(info, overwrite, workers,
                                            format, delete_policy,
                                            PFS_COMPRESS_PROFILE_SPACE,
                                            NULL,
                                            err, err_size);
}

int
pfs_compress_source_to_ffpfsc_opts_profile_output_ex(
                                           const char *path, int overwrite,
                                           int workers, int format,
                                           int delete_policy,
                                           int compression_profile,
                                           const char *output_path,
                                           const pfs_stream_options_t *stream_opts,
                                           pfs_app_info_t *info,
                                           char *err, size_t err_size) {
  pfs_app_info_t local_info;
  struct stat st;
  if(!info) info = &local_info;
  if(pfs_compress_probe(path, info, err, err_size) != 0) return -1;
  if(output_path && output_path[0]) {
    if(normalize_app_path(output_path, info->output_path,
                          sizeof(info->output_path)) != 0 ||
       !ends_with_ci(info->output_path, ".ffpfsc")) {
      set_err(err, err_size, "bad compression output path");
      errno = EINVAL;
      return -1;
    }
    info->output_exists = stat(info->output_path, &st) == 0;
  }
  return pfs_compress_probed_to_ffpfsc_opts(info, overwrite, workers,
                                            format, delete_policy,
                                            compression_profile,
                                            stream_opts,
                                            err, err_size);
}

int
pfs_compress_source_to_ffpfsc_opts_profile_output(
                                           const char *path, int overwrite,
                                           int workers, int format,
                                           int delete_policy,
                                           int compression_profile,
                                           const char *output_path,
                                           pfs_app_info_t *info,
                                           char *err, size_t err_size) {
  return pfs_compress_source_to_ffpfsc_opts_profile_output_ex(
      path, overwrite, workers, format, delete_policy, compression_profile,
      output_path, NULL, info, err, err_size);
}

int
pfs_compress_source_to_ffpfsc_opts_profile(const char *path, int overwrite,
                                           int workers, int format,
                                           int delete_policy,
                                           int compression_profile,
                                           pfs_app_info_t *info,
                                           char *err, size_t err_size) {
  return pfs_compress_source_to_ffpfsc_opts_profile_output(
      path, overwrite, workers, format, delete_policy, compression_profile,
      NULL, info, err, err_size);
}

int
pfs_compress_source_to_ffpfsc_opts(const char *path, int overwrite,
                                   int workers, int format,
                                   int delete_policy,
                                   pfs_app_info_t *info,
                                   char *err, size_t err_size) {
  return pfs_compress_source_to_ffpfsc_opts_profile(
      path, overwrite, workers, format, delete_policy,
      PFS_COMPRESS_PROFILE_SPACE, info, err, err_size);
}

int
pfs_compress_app_to_ffpfsc_ex(const char *path, int overwrite,
                              int workers, int convert,
                              pfs_app_info_t *info,
                              char *err, size_t err_size) {
  return pfs_compress_app_to_ffpfsc_opts(path, overwrite, workers,
                                         PFS_COMPRESS_FORMAT_PFS,
                                         convert ? PFS_DELETE_STREAM
                                                 : PFS_DELETE_KEEP,
                                         info, err, err_size);
}

int
pfs_compress_app_to_ffpfsc(const char *path, int overwrite,
                           pfs_app_info_t *info,
                           char *err, size_t err_size) {
  return pfs_compress_app_to_ffpfsc_opts(path, overwrite,
                                         PFS_COMPRESS_DEFAULT_WORKERS,
                                         PFS_COMPRESS_FORMAT_PFS,
                                         PFS_DELETE_KEEP,
                                         info, err, err_size);
}
