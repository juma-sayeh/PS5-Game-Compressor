/*
 * File Explorer - PFSC post-mount repair.
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
#include <sys/statvfs.h>
#include <sys/types.h>
#include <time.h>
#include <unistd.h>

#include "miniz.h"
#include "pfs_compress.h"
#include "pfs_repair.h"
#include "pfs_validate_hash.h"
#include "transfer_internal.h"

#define PFS_BLOCK_SIZE 65536ULL
#define PFS_INODE_SIZE 0xA8ULL
#define PFS_VERSION_PS5 2ULL
#define PFS_MAGIC 20130315ULL
#define PFS_INODE_MODE_DIR 0x4000U
#define PFS_INODE_FLAG_COMPRESSED 0x1U
#define PFS_DIRENT_TYPE_FILE 2U

#define PFSC_MAGIC 0x43534650U
#define PFSC_UNK4 0U
#define PFSC_UNK8 6U
#define PFSC_HEADER_SIZE 0x30U
#define PFSC_BLOCK_OFFSETS_OFFSET 0x400ULL
#define PFSC_INITIAL_DATA_OFFSET 0x10000ULL
#define PFSC_OFFSET_ENTRY_SIZE 8ULL

#define REPAIR_BASE "/data/GameCompressor/logs/repair"
#define SHADOW_PFSC_BASE "/mnt/shadowmnt/pfsc"
#define REPAIR_PROGRESS_INTERVAL 256ULL
#define REPAIR_DEFAULT_WORKERS 4
#define REPAIR_MAX_WORKERS 10
#define REPAIR_DEFAULT_READ_AHEAD 128
#define REPAIR_MAX_READ_AHEAD 256
#define REPAIR_SMOKE_NEIGHBOR_BLOCKS 1ULL
#define REPAIR_COPY_CHUNK_MAX (32ULL * 1024ULL * 1024ULL)
#define REPAIR_COPY_CHUNK_MID (16ULL * 1024ULL * 1024ULL)
#define REPAIR_COPY_CHUNK_LOW (8ULL * 1024ULL * 1024ULL)
#define REPAIR_COPY_CHUNK_MIN (1ULL * 1024ULL * 1024ULL)
#define REPAIR_IO_SYSCALL_CHUNK_SIZE (1U * 1024U * 1024U)
#define REPAIR_VHASH_CACHE_BYTES (8ULL * 1024ULL * 1024ULL)
#define JOURNAL_SYNC_INTERVAL_SECONDS 2

#define JOURNAL_SUFFIX "_scan_journal.bin"
#define JOURNAL_MAGIC "PFSRJNL1"
#define JOURNAL_VERSION 1U
#define JOURNAL_HEADER_SIZE 4096ULL
#define JOURNAL_STATE_UNKNOWN 0U
#define JOURNAL_STATE_MATCHED 1U
#define JOURNAL_STATE_BAD 2U
#define JOURNAL_PHASE_SCAN 0U
#define JOURNAL_PHASE_SCAN_DONE 1U

#define JOURNAL_OFF_MAGIC 0x000U
#define JOURNAL_OFF_VERSION 0x008U
#define JOURNAL_OFF_HEADER_SIZE 0x00cU
#define JOURNAL_OFF_BLOCK_COUNT 0x010U
#define JOURNAL_OFF_LOGICAL_SIZE 0x018U
#define JOURNAL_OFF_STORED_SIZE 0x020U
#define JOURNAL_OFF_OUTER_SIZE 0x028U
#define JOURNAL_OFF_PFSC_HASH 0x030U
#define JOURNAL_OFF_TABLE_HASH 0x038U
#define JOURNAL_OFF_PHASE 0x040U
#define JOURNAL_OFF_SCANNED_BLOCKS 0x048U
#define JOURNAL_OFF_MATCHED_BLOCKS 0x050U
#define JOURNAL_OFF_REPAIRED_BLOCKS 0x058U
#define JOURNAL_OFF_SOURCE_PATH 0x100U
#define JOURNAL_OFF_MOUNTED_PATH 0x500U
#define JOURNAL_PATH_FIELD_SIZE 1024U

typedef struct pfs_inode_info {
  uint16_t mode;
  uint32_t flags;
  uint64_t size;
  uint64_t size_comp;
  uint32_t blocks;
  int32_t db0;
} pfs_inode_info_t;

typedef struct pfsc_image {
  int fd;
  uint64_t outer_size;
  uint64_t file_start;
  uint64_t stored_size;
  uint64_t logical_size;
  uint64_t nested_size;
  uint64_t header_size;
  uint64_t block_count;
  uint64_t pfsc_header_hash;
  uint64_t offset_table_hash;
  char nested_name[256];
  int nested_type;
  uint64_t *offsets;
} pfsc_image_t;

typedef struct repair_ctx {
  pfs_repair_info_t info;
  FILE *progress;
  FILE *bad_tsv;
  char patch_path[1024];
  char bad_path[1024];
  char summary_path[1024];
  char progress_path[1024];
  char journal_path[1024];
  char *bad_blocks;
  unsigned char *journal_states;
  uint64_t *new_offsets;
  pfs_vhash_reader_t vhash;
  int journal_fd;
  int vhash_open;
  uint32_t journal_phase;
  uint64_t journal_flushed_blocks;
  time_t journal_last_sync_at;
  time_t progress_last_sync_at;
  char journal_resume_status[160];
  uint64_t raw_mismatches;
  uint64_t read_errors;
  uint64_t decode_errors;
  uint64_t block_limit;
  unsigned char *vhash_cache;
  uint64_t vhash_cache_start;
  uint64_t vhash_cache_count;
  uint64_t vhash_cache_cap;
  int vhash_cache_full;
} repair_ctx_t;

typedef struct repair_options {
  int mode;
  int scan_only;
  int temp_only;
  int force_rebuild;
  int storage_check_ok;
  uint64_t source_size;
  uint64_t required_free_bytes;
  uint64_t available_free_bytes;
  char storage_error[256];
} repair_options_t;

static uint32_t
rd32(const void *p) {
  const unsigned char *b = p;
  return (uint32_t)b[0] | ((uint32_t)b[1] << 8) |
         ((uint32_t)b[2] << 16) | ((uint32_t)b[3] << 24);
}

static uint64_t
rd64(const void *p) {
  const unsigned char *b = p;
  return (uint64_t)b[0] | ((uint64_t)b[1] << 8) |
         ((uint64_t)b[2] << 16) | ((uint64_t)b[3] << 24) |
         ((uint64_t)b[4] << 32) | ((uint64_t)b[5] << 40) |
         ((uint64_t)b[6] << 48) | ((uint64_t)b[7] << 56);
}

static void
le32(unsigned char *p, uint32_t v) {
  p[0] = (unsigned char)v;
  p[1] = (unsigned char)(v >> 8);
  p[2] = (unsigned char)(v >> 16);
  p[3] = (unsigned char)(v >> 24);
}

static void
le64(unsigned char *p, uint64_t v) {
  p[0] = (unsigned char)v;
  p[1] = (unsigned char)(v >> 8);
  p[2] = (unsigned char)(v >> 16);
  p[3] = (unsigned char)(v >> 24);
  p[4] = (unsigned char)(v >> 32);
  p[5] = (unsigned char)(v >> 40);
  p[6] = (unsigned char)(v >> 48);
  p[7] = (unsigned char)(v >> 56);
}

static uint64_t
ceil_div_u64(uint64_t a, uint64_t b) {
  return b ? (a + b - 1ULL) / b : 0;
}

static long
job_long_from_u64(uint64_t v) {
  return v > (uint64_t)LONG_MAX ? LONG_MAX : (long)v;
}

static void
set_err(char *err, size_t err_size, const char *fmt, ...) {
  if(!err || err_size == 0 || err[0]) return;
  va_list ap;
  va_start(ap, fmt);
  vsnprintf(err, err_size, fmt, ap);
  va_end(ap);
}

static int
path_join(char *out, size_t out_size, const char *base, const char *name) {
  int n;
  if(base[0] && base[strlen(base) - 1] == '/') {
    n = snprintf(out, out_size, "%s%s", base, name);
  } else {
    n = snprintf(out, out_size, "%s/%s", base, name);
  }
  return n >= 0 && (size_t)n < out_size ? 0 : -1;
}

static int
path_parent(const char *path, char *out, size_t out_size) {
  const char *slash = strrchr(path ? path : "", '/');
  if(!slash || slash == path) {
    if(out_size < 2) return -1;
    snprintf(out, out_size, "/");
    return 0;
  }
  size_t len = (size_t)(slash - path);
  if(len == 0 || len >= out_size) return -1;
  memcpy(out, path, len);
  out[len] = 0;
  return 0;
}

static int
repair_required_free_bytes(uint64_t source_size, uint64_t *out) {
  if(!out || source_size > (UINT64_MAX - 9ULL) / 12ULL) {
    errno = EOVERFLOW;
    return -1;
  }
  *out = (source_size * 12ULL + 9ULL) / 10ULL;
  return 0;
}

static uint64_t
saturating_mul_u64(uint64_t a, uint64_t b) {
  if(a != 0 && b > UINT64_MAX / a) return UINT64_MAX;
  return a * b;
}

static void
repair_choose_strategy(const char *path, repair_options_t *opts) {
  struct stat st;
  struct statvfs sv;
  char parent[1024];

  memset(opts, 0, sizeof(*opts));
  opts->mode = PFS_REPAIR_MODE_INPLACE;

  if(!path || !*path) {
    snprintf(opts->storage_error, sizeof(opts->storage_error), "bad path");
    return;
  }
  if(stat(path, &st) != 0 || st.st_size < 0) {
    snprintf(opts->storage_error, sizeof(opts->storage_error),
             "stat source: %s", strerror(errno));
    return;
  }
  opts->source_size = (uint64_t)st.st_size;
  if(repair_required_free_bytes(opts->source_size,
                                &opts->required_free_bytes) != 0) {
    snprintf(opts->storage_error, sizeof(opts->storage_error),
             "required space overflow");
    return;
  }
  if(path_parent(path, parent, sizeof(parent)) != 0) {
    snprintf(opts->storage_error, sizeof(opts->storage_error),
             "source parent path too long");
    return;
  }
  if(statvfs(parent, &sv) != 0) {
    snprintf(opts->storage_error, sizeof(opts->storage_error),
             "statvfs %s: %s", parent, strerror(errno));
    return;
  }

  uint64_t block_size = sv.f_frsize ? (uint64_t)sv.f_frsize :
      (sv.f_bsize ? (uint64_t)sv.f_bsize : 1ULL);
  opts->available_free_bytes = saturating_mul_u64((uint64_t)sv.f_bavail,
                                                  block_size);
  opts->storage_check_ok = 1;
  if(opts->available_free_bytes >= opts->required_free_bytes) {
    opts->mode = PFS_REPAIR_MODE_COPY_REPLACE;
  }
}

static int
mkdir_p_local(const char *path) {
  char tmp[1024];
  int n = snprintf(tmp, sizeof(tmp), "%s", path);
  if(n < 0 || (size_t)n >= sizeof(tmp)) return -1;
  size_t len = strlen(tmp);
  if(len == 0) return -1;
  if(tmp[len - 1] == '/') tmp[len - 1] = 0;
  for(char *p = tmp + 1; *p; p++) {
    if(*p == '/') {
      *p = 0;
      if(mkdir(tmp, 0777) != 0 && errno != EEXIST) return -1;
      chmod(tmp, 0777);
      *p = '/';
    }
  }
  if(mkdir(tmp, 0777) != 0 && errno != EEXIST) return -1;
  chmod(tmp, 0777);
  return 0;
}

static const char *
repair_output_base(void) {
  return REPAIR_BASE;
}

static int
read_exact_at(int fd, void *data, size_t size, uint64_t offset,
              char *err, size_t err_size) {
  unsigned char *dst = data;
  size_t done = 0;
  while(done < size) {
    if(atomic_load(&g_job.busy) && job_cancelled()) {
      set_err(err, err_size, "cancelled");
      errno = EINTR;
      return -1;
    }
    size_t left = size - done;
    size_t chunk = left > REPAIR_IO_SYSCALL_CHUNK_SIZE ?
        REPAIR_IO_SYSCALL_CHUNK_SIZE : left;
    ssize_t n = pread(fd, dst + done, chunk, (off_t)(offset + done));
    if(n < 0) {
      if(errno == EINTR) continue;
      set_err(err, err_size, "read 0x%llx: %s",
              (unsigned long long)(offset + done), strerror(errno));
      return -1;
    }
    if(n == 0) {
      set_err(err, err_size, "read 0x%llx: unexpected EOF",
              (unsigned long long)(offset + done));
      errno = EIO;
      return -1;
    }
    done += (size_t)n;
  }
  return 0;
}

static int
write_exact_at(int fd, const void *data, size_t size, uint64_t offset,
               char *err, size_t err_size) {
  const unsigned char *src = data;
  size_t done = 0;
  while(done < size) {
    if(atomic_load(&g_job.busy) && job_cancelled()) {
      set_err(err, err_size, "cancelled");
      errno = EINTR;
      return -1;
    }
    size_t left = size - done;
    size_t chunk = left > REPAIR_IO_SYSCALL_CHUNK_SIZE ?
        REPAIR_IO_SYSCALL_CHUNK_SIZE : left;
    ssize_t n = pwrite(fd, src + done, chunk, (off_t)(offset + done));
    if(n < 0) {
      if(errno == EINTR) continue;
      set_err(err, err_size, "write 0x%llx: %s",
              (unsigned long long)(offset + done), strerror(errno));
      return -1;
    }
    if(n == 0) {
      set_err(err, err_size, "write 0x%llx: no progress",
              (unsigned long long)(offset + done));
      errno = EIO;
      return -1;
    }
    done += (size_t)n;
  }
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

static void
job_add_bytes(atomic_long *counter, uint64_t bytes) {
  if(bytes == 0) return;
  atomic_fetch_add(counter, bytes > (uint64_t)LONG_MAX ? LONG_MAX : (long)bytes);
}

static int
repair_vhash_cache_init(repair_ctx_t *ctx, char *err, size_t err_size) {
  if(!ctx || !ctx->vhash_open || ctx->info.block_count == 0) return 0;
  if(ctx->info.block_count <= (uint64_t)SIZE_MAX / PFS_VHASH_HASH_SIZE &&
     ctx->info.block_count <= UINT64_MAX / PFS_VHASH_HASH_SIZE) {
    uint64_t full_size = ctx->info.block_count * (uint64_t)PFS_VHASH_HASH_SIZE;
    ctx->vhash_cache = malloc((size_t)full_size);
    if(ctx->vhash_cache &&
       pfs_vhash_reader_read_hashes(&ctx->vhash, 0, ctx->info.block_count,
                                    ctx->vhash_cache, err, err_size) == 0) {
      ctx->vhash_cache_start = 0;
      ctx->vhash_cache_count = ctx->info.block_count;
      ctx->vhash_cache_cap = ctx->info.block_count;
      ctx->vhash_cache_full = 1;
      return 0;
    }
    free(ctx->vhash_cache);
    ctx->vhash_cache = NULL;
    if(err && err[0]) err[0] = 0;
  }

  uint64_t cap = REPAIR_VHASH_CACHE_BYTES / PFS_VHASH_HASH_SIZE;
  if(cap > ctx->info.block_count) cap = ctx->info.block_count;
  while(cap > 0) {
    if(cap <= (uint64_t)SIZE_MAX / PFS_VHASH_HASH_SIZE) {
      ctx->vhash_cache = malloc((size_t)(cap * PFS_VHASH_HASH_SIZE));
      if(ctx->vhash_cache) break;
    }
    cap /= 2;
  }
  if(!ctx->vhash_cache) {
    return 0;
  }
  ctx->vhash_cache_cap = cap;
  ctx->vhash_cache_start = UINT64_MAX;
  ctx->vhash_cache_count = 0;
  ctx->vhash_cache_full = 0;
  return 0;
}

static int
repair_vhash_get_hash(repair_ctx_t *ctx, uint64_t index,
                      unsigned char hash[PFS_VHASH_HASH_SIZE],
                      char *err, size_t err_size) {
  if(!ctx || !ctx->vhash_open || !hash) {
    set_err(err, err_size, "bad validation hash cache");
    errno = EINVAL;
    return -1;
  }
  if(ctx->vhash_cache) {
    if(ctx->vhash_cache_full) {
      memcpy(hash, ctx->vhash_cache + index * PFS_VHASH_HASH_SIZE,
             PFS_VHASH_HASH_SIZE);
      return 0;
    }
    if(ctx->vhash_cache_start == UINT64_MAX ||
       index < ctx->vhash_cache_start ||
       index >= ctx->vhash_cache_start + ctx->vhash_cache_count) {
      uint64_t count = ctx->vhash_cache_cap;
      if(count > ctx->info.block_count - index) {
        count = ctx->info.block_count - index;
      }
      if(pfs_vhash_reader_read_hashes(&ctx->vhash, index, count,
                                      ctx->vhash_cache, err, err_size) != 0) {
        return -1;
      }
      ctx->vhash_cache_start = index;
      ctx->vhash_cache_count = count;
    }
    memcpy(hash,
           ctx->vhash_cache +
               (index - ctx->vhash_cache_start) * PFS_VHASH_HASH_SIZE,
           PFS_VHASH_HASH_SIZE);
    return 0;
  }
  return pfs_vhash_reader_read_hash(&ctx->vhash, index, hash, err, err_size);
}

static uint64_t
fnv1a64(const unsigned char *data, size_t size) {
  uint64_t h = 1469598103934665603ULL;
  for(size_t i = 0; i < size; i++) {
    h ^= data[i];
    h *= 1099511628211ULL;
  }
  return h;
}

static uint32_t
fnv1a32_string(const char *s) {
  uint32_t h = 2166136261U;
  while(s && *s) {
    h ^= (uint8_t)*s++;
    h *= 16777619U;
  }
  return h;
}

static long long
first_diff(const unsigned char *a, const unsigned char *b, size_t size) {
  for(size_t i = 0; i < size; i++) {
    if(a[i] != b[i]) return (long long)i;
  }
  return -1;
}

static void
json_escape(FILE *f, const char *s) {
  fputc('"', f);
  for(const unsigned char *p = (const unsigned char *)s; p && *p; p++) {
    if(*p == '"' || *p == '\\') {
      fputc('\\', f);
      fputc(*p, f);
    } else if(*p == '\n') {
      fputs("\\n", f);
    } else if(*p == '\r') {
      fputs("\\r", f);
    } else if(*p == '\t') {
      fputs("\\t", f);
    } else if(*p < 0x20) {
      fprintf(f, "\\u%04x", *p);
    } else {
      fputc(*p, f);
    }
  }
  fputc('"', f);
}

static int
ends_with_ci(const char *s, const char *suffix) {
  size_t sl = strlen(s);
  size_t tl = strlen(suffix);
  return sl >= tl && strcasecmp(s + sl - tl, suffix) == 0;
}

static int
title_from_path(const char *path, char *out, size_t out_size) {
  const char *base = strrchr(path, '/');
  base = base ? base + 1 : path;
  if(!ends_with_ci(base, ".ffpfsc")) return -1;
  size_t len = strlen(base) - strlen(".ffpfsc");
  if(out_size < 10) return -1;
  for(size_t i = 0; i + 9 <= len; i++) {
    if(toupper((unsigned char)base[i]) != 'P' ||
       toupper((unsigned char)base[i + 1]) != 'P' ||
       toupper((unsigned char)base[i + 2]) != 'S' ||
       toupper((unsigned char)base[i + 3]) != 'A') {
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
    snprintf(out, out_size, "PPSA%c%c%c%c%c",
             base[i + 4], base[i + 5], base[i + 6],
             base[i + 7], base[i + 8]);
    return 0;
  }
  return -1;
}

static int
ffpfsc_stem_from_path(const char *path, const char **stem, size_t *stem_len) {
  const char *base = strrchr(path ? path : "", '/');
  base = base ? base + 1 : path;
  if(!base || !ends_with_ci(base, ".ffpfsc")) return -1;
  size_t len = strlen(base) - strlen(".ffpfsc");
  if(len == 0) return -1;
  if(stem) *stem = base;
  if(stem_len) *stem_len = len;
  return 0;
}

static int
repair_id_from_path(const char *path, char *out, size_t out_size) {
  const char *stem = NULL;
  size_t stem_len = 0;
  size_t pos = 0;
  if(!out || out_size == 0) return -1;
  out[0] = 0;
  if(title_from_path(path, out, out_size) == 0) return 0;
  if(ffpfsc_stem_from_path(path, &stem, &stem_len) != 0) return -1;
  for(size_t i = 0; i < stem_len && pos + 1 < out_size; i++) {
    unsigned char ch = (unsigned char)stem[i];
    if(isalnum(ch) || ch == '_' || ch == '-') {
      out[pos++] = (char)ch;
    } else if(pos > 0 && out[pos - 1] != '-') {
      out[pos++] = '-';
    }
  }
  while(pos > 0 && out[pos - 1] == '-') pos--;
  if(pos == 0) {
    const char fallback[] = "pfsc";
    size_t n = sizeof(fallback) - 1;
    if(n >= out_size) n = out_size - 1;
    memcpy(out, fallback, n);
    pos = n;
  }
  out[pos] = 0;
  return 0;
}

static const char *
nested_type_name(int nested_type) {
  if(nested_type == PFS_NESTED_PFS) return "pfs";
  if(nested_type == PFS_NESTED_EXFAT) return "exfat";
  return "unknown";
}

static const char *
repair_mode_name(int mode) {
  return mode == PFS_REPAIR_MODE_COPY_REPLACE ? "copy-replace" : "in-place";
}

static int
repair_worker_count(void) {
  return REPAIR_DEFAULT_WORKERS;
}

static int
repair_read_ahead_slots(int worker_count) {
  int slots = REPAIR_DEFAULT_READ_AHEAD;
  if(slots < worker_count) slots = worker_count;
  if(slots > REPAIR_MAX_READ_AHEAD) slots = REPAIR_MAX_READ_AHEAD;
  return slots;
}

static int
nested_type_from_name(const char *name) {
  if(!name || !*name) return PFS_NESTED_UNKNOWN;
  if(!strcasecmp(name, "pfs_image.dat") || ends_with_ci(name, ".ffpfs")) {
    return PFS_NESTED_PFS;
  }
  if(ends_with_ci(name, ".exfat")) return PFS_NESTED_EXFAT;
  return PFS_NESTED_UNKNOWN;
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

static void
progress_line(repair_ctx_t *ctx, const char *fmt, ...) {
  if(!ctx->progress) return;
  fprintf(ctx->progress, "%ld\t", (long)time(NULL));
  va_list ap;
  va_start(ap, fmt);
  vfprintf(ctx->progress, fmt, ap);
  va_end(ap);
  fputc('\n', ctx->progress);
  fflush(ctx->progress);
  time_t now = time(NULL);
  if(ctx->progress_last_sync_at == 0 ||
     now - ctx->progress_last_sync_at >= JOURNAL_SYNC_INTERVAL_SECONDS) {
    fsync(fileno(ctx->progress));
    ctx->progress_last_sync_at = now;
  }
}

static void
parse_inode_info(const unsigned char *data, pfs_inode_info_t *ino) {
  memset(ino, 0, sizeof(*ino));
  ino->mode = (uint16_t)(data[0] | ((uint16_t)data[1] << 8));
  ino->flags = rd32(data + 0x04);
  ino->size = rd64(data + 0x08);
  ino->size_comp = rd64(data + 0x10);
  ino->blocks = rd32(data + 0x60);
  ino->db0 = (int32_t)rd32(data + 0x64);
}

static int
pfsc_header_span(uint64_t block_count, uint64_t *out) {
  uint64_t pointer_table_size = (block_count + 1ULL) * PFSC_OFFSET_ENTRY_SIZE;
  uint64_t initial_capacity = PFSC_INITIAL_DATA_OFFSET - PFSC_BLOCK_OFFSETS_OFFSET;
  uint64_t extra = pointer_table_size > initial_capacity
      ? pointer_table_size - initial_capacity
      : 0;
  if(extra > UINT64_MAX - PFSC_INITIAL_DATA_OFFSET) return -1;
  *out = PFSC_INITIAL_DATA_OFFSET +
         ceil_div_u64(extra, PFS_BLOCK_SIZE) * PFS_BLOCK_SIZE;
  return 0;
}

static void
pfsc_close(pfsc_image_t *img) {
  if(!img) return;
  if(img->fd >= 0) close(img->fd);
  free(img->offsets);
  memset(img, 0, sizeof(*img));
  img->fd = -1;
}

static int
read_outer_nested_name(int fd, pfsc_image_t *img, char *err, size_t err_size) {
  unsigned char inode_raw[PFS_INODE_SIZE];
  unsigned char *dir_block = NULL;
  pfs_inode_info_t root;
  int rc = -1;

  snprintf(img->nested_name, sizeof(img->nested_name), "pfs_image.dat");
  if(read_exact_at(fd, inode_raw, sizeof(inode_raw),
                   PFS_BLOCK_SIZE + 2ULL * PFS_INODE_SIZE,
                   err, err_size) != 0) {
    return -1;
  }
  parse_inode_info(inode_raw, &root);
  if((root.mode & PFS_INODE_MODE_DIR) == 0 || root.db0 < 0 ||
     root.size == 0 || root.size > PFS_BLOCK_SIZE) {
    set_err(err, err_size, "outer PFS root directory is invalid");
    errno = EINVAL;
    return -1;
  }
  dir_block = malloc((size_t)PFS_BLOCK_SIZE);
  if(!dir_block) {
    set_err(err, err_size, "out of memory");
    errno = ENOMEM;
    return -1;
  }
  if(read_exact_at(fd, dir_block, (size_t)root.size,
                   (uint64_t)root.db0 * PFS_BLOCK_SIZE,
                   err, err_size) != 0) {
    goto done;
  }

  for(uint64_t off = 0; off + 16ULL <= root.size;) {
    uint32_t child_inode = rd32(dir_block + off);
    uint32_t type = rd32(dir_block + off + 4);
    uint32_t name_len = rd32(dir_block + off + 8);
    uint32_t ent_size = rd32(dir_block + off + 12);
    if(ent_size == 0) break;
    if(ent_size < 16 || ent_size > PFS_BLOCK_SIZE ||
       name_len == 0 || name_len >= sizeof(img->nested_name) ||
       16ULL + name_len > ent_size || off + ent_size > root.size) {
      set_err(err, err_size, "outer PFS root directory entry is invalid");
      errno = EINVAL;
      goto done;
    }
    if(child_inode == 3 && type == PFS_DIRENT_TYPE_FILE) {
      memcpy(img->nested_name, dir_block + off + 16, name_len);
      img->nested_name[name_len] = 0;
      if(!path_segment_supported(img->nested_name)) {
        set_err(err, err_size, "nested image name is unsupported");
        errno = EINVAL;
        goto done;
      }
      rc = 0;
      goto done;
    }
    off += ent_size;
  }

  set_err(err, err_size, "outer PFS nested image entry was not found");
  errno = EINVAL;

done:
  free(dir_block);
  return rc;
}

static int
pfsc_open(const char *path, int writable, pfsc_image_t *img,
          char *err, size_t err_size) {
  unsigned char *header = NULL;
  unsigned char inode_raw[PFS_INODE_SIZE];
  unsigned char pfsc[PFSC_HEADER_SIZE];
  pfs_inode_info_t ino;
  uint64_t table_offset = 0;
  uint64_t data_offset = 0;
  uint64_t expected_header = 0;
  unsigned char *table = NULL;
  struct stat st;
  int rc = -1;

  memset(img, 0, sizeof(*img));
  img->fd = -1;
  snprintf(img->nested_name, sizeof(img->nested_name), "pfs_image.dat");
  header = malloc((size_t)PFS_BLOCK_SIZE);
  if(!header) {
    set_err(err, err_size, "out of memory");
    errno = ENOMEM;
    goto done;
  }

  int fd = open(path, writable ? O_RDWR : O_RDONLY);
  if(fd < 0) {
    set_err(err, err_size, "open input: %s", strerror(errno));
    goto done;
  }
  img->fd = fd;
  if(fstat(fd, &st) != 0 || st.st_size < 0) {
    set_err(err, err_size, "stat input: %s", strerror(errno));
    goto done;
  }
  img->outer_size = (uint64_t)st.st_size;

  if(read_exact_at(fd, header, (size_t)PFS_BLOCK_SIZE, 0, err, err_size) != 0) {
    goto done;
  }
  if(rd64(header + 0x00) != PFS_VERSION_PS5 ||
     rd64(header + 0x08) != PFS_MAGIC ||
     rd32(header + 0x20) != (uint32_t)PFS_BLOCK_SIZE) {
    set_err(err, err_size, "not a supported outer PFS container");
    errno = EINVAL;
    goto done;
  }

  if(read_exact_at(fd, inode_raw, sizeof(inode_raw),
                   PFS_BLOCK_SIZE + 3ULL * PFS_INODE_SIZE,
                   err, err_size) != 0) {
    goto done;
  }
  parse_inode_info(inode_raw, &ino);
  if((ino.flags & PFS_INODE_FLAG_COMPRESSED) == 0 || ino.db0 < 0 ||
     ino.size == 0 || ino.size_comp == 0) {
    set_err(err, err_size, "outer PFS does not contain a compressed nested image");
    errno = EINVAL;
    goto done;
  }

  img->file_start = (uint64_t)ino.db0 * PFS_BLOCK_SIZE;
  img->stored_size = ino.size;
  img->logical_size = ino.size_comp;
  img->nested_size = ino.size_comp;
  if(read_outer_nested_name(fd, img, err, err_size) != 0) {
    goto done;
  }
  if(read_exact_at(fd, pfsc, sizeof(pfsc), img->file_start, err,
                   err_size) != 0) {
    goto done;
  }
  img->pfsc_header_hash = fnv1a64(pfsc, sizeof(pfsc));
  if(rd32(pfsc + 0x00) != PFSC_MAGIC ||
     rd32(pfsc + 0x04) != PFSC_UNK4 ||
     rd32(pfsc + 0x08) != PFSC_UNK8 ||
     rd32(pfsc + 0x0c) != (uint32_t)PFS_BLOCK_SIZE ||
     rd64(pfsc + 0x10) != PFS_BLOCK_SIZE) {
    set_err(err, err_size, "invalid PFSC header");
    errno = EINVAL;
    goto done;
  }
  table_offset = rd64(pfsc + 0x18);
  data_offset = rd64(pfsc + 0x20);
  img->logical_size = rd64(pfsc + 0x28);
  if(table_offset != PFSC_BLOCK_OFFSETS_OFFSET ||
     data_offset < PFSC_INITIAL_DATA_OFFSET ||
     img->logical_size == 0 ||
     img->logical_size % PFS_BLOCK_SIZE != 0) {
    set_err(err, err_size, "unsupported PFSC layout");
    errno = EINVAL;
    goto done;
  }
  img->block_count = img->logical_size / PFS_BLOCK_SIZE;
  if(img->nested_size == 0 || img->nested_size > img->logical_size) {
    set_err(err, err_size, "invalid nested image logical size");
    errno = EINVAL;
    goto done;
  }
  if(img->block_count == 0 ||
     img->block_count > (SIZE_MAX / sizeof(uint64_t)) - 1ULL) {
    set_err(err, err_size, "PFSC block count is too large");
    errno = EINVAL;
    goto done;
  }
  if(pfsc_header_span(img->block_count, &expected_header) != 0 ||
     expected_header != data_offset) {
    set_err(err, err_size, "unsupported PFSC data offset");
    errno = EINVAL;
    goto done;
  }
  uint64_t table_size = (img->block_count + 1ULL) * PFSC_OFFSET_ENTRY_SIZE;
  if(table_offset + table_size > data_offset ||
     data_offset > img->stored_size) {
    set_err(err, err_size, "invalid PFSC offset table");
    errno = EINVAL;
    goto done;
  }
  img->header_size = data_offset;
  img->offsets = calloc((size_t)(img->block_count + 1ULL),
                        sizeof(*img->offsets));
  if(!img->offsets) {
    set_err(err, err_size, "out of memory");
    errno = ENOMEM;
    goto done;
  }
  table = malloc((size_t)table_size);
  if(!table) {
    set_err(err, err_size, "out of memory");
    errno = ENOMEM;
    goto done;
  }
  if(read_exact_at(fd, table, (size_t)table_size,
                   img->file_start + table_offset, err, err_size) != 0) {
    goto done;
  }
  img->offset_table_hash = fnv1a64(table, (size_t)table_size);
  for(uint64_t i = 0; i <= img->block_count; i++) {
    img->offsets[i] = rd64(table + i * 8ULL);
    if(i > 0 && img->offsets[i] < img->offsets[i - 1]) {
      set_err(err, err_size, "PFSC offsets are not monotonic");
      errno = EINVAL;
      goto done;
    }
  }
  if(img->offsets[0] != data_offset ||
     img->offsets[img->block_count] > img->stored_size) {
    set_err(err, err_size, "PFSC offsets exceed stored size");
    errno = EINVAL;
    goto done;
  }
  rc = 0;
done:
  free(table);
  free(header);
  return rc;
}

static int
pfsc_read_stored(const pfsc_image_t *img, uint64_t index,
                 unsigned char *stored, size_t *stored_len,
                 char *err, size_t err_size) {
  if(index >= img->block_count) {
    set_err(err, err_size, "PFSC block outside image");
    errno = EINVAL;
    return -1;
  }
  uint64_t start = img->offsets[index];
  uint64_t end = img->offsets[index + 1ULL];
  if(end < start || end - start > PFS_BLOCK_SIZE || end > img->stored_size) {
    set_err(err, err_size, "invalid PFSC block span");
    errno = EINVAL;
    return -1;
  }
  size_t len = (size_t)(end - start);
  if(len == 0) {
    set_err(err, err_size, "empty PFSC block span");
    errno = EINVAL;
    return -1;
  }
  if(read_exact_at(img->fd, stored, len, img->file_start + start,
                   err, err_size) != 0) {
    return -1;
  }
  job_add_bytes(&g_job.repair_read_bytes, len);
  *stored_len = len;
  return 0;
}

static int
pfsc_decode_block(const pfsc_image_t *img, uint64_t index,
                  unsigned char *stored, unsigned char *decoded,
                  size_t *stored_len, char *err, size_t err_size) {
  if(pfsc_read_stored(img, index, stored, stored_len, err, err_size) != 0) {
    return -1;
  }
  if(*stored_len == (size_t)PFS_BLOCK_SIZE) {
    memcpy(decoded, stored, (size_t)PFS_BLOCK_SIZE);
    return 0;
  }
  size_t out_len = tinfl_decompress_mem_to_mem(
      decoded, (size_t)PFS_BLOCK_SIZE, stored, *stored_len,
      TINFL_FLAG_PARSE_ZLIB_HEADER);
  if(out_len != (size_t)PFS_BLOCK_SIZE) {
    set_err(err, err_size, "decompress PFSC block %llu failed",
            (unsigned long long)index);
    errno = EINVAL;
    return -1;
  }
  return 0;
}

static int
shadowmount_pfsc_mount_dir(const char *source_path, char *out, size_t out_size,
                           char *err, size_t err_size) {
  const char *stem = NULL;
  size_t stem_len = 0;
  char mount_name[1024];
  if(ffpfsc_stem_from_path(source_path, &stem, &stem_len) != 0) {
    set_err(err, err_size, "repair requires a .ffpfsc container");
    errno = EINVAL;
    return -1;
  }
  size_t max_stem = sizeof(mount_name) - 1U - 9U;
  if(stem_len > max_stem) stem_len = max_stem;
  int n = snprintf(mount_name, sizeof(mount_name), "%.*s_%08x",
                   (int)stem_len, stem, fnv1a32_string(source_path));
  if(n < 0 || (size_t)n >= sizeof(mount_name)) {
    set_err(err, err_size, "ShadowMount mount name too long");
    errno = ENAMETOOLONG;
    return -1;
  }
  if(path_join(out, out_size, SHADOW_PFSC_BASE, mount_name) != 0) {
    set_err(err, err_size, "ShadowMount mount path too long");
    errno = ENAMETOOLONG;
    return -1;
  }
  return 0;
}

static int
find_mounted_nested(const char *source_path, const char *nested_name,
                    char *out, size_t out_size,
                    char *err, size_t err_size) {
  if(!nested_name || !path_segment_supported(nested_name)) {
    set_err(err, err_size, "bad nested image name");
    errno = EINVAL;
    return -1;
  }
  char mount_dir[1024];
  if(shadowmount_pfsc_mount_dir(source_path, mount_dir, sizeof(mount_dir),
                                err, err_size) != 0) {
    return -1;
  }
  struct stat st;
  if(stat(mount_dir, &st) != 0 || !S_ISDIR(st.st_mode)) {
    set_err(err, err_size, "not mounted by ShadowMountPlus: expected %s",
            mount_dir);
    errno = ENOENT;
    return -1;
  }
  if(path_join(out, out_size, mount_dir, nested_name) != 0) {
    set_err(err, err_size, "mounted nested path too long");
    errno = ENAMETOOLONG;
    return -1;
  }
  if(stat(out, &st) != 0 || !S_ISREG(st.st_mode)) {
    set_err(err, err_size, "not mounted by ShadowMountPlus: expected %s",
            out);
    errno = ENOENT;
    return -1;
  }
  return 0;
}

static int
setup_output_paths(repair_ctx_t *ctx, char *err, size_t err_size) {
  if(path_join(ctx->progress_path, sizeof(ctx->progress_path),
               ctx->info.outdir, "progress.log") != 0 ||
     path_join(ctx->patch_path, sizeof(ctx->patch_path),
               ctx->info.outdir, "patch_request.json") != 0 ||
     path_join(ctx->bad_path, sizeof(ctx->bad_path),
               ctx->info.outdir, "bad_blocks.tsv") != 0 ||
     path_join(ctx->summary_path, sizeof(ctx->summary_path),
               ctx->info.outdir, "summary.json") != 0) {
    set_err(err, err_size, "create repair output paths failed");
    return -1;
  }
  return 0;
}

static int
setup_journal_path(repair_ctx_t *ctx, char *err, size_t err_size) {
  char parent[1024];
  char name[160];
  if(path_parent(ctx->info.source_path, parent, sizeof(parent)) != 0) {
    set_err(err, err_size, "journal parent path too long");
    return -1;
  }
  int n = snprintf(name, sizeof(name), "%s%s", ctx->info.title_id,
                   JOURNAL_SUFFIX);
  if(n < 0 || (size_t)n >= sizeof(name) ||
     path_join(ctx->journal_path, sizeof(ctx->journal_path),
               parent, name) != 0) {
    set_err(err, err_size, "journal path too long");
    return -1;
  }
  snprintf(ctx->info.journal_path, sizeof(ctx->info.journal_path),
           "%s", ctx->journal_path);
  return 0;
}

static int
open_log_outputs(repair_ctx_t *ctx, int resume, char *err, size_t err_size) {
  struct stat st;
  int bad_has_content = stat(ctx->bad_path, &st) == 0 && st.st_size > 0;
  ctx->progress = fopen(ctx->progress_path, resume ? "a" : "w");
  ctx->bad_tsv = fopen(ctx->bad_path, resume ? "a" : "w");
  if(!ctx->progress || !ctx->bad_tsv) {
    set_err(err, err_size, "open repair output: %s", strerror(errno));
    return -1;
  }
  if(!resume || !bad_has_content) {
    fprintf(ctx->bad_tsv, "block\tstored_len\tstored_offset\tfirst_diff\t"
                          "software_fnv\tmounted_fnv\n");
    fflush(ctx->bad_tsv);
    fsync(fileno(ctx->bad_tsv));
  }
  return 0;
}

static void
journal_header_fill(unsigned char *hdr, const repair_ctx_t *ctx,
                    const pfsc_image_t *img, uint32_t phase) {
  memset(hdr, 0, (size_t)JOURNAL_HEADER_SIZE);
  memcpy(hdr + JOURNAL_OFF_MAGIC, JOURNAL_MAGIC, 8);
  le32(hdr + JOURNAL_OFF_VERSION, JOURNAL_VERSION);
  le32(hdr + JOURNAL_OFF_HEADER_SIZE, (uint32_t)JOURNAL_HEADER_SIZE);
  le64(hdr + JOURNAL_OFF_BLOCK_COUNT, img->block_count);
  le64(hdr + JOURNAL_OFF_LOGICAL_SIZE, img->logical_size);
  le64(hdr + JOURNAL_OFF_STORED_SIZE, img->stored_size);
  le64(hdr + JOURNAL_OFF_OUTER_SIZE, img->outer_size);
  le64(hdr + JOURNAL_OFF_PFSC_HASH, img->pfsc_header_hash);
  le64(hdr + JOURNAL_OFF_TABLE_HASH, img->offset_table_hash);
  le32(hdr + JOURNAL_OFF_PHASE, phase);
  le64(hdr + JOURNAL_OFF_SCANNED_BLOCKS, ctx->info.scanned_blocks);
  le64(hdr + JOURNAL_OFF_MATCHED_BLOCKS, ctx->info.matched_blocks);
  le64(hdr + JOURNAL_OFF_REPAIRED_BLOCKS, ctx->info.repaired_blocks);
  snprintf((char *)hdr + JOURNAL_OFF_SOURCE_PATH, JOURNAL_PATH_FIELD_SIZE,
           "%s", ctx->info.source_path);
  snprintf((char *)hdr + JOURNAL_OFF_MOUNTED_PATH, JOURNAL_PATH_FIELD_SIZE,
           "%s", ctx->info.mounted_path);
}

static int
journal_header_basic_valid(const unsigned char *hdr) {
  return memcmp(hdr + JOURNAL_OFF_MAGIC, JOURNAL_MAGIC, 8) == 0 &&
         rd32(hdr + JOURNAL_OFF_VERSION) == JOURNAL_VERSION &&
         rd32(hdr + JOURNAL_OFF_HEADER_SIZE) == (uint32_t)JOURNAL_HEADER_SIZE;
}

static int
journal_header_identity_matches(const unsigned char *hdr,
                                const repair_ctx_t *ctx,
                                const pfsc_image_t *img) {
  if(!journal_header_basic_valid(hdr)) return 0;
  if(rd64(hdr + JOURNAL_OFF_BLOCK_COUNT) != img->block_count ||
     rd64(hdr + JOURNAL_OFF_LOGICAL_SIZE) != img->logical_size ||
     rd64(hdr + JOURNAL_OFF_STORED_SIZE) != img->stored_size ||
     rd64(hdr + JOURNAL_OFF_OUTER_SIZE) != img->outer_size ||
     rd64(hdr + JOURNAL_OFF_PFSC_HASH) != img->pfsc_header_hash ||
     rd64(hdr + JOURNAL_OFF_TABLE_HASH) != img->offset_table_hash) {
    return 0;
  }
  if(strncmp((const char *)hdr + JOURNAL_OFF_SOURCE_PATH,
             ctx->info.source_path, JOURNAL_PATH_FIELD_SIZE) != 0 ||
     strncmp((const char *)hdr + JOURNAL_OFF_MOUNTED_PATH,
             ctx->info.mounted_path, JOURNAL_PATH_FIELD_SIZE) != 0) {
    return 0;
  }
  return 1;
}

static int
journal_read_header_at(const char *path, unsigned char *hdr) {
  int fd = open(path, O_RDONLY);
  if(fd < 0) return -1;
  int rc = read_exact_at(fd, hdr, (size_t)JOURNAL_HEADER_SIZE, 0, NULL, 0);
  close(fd);
  return rc;
}

static int
journal_resume_available(repair_ctx_t *ctx, const pfsc_image_t *img,
                         char *err, size_t err_size) {
  struct stat st;
  if(stat(ctx->journal_path, &st) != 0) {
    if(errno == ENOENT) {
      snprintf(ctx->journal_resume_status,
               sizeof(ctx->journal_resume_status), "%s", "missing");
      return 0;
    }
    set_err(err, err_size, "stat journal: %s", strerror(errno));
    return -1;
  }
  if(!S_ISREG(st.st_mode)) {
    set_err(err, err_size, "journal path is not a file");
    errno = EINVAL;
    return -1;
  }

  unsigned char hdr[JOURNAL_HEADER_SIZE];
  if(journal_read_header_at(ctx->journal_path, hdr) != 0 ||
     !journal_header_identity_matches(hdr, ctx, img)) {
    snprintf(ctx->journal_resume_status, sizeof(ctx->journal_resume_status),
             "%s", "rejected identity mismatch");
    if(unlink(ctx->journal_path) != 0 && errno != ENOENT) {
      set_err(err, err_size, "remove stale journal: %s", strerror(errno));
      return -1;
    }
    return 0;
  }

  uint32_t phase = rd32(hdr + JOURNAL_OFF_PHASE);
  if(phase == JOURNAL_PHASE_SCAN) {
    snprintf(ctx->journal_resume_status, sizeof(ctx->journal_resume_status),
             "%s", "accepted");
    return 1;
  }
  snprintf(ctx->journal_resume_status, sizeof(ctx->journal_resume_status),
           "rejected phase %u", phase);
  if(unlink(ctx->journal_path) != 0 && errno != ENOENT) {
    set_err(err, err_size, "remove completed journal: %s", strerror(errno));
    return -1;
  }
  return 0;
}

static int
open_outputs(repair_ctx_t *ctx, const pfsc_image_t *img,
             char *err, size_t err_size) {
  const char *base = repair_output_base();
  if(mkdir_p_local(base) != 0) {
    set_err(err, err_size, "create repair base: %s", strerror(errno));
    return -1;
  }
  if(setup_journal_path(ctx, err, err_size) != 0) return -1;

  int resume = journal_resume_available(ctx, img, err, err_size);
  if(resume < 0) return -1;
  if(resume) {
    ctx->info.resumed = 1;
  }
  int n = snprintf(ctx->info.outdir, sizeof(ctx->info.outdir),
                   "%s/%s-%ld", base, ctx->info.title_id,
                   (long)time(NULL));
  if(n < 0 || (size_t)n >= sizeof(ctx->info.outdir) ||
     mkdir_p_local(ctx->info.outdir) != 0) {
    set_err(err, err_size, "create repair output paths failed");
    return -1;
  }
  if(setup_output_paths(ctx, err, err_size) != 0) {
    return -1;
  }
  return open_log_outputs(ctx, 0, err, err_size);
}

static void
write_summary(repair_ctx_t *ctx, const char *status, const char *err_text) {
  FILE *f = fopen(ctx->summary_path, "w");
  if(!f) return;
  fprintf(f, "{\n");
  fprintf(f, "  \"status\":");
  json_escape(f, status ? status : "");
  fprintf(f, ",\n  \"error\":");
  json_escape(f, err_text ? err_text : "");
  fprintf(f, ",\n  \"titleId\":");
  json_escape(f, ctx->info.title_id);
  fprintf(f, ",\n  \"path\":");
  json_escape(f, ctx->info.source_path);
  fprintf(f, ",\n  \"output\":");
  json_escape(f, ctx->info.output_path);
  fprintf(f, ",\n  \"temp\":");
  json_escape(f, ctx->info.temp_path);
  fprintf(f, ",\n  \"repairMode\":");
  json_escape(f, repair_mode_name(ctx->info.repair_mode));
  fprintf(f, ",\n  \"hashMode\":");
  json_escape(f, ctx->info.hash_mode);
  fprintf(f, ",\n  \"storageError\":");
  json_escape(f, ctx->info.storage_error);
  fprintf(f, ",\n  \"mounted\":");
  json_escape(f, ctx->info.mounted_path);
  fprintf(f, ",\n  \"nestedName\":");
  json_escape(f, ctx->info.nested_name);
  fprintf(f, ",\n  \"nestedType\":");
  json_escape(f, nested_type_name(ctx->info.nested_type));
  fprintf(f, ",\n  \"outdir\":");
  json_escape(f, ctx->info.outdir);
  fprintf(f, ",\n  \"journal\":");
  json_escape(f, ctx->info.journal_path);
  fprintf(f,
          ",\n  \"resumed\":%s,\n"
          "  \"storageCheckOk\":%s,\n"
          "  \"sourceSize\":%llu,\n"
          "  \"requiredFreeBytes\":%llu,\n"
          "  \"availableFreeBytes\":%llu,\n"
          "  \"journalPhase\":%u,\n"
          "  \"logicalSize\":%llu,\n"
          "  \"nestedSize\":%llu,\n"
          "  \"blockCount\":%llu,\n"
          "  \"scannedBlocks\":%llu,\n"
          "  \"matchedBlocks\":%llu,\n"
          "  \"repairedBlocks\":%llu,\n"
          "  \"hashCheckedBlocks\":%llu,\n"
          "  \"hashMatchedBlocks\":%llu,\n"
          "  \"hashMismatchedBlocks\":%llu,\n"
          "  \"softwareComparedBlocks\":%llu,\n"
          "  \"postVerifyBlocks\":%llu,\n"
          "  \"postVerifyMountBlocks\":%llu,\n"
          "  \"rawMismatches\":%llu,\n"
          "  \"readErrors\":%llu,\n"
          "  \"decodeErrors\":%llu,\n"
          "  \"oldStoredSize\":%llu,\n"
          "  \"newStoredSize\":%llu,\n"
          "  \"bytesMoved\":%llu,\n"
          "  \"noop\":%s\n"
          "}\n",
          ctx->info.resumed ? "true" : "false",
          ctx->info.storage_check_ok ? "true" : "false",
          (unsigned long long)ctx->info.source_size,
          (unsigned long long)ctx->info.required_free_bytes,
          (unsigned long long)ctx->info.available_free_bytes,
          ctx->journal_phase,
          (unsigned long long)ctx->info.logical_size,
          (unsigned long long)ctx->info.nested_size,
          (unsigned long long)ctx->info.block_count,
          (unsigned long long)ctx->info.scanned_blocks,
          (unsigned long long)ctx->info.matched_blocks,
          (unsigned long long)ctx->info.repaired_blocks,
          (unsigned long long)ctx->info.hash_checked_blocks,
          (unsigned long long)ctx->info.hash_matched_blocks,
          (unsigned long long)ctx->info.hash_mismatched_blocks,
          (unsigned long long)ctx->info.software_compared_blocks,
          (unsigned long long)ctx->info.post_verify_blocks,
          (unsigned long long)ctx->info.post_verify_mount_blocks,
          (unsigned long long)ctx->raw_mismatches,
          (unsigned long long)ctx->read_errors,
          (unsigned long long)ctx->decode_errors,
          (unsigned long long)ctx->info.old_stored_size,
          (unsigned long long)ctx->info.new_stored_size,
          (unsigned long long)ctx->info.bytes_moved,
          ctx->info.noop ? "true" : "false");
  fclose(f);
}

static void
write_patch_request(repair_ctx_t *ctx, const pfsc_image_t *img) {
  FILE *f = fopen(ctx->patch_path, "w");
  if(!f) return;
  fprintf(f, "{\n  \"path\":");
  json_escape(f, ctx->info.source_path);
  fprintf(f, ",\n  \"mounted\":");
  json_escape(f, ctx->info.mounted_path);
  fprintf(f, ",\n  \"nestedName\":");
  json_escape(f, ctx->info.nested_name);
  fprintf(f, ",\n  \"nestedType\":");
  json_escape(f, nested_type_name(ctx->info.nested_type));
  fprintf(f,
          ",\n  \"titleId\":");
  json_escape(f, ctx->info.title_id);
  fprintf(f,
	          ",\n  \"blockSize\":%llu,\n"
	          "  \"nestedSize\":%llu,\n"
	          "  \"blockCount\":%llu,\n"
          "  \"oldStoredSize\":%llu,\n"
          "  \"newStoredSize\":%llu,\n"
          "  \"badBlocks\":[",
	          (unsigned long long)PFS_BLOCK_SIZE,
	          (unsigned long long)ctx->info.nested_size,
	          (unsigned long long)img->block_count,
          (unsigned long long)ctx->info.old_stored_size,
          (unsigned long long)ctx->info.new_stored_size);
  int first = 1;
  for(uint64_t i = 0; i < img->block_count; i++) {
    if(!ctx->bad_blocks[i]) continue;
    uint64_t start = img->offsets[i];
    uint64_t end = img->offsets[i + 1ULL];
    fprintf(f, "%s\n    {\"block\":%llu,\"storedOffset\":%llu,"
               "\"storedLength\":%llu,\"rawLength\":%llu}",
            first ? "" : ",",
            (unsigned long long)i,
            (unsigned long long)start,
            (unsigned long long)(end - start),
            (unsigned long long)PFS_BLOCK_SIZE);
    first = 0;
  }
  fprintf(f, "\n  ]\n}\n");
  fclose(f);
}

static int
journal_write_meta(repair_ctx_t *ctx, uint32_t phase,
                   char *err, size_t err_size) {
  if(ctx->journal_fd < 0) return 0;
  unsigned char meta[32];
  memset(meta, 0, sizeof(meta));
  le32(meta + 0, phase);
  le64(meta + 8, ctx->info.scanned_blocks);
  le64(meta + 16, ctx->info.matched_blocks);
  le64(meta + 24, ctx->info.repaired_blocks);
  if(write_exact_at(ctx->journal_fd, meta, sizeof(meta),
                    JOURNAL_OFF_PHASE, err, err_size) != 0) {
    return -1;
  }
  ctx->journal_phase = phase;
  return 0;
}

static int
journal_flush_scan_phase(repair_ctx_t *ctx, uint64_t upto, uint32_t phase,
                         int force, char *err, size_t err_size) {
  if(ctx->journal_fd < 0) return 0;
  if(upto > ctx->info.block_count) upto = ctx->info.block_count;
  time_t now = time(NULL);
  if(!force &&
     ctx->journal_last_sync_at != 0 &&
     now - ctx->journal_last_sync_at < JOURNAL_SYNC_INTERVAL_SECONDS) {
    return 0;
  }
  if(upto > ctx->journal_flushed_blocks) {
    uint64_t start = ctx->journal_flushed_blocks;
    uint64_t count = upto - start;
    if(count > (uint64_t)SIZE_MAX) {
      set_err(err, err_size, "journal flush range too large");
      errno = EOVERFLOW;
      return -1;
    }
    if(write_exact_at(ctx->journal_fd, ctx->journal_states + start,
                      (size_t)count, JOURNAL_HEADER_SIZE + start,
                      err, err_size) != 0) {
      return -1;
    }
    ctx->journal_flushed_blocks = upto;
  }
  if(journal_write_meta(ctx, phase, err, err_size) != 0) {
    return -1;
  }
  if(fsync(ctx->journal_fd) != 0) {
    set_err(err, err_size, "sync journal: %s", strerror(errno));
    return -1;
  }
  ctx->journal_last_sync_at = now;
  return 0;
}

static int
journal_flush_scan(repair_ctx_t *ctx, uint64_t upto, int force,
                   char *err, size_t err_size) {
  return journal_flush_scan_phase(ctx, upto, JOURNAL_PHASE_SCAN, force,
                                  err, err_size);
}

static int
journal_remove_after_scan(repair_ctx_t *ctx, char *err, size_t err_size) {
  if(ctx->journal_fd >= 0) {
    if(close(ctx->journal_fd) != 0) {
      ctx->journal_fd = -1;
      set_err(err, err_size, "close journal: %s", strerror(errno));
      return -1;
    }
    ctx->journal_fd = -1;
  }
  if(ctx->journal_path[0] &&
     unlink(ctx->journal_path) != 0 && errno != ENOENT) {
    set_err(err, err_size, "remove scan journal: %s", strerror(errno));
    return -1;
  }
  return 0;
}

static uint64_t
journal_known_prefix(const unsigned char *states, uint64_t block_count) {
  uint64_t i = 0;
  while(i < block_count && states[i] != JOURNAL_STATE_UNKNOWN) i++;
  return i;
}

static void
journal_recount(repair_ctx_t *ctx) {
  ctx->info.scanned_blocks = 0;
  ctx->info.matched_blocks = 0;
  ctx->info.repaired_blocks = 0;
  memset(ctx->bad_blocks, 0, (size_t)ctx->info.block_count);
  for(uint64_t i = 0; i < ctx->info.block_count; i++) {
    unsigned char state = ctx->journal_states[i];
    if(state == JOURNAL_STATE_MATCHED) {
      ctx->info.scanned_blocks++;
      ctx->info.matched_blocks++;
    } else if(state == JOURNAL_STATE_BAD) {
      ctx->info.scanned_blocks++;
      ctx->info.repaired_blocks++;
      ctx->bad_blocks[i] = 1;
    } else if(state != JOURNAL_STATE_UNKNOWN) {
      ctx->journal_states[i] = JOURNAL_STATE_UNKNOWN;
    }
  }
}

static int
journal_load_existing(repair_ctx_t *ctx, const pfsc_image_t *img,
                      char *err, size_t err_size) {
  unsigned char hdr[JOURNAL_HEADER_SIZE];
  if(read_exact_at(ctx->journal_fd, hdr, (size_t)JOURNAL_HEADER_SIZE,
                   0, err, err_size) != 0) {
    return -1;
  }
  if(!journal_header_identity_matches(hdr, ctx, img)) {
    set_err(err, err_size, "resume journal identity mismatch");
    errno = EINVAL;
    return -1;
  }
  uint32_t phase = rd32(hdr + JOURNAL_OFF_PHASE);
  if(phase != JOURNAL_PHASE_SCAN) {
    set_err(err, err_size, "journal is not in scan phase");
    errno = EINVAL;
    return -1;
  }
  struct stat st;
  if(fstat(ctx->journal_fd, &st) != 0 || st.st_size < 0) {
    set_err(err, err_size, "stat journal: %s", strerror(errno));
    return -1;
  }
  uint64_t expected = JOURNAL_HEADER_SIZE + img->block_count;
  if((uint64_t)st.st_size < expected &&
     ftruncate(ctx->journal_fd, (off_t)expected) != 0) {
    set_err(err, err_size, "extend journal: %s", strerror(errno));
    return -1;
  }
  uint64_t available = (uint64_t)st.st_size > JOURNAL_HEADER_SIZE
      ? (uint64_t)st.st_size - JOURNAL_HEADER_SIZE
      : 0;
  if(available > img->block_count) available = img->block_count;
  if(available > 0 &&
     read_exact_at(ctx->journal_fd, ctx->journal_states, (size_t)available,
                   JOURNAL_HEADER_SIZE, err, err_size) != 0) {
    return -1;
  }
  ctx->journal_phase = phase;
  journal_recount(ctx);
  ctx->journal_flushed_blocks =
      journal_known_prefix(ctx->journal_states, img->block_count);
  ctx->journal_last_sync_at = time(NULL);
  return 0;
}

static int
journal_create_new(repair_ctx_t *ctx, const pfsc_image_t *img,
                   char *err, size_t err_size) {
  uint64_t expected = JOURNAL_HEADER_SIZE + img->block_count;
  if(ftruncate(ctx->journal_fd, (off_t)expected) != 0) {
    set_err(err, err_size, "create journal: %s", strerror(errno));
    return -1;
  }
  unsigned char hdr[JOURNAL_HEADER_SIZE];
  journal_header_fill(hdr, ctx, img, JOURNAL_PHASE_SCAN);
  if(write_exact_at(ctx->journal_fd, hdr, (size_t)JOURNAL_HEADER_SIZE,
                    0, err, err_size) != 0) {
    return -1;
  }
  ctx->journal_phase = JOURNAL_PHASE_SCAN;
  if(fsync(ctx->journal_fd) != 0) {
    set_err(err, err_size, "sync journal: %s", strerror(errno));
    return -1;
  }
  ctx->journal_last_sync_at = time(NULL);
  return 0;
}

static int
journal_open_or_create(repair_ctx_t *ctx, const pfsc_image_t *img,
                       char *err, size_t err_size) {
  ctx->journal_states = calloc((size_t)img->block_count, 1);
  if(!ctx->journal_states) {
    set_err(err, err_size, "out of memory");
    errno = ENOMEM;
    return -1;
  }
  int flags = O_RDWR | O_CREAT | (ctx->info.resumed ? 0 : O_TRUNC);
  ctx->journal_fd = open(ctx->journal_path, flags, 0666);
  if(ctx->journal_fd < 0) {
    set_err(err, err_size, "open journal: %s", strerror(errno));
    return -1;
  }
  if(ctx->info.resumed) {
    if(journal_load_existing(ctx, img, err, err_size) != 0) return -1;
    progress_line(ctx,
                  "RESUME journal=%s scanned=%llu matched=%llu repair=%llu phase=%u",
                  ctx->journal_path,
                  (unsigned long long)ctx->info.scanned_blocks,
                  (unsigned long long)ctx->info.matched_blocks,
                  (unsigned long long)ctx->info.repaired_blocks,
                  ctx->journal_phase);
    return 0;
  }
  return journal_create_new(ctx, img, err, err_size);
}

static size_t
repair_block_compare_size(const pfsc_image_t *img, uint64_t index) {
  uint64_t offset = index * PFS_BLOCK_SIZE;
  if(!img || offset >= img->nested_size) return 0;
  uint64_t remaining = img->nested_size - offset;
  return remaining < PFS_BLOCK_SIZE ? (size_t)remaining : (size_t)PFS_BLOCK_SIZE;
}

static int
repair_mark_smoke_verify_blocks(const repair_ctx_t *ctx,
                                const pfsc_image_t *img,
                                unsigned char *verify_blocks,
                                uint64_t *verify_count) {
  if(!ctx || !img || !ctx->bad_blocks || !verify_blocks || !verify_count) {
    errno = EINVAL;
    return -1;
  }
  *verify_count = 0;
  for(uint64_t i = 0; i < img->block_count; i++) {
    if(!ctx->bad_blocks[i]) continue;
    uint64_t start = i > REPAIR_SMOKE_NEIGHBOR_BLOCKS
        ? i - REPAIR_SMOKE_NEIGHBOR_BLOCKS
        : 0;
    uint64_t end = i + REPAIR_SMOKE_NEIGHBOR_BLOCKS;
    if(end >= img->block_count) end = img->block_count - 1ULL;
    for(uint64_t block = start; block <= end; block++) {
      if(!verify_blocks[block]) {
        verify_blocks[block] = 1;
        (*verify_count)++;
      }
    }
  }
  return 0;
}

static int
repair_verify_repaired_smoke(repair_ctx_t *ctx,
                             const pfsc_image_t *expected,
                             const char *path,
                             int mounted_fd,
                             int verify_mounted,
                             char *err, size_t err_size) {
  pfsc_image_t repaired;
  unsigned char *verify_blocks = NULL;
  unsigned char *stored = NULL;
  unsigned char *decoded = NULL;
  unsigned char *mounted = NULL;
  uint64_t verify_count = 0;
  uint64_t done = 0;
  int rc = -1;

  memset(&repaired, 0, sizeof(repaired));
  repaired.fd = -1;
  if(!ctx || !expected || !path || ctx->info.repaired_blocks == 0) return 0;
  verify_blocks = calloc((size_t)expected->block_count, 1);
  stored = malloc((size_t)PFS_BLOCK_SIZE);
  decoded = malloc((size_t)PFS_BLOCK_SIZE);
  mounted = malloc((size_t)PFS_BLOCK_SIZE);
  if(!verify_blocks || !stored || !decoded || !mounted) {
    set_err(err, err_size, "out of memory");
    errno = ENOMEM;
    goto done;
  }
  if(repair_mark_smoke_verify_blocks(ctx, expected, verify_blocks,
                                     &verify_count) != 0) {
    set_err(err, err_size, "build repair smoke verification set failed");
    goto done;
  }
  if(verify_count == 0) {
    rc = 0;
    goto done;
  }
  if(pfsc_open(path, 0, &repaired, err, err_size) != 0) {
    goto done;
  }
  repaired.nested_type = nested_type_from_name(repaired.nested_name);
  if(repaired.logical_size != expected->logical_size ||
     repaired.nested_size != expected->nested_size ||
     repaired.block_count != expected->block_count ||
     repaired.nested_type != expected->nested_type ||
     strcmp(repaired.nested_name, expected->nested_name) != 0) {
    set_err(err, err_size, "post-repair PFSC identity changed");
    errno = EINVAL;
    goto done;
  }

  progress_line(ctx,
                "POST_VERIFY_START blocks=%llu repaired=%llu neighbors=%llu mounted=%s",
                (unsigned long long)verify_count,
                (unsigned long long)ctx->info.repaired_blocks,
                (unsigned long long)REPAIR_SMOKE_NEIGHBOR_BLOCKS,
                verify_mounted && mounted_fd >= 0 ? "yes" : "no");
  if(!(verify_mounted && mounted_fd >= 0)) {
    progress_line(ctx,
                  "POST_VERIFY_MOUNT_SKIPPED mode=%s reason=%s",
                  repair_mode_name(ctx->info.repair_mode),
                  ctx->info.repair_mode == PFS_REPAIR_MODE_COPY_REPLACE
                      ? "copy-replace requires remount"
                      : "mounted fd unavailable");
  }
  job_set_phase("validating", 0, job_long_from_u64(verify_count),
                "Smoke verifying repaired blocks");
  atomic_store(&g_job.total_files,
               verify_count > (uint64_t)INT_MAX ? INT_MAX : (int)verify_count);
  atomic_store(&g_job.done_files, 0);
  atomic_store(&g_job.total_blocks, job_long_from_u64(verify_count));
  atomic_store(&g_job.copied_bytes, 0);
  atomic_store(&g_job.total_bytes,
               verify_count > (uint64_t)LONG_MAX / PFS_BLOCK_SIZE
                   ? LONG_MAX
                   : (long)(verify_count * PFS_BLOCK_SIZE));

  for(uint64_t i = 0; i < repaired.block_count; i++) {
    if(!verify_blocks[i]) continue;
    if(job_cancelled()) {
      set_err(err, err_size, "cancelled");
      errno = EINTR;
      goto done;
    }
    if((done % REPAIR_PROGRESS_INTERVAL) == 0) {
      char current[160];
      snprintf(current, sizeof(current),
               "Smoke verifying repair block %llu/%llu",
               (unsigned long long)i,
               (unsigned long long)repaired.block_count);
      job_set_phase("validating", job_long_from_u64(done),
                    job_long_from_u64(verify_count), current);
    }

    size_t stored_len = 0;
    if(pfsc_decode_block(&repaired, i, stored, decoded, &stored_len,
                         err, err_size) != 0) {
      goto done;
    }
    if(ctx->bad_blocks[i] && stored_len != (size_t)PFS_BLOCK_SIZE) {
      set_err(err, err_size,
              "post-repair block %llu was not stored raw",
              (unsigned long long)i);
      errno = EIO;
      goto done;
    }
    ctx->info.post_verify_blocks++;

    if(verify_mounted && mounted_fd >= 0) {
      size_t compare_len = repair_block_compare_size(&repaired, i);
      if(compare_len > 0) {
        if(read_exact_at(mounted_fd, mounted, compare_len,
                         i * PFS_BLOCK_SIZE, err, err_size) != 0) {
          goto done;
        }
        long long diff = first_diff(decoded, mounted, compare_len);
        if(diff >= 0) {
          set_err(err, err_size,
                  "post-repair mounted smoke mismatch block %llu first_diff=%lld",
                  (unsigned long long)i, diff);
          errno = EIO;
          goto done;
        }
        ctx->info.post_verify_mount_blocks++;
      }
    }

    done++;
    atomic_store(&g_job.done_files,
                 done > (uint64_t)INT_MAX ? INT_MAX : (int)done);
    atomic_store(&g_job.copied_bytes,
                 done > (uint64_t)LONG_MAX / PFS_BLOCK_SIZE
                     ? LONG_MAX
                     : (long)(done * PFS_BLOCK_SIZE));
  }

  job_set_phase("validating", job_long_from_u64(verify_count),
                job_long_from_u64(verify_count),
                "Repair smoke verification complete");
  progress_line(ctx,
                "POST_VERIFY_DONE blocks=%llu mounted=%llu",
                (unsigned long long)ctx->info.post_verify_blocks,
                (unsigned long long)ctx->info.post_verify_mount_blocks);
  rc = 0;

done:
  pfsc_close(&repaired);
  free(verify_blocks);
  free(stored);
  free(decoded);
  free(mounted);
  return rc;
}

typedef struct repair_slot {
  uint64_t index;
  size_t compare_len;
  size_t stored_len;
  long long diff;
  int used_hash;
  int hash_match;
  int hash_mismatch;
  int software_compared;
  int bad;
  int raw_mismatch;
  int error;
  char error_text[256];
  unsigned char expected_hash[PFS_VHASH_HASH_SIZE];
  unsigned char mounted_hash[PFS_VHASH_HASH_SIZE];
  unsigned char *mounted;
  unsigned char *stored;
  unsigned char *decoded;
} repair_slot_t;

typedef struct repair_slab_read_task {
  int fd;
  unsigned char *buf;
  size_t size;
  uint64_t offset;
  int rc;
  int error;
  char error_text[256];
} repair_slab_read_task_t;

typedef struct repair_slab_work {
  repair_ctx_t *ctx;
  const pfsc_image_t *img;
  uint64_t block_start;
  uint64_t block_count;
  uint64_t stored_start;
  uint64_t stored_end;
  int hash_mode;
  unsigned char *stored_slab;
  unsigned char *mounted_slab;
  unsigned char *decoded_slab;
  unsigned char *expected_hashes;
  repair_slot_t *slots;
  atomic_ullong next_local;
  atomic_int error;
  pthread_mutex_t error_lock;
  char error_text[256];
  int error_no;
} repair_slab_work_t;

static void
repair_slot_clear_result(repair_slot_t *slot) {
  slot->compare_len = 0;
  slot->stored_len = 0;
  slot->diff = -1;
  slot->used_hash = 0;
  slot->hash_match = 0;
  slot->hash_mismatch = 0;
  slot->software_compared = 0;
  slot->bad = 0;
  slot->raw_mismatch = 0;
  slot->error = 0;
  slot->error_text[0] = 0;
  memset(slot->expected_hash, 0, sizeof(slot->expected_hash));
  memset(slot->mounted_hash, 0, sizeof(slot->mounted_hash));
}

static void
repair_slot_set_error(repair_slot_t *slot, int err_no, const char *text) {
  slot->error = err_no ? err_no : EIO;
  snprintf(slot->error_text, sizeof(slot->error_text), "%s",
           text && text[0] ? text : strerror(slot->error));
}

static void *
repair_slab_read_main(void *arg) {
  repair_slab_read_task_t *task = arg;
  char read_err[256] = {0};
  task->rc = 0;
  task->error = 0;
  task->error_text[0] = 0;
  if(task->size > 0 &&
     read_exact_at(task->fd, task->buf, task->size, task->offset,
                   read_err, sizeof(read_err)) != 0) {
    task->rc = -1;
    task->error = errno ? errno : EIO;
    snprintf(task->error_text, sizeof(task->error_text), "%s",
             read_err[0] ? read_err : strerror(task->error));
  }
  return NULL;
}

static void
repair_slab_set_error(repair_slab_work_t *work, int err_no,
                      const char *text) {
  if(atomic_exchange(&work->error, 1) != 0) return;
  pthread_mutex_lock(&work->error_lock);
  work->error_no = err_no ? err_no : EIO;
  snprintf(work->error_text, sizeof(work->error_text), "%s",
           text && text[0] ? text : strerror(work->error_no));
  pthread_mutex_unlock(&work->error_lock);
}

static int
repair_decode_block_from_slab(const repair_slab_work_t *work, uint64_t local,
                              char *err, size_t err_size) {
  uint64_t block = work->block_start + local;
  if(block >= work->img->block_count) {
    set_err(err, err_size, "PFSC block outside image");
    errno = EINVAL;
    return -1;
  }
  uint64_t start = work->img->offsets[block];
  uint64_t end = work->img->offsets[block + 1ULL];
  if(end < start || end - start > PFS_BLOCK_SIZE ||
     end > work->img->stored_size ||
     start < work->stored_start || end > work->stored_end) {
    set_err(err, err_size, "invalid PFSC block span");
    errno = EINVAL;
    return -1;
  }
  repair_slot_t *slot = &work->slots[local];
  size_t stored_len = (size_t)(end - start);
  if(stored_len == 0) {
    set_err(err, err_size, "empty PFSC block span");
    errno = EINVAL;
    return -1;
  }
  const unsigned char *stored = work->stored_slab + (start - work->stored_start);
  unsigned char *decoded = work->decoded_slab + local * PFS_BLOCK_SIZE;
  slot->stored_len = stored_len;
  if(stored_len == (size_t)PFS_BLOCK_SIZE) {
    memcpy(decoded, stored, (size_t)PFS_BLOCK_SIZE);
    return 0;
  }
  size_t out_len = tinfl_decompress_mem_to_mem(
      decoded, (size_t)PFS_BLOCK_SIZE, stored, stored_len,
      TINFL_FLAG_PARSE_ZLIB_HEADER);
  if(out_len != (size_t)PFS_BLOCK_SIZE) {
    set_err(err, err_size, "decompress PFSC block %llu failed",
            (unsigned long long)block);
    errno = EINVAL;
    return -1;
  }
  return 0;
}

static void *
repair_slab_worker_main(void *arg) {
  repair_slab_work_t *work = arg;

  for(;;) {
    if(atomic_load(&work->error)) break;
    uint64_t local = atomic_fetch_add(&work->next_local, 1ULL);
    if(local >= work->block_count) break;
    repair_slot_t *slot = &work->slots[local];
    if(work->hash_mode) {
      slot->used_hash = 1;
      memcpy(slot->expected_hash,
             work->expected_hashes + local * PFS_VHASH_HASH_SIZE,
             PFS_VHASH_HASH_SIZE);
      pfs_sha256(slot->mounted, slot->compare_len, slot->mounted_hash);
      if(memcmp(slot->mounted_hash, slot->expected_hash,
                PFS_VHASH_HASH_SIZE) == 0) {
        slot->hash_match = 1;
      } else {
        slot->hash_mismatch = 1;
      }
    } else {
      char block_err[256] = {0};
      if(repair_decode_block_from_slab(work, local, block_err,
                                       sizeof(block_err)) != 0) {
        char text[320];
        snprintf(text, sizeof(text), "block %llu: %s",
                 (unsigned long long)slot->index,
                 block_err[0] ? block_err : "decode failed");
        repair_slot_set_error(slot, errno, text);
        repair_slab_set_error(work, errno, text);
        break;
      }
    }
  }
  return NULL;
}

static int
repair_slab_run_workers(repair_slab_work_t *work, pthread_t *threads,
                        int worker_count, char *err, size_t err_size) {
  int started = 0;
  atomic_store(&work->next_local, 0);
  atomic_store(&work->error, 0);
  work->error_no = 0;
  work->error_text[0] = 0;
  if(pthread_mutex_init(&work->error_lock, NULL) != 0) {
    set_err(err, err_size, "init repair slab task failed");
    errno = EINVAL;
    return -1;
  }
  for(int i = 0; i < worker_count; i++) {
    int trc = pthread_create(&threads[i], NULL, repair_slab_worker_main, work);
    if(trc != 0) {
      repair_slab_set_error(work, trc, strerror(trc));
      break;
    }
    started++;
  }
  for(int i = 0; i < started; i++) {
    pthread_join(threads[i], NULL);
  }
  pthread_mutex_destroy(&work->error_lock);
  if(atomic_load(&work->error)) {
    set_err(err, err_size, "%s",
            work->error_text[0] ? work->error_text : "repair slab failed");
    errno = work->error_no ? work->error_no : EIO;
    return -1;
  }
  return 0;
}

static int
repair_read_stored_slab(const pfsc_image_t *img, uint64_t block_start,
                        uint64_t block_count, unsigned char *stored_slab,
                        size_t slab_bytes, uint64_t *stored_start_out,
                        uint64_t *stored_end_out,
                        char *err, size_t err_size) {
  uint64_t stored_start = img->offsets[block_start];
  uint64_t stored_end = img->offsets[block_start + block_count];
  if(stored_end < stored_start || stored_end > img->stored_size ||
     stored_end - stored_start > (uint64_t)slab_bytes) {
    set_err(err, err_size, "invalid PFSC slab span");
    errno = EINVAL;
    return -1;
  }
  size_t stored_len = (size_t)(stored_end - stored_start);
  if(stored_len == 0) {
    set_err(err, err_size, "empty PFSC slab span");
    errno = EINVAL;
    return -1;
  }
  if(read_exact_at(img->fd, stored_slab, stored_len,
                   img->file_start + stored_start, err, err_size) != 0) {
    return -1;
  }
  job_add_bytes(&g_job.repair_read_bytes, stored_len);
  *stored_start_out = stored_start;
  *stored_end_out = stored_end;
  return 0;
}

static int
repair_start_mounted_slab_read(int mounted_fd, uint64_t logical_offset,
                               size_t logical_bytes,
                               unsigned char *mounted_slab,
                               pthread_t *thread,
                               repair_slab_read_task_t *task,
                               char *err, size_t err_size) {
  memset(task, 0, sizeof(*task));
  task->fd = mounted_fd;
  task->buf = mounted_slab;
  task->size = logical_bytes;
  task->offset = logical_offset;
  int trc = pthread_create(thread, NULL, repair_slab_read_main, task);
  if(trc != 0) {
    set_err(err, err_size, "start mounted slab read: %s", strerror(trc));
    errno = trc;
    return -1;
  }
  return 0;
}

static int
repair_finish_mounted_slab_read(repair_ctx_t *ctx,
                                repair_slab_read_task_t *task,
                                pthread_t thread,
                                char *err, size_t err_size) {
  pthread_join(thread, NULL);
  if(task->rc != 0) {
    ctx->read_errors++;
    set_err(err, err_size, "%s",
            task->error_text[0] ? task->error_text : "mounted read failed");
    errno = task->error ? task->error : EIO;
    return -1;
  }
  job_add_bytes(&g_job.repair_read_bytes, task->size);
  return 0;
}

static void
repair_store_hash_counters(repair_ctx_t *ctx) {
  atomic_store(&g_job.hash_checked_blocks,
               job_long_from_u64(ctx->info.hash_checked_blocks));
  atomic_store(&g_job.hash_matched_blocks,
               job_long_from_u64(ctx->info.hash_matched_blocks));
  atomic_store(&g_job.hash_mismatched_blocks,
               job_long_from_u64(ctx->info.hash_mismatched_blocks));
  atomic_store(&g_job.software_compared_blocks,
               job_long_from_u64(ctx->info.software_compared_blocks));
}

static void
repair_store_found_counters(repair_ctx_t *ctx) {
  long blocks = ctx->info.repaired_blocks > (uint64_t)LONG_MAX ? LONG_MAX :
      (long)ctx->info.repaired_blocks;
  atomic_store(&g_job.bad_blocks_found, blocks);
  atomic_store(&g_job.raw_blocks, blocks);
}

static void
repair_account_result(repair_ctx_t *ctx, const repair_slot_t *slot) {
  if(slot->used_hash) ctx->info.hash_checked_blocks++;
  if(slot->hash_match) ctx->info.hash_matched_blocks++;
  if(slot->hash_mismatch) ctx->info.hash_mismatched_blocks++;
  if(slot->software_compared) ctx->info.software_compared_blocks++;
  repair_store_hash_counters(ctx);
}

static int
repair_record_validation_result(repair_ctx_t *ctx, pfsc_image_t *img,
                                const repair_slot_t *slot,
                                char *err, size_t err_size) {
  repair_account_result(ctx, slot);
  if(slot->error) {
    ctx->decode_errors++;
    set_err(err, err_size, "%s", slot->error_text[0] ?
            slot->error_text : strerror(slot->error));
    errno = slot->error;
    return -1;
  }
  if(slot->raw_mismatch) {
    ctx->raw_mismatches++;
    set_err(err, err_size,
            "mounted raw block %llu differs from stored raw data",
            (unsigned long long)slot->index);
    errno = EIO;
    return -1;
  }

  if(slot->bad) {
    if(ctx->journal_states) ctx->journal_states[slot->index] = JOURNAL_STATE_BAD;
    ctx->bad_blocks[slot->index] = 1;
    ctx->info.repaired_blocks++;
    repair_store_found_counters(ctx);
    fprintf(ctx->bad_tsv, "%llu\t%zu\t%llu\t%lld\t%016llx\t%016llx\n",
            (unsigned long long)slot->index,
            slot->stored_len,
            (unsigned long long)img->offsets[slot->index],
            slot->diff,
            (unsigned long long)fnv1a64(slot->decoded, slot->compare_len),
            (unsigned long long)fnv1a64(slot->mounted, slot->compare_len));
    fflush(ctx->bad_tsv);
    progress_line(ctx,
                  "BAD_BLOCK block=%llu stored_len=%zu stored_offset=%llu first_diff=%lld",
                  (unsigned long long)slot->index,
                  slot->stored_len,
                  (unsigned long long)img->offsets[slot->index],
                  slot->diff);
  } else {
    if(ctx->journal_states) {
      ctx->journal_states[slot->index] = JOURNAL_STATE_MATCHED;
    }
    ctx->info.matched_blocks++;
    atomic_store(&g_job.compressed_blocks,
                 job_long_from_u64(ctx->info.matched_blocks));
  }

  ctx->info.scanned_blocks++;
  uint64_t copied = ctx->info.scanned_blocks * PFS_BLOCK_SIZE;
  if(copied > img->nested_size) copied = img->nested_size;
  atomic_store(&g_job.copied_bytes, job_long_from_u64(copied));
  atomic_store(&g_job.done_files,
               ctx->info.scanned_blocks > (uint64_t)INT_MAX ? INT_MAX :
               (int)ctx->info.scanned_blocks);
  return 0;
}

static void
repair_prepare_slab_slots(repair_slot_t *slots, uint64_t block_start,
                          uint64_t block_count, const pfsc_image_t *img,
                          unsigned char *mounted_slab,
                          unsigned char *decoded_slab,
                          unsigned char *stored_slab) {
  for(uint64_t local = 0; local < block_count; local++) {
    repair_slot_t *slot = &slots[local];
    repair_slot_clear_result(slot);
    slot->index = block_start + local;
    slot->compare_len = repair_block_compare_size(img, slot->index);
    slot->mounted = mounted_slab + local * PFS_BLOCK_SIZE;
    slot->decoded = decoded_slab + local * PFS_BLOCK_SIZE;
    slot->stored = stored_slab;
  }
}

static int
repair_load_expected_slab_hashes(repair_ctx_t *ctx, uint64_t block_start,
                                 uint64_t block_count,
                                 unsigned char *expected_hashes) {
  if(!ctx->vhash_open) return 1;
  for(uint64_t local = 0; local < block_count; local++) {
    uint64_t block = block_start + local;
    char hash_err[256] = {0};
    if(repair_vhash_get_hash(ctx, block,
                             expected_hashes +
                                 local * PFS_VHASH_HASH_SIZE,
                             hash_err, sizeof(hash_err)) != 0) {
      progress_line(ctx, "HASH_INVALID block=%llu error=%s",
                    (unsigned long long)block,
                    hash_err[0] ? hash_err : "read failed");
      pfs_vhash_reader_close(&ctx->vhash);
      ctx->vhash_open = 0;
      snprintf(ctx->info.hash_mode, sizeof(ctx->info.hash_mode), "%s",
               pfs_vhash_mode_name(PFS_VHASH_MODE_INVALID));
      return 1;
    }
  }
  return 0;
}

static void
repair_compare_decoded_slab(repair_slot_t *slots, uint64_t block_count) {
  for(uint64_t local = 0; local < block_count; local++) {
    repair_slot_t *slot = &slots[local];
    if(slot->error || slot->compare_len == 0) continue;
    slot->software_compared = 1;
    slot->diff = first_diff(slot->decoded, slot->mounted, slot->compare_len);
    if(slot->diff >= 0) {
      if(slot->stored_len == (size_t)PFS_BLOCK_SIZE) {
        slot->raw_mismatch = 1;
      } else {
        slot->bad = 1;
      }
    }
  }
}

static void
repair_resolve_hash_mismatches(repair_ctx_t *ctx, pfsc_image_t *img,
                               repair_slot_t *slots,
                               uint64_t block_count,
                               unsigned char *stored_scratch) {
  (void)ctx;
  for(uint64_t local = 0; local < block_count; local++) {
    repair_slot_t *slot = &slots[local];
    if(!slot->hash_mismatch || slot->error || slot->compare_len == 0) continue;
    char block_err[256] = {0};
    slot->software_compared = 1;
    if(pfsc_decode_block(img, slot->index, stored_scratch, slot->decoded,
                         &slot->stored_len,
                         block_err, sizeof(block_err)) != 0) {
      repair_slot_set_error(slot, errno,
                            block_err[0] ? block_err : "decode failed");
      continue;
    }
    slot->diff = first_diff(slot->decoded, slot->mounted, slot->compare_len);
    if(slot->diff >= 0) {
      if(slot->stored_len == (size_t)PFS_BLOCK_SIZE) {
        slot->raw_mismatch = 1;
      } else {
        slot->bad = 1;
      }
    }
  }
}

static int
repair_record_slab_results(repair_ctx_t *ctx, pfsc_image_t *img,
                           repair_slot_t *slots, uint64_t block_count,
                           char *err, size_t err_size) {
  for(uint64_t local = 0; local < block_count; local++) {
    repair_slot_t *slot = &slots[local];
    if(repair_record_validation_result(ctx, img, slot,
                                       err, err_size) != 0) {
      return -1;
    }
    if(((slot->index + 1ULL) % REPAIR_PROGRESS_INTERVAL) == 0 &&
       journal_flush_scan(ctx, slot->index + 1ULL, 0, err, err_size) != 0) {
      return -1;
    }
  }
  return 0;
}

static int
repair_scan_slab(repair_ctx_t *ctx, pfsc_image_t *img, int mounted_fd,
                 uint64_t block_start, uint64_t block_count,
                 int worker_count, size_t slab_bytes,
                 unsigned char *stored_slab,
                 unsigned char *mounted_slab,
                 unsigned char *decoded_slab,
                 unsigned char *expected_hashes,
                 repair_slot_t *slots,
                 pthread_t *threads,
                 char *err, size_t err_size) {
  uint64_t logical_offset = block_start * PFS_BLOCK_SIZE;
  uint64_t logical_bytes64 = block_count * PFS_BLOCK_SIZE;
  if(logical_offset + logical_bytes64 > img->nested_size) {
    logical_bytes64 = img->nested_size > logical_offset
        ? img->nested_size - logical_offset
        : 0;
  }
  if(logical_bytes64 > (uint64_t)slab_bytes) {
    set_err(err, err_size, "repair slab logical range overflow");
    errno = EINVAL;
    return -1;
  }
  size_t logical_bytes = (size_t)logical_bytes64;
  repair_prepare_slab_slots(slots, block_start, block_count, img,
                            mounted_slab, decoded_slab, stored_slab);

  int use_hash = ctx->vhash_open;
  int mounted_ready = 0;
  pthread_t mounted_thread;
  repair_slab_read_task_t mounted_task;

  if(use_hash) {
    if(repair_start_mounted_slab_read(mounted_fd, logical_offset,
                                      logical_bytes, mounted_slab,
                                      &mounted_thread, &mounted_task,
                                      err, err_size) != 0) {
      return -1;
    }
    int hash_status = repair_load_expected_slab_hashes(
        ctx, block_start, block_count, expected_hashes);
    if(repair_finish_mounted_slab_read(ctx, &mounted_task, mounted_thread,
                                       err, err_size) != 0) {
      return -1;
    }
    mounted_ready = 1;
    if(hash_status != 0) use_hash = 0;
  }

  repair_slab_work_t work;
  memset(&work, 0, sizeof(work));
  work.ctx = ctx;
  work.img = img;
  work.block_start = block_start;
  work.block_count = block_count;
  work.hash_mode = use_hash;
  work.stored_slab = stored_slab;
  work.mounted_slab = mounted_slab;
  work.decoded_slab = decoded_slab;
  work.expected_hashes = expected_hashes;
  work.slots = slots;

  if(use_hash) {
    if(repair_slab_run_workers(&work, threads, worker_count,
                               err, err_size) != 0) {
      return -1;
    }
    repair_resolve_hash_mismatches(ctx, img, slots, block_count, stored_slab);
  } else {
    if(repair_read_stored_slab(img, block_start, block_count,
                               stored_slab, slab_bytes,
                               &work.stored_start, &work.stored_end,
                               err, err_size) != 0) {
      return -1;
    }
    if(!mounted_ready &&
       repair_start_mounted_slab_read(mounted_fd, logical_offset,
                                      logical_bytes, mounted_slab,
                                      &mounted_thread, &mounted_task,
                                      err, err_size) != 0) {
      return -1;
    }
    int worker_rc = repair_slab_run_workers(&work, threads, worker_count,
                                            err, err_size);
    int mount_rc = 0;
    if(!mounted_ready) {
      mount_rc = repair_finish_mounted_slab_read(ctx, &mounted_task,
                                                 mounted_thread,
                                                 err, err_size);
    }
    if(worker_rc != 0) return -1;
    if(mount_rc != 0) return -1;
    repair_compare_decoded_slab(slots, block_count);
  }

  return repair_record_slab_results(ctx, img, slots, block_count,
                                    err, err_size);
}

static int
scan_blocks(repair_ctx_t *ctx, pfsc_image_t *img, int mounted_fd,
            char *err, size_t err_size) {
  int worker_count = repair_worker_count();
  uint64_t slab_blocks = (uint64_t)repair_read_ahead_slots(worker_count);
  uint64_t block_limit = ctx->block_limit &&
      ctx->block_limit < img->block_count ? ctx->block_limit : img->block_count;
  uint64_t nested_limit = block_limit >= img->block_count
      ? img->nested_size
      : block_limit * PFS_BLOCK_SIZE;
  if(nested_limit > img->nested_size) nested_limit = img->nested_size;
  if(block_limit < slab_blocks) slab_blocks = block_limit;
  if(slab_blocks == 0) slab_blocks = 1;
  if((uint64_t)worker_count > slab_blocks) worker_count = (int)slab_blocks;
  if(worker_count < 1) worker_count = 1;
  uint64_t slab_bytes64 = slab_blocks * PFS_BLOCK_SIZE;
  if(slab_bytes64 > (uint64_t)SIZE_MAX) {
    set_err(err, err_size, "repair slab is too large");
    errno = EOVERFLOW;
    return -1;
  }
  size_t slab_bytes = (size_t)slab_bytes64;
  unsigned char *stored_slab = NULL;
  unsigned char *mounted_slab = NULL;
  unsigned char *decoded_slab = NULL;
  unsigned char *expected_hashes = NULL;
  repair_slot_t *slots = NULL;
  pthread_t *threads = NULL;
  uint64_t block = 0;
  int rc = -1;

  job_set_phase("scanning", job_long_from_u64(ctx->info.scanned_blocks),
                job_long_from_u64(block_limit),
                "Scanning mounted image");
  atomic_store(&g_job.total_bytes,
               nested_limit > (uint64_t)LONG_MAX ? LONG_MAX :
               (long)nested_limit);
  atomic_store(&g_job.copied_bytes, 0);
  atomic_store(&g_job.total_files,
               block_limit > (uint64_t)INT_MAX ? INT_MAX :
               (int)block_limit);
  atomic_store(&g_job.done_files, 0);
  atomic_store(&g_job.total_blocks,
               block_limit > (uint64_t)LONG_MAX ? LONG_MAX :
               (long)block_limit);
  atomic_store(&g_job.raw_blocks,
               ctx->info.repaired_blocks > (uint64_t)LONG_MAX ? LONG_MAX :
               (long)ctx->info.repaired_blocks);
  repair_store_found_counters(ctx);
  atomic_store(&g_job.repaired_blocks, 0);
  atomic_store(&g_job.compressed_blocks,
               ctx->info.matched_blocks > (uint64_t)LONG_MAX ? LONG_MAX :
               (long)ctx->info.matched_blocks);
  atomic_store(&g_job.copied_bytes,
               ctx->info.scanned_blocks * PFS_BLOCK_SIZE >
                   (uint64_t)LONG_MAX ? LONG_MAX :
               (long)(ctx->info.scanned_blocks * PFS_BLOCK_SIZE));
  atomic_store(&g_job.done_files,
               ctx->info.scanned_blocks > (uint64_t)INT_MAX ? INT_MAX :
               (int)ctx->info.scanned_blocks);
  repair_store_hash_counters(ctx);
  atomic_store(&g_job.writer_wait_us, 0);
  atomic_store(&g_job.worker_wait_us, 0);

  stored_slab = malloc(slab_bytes);
  mounted_slab = malloc(slab_bytes);
  decoded_slab = malloc(slab_bytes);
  expected_hashes = malloc((size_t)slab_blocks * PFS_VHASH_HASH_SIZE);
  slots = calloc((size_t)slab_blocks, sizeof(*slots));
  threads = calloc((size_t)worker_count, sizeof(*threads));
  if(!stored_slab || !mounted_slab || !decoded_slab ||
     !expected_hashes || !slots || !threads) {
    set_err(err, err_size, "out of memory");
    errno = ENOMEM;
    goto done;
  }

  progress_line(ctx,
                "SCAN_SLAB mode=%s workers=%d slab_blocks=%llu slab_bytes=%llu",
                ctx->info.hash_mode, worker_count,
                (unsigned long long)slab_blocks,
                (unsigned long long)slab_bytes64);

  while(block < block_limit) {
    if(ctx->journal_states &&
       (ctx->journal_states[block] == JOURNAL_STATE_MATCHED ||
        ctx->journal_states[block] == JOURNAL_STATE_BAD)) {
      block++;
      continue;
    }

    uint64_t count = 0;
    while(block + count < block_limit && count < slab_blocks) {
      if(ctx->journal_states &&
         (ctx->journal_states[block + count] == JOURNAL_STATE_MATCHED ||
          ctx->journal_states[block + count] == JOURNAL_STATE_BAD)) {
        break;
      }
      count++;
    }
    if(count == 0) {
      block++;
      continue;
    }

    if(job_cancelled()) {
      set_err(err, err_size, "cancelled");
      errno = EINTR;
      goto done;
    }
    if((block % REPAIR_PROGRESS_INTERVAL) == 0) {
      char current[160];
      snprintf(current, sizeof(current),
               "Scanning mounted %s block %llu/%llu",
               nested_type_name(img->nested_type),
               (unsigned long long)block,
               (unsigned long long)block_limit);
      job_set_phase("scanning", job_long_from_u64(ctx->info.scanned_blocks),
                    job_long_from_u64(block_limit), current);
    }
    if((block % 4096ULL) == 0) {
      progress_line(ctx,
                    "SCAN_PROGRESS block=%llu matched=%llu repair=%llu hash=%s",
                    (unsigned long long)block,
                    (unsigned long long)ctx->info.matched_blocks,
                    (unsigned long long)ctx->info.repaired_blocks,
                    ctx->info.hash_mode);
    }

    if(repair_scan_slab(ctx, img, mounted_fd, block, count,
                        worker_count, slab_bytes, stored_slab,
                        mounted_slab, decoded_slab, expected_hashes,
                        slots, threads, err, err_size) != 0) {
      goto done;
    }
    block += count;
  }

  uint32_t phase = block_limit < img->block_count
      ? JOURNAL_PHASE_SCAN
      : JOURNAL_PHASE_SCAN_DONE;
  if(journal_flush_scan_phase(ctx, block_limit, phase, 1, err, err_size) != 0) {
    goto done;
  }
  job_set_phase("scanning", job_long_from_u64(ctx->info.scanned_blocks),
                job_long_from_u64(block_limit),
                block_limit < img->block_count ?
                "Mounted image sample complete" :
                "Mounted image scan complete");
  progress_line(ctx, "%s matched=%llu repair=%llu scanned=%llu/%llu",
                block_limit < img->block_count ? "SCAN_SAMPLE_DONE" : "SCAN_DONE",
                (unsigned long long)ctx->info.matched_blocks,
                (unsigned long long)ctx->info.repaired_blocks,
                (unsigned long long)ctx->info.scanned_blocks,
                (unsigned long long)img->block_count);
  fsync(fileno(ctx->bad_tsv));
  rc = 0;
done:
  free(stored_slab);
  free(mounted_slab);
  free(decoded_slab);
  free(expected_hashes);
  free(slots);
  free(threads);
  return rc;
}

static int
compute_new_offsets(repair_ctx_t *ctx, const pfsc_image_t *img,
                    char *err, size_t err_size) {
  ctx->new_offsets = calloc((size_t)(img->block_count + 1ULL),
                            sizeof(*ctx->new_offsets));
  if(!ctx->new_offsets) {
    set_err(err, err_size, "out of memory");
    errno = ENOMEM;
    return -1;
  }
  ctx->new_offsets[0] = img->header_size;
  for(uint64_t i = 0; i < img->block_count; i++) {
    uint64_t old_len = img->offsets[i + 1ULL] - img->offsets[i];
    uint64_t new_len = ctx->bad_blocks[i] ? PFS_BLOCK_SIZE : old_len;
    if(new_len > UINT64_MAX - ctx->new_offsets[i]) {
      set_err(err, err_size, "repaired PFSC size overflow");
      errno = EOVERFLOW;
      return -1;
    }
    ctx->new_offsets[i + 1ULL] = ctx->new_offsets[i] + new_len;
  }
  ctx->info.old_stored_size = img->stored_size;
  ctx->info.new_stored_size = ctx->new_offsets[img->block_count];
  return 0;
}

static int
write_pfsc_header_to_fd(int fd, const pfsc_image_t *img,
                        const uint64_t *offsets,
                        char *err, size_t err_size) {
  unsigned char *header = calloc(1, (size_t)img->header_size);
  if(!header) {
    set_err(err, err_size, "out of memory");
    errno = ENOMEM;
    return -1;
  }
  le32(header + 0x00, PFSC_MAGIC);
  le32(header + 0x04, PFSC_UNK4);
  le32(header + 0x08, PFSC_UNK8);
  le32(header + 0x0c, (uint32_t)PFS_BLOCK_SIZE);
  le64(header + 0x10, PFS_BLOCK_SIZE);
  le64(header + 0x18, PFSC_BLOCK_OFFSETS_OFFSET);
  le64(header + 0x20, img->header_size);
  le64(header + 0x28, img->logical_size);
  for(uint64_t i = 0; i <= img->block_count; i++) {
    le64(header + PFSC_BLOCK_OFFSETS_OFFSET + i * 8ULL, offsets[i]);
  }
  int rc = write_exact_at(fd, header, (size_t)img->header_size,
                          img->file_start, err, err_size);
  free(header);
  return rc;
}

static int
write_pfsc_header(const pfsc_image_t *img, const uint64_t *offsets,
                  char *err, size_t err_size) {
  return write_pfsc_header_to_fd(img->fd, img, offsets, err, err_size);
}

static int
update_outer_metadata_fd(int fd, const pfsc_image_t *img,
                         uint64_t new_stored_size,
                         char *err, size_t err_size) {
  uint64_t file_blocks = ceil_div_u64(new_stored_size, PFS_BLOCK_SIZE);
  if(file_blocks == 0) file_blocks = 1;
  uint64_t final_ndblock = 6ULL + file_blocks;
  uint64_t final_size = final_ndblock * PFS_BLOCK_SIZE;
  unsigned char raw8[8];
  unsigned char raw4[4];
  if(ftruncate(fd, (off_t)final_size) != 0) {
    set_err(err, err_size, "truncate repaired image: %s", strerror(errno));
    return -1;
  }
  le64(raw8, final_ndblock);
  if(write_exact_at(fd, raw8, sizeof(raw8), 0x38, err, err_size) != 0) {
    return -1;
  }
  uint64_t inode_off = PFS_BLOCK_SIZE + 3ULL * PFS_INODE_SIZE;
  le64(raw8, new_stored_size);
  if(write_exact_at(fd, raw8, sizeof(raw8), inode_off + 0x08,
                    err, err_size) != 0) {
    return -1;
  }
  /* The outer inode stores the real nested image size; PFSC stores block-rounded logical size. */
  le64(raw8, img->nested_size);
  if(write_exact_at(fd, raw8, sizeof(raw8), inode_off + 0x10,
                    err, err_size) != 0) {
    return -1;
  }
  le32(raw4, (uint32_t)file_blocks);
  if(write_exact_at(fd, raw4, sizeof(raw4), inode_off + 0x60,
                    err, err_size) != 0) {
    return -1;
  }
  return 0;
}

static int
update_outer_metadata(const pfsc_image_t *img, uint64_t new_stored_size,
                      char *err, size_t err_size) {
  return update_outer_metadata_fd(img->fd, img, new_stored_size,
                                  err, err_size);
}

static int
fsync_parent_dir_best_effort(const char *path) {
  char parent[1024];
  if(path_parent(path, parent, sizeof(parent)) != 0) return -1;
  int fd = open(parent, O_RDONLY);
  if(fd < 0) return -1;
  fsync(fd);
  close(fd);
  return 0;
}

static int
open_repair_temp_file(repair_ctx_t *ctx, mode_t source_mode,
                      char *err, size_t err_size) {
  char parent[1024];
  char name[160];
  if(path_parent(ctx->info.source_path, parent, sizeof(parent)) != 0) {
    set_err(err, err_size, "repair temp parent path too long");
    return -1;
  }
  mode_t mode = source_mode & 0777;
  if(mode == 0) mode = 0666;
  for(unsigned attempt = 0; attempt < 32; attempt++) {
    int n = snprintf(name, sizeof(name), ".%s.repair.%ld.%ld.%u.tmp",
                     ctx->info.title_id[0] ? ctx->info.title_id : "pfsc",
                     (long)getpid(), (long)time(NULL), attempt);
    if(n < 0 || (size_t)n >= sizeof(name) ||
       path_join(ctx->info.temp_path, sizeof(ctx->info.temp_path),
                 parent, name) != 0) {
      set_err(err, err_size, "repair temp path too long");
      return -1;
    }
    int fd = open(ctx->info.temp_path, O_CREAT | O_EXCL | O_RDWR, mode);
    if(fd >= 0) {
      fchmod(fd, mode);
      return fd;
    }
    if(errno != EEXIST) {
      set_err(err, err_size, "create repair temp: %s", strerror(errno));
      return -1;
    }
  }
  set_err(err, err_size, "create repair temp: name collision");
  errno = EEXIST;
  return -1;
}

static unsigned char *
repair_alloc_copy_buffer(size_t *cap_out) {
  static const uint64_t tries[] = {
    REPAIR_COPY_CHUNK_MAX,
    REPAIR_COPY_CHUNK_MID,
    REPAIR_COPY_CHUNK_LOW,
    REPAIR_COPY_CHUNK_MIN,
  };
  if(cap_out) *cap_out = 0;
  for(size_t i = 0; i < sizeof(tries) / sizeof(tries[0]); i++) {
    if(tries[i] > (uint64_t)SIZE_MAX) continue;
    unsigned char *buf = malloc((size_t)tries[i]);
    if(buf) {
      if(cap_out) *cap_out = (size_t)tries[i];
      return buf;
    }
  }
  return NULL;
}

static int
copy_bytes_between_fds_serial(int src_fd, int dst_fd, uint64_t size,
                              uint64_t src_offset, uint64_t dst_offset,
                              unsigned char *buf, size_t buf_cap,
                              char *err, size_t err_size) {
  uint64_t done = 0;
  while(done < size) {
    size_t chunk = (size - done) > (uint64_t)buf_cap ?
        buf_cap : (size_t)(size - done);
    if(read_exact_at(src_fd, buf, chunk, src_offset + done,
                     err, err_size) != 0) {
      return -1;
    }
    job_add_bytes(&g_job.repair_read_bytes, chunk);
    if(write_exact_at(dst_fd, buf, chunk, dst_offset + done,
                      err, err_size) != 0) {
      return -1;
    }
    job_add_bytes(&g_job.repair_written_bytes, chunk);
    job_add_bytes(&g_job.repair_copy_bytes, chunk);
    done += chunk;
  }
  return 0;
}

typedef enum repair_copy_slot_state {
  REPAIR_COPY_SLOT_FREE = 0,
  REPAIR_COPY_SLOT_FILLING = 1,
  REPAIR_COPY_SLOT_FULL = 2,
} repair_copy_slot_state_t;

typedef struct repair_copy_slot {
  unsigned char *buf;
  size_t len;
  uint64_t offset;
  int error;
  char error_text[256];
  repair_copy_slot_state_t state;
} repair_copy_slot_t;

typedef struct repair_copy_pipe {
  pthread_mutex_t lock;
  pthread_cond_t cond;
  int src_fd;
  uint64_t size;
  uint64_t src_offset;
  size_t buf_cap;
  int stop;
  int reader_done;
  repair_copy_slot_t slots[2];
} repair_copy_pipe_t;

static void *
repair_copy_reader_main(void *arg) {
  repair_copy_pipe_t *pipe = arg;
  uint64_t done = 0;
  int slot_index = 0;

  while(done < pipe->size) {
    repair_copy_slot_t *slot = &pipe->slots[slot_index];
    pthread_mutex_lock(&pipe->lock);
    while(!pipe->stop && slot->state != REPAIR_COPY_SLOT_FREE) {
      uint64_t wait_started = monotonic_us();
      pthread_cond_wait(&pipe->cond, &pipe->lock);
      job_add_wait_us(&g_job.worker_wait_us, wait_started);
    }
    if(pipe->stop) {
      pthread_mutex_unlock(&pipe->lock);
      break;
    }
    slot->state = REPAIR_COPY_SLOT_FILLING;
    pthread_mutex_unlock(&pipe->lock);

    size_t chunk = (pipe->size - done) > (uint64_t)pipe->buf_cap ?
        pipe->buf_cap : (size_t)(pipe->size - done);
    char read_err[256] = {0};
    if(read_exact_at(pipe->src_fd, slot->buf, chunk,
                     pipe->src_offset + done,
                     read_err, sizeof(read_err)) != 0) {
      pthread_mutex_lock(&pipe->lock);
      slot->len = 0;
      slot->offset = done;
      slot->error = errno ? errno : EIO;
      snprintf(slot->error_text, sizeof(slot->error_text), "%s",
               read_err[0] ? read_err : strerror(slot->error));
      slot->state = REPAIR_COPY_SLOT_FULL;
      pipe->stop = 1;
      pipe->reader_done = 1;
      pthread_cond_broadcast(&pipe->cond);
      pthread_mutex_unlock(&pipe->lock);
      return NULL;
    }
    job_add_bytes(&g_job.repair_read_bytes, chunk);

    pthread_mutex_lock(&pipe->lock);
    slot->len = chunk;
    slot->offset = done;
    slot->error = 0;
    slot->error_text[0] = 0;
    slot->state = REPAIR_COPY_SLOT_FULL;
    pthread_cond_broadcast(&pipe->cond);
    pthread_mutex_unlock(&pipe->lock);

    done += (uint64_t)chunk;
    slot_index ^= 1;
  }

  pthread_mutex_lock(&pipe->lock);
  pipe->reader_done = 1;
  pthread_cond_broadcast(&pipe->cond);
  pthread_mutex_unlock(&pipe->lock);
  return NULL;
}

static int
copy_bytes_between_fds(int src_fd, int dst_fd, uint64_t size,
                       uint64_t src_offset, uint64_t dst_offset,
                       char *err, size_t err_size) {
  if(size == 0) return 0;
  size_t buf_cap = 0;
  unsigned char *buf0 = repair_alloc_copy_buffer(&buf_cap);
  if(!buf0) {
    set_err(err, err_size, "out of memory");
    errno = ENOMEM;
    return -1;
  }
  unsigned char *buf1 = malloc(buf_cap);
  if(!buf1) {
    int serial_rc = copy_bytes_between_fds_serial(
        src_fd, dst_fd, size, src_offset, dst_offset, buf0, buf_cap,
        err, err_size);
    free(buf0);
    return serial_rc;
  }

  repair_copy_pipe_t pipe;
  memset(&pipe, 0, sizeof(pipe));
  pipe.src_fd = src_fd;
  pipe.size = size;
  pipe.src_offset = src_offset;
  pipe.buf_cap = buf_cap;
  pipe.slots[0].buf = buf0;
  pipe.slots[1].buf = buf1;
  int mutex_initialized = 0;
  int cond_initialized = 0;
  pthread_t reader;
  int reader_started = 0;
  int rc = -1;

  if(pthread_mutex_init(&pipe.lock, NULL) != 0) {
    set_err(err, err_size, "init copy pipe failed");
    goto out;
  }
  mutex_initialized = 1;
  if(pthread_cond_init(&pipe.cond, NULL) != 0) {
    set_err(err, err_size, "init copy pipe failed");
    goto out;
  }
  cond_initialized = 1;
  int trc = pthread_create(&reader, NULL, repair_copy_reader_main, &pipe);
  if(trc != 0) {
    set_err(err, err_size, "start copy reader: %s", strerror(trc));
    goto out;
  }
  reader_started = 1;

  uint64_t done = 0;
  int slot_index = 0;
  while(done < size) {
    repair_copy_slot_t *slot = &pipe.slots[slot_index];
    pthread_mutex_lock(&pipe.lock);
    while(slot->state != REPAIR_COPY_SLOT_FULL &&
          !pipe.reader_done &&
          !pipe.stop) {
      uint64_t wait_started = monotonic_us();
      pthread_cond_wait(&pipe.cond, &pipe.lock);
      job_add_wait_us(&g_job.writer_wait_us, wait_started);
    }
    if(slot->state != REPAIR_COPY_SLOT_FULL) {
      pthread_mutex_unlock(&pipe.lock);
      set_err(err, err_size, "copy reader stopped unexpectedly");
      errno = EIO;
      goto out;
    }
    size_t len = slot->len;
    uint64_t offset = slot->offset;
    int slot_error = slot->error;
    char slot_error_text[256];
    snprintf(slot_error_text, sizeof(slot_error_text), "%s", slot->error_text);
    pthread_mutex_unlock(&pipe.lock);

    if(slot_error) {
      set_err(err, err_size, "%s", slot_error_text[0] ?
              slot_error_text : strerror(slot_error));
      errno = slot_error;
      goto out;
    }
    if(write_exact_at(dst_fd, slot->buf, len, dst_offset + offset,
                      err, err_size) != 0) {
      goto out;
    }
    job_add_bytes(&g_job.repair_written_bytes, len);
    job_add_bytes(&g_job.repair_copy_bytes, len);

    pthread_mutex_lock(&pipe.lock);
    slot->state = REPAIR_COPY_SLOT_FREE;
    pthread_cond_broadcast(&pipe.cond);
    pthread_mutex_unlock(&pipe.lock);

    done += (uint64_t)len;
    slot_index ^= 1;
  }
  rc = 0;
out:
  if(mutex_initialized && cond_initialized) {
    pthread_mutex_lock(&pipe.lock);
    pipe.stop = 1;
    pthread_cond_broadcast(&pipe.cond);
    pthread_mutex_unlock(&pipe.lock);
  }
  if(reader_started) pthread_join(reader, NULL);
  if(cond_initialized) {
    pthread_cond_destroy(&pipe.cond);
  }
  if(mutex_initialized) {
    pthread_mutex_destroy(&pipe.lock);
  }
  free(buf0);
  free(buf1);
  return rc;
}

static int
copy_range_backward(int fd, uint64_t src_offset, uint64_t dst_offset,
                    uint64_t size, unsigned char *buf, size_t buf_cap,
                    char *err, size_t err_size) {
  if(size == 0 || src_offset == dst_offset) return 0;
  if(!buf || buf_cap == 0) {
    set_err(err, err_size, "copy buffer unavailable");
    errno = ENOMEM;
    return -1;
  }
  uint64_t remaining = size;
  while(remaining > 0) {
    size_t chunk = remaining > (uint64_t)buf_cap ?
        buf_cap : (size_t)remaining;
    uint64_t pos = remaining - (uint64_t)chunk;
    if(read_exact_at(fd, buf, chunk, src_offset + pos,
                     err, err_size) != 0) {
      return -1;
    }
    job_add_bytes(&g_job.repair_read_bytes, chunk);
    if(write_exact_at(fd, buf, chunk, dst_offset + pos,
                      err, err_size) != 0) {
      return -1;
    }
    job_add_bytes(&g_job.repair_written_bytes, chunk);
    job_add_bytes(&g_job.repair_copy_bytes, chunk);
    remaining = pos;
  }
  return 0;
}

static int
copy_replace_repair(repair_ctx_t *ctx, pfsc_image_t *img, int mounted_fd,
                    int temp_only, int force_rebuild,
                    char *err, size_t err_size) {
  unsigned char *stored = malloc((size_t)PFS_BLOCK_SIZE);
  unsigned char *decoded = malloc((size_t)PFS_BLOCK_SIZE);
  int temp_fd = -1;
  int temp_active = 0;
  int renamed = 0;
  int rc = -1;
  struct stat st;

  if(!stored || !decoded) {
    set_err(err, err_size, "out of memory");
    errno = ENOMEM;
    goto done;
  }
  if(fstat(img->fd, &st) != 0) {
    set_err(err, err_size, "stat input: %s", strerror(errno));
    goto done;
  }
  snprintf(ctx->info.output_path, sizeof(ctx->info.output_path), "%s",
           ctx->info.source_path);

  progress_line(ctx, "COPY_REPLACE_START old_stored=%llu",
                (unsigned long long)img->stored_size);

  ctx->info.old_stored_size = img->stored_size;
  progress_line(ctx, "SCAN mounted=%s nested=%s type=%s blocks=%llu",
                ctx->info.mounted_path, ctx->info.nested_name,
                nested_type_name(ctx->info.nested_type),
                (unsigned long long)img->block_count);
  if(scan_blocks(ctx, img, mounted_fd, err, err_size) != 0) goto done;
  uint64_t rebuild_limit = temp_only && ctx->block_limit &&
      ctx->block_limit < img->block_count ? ctx->block_limit : img->block_count;

  if(ctx->info.repaired_blocks == 0 && !force_rebuild) {
    ctx->info.noop = 1;
    ctx->info.new_stored_size = ctx->info.old_stored_size;
    if(journal_remove_after_scan(ctx, err, err_size) != 0) goto done;
    progress_line(ctx, "JOURNAL_REMOVED path=%s", ctx->info.journal_path);
    job_set_phase("done", job_long_from_u64(ctx->info.scanned_blocks),
                  job_long_from_u64(img->block_count),
                  "PFSC repair complete - no bad blocks found");
    progress_line(ctx, "NOOP matched=%llu copy_replace_temp_created=0",
                  (unsigned long long)ctx->info.matched_blocks);
    write_summary(ctx, "noop", "");
    rc = 0;
    goto done;
  }
  if(ctx->info.repaired_blocks == 0) {
    ctx->info.noop = 1;
  }

  job_set_phase("planning", job_long_from_u64(ctx->info.scanned_blocks),
                job_long_from_u64(img->block_count),
                "Planning PFSC copy-replace repair");
  if(compute_new_offsets(ctx, img, err, err_size) != 0) goto done;

  temp_fd = open_repair_temp_file(ctx, st.st_mode, err, err_size);
  if(temp_fd < 0) goto done;
  temp_active = 1;
  progress_line(ctx, "COPY_REPLACE_TEMP temp=%s prefix=%llu",
                ctx->info.temp_path,
                (unsigned long long)(img->file_start + img->header_size));

  job_set_phase("copying", 0, job_long_from_u64(ctx->info.new_stored_size),
                "Rebuilding repaired PFSC image");
  atomic_store(&g_job.copied_bytes, 0);
  atomic_store(&g_job.repaired_blocks, 0);
  progress_line(ctx, "COPY_REPLACE_REBUILD_LIMIT blocks=%llu/%llu",
                (unsigned long long)rebuild_limit,
                (unsigned long long)img->block_count);
  if(copy_bytes_between_fds(img->fd, temp_fd,
                            img->file_start + img->header_size,
                            0, 0, err, err_size) != 0) {
    goto done;
  }

  uint64_t block = 0;
  while(block < rebuild_limit) {
    if(job_cancelled()) {
      set_err(err, err_size, "cancelled");
      errno = EINTR;
      goto done;
    }
    if(!ctx->bad_blocks[block]) {
      uint64_t run_start = block;
      while(block < rebuild_limit && !ctx->bad_blocks[block]) {
        block++;
      }
      uint64_t old_start = img->offsets[run_start];
      uint64_t old_end = img->offsets[block];
      uint64_t new_start = ctx->new_offsets[run_start];
      uint64_t new_end = ctx->new_offsets[block];
      if(old_end < old_start || new_end < new_start ||
         new_end - new_start != old_end - old_start) {
        set_err(err, err_size, "invalid coalesced PFSC copy range");
        errno = EINVAL;
        goto done;
      }
      uint64_t run_size = old_end - old_start;
      if(run_size > 0 &&
         copy_bytes_between_fds(img->fd, temp_fd, run_size,
                                img->file_start + old_start,
                                img->file_start + new_start,
                                err, err_size) != 0) {
        goto done;
      }
      ctx->info.bytes_moved += run_size;
    } else {
      uint64_t old_start = img->offsets[block];
      uint64_t old_len = img->offsets[block + 1ULL] - img->offsets[block];
      size_t stored_len = 0;
      if(pfsc_decode_block(img, block, stored, decoded, &stored_len,
                           err, err_size) != 0) {
        goto done;
      }
      if(write_exact_at(temp_fd, decoded, (size_t)PFS_BLOCK_SIZE,
                        img->file_start + ctx->new_offsets[block],
                        err, err_size) != 0) {
        goto done;
      }
      job_add_bytes(&g_job.repair_written_bytes, PFS_BLOCK_SIZE);
      job_add_bytes(&g_job.repaired_blocks, 1);
      ctx->info.bytes_moved += PFS_BLOCK_SIZE;
      progress_line(ctx,
                    "RAW_ADDED block=%llu old_offset=%llu new_offset=%llu old_len=%llu",
                    (unsigned long long)block,
                    (unsigned long long)old_start,
                    (unsigned long long)ctx->new_offsets[block],
                    (unsigned long long)old_len);
      block++;
    }
    atomic_store(&g_job.copied_bytes,
                 ctx->info.bytes_moved > (uint64_t)LONG_MAX ? LONG_MAX :
                 (long)ctx->info.bytes_moved);
    atomic_store(&g_job.done_files,
                 block > (uint64_t)INT_MAX ? INT_MAX : (int)block);
    if((block % 4096ULL) == 0 || block == rebuild_limit) {
      progress_line(ctx,
                    "COPY_REPLACE_WRITE_PROGRESS block=%llu/%llu written=%llu",
                    (unsigned long long)block,
                    (unsigned long long)rebuild_limit,
                    (unsigned long long)ctx->info.bytes_moved);
    }
  }

  if(temp_only && rebuild_limit < img->block_count) {
    if(fsync(temp_fd) != 0) {
      set_err(err, err_size, "sync repair temp: %s", strerror(errno));
      goto done;
    }
    if(close(temp_fd) != 0) {
      temp_fd = -1;
      set_err(err, err_size, "close repair temp: %s", strerror(errno));
      goto done;
    }
    temp_fd = -1;
    if(journal_remove_after_scan(ctx, err, err_size) != 0) goto done;
    progress_line(ctx, "JOURNAL_REMOVED path=%s", ctx->info.journal_path);
    progress_line(ctx,
                  "BENCH_TEMP_SAMPLE_DONE blocks=%llu/%llu repaired=%llu bytes_written=%llu temp=%s",
                  (unsigned long long)rebuild_limit,
                  (unsigned long long)img->block_count,
                  (unsigned long long)ctx->info.repaired_blocks,
                  (unsigned long long)ctx->info.bytes_moved,
                  ctx->info.temp_path);
    write_summary(ctx, "bench-temp-sampled", "");
    if(unlink(ctx->info.temp_path) != 0 && errno != ENOENT) {
      set_err(err, err_size, "remove repair temp: %s", strerror(errno));
      goto done;
    }
    temp_active = 0;
    job_set_phase("done", job_long_from_u64(rebuild_limit),
                  job_long_from_u64(img->block_count),
                  "PFSC temp rebuild sample complete");
    rc = 0;
    goto done;
  }

  job_set_phase("metadata", job_long_from_u64(img->block_count),
                job_long_from_u64(img->block_count),
                "Writing PFSC repair metadata");
  if(write_pfsc_header_to_fd(temp_fd, img, ctx->new_offsets,
                             err, err_size) != 0 ||
     update_outer_metadata_fd(temp_fd, img, ctx->info.new_stored_size,
                              err, err_size) != 0) {
    goto done;
  }
  fsync(temp_fd);
  if(close(temp_fd) != 0) {
    temp_fd = -1;
    set_err(err, err_size, "close repair temp: %s", strerror(errno));
    goto done;
  }
  temp_fd = -1;

  if(temp_only) {
    if(journal_remove_after_scan(ctx, err, err_size) != 0) goto done;
    progress_line(ctx, "JOURNAL_REMOVED path=%s", ctx->info.journal_path);
    write_patch_request(ctx, img);
    progress_line(ctx,
                  "BENCH_TEMP_DONE repaired=%llu bytes_written=%llu temp=%s",
                  (unsigned long long)ctx->info.repaired_blocks,
                  (unsigned long long)ctx->info.bytes_moved,
                  ctx->info.temp_path);
    write_summary(ctx, ctx->info.repaired_blocks ? "bench-temp-repaired" :
                  "bench-temp-noop", "");
    if(unlink(ctx->info.temp_path) != 0 && errno != ENOENT) {
      set_err(err, err_size, "remove repair temp: %s", strerror(errno));
      goto done;
    }
    temp_active = 0;
    job_set_phase("done", job_long_from_u64(img->block_count),
                  job_long_from_u64(img->block_count),
                  "PFSC temp rebuild complete");
    rc = 0;
    goto done;
  }

  if(journal_remove_after_scan(ctx, err, err_size) != 0) goto done;
  progress_line(ctx, "JOURNAL_REMOVED path=%s", ctx->info.journal_path);
  write_patch_request(ctx, img);

  int source_fd = img->fd;
  img->fd = -1;
  if(source_fd >= 0 && close(source_fd) != 0) {
    set_err(err, err_size, "close input before rename: %s", strerror(errno));
    goto done;
  }
  if(rename(ctx->info.temp_path, ctx->info.source_path) != 0) {
    set_err(err, err_size, "replace repaired image: %s", strerror(errno));
    goto done;
  }
  temp_active = 0;
  renamed = 1;
  fsync_parent_dir_best_effort(ctx->info.source_path);
  if(repair_verify_repaired_smoke(ctx, img, ctx->info.source_path,
                                  mounted_fd, 0, err, err_size) != 0) {
    goto done;
  }
  job_set_phase("done", job_long_from_u64(img->block_count),
                job_long_from_u64(img->block_count),
                "PFSC repair complete");
  progress_line(ctx, "DONE repaired=%llu bytes_written=%llu temp=%s",
                (unsigned long long)ctx->info.repaired_blocks,
                (unsigned long long)ctx->info.bytes_moved,
                ctx->info.temp_path);
  write_summary(ctx, "repaired", "");
  rc = 0;

done:
  if(temp_fd >= 0) close(temp_fd);
  if(rc != 0 && temp_active && !renamed && ctx->info.temp_path[0]) {
    unlink(ctx->info.temp_path);
  }
  free(stored);
  free(decoded);
  return rc;
}

static int
apply_repair(repair_ctx_t *ctx, pfsc_image_t *img,
             char *err, size_t err_size) {
  unsigned char *stored = malloc((size_t)PFS_BLOCK_SIZE);
  unsigned char *decoded = malloc((size_t)PFS_BLOCK_SIZE);
  size_t copy_cap = 0;
  unsigned char *copy_buf = repair_alloc_copy_buffer(&copy_cap);
  int rc = -1;
  if(!stored || !decoded || !copy_buf) {
    set_err(err, err_size, "out of memory");
    errno = ENOMEM;
    goto done;
  }

  uint64_t new_file_blocks =
      ceil_div_u64(ctx->info.new_stored_size, PFS_BLOCK_SIZE);
  if(new_file_blocks == 0) new_file_blocks = 1;
  uint64_t new_final_size = (6ULL + new_file_blocks) * PFS_BLOCK_SIZE;
  if(ftruncate(img->fd, (off_t)new_final_size) != 0) {
    set_err(err, err_size, "extend repaired image: %s", strerror(errno));
    goto done;
  }

  job_set_phase("repairing", 0, job_long_from_u64(img->block_count),
                "Repairing PFSC blocks");
  atomic_store(&g_job.total_bytes,
	               ctx->info.new_stored_size > (uint64_t)LONG_MAX ? LONG_MAX :
	               (long)ctx->info.new_stored_size);
  atomic_store(&g_job.copied_bytes, 0);
  atomic_store(&g_job.total_files,
               img->block_count > (uint64_t)INT_MAX ? INT_MAX :
               (int)img->block_count);
  atomic_store(&g_job.done_files, 0);
  atomic_store(&g_job.raw_blocks,
               ctx->info.repaired_blocks > (uint64_t)LONG_MAX ? LONG_MAX :
               (long)ctx->info.repaired_blocks);
  repair_store_found_counters(ctx);
  atomic_store(&g_job.repaired_blocks, 0);
  atomic_store(&g_job.compressed_blocks,
               ctx->info.matched_blocks > (uint64_t)LONG_MAX ? LONG_MAX :
               (long)ctx->info.matched_blocks);

  for(uint64_t remaining = img->block_count; remaining > 0;) {
    if(job_cancelled()) {
      set_err(err, err_size, "cancelled");
      errno = EINTR;
      goto done;
    }
    uint64_t i = remaining - 1ULL;
    uint64_t processed = img->block_count - remaining;
    if((processed % REPAIR_PROGRESS_INTERVAL) == 0 ||
       remaining == img->block_count) {
      char current[160];
      snprintf(current, sizeof(current), "Repairing PFSC block %llu/%llu",
	               (unsigned long long)i,
	               (unsigned long long)img->block_count);
      job_set_phase("repairing", job_long_from_u64(processed),
                    job_long_from_u64(img->block_count), current);
    }
    if((processed % 4096ULL) == 0 || remaining == img->block_count) {
      progress_line(ctx, "APPLY_PROGRESS block=%llu bytes_moved=%llu",
	                    (unsigned long long)i,
	                    (unsigned long long)ctx->info.bytes_moved);
    }
    uint64_t old_start = img->offsets[i];
    uint64_t old_len = img->offsets[i + 1ULL] - img->offsets[i];
    uint64_t new_start = ctx->new_offsets[i];
    uint64_t new_len = ctx->new_offsets[i + 1ULL] - ctx->new_offsets[i];
    size_t stored_len = 0;
    if(ctx->bad_blocks[i]) {
      if(pfsc_decode_block(img, i, stored, decoded, &stored_len,
                           err, err_size) != 0) {
        goto done;
      }
      if(write_exact_at(img->fd, decoded, (size_t)PFS_BLOCK_SIZE,
                        img->file_start + new_start, err, err_size) != 0) {
        goto done;
      }
      job_add_bytes(&g_job.repair_written_bytes, PFS_BLOCK_SIZE);
      job_add_bytes(&g_job.repaired_blocks, 1);
      ctx->info.bytes_moved += PFS_BLOCK_SIZE;
      progress_line(ctx,
                    "RAW_ADDED block=%llu old_offset=%llu new_offset=%llu old_len=%zu",
                    (unsigned long long)i,
                    (unsigned long long)old_start,
                    (unsigned long long)new_start,
                    stored_len);
      remaining--;
    } else {
      if(old_len > PFS_BLOCK_SIZE || new_len != old_len) {
        set_err(err, err_size, "invalid non-repair block span");
        errno = EINVAL;
        goto done;
      }
      if(new_start < old_start) {
        set_err(err, err_size, "invalid backward repair offset");
        errno = EINVAL;
        goto done;
      }
      uint64_t delta = new_start - old_start;
      uint64_t run_start = i;
      while(run_start > 0) {
        uint64_t prev = run_start - 1ULL;
        if(ctx->bad_blocks[prev]) break;
        uint64_t prev_old_start = img->offsets[prev];
        uint64_t prev_old_len = img->offsets[prev + 1ULL] - img->offsets[prev];
        uint64_t prev_new_start = ctx->new_offsets[prev];
        uint64_t prev_new_len =
            ctx->new_offsets[prev + 1ULL] - ctx->new_offsets[prev];
        if(prev_old_len > PFS_BLOCK_SIZE || prev_new_len != prev_old_len ||
           prev_new_start < prev_old_start ||
           prev_new_start - prev_old_start != delta) {
          break;
        }
        run_start = prev;
      }

      uint64_t old_range_start = img->offsets[run_start];
      uint64_t old_range_end = img->offsets[remaining];
      uint64_t new_range_start = ctx->new_offsets[run_start];
      uint64_t run_size = old_range_end - old_range_start;
      if(delta != 0) {
        if(copy_range_backward(img->fd,
                               img->file_start + old_range_start,
                               img->file_start + new_range_start,
                               run_size, copy_buf, copy_cap,
                               err, err_size) != 0) {
          goto done;
        }
        ctx->info.bytes_moved += run_size;
      }
      remaining = run_start;
    }

    atomic_store(&g_job.copied_bytes,
                 ctx->info.bytes_moved > (uint64_t)LONG_MAX ? LONG_MAX :
                 (long)ctx->info.bytes_moved);
    atomic_store(&g_job.done_files,
                 img->block_count - remaining > (uint64_t)INT_MAX ? INT_MAX :
                 (int)(img->block_count - remaining));
  }

  job_set_phase("metadata", job_long_from_u64(img->block_count),
                job_long_from_u64(img->block_count),
                "Writing PFSC repair metadata");
  if(write_pfsc_header(img, ctx->new_offsets, err, err_size) != 0 ||
     update_outer_metadata(img, ctx->info.new_stored_size, err,
	                           err_size) != 0) {
    goto done;
  }
  fsync(img->fd);
  job_set_phase("done", job_long_from_u64(img->block_count),
                job_long_from_u64(img->block_count),
                "PFSC repair complete");
  rc = 0;
done:
  free(stored);
  free(decoded);
  free(copy_buf);
  return rc;
}

static int
pfs_repair_ffpfsc_run(const char *path, const repair_options_t *opts,
                      pfs_repair_info_t *info,
                      char *err, size_t err_size) {
  repair_ctx_t ctx;
  pfsc_image_t img;
  repair_options_t local_opts;
  int mounted_fd = -1;
  int rc = -1;
  char final_err[256] = {0};

  memset(&ctx, 0, sizeof(ctx));
  memset(&img, 0, sizeof(img));
  memset(&local_opts, 0, sizeof(local_opts));
  local_opts.mode = PFS_REPAIR_MODE_INPLACE;
  if(opts) local_opts = *opts;
  if(local_opts.mode != PFS_REPAIR_MODE_COPY_REPLACE) {
    local_opts.mode = PFS_REPAIR_MODE_INPLACE;
  }
  img.fd = -1;
  ctx.journal_fd = -1;
  ctx.vhash.fd = -1;
  snprintf(ctx.info.hash_mode, sizeof(ctx.info.hash_mode), "%s",
           pfs_vhash_mode_name(PFS_VHASH_MODE_MISSING));
  if(info) memset(info, 0, sizeof(*info));
  if(!path || !ends_with_ci(path, ".ffpfsc")) {
    set_err(err, err_size, "repair requires a .ffpfsc container");
    return -1;
  }
  atomic_store(&g_job.repair_read_bytes, 0);
  atomic_store(&g_job.repair_written_bytes, 0);
  atomic_store(&g_job.repair_copy_bytes, 0);
  snprintf(ctx.info.source_path, sizeof(ctx.info.source_path), "%s", path);
  snprintf(ctx.info.output_path, sizeof(ctx.info.output_path), "%s", path);
  ctx.info.repair_mode = local_opts.mode;
  ctx.info.storage_check_ok = local_opts.storage_check_ok;
  ctx.info.source_size = local_opts.source_size;
  ctx.info.required_free_bytes = local_opts.required_free_bytes;
  ctx.info.available_free_bytes = local_opts.available_free_bytes;
  if(local_opts.storage_error[0]) {
    snprintf(ctx.info.storage_error, sizeof(ctx.info.storage_error), "%s",
             local_opts.storage_error);
  }
  if(repair_id_from_path(path, ctx.info.title_id, sizeof(ctx.info.title_id)) != 0) {
    set_err(err, err_size, "bad .ffpfsc path");
    return -1;
  }

  job_set_phase("opening", 0, 0, "Opening PFSC repair input");
  if(pfsc_open(path,
               local_opts.mode == PFS_REPAIR_MODE_INPLACE &&
                   !local_opts.scan_only,
               &img, err, err_size) != 0) {
    goto done;
  }
  ctx.info.logical_size = img.logical_size;
  ctx.info.nested_size = img.nested_size;
  ctx.info.block_count = img.block_count;
  ctx.info.old_stored_size = img.stored_size;
  snprintf(ctx.info.nested_name, sizeof(ctx.info.nested_name), "%s",
           img.nested_name);
  img.nested_type = nested_type_from_name(img.nested_name);
  ctx.info.nested_type = img.nested_type;

  pfs_vhash_mode_t hash_mode = PFS_VHASH_MODE_MISSING;
  char hash_err[256] = {0};
  int hash_rc = 1;
  hash_rc = pfs_vhash_reader_open_for_image(
      ctx.info.source_path, img.logical_size, img.nested_size,
      img.block_count, img.nested_name, img.nested_type,
      &ctx.vhash, &hash_mode, hash_err, sizeof(hash_err));
  if(hash_rc < 0) {
    set_err(err, err_size, "%s", hash_err[0] ? hash_err :
            "open validation hash failed");
    goto done;
  }
  ctx.vhash_open = hash_rc == 0;
  snprintf(ctx.info.hash_mode, sizeof(ctx.info.hash_mode), "%s",
           pfs_vhash_mode_name(hash_mode));

  {
    char current[160];
    snprintf(current, sizeof(current), "Checking ShadowMount for %s",
             ctx.info.nested_name);
    job_set_phase("finding-mounted-image", 0,
                  job_long_from_u64(img.block_count), current);
  }
  if(find_mounted_nested(ctx.info.source_path, ctx.info.nested_name,
                         ctx.info.mounted_path,
                         sizeof(ctx.info.mounted_path), err, err_size) != 0) {
    goto done;
  }
  mounted_fd = open(ctx.info.mounted_path, O_RDONLY);
  if(mounted_fd < 0) {
    set_err(err, err_size, "open mounted %s: %s",
            ctx.info.nested_name, strerror(errno));
    goto done;
  }
  struct stat mst;
  if(fstat(mounted_fd, &mst) != 0 || mst.st_size < 0 ||
     (uint64_t)mst.st_size < img.nested_size) {
    set_err(err, err_size, "mounted %s is too small", ctx.info.nested_name);
    goto done;
  }

  if(open_outputs(&ctx, &img, err, err_size) != 0) goto done;
  progress_line(&ctx, "START path=%s title=%s nested=%s type=%s%s",
                path, ctx.info.title_id, ctx.info.nested_name,
                nested_type_name(ctx.info.nested_type),
                ctx.info.resumed ? " resume=1" : "");
  progress_line(&ctx, "JOURNAL status=%s path=%s",
                ctx.journal_resume_status[0] ?
                    ctx.journal_resume_status : "new",
                ctx.info.journal_path);
  progress_line(&ctx,
                "STRATEGY mode=%s%s%s%s source=%llu required_free=%llu available_free=%llu storage_check=%s%s%s",
                repair_mode_name(ctx.info.repair_mode),
                local_opts.scan_only ? " scan-only" : "",
                local_opts.temp_only ? " temp-only" : "",
                local_opts.force_rebuild ? " force-rebuild" : "",
                (unsigned long long)ctx.info.source_size,
                (unsigned long long)ctx.info.required_free_bytes,
                (unsigned long long)ctx.info.available_free_bytes,
                ctx.info.storage_check_ok ? "ok" : "failed",
                ctx.info.storage_error[0] ? " error=" : "",
                ctx.info.storage_error[0] ? ctx.info.storage_error : "");
  progress_line(&ctx, "HASH_MODE mode=%s%s%s",
                ctx.info.hash_mode,
                hash_err[0] ? " detail=" : "",
                hash_err[0] ? hash_err : "");
  if(repair_vhash_cache_init(&ctx, err, err_size) != 0) {
    goto done;
  }
  if(ctx.vhash_open && ctx.vhash_cache) {
    progress_line(&ctx, "HASH_CACHE mode=%s blocks=%llu bytes=%llu",
                  ctx.vhash_cache_full ? "full" : "sliding",
                  (unsigned long long)ctx.vhash_cache_cap,
                  (unsigned long long)(ctx.vhash_cache_cap *
                                       (uint64_t)PFS_VHASH_HASH_SIZE));
  }

  ctx.bad_blocks = calloc((size_t)img.block_count, 1);
  if(!ctx.bad_blocks) {
    set_err(err, err_size, "out of memory");
    errno = ENOMEM;
    goto done;
  }
  if(journal_open_or_create(&ctx, &img, err, err_size) != 0) goto done;

  if(local_opts.mode == PFS_REPAIR_MODE_COPY_REPLACE) {
    if(copy_replace_repair(&ctx, &img, mounted_fd, local_opts.temp_only,
                           local_opts.force_rebuild, err, err_size) != 0) {
      goto done;
    }
    rc = 0;
    goto done;
  }

  progress_line(&ctx, "SCAN mounted=%s nested=%s type=%s blocks=%llu",
                ctx.info.mounted_path, ctx.info.nested_name,
                nested_type_name(ctx.info.nested_type),
                (unsigned long long)img.block_count);
  if(scan_blocks(&ctx, &img, mounted_fd, err, err_size) != 0) goto done;
  if(local_opts.scan_only && ctx.block_limit && ctx.block_limit < img.block_count) {
    if(journal_remove_after_scan(&ctx, err, err_size) != 0) goto done;
    progress_line(&ctx, "JOURNAL_REMOVED path=%s", ctx.info.journal_path);
    job_set_phase("done", job_long_from_u64(ctx.info.scanned_blocks),
                  job_long_from_u64(ctx.block_limit),
                  "PFSC repair sample complete");
    write_summary(&ctx, "sampled", "");
    rc = 0;
    goto done;
  }
  job_set_phase("planning", job_long_from_u64(ctx.info.scanned_blocks),
                job_long_from_u64(img.block_count), "Planning PFSC repair");
  if(journal_remove_after_scan(&ctx, err, err_size) != 0) goto done;
  progress_line(&ctx, "JOURNAL_REMOVED path=%s", ctx.info.journal_path);
  if(compute_new_offsets(&ctx, &img, err, err_size) != 0) goto done;
  write_patch_request(&ctx, &img);

  if(ctx.info.repaired_blocks == 0) {
    ctx.info.noop = 1;
    ctx.info.new_stored_size = ctx.info.old_stored_size;
    job_set_phase("done", job_long_from_u64(ctx.info.scanned_blocks),
                  job_long_from_u64(img.block_count),
                  "PFSC repair complete - no bad blocks found");
    progress_line(&ctx, "NOOP matched=%llu",
                  (unsigned long long)ctx.info.matched_blocks);
    write_summary(&ctx, "noop", "");
    rc = 0;
    goto done;
  }

  if(local_opts.scan_only) {
    job_set_phase("planning", job_long_from_u64(ctx.info.scanned_blocks),
                  job_long_from_u64(img.block_count),
                  "PFSC repair needed");
    progress_line(&ctx, "SCAN_ONLY_REPAIR_NEEDED repair_blocks=%llu",
                  (unsigned long long)ctx.info.repaired_blocks);
    write_summary(&ctx, "repair-needed", "");
    rc = PFS_REPAIR_SCAN_REPAIR_NEEDED;
    goto done;
  }

  progress_line(&ctx, "APPLY repair_blocks=%llu old_stored=%llu new_stored=%llu",
                (unsigned long long)ctx.info.repaired_blocks,
                (unsigned long long)ctx.info.old_stored_size,
                (unsigned long long)ctx.info.new_stored_size);
  if(apply_repair(&ctx, &img, err, err_size) != 0) goto done;
  if(repair_verify_repaired_smoke(&ctx, &img, ctx.info.source_path,
                                  mounted_fd, 1, err, err_size) != 0) {
    goto done;
  }
  job_set_phase("done", job_long_from_u64(img.block_count),
                job_long_from_u64(img.block_count), "PFSC repair complete");
  progress_line(&ctx, "DONE repaired=%llu bytes_moved=%llu",
                (unsigned long long)ctx.info.repaired_blocks,
                (unsigned long long)ctx.info.bytes_moved);
  write_summary(&ctx, "repaired", "");
  rc = 0;

done:
  if(rc != 0 && rc != PFS_REPAIR_SCAN_REPAIR_NEEDED) {
    snprintf(final_err, sizeof(final_err), "%s",
             err && err[0] ? err : strerror(errno));
    job_set_phase("failed", job_long_from_u64(ctx.info.scanned_blocks),
                  job_long_from_u64(ctx.info.block_count),
                  final_err[0] ? final_err : "PFSC repair failed");
    if(ctx.journal_fd >= 0 && ctx.journal_states) {
      uint64_t known = journal_known_prefix(ctx.journal_states,
                                            ctx.info.block_count);
      journal_flush_scan(&ctx, known, 1, err, err_size);
    }
    if(ctx.progress) progress_line(&ctx, "ERROR %s", final_err);
    if(ctx.summary_path[0]) write_summary(&ctx, "failed", final_err);
  }
  if(info) *info = ctx.info;
  if(mounted_fd >= 0) close(mounted_fd);
  if(ctx.vhash_open) pfs_vhash_reader_close(&ctx.vhash);
  pfsc_close(&img);
  if(ctx.journal_fd >= 0) close(ctx.journal_fd);
  if(ctx.progress) fclose(ctx.progress);
  if(ctx.bad_tsv) fclose(ctx.bad_tsv);
  free(ctx.vhash_cache);
  free(ctx.bad_blocks);
  free(ctx.journal_states);
  free(ctx.new_offsets);
  return rc;
}

static int
repair_load_bad_blocks_tsv(repair_ctx_t *ctx, const pfsc_image_t *img,
                           char *err, size_t err_size) {
  char bad_path[1024];
  char line[512];
  uint64_t count = 0;
  if(path_join(bad_path, sizeof(bad_path), ctx->info.outdir,
               "bad_blocks.tsv") != 0) {
    set_err(err, err_size, "bad repair smoke path");
    errno = ENAMETOOLONG;
    return -1;
  }
  FILE *f = fopen(bad_path, "r");
  if(!f) {
    set_err(err, err_size, "open repair bad block list: %s",
            strerror(errno));
    return -1;
  }
  while(fgets(line, sizeof(line), f)) {
    char *end = NULL;
    errno = 0;
    unsigned long long block = strtoull(line, &end, 10);
    if(end == line) continue;
    if(errno != 0 || block >= img->block_count) {
      fclose(f);
      set_err(err, err_size, "invalid repair bad block list");
      errno = EINVAL;
      return -1;
    }
    if(!ctx->bad_blocks[block]) {
      ctx->bad_blocks[block] = 1;
      count++;
    }
  }
  fclose(f);
  if(count == 0) {
    set_err(err, err_size, "repair bad block list is empty");
    errno = EINVAL;
    return -1;
  }
  if(ctx->info.repaired_blocks != 0 && count != ctx->info.repaired_blocks) {
    set_err(err, err_size,
            "repair bad block list count mismatch");
    errno = EINVAL;
    return -1;
  }
  ctx->info.repaired_blocks = count;
  return 0;
}

int
pfs_repair_ffpfsc_smoke_verify(const char *path, pfs_repair_info_t *info,
                               char *err, size_t err_size) {
  repair_ctx_t ctx;
  pfsc_image_t img;
  int mounted_fd = -1;
  int rc = -1;

  memset(&ctx, 0, sizeof(ctx));
  memset(&img, 0, sizeof(img));
  img.fd = -1;
  ctx.journal_fd = -1;
  ctx.vhash.fd = -1;

  if(!path || !info || info->repaired_blocks == 0) return 0;
  ctx.info = *info;
  ctx.info.post_verify_blocks = 0;
  ctx.info.post_verify_mount_blocks = 0;
  snprintf(ctx.info.source_path, sizeof(ctx.info.source_path), "%s", path);

  if(pfsc_open(path, 0, &img, err, err_size) != 0) goto done;
  img.nested_type = nested_type_from_name(img.nested_name);
  if(ctx.info.block_count && ctx.info.block_count != img.block_count) {
    set_err(err, err_size, "repair smoke image block count changed");
    errno = EINVAL;
    goto done;
  }
  if(ctx.info.nested_name[0] &&
     strcmp(ctx.info.nested_name, img.nested_name) != 0) {
    set_err(err, err_size, "repair smoke nested image changed");
    errno = EINVAL;
    goto done;
  }
  ctx.info.logical_size = img.logical_size;
  ctx.info.nested_size = img.nested_size;
  ctx.info.block_count = img.block_count;
  ctx.info.nested_type = img.nested_type;
  snprintf(ctx.info.nested_name, sizeof(ctx.info.nested_name), "%s",
           img.nested_name);

  ctx.bad_blocks = calloc((size_t)img.block_count, 1);
  if(!ctx.bad_blocks) {
    set_err(err, err_size, "out of memory");
    errno = ENOMEM;
    goto done;
  }
  if(repair_load_bad_blocks_tsv(&ctx, &img, err, err_size) != 0) {
    goto done;
  }
  if(find_mounted_nested(ctx.info.source_path, ctx.info.nested_name,
                         ctx.info.mounted_path,
                         sizeof(ctx.info.mounted_path), err, err_size) != 0) {
    goto done;
  }
  mounted_fd = open(ctx.info.mounted_path, O_RDONLY);
  if(mounted_fd < 0) {
    set_err(err, err_size, "open mounted %s: %s",
            ctx.info.nested_name, strerror(errno));
    goto done;
  }
  if(repair_verify_repaired_smoke(&ctx, &img, path, mounted_fd, 1,
                                  err, err_size) != 0) {
    goto done;
  }
  *info = ctx.info;
  rc = 0;

done:
  if(mounted_fd >= 0) close(mounted_fd);
  pfsc_close(&img);
  free(ctx.bad_blocks);
  return rc;
}

int
pfs_repair_ffpfsc_auto(const char *path, pfs_repair_info_t *info,
                       char *err, size_t err_size) {
  repair_options_t opts;
  job_set_phase("checking-storage", 0, 0,
                "Checking storage for PFSC repair");
  repair_choose_strategy(path, &opts);
  if(opts.storage_check_ok) {
    char current[192];
    snprintf(current, sizeof(current), "Repair strategy: %s",
             repair_mode_name(opts.mode));
    job_set_phase("checking-storage", 0, 0, current);
  } else if(opts.storage_error[0]) {
    char current[256];
    snprintf(current, sizeof(current),
             "Storage check failed; using in-place repair");
    job_set_phase("checking-storage", 0, 0, current);
  }
  return pfs_repair_ffpfsc_run(path, &opts, info, err, err_size);
}

int
pfs_repair_ffpfsc_scan_only(const char *path, pfs_repair_info_t *info,
                            char *err, size_t err_size) {
  repair_options_t opts;
  memset(&opts, 0, sizeof(opts));
  opts.mode = PFS_REPAIR_MODE_INPLACE;
  opts.scan_only = 1;
  return pfs_repair_ffpfsc_run(path, &opts, info, err, err_size);
}
