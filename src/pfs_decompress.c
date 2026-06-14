/*
 * File Explorer - streamed PFS/PFSC app decompression.
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

#include "miniz.h"
#include "pfs_block_pipeline.h"
#include "pfs_compress.h"
#include "pfs_fs_util.h"
#include "pfs_io_policy.h"
#include "pfs_job_util.h"
#include "transfer_internal.h"
#if defined(GAME_COMPRESSOR_VERSION)
#include "gc_diag.h"
#define PFS_DECOMPRESS_LOG(...) gc_log(__VA_ARGS__)
#else
#define PFS_DECOMPRESS_LOG(...) do { } while(0)
#endif

#define PFS_BLOCK_SIZE 65536ULL
#define PFS_INODE_SIZE 0xA8
#define PFS_VERSION_PS5 2LL
#define PFS_MAGIC 20130315LL

#define PFS_INODE_MODE_DIR 0x4000
#define PFS_INODE_MODE_FILE 0x8000
#define PFS_INODE_FLAG_COMPRESSED 0x1

#define PFS_DIRENT_TYPE_FILE 2
#define PFS_DIRENT_TYPE_DIRECTORY 3

#define PFSC_MAGIC 0x43534650U
#define PFSC_UNK4 0
#define PFSC_UNK8 6
#define PFSC_HEADER_SIZE 0x30
#define PFSC_BLOCK_OFFSETS_OFFSET 0x400
#define PFSC_INITIAL_DATA_OFFSET 0x10000ULL
#define PFSC_OFFSET_ENTRY_SIZE 8

#define PFSC_DECOMPRESS_SLOTS_PER_WORKER 32
#define PFSC_DECOMPRESS_WINDOW_RC_FALLBACK 1
#define PFSC_DECOMPRESS_WINDOW_SLOTS 2
#define PFSC_DECOMPRESS_LANE_MAX_SIZE (32ULL * 1024ULL * 1024ULL)
#define PFSC_DECOMPRESS_LANE_MIN_SIZE (8ULL * 1024ULL * 1024ULL)
#define PFSC_DECOMPRESS_DECODE_CHUNK_BLOCKS 8U
#define PFS_EXTRACT_OUTPUT_BUFFER_SIZE (64U * 1024U * 1024U)
#define PFS_EXTRACT_OUTPUT_BUFFER_MIN_SIZE (64U * 1024U)
#define PFS_IMAGE_OUTPUT_BUFFER_SIZE (128U * 1024U * 1024U)
#define PFSC_OFFSET_READ_CHUNK_SIZE (1U * 1024U * 1024U)
#define PFS_IO_SYSCALL_CHUNK_SIZE (1U * 1024U * 1024U)
#define SHADOW_PFSC_BASE "/mnt/shadowmnt/pfsc"

typedef struct pfsc_reader {
  int fd;
  uint64_t file_start;
  uint64_t logical_size;
  uint64_t nested_size;
  uint64_t block_size;
  uint64_t block_count;
  char nested_name[256];
  char mounted_path[1024];
  uint64_t *offsets;
  unsigned char *stored;
  unsigned char *block;
  unsigned char *mounted_block;
  unsigned char *last_mounted_block;
  uint64_t cached_index;
  uint64_t last_mounted_index;
  uint64_t mounted_stale_blocks;
  uint64_t mounted_calibration_misses;
  int mounted_fd;
  int mounted_enabled;
  int mounted_calibrated;
  int mounted_has_last;
  int cache_valid;
} pfsc_reader_t;

typedef struct pfs_nested_reader {
  pfsc_reader_t pfsc;
  uint64_t block_size;
  uint64_t inode_count;
  uint64_t inode_block_count;
  uint32_t uroot_inode;
} pfs_nested_reader_t;

typedef struct pfs_inode_info {
  uint16_t mode;
  uint32_t flags;
  uint64_t size;
  uint64_t size_comp;
  uint32_t blocks;
  int32_t db0;
} pfs_inode_info_t;

typedef struct pfs_extract_entry {
  char rel[1024];
  char out_path[1024];
  uint32_t inode_num;
  uint64_t image_offset;
  uint64_t first_block;
  uint64_t block_count;
  uint64_t size;
  uint64_t written;
  int fd;
  int done;
} pfs_extract_entry_t;

typedef struct pfs_extract_plan {
  pfs_extract_entry_t *items;
  size_t count;
  size_t cap;
  uint64_t total_bytes;
  uint64_t total_blocks;
} pfs_extract_plan_t;

typedef struct pfsc_decode_pool {
  pthread_mutex_t lock;
  pthread_cond_t cond;
  const pfsc_reader_t *reader;
  pfs_block_slot_t *slots;
  int slot_count;
  int stop;
  int error;
} pfsc_decode_pool_t;

enum pfsc_decomp_phase {
  PFSC_DECOMP_PHASE_IDLE = 0,
  PFSC_DECOMP_PHASE_READ,
  PFSC_DECOMP_PHASE_WRITE,
};

typedef enum pfsc_decomp_task_type {
  PFSC_DECOMP_TASK_READ = 1,
  PFSC_DECOMP_TASK_DECODE,
} pfsc_decomp_task_type_t;

typedef struct pfsc_decomp_block {
  uint32_t stored_len;
  uint8_t ready;
  uint8_t raw;
} pfsc_decomp_block_t;

typedef struct pfsc_decomp_lane {
  uint64_t start_block;
  uint64_t block_count;
  uint64_t stored_offset;
  uint64_t stored_size;
  uint64_t decoded_size;
  int read_done;
  int decode_started;
  unsigned char *stored;
  unsigned char *decoded;
  pfsc_decomp_block_t *blocks;
} pfsc_decomp_lane_t;

typedef struct pfsc_decomp_window {
  uint64_t index;
  uint64_t start_block;
  uint64_t block_count;
  int lane_count;
  int read_tasks_remaining;
  int decode_tasks_remaining;
  int decode_started;
  pfsc_decomp_lane_t *lanes;
} pfsc_decomp_window_t;

typedef struct pfsc_decomp_task {
  pfsc_decomp_task_type_t type;
  pfsc_decomp_window_t *window;
  int lane_index;
  uint32_t first_block;
  uint32_t block_count;
  struct pfsc_decomp_task *next;
} pfsc_decomp_task_t;

typedef struct pfsc_decomp_pool {
  pthread_mutex_t lock;
  pthread_cond_t cond;
  pfsc_decomp_task_t *read_head;
  pfsc_decomp_task_t *read_tail;
  pfsc_decomp_task_t *decode_head;
  pfsc_decomp_task_t *decode_tail;
  pthread_t *threads;
  int thread_count;
  int workers_started;
  int phase;
  int stop;
  int error;
  char error_msg[256];
  int read_permits;
  int decode_permits;
  int running_read;
  int running_decode;
  const pfsc_reader_t *reader;
} pfsc_decomp_pool_t;

typedef struct pfsc_decomp_plan_writer {
  pfs_extract_plan_t *plan;
  size_t entry_index;
  int sync_files;
} pfsc_decomp_plan_writer_t;

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
ffpfsc_path_supported(const char *path) {
  return path && ends_with_ci(path, ".ffpfsc");
}

static int
join_rel(char *out, size_t out_size, const char *parent, const char *name) {
  int n;
  if(!parent || !parent[0]) n = snprintf(out, out_size, "%s", name);
  else n = snprintf(out, out_size, "%s/%s", parent, name);
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
hidden_tmp_path_for_output(const char *output_path, const char *tag,
                           char *out, size_t out_size) {
  char parent[1024];
  char base[256];
  const char *safe_tag = tag && tag[0] ? tag : "gc-temp";
  if(!output_path || !output_path[0] || !out || out_size == 0) {
    errno = EINVAL;
    return -1;
  }
  if(path_parent_base(output_path, parent, sizeof(parent),
                      base, sizeof(base)) != 0) {
    return -1;
  }
  int n = snprintf(out, out_size, "%s%s.%s.%s.tmp",
                   parent, parent[1] ? "/" : "", base, safe_tag);
  if(n < 0 || (size_t)n >= out_size) {
    errno = ENAMETOOLONG;
    return -1;
  }
  return 0;
}

static uint16_t
rd16(const unsigned char *p) {
  return (uint16_t)p[0] | ((uint16_t)p[1] << 8);
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
pfs_decompress_cancel_requested(void) {
  return atomic_load(&g_job.busy) && job_cancelled();
}

static int
read_exact_at(int fd, void *data, size_t size, off_t offset) {
  unsigned char *p = data;
  while(size > 0) {
    if(pfs_decompress_cancel_requested()) {
      errno = EINTR;
      return -1;
    }
    size_t chunk = size > PFS_IO_SYSCALL_CHUNK_SIZE ?
        PFS_IO_SYSCALL_CHUNK_SIZE : size;
    ssize_t n = pread(fd, p, chunk, offset);
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
write_exact_at(int fd, const void *data, size_t size, off_t offset) {
  const unsigned char *p = data;
  while(size > 0) {
    if(pfs_decompress_cancel_requested()) {
      errno = EINTR;
      return -1;
    }
    size_t chunk = size > PFS_IO_SYSCALL_CHUNK_SIZE ?
        PFS_IO_SYSCALL_CHUNK_SIZE : size;
    ssize_t n = pwrite(fd, p, chunk, offset);
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
write_all_fd_cancelable(int fd, const void *data, size_t size) {
  const unsigned char *p = data;
  while(size > 0) {
    if(pfs_decompress_cancel_requested()) {
      errno = EINTR;
      return -1;
    }
    size_t chunk = size > PFS_IO_SYSCALL_CHUNK_SIZE ?
        PFS_IO_SYSCALL_CHUNK_SIZE : size;
    ssize_t n = write(fd, p, chunk);
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

static int
mkdir_one_local(const char *path) {
  if(mkdir(path, 0777) == 0) {
    chmod(path, 0777);
    return 0;
  }
  if(errno == EEXIST) {
    struct stat st;
    if(stat(path, &st) == 0 && S_ISDIR(st.st_mode)) {
      chmod(path, 0777);
      return 0;
    }
  }
  return -1;
}

static long
job_long_from_u64(uint64_t v) {
  return v > (uint64_t)LONG_MAX ? LONG_MAX : (long)v;
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

static int
ffpfsc_stem_from_path(const char *path, const char **stem,
                      size_t *stem_len) {
  const char *name = strrchr(path ? path : "", '/');
  name = name ? name + 1 : path;
  if(!name || !*name || !ffpfsc_path_supported(name)) return -1;
  size_t len = strlen(name);
  size_t ext_len = strlen(".ffpfsc");
  if(len <= ext_len) return -1;
  if(stem) *stem = name;
  if(stem_len) *stem_len = len - ext_len;
  return 0;
}

static int
shadowmount_pfsc_mount_dir(const char *source_path, char *out,
                           size_t out_size) {
  const char *stem = NULL;
  size_t stem_len = 0;
  char mount_name[1024];
  if(ffpfsc_stem_from_path(source_path, &stem, &stem_len) != 0) return -1;
  size_t max_stem = sizeof(mount_name) - 1U - 9U;
  if(stem_len > max_stem) stem_len = max_stem;
  int n = snprintf(mount_name, sizeof(mount_name), "%.*s_%08x",
                   (int)stem_len, stem, fnv1a32_string(source_path));
  if(n < 0 || (size_t)n >= sizeof(mount_name)) return -1;
  n = snprintf(out, out_size, "%s/%s", SHADOW_PFSC_BASE, mount_name);
  return n < 0 || (size_t)n >= out_size ? -1 : 0;
}

static int
mounted_nested_path(const char *source_path, const char *nested_name,
                    char *out, size_t out_size) {
  char mount_dir[1024];
  if(!nested_name || !path_segment_supported(nested_name)) return -1;
  if(shadowmount_pfsc_mount_dir(source_path, mount_dir,
                                sizeof(mount_dir)) != 0) {
    return -1;
  }
  int n = snprintf(out, out_size, "%s/%s", mount_dir, nested_name);
  return n < 0 || (size_t)n >= out_size ? -1 : 0;
}

static size_t
pfsc_reader_block_useful_size(const pfsc_reader_t *r, uint64_t index) {
  uint64_t off = index * PFS_BLOCK_SIZE;
  if(!r || off >= r->nested_size) return 0;
  uint64_t left = r->nested_size - off;
  return left < PFS_BLOCK_SIZE ? (size_t)left : (size_t)PFS_BLOCK_SIZE;
}

static int
read_mounted_block(int fd, unsigned char *out, size_t useful,
                   uint64_t offset) {
  size_t done = 0;
  while(done < useful) {
    ssize_t n = pread(fd, out + done, useful - done, (off_t)(offset + done));
    if(n < 0) {
      if(errno == EINTR) continue;
      return -1;
    }
    if(n == 0) {
      errno = EIO;
      return -1;
    }
    done += (size_t)n;
  }
  if(useful < (size_t)PFS_BLOCK_SIZE) {
    memset(out + useful, 0, (size_t)PFS_BLOCK_SIZE - useful);
  }
  return 0;
}

static void
pfsc_reader_set_cache(pfsc_reader_t *r, uint64_t index) {
  r->cached_index = index;
  r->cache_valid = 1;
}

static void
pfsc_reader_disable_mounted(pfsc_reader_t *r, const char *reason) {
  if(!r || !r->mounted_enabled) return;
  PFS_DECOMPRESS_LOG("decompress mounted fast path disabled path=%s reason=%s stale=%llu calibration_misses=%llu",
                     r->mounted_path,
                     reason ? reason : "unknown",
                     (unsigned long long)r->mounted_stale_blocks,
                     (unsigned long long)r->mounted_calibration_misses);
  if(r->mounted_fd >= 0) {
    close(r->mounted_fd);
    r->mounted_fd = -1;
  }
  r->mounted_enabled = 0;
  r->mounted_calibrated = 0;
  r->mounted_has_last = 0;
}

static int
pfsc_reader_try_open_mounted(pfsc_reader_t *r, const char *source_path) {
  char mounted_path[1024];

  if(!r || r->mounted_enabled || r->mounted_fd >= 0) return 0;
  if(mounted_nested_path(source_path, r->nested_name, mounted_path,
                         sizeof(mounted_path)) != 0) {
    PFS_DECOMPRESS_LOG("decompress mounted fast path disabled source=%s nested=%s reason=path",
                       source_path ? source_path : "",
                       r->nested_name);
    return 0;
  }
  PFS_DECOMPRESS_LOG("decompress mounted fast path disabled path=%s reason=software-windowed-unpack",
                     mounted_path);
  return 0;
}

static void
parse_inode_info(const unsigned char *data, pfs_inode_info_t *ino) {
  memset(ino, 0, sizeof(*ino));
  ino->mode = rd16(data + 0x00);
  ino->flags = rd32(data + 0x04);
  ino->size = rd64(data + 0x08);
  ino->size_comp = rd64(data + 0x10);
  ino->blocks = rd32(data + 0x60);
  ino->db0 = (int32_t)rd32(data + 0x64);
}

static void
pfsc_reader_close(pfsc_reader_t *r) {
  if(!r) return;
  if(r->mounted_path[0]) {
    PFS_DECOMPRESS_LOG("decompress mounted fast path summary path=%s enabled=%d calibrated=%d stale=%llu calibration_misses=%llu",
                       r->mounted_path,
                       r->mounted_enabled,
                       r->mounted_calibrated,
                       (unsigned long long)r->mounted_stale_blocks,
                       (unsigned long long)r->mounted_calibration_misses);
  }
  if(r->fd >= 0) close(r->fd);
  if(r->mounted_fd >= 0) close(r->mounted_fd);
  free(r->offsets);
  free(r->stored);
  free(r->block);
  free(r->mounted_block);
  free(r->last_mounted_block);
  memset(r, 0, sizeof(*r));
  r->fd = -1;
  r->mounted_fd = -1;
}

static int
pfsc_reader_load_offsets(pfsc_reader_t *r, uint64_t table_offset,
                         char *err, size_t err_size) {
  uint64_t count = r->block_count + 1;
  size_t chunk_entries = PFSC_OFFSET_READ_CHUNK_SIZE / PFSC_OFFSET_ENTRY_SIZE;
  unsigned char *chunk = NULL;
  if(count > (uint64_t)SIZE_MAX / PFSC_OFFSET_ENTRY_SIZE) {
    set_err(err, err_size, "PFSC offset table is too large");
    errno = EINVAL;
    return -1;
  }
  r->offsets = calloc((size_t)count, sizeof(*r->offsets));
  if(!r->offsets) {
    set_err(err, err_size, "out of memory");
    errno = ENOMEM;
    return -1;
  }
  if(chunk_entries == 0) chunk_entries = 1;
  chunk = malloc(chunk_entries * PFSC_OFFSET_ENTRY_SIZE);
  if(!chunk) {
    set_err(err, err_size, "out of memory");
    errno = ENOMEM;
    return -1;
  }

  for(uint64_t i = 0; i < count;) {
    size_t entries = count - i > (uint64_t)chunk_entries
        ? chunk_entries
        : (size_t)(count - i);
    size_t bytes = entries * PFSC_OFFSET_ENTRY_SIZE;
    if(read_exact_at(r->fd, chunk, bytes,
                     (off_t)(r->file_start + table_offset +
                             i * PFSC_OFFSET_ENTRY_SIZE)) != 0) {
      set_err(err, err_size, "read PFSC offsets: %s", strerror(errno));
      free(chunk);
      return -1;
    }
    for(size_t j = 0; j < entries; j++, i++) {
      r->offsets[i] = rd64(chunk + j * PFSC_OFFSET_ENTRY_SIZE);
      if(i > 0 && r->offsets[i] < r->offsets[i - 1]) {
        set_err(err, err_size, "PFSC offsets are not monotonic");
        errno = EINVAL;
        free(chunk);
        return -1;
      }
    }
  }
  free(chunk);
  return 0;
}

static void
pfsc_reader_read_outer_nested_name(int fd, pfsc_reader_t *r) {
  unsigned char inode[PFS_INODE_SIZE];
  unsigned char *dir_block = NULL;
  pfs_inode_info_t root;

  snprintf(r->nested_name, sizeof(r->nested_name), "pfs_image.dat");
  dir_block = malloc((size_t)PFS_BLOCK_SIZE);
  if(!dir_block) {
    return;
  }
  if(read_exact_at(fd, inode, sizeof(inode),
                   (off_t)(PFS_BLOCK_SIZE + 2 * PFS_INODE_SIZE)) != 0) {
    goto done;
  }
  parse_inode_info(inode, &root);
  if((root.mode & PFS_INODE_MODE_DIR) == 0 || root.db0 < 0 ||
     root.size == 0 || root.size > PFS_BLOCK_SIZE) {
    goto done;
  }
  if(read_exact_at(fd, dir_block, (size_t)root.size,
                   (off_t)((uint64_t)root.db0 * PFS_BLOCK_SIZE)) != 0) {
    goto done;
  }
  for(uint64_t off = 0; off + 16 <= root.size;) {
    uint32_t child_inode = rd32(dir_block + off + 0);
    uint32_t type = rd32(dir_block + off + 4);
    uint32_t name_len = rd32(dir_block + off + 8);
    uint32_t ent_size = rd32(dir_block + off + 12);
    if(ent_size == 0) break;
    if(ent_size < 16 || ent_size > PFS_BLOCK_SIZE ||
       name_len == 0 || name_len >= sizeof(r->nested_name) ||
       16ULL + name_len > ent_size ||
       off + ent_size > root.size) {
      break;
    }
    if(child_inode == 3 && type == PFS_DIRENT_TYPE_FILE) {
      memcpy(r->nested_name, dir_block + off + 16, name_len);
      r->nested_name[name_len] = 0;
      goto done;
    }
    off += ent_size;
  }

done:
  free(dir_block);
}

static int
pfsc_reader_open_from_outer(int fd, pfsc_reader_t *r,
                            char *err, size_t err_size) {
  unsigned char *header = NULL;
  unsigned char inode[PFS_INODE_SIZE];
  pfs_inode_info_t outer_file;
  uint64_t outer_block_size;
  uint64_t table_offset;
  uint64_t data_offset;
  uint64_t stored_size;
  int rc = -1;

  memset(r, 0, sizeof(*r));
  r->fd = -1;
  r->mounted_fd = -1;
  r->cached_index = UINT64_MAX;
  r->last_mounted_index = UINT64_MAX;
  snprintf(r->nested_name, sizeof(r->nested_name), "pfs_image.dat");

  header = malloc((size_t)PFS_BLOCK_SIZE);
  if(!header) {
    set_err(err, err_size, "out of memory");
    errno = ENOMEM;
    return -1;
  }

  if(read_exact_at(fd, header, (size_t)PFS_BLOCK_SIZE, 0) != 0) {
    set_err(err, err_size, "read outer PFS header: %s", strerror(errno));
    goto done;
  }
  if(rd64(header + 0x00) != (uint64_t)PFS_VERSION_PS5 ||
     rd64(header + 0x08) != (uint64_t)PFS_MAGIC) {
    set_err(err, err_size, "not a PS5 PFS container");
    errno = EINVAL;
    goto done;
  }
  outer_block_size = rd32(header + 0x20);
  if(outer_block_size != PFS_BLOCK_SIZE) {
    set_err(err, err_size, "unsupported outer PFS block size");
    errno = EINVAL;
    goto done;
  }

  if(read_exact_at(fd, inode, sizeof(inode),
                   (off_t)(PFS_BLOCK_SIZE + 3 * PFS_INODE_SIZE)) != 0) {
    set_err(err, err_size, "read pfs_image.dat inode: %s", strerror(errno));
    goto done;
  }
  parse_inode_info(inode, &outer_file);
  if((outer_file.flags & PFS_INODE_FLAG_COMPRESSED) == 0 ||
     outer_file.db0 < 0) {
    set_err(err, err_size, "missing compressed nested payload");
    errno = EINVAL;
    goto done;
  }
  pfsc_reader_read_outer_nested_name(fd, r);

  r->fd = fd;
  r->file_start = (uint64_t)outer_file.db0 * PFS_BLOCK_SIZE;
  r->nested_size = outer_file.size_comp;
  stored_size = outer_file.size;

  unsigned char pfsc[PFSC_HEADER_SIZE];
  if(read_exact_at(fd, pfsc, sizeof(pfsc), (off_t)r->file_start) != 0) {
    set_err(err, err_size, "read PFSC header: %s", strerror(errno));
    goto done;
  }
  if(rd32(pfsc + 0x00) != PFSC_MAGIC ||
     rd32(pfsc + 0x04) != PFSC_UNK4 ||
     rd32(pfsc + 0x08) != PFSC_UNK8) {
    set_err(err, err_size, "invalid PFSC header");
    errno = EINVAL;
    goto done;
  }

  r->block_size = rd32(pfsc + 0x0c);
  if(r->block_size != PFS_BLOCK_SIZE || rd64(pfsc + 0x10) != r->block_size) {
    set_err(err, err_size, "unsupported PFSC block size");
    errno = EINVAL;
    goto done;
  }
  table_offset = rd64(pfsc + 0x18);
  data_offset = rd64(pfsc + 0x20);
  r->logical_size = rd64(pfsc + 0x28);
  if(table_offset != PFSC_BLOCK_OFFSETS_OFFSET ||
     data_offset < PFSC_INITIAL_DATA_OFFSET ||
     r->logical_size == 0 ||
     r->logical_size % r->block_size != 0) {
    set_err(err, err_size, "unsupported PFSC layout");
    errno = EINVAL;
    goto done;
  }
  if(stored_size != 0 && data_offset > stored_size) {
    set_err(err, err_size, "truncated PFSC payload");
    errno = EINVAL;
    goto done;
  }
  r->block_count = r->logical_size / r->block_size;
  if(r->block_count == 0 || r->block_count > SIZE_MAX / PFSC_OFFSET_ENTRY_SIZE) {
    set_err(err, err_size, "PFSC image is too large");
    errno = EINVAL;
    goto done;
  }
  uint64_t table_size = (r->block_count + 1) * PFSC_OFFSET_ENTRY_SIZE;
  if(table_offset + table_size > data_offset) {
    set_err(err, err_size, "PFSC offset table overlaps data");
    errno = EINVAL;
    goto done;
  }

  if(pfsc_reader_load_offsets(r, table_offset, err, err_size) != 0) goto done;
  if(r->offsets[0] != data_offset ||
     (stored_size != 0 && r->offsets[r->block_count] > stored_size)) {
    set_err(err, err_size, "PFSC offsets exceed payload size");
    errno = EINVAL;
    goto done;
  }

  r->stored = malloc((size_t)r->block_size);
  r->block = malloc((size_t)r->block_size);
  if(!r->stored || !r->block) {
    set_err(err, err_size, "out of memory");
    errno = ENOMEM;
    goto done;
  }
  rc = 0;

done:
  free(header);
  if(rc != 0) {
    free(r->offsets);
    free(r->stored);
    free(r->block);
    free(r->mounted_block);
    free(r->last_mounted_block);
    r->offsets = NULL;
    r->stored = NULL;
    r->block = NULL;
    r->mounted_block = NULL;
    r->last_mounted_block = NULL;
    r->fd = -1;
    r->mounted_fd = -1;
  }
  return rc;
}

static int
pfsc_reader_decode_block_software(pfsc_reader_t *r, uint64_t index,
                                  unsigned char *out,
                                  char *err, size_t err_size) {
  if(index >= r->block_count) {
    set_err(err, err_size, "PFSC read outside logical image");
    errno = EINVAL;
    return -1;
  }
  uint64_t start = r->offsets[index];
  uint64_t end = r->offsets[index + 1];
  if(end < start || end - start > r->block_size) {
    set_err(err, err_size, "invalid PFSC block span");
    errno = EINVAL;
    return -1;
  }
  size_t stored_len = (size_t)(end - start);
  if(read_exact_at(r->fd, r->stored, stored_len,
                   (off_t)(r->file_start + start)) != 0) {
    set_err(err, err_size, "read PFSC block: %s", strerror(errno));
    return -1;
  }
  if(stored_len == (size_t)r->block_size) {
    memcpy(out, r->stored, stored_len);
  } else {
    size_t out_len = tinfl_decompress_mem_to_mem(
        out, (size_t)r->block_size, r->stored, stored_len,
        TINFL_FLAG_PARSE_ZLIB_HEADER);
    if(out_len != (size_t)r->block_size) {
      set_err(err, err_size, "decompress PFSC block failed");
      errno = EINVAL;
      return -1;
    }
  }
  return 0;
}

static int
pfsc_decode_block_from_memory(const pfsc_reader_t *r, uint64_t index,
                              const unsigned char *stored, size_t stored_len,
                              unsigned char *out,
                              char *err, size_t err_size) {
  if(!r || !stored || !out || index >= r->block_count) {
    set_err(err, err_size, "PFSC read outside logical image");
    errno = EINVAL;
    return -1;
  }
  if(stored_len == 0 || stored_len > (size_t)r->block_size) {
    set_err(err, err_size, "invalid PFSC block span");
    errno = EINVAL;
    return -1;
  }
  if(stored_len == (size_t)r->block_size) {
    memcpy(out, stored, stored_len);
    atomic_fetch_add(&g_job.raw_blocks, 1);
    return 1;
  }
  size_t out_len = tinfl_decompress_mem_to_mem(
      out, (size_t)r->block_size, stored, stored_len,
      TINFL_FLAG_PARSE_ZLIB_HEADER);
  if(out_len != (size_t)r->block_size) {
    set_err(err, err_size, "decompress PFSC block failed");
    errno = EINVAL;
    return -1;
  }
  atomic_fetch_add(&g_job.compressed_blocks, 1);
  return 0;
}

static int
pfsc_reader_read_mounted_checked(pfsc_reader_t *r, uint64_t index) {
  size_t useful = pfsc_reader_block_useful_size(r, index);
  if(read_mounted_block(r->mounted_fd, r->mounted_block, useful,
                        index * PFS_BLOCK_SIZE) != 0) {
    return -1;
  }
  atomic_fetch_add(&g_job.hash_checked_blocks, 1);
  return 0;
}

static int
pfsc_reader_decode_block_mounted(pfsc_reader_t *r, uint64_t index,
                                 char *err, size_t err_size) {
  if(!r->mounted_calibrated) {
    if(pfsc_reader_decode_block_software(r, index, r->block,
                                         err, err_size) != 0) {
      return -1;
    }
    atomic_fetch_add(&g_job.software_compared_blocks, 1);
    if(pfsc_reader_read_mounted_checked(r, index) != 0) {
      pfsc_reader_disable_mounted(r, "mounted read failed during calibration");
      pfsc_reader_set_cache(r, index);
      return 0;
    }
    if(memcmp(r->block, r->mounted_block, (size_t)r->block_size) == 0) {
      memcpy(r->last_mounted_block, r->mounted_block, (size_t)r->block_size);
      r->last_mounted_index = index;
      r->mounted_has_last = 1;
      r->mounted_calibrated = 1;
      atomic_fetch_add(&g_job.hash_matched_blocks, 1);
      PFS_DECOMPRESS_LOG("decompress mounted fast path calibrated path=%s block=%llu",
                         r->mounted_path, (unsigned long long)index);
    } else {
      r->mounted_calibration_misses++;
      if(r->mounted_calibration_misses == 1 ||
         (r->mounted_calibration_misses % 256ULL) == 0) {
        PFS_DECOMPRESS_LOG("decompress mounted fast path calibration miss path=%s block=%llu misses=%llu",
                           r->mounted_path,
                           (unsigned long long)index,
                           (unsigned long long)r->mounted_calibration_misses);
      }
    }
    pfsc_reader_set_cache(r, index);
    return 0;
  }

  if(pfsc_reader_read_mounted_checked(r, index) != 0) {
    pfsc_reader_disable_mounted(r, "mounted read failed");
    if(pfsc_reader_decode_block_software(r, index, r->block,
                                         err, err_size) != 0) {
      return -1;
    }
    atomic_fetch_add(&g_job.software_compared_blocks, 1);
    pfsc_reader_set_cache(r, index);
    return 0;
  }

  if(r->mounted_has_last &&
     memcmp(r->mounted_block, r->last_mounted_block,
            (size_t)r->block_size) == 0) {
    r->mounted_stale_blocks++;
    atomic_fetch_add(&g_job.hash_mismatched_blocks, 1);
    if(pfsc_reader_decode_block_software(r, index, r->block,
                                         err, err_size) != 0) {
      return -1;
    }
    atomic_fetch_add(&g_job.software_compared_blocks, 1);
    if(r->mounted_stale_blocks == 1 ||
       (r->mounted_stale_blocks % 256ULL) == 0) {
      PFS_DECOMPRESS_LOG("decompress mounted fast path stale block path=%s block=%llu stale=%llu last_valid=%llu",
                         r->mounted_path,
                         (unsigned long long)index,
                         (unsigned long long)r->mounted_stale_blocks,
                         (unsigned long long)r->last_mounted_index);
    }
  } else {
    memcpy(r->block, r->mounted_block, (size_t)r->block_size);
    memcpy(r->last_mounted_block, r->mounted_block, (size_t)r->block_size);
    r->last_mounted_index = index;
    r->mounted_has_last = 1;
    atomic_fetch_add(&g_job.hash_matched_blocks, 1);
  }
  pfsc_reader_set_cache(r, index);
  return 0;
}

static int
pfsc_reader_decode_block(pfsc_reader_t *r, uint64_t index,
                         char *err, size_t err_size) {
  if(index >= r->block_count) {
    set_err(err, err_size, "PFSC read outside logical image");
    errno = EINVAL;
    return -1;
  }
  if(r->cache_valid && r->cached_index == index) return 0;
  if(r->mounted_enabled) {
    return pfsc_reader_decode_block_mounted(r, index, err, err_size);
  }
  if(pfsc_reader_decode_block_software(r, index, r->block,
                                       err, err_size) != 0) {
    return -1;
  }
  pfsc_reader_set_cache(r, index);
  return 0;
}

static int
pfsc_reader_decode_block_direct(pfsc_reader_t *r, uint64_t index,
                                unsigned char *out,
                                char *err, size_t err_size) {
  if(index >= r->block_count) {
    set_err(err, err_size, "PFSC read outside logical image");
    errno = EINVAL;
    return -1;
  }

  if(r->mounted_enabled) {
    if(!r->mounted_calibrated) {
      if(pfsc_reader_decode_block_software(r, index, out,
                                           err, err_size) != 0) {
        return -1;
      }
      atomic_fetch_add(&g_job.software_compared_blocks, 1);
      if(pfsc_reader_read_mounted_checked(r, index) != 0) {
        pfsc_reader_disable_mounted(r,
                                    "mounted read failed during calibration");
        return 0;
      }
      if(memcmp(out, r->mounted_block, (size_t)r->block_size) == 0) {
        memcpy(r->last_mounted_block, r->mounted_block,
               (size_t)r->block_size);
        r->last_mounted_index = index;
        r->mounted_has_last = 1;
        r->mounted_calibrated = 1;
        atomic_fetch_add(&g_job.hash_matched_blocks, 1);
        PFS_DECOMPRESS_LOG("decompress mounted fast path calibrated path=%s block=%llu",
                           r->mounted_path, (unsigned long long)index);
      } else {
        r->mounted_calibration_misses++;
        if(r->mounted_calibration_misses == 1 ||
           (r->mounted_calibration_misses % 256ULL) == 0) {
          PFS_DECOMPRESS_LOG("decompress mounted fast path calibration miss path=%s block=%llu misses=%llu",
                             r->mounted_path,
                             (unsigned long long)index,
                             (unsigned long long)r->mounted_calibration_misses);
        }
      }
      return 0;
    }

    if(pfsc_reader_read_mounted_checked(r, index) != 0) {
      pfsc_reader_disable_mounted(r, "mounted read failed");
      if(pfsc_reader_decode_block_software(r, index, out,
                                           err, err_size) != 0) {
        return -1;
      }
      atomic_fetch_add(&g_job.software_compared_blocks, 1);
      return 0;
    }

    if(r->mounted_has_last &&
       memcmp(r->mounted_block, r->last_mounted_block,
              (size_t)r->block_size) == 0) {
      r->mounted_stale_blocks++;
      atomic_fetch_add(&g_job.hash_mismatched_blocks, 1);
      if(pfsc_reader_decode_block_software(r, index, out,
                                           err, err_size) != 0) {
        return -1;
      }
      atomic_fetch_add(&g_job.software_compared_blocks, 1);
      if(r->mounted_stale_blocks == 1 ||
         (r->mounted_stale_blocks % 256ULL) == 0) {
        PFS_DECOMPRESS_LOG("decompress mounted fast path stale block path=%s block=%llu stale=%llu last_valid=%llu",
                           r->mounted_path,
                           (unsigned long long)index,
                           (unsigned long long)r->mounted_stale_blocks,
                           (unsigned long long)r->last_mounted_index);
      }
    } else {
      memcpy(out, r->mounted_block, (size_t)r->block_size);
      memcpy(r->last_mounted_block, r->mounted_block,
             (size_t)r->block_size);
      r->last_mounted_index = index;
      r->mounted_has_last = 1;
      atomic_fetch_add(&g_job.hash_matched_blocks, 1);
    }
    return 0;
  }

  return pfsc_reader_decode_block_software(r, index, out, err, err_size);
}

static int
pfsc_reader_read(pfsc_reader_t *r, uint64_t offset, void *out, size_t size,
                 char *err, size_t err_size) {
  unsigned char *dst = out;
  if(size == 0) return 0;
  if(offset >= r->logical_size || size > r->logical_size - offset) {
    set_err(err, err_size, "PFSC read outside logical image");
    errno = EINVAL;
    return -1;
  }
  while(size > 0) {
    uint64_t index = offset / r->block_size;
    size_t block_off = (size_t)(offset % r->block_size);
    size_t avail = (size_t)r->block_size - block_off;
    size_t n = avail < size ? avail : size;
    if(pfsc_reader_decode_block(r, index, err, err_size) != 0) return -1;
    memcpy(dst, r->block + block_off, n);
    dst += n;
    offset += n;
    size -= n;
  }
  return 0;
}

static int
pfsc_reader_write_buffered_range(pfsc_reader_t *r, uint64_t offset,
                                 size_t size, pfs_stream_buffer_t *outbuf,
                                 int out, unsigned char *fallback,
                                 char *err, size_t err_size) {
  if(size == 0) return 0;
  if(size <= (size_t)r->block_size &&
     (offset % r->block_size) == 0) {
    unsigned char *dst = pfs_stream_buffer_reserve(
        outbuf, out, write_all_fd_cancelable, (size_t)r->block_size);
    if(dst) {
      if(pfsc_reader_decode_block_direct(r, offset / r->block_size,
                                         dst, err, err_size) != 0) {
        return -1;
      }
      pfs_stream_buffer_commit(outbuf, size);
      return 0;
    }
  }
  if(pfsc_reader_read(r, offset, fallback, size, err, err_size) != 0 ||
     pfs_stream_buffer_write(outbuf, out, write_all_fd_cancelable,
                             fallback, size) != 0) {
    if(!err[0]) set_err(err, err_size, "write output file: %s", strerror(errno));
    return -1;
  }
  return 0;
}

static int
pfsc_reader_detect_nested_type(pfsc_reader_t *r,
                               char *err, size_t err_size) {
  unsigned char *header = malloc((size_t)PFS_BLOCK_SIZE);
  int nested_type = PFS_NESTED_UNKNOWN;
  if(!header) {
    set_err(err, err_size, "out of memory");
    errno = ENOMEM;
    return PFS_NESTED_UNKNOWN;
  }
  if(pfsc_reader_read(r, 0, header, (size_t)PFS_BLOCK_SIZE,
                      err, err_size) != 0) {
    goto done;
  }
  if(rd64(header + 0x00) == (uint64_t)PFS_VERSION_PS5 &&
     rd64(header + 0x08) == (uint64_t)PFS_MAGIC) {
    nested_type = PFS_NESTED_PFS;
    goto done;
  }
  if(memcmp(header + 3, "EXFAT   ", 8) == 0) {
    nested_type = PFS_NESTED_EXFAT;
    goto done;
  }
  set_err(err, err_size, "unsupported nested image type");
  errno = EINVAL;
done:
  free(header);
  return nested_type;
}

static void
pfs_nested_close(pfs_nested_reader_t *nr) {
  if(!nr) return;
  pfsc_reader_close(&nr->pfsc);
  memset(nr, 0, sizeof(*nr));
}

static int
pfs_nested_open(const char *path, pfs_nested_reader_t *nr,
                int writable, char *err, size_t err_size) {
  int fd = -1;
  unsigned char *header = NULL;
  int rc = -1;
  memset(nr, 0, sizeof(*nr));
  nr->pfsc.fd = -1;

  header = malloc((size_t)PFS_BLOCK_SIZE);
  if(!header) {
    set_err(err, err_size, "out of memory");
    errno = ENOMEM;
    return -1;
  }

  fd = open(path, writable ? O_RDWR : O_RDONLY);
  if(fd < 0) {
    set_err(err, err_size, "open input: %s", strerror(errno));
    goto done;
  }
  if(pfsc_reader_open_from_outer(fd, &nr->pfsc, err, err_size) != 0) {
    if(nr->pfsc.fd >= 0) pfsc_reader_close(&nr->pfsc);
    else close(fd);
    fd = -1;
    goto done;
  }
  fd = -1;
  if(!writable) pfsc_reader_try_open_mounted(&nr->pfsc, path);

  if(pfsc_reader_read(&nr->pfsc, 0, header, (size_t)PFS_BLOCK_SIZE,
                      err, err_size) != 0) {
    pfs_nested_close(nr);
    goto done;
  }
  if(rd64(header + 0x00) != (uint64_t)PFS_VERSION_PS5 ||
     rd64(header + 0x08) != (uint64_t)PFS_MAGIC) {
    set_err(err, err_size, "nested image is not PS5 PFS");
    errno = EINVAL;
    pfs_nested_close(nr);
    goto done;
  }
  nr->block_size = rd32(header + 0x20);
  nr->inode_count = rd64(header + 0x30);
  nr->inode_block_count = rd64(header + 0x40);
  uint64_t inodes_per_block = nr->block_size / PFS_INODE_SIZE;
  if(nr->block_size != PFS_BLOCK_SIZE ||
     nr->inode_count < 3 ||
     nr->inode_block_count == 0 ||
     inodes_per_block == 0 ||
     nr->inode_count > nr->inode_block_count * inodes_per_block) {
    set_err(err, err_size, "unsupported nested PFS layout");
    errno = EINVAL;
    pfs_nested_close(nr);
    goto done;
  }
  rc = 0;

done:
  free(header);
  if(fd >= 0) close(fd);
  return rc;
}

static int
pfs_nested_read_inode(pfs_nested_reader_t *nr, uint32_t inode_num,
                      pfs_inode_info_t *ino, char *err, size_t err_size) {
  unsigned char raw[PFS_INODE_SIZE];
  if((uint64_t)inode_num >= nr->inode_count) {
    set_err(err, err_size, "inode outside table");
    errno = EINVAL;
    return -1;
  }
  uint64_t inodes_per_block = nr->block_size / PFS_INODE_SIZE;
  uint64_t inode_block = (uint64_t)inode_num / inodes_per_block;
  uint64_t inode_index = (uint64_t)inode_num % inodes_per_block;
  if(inode_block >= nr->inode_block_count) {
    set_err(err, err_size, "inode outside table");
    errno = EINVAL;
    return -1;
  }
  uint64_t off = PFS_BLOCK_SIZE + inode_block * nr->block_size +
                 inode_index * PFS_INODE_SIZE;
  if(pfsc_reader_read(&nr->pfsc, off, raw, sizeof(raw), err, err_size) != 0) {
    return -1;
  }
  parse_inode_info(raw, ino);
  return 0;
}

static int
pfs_nested_read_inode_data(pfs_nested_reader_t *nr, const pfs_inode_info_t *ino,
                           uint64_t file_off, void *out, size_t size,
                           char *err, size_t err_size) {
  unsigned char *dst = out;
  if(size == 0) return 0;
  if(ino->db0 < 0 || file_off > ino->size || size > ino->size - file_off) {
    set_err(err, err_size, "inode read outside file");
    errno = EINVAL;
    return -1;
  }
  while(size > 0) {
    uint64_t block_index = file_off / nr->block_size;
    uint64_t block_off = file_off % nr->block_size;
    size_t avail = (size_t)(nr->block_size - block_off);
    size_t n = avail < size ? avail : size;
    uint64_t image_off = ((uint64_t)ino->db0 + block_index) * nr->block_size +
                         block_off;
    if(pfsc_reader_read(&nr->pfsc, image_off, dst, n, err, err_size) != 0) {
      return -1;
    }
    dst += n;
    file_off += n;
    size -= n;
  }
  return 0;
}

typedef struct pfs_dirent_info {
  uint32_t child_inode;
  uint32_t type;
  uint32_t name_len;
  uint32_t ent_size;
  char name[256];
} pfs_dirent_info_t;

static int
pfs_nested_read_dirent(pfs_nested_reader_t *nr, const pfs_inode_info_t *dir_ino,
                       uint64_t off, pfs_dirent_info_t *ent, int *end,
                       char *err, size_t err_size) {
  unsigned char hdr[16];
  if(end) *end = 0;
  if(pfs_nested_read_inode_data(nr, dir_ino, off, hdr, sizeof(hdr),
                                err, err_size) != 0) {
    return -1;
  }
  ent->child_inode = rd32(hdr + 0);
  ent->type = rd32(hdr + 4);
  ent->name_len = rd32(hdr + 8);
  ent->ent_size = rd32(hdr + 12);
  if(ent->ent_size == 0) {
    if(end) *end = 1;
    return 0;
  }
  if(ent->ent_size < 16 || ent->ent_size > PFS_BLOCK_SIZE ||
     ent->name_len == 0 || ent->name_len >= sizeof(ent->name) ||
     16ULL + ent->name_len > ent->ent_size ||
     off + ent->ent_size > dir_ino->size) {
    set_err(err, err_size, "invalid PFS dirent");
    errno = EINVAL;
    return -1;
  }

  if(pfs_nested_read_inode_data(nr, dir_ino, off + 16, ent->name,
                                ent->name_len, err, err_size) != 0) {
    return -1;
  }
  ent->name[ent->name_len] = 0;
  return 0;
}

static int
pfs_find_child_inode(pfs_nested_reader_t *nr, uint32_t dir_inode_num,
                     const char *wanted, uint32_t wanted_type,
                     uint32_t *inode_out, char *err, size_t err_size) {
  pfs_inode_info_t dir_ino;
  if(pfs_nested_read_inode(nr, dir_inode_num, &dir_ino, err, err_size) != 0) {
    return -1;
  }
  if((dir_ino.mode & PFS_INODE_MODE_DIR) == 0) {
    set_err(err, err_size, "expected directory inode");
    errno = EINVAL;
    return -1;
  }

  for(uint64_t off = 0; off + 16 <= dir_ino.size;) {
    pfs_dirent_info_t ent;
    int end = 0;
    if(pfs_nested_read_dirent(nr, &dir_ino, off, &ent, &end,
                              err, err_size) != 0) {
      return -1;
    }
    if(end) break;
    if(ent.type == wanted_type && strcmp(ent.name, wanted) == 0) {
      *inode_out = ent.child_inode;
      return 0;
    }
    off += ent.ent_size;
  }

  set_err(err, err_size, "PFS superroot missing uroot");
  errno = EINVAL;
  return -1;
}

static int pfs_extract_file(pfs_nested_reader_t *nr, uint32_t inode_num,
                            const char *out_path, const char *rel,
                            int workers, int sync_file,
                            char *err, size_t err_size);
static int pfs_extract_dir(pfs_nested_reader_t *nr, uint32_t inode_num,
                           const char *out_dir, const char *rel,
                           int workers, int sync_files,
                           char *err, size_t err_size);

static void
pfs_extract_plan_free(pfs_extract_plan_t *plan) {
  if(!plan) return;
  for(size_t i = 0; i < plan->count; i++) {
    if(plan->items[i].fd >= 0) {
      close(plan->items[i].fd);
      plan->items[i].fd = -1;
    }
  }
  free(plan->items);
  memset(plan, 0, sizeof(*plan));
}

static int
pfs_extract_plan_push(pfs_extract_plan_t *plan, uint32_t inode_num,
                      const pfs_inode_info_t *ino, const char *out_path,
                      const char *rel, char *err, size_t err_size) {
  if(!plan || !ino || !out_path || !rel) {
    errno = EINVAL;
    return -1;
  }
  uint64_t physical_blocks = ino->blocks ? ino->blocks :
    ceil_div_u64(ino->size, PFS_BLOCK_SIZE);
  if((ino->mode & PFS_INODE_MODE_FILE) == 0 ||
     (physical_blocks > 0 && ino->db0 < 0)) {
    set_err(err, err_size, "invalid file inode");
    errno = EINVAL;
    return -1;
  }
  if(plan->count == plan->cap) {
    size_t next = plan->cap ? plan->cap * 2 : 128;
    pfs_extract_entry_t *p = realloc(plan->items, next * sizeof(*p));
    if(!p) {
      set_err(err, err_size, "out of memory");
      errno = ENOMEM;
      return -1;
    }
    plan->items = p;
    plan->cap = next;
  }
  pfs_extract_entry_t *e = &plan->items[plan->count++];
  memset(e, 0, sizeof(*e));
  e->fd = -1;
  snprintf(e->rel, sizeof(e->rel), "%s", rel);
  snprintf(e->out_path, sizeof(e->out_path), "%s", out_path);
  e->inode_num = inode_num;
  e->first_block = ino->db0 < 0 ? 0 : (uint64_t)ino->db0;
  e->image_offset = e->first_block * PFS_BLOCK_SIZE;
  e->block_count = physical_blocks;
  e->size = ino->size;
  plan->total_bytes += ino->size;
  plan->total_blocks += ceil_div_u64(ino->size, PFS_BLOCK_SIZE);
  return 0;
}

static int
pfs_extract_entry_cmp_asc(const void *a, const void *b) {
  const pfs_extract_entry_t *ea = a;
  const pfs_extract_entry_t *eb = b;
  if(ea->image_offset < eb->image_offset) return -1;
  if(ea->image_offset > eb->image_offset) return 1;
  if(ea->size < eb->size) return -1;
  if(ea->size > eb->size) return 1;
  return strcmp(ea->rel, eb->rel);
}

static int
pfs_collect_extract_plan(pfs_nested_reader_t *nr, uint32_t inode_num,
                         const char *out_dir, const char *rel,
                         pfs_extract_plan_t *plan,
                         char *err, size_t err_size) {
  pfs_inode_info_t dir_ino;
  if(pfs_nested_read_inode(nr, inode_num, &dir_ino, err, err_size) != 0) {
    return -1;
  }
  if((dir_ino.mode & PFS_INODE_MODE_DIR) == 0) {
    set_err(err, err_size, "expected directory inode");
    errno = EINVAL;
    return -1;
  }
  if(mkdir_one_local(out_dir) != 0) {
    set_err(err, err_size, "create output folder: %s", strerror(errno));
    return -1;
  }

  for(uint64_t off = 0; off + 16 <= dir_ino.size;) {
    if(job_cancelled()) {
      set_err(err, err_size, "cancelled");
      errno = EINTR;
      return -1;
    }
    pfs_dirent_info_t ent;
    int end = 0;
    if(pfs_nested_read_dirent(nr, &dir_ino, off, &ent, &end,
                              err, err_size) != 0) {
      return -1;
    }
    if(end) break;
    if(strcmp(ent.name, ".") != 0 && strcmp(ent.name, "..") != 0) {
      if(!path_segment_supported(ent.name)) {
        set_err(err, err_size, "unsafe PFS path segment");
        errno = EINVAL;
        return -1;
      }
      char child_out[1024];
      char child_rel[1024];
      if(join_abs(child_out, sizeof(child_out), out_dir, ent.name) != 0 ||
         join_rel(child_rel, sizeof(child_rel), rel ? rel : "", ent.name) != 0) {
        set_err(err, err_size, "output path too long");
        return -1;
      }
      if(ent.type == PFS_DIRENT_TYPE_DIRECTORY) {
        if(pfs_collect_extract_plan(nr, ent.child_inode, child_out, child_rel,
                                    plan, err, err_size) != 0) {
          return -1;
        }
      } else if(ent.type == PFS_DIRENT_TYPE_FILE) {
        pfs_inode_info_t file_ino;
        if(pfs_nested_read_inode(nr, ent.child_inode, &file_ino,
                                 err, err_size) != 0 ||
           pfs_extract_plan_push(plan, ent.child_inode, &file_ino, child_out,
                                 child_rel, err, err_size) != 0) {
          return -1;
        }
      } else {
        set_err(err, err_size, "unsupported PFS dirent type");
        errno = EINVAL;
        return -1;
      }
    }
    off += ent.ent_size;
  }
  chmod(out_dir, 0777);
  return 0;
}

static uint64_t
pfs_extract_entry_end(const pfs_extract_entry_t *e) {
  if(!e) return 0;
  return UINT64_MAX - e->image_offset < e->size
      ? UINT64_MAX
      : e->image_offset + e->size;
}

static int
pfsc_decomp_worker_count(int requested_workers) {
  int workers = requested_workers;
  if(workers <= 0) workers = PFS_DECOMPRESS_DEFAULT_WORKERS;
  if(workers > PFS_COMPRESS_MAX_WORKERS) workers = PFS_COMPRESS_MAX_WORKERS;
  if(workers < 1) workers = 1;
  return workers;
}

static void
pfsc_decomp_push_task(pfsc_decomp_task_t **head, pfsc_decomp_task_t **tail,
                      pfsc_decomp_task_t *task) {
  task->next = NULL;
  if(*tail) (*tail)->next = task;
  else *head = task;
  *tail = task;
}

static pfsc_decomp_task_t *
pfsc_decomp_pop_task(pfsc_decomp_task_t **head, pfsc_decomp_task_t **tail) {
  pfsc_decomp_task_t *task = *head;
  if(!task) return NULL;
  *head = task->next;
  if(!*head) *tail = NULL;
  task->next = NULL;
  return task;
}

static void
pfsc_decomp_pool_set_error(pfsc_decomp_pool_t *pool, int err,
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
pfsc_decomp_pool_check_error_locked(pfsc_decomp_pool_t *pool,
                                    char *err, size_t err_size) {
  if(!pool->error) return 0;
  set_err(err, err_size, "%s",
          pool->error_msg[0] ? pool->error_msg : strerror(pool->error));
  errno = pool->error;
  return -1;
}

static int
pfsc_decomp_pool_cancel_if_requested_locked(pfsc_decomp_pool_t *pool,
                                            char *err, size_t err_size) {
  if(!job_cancelled()) return 0;
  if(!pool->error) {
    pool->error = EINTR;
    snprintf(pool->error_msg, sizeof(pool->error_msg), "%s", "cancelled");
  }
  pool->stop = 1;
  pthread_cond_broadcast(&pool->cond);
  set_err(err, err_size, "%s", "cancelled");
  errno = EINTR;
  return -1;
}

static void
pfsc_decomp_pool_set_phase(pfsc_decomp_pool_t *pool, int phase,
                           int read_permits, int decode_permits) {
  pthread_mutex_lock(&pool->lock);
  pool->phase = phase;
  pool->read_permits = read_permits;
  pool->decode_permits = decode_permits;
  pthread_cond_broadcast(&pool->cond);
  pthread_mutex_unlock(&pool->lock);
}

static int
pfsc_decomp_pool_enqueue_locked(pfsc_decomp_pool_t *pool,
                                pfsc_decomp_task_type_t type,
                                pfsc_decomp_window_t *window,
                                int lane_index,
                                uint32_t first_block,
                                uint32_t block_count) {
  pfsc_decomp_task_t *task = malloc(sizeof(*task));
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
  if(type == PFSC_DECOMP_TASK_READ) {
    window->read_tasks_remaining++;
    pfsc_decomp_push_task(&pool->read_head, &pool->read_tail, task);
  } else {
    window->decode_tasks_remaining++;
    pfsc_decomp_push_task(&pool->decode_head, &pool->decode_tail, task);
  }
  pthread_cond_broadcast(&pool->cond);
  return 0;
}

static pfsc_decomp_task_t *
pfsc_decomp_pool_take_task_locked(pfsc_decomp_pool_t *pool) {
  if(pool->phase == PFSC_DECOMP_PHASE_READ) {
    if(pool->read_head && pool->running_read < pool->read_permits) {
      pool->running_read++;
      return pfsc_decomp_pop_task(&pool->read_head, &pool->read_tail);
    }
    if(pool->decode_head && pool->running_decode < pool->decode_permits) {
      pool->running_decode++;
      return pfsc_decomp_pop_task(&pool->decode_head, &pool->decode_tail);
    }
  } else {
    if(pool->decode_head && pool->running_decode < pool->decode_permits) {
      pool->running_decode++;
      return pfsc_decomp_pop_task(&pool->decode_head, &pool->decode_tail);
    }
  }
  return NULL;
}

static void
pfsc_decomp_pool_finish_task_locked(pfsc_decomp_pool_t *pool,
                                    const pfsc_decomp_task_t *task) {
  if(task->type == PFSC_DECOMP_TASK_READ) {
    if(pool->running_read > 0) pool->running_read--;
    if(task->window->read_tasks_remaining > 0) {
      task->window->read_tasks_remaining--;
    }
  } else {
    if(pool->running_decode > 0) pool->running_decode--;
    if(task->window->decode_tasks_remaining > 0) {
      task->window->decode_tasks_remaining--;
    }
  }
  pthread_cond_broadcast(&pool->cond);
}

static void
pfsc_decomp_read_task(pfsc_decomp_pool_t *pool, pfsc_decomp_task_t *task) {
  pfsc_decomp_lane_t *lane = &task->window->lanes[task->lane_index];
  char local_err[256] = {0};
  int saved_errno = 0;
  if(job_cancelled()) {
    saved_errno = EINTR;
    snprintf(local_err, sizeof(local_err), "%s", "cancelled");
  } else if(lane->block_count == 0 || lane->stored_size == 0 ||
            read_exact_at(pool->reader->fd, lane->stored,
                          (size_t)lane->stored_size,
                          (off_t)(pool->reader->file_start +
                                  lane->stored_offset)) != 0) {
    saved_errno = errno ? errno : EIO;
    snprintf(local_err, sizeof(local_err), "read PFSC window: %s",
             strerror(saved_errno));
  }
  if(saved_errno) {
    pfsc_decomp_pool_set_error(pool, saved_errno, local_err);
    return;
  }
  pthread_mutex_lock(&pool->lock);
  lane->read_done = 1;
  pthread_cond_broadcast(&pool->cond);
  pthread_mutex_unlock(&pool->lock);
}

static void
pfsc_decomp_decode_task(pfsc_decomp_pool_t *pool, pfsc_decomp_task_t *task) {
  pfsc_decomp_lane_t *lane = &task->window->lanes[task->lane_index];
  char local_err[256] = {0};
  int saved_errno = 0;

  if(!lane->read_done) {
    pfsc_decomp_pool_set_error(pool, EIO, "decode before read");
    return;
  }
  for(uint32_t i = 0; i < task->block_count; i++) {
    if(job_cancelled()) {
      saved_errno = EINTR;
      snprintf(local_err, sizeof(local_err), "%s", "cancelled");
      break;
    }
    uint64_t local_block = (uint64_t)task->first_block + i;
    uint64_t global_block = lane->start_block + local_block;
    if(local_block >= lane->block_count ||
       global_block >= pool->reader->block_count) {
      saved_errno = EINVAL;
      snprintf(local_err, sizeof(local_err), "%s",
               "PFSC decode outside window");
      break;
    }
    uint64_t start = pool->reader->offsets[global_block];
    uint64_t end = pool->reader->offsets[global_block + 1];
    if(end < start || start < lane->stored_offset ||
       end - lane->stored_offset > lane->stored_size ||
       end - start > pool->reader->block_size) {
      saved_errno = EINVAL;
      snprintf(local_err, sizeof(local_err), "%s",
               "invalid PFSC block span");
      break;
    }
    size_t stored_len = (size_t)(end - start);
    const unsigned char *stored =
        lane->stored + (size_t)(start - lane->stored_offset);
    unsigned char *decoded =
        lane->decoded + (size_t)local_block * (size_t)pool->reader->block_size;
    int decode_rc = pfsc_decode_block_from_memory(
        pool->reader, global_block, stored, stored_len, decoded,
        local_err, sizeof(local_err));
    if(decode_rc < 0) {
      saved_errno = errno ? errno : EIO;
      break;
    }
    pthread_mutex_lock(&pool->lock);
    pfsc_decomp_block_t *block = &lane->blocks[local_block];
    block->stored_len = (uint32_t)stored_len;
    block->raw = decode_rc > 0 ? 1 : 0;
    block->ready = 1;
    pthread_cond_broadcast(&pool->cond);
    pthread_mutex_unlock(&pool->lock);
  }

  if(saved_errno) {
    pfsc_decomp_pool_set_error(pool, saved_errno, local_err);
  }
}

static void *
pfsc_decomp_worker_main(void *arg) {
  pfsc_decomp_pool_t *pool = arg;
  for(;;) {
    pthread_mutex_lock(&pool->lock);
	    pfsc_decomp_task_t *task = NULL;
	    while(!pool->stop && !(task = pfsc_decomp_pool_take_task_locked(pool))) {
	      uint64_t wait_started = monotonic_us();
	      cancel_poll_cond_wait(&pool->cond, &pool->lock);
	      job_add_wait_us(&g_job.worker_wait_us, wait_started);
	    }
    if(pool->stop && !task) {
      pthread_mutex_unlock(&pool->lock);
      break;
    }
    pthread_mutex_unlock(&pool->lock);

    if(task->type == PFSC_DECOMP_TASK_READ) {
      pfsc_decomp_read_task(pool, task);
    } else {
      pfsc_decomp_decode_task(pool, task);
    }

    pthread_mutex_lock(&pool->lock);
    pfsc_decomp_pool_finish_task_locked(pool, task);
    pthread_mutex_unlock(&pool->lock);
    free(task);
  }
  return NULL;
}

static void
pfsc_decomp_free_task_list(pfsc_decomp_task_t *task) {
  while(task) {
    pfsc_decomp_task_t *next = task->next;
    free(task);
    task = next;
  }
}

static void
pfsc_decomp_pool_stop(pfsc_decomp_pool_t *pool) {
  pthread_mutex_lock(&pool->lock);
  pool->stop = 1;
  pthread_cond_broadcast(&pool->cond);
  pthread_mutex_unlock(&pool->lock);
}

static int
pfsc_decomp_pool_start(pfsc_decomp_pool_t *pool,
                       const pfsc_reader_t *reader,
                       int decode_workers) {
  memset(pool, 0, sizeof(*pool));
  pool->reader = reader;
  pool->decode_permits = decode_workers;
  pool->read_permits = 1;
  pool->thread_count = decode_workers + 1;
  pool->threads = calloc((size_t)pool->thread_count, sizeof(*pool->threads));
  if(!pool->threads) return -1;
  if(pthread_mutex_init(&pool->lock, NULL) != 0 ||
     pthread_cond_init(&pool->cond, NULL) != 0) {
    free(pool->threads);
    pool->threads = NULL;
    return -1;
  }
  for(int i = 0; i < pool->thread_count; i++) {
    int trc = pthread_create(&pool->threads[i], NULL,
                             pfsc_decomp_worker_main, pool);
    if(trc != 0) {
      pool->error = trc;
      snprintf(pool->error_msg, sizeof(pool->error_msg),
               "start decompression window worker: %s", strerror(trc));
      pfsc_decomp_pool_stop(pool);
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
pfsc_decomp_pool_destroy(pfsc_decomp_pool_t *pool) {
  if(!pool) return;
  if(pool->threads) {
    pfsc_decomp_pool_stop(pool);
    for(int i = 0; i < pool->workers_started; i++) {
      pthread_join(pool->threads[i], NULL);
    }
    free(pool->threads);
  }
  pfsc_decomp_free_task_list(pool->read_head);
  pfsc_decomp_free_task_list(pool->decode_head);
  pthread_cond_destroy(&pool->cond);
  pthread_mutex_destroy(&pool->lock);
  memset(pool, 0, sizeof(*pool));
}

static void
pfsc_decomp_free_windows(pfsc_decomp_window_t *windows, int worker_count) {
  if(!windows) return;
  for(int w = 0; w < PFSC_DECOMPRESS_WINDOW_SLOTS; w++) {
    if(!windows[w].lanes) continue;
    for(int i = 0; i < worker_count; i++) {
      free(windows[w].lanes[i].stored);
      free(windows[w].lanes[i].decoded);
      free(windows[w].lanes[i].blocks);
    }
    free(windows[w].lanes);
  }
  free(windows);
}

static int
pfsc_decomp_alloc_windows(pfsc_decomp_window_t **windows_out,
                          int worker_count, size_t lane_size,
                          uint64_t lane_blocks,
                          char *err, size_t err_size) {
  pfsc_decomp_window_t *windows =
      calloc(PFSC_DECOMPRESS_WINDOW_SLOTS, sizeof(*windows));
  if(!windows) {
    set_err(err, err_size, "window buffers unavailable");
    return -1;
  }
  for(int w = 0; w < PFSC_DECOMPRESS_WINDOW_SLOTS; w++) {
    windows[w].lanes = calloc((size_t)worker_count, sizeof(*windows[w].lanes));
    if(!windows[w].lanes) {
      set_err(err, err_size, "window buffers unavailable");
      pfsc_decomp_free_windows(windows, worker_count);
      return -1;
    }
    for(int i = 0; i < worker_count; i++) {
      windows[w].lanes[i].stored = malloc(lane_size);
      windows[w].lanes[i].decoded = malloc(lane_size);
      windows[w].lanes[i].blocks =
          calloc((size_t)lane_blocks, sizeof(*windows[w].lanes[i].blocks));
      if(!windows[w].lanes[i].stored ||
         !windows[w].lanes[i].decoded ||
         !windows[w].lanes[i].blocks) {
        set_err(err, err_size, "window buffers unavailable");
        pfsc_decomp_free_windows(windows, worker_count);
        return -1;
      }
    }
  }
  *windows_out = windows;
  return 0;
}

static int
pfsc_decomp_window_prepare(pfsc_decomp_window_t *window,
                           const pfsc_reader_t *reader,
                           uint64_t window_index,
                           uint64_t start_block,
                           uint64_t requested_blocks,
                           uint64_t lane_blocks,
                           int worker_count,
                           char *err, size_t err_size) {
  if(!window || !reader || start_block >= reader->block_count ||
     requested_blocks == 0) {
    set_err(err, err_size, "bad decompression window");
    errno = EINVAL;
    return -1;
  }
  pfsc_decomp_lane_t *lanes = window->lanes;
  memset(window, 0, sizeof(*window));
  window->lanes = lanes;
  window->index = window_index;
  window->start_block = start_block;

  uint64_t remaining = requested_blocks;
  if(remaining > reader->block_count - start_block) {
    remaining = reader->block_count - start_block;
  }
  for(int i = 0; i < worker_count && remaining > 0; i++) {
    uint64_t count = remaining > lane_blocks ? lane_blocks : remaining;
    pfsc_decomp_lane_t *lane = &window->lanes[i];
    unsigned char *stored = lane->stored;
    unsigned char *decoded = lane->decoded;
    pfsc_decomp_block_t *blocks = lane->blocks;
    memset(lane, 0, sizeof(*lane));
    lane->stored = stored;
    lane->decoded = decoded;
    lane->blocks = blocks;
    lane->start_block = start_block + window->block_count;
    lane->block_count = count;
    lane->stored_offset = reader->offsets[lane->start_block];
    uint64_t stored_end = reader->offsets[lane->start_block + count];
    if(stored_end < lane->stored_offset ||
       stored_end - lane->stored_offset > count * reader->block_size) {
      set_err(err, err_size, "invalid PFSC window span");
      errno = EINVAL;
      return -1;
    }
    lane->stored_size = stored_end - lane->stored_offset;
    lane->decoded_size = count * reader->block_size;
    memset(lane->blocks, 0, (size_t)lane_blocks * sizeof(*lane->blocks));
    window->block_count += count;
    window->lane_count++;
    remaining -= count;
  }
  return window->lane_count > 0 ? 0 : -1;
}

static int
pfsc_decomp_queue_read_window(pfsc_decomp_pool_t *pool,
                              pfsc_decomp_window_t *window,
                              char *err, size_t err_size) {
  pthread_mutex_lock(&pool->lock);
  for(int i = 0; i < window->lane_count; i++) {
    if(pfsc_decomp_pool_enqueue_locked(pool, PFSC_DECOMP_TASK_READ,
                                       window, i, 0, 0) != 0) {
      pthread_mutex_unlock(&pool->lock);
      set_err(err, err_size, "out of memory");
      return -1;
    }
  }
  pthread_mutex_unlock(&pool->lock);
  return 0;
}

static int
pfsc_decomp_wait_read_window(pfsc_decomp_pool_t *pool,
                             pfsc_decomp_window_t *window,
                             char *err, size_t err_size) {
  pthread_mutex_lock(&pool->lock);
  while(window->read_tasks_remaining > 0 || pool->running_read > 0) {
    if(pfsc_decomp_pool_check_error_locked(pool, err, err_size) != 0) {
      pthread_mutex_unlock(&pool->lock);
      return -1;
    }
    if(pfsc_decomp_pool_cancel_if_requested_locked(pool, err,
                                                   err_size) != 0) {
      pthread_mutex_unlock(&pool->lock);
      return -1;
    }
    cancel_poll_cond_wait(&pool->cond, &pool->lock);
  }
  if(pfsc_decomp_pool_check_error_locked(pool, err, err_size) != 0) {
    pthread_mutex_unlock(&pool->lock);
    return -1;
  }
  pthread_mutex_unlock(&pool->lock);
  return 0;
}

static int
pfsc_decomp_read_window(pfsc_decomp_pool_t *pool,
                        pfsc_decomp_window_t *window,
                        int decode_workers,
                        char *err, size_t err_size) {
  pfsc_decomp_pool_set_phase(pool, PFSC_DECOMP_PHASE_READ, 1,
                             decode_workers);
  if(pfsc_decomp_queue_read_window(pool, window, err, err_size) != 0) {
    return -1;
  }
  return pfsc_decomp_wait_read_window(pool, window, err, err_size);
}

static int
pfsc_decomp_start_decode(pfsc_decomp_pool_t *pool,
                         pfsc_decomp_window_t *window,
                         char *err, size_t err_size) {
  if(window->decode_started) return 0;
  pthread_mutex_lock(&pool->lock);
  window->decode_started = 1;
  for(int i = 0; i < window->lane_count; i++) {
    pfsc_decomp_lane_t *lane = &window->lanes[i];
    lane->decode_started = 1;
    for(uint32_t first = 0; first < lane->block_count;
        first += PFSC_DECOMPRESS_DECODE_CHUNK_BLOCKS) {
      uint64_t left = lane->block_count - first;
      uint32_t count = left > PFSC_DECOMPRESS_DECODE_CHUNK_BLOCKS
          ? PFSC_DECOMPRESS_DECODE_CHUNK_BLOCKS
          : (uint32_t)left;
      if(pfsc_decomp_pool_enqueue_locked(pool, PFSC_DECOMP_TASK_DECODE,
                                         window, i, first, count) != 0) {
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
pfsc_decomp_lane_ready_locked(const pfsc_decomp_lane_t *lane) {
  for(uint64_t i = 0; i < lane->block_count; i++) {
    if(!lane->blocks[i].ready) return 0;
  }
  return 1;
}

static int
pfsc_decomp_wait_lane_ready(pfsc_decomp_pool_t *pool,
                            const pfsc_decomp_lane_t *lane,
                            char *err, size_t err_size) {
  pthread_mutex_lock(&pool->lock);
  while(!pfsc_decomp_lane_ready_locked(lane)) {
	    if(pfsc_decomp_pool_check_error_locked(pool, err, err_size) != 0) {
	      pthread_mutex_unlock(&pool->lock);
	      return -1;
	    }
	    if(pfsc_decomp_pool_cancel_if_requested_locked(pool, err,
	                                                   err_size) != 0) {
	      pthread_mutex_unlock(&pool->lock);
	      return -1;
	    }
	    uint64_t wait_started = monotonic_us();
	    cancel_poll_cond_wait(&pool->cond, &pool->lock);
	    job_add_wait_us(&g_job.writer_wait_us, wait_started);
	  }
  if(pfsc_decomp_pool_check_error_locked(pool, err, err_size) != 0) {
    pthread_mutex_unlock(&pool->lock);
    return -1;
  }
  pthread_mutex_unlock(&pool->lock);
  return 0;
}

static int
pfsc_decomp_write_image_window(pfsc_decomp_pool_t *pool,
                               pfsc_decomp_window_t *window,
                               int decode_workers, int out,
                               uint64_t nested_size,
                               int allow_read_overlap,
                               char *err, size_t err_size) {
  if(allow_read_overlap) {
    pfsc_decomp_pool_set_phase(pool, PFSC_DECOMP_PHASE_READ, 1,
                               decode_workers);
  } else {
    pfsc_decomp_pool_set_phase(pool, PFSC_DECOMP_PHASE_WRITE, 0,
                               decode_workers);
  }
  for(int i = 0; i < window->lane_count; i++) {
    pfsc_decomp_lane_t *lane = &window->lanes[i];
    if(pfsc_decomp_wait_lane_ready(pool, lane, err, err_size) != 0) {
      return -1;
    }
    uint64_t lane_start = lane->start_block * PFS_BLOCK_SIZE;
    if(lane_start >= nested_size) continue;
    uint64_t left = nested_size - lane_start;
    size_t useful = left < lane->decoded_size ? (size_t)left
                                              : (size_t)lane->decoded_size;
    if(useful > 0 && write_all_fd_cancelable(out, lane->decoded, useful) != 0) {
      set_err(err, err_size, "write output image: %s", strerror(errno));
      return -1;
    }
    atomic_fetch_add(&g_job.copied_bytes,
                     useful > (size_t)LONG_MAX ? LONG_MAX : (long)useful);
  }
  return 0;
}

static int
pfs_extract_plan_prepare_outputs(pfs_extract_plan_t *plan, int sync_files,
                                 char *err, size_t err_size) {
  for(size_t i = 0; i < plan->count; i++) {
    pfs_extract_entry_t *e = &plan->items[i];
    e->fd = -1;
    e->written = 0;
    e->done = 0;
    if(e->size != 0) continue;
    int fd = open(e->out_path, O_WRONLY | O_CREAT | O_TRUNC, 0777);
    if(fd < 0) {
      set_err(err, err_size, "create output file: %s", strerror(errno));
      return -1;
    }
    chmod(e->out_path, 0777);
    if(sync_files && fsync(fd) != 0) {
      set_err(err, err_size, "sync output file: %s", strerror(errno));
      close(fd);
      return -1;
    }
    if(close(fd) != 0) {
      set_err(err, err_size, "close output file: %s", strerror(errno));
      return -1;
    }
    e->done = 1;
    atomic_fetch_add(&g_job.done_files, 1);
  }
  return 0;
}

static int
pfsc_decomp_open_plan_file(pfs_extract_entry_t *e,
                           char *err, size_t err_size) {
  if(e->fd >= 0) return 0;
  job_set_current(e->rel[0] ? e->rel : e->out_path);
  e->fd = open(e->out_path, O_WRONLY | O_CREAT | O_TRUNC, 0777);
  if(e->fd < 0) {
    set_err(err, err_size, "create output file: %s", strerror(errno));
    return -1;
  }
  return 0;
}

static int
pfsc_decomp_finish_plan_file(pfs_extract_entry_t *e, int sync_file,
                             char *err, size_t err_size) {
  chmod(e->out_path, 0777);
  if(sync_file && fsync(e->fd) != 0) {
    set_err(err, err_size, "sync output file: %s", strerror(errno));
    return -1;
  }
  if(close(e->fd) != 0) {
    e->fd = -1;
    set_err(err, err_size, "close output file: %s", strerror(errno));
    return -1;
  }
  e->fd = -1;
  e->done = 1;
  atomic_fetch_add(&g_job.done_files, 1);
  return 0;
}

static void
pfsc_decomp_plan_advance(pfsc_decomp_plan_writer_t *writer) {
  while(writer->entry_index < writer->plan->count) {
    pfs_extract_entry_t *e = &writer->plan->items[writer->entry_index];
    if(e->size == 0 || e->done) writer->entry_index++;
    else break;
  }
}

static int
pfsc_decomp_write_plan_lane(pfsc_decomp_plan_writer_t *writer,
                            const pfsc_decomp_lane_t *lane,
                            char *err, size_t err_size) {
  pfs_extract_plan_t *plan = writer->plan;
  uint64_t lane_start = lane->start_block * PFS_BLOCK_SIZE;
  uint64_t lane_end = lane_start + lane->decoded_size;
  pfsc_decomp_plan_advance(writer);
  for(size_t i = writer->entry_index; i < plan->count; i++) {
    pfs_extract_entry_t *e = &plan->items[i];
    if(e->size == 0 || e->done) continue;
    uint64_t entry_end = pfs_extract_entry_end(e);
    if(entry_end <= lane_start) {
      if(e->written < e->size) {
        set_err(err, err_size, "output file missed decoded range: %s",
                e->rel[0] ? e->rel : e->out_path);
        errno = EIO;
        return -1;
      }
      continue;
    }
    if(e->image_offset >= lane_end) break;
    uint64_t overlap_start =
        e->image_offset > lane_start ? e->image_offset : lane_start;
    uint64_t overlap_end = entry_end < lane_end ? entry_end : lane_end;
    if(overlap_end <= overlap_start) continue;
    if(overlap_start < lane_start ||
       overlap_end - lane_start > lane->decoded_size) {
      set_err(err, err_size, "bad decoded output span");
      errno = EINVAL;
      return -1;
    }
    size_t n = (size_t)(overlap_end - overlap_start);
    const unsigned char *src =
        lane->decoded + (size_t)(overlap_start - lane_start);
    off_t out_off = (off_t)(overlap_start - e->image_offset);
    if((uint64_t)out_off != e->written) {
      set_err(err, err_size, "output file write order mismatch: %s",
              e->rel[0] ? e->rel : e->out_path);
      errno = EIO;
      return -1;
    }
    if(pfsc_decomp_open_plan_file(e, err, err_size) != 0) return -1;
    if(write_exact_at(e->fd, src, n, out_off) != 0) {
      set_err(err, err_size, "write output file: %s", strerror(errno));
      return -1;
    }
    e->written += n;
    atomic_fetch_add(&g_job.copied_bytes,
                     n > (size_t)LONG_MAX ? LONG_MAX : (long)n);
    if(e->written == e->size) {
      if(pfsc_decomp_finish_plan_file(e, writer->sync_files,
                                      err, err_size) != 0) {
        return -1;
      }
      if(i == writer->entry_index) pfsc_decomp_plan_advance(writer);
    } else if(e->written > e->size) {
      set_err(err, err_size, "output file overrun: %s",
              e->rel[0] ? e->rel : e->out_path);
      errno = EIO;
      return -1;
    }
  }
  return 0;
}

static int
pfsc_decomp_write_plan_window(pfsc_decomp_pool_t *pool,
                              pfsc_decomp_window_t *window,
                              int decode_workers,
                              pfsc_decomp_plan_writer_t *writer,
                              int allow_read_overlap,
                              char *err, size_t err_size) {
  if(allow_read_overlap) {
    pfsc_decomp_pool_set_phase(pool, PFSC_DECOMP_PHASE_READ, 1,
                               decode_workers);
  } else {
    pfsc_decomp_pool_set_phase(pool, PFSC_DECOMP_PHASE_WRITE, 0,
                               decode_workers);
  }
  for(int i = 0; i < window->lane_count; i++) {
    pfsc_decomp_lane_t *lane = &window->lanes[i];
    if(pfsc_decomp_wait_lane_ready(pool, lane, err, err_size) != 0) {
      return -1;
    }
    if(pfsc_decomp_write_plan_lane(writer, lane, err, err_size) != 0) {
      return -1;
    }
  }
  return 0;
}

static int
pfsc_decomp_plan_next_block(const pfs_extract_plan_t *plan,
                            size_t start_entry,
                            uint64_t min_block,
                            uint64_t *block_out) {
  for(size_t i = start_entry; i < plan->count; i++) {
    const pfs_extract_entry_t *e = &plan->items[i];
    if(e->size == 0 || e->done) continue;
    uint64_t entry_end = pfs_extract_entry_end(e);
    uint64_t end_block = ceil_div_u64(entry_end, PFS_BLOCK_SIZE);
    if(end_block <= min_block) continue;
    uint64_t first = e->image_offset / PFS_BLOCK_SIZE;
    *block_out = first > min_block ? first : min_block;
    return 0;
  }
  return -1;
}

static int
pfsc_decomp_run_image_with_lane(pfsc_reader_t *reader, int out,
                                int decode_workers, size_t lane_size,
                                int allow_io_overlap,
                                char *err, size_t err_size) {
  uint64_t lane_blocks = lane_size / (size_t)PFS_BLOCK_SIZE;
  uint64_t window_blocks = lane_blocks * (uint64_t)decode_workers;
  uint64_t total_blocks = ceil_div_u64(reader->nested_size, PFS_BLOCK_SIZE);
  pfsc_decomp_window_t *windows = NULL;
  pfsc_decomp_pool_t pool;
  int pool_started = 0;
  int rc = -1;

  if(total_blocks == 0) return 0;
  memset(&pool, 0, sizeof(pool));
  if(lane_blocks == 0 || window_blocks == 0 ||
     pfsc_decomp_alloc_windows(&windows, decode_workers, lane_size,
                               lane_blocks, err, err_size) != 0) {
    return PFSC_DECOMPRESS_WINDOW_RC_FALLBACK;
  }
  if(pfsc_decomp_pool_start(&pool, reader, decode_workers) != 0) {
    rc = PFSC_DECOMPRESS_WINDOW_RC_FALLBACK;
    goto done;
  }
  pool_started = 1;
  PFS_DECOMPRESS_LOG("decompress window start mode=image blocks=%llu workers=%d readPermits=1 laneBytes=%llu ioPolicy=%s",
                     (unsigned long long)total_blocks, decode_workers,
                     (unsigned long long)lane_size,
                     allow_io_overlap ? "pipelined" : "serial");

  uint64_t next_block = 0;
  uint64_t next_window_index = 0;
  pfsc_decomp_window_t *current = &windows[0];
  uint64_t nblocks = total_blocks > window_blocks ? window_blocks
                                                  : total_blocks;
  if(pfsc_decomp_window_prepare(current, reader, next_window_index++,
                                next_block, nblocks, lane_blocks,
                                decode_workers, err, err_size) != 0 ||
     pfsc_decomp_read_window(&pool, current, decode_workers,
                             err, err_size) != 0) {
    goto done;
  }
  next_block += current->block_count;

  while(current) {
    if(pfsc_decomp_start_decode(&pool, current, err, err_size) != 0) {
      goto done;
    }
    pfsc_decomp_window_t *next = NULL;
    int next_read_queued = 0;
    if(next_block < total_blocks) {
      next = current == &windows[0] ? &windows[1] : &windows[0];
      nblocks = total_blocks - next_block;
      if(nblocks > window_blocks) nblocks = window_blocks;
      if(pfsc_decomp_window_prepare(next, reader, next_window_index++,
                                    next_block, nblocks, lane_blocks,
                                    decode_workers, err, err_size) != 0) {
        goto done;
      }
      next_block += next->block_count;
      if(allow_io_overlap) {
        pfsc_decomp_pool_set_phase(&pool, PFSC_DECOMP_PHASE_READ, 1,
                                   decode_workers);
        if(pfsc_decomp_queue_read_window(&pool, next, err, err_size) != 0) {
          goto done;
        }
        next_read_queued = 1;
      } else {
        if(pfsc_decomp_read_window(&pool, next, decode_workers,
                                   err, err_size) != 0 ||
           pfsc_decomp_start_decode(&pool, next, err, err_size) != 0) {
          goto done;
        }
      }
    }
    if(pfsc_decomp_write_image_window(&pool, current, decode_workers, out,
                                      reader->nested_size,
                                      allow_io_overlap && next_read_queued,
                                      err,
                                      err_size) != 0) {
      goto done;
    }
    if(next_read_queued) {
      pfsc_decomp_pool_set_phase(&pool, PFSC_DECOMP_PHASE_READ, 1,
                                 decode_workers);
      if(pfsc_decomp_wait_read_window(&pool, next, err, err_size) != 0 ||
         pfsc_decomp_start_decode(&pool, next, err, err_size) != 0) {
        goto done;
      }
    }
    current = next;
  }
  rc = 0;

done:
  if(pool_started) {
    pfsc_decomp_pool_set_phase(&pool, PFSC_DECOMP_PHASE_IDLE, 0,
                               decode_workers);
    pfsc_decomp_pool_destroy(&pool);
  }
  pfsc_decomp_free_windows(windows, decode_workers);
  return rc;
}

static int
pfsc_decomp_run_plan_with_lane(pfsc_reader_t *reader,
                               pfs_extract_plan_t *plan,
                               int decode_workers, size_t lane_size,
                               int sync_files,
                               int allow_io_overlap,
                               char *err, size_t err_size) {
  uint64_t lane_blocks = lane_size / (size_t)PFS_BLOCK_SIZE;
  uint64_t window_blocks = lane_blocks * (uint64_t)decode_workers;
  pfsc_decomp_window_t *windows = NULL;
  pfsc_decomp_pool_t pool;
  pfsc_decomp_plan_writer_t writer;
  int pool_started = 0;
  int rc = -1;

  memset(&pool, 0, sizeof(pool));
  memset(&writer, 0, sizeof(writer));
  if(lane_blocks == 0 || window_blocks == 0 ||
     pfsc_decomp_alloc_windows(&windows, decode_workers, lane_size,
                               lane_blocks, err, err_size) != 0) {
    return PFSC_DECOMPRESS_WINDOW_RC_FALLBACK;
  }
  if(pfsc_decomp_pool_start(&pool, reader, decode_workers) != 0) {
    rc = PFSC_DECOMPRESS_WINDOW_RC_FALLBACK;
    goto done;
  }
  pool_started = 1;
  qsort(plan->items, plan->count, sizeof(plan->items[0]),
        pfs_extract_entry_cmp_asc);
  if(pfs_extract_plan_prepare_outputs(plan, sync_files,
                                      err, err_size) != 0) {
    goto done;
  }
  writer.plan = plan;
  writer.sync_files = sync_files;
  pfsc_decomp_plan_advance(&writer);
  PFS_DECOMPRESS_LOG("decompress window start mode=app files=%llu blocks=%llu workers=%d readPermits=1 laneBytes=%llu ioPolicy=%s",
                     (unsigned long long)plan->count,
                     (unsigned long long)plan->total_blocks,
                     decode_workers,
                     (unsigned long long)lane_size,
                     allow_io_overlap ? "pipelined" : "serial");

  uint64_t next_block = 0;
  uint64_t next_window_index = 0;
  if(pfsc_decomp_plan_next_block(plan, writer.entry_index, next_block,
                                 &next_block) != 0) {
    rc = 0;
    goto done;
  }
  pfsc_decomp_window_t *current = &windows[0];
  uint64_t nblocks = reader->block_count - next_block;
  if(nblocks > window_blocks) nblocks = window_blocks;
  if(pfsc_decomp_window_prepare(current, reader, next_window_index++,
                                next_block, nblocks, lane_blocks,
                                decode_workers, err, err_size) != 0 ||
     pfsc_decomp_read_window(&pool, current, decode_workers,
                             err, err_size) != 0) {
    goto done;
  }
  next_block = current->start_block + current->block_count;

  while(current) {
    if(pfsc_decomp_start_decode(&pool, current, err, err_size) != 0) {
      goto done;
    }
    pfsc_decomp_window_t *next = NULL;
    int next_read_queued = 0;
    uint64_t wanted = 0;
    if(pfsc_decomp_plan_next_block(plan, writer.entry_index, next_block,
                                   &wanted) == 0) {
      next = current == &windows[0] ? &windows[1] : &windows[0];
      nblocks = reader->block_count - wanted;
      if(nblocks > window_blocks) nblocks = window_blocks;
      if(pfsc_decomp_window_prepare(next, reader, next_window_index++,
                                    wanted, nblocks, lane_blocks,
                                    decode_workers, err, err_size) != 0) {
        goto done;
      }
      next_block = next->start_block + next->block_count;
      if(allow_io_overlap) {
        pfsc_decomp_pool_set_phase(&pool, PFSC_DECOMP_PHASE_READ, 1,
                                   decode_workers);
        if(pfsc_decomp_queue_read_window(&pool, next, err, err_size) != 0) {
          goto done;
        }
        next_read_queued = 1;
      } else {
        if(pfsc_decomp_read_window(&pool, next, decode_workers,
                                   err, err_size) != 0 ||
           pfsc_decomp_start_decode(&pool, next, err, err_size) != 0) {
          goto done;
        }
      }
    }
    if(pfsc_decomp_write_plan_window(&pool, current, decode_workers,
                                     &writer,
                                     allow_io_overlap && next_read_queued,
                                     err, err_size) != 0) {
      goto done;
    }
    if(next_read_queued) {
      pfsc_decomp_pool_set_phase(&pool, PFSC_DECOMP_PHASE_READ, 1,
                                 decode_workers);
      if(pfsc_decomp_wait_read_window(&pool, next, err, err_size) != 0 ||
         pfsc_decomp_start_decode(&pool, next, err, err_size) != 0) {
        goto done;
      }
    }
    current = next;
  }
  pfsc_decomp_plan_advance(&writer);
  if(writer.entry_index != plan->count) {
    set_err(err, err_size, "not all output files were written");
    errno = EIO;
    goto done;
  }
  rc = 0;

done:
  if(pool_started) {
    pfsc_decomp_pool_set_phase(&pool, PFSC_DECOMP_PHASE_IDLE, 0,
                               decode_workers);
    pfsc_decomp_pool_destroy(&pool);
  }
  pfsc_decomp_free_windows(windows, decode_workers);
  return rc;
}

static int
pfsc_decomp_write_image_windowed(pfsc_reader_t *reader, int out,
                                 int requested_workers,
                                 int allow_io_overlap,
                                 char *err, size_t err_size) {
  int workers = pfsc_decomp_worker_count(requested_workers);
  for(size_t lane_size = (size_t)PFSC_DECOMPRESS_LANE_MAX_SIZE;
      lane_size >= (size_t)PFSC_DECOMPRESS_LANE_MIN_SIZE;
      lane_size /= 2) {
    char local_err[256] = {0};
    int rc = pfsc_decomp_run_image_with_lane(reader, out, workers,
                                             lane_size, allow_io_overlap,
                                             local_err,
                                             sizeof(local_err));
    if(rc == 0) return 0;
    if(rc != PFSC_DECOMPRESS_WINDOW_RC_FALLBACK) {
      set_err(err, err_size, "%s",
              local_err[0] ? local_err : "windowed decompression failed");
      return -1;
    }
    PFS_DECOMPRESS_LOG("decompress window fallback candidate failed mode=image workers=%d laneBytes=%llu reason=%s",
                       workers, (unsigned long long)lane_size,
                       local_err[0] ? local_err : "unavailable");
    if(lane_size == (size_t)PFSC_DECOMPRESS_LANE_MIN_SIZE) break;
  }
  return PFSC_DECOMPRESS_WINDOW_RC_FALLBACK;
}

static int
pfsc_decomp_extract_plan_windowed(pfsc_reader_t *reader,
                                  pfs_extract_plan_t *plan,
                                  int requested_workers,
                                  int sync_files,
                                  int allow_io_overlap,
                                  char *err, size_t err_size) {
  int workers = pfsc_decomp_worker_count(requested_workers);
  for(size_t lane_size = (size_t)PFSC_DECOMPRESS_LANE_MAX_SIZE;
      lane_size >= (size_t)PFSC_DECOMPRESS_LANE_MIN_SIZE;
      lane_size /= 2) {
    char local_err[256] = {0};
    int rc = pfsc_decomp_run_plan_with_lane(reader, plan, workers, lane_size,
                                            sync_files, allow_io_overlap,
                                            local_err,
                                            sizeof(local_err));
    if(rc == 0) return 0;
    if(rc != PFSC_DECOMPRESS_WINDOW_RC_FALLBACK) {
      set_err(err, err_size, "%s",
              local_err[0] ? local_err : "windowed decompression failed");
      return -1;
    }
    PFS_DECOMPRESS_LOG("decompress window fallback candidate failed mode=app workers=%d laneBytes=%llu reason=%s",
                       workers, (unsigned long long)lane_size,
                       local_err[0] ? local_err : "unavailable");
    if(lane_size == (size_t)PFSC_DECOMPRESS_LANE_MIN_SIZE) break;
  }
  return PFSC_DECOMPRESS_WINDOW_RC_FALLBACK;
}

static int
pfsc_decode_find_ready_slot(pfsc_decode_pool_t *pool) {
  for(int i = 0; i < pool->slot_count; i++) {
    if(pool->slots[i].state == PFS_BLOCK_SLOT_READY) return i;
  }
  return -1;
}

static int
pfsc_decode_slot(const pfsc_reader_t *r, pfs_block_slot_t *slot) {
  if(slot->index >= r->block_count) return EINVAL;
  uint64_t start = r->offsets[slot->index];
  uint64_t end = r->offsets[slot->index + 1];
  if(end < start || end - start > r->block_size) return EINVAL;
  size_t stored_len = (size_t)(end - start);

  if(stored_len == (size_t)r->block_size) {
    if(read_exact_at(r->fd, slot->output, stored_len,
                     (off_t)(r->file_start + start)) != 0) {
      return errno ? errno : EIO;
    }
    slot->input_len = stored_len;
    slot->flag = 1;
    atomic_fetch_add(&g_job.raw_blocks, 1);
    return 0;
  }

  if(read_exact_at(r->fd, slot->input, stored_len,
                   (off_t)(r->file_start + start)) != 0) {
    return errno ? errno : EIO;
  }
  size_t out_len = tinfl_decompress_mem_to_mem(
      slot->output, (size_t)r->block_size, slot->input, stored_len,
      TINFL_FLAG_PARSE_ZLIB_HEADER);
  if(out_len != (size_t)r->block_size) return EINVAL;
  slot->input_len = stored_len;
  slot->flag = 0;
  atomic_fetch_add(&g_job.compressed_blocks, 1);
  return 0;
}

static void *
pfsc_decode_worker_main(void *arg) {
  pfsc_decode_pool_t *pool = arg;
  for(;;) {
    pthread_mutex_lock(&pool->lock);
	    int slot_index;
	    while(!pool->stop && (slot_index = pfsc_decode_find_ready_slot(pool)) < 0) {
	      uint64_t wait_started = monotonic_us();
	      cancel_poll_cond_wait(&pool->cond, &pool->lock);
	      job_add_wait_us(&g_job.worker_wait_us, wait_started);
	    }
    if(pool->stop) {
      pthread_mutex_unlock(&pool->lock);
      break;
    }
    pfs_block_slot_t *slot = &pool->slots[slot_index];
    slot->state = PFS_BLOCK_SLOT_BUSY;
    pthread_mutex_unlock(&pool->lock);

    int rc = pfsc_decode_slot(pool->reader, slot);

    pthread_mutex_lock(&pool->lock);
    if(rc != 0) {
      if(!pool->error) pool->error = rc;
      pool->stop = 1;
    }
    slot->state = PFS_BLOCK_SLOT_DONE;
    pthread_cond_broadcast(&pool->cond);
    pthread_mutex_unlock(&pool->lock);

    if(rc != 0) break;
  }
  return NULL;
}

static void
pfsc_decode_pool_stop(pfsc_decode_pool_t *pool) {
  pthread_mutex_lock(&pool->lock);
  pool->stop = 1;
  pthread_cond_broadcast(&pool->cond);
  pthread_mutex_unlock(&pool->lock);
}

static int
pfs_extract_file_serial(pfs_nested_reader_t *nr, const pfs_inode_info_t *ino,
                        int out, char *err, size_t err_size) {
  unsigned char *buf = malloc((size_t)PFS_BLOCK_SIZE);
  pfs_stream_buffer_t outbuf;
  int rc = -1;
  pfs_stream_buffer_init(&outbuf, PFS_EXTRACT_OUTPUT_BUFFER_SIZE,
                         PFS_EXTRACT_OUTPUT_BUFFER_MIN_SIZE);
  if(!buf) {
    pfs_stream_buffer_free(&outbuf);
    set_err(err, err_size, "out of memory");
    errno = ENOMEM;
    return -1;
  }
  if(ino->db0 < 0) {
    pfs_stream_buffer_free(&outbuf);
    free(buf);
    set_err(err, err_size, "file payload outside nested image");
    errno = EINVAL;
    return -1;
  }

  for(uint64_t off = 0; off < ino->size;) {
    if(job_cancelled()) {
      set_err(err, err_size, "cancelled");
      errno = EINTR;
      goto done;
    }
    size_t n = ino->size - off > PFS_BLOCK_SIZE
                   ? (size_t)PFS_BLOCK_SIZE
                   : (size_t)(ino->size - off);
    uint64_t image_off = (uint64_t)ino->db0 * nr->block_size + off;
    if(pfsc_reader_write_buffered_range(&nr->pfsc, image_off, n,
                                        &outbuf, out, buf,
                                        err, err_size) != 0) {
      goto done;
    }
    off += n;
    atomic_fetch_add(&g_job.copied_bytes,
                     n > (size_t)LONG_MAX ? LONG_MAX : (long)n);
  }
  if(pfs_stream_buffer_flush(&outbuf, out, write_all_fd_cancelable) != 0) {
    set_err(err, err_size, "write output file: %s", strerror(errno));
    goto done;
  }
  rc = 0;

done:
  pfs_stream_buffer_free(&outbuf);
  free(buf);
  return rc;
}

static int
pfs_extract_file_parallel(pfs_nested_reader_t *nr, const pfs_inode_info_t *ino,
                          int out, int requested_workers,
                          char *err, size_t err_size) {
  uint64_t block_count = ceil_div_u64(ino->size, PFS_BLOCK_SIZE);
  uint64_t base_block = (uint64_t)ino->db0;
  int worker_count = requested_workers;
  int slot_count;
  pfs_block_slot_t *slots = NULL;
  pthread_t *threads = NULL;
  int workers_started = 0;
  int pool_initialized = 0;
  pfsc_decode_pool_t pool;
  pfs_stream_buffer_t outbuf;
  int rc = -1;

  if(block_count == 0) return 0;
  if(ino->db0 < 0 || base_block > nr->pfsc.block_count ||
     block_count > nr->pfsc.block_count - base_block) {
    set_err(err, err_size, "file payload outside nested image");
    errno = EINVAL;
    return -1;
  }
  if(nr->pfsc.mounted_enabled) {
    return pfs_extract_file_serial(nr, ino, out, err, err_size);
  }

  if(worker_count <= 0) worker_count = PFS_COMPRESS_DEFAULT_WORKERS;
  if(worker_count > PFS_COMPRESS_MAX_WORKERS) worker_count = PFS_COMPRESS_MAX_WORKERS;
  if(block_count < (uint64_t)worker_count) worker_count = (int)block_count;
  if(worker_count < 1) worker_count = 1;

  if(worker_count <= 1 || block_count < 4) {
    return pfs_extract_file_serial(nr, ino, out, err, err_size);
  }

  slot_count = worker_count * PFSC_DECOMPRESS_SLOTS_PER_WORKER;
  if(block_count < (uint64_t)slot_count) slot_count = (int)block_count;
  if(slot_count < 1) slot_count = 1;

  memset(&pool, 0, sizeof(pool));
  pfs_stream_buffer_init(&outbuf, PFS_EXTRACT_OUTPUT_BUFFER_SIZE,
                         PFS_EXTRACT_OUTPUT_BUFFER_MIN_SIZE);
  if(pfs_block_slots_alloc(&slots, slot_count, (size_t)PFS_BLOCK_SIZE,
                           (size_t)PFS_BLOCK_SIZE) != 0) {
    set_err(err, err_size, "out of memory");
    goto done;
  }
  threads = calloc((size_t)worker_count, sizeof(*threads));
  if(!threads) {
    set_err(err, err_size, "out of memory");
    goto done;
  }
  if(pthread_mutex_init(&pool.lock, NULL) != 0 ||
     pthread_cond_init(&pool.cond, NULL) != 0) {
    set_err(err, err_size, "init decompression tasks failed");
    goto done;
  }
  pool_initialized = 1;
  pool.reader = &nr->pfsc;
  pool.slots = slots;
  pool.slot_count = slot_count;

  for(int i = 0; i < worker_count; i++) {
    int trc = pthread_create(&threads[i], NULL, pfsc_decode_worker_main, &pool);
    if(trc != 0) {
      set_err(err, err_size, "start decompression task: %s", strerror(trc));
      goto done;
    }
    workers_started++;
  }

  uint64_t next_read = 0;
  uint64_t next_write = 0;
  while(next_write < block_count) {
    while(next_read < block_count &&
          next_read - next_write < (uint64_t)slot_count) {
      pfs_block_slot_t *slot = &slots[next_read % (uint64_t)slot_count];

      pthread_mutex_lock(&pool.lock);
      while(slot->state != PFS_BLOCK_SLOT_FREE && !pool.error) {
        if(job_cancelled()) {
          if(!pool.error) pool.error = EINTR;
          pool.stop = 1;
          pthread_cond_broadcast(&pool.cond);
          break;
        }
        cancel_poll_cond_wait(&pool.cond, &pool.lock);
      }
      if(pool.error) {
        int saved = pool.error;
        pthread_mutex_unlock(&pool.lock);
        errno = saved;
        if(saved == EINTR) set_err(err, err_size, "cancelled");
        else set_err(err, err_size, "decompression task failed: %s",
                     strerror(saved));
        goto done;
      }
      slot->state = PFS_BLOCK_SLOT_FILLING;
      slot->index = base_block + next_read;
      uint64_t left = ino->size - next_read * PFS_BLOCK_SIZE;
      slot->output_len = left > PFS_BLOCK_SIZE ? (size_t)PFS_BLOCK_SIZE
                                               : (size_t)left;
      slot->input_len = 0;
      slot->flag = 0;
      pthread_mutex_unlock(&pool.lock);

      if(job_cancelled()) {
        pthread_mutex_lock(&pool.lock);
        slot->state = PFS_BLOCK_SLOT_FREE;
        pthread_cond_broadcast(&pool.cond);
        pthread_mutex_unlock(&pool.lock);
        set_err(err, err_size, "cancelled");
        errno = EINTR;
        goto done;
      }

      pthread_mutex_lock(&pool.lock);
      slot->state = PFS_BLOCK_SLOT_READY;
      pthread_cond_broadcast(&pool.cond);
      pthread_mutex_unlock(&pool.lock);
      next_read++;
    }

    pfs_block_slot_t *slot = &slots[next_write % (uint64_t)slot_count];
    pthread_mutex_lock(&pool.lock);
    while((slot->state != PFS_BLOCK_SLOT_DONE ||
           slot->index != base_block + next_write) &&
          !pool.error) {
      uint64_t wait_started = monotonic_us();
      if(job_cancelled()) {
        if(!pool.error) pool.error = EINTR;
        pool.stop = 1;
        pthread_cond_broadcast(&pool.cond);
        break;
      }
      cancel_poll_cond_wait(&pool.cond, &pool.lock);
      job_add_wait_us(&g_job.writer_wait_us, wait_started);
    }
    if(pool.error) {
      int saved = pool.error;
      pthread_mutex_unlock(&pool.lock);
      errno = saved;
      if(saved == EINTR) set_err(err, err_size, "cancelled");
      else set_err(err, err_size, "decompression task failed: %s",
                   strerror(saved));
      goto done;
    }
    pthread_mutex_unlock(&pool.lock);

    if(pfs_stream_buffer_write(&outbuf, out, write_all_fd_cancelable,
                               slot->output, slot->output_len) != 0) {
      set_err(err, err_size, "write output file: %s", strerror(errno));
      goto done;
    }
    atomic_fetch_add(&g_job.copied_bytes,
                     slot->output_len > (size_t)LONG_MAX ? LONG_MAX :
                     (long)slot->output_len);

    pthread_mutex_lock(&pool.lock);
    slot->state = PFS_BLOCK_SLOT_FREE;
    pthread_cond_broadcast(&pool.cond);
    pthread_mutex_unlock(&pool.lock);
    next_write++;
  }

  if(pfs_stream_buffer_flush(&outbuf, out, write_all_fd_cancelable) != 0) {
    set_err(err, err_size, "write output file: %s", strerror(errno));
    goto done;
  }
  rc = 0;

done:
  if(pool_initialized) pfsc_decode_pool_stop(&pool);
  for(int i = 0; i < workers_started; i++) pthread_join(threads[i], NULL);
  if(pool_initialized) {
    pthread_cond_destroy(&pool.cond);
    pthread_mutex_destroy(&pool.lock);
  }
  free(threads);
  pfs_block_slots_free(slots, slot_count);
  pfs_stream_buffer_free(&outbuf);
  return rc;
}

static int
pfs_extract_file(pfs_nested_reader_t *nr, uint32_t inode_num,
                 const char *out_path, const char *rel, int workers,
                 int sync_file, char *err, size_t err_size) {
  pfs_inode_info_t ino;
  int out = -1;
  int rc = -1;

  if(pfs_nested_read_inode(nr, inode_num, &ino, err, err_size) != 0) return -1;
  if((ino.mode & PFS_INODE_MODE_FILE) == 0) {
    set_err(err, err_size, "expected file inode");
    errno = EINVAL;
    return -1;
  }

  out = open(out_path, O_WRONLY | O_CREAT | O_TRUNC, 0777);
  if(out < 0) {
    set_err(err, err_size, "create output file: %s", strerror(errno));
    return -1;
  }

  job_set_current(rel && rel[0] ? rel : out_path);
  if(pfs_extract_file_parallel(nr, &ino, out, workers, err, err_size) != 0) {
    goto done;
  }
  chmod(out_path, 0777);
  if(sync_file && fsync(out) != 0) {
    set_err(err, err_size, "sync output file: %s", strerror(errno));
    goto done;
  }
  atomic_fetch_add(&g_job.done_files, 1);
  rc = 0;

done:
  if(out >= 0) close(out);
  if(rc != 0) unlink(out_path);
  return rc;
}

static int
pfs_extract_dir(pfs_nested_reader_t *nr, uint32_t inode_num,
                const char *out_dir, const char *rel, int workers,
                int sync_files, char *err, size_t err_size) {
  pfs_inode_info_t dir_ino;
  if(pfs_nested_read_inode(nr, inode_num, &dir_ino, err, err_size) != 0) {
    return -1;
  }
  if((dir_ino.mode & PFS_INODE_MODE_DIR) == 0) {
    set_err(err, err_size, "expected directory inode");
    errno = EINVAL;
    return -1;
  }
  if(mkdir_one_local(out_dir) != 0) {
    set_err(err, err_size, "create output folder: %s", strerror(errno));
    return -1;
  }

  for(uint64_t off = 0; off + 16 <= dir_ino.size;) {
    pfs_dirent_info_t ent;
    int end = 0;
    if(pfs_nested_read_dirent(nr, &dir_ino, off, &ent, &end,
                              err, err_size) != 0) {
      return -1;
    }
    if(end) break;
    if(strcmp(ent.name, ".") != 0 && strcmp(ent.name, "..") != 0) {
      if(!path_segment_supported(ent.name)) {
        set_err(err, err_size, "unsafe PFS path segment");
        errno = EINVAL;
        return -1;
      }
      char child_out[1024];
      char child_rel[1024];
      if(join_abs(child_out, sizeof(child_out), out_dir, ent.name) != 0 ||
         join_rel(child_rel, sizeof(child_rel), rel ? rel : "", ent.name) != 0) {
        set_err(err, err_size, "output path too long");
        return -1;
      }
      if(ent.type == PFS_DIRENT_TYPE_DIRECTORY) {
        if(pfs_extract_dir(nr, ent.child_inode, child_out, child_rel, workers,
                           sync_files, err, err_size) != 0) {
          return -1;
        }
      } else if(ent.type == PFS_DIRENT_TYPE_FILE) {
        if(pfs_extract_file(nr, ent.child_inode, child_out, child_rel, workers,
                            sync_files, err, err_size) != 0) {
          return -1;
        }
      } else {
        set_err(err, err_size, "unsupported PFS dirent type");
        errno = EINVAL;
        return -1;
      }
    }
    off += ent.ent_size;
  }
  chmod(out_dir, 0777);
  return 0;
}

typedef struct exfat_reader {
  pfsc_reader_t pfsc;
  uint64_t sector_size;
  uint64_t cluster_size;
  uint32_t fat_offset;
  uint32_t fat_length;
  uint32_t cluster_heap_offset;
  uint32_t cluster_count;
  uint32_t root_cluster;
} exfat_reader_t;

static void
exfat_reader_close(exfat_reader_t *er) {
  if(!er) return;
  pfsc_reader_close(&er->pfsc);
  memset(er, 0, sizeof(*er));
}

static uint64_t
exfat_reader_cluster_offset(const exfat_reader_t *er, uint32_t cluster) {
  return (uint64_t)er->cluster_heap_offset * er->sector_size +
         ((uint64_t)cluster - 2ULL) * er->cluster_size;
}

static int
exfat_reader_check_cluster(const exfat_reader_t *er, uint32_t cluster,
                           char *err, size_t err_size) {
  if(cluster < 2 || (uint64_t)cluster - 2ULL >= er->cluster_count) {
    set_err(err, err_size, "exFAT cluster outside image");
    errno = EINVAL;
    return -1;
  }
  return 0;
}

static int
exfat_reader_read_contiguous(exfat_reader_t *er, uint32_t first_cluster,
                             uint64_t stream_off, void *out, size_t size,
                             char *err, size_t err_size) {
  if(size == 0) return 0;
  if(first_cluster == 0) {
    set_err(err, err_size, "exFAT file has no data cluster");
    errno = EINVAL;
    return -1;
  }
  uint64_t cluster_delta = stream_off / er->cluster_size;
  uint64_t cluster_off = stream_off % er->cluster_size;
  if((uint64_t)first_cluster + cluster_delta > UINT32_MAX ||
     exfat_reader_check_cluster(er, first_cluster + (uint32_t)cluster_delta,
                                err, err_size) != 0) {
    return -1;
  }
  uint64_t image_off =
    exfat_reader_cluster_offset(er, first_cluster + (uint32_t)cluster_delta) +
    cluster_off;
  return pfsc_reader_read(&er->pfsc, image_off, out, size, err, err_size);
}

static int
exfat_reader_read_fat(exfat_reader_t *er, uint32_t cluster, uint32_t *next,
                      char *err, size_t err_size) {
  unsigned char raw[4];
  uint64_t off = (uint64_t)er->fat_offset * er->sector_size +
                 (uint64_t)cluster * 4ULL;
  uint64_t fat_end = ((uint64_t)er->fat_offset + er->fat_length) *
                     er->sector_size;
  if(off + sizeof(raw) > fat_end) {
    set_err(err, err_size, "exFAT FAT entry outside table");
    errno = EINVAL;
    return -1;
  }
  if(pfsc_reader_read(&er->pfsc, off, raw, sizeof(raw),
                      err, err_size) != 0) {
    return -1;
  }
  *next = rd32(raw);
  return 0;
}

static int
exfat_reader_chain_clusters(exfat_reader_t *er, uint32_t first_cluster,
                            uint32_t *clusters, char *err, size_t err_size) {
  uint32_t count = 0;
  uint32_t cluster = first_cluster;
  if(exfat_reader_check_cluster(er, cluster, err, err_size) != 0) return -1;
  for(;;) {
    uint32_t next = 0;
    count++;
    if(count > er->cluster_count) {
      set_err(err, err_size, "exFAT directory chain loops");
      errno = EINVAL;
      return -1;
    }
    if(exfat_reader_read_fat(er, cluster, &next, err, err_size) != 0) {
      return -1;
    }
    if(next >= 0xfffffff8U || next == 0xffffffffU) break;
    if(next != cluster + 1) {
      set_err(err, err_size, "fragmented exFAT directory is not supported");
      errno = EINVAL;
      return -1;
    }
    cluster = next;
  }
  *clusters = count;
  return 0;
}

static int
exfat_reader_open(const char *path, exfat_reader_t *er, int writable,
                  char *err, size_t err_size) {
  int fd = -1;
  unsigned char boot[512];
  int rc = -1;
  memset(er, 0, sizeof(*er));
  er->pfsc.fd = -1;

  fd = open(path, writable ? O_RDWR : O_RDONLY);
  if(fd < 0) {
    set_err(err, err_size, "open input: %s", strerror(errno));
    return -1;
  }
  if(pfsc_reader_open_from_outer(fd, &er->pfsc, err, err_size) != 0) {
    if(er->pfsc.fd >= 0) pfsc_reader_close(&er->pfsc);
    else close(fd);
    return -1;
  }
  fd = -1;
  if(!writable) pfsc_reader_try_open_mounted(&er->pfsc, path);

  if(pfsc_reader_read(&er->pfsc, 0, boot, sizeof(boot), err, err_size) != 0) {
    goto done;
  }
  if(memcmp(boot + 3, "EXFAT   ", 8) != 0) {
    set_err(err, err_size, "nested image is not exFAT");
    errno = EINVAL;
    goto done;
  }
  uint8_t sector_shift = boot[0x6c];
  uint8_t cluster_shift = boot[0x6d];
  if(sector_shift >= 31 || cluster_shift >= 31) {
    set_err(err, err_size, "unsupported exFAT sector geometry");
    errno = EINVAL;
    goto done;
  }
  er->sector_size = 1ULL << sector_shift;
  er->cluster_size = er->sector_size * (1ULL << cluster_shift);
  er->fat_offset = rd32(boot + 0x50);
  er->fat_length = rd32(boot + 0x54);
  er->cluster_heap_offset = rd32(boot + 0x58);
  er->cluster_count = rd32(boot + 0x5c);
  er->root_cluster = rd32(boot + 0x60);
  if(er->sector_size != 512 || er->cluster_size != PFS_BLOCK_SIZE ||
     boot[0x6e] != 1 || er->fat_offset == 0 || er->fat_length == 0 ||
     er->cluster_heap_offset <= er->fat_offset ||
     er->cluster_count == 0 ||
     exfat_reader_check_cluster(er, er->root_cluster, err, err_size) != 0) {
    set_err(err, err_size, "unsupported exFAT layout");
    errno = EINVAL;
    goto done;
  }
  rc = 0;

done:
  if(rc != 0) exfat_reader_close(er);
  if(fd >= 0) close(fd);
  return rc;
}

static int
exfat_plan_push(pfs_extract_plan_t *plan, uint64_t image_offset,
                uint64_t size, const char *out_path, const char *rel,
                char *err, size_t err_size) {
  if(plan->count == plan->cap) {
    size_t next = plan->cap ? plan->cap * 2 : 128;
    pfs_extract_entry_t *p = realloc(plan->items, next * sizeof(*p));
    if(!p) {
      set_err(err, err_size, "out of memory");
      errno = ENOMEM;
      return -1;
    }
    plan->items = p;
    plan->cap = next;
  }
  pfs_extract_entry_t *e = &plan->items[plan->count++];
  memset(e, 0, sizeof(*e));
  e->fd = -1;
  snprintf(e->rel, sizeof(e->rel), "%s", rel);
  snprintf(e->out_path, sizeof(e->out_path), "%s", out_path);
  e->image_offset = image_offset;
  e->first_block = image_offset / PFS_BLOCK_SIZE;
  e->block_count = ceil_div_u64(size, PFS_BLOCK_SIZE);
  e->size = size;
  plan->total_bytes += size;
  plan->total_blocks += e->block_count;
  return 0;
}

static int
exfat_decode_name(const unsigned char *set, size_t entry_count,
                  size_t name_len, char *name, size_t name_size,
                  char *err, size_t err_size) {
  if(name_len == 0 || name_len >= name_size) {
    set_err(err, err_size, "invalid exFAT name length");
    errno = EINVAL;
    return -1;
  }
  size_t pos = 0;
  for(size_t e = 2; e < entry_count && pos < name_len; e++) {
    const unsigned char *fn = set + e * 32;
    if(fn[0] != 0xC1) {
      set_err(err, err_size, "invalid exFAT filename entry");
      errno = EINVAL;
      return -1;
    }
    for(size_t i = 0; i < 15 && pos < name_len; i++) {
      uint16_t ch = rd16(fn + 2 + i * 2);
      if(ch == 0 || ch >= 0x80) {
        set_err(err, err_size, "unsupported exFAT filename");
        errno = EINVAL;
        return -1;
      }
      name[pos++] = (char)ch;
    }
  }
  if(pos != name_len) {
    set_err(err, err_size, "truncated exFAT filename");
    errno = EINVAL;
    return -1;
  }
  name[pos] = 0;
  if(!path_segment_supported(name)) {
    set_err(err, err_size, "unsafe exFAT path segment");
    errno = EINVAL;
    return -1;
  }
  return 0;
}

static int
exfat_collect_dir(exfat_reader_t *er, uint32_t first_cluster,
                  uint64_t dir_size, const char *out_dir, const char *rel,
                  pfs_extract_plan_t *plan, char *err, size_t err_size) {
  if(mkdir_one_local(out_dir) != 0) {
    set_err(err, err_size, "create output folder: %s", strerror(errno));
    return -1;
  }
  if(dir_size == 0) {
    uint32_t clusters = 0;
    if(exfat_reader_chain_clusters(er, first_cluster, &clusters,
                                   err, err_size) != 0) {
      return -1;
    }
    dir_size = (uint64_t)clusters * er->cluster_size;
  }

  unsigned char entry[32];
  for(uint64_t off = 0; off + 32 <= dir_size;) {
    if(job_cancelled()) {
      set_err(err, err_size, "cancelled");
      errno = EINTR;
      return -1;
    }
    if(exfat_reader_read_contiguous(er, first_cluster, off, entry,
                                    sizeof(entry), err, err_size) != 0) {
      return -1;
    }
    if(entry[0] == 0x00) break;
    if(entry[0] == 0x81 || entry[0] == 0x82) {
      off += 32;
      continue;
    }
    if(entry[0] != 0x85) {
      off += 32;
      continue;
    }

    size_t secondary_count = entry[1];
    size_t entry_count = secondary_count + 1;
    if(entry_count < 3 || entry_count > 19 ||
       off + entry_count * 32ULL > dir_size) {
      set_err(err, err_size, "invalid exFAT directory entry set");
      errno = EINVAL;
      return -1;
    }
    unsigned char set[32 * 19];
    memcpy(set, entry, sizeof(entry));
    if(exfat_reader_read_contiguous(er, first_cluster, off + 32,
                                    set + 32, secondary_count * 32,
                                    err, err_size) != 0) {
      return -1;
    }
    const unsigned char *stream = set + 32;
    if(stream[0] != 0xC0) {
      set_err(err, err_size, "invalid exFAT stream entry");
      errno = EINVAL;
      return -1;
    }
    char name[256];
    size_t name_len = stream[3];
    if(exfat_decode_name(set, entry_count, name_len, name, sizeof(name),
                         err, err_size) != 0) {
      return -1;
    }

    uint16_t attrs = rd16(set + 4);
    uint32_t child_cluster = rd32(stream + 20);
    uint64_t data_len = rd64(stream + 24);
    char child_out[1024];
    char child_rel[1024];
    if(join_abs(child_out, sizeof(child_out), out_dir, name) != 0 ||
       join_rel(child_rel, sizeof(child_rel), rel ? rel : "", name) != 0) {
      set_err(err, err_size, "output path too long");
      return -1;
    }
    if(attrs & 0x10) {
      if(child_cluster == 0 ||
         exfat_collect_dir(er, child_cluster, data_len, child_out, child_rel,
                           plan, err, err_size) != 0) {
        return -1;
      }
    } else {
      if(data_len > 0 && child_cluster == 0) {
        set_err(err, err_size, "exFAT file has data without a cluster");
        errno = EINVAL;
        return -1;
      }
      uint64_t image_offset = child_cluster
        ? exfat_reader_cluster_offset(er, child_cluster)
        : 0;
      if(child_cluster != 0 &&
         exfat_reader_check_cluster(er, child_cluster, err, err_size) != 0) {
        return -1;
      }
      if(exfat_plan_push(plan, image_offset, data_len, child_out, child_rel,
                         err, err_size) != 0) {
        return -1;
      }
    }
    off += entry_count * 32ULL;
  }
  chmod(out_dir, 0777);
  return 0;
}

static int
exfat_extract_file(exfat_reader_t *er, const pfs_extract_entry_t *e,
                   int sync_file, char *err, size_t err_size) {
  unsigned char *buf = malloc((size_t)PFS_BLOCK_SIZE);
  pfs_stream_buffer_t outbuf;
  int out = -1;
  int rc = -1;
  pfs_stream_buffer_init(&outbuf, PFS_EXTRACT_OUTPUT_BUFFER_SIZE,
                         PFS_EXTRACT_OUTPUT_BUFFER_MIN_SIZE);
  if(!buf) {
    pfs_stream_buffer_free(&outbuf);
    set_err(err, err_size, "out of memory");
    errno = ENOMEM;
    return -1;
  }
  out = open(e->out_path, O_WRONLY | O_CREAT | O_TRUNC, 0777);
  if(out < 0) {
    set_err(err, err_size, "create output file: %s", strerror(errno));
    goto done;
  }

  job_set_current(e->rel[0] ? e->rel : e->out_path);
  for(uint64_t off = 0; off < e->size;) {
    if(job_cancelled()) {
      set_err(err, err_size, "cancelled");
      errno = EINTR;
      goto done;
    }
    size_t n = e->size - off > PFS_BLOCK_SIZE
      ? (size_t)PFS_BLOCK_SIZE
      : (size_t)(e->size - off);
    if(pfsc_reader_write_buffered_range(&er->pfsc, e->image_offset + off,
                                        n, &outbuf, out, buf,
                                        err, err_size) != 0) {
      goto done;
    }
    off += n;
    atomic_fetch_add(&g_job.copied_bytes,
                     n > (size_t)LONG_MAX ? LONG_MAX : (long)n);
  }
  if(pfs_stream_buffer_flush(&outbuf, out, write_all_fd_cancelable) != 0) {
    set_err(err, err_size, "write output file: %s", strerror(errno));
    goto done;
  }
  chmod(e->out_path, 0777);
  if(sync_file && fsync(out) != 0) {
    set_err(err, err_size, "sync output file: %s", strerror(errno));
    goto done;
  }
  atomic_fetch_add(&g_job.done_files, 1);
  rc = 0;

done:
  if(out >= 0) close(out);
  if(rc != 0) unlink(e->out_path);
  pfs_stream_buffer_free(&outbuf);
  free(buf);
  return rc;
}

static int
exfat_extract_plan(exfat_reader_t *er, pfs_extract_plan_t *plan,
                   char *err, size_t err_size) {
  for(size_t i = 0; i < plan->count; i++) {
    if(job_cancelled()) {
      set_err(err, err_size, "cancelled");
      errno = EINTR;
      return -1;
    }
    if(exfat_extract_file(er, &plan->items[i], 1, err, err_size) != 0) {
      return -1;
    }
  }
  return 0;
}

int
pfs_decompress_probe(const char *path, pfs_decompress_info_t *info,
                     char *err, size_t err_size) {
  char input[1024];
  char clean[1024];
  char parent[1024];
  char base[256];
  char stem[256];
  struct stat st;
  pfs_decompress_info_t local;
  int input_n = snprintf(input, sizeof(input), "%s", path ? path : "");
  if(input_n < 0 || (size_t)input_n >= sizeof(input)) {
    set_err(err, err_size, "bad path");
    errno = ENAMETOOLONG;
    return -1;
  }
  if(!info) info = &local;
  memset(info, 0, sizeof(*info));

  if(normalize_app_path(input, clean, sizeof(clean)) != 0) {
    set_err(err, err_size, "bad path");
    return -1;
  }
  if(!ffpfsc_path_supported(clean)) {
    set_err(err, err_size, "not an .ffpfsc file");
    errno = EINVAL;
    return -1;
  }
  if(stat(clean, &st) != 0 || !S_ISREG(st.st_mode)) {
    set_err(err, err_size, "not a file");
    return -1;
  }
  if(path_parent_base(clean, parent, sizeof(parent), base, sizeof(base)) != 0) {
    set_err(err, err_size, "bad image path");
    return -1;
  }

  size_t base_len = strlen(base);
  size_t ext_len = strlen(".ffpfsc");
  if(base_len <= ext_len || base_len - ext_len >= sizeof(stem)) {
    set_err(err, err_size, "bad image name");
    errno = EINVAL;
    return -1;
  }
  memcpy(stem, base, base_len - ext_len);
  stem[base_len - ext_len] = 0;

  snprintf(info->source_path, sizeof(info->source_path), "%s", clean);
  int n = snprintf(info->output_path, sizeof(info->output_path),
                   "%s%s%s-app", parent, parent[1] ? "/" : "", stem);
  if(n < 0 || (size_t)n >= sizeof(info->output_path)) {
    set_err(err, err_size, "output path too long");
    return -1;
  }
  info->output_exists = stat(info->output_path, &st) == 0;
  return 0;
}

int
pfs_decompress_detect_nested(const char *path, pfs_decompress_info_t *info,
                             char *err, size_t err_size) {
  pfs_decompress_info_t local_info;
  pfsc_reader_t reader;
  int fd = -1;
  int rc = -1;
  memset(&reader, 0, sizeof(reader));
  reader.fd = -1;
  if(!info) info = &local_info;
  if(pfs_decompress_probe(path, info, err, err_size) != 0) return -1;
  fd = open(info->source_path, O_RDONLY);
  if(fd < 0) {
    set_err(err, err_size, "open input: %s", strerror(errno));
    goto done;
  }
  if(pfsc_reader_open_from_outer(fd, &reader, err, err_size) != 0) {
    if(reader.fd >= 0) pfsc_reader_close(&reader);
    else close(fd);
    fd = -1;
    goto done;
  }
  fd = -1;
  info->nested_type = pfsc_reader_detect_nested_type(&reader, err, err_size);
  snprintf(info->nested_name, sizeof(info->nested_name), "%s",
           reader.nested_name);
  info->nested_size = reader.nested_size;
  if(info->nested_type == PFS_NESTED_UNKNOWN) goto done;
  rc = 0;

done:
  if(reader.fd >= 0) pfsc_reader_close(&reader);
  if(fd >= 0) close(fd);
  return rc;
}

static int
pfs_decompress_set_image_output_path(pfs_decompress_info_t *info,
                                     char *err, size_t err_size) {
  char parent[1024];
  char base[256];
  char stem[256];
  struct stat st;
  if(!info || !info->source_path[0]) {
    set_err(err, err_size, "bad image path");
    errno = EINVAL;
    return -1;
  }
  if(info->nested_type != PFS_NESTED_PFS &&
     info->nested_type != PFS_NESTED_EXFAT) {
    set_err(err, err_size, "unsupported nested image type");
    errno = EINVAL;
    return -1;
  }
  if(path_parent_base(info->source_path, parent, sizeof(parent),
                      base, sizeof(base)) != 0) {
    set_err(err, err_size, "bad image path");
    return -1;
  }
  size_t base_len = strlen(base);
  size_t ext_len = strlen(".ffpfsc");
  if(base_len <= ext_len || base_len - ext_len >= sizeof(stem)) {
    set_err(err, err_size, "bad image name");
    errno = EINVAL;
    return -1;
  }
  memcpy(stem, base, base_len - ext_len);
  stem[base_len - ext_len] = 0;
  const char *ext = info->nested_type == PFS_NESTED_EXFAT ? ".exfat" : ".pfs";
  int n = snprintf(info->output_path, sizeof(info->output_path), "%s%s%s%s",
                   parent, parent[1] ? "/" : "", stem, ext);
  if(n < 0 || (size_t)n >= sizeof(info->output_path)) {
    set_err(err, err_size, "output path too long");
    return -1;
  }
  info->output_exists = stat(info->output_path, &st) == 0;
  return 0;
}

static int
pfs_decompress_apply_output_path(pfs_decompress_info_t *info,
                                 const char *output_path,
                                 char *err, size_t err_size) {
  char clean[1024];
  struct stat st;
  if(!output_path || !output_path[0]) return 0;
  if(normalize_app_path(output_path, clean, sizeof(clean)) != 0) {
    set_err(err, err_size, "bad output path");
    return -1;
  }
  snprintf(info->output_path, sizeof(info->output_path), "%s", clean);
  info->output_exists = stat(info->output_path, &st) == 0;
  return 0;
}

int
pfs_decompress_probe_image(const char *path, pfs_decompress_info_t *info,
                           char *err, size_t err_size) {
  pfs_decompress_info_t local_info;
  if(!info) info = &local_info;
  if(pfs_decompress_detect_nested(path, info, err, err_size) != 0) {
    return -1;
  }
  return pfs_decompress_set_image_output_path(info, err, err_size);
}

static unsigned char *
pfs_alloc_image_buffer(size_t *size_out) {
  size_t size = PFS_IMAGE_OUTPUT_BUFFER_SIZE;
  while(size >= (size_t)PFS_BLOCK_SIZE) {
    unsigned char *buf = malloc(size);
    if(buf) {
      if(size_out) *size_out = size;
      return buf;
    }
    size /= 2;
  }
  if(size_out) *size_out = 0;
  return NULL;
}

static int
pfsc_reader_write_image_to_fd(pfsc_reader_t *r, int out,
                              char *err, size_t err_size) {
  size_t buffer_size = 0;
  unsigned char *buffer = pfs_alloc_image_buffer(&buffer_size);
  size_t used = 0;
  uint64_t copied = 0;
  uint64_t index = 0;
  int rc = -1;

  if(!buffer) {
    set_err(err, err_size, "out of memory");
    errno = ENOMEM;
    return -1;
  }

  while(copied < r->nested_size) {
    if(job_cancelled()) {
      set_err(err, err_size, "cancelled");
      errno = EINTR;
      goto done;
    }
    size_t useful = pfsc_reader_block_useful_size(r, index);
    if(useful == 0) break;
    if(used + (size_t)r->block_size > buffer_size) {
      if(write_all_fd_cancelable(out, buffer, used) != 0) {
        set_err(err, err_size, "write output image: %s", strerror(errno));
        goto done;
      }
      used = 0;
    }
    if(pfsc_reader_decode_block_direct(r, index, buffer + used,
                                       err, err_size) != 0) {
      goto done;
    }
    used += useful;
    copied += useful;
    index++;
    atomic_fetch_add(&g_job.copied_bytes,
                     useful > (size_t)LONG_MAX ? LONG_MAX : (long)useful);
  }

  if(used > 0 && write_all_fd_cancelable(out, buffer, used) != 0) {
    set_err(err, err_size, "write output image: %s", strerror(errno));
    goto done;
  }
  rc = 0;

done:
  free(buffer);
  return rc;
}

int
pfs_decompress_ffpfsc_to_image_opts_output(const char *path, int overwrite,
                                    int workers, int delete_policy,
                                    const char *output_path,
                                    pfs_decompress_info_t *info,
                                    char *err, size_t err_size) {
  pfs_decompress_info_t local_info;
  pfsc_reader_t reader;
  char tmp_path[1024];
  char legacy_tmp_path[1024];
  int fd = -1;
  int out = -1;
  int rc = -1;
  pfs_io_policy_info_t io_policy;
  int allow_io_overlap = 0;

  memset(&reader, 0, sizeof(reader));
  reader.fd = -1;
  reader.mounted_fd = -1;
  if(!info) info = &local_info;
  if(delete_policy != PFS_DELETE_KEEP && delete_policy != PFS_DELETE_AFTER) {
    set_err(err, err_size,
            "image output is not available for stream delete");
    errno = EINVAL;
    return -1;
  }
  if(pfs_decompress_probe_image(path, info, err, err_size) != 0) return -1;
  if(pfs_decompress_apply_output_path(info, output_path, err, err_size) != 0) {
    return -1;
  }
  info->delete_policy = delete_policy;
  if(info->output_exists && !overwrite) {
    set_err(err, err_size, "output exists");
    errno = EEXIST;
    return -2;
  }
  if(hidden_tmp_path_for_output(info->output_path, "gc-unpack",
                                tmp_path, sizeof(tmp_path)) != 0) {
    set_err(err, err_size, "temporary output path too long");
    return -1;
  }
  if(snprintf(legacy_tmp_path, sizeof(legacy_tmp_path), "%s.tmp",
              info->output_path) >= (int)sizeof(legacy_tmp_path)) {
    set_err(err, err_size, "legacy temporary output path too long");
    return -1;
  }

  job_set_target(info->output_path);
  job_set_current("Opening compressed image");
  if(remove_tree_local(tmp_path) != 0) {
    set_err(err, err_size, "remove old temp output: %s", strerror(errno));
    goto done;
  }
  if(remove_tree_local(legacy_tmp_path) != 0) {
    set_err(err, err_size, "remove old legacy temp output: %s",
            strerror(errno));
    goto done;
  }

  fd = open(info->source_path, O_RDONLY);
  if(fd < 0) {
    set_err(err, err_size, "open input: %s", strerror(errno));
    goto done;
  }
  if(pfsc_reader_open_from_outer(fd, &reader, err, err_size) != 0) {
    close(fd);
    fd = -1;
    goto done;
  }
  fd = -1;
  snprintf(info->nested_name, sizeof(info->nested_name), "%s",
           reader.nested_name);
  info->nested_size = reader.nested_size;
  if(pfs_decompress_set_image_output_path(info, err, err_size) != 0) {
    goto done;
  }
  if(pfs_decompress_apply_output_path(info, output_path, err, err_size) != 0) {
    goto done;
  }
  pfsc_reader_try_open_mounted(&reader, info->source_path);
  pfs_io_policy_for_paths(info->source_path, info->output_path, &io_policy);
  allow_io_overlap = pfs_io_policy_allows_overlap(&io_policy);
  PFS_DECOMPRESS_LOG("pfsc io policy action=uncompress mode=image ioPolicy=%s sourceDevice=\"%s\" destDevice=\"%s\" reason=%s source=\"%s\" dest=\"%s\"",
                     pfs_io_policy_name(io_policy.policy),
                     io_policy.source_key, io_policy.dest_key,
                     io_policy.reason, info->source_path, info->output_path);

  atomic_store(&g_job.total_bytes, job_long_from_u64(reader.nested_size));
  atomic_store(&g_job.copied_bytes, 0);
  atomic_store(&g_job.compressed_output_bytes, 0);
  atomic_store(&g_job.raw_blocks, 0);
  atomic_store(&g_job.compressed_blocks, 0);
  atomic_store(&g_job.skipped_zlib_blocks, 0);
  atomic_store(&g_job.hash_checked_blocks, 0);
  atomic_store(&g_job.hash_matched_blocks, 0);
  atomic_store(&g_job.hash_mismatched_blocks, 0);
  atomic_store(&g_job.software_compared_blocks, 0);
  atomic_store(&g_job.total_blocks,
               reader.block_count > (uint64_t)LONG_MAX ? LONG_MAX :
               (long)reader.block_count);
  atomic_store(&g_job.writer_wait_us, 0);
  atomic_store(&g_job.worker_wait_us, 0);
  atomic_store(&g_job.total_files, 1);
  atomic_store(&g_job.done_files, 0);
  atomic_store(&g_job.failed_files, 0);

  job_set_current(info->nested_type == PFS_NESTED_EXFAT
      ? "Decompressing exFAT image"
      : "Decompressing PFS image");
  out = open(tmp_path, O_WRONLY | O_CREAT | O_TRUNC, 0777);
  if(out < 0) {
    set_err(err, err_size, "create output image: %s", strerror(errno));
    goto done;
  }
  int image_rc = pfsc_decomp_write_image_windowed(&reader, out, workers,
                                                  allow_io_overlap,
                                                  err, err_size);
  if(image_rc == PFSC_DECOMPRESS_WINDOW_RC_FALLBACK) {
    if(err && err_size > 0) err[0] = 0;
    PFS_DECOMPRESS_LOG("decompress window unavailable mode=image fallback=serial");
    if(pfsc_reader_write_image_to_fd(&reader, out, err, err_size) != 0) {
      goto done;
    }
  } else if(image_rc != 0) {
    goto done;
  }
  if(fsync(out) != 0) {
    set_err(err, err_size, "sync output image: %s", strerror(errno));
    goto done;
  }
  if(close(out) != 0) {
    out = -1;
    set_err(err, err_size, "close output image: %s", strerror(errno));
    goto done;
  }
  out = -1;

  if(job_cancelled()) {
    set_err(err, err_size, "cancelled");
    errno = EINTR;
    goto done;
  }

  job_set_current("Finalizing image");
  if(info->output_exists || overwrite) {
    if(remove_tree_local(info->output_path) != 0) {
      set_err(err, err_size, "replace output image: %s", strerror(errno));
      goto done;
    }
  }
  if(rename(tmp_path, info->output_path) != 0) {
    set_err(err, err_size, "rename output image: %s", strerror(errno));
    goto done;
  }
  chmod(info->output_path, 0777);
  info->output_exists = 1;
  atomic_store(&g_job.done_files, 1);

  if(delete_policy == PFS_DELETE_AFTER) {
    job_set_current("Removing compressed image");
    if(unlink(info->source_path) != 0 && errno != ENOENT) {
      set_err(err, err_size, "remove compressed image: %s", strerror(errno));
      goto done;
    }
  }
  rc = 0;

done:
  if(out >= 0) close(out);
  if(reader.fd >= 0) pfsc_reader_close(&reader);
  if(fd >= 0) close(fd);
  if(rc != 0) {
    remove_tree_local(tmp_path);
    remove_tree_local(legacy_tmp_path);
    if(err && err_size > 0 && !err[0]) {
      set_err(err, err_size, "%s", strerror(errno));
    }
  }
  return rc;
}

int
pfs_decompress_ffpfsc_to_app_opts_output(const char *path, int overwrite,
                                  int workers, int delete_policy,
                                  const char *output_path,
                                  pfs_decompress_info_t *info,
                                  char *err, size_t err_size) {
  pfs_decompress_info_t local_info;
  pfs_nested_reader_t nr;
  exfat_reader_t er;
  pfs_extract_plan_t plan = {0};
  char tmp_path[1024];
  char legacy_tmp_path[1024];
  uint64_t file_count = 0;
  uint64_t data_bytes = 0;
  uint64_t data_blocks = 0;
  int opened = 0;
  int exfat_opened = 0;
  int rc = -1;
  int nested_type = PFS_NESTED_UNKNOWN;
  pfs_io_policy_info_t io_policy;
  int allow_io_overlap = 0;

  memset(&nr, 0, sizeof(nr));
  nr.pfsc.fd = -1;
  memset(&er, 0, sizeof(er));
  er.pfsc.fd = -1;
  if(!info) info = &local_info;
  if(delete_policy != PFS_DELETE_KEEP &&
     delete_policy != PFS_DELETE_AFTER) {
    set_err(err, err_size, "unsupported delete policy");
    errno = EINVAL;
    return -1;
  }
  if(pfs_decompress_probe(path, info, err, err_size) != 0) return -1;
  if(pfs_decompress_apply_output_path(info, output_path, err, err_size) != 0) {
    return -1;
  }
  info->delete_policy = delete_policy;
  if(info->output_exists && !overwrite) {
    set_err(err, err_size, "output exists");
    errno = EEXIST;
    return -2;
  }

  if(workers <= 0) workers = PFS_DECOMPRESS_DEFAULT_WORKERS;
  if(workers > PFS_COMPRESS_MAX_WORKERS) workers = PFS_COMPRESS_MAX_WORKERS;

  if(hidden_tmp_path_for_output(info->output_path, "gc-unpack",
                                tmp_path, sizeof(tmp_path)) != 0) {
    set_err(err, err_size, "temporary output path too long");
    return -1;
  }
  if(snprintf(legacy_tmp_path, sizeof(legacy_tmp_path), "%s.tmp",
              info->output_path) >= (int)sizeof(legacy_tmp_path)) {
    set_err(err, err_size, "legacy temporary output path too long");
    return -1;
  }

  job_set_target(info->output_path);
  job_set_current("Opening compressed image");
  if(remove_tree_local(tmp_path) != 0) {
    set_err(err, err_size, "remove old temp output: %s", strerror(errno));
    goto done;
  }
  if(remove_tree_local(legacy_tmp_path) != 0) {
    set_err(err, err_size, "remove old legacy temp output: %s",
            strerror(errno));
    goto done;
  }

  if(pfs_decompress_detect_nested(info->source_path, info,
                                  err, err_size) != 0) {
    goto done;
  }
  if(pfs_decompress_apply_output_path(info, output_path, err, err_size) != 0) {
    goto done;
  }
  nested_type = info->nested_type;

  if(nested_type == PFS_NESTED_PFS) {
    if(pfs_nested_open(info->source_path, &nr, 0,
                       err, err_size) != 0) {
      goto done;
    }
    opened = 1;
    snprintf(info->nested_name, sizeof(info->nested_name), "%s",
             nr.pfsc.nested_name);
    if(pfs_find_child_inode(&nr, 0, "uroot", PFS_DIRENT_TYPE_DIRECTORY,
                            &nr.uroot_inode, err, err_size) != 0) {
      goto done;
    }

    job_set_current("Scanning nested PFS");
    if(pfs_collect_extract_plan(&nr, nr.uroot_inode, tmp_path, "", &plan,
                                err, err_size) != 0) {
      goto done;
    }
    file_count = plan.count;
    data_bytes = plan.total_bytes;
    data_blocks = plan.total_blocks;
  } else if(nested_type == PFS_NESTED_EXFAT) {
    if(exfat_reader_open(info->source_path, &er, 0,
                         err, err_size) != 0) {
      goto done;
    }
    exfat_opened = 1;
    snprintf(info->nested_name, sizeof(info->nested_name), "%s",
             er.pfsc.nested_name);
    job_set_current("Scanning nested exFAT");
    if(exfat_collect_dir(&er, er.root_cluster, 0, tmp_path, "",
                         &plan, err, err_size) != 0) {
      goto done;
    }
    file_count = plan.count;
    data_bytes = plan.total_bytes;
    data_blocks = plan.total_blocks;
  } else {
    set_err(err, err_size, "unsupported nested image type");
    errno = EINVAL;
    goto done;
  }
  pfs_io_policy_for_paths(info->source_path, info->output_path, &io_policy);
  allow_io_overlap = pfs_io_policy_allows_overlap(&io_policy);
  PFS_DECOMPRESS_LOG("pfsc io policy action=uncompress mode=app ioPolicy=%s sourceDevice=\"%s\" destDevice=\"%s\" reason=%s source=\"%s\" dest=\"%s\"",
                     pfs_io_policy_name(io_policy.policy),
                     io_policy.source_key, io_policy.dest_key,
                     io_policy.reason, info->source_path, info->output_path);

  atomic_store(&g_job.total_bytes, job_long_from_u64(data_bytes));
  atomic_store(&g_job.copied_bytes, 0);
  atomic_store(&g_job.compressed_output_bytes, 0);
  atomic_store(&g_job.raw_blocks, 0);
  atomic_store(&g_job.compressed_blocks, 0);
  atomic_store(&g_job.skipped_zlib_blocks, 0);
  atomic_store(&g_job.hash_checked_blocks, 0);
  atomic_store(&g_job.hash_matched_blocks, 0);
  atomic_store(&g_job.hash_mismatched_blocks, 0);
  atomic_store(&g_job.software_compared_blocks, 0);
  atomic_store(&g_job.total_blocks,
               data_blocks > (uint64_t)LONG_MAX ? LONG_MAX :
               (long)data_blocks);
  atomic_store(&g_job.writer_wait_us, 0);
  atomic_store(&g_job.worker_wait_us, 0);
  atomic_store(&g_job.total_files,
               file_count > (uint64_t)INT_MAX ? INT_MAX : (int)file_count);
  atomic_store(&g_job.done_files, 0);
  atomic_store(&g_job.failed_files, 0);

  if(job_cancelled()) {
    set_err(err, err_size, "cancelled");
    errno = EINTR;
    goto done;
  }
  job_set_current("Decompressing app folder");
  pfsc_reader_t *reader = nested_type == PFS_NESTED_PFS ? &nr.pfsc : &er.pfsc;
  int plan_rc = pfsc_decomp_extract_plan_windowed(reader, &plan, workers, 1,
                                                  allow_io_overlap,
                                                  err, err_size);
  if(plan_rc == PFSC_DECOMPRESS_WINDOW_RC_FALLBACK) {
    if(err && err_size > 0) err[0] = 0;
    PFS_DECOMPRESS_LOG("decompress window unavailable mode=app fallback=serial");
    if(nested_type == PFS_NESTED_PFS) {
      if(pfs_extract_dir(&nr, nr.uroot_inode, tmp_path, "", 1,
                         1, err, err_size) != 0) {
        goto done;
      }
    } else if(nested_type == PFS_NESTED_EXFAT) {
      if(exfat_extract_plan(&er, &plan, err, err_size) != 0) {
        goto done;
      }
    }
  } else if(plan_rc != 0) {
    goto done;
  }
  if(job_cancelled()) {
    set_err(err, err_size, "cancelled");
    errno = EINTR;
    goto done;
  }

  job_set_current("Finalizing app folder");
  if(info->output_exists || overwrite) {
    if(remove_tree_local(info->output_path) != 0) {
      set_err(err, err_size, "replace output: %s", strerror(errno));
      goto done;
    }
  }
  if(rename(tmp_path, info->output_path) != 0) {
    set_err(err, err_size, "rename output: %s", strerror(errno));
    goto done;
  }
  chmod(info->output_path, 0777);
  info->output_exists = 1;

  if(opened) {
    pfs_nested_close(&nr);
    opened = 0;
  }
  if(exfat_opened) {
    exfat_reader_close(&er);
    exfat_opened = 0;
  }
  if(delete_policy != PFS_DELETE_KEEP) {
    job_set_current("Removing compressed image");
    if(unlink(info->source_path) != 0 && errno != ENOENT) {
      set_err(err, err_size, "remove compressed image: %s", strerror(errno));
      goto done;
    }
  }
  rc = 0;

done:
  if(opened) pfs_nested_close(&nr);
  if(exfat_opened) exfat_reader_close(&er);
  if(rc != 0) {
    remove_tree_local(tmp_path);
    remove_tree_local(legacy_tmp_path);
    if(err && err_size > 0 && !err[0]) {
      set_err(err, err_size, "%s", strerror(errno));
    }
  }
  pfs_extract_plan_free(&plan);
  return rc;
}
