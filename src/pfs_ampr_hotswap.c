/*
 * Game Compressor - APR-EMU hot-swap helpers for exFAT/PFSC images.
 */

#include "pfs_ampr_hotswap.h"

#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <stdarg.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <sys/stat.h>
#include <sys/statvfs.h>
#include <sys/types.h>
#include <unistd.h>
#include <zlib.h>

#include "ampr_index.h"
#include "miniz_tinfl.h"
#include "pfs_compress.h"
#include "pfs_fs_util.h"
#include "transfer_internal.h"

#ifndef GC_PFSC_ZLIB_LEVEL
#define GC_PFSC_ZLIB_LEVEL 7
#endif

#ifndef GC_PFSC_THRESHOLD_GAIN
#define GC_PFSC_THRESHOLD_GAIN 5
#endif

#define HS_BLOCK_SIZE 65536ULL
#define HS_EXFAT_SECTOR_SIZE 512ULL
#define HS_PFS_INODE_SIZE 0xA8ULL
#define HS_PFS_VERSION_PS5 2ULL
#define HS_PFS_MAGIC 20130315ULL
#define HS_PFS_INODE_MODE_DIR 0x4000U
#define HS_PFS_INODE_FLAG_COMPRESSED 0x1U
#define HS_PFS_DIRENT_TYPE_FILE 2U
#define HS_PFSC_MAGIC 0x43534650U
#define HS_PFSC_UNK4 0U
#define HS_PFSC_UNK8 6U
#define HS_PFSC_HEADER_SIZE 0x30U
#define HS_PFSC_BLOCK_OFFSETS_OFFSET 0x400ULL
#define HS_PFSC_INITIAL_DATA_OFFSET 0x10000ULL
#define HS_PFSC_OFFSET_ENTRY_SIZE 8ULL
#define HS_IO_CHUNK (1024U * 1024U)
#define HS_AMPR_SLACK_CLUSTERS 16U
#define HS_MAX_AMPR_CLUSTERS 4096U
#define HS_EXFAT_NAME_SKIP 1

typedef int (*hs_read_fn)(void *ctx, uint64_t off, void *out, size_t size,
                          char *err, size_t err_size);

typedef struct hs_patch {
  uint64_t off;
  size_t size;
  unsigned char *data;
} hs_patch_t;

typedef struct hs_patch_list {
  hs_patch_t *items;
  size_t count;
  size_t cap;
} hs_patch_list_t;

typedef struct hs_exfat {
  hs_read_fn read_fn;
  void *read_ctx;
  uint64_t sector_size;
  uint64_t cluster_size;
  uint64_t volume_length;
  uint32_t fat_offset;
  uint32_t fat_length;
  uint32_t cluster_heap_offset;
  uint32_t cluster_count;
  uint32_t root_cluster;
  uint32_t bitmap_cluster;
  uint64_t bitmap_size;
  uint64_t bitmap_entry_off;
  int has_bitmap;
} hs_exfat_t;

typedef struct hs_exfat_file {
  char rel[128];
  uint64_t entry_set_off;
  size_t entry_count;
  unsigned char entry_set[32 * 19];
  uint8_t stream_flags;
  uint16_t attrs;
  uint32_t first_cluster;
  uint32_t allocated_clusters;
  uint64_t data_len;
} hs_exfat_file_t;

typedef struct hs_index_file {
  char rel[1024];
  uint64_t size;
  int64_t mtime;
} hs_index_file_t;

typedef struct hs_index_list {
  hs_index_file_t *items;
  size_t count;
  size_t cap;
} hs_index_list_t;

typedef struct hs_root_index_slot {
  int found_existing;
  int needs_root_extend;
  uint64_t insert_off;
  uint64_t insert_stream_off;
  uint64_t old_dir_size;
  uint32_t root_last_cluster;
  hs_exfat_file_t existing;
} hs_root_index_slot_t;

typedef struct hs_pfsc_image {
  int fd;
  uint64_t outer_size;
  uint64_t file_start;
  uint64_t stored_size;
  uint64_t logical_size;
  uint64_t nested_size;
  uint64_t header_size;
  uint64_t block_count;
  char nested_name[256];
  uint64_t *offsets;
} hs_pfsc_image_t;

typedef struct hs_changed_block {
  uint64_t index;
  unsigned char *raw;
  unsigned char *stored;
  size_t stored_len;
} hs_changed_block_t;

typedef struct hs_changed_blocks {
  hs_changed_block_t *items;
  size_t count;
  size_t cap;
} hs_changed_blocks_t;

typedef enum hs_pfsc_apply_strategy {
  HS_PFSC_APPLY_OVERWRITE = 0,
  HS_PFSC_APPLY_MOVE_LEFT,
  HS_PFSC_APPLY_MOVE_RIGHT,
  HS_PFSC_APPLY_MIXED,
} hs_pfsc_apply_strategy_t;

static void
hs_set_err(char *err, size_t err_size, const char *fmt, ...) {
  if(!err || err_size == 0) return;
  va_list ap;
  va_start(ap, fmt);
  vsnprintf(err, err_size, fmt, ap);
  va_end(ap);
}

static int
hs_exfat_require_contiguous_allocation(const hs_exfat_file_t *file,
                                       const char *label,
                                       char *err, size_t err_size) {
  if(!file) {
    hs_set_err(err, err_size, "bad exFAT allocation check");
    errno = EINVAL;
    return -1;
  }
  if(file->first_cluster && file->allocated_clusters &&
     (file->stream_flags & 0x02U) == 0) {
    hs_set_err(err, err_size,
               "%s uses a fragmented exFAT allocation; recompress with AMPR hot-swap layout",
               label ? label : "target file");
    errno = EINVAL;
    return -1;
  }
  return 0;
}

static uint16_t
hs_rd16(const unsigned char *p) {
  return (uint16_t)p[0] | ((uint16_t)p[1] << 8);
}

static uint32_t
hs_rd32(const unsigned char *p) {
  return (uint32_t)p[0] |
         ((uint32_t)p[1] << 8) |
         ((uint32_t)p[2] << 16) |
         ((uint32_t)p[3] << 24);
}

static uint64_t
hs_rd64(const unsigned char *p) {
  uint64_t v = 0;
  for(int i = 7; i >= 0; i--) v = (v << 8) | p[i];
  return v;
}

static void
hs_le16(unsigned char *p, uint16_t v) {
  p[0] = (unsigned char)(v & 0xff);
  p[1] = (unsigned char)((v >> 8) & 0xff);
}

static void
hs_le32(unsigned char *p, uint32_t v) {
  p[0] = (unsigned char)(v & 0xff);
  p[1] = (unsigned char)((v >> 8) & 0xff);
  p[2] = (unsigned char)((v >> 16) & 0xff);
  p[3] = (unsigned char)((v >> 24) & 0xff);
}

static void
hs_le64(unsigned char *p, uint64_t v) {
  for(int i = 0; i < 8; i++) {
    p[i] = (unsigned char)(v & 0xff);
    v >>= 8;
  }
}

static uint64_t
hs_ceil_div_u64(uint64_t a, uint64_t b) {
  return b ? (a + b - 1ULL) / b : 0;
}

static long
hs_job_long_from_u64(uint64_t v) {
  return v > (uint64_t)LONG_MAX ? LONG_MAX : (long)v;
}

static int
hs_read_exact_at(int fd, void *data, size_t size, uint64_t offset,
                 char *err, size_t err_size) {
  unsigned char *p = data;
  while(size > 0) {
    if(job_cancelled()) {
      hs_set_err(err, err_size, "cancelled");
      errno = EINTR;
      return -1;
    }
    size_t chunk = size > HS_IO_CHUNK ? HS_IO_CHUNK : size;
    ssize_t n = pread(fd, p, chunk, (off_t)offset);
    if(n < 0) {
      if(errno == EINTR) continue;
      hs_set_err(err, err_size, "read 0x%llx: %s",
                 (unsigned long long)offset, strerror(errno));
      return -1;
    }
    if(n == 0) {
      hs_set_err(err, err_size, "read 0x%llx: unexpected EOF",
                 (unsigned long long)offset);
      errno = EIO;
      return -1;
    }
    p += n;
    size -= (size_t)n;
    offset += (uint64_t)n;
  }
  return 0;
}

static int
hs_write_exact_at(int fd, const void *data, size_t size, uint64_t offset,
                  char *err, size_t err_size) {
  const unsigned char *p = data;
  while(size > 0) {
    if(job_cancelled()) {
      hs_set_err(err, err_size, "cancelled");
      errno = EINTR;
      return -1;
    }
    size_t chunk = size > HS_IO_CHUNK ? HS_IO_CHUNK : size;
    ssize_t n = pwrite(fd, p, chunk, (off_t)offset);
    if(n < 0) {
      if(errno == EINTR) continue;
      hs_set_err(err, err_size, "write 0x%llx: %s",
                 (unsigned long long)offset, strerror(errno));
      return -1;
    }
    if(n == 0) {
      hs_set_err(err, err_size, "write 0x%llx: no progress",
                 (unsigned long long)offset);
      errno = EIO;
      return -1;
    }
    p += n;
    size -= (size_t)n;
    offset += (uint64_t)n;
  }
  return 0;
}

static int
hs_copy_fd_range(int src_fd, int dst_fd, uint64_t size,
                 uint64_t src_off, uint64_t dst_off,
                 char *err, size_t err_size) {
  unsigned char *buf = malloc(HS_IO_CHUNK);
  uint64_t done = 0;
  if(!buf) {
    hs_set_err(err, err_size, "out of memory");
    errno = ENOMEM;
    return -1;
  }
  while(done < size) {
    size_t chunk = size - done > HS_IO_CHUNK ? HS_IO_CHUNK : (size_t)(size - done);
    if(hs_read_exact_at(src_fd, buf, chunk, src_off + done,
                        err, err_size) != 0 ||
       hs_write_exact_at(dst_fd, buf, chunk, dst_off + done,
                         err, err_size) != 0) {
      free(buf);
      return -1;
    }
    done += chunk;
  }
  free(buf);
  return 0;
}

typedef struct hs_rebuild_progress {
  uint64_t done_blocks;
  uint64_t total_blocks;
  uint64_t last_reported_blocks;
} hs_rebuild_progress_t;

static void
hs_rebuild_report_progress(hs_rebuild_progress_t *progress,
                           uint64_t done_blocks,
                           const char *current) {
  if(!progress || progress->total_blocks == 0) return;
  if(done_blocks > progress->total_blocks) done_blocks = progress->total_blocks;
  progress->done_blocks = done_blocks;
  atomic_store(&g_job.total_blocks,
               hs_job_long_from_u64(progress->total_blocks));
  atomic_store(&g_job.copied_bytes,
               hs_job_long_from_u64(done_blocks > UINT64_MAX / HS_BLOCK_SIZE
                                    ? UINT64_MAX
                                    : done_blocks * HS_BLOCK_SIZE));
  job_set_phase("rebuilding",
                hs_job_long_from_u64(done_blocks),
                hs_job_long_from_u64(progress->total_blocks),
                current ? current : "Rebuilding compressed image");
  progress->last_reported_blocks = done_blocks;
}

static void
hs_rebuild_advance_progress(hs_rebuild_progress_t *progress,
                            uint64_t blocks,
                            const char *current) {
  if(!progress || blocks == 0) return;
  uint64_t done = progress->done_blocks;
  if(blocks > UINT64_MAX - done) {
    done = progress->total_blocks;
  } else {
    done += blocks;
  }
  hs_rebuild_report_progress(progress, done, current);
}

static int
hs_copy_fd_range_rebuild_progress(int src_fd, int dst_fd, uint64_t size,
                                  uint64_t src_off, uint64_t dst_off,
                                  uint64_t base_blocks,
                                  uint64_t run_blocks,
                                  hs_rebuild_progress_t *progress,
                                  const char *current,
                                  char *err, size_t err_size) {
  unsigned char *buf = malloc(HS_IO_CHUNK);
  uint64_t done = 0;
  if(!buf) {
    hs_set_err(err, err_size, "out of memory");
    errno = ENOMEM;
    return -1;
  }
  while(done < size) {
    size_t chunk = size - done > HS_IO_CHUNK ? HS_IO_CHUNK : (size_t)(size - done);
    if(hs_read_exact_at(src_fd, buf, chunk, src_off + done,
                        err, err_size) != 0 ||
       hs_write_exact_at(dst_fd, buf, chunk, dst_off + done,
                         err, err_size) != 0) {
      free(buf);
      return -1;
    }
    done += chunk;
    if(progress && size > 0 && run_blocks > 0) {
      uint64_t run_done = done >= size ? run_blocks :
          (uint64_t)(((unsigned __int128)done * run_blocks) / size);
      hs_rebuild_report_progress(progress, base_blocks + run_done, current);
    }
  }
  free(buf);
  return 0;
}

static int
hs_file_to_memory(const char *path, unsigned char **out, size_t *out_size,
                  char *err, size_t err_size) {
  int fd = -1;
  struct stat st;
  unsigned char *buf = NULL;
  if(!path || !out || !out_size) {
    errno = EINVAL;
    return -1;
  }
  *out = NULL;
  *out_size = 0;
  fd = open(path, O_RDONLY);
  if(fd < 0) {
    hs_set_err(err, err_size, "open replacement: %s", strerror(errno));
    return -1;
  }
  if(fstat(fd, &st) != 0 || st.st_size <= 0 ||
     (uint64_t)st.st_size > 128ULL * 1024ULL * 1024ULL) {
    hs_set_err(err, err_size, "bad replacement size");
    close(fd);
    errno = EINVAL;
    return -1;
  }
  buf = malloc((size_t)st.st_size);
  if(!buf) {
    hs_set_err(err, err_size, "out of memory");
    close(fd);
    errno = ENOMEM;
    return -1;
  }
  if(hs_read_exact_at(fd, buf, (size_t)st.st_size, 0, err, err_size) != 0) {
    free(buf);
    close(fd);
    return -1;
  }
  close(fd);
  *out = buf;
  *out_size = (size_t)st.st_size;
  return 0;
}

static void
hs_patch_list_free(hs_patch_list_t *patches) {
  if(!patches) return;
  for(size_t i = 0; i < patches->count; i++) free(patches->items[i].data);
  free(patches->items);
  memset(patches, 0, sizeof(*patches));
}

static int
hs_patch_append(hs_patch_list_t *patches, uint64_t off,
                const void *data, size_t size,
                char *err, size_t err_size) {
  if(size == 0) return 0;
  if(!patches || !data) {
    errno = EINVAL;
    return -1;
  }
  if(patches->count == patches->cap) {
    size_t next = patches->cap ? patches->cap * 2 : 8;
    hs_patch_t *p = realloc(patches->items, next * sizeof(*p));
    if(!p) {
      hs_set_err(err, err_size, "out of memory");
      errno = ENOMEM;
      return -1;
    }
    patches->items = p;
    patches->cap = next;
  }
  unsigned char *copy = malloc(size);
  if(!copy) {
    hs_set_err(err, err_size, "out of memory");
    errno = ENOMEM;
    return -1;
  }
  memcpy(copy, data, size);
  patches->items[patches->count].off = off;
  patches->items[patches->count].size = size;
  patches->items[patches->count].data = copy;
  patches->count++;
  return 0;
}

static int
hs_fd_read_cb(void *ctx, uint64_t off, void *out, size_t size,
              char *err, size_t err_size) {
  int fd = *(int *)ctx;
  return hs_read_exact_at(fd, out, size, off, err, err_size);
}

static uint16_t
hs_exfat_rotate16(uint16_t value) {
  return (uint16_t)(((value & 1U) ? 0x8000U : 0U) + (value >> 1));
}

static uint32_t
hs_exfat_rotate32(uint32_t value) {
  return ((value & 1U) ? 0x80000000U : 0U) + (value >> 1);
}

static uint16_t
hs_exfat_entry_set_checksum(const unsigned char *entries, size_t entry_count) {
  uint16_t sum = 0;
  size_t size = entry_count * 32U;
  for(size_t i = 0; i < size; i++) {
    if(i == 2 || i == 3) continue;
    sum = (uint16_t)(hs_exfat_rotate16(sum) + entries[i]);
  }
  return sum;
}

static uint32_t
hs_exfat_boot_checksum(const unsigned char *boot, size_t sector_size) {
  uint32_t sum = 0;
  size_t limit = 11U * sector_size;
  for(size_t i = 0; i < limit; i++) {
    if(i == 106 || i == 107 || i == 112) continue;
    sum = hs_exfat_rotate32(sum) + boot[i];
  }
  return sum;
}

static uint16_t
hs_exfat_upcase_char(uint16_t ch) {
  if(ch >= 'a' && ch <= 'z') return (uint16_t)(ch - 32);
  return ch;
}

static uint16_t
hs_exfat_name_hash(const char *name) {
  uint16_t hash = 0;
  for(const unsigned char *p = (const unsigned char *)(name ? name : "");
      *p; p++) {
    uint16_t ch = hs_exfat_upcase_char(*p);
    hash = (uint16_t)(hs_exfat_rotate16(hash) + (ch & 0xff));
    hash = (uint16_t)(hs_exfat_rotate16(hash) + ((ch >> 8) & 0xff));
  }
  return hash;
}

static size_t
hs_exfat_name_entry_count(const char *name) {
  size_t len = strlen(name ? name : "");
  return len ? hs_ceil_div_u64(len, 15) : 1;
}

static int
hs_exfat_make_file_entry_set(const char *name, uint32_t first_cluster,
                             uint64_t data_len, unsigned char *out,
                             size_t *entry_count_out,
                             char *err, size_t err_size) {
  size_t name_len = strlen(name ? name : "");
  size_t name_entries = hs_exfat_name_entry_count(name);
  size_t entry_count = 2 + name_entries;
  if(!out || !entry_count_out || name_len == 0 || name_len > 255 ||
     name_entries > 17 || !path_segment_supported(name)) {
    hs_set_err(err, err_size, "bad exFAT file name");
    errno = EINVAL;
    return -1;
  }
  memset(out, 0, entry_count * 32U);
  out[0] = 0x85;
  out[1] = (unsigned char)(entry_count - 1);
  hs_le16(out + 4, 0x20);

  unsigned char *stream = out + 32;
  stream[0] = 0xC0;
  stream[1] = first_cluster ? 0x03 : 0x00;
  stream[3] = (unsigned char)name_len;
  hs_le16(stream + 4, hs_exfat_name_hash(name));
  hs_le64(stream + 8, data_len);
  hs_le32(stream + 20, first_cluster);
  hs_le64(stream + 24, data_len);

  size_t consumed = 0;
  for(size_t e = 0; e < name_entries; e++) {
    unsigned char *fn = out + 32 * (2 + e);
    fn[0] = 0xC1;
    for(size_t i = 0; i < 15; i++) {
      uint16_t ch = 0;
      if(consumed < name_len) ch = (unsigned char)name[consumed++];
      hs_le16(fn + 2 + i * 2, ch);
    }
  }

  hs_le16(out + 2, hs_exfat_entry_set_checksum(out, entry_count));
  *entry_count_out = entry_count;
  return 0;
}

static uint64_t
hs_exfat_cluster_offset(const hs_exfat_t *ex, uint32_t cluster) {
  return (uint64_t)ex->cluster_heap_offset * ex->sector_size +
         ((uint64_t)cluster - 2ULL) * ex->cluster_size;
}

static int
hs_exfat_check_cluster(const hs_exfat_t *ex, uint32_t cluster,
                       char *err, size_t err_size) {
  if(cluster < 2 || (uint64_t)cluster - 2ULL >= ex->cluster_count) {
    hs_set_err(err, err_size, "exFAT cluster outside image");
    errno = EINVAL;
    return -1;
  }
  return 0;
}

static int
hs_exfat_read_fat(const hs_exfat_t *ex, uint32_t cluster, uint32_t *next,
                  char *err, size_t err_size) {
  unsigned char raw[4];
  uint64_t off = (uint64_t)ex->fat_offset * ex->sector_size +
                 (uint64_t)cluster * 4ULL;
  uint64_t fat_end = ((uint64_t)ex->fat_offset + ex->fat_length) *
                     ex->sector_size;
  if(off + sizeof(raw) > fat_end) {
    hs_set_err(err, err_size, "exFAT FAT entry outside table");
    errno = EINVAL;
    return -1;
  }
  if(ex->read_fn(ex->read_ctx, off, raw, sizeof(raw), err, err_size) != 0) {
    return -1;
  }
  *next = hs_rd32(raw);
  return 0;
}

static int
hs_exfat_chain_count(const hs_exfat_t *ex, uint32_t first_cluster,
                     uint64_t data_len, uint8_t flags, uint32_t *count_out,
                     char *err, size_t err_size) {
  uint32_t cluster = first_cluster;
  uint32_t count = 0;
  if(first_cluster == 0) {
    *count_out = 0;
    return 0;
  }
  if(hs_exfat_check_cluster(ex, first_cluster, err, err_size) != 0) {
    return -1;
  }
  for(;;) {
    uint32_t next = 0;
    count++;
    if(count > ex->cluster_count || count > HS_MAX_AMPR_CLUSTERS) break;
    if(hs_exfat_read_fat(ex, cluster, &next, err, err_size) != 0) {
      break;
    }
    if(next >= 0xfffffff8U || next == 0xffffffffU) {
      *count_out = count;
      return 0;
    }
    if(next < 2 || (uint64_t)next - 2ULL >= ex->cluster_count || next == cluster) {
      break;
    }
    cluster = next;
  }
  if(flags & 0x02) {
    uint64_t fallback = hs_ceil_div_u64(data_len, ex->cluster_size);
    if(fallback > UINT32_MAX) {
      hs_set_err(err, err_size, "exFAT file allocation is too large");
      errno = EFBIG;
      return -1;
    }
    *count_out = (uint32_t)fallback;
    return 0;
  }
  hs_set_err(err, err_size, "unsupported fragmented exFAT allocation");
  errno = EINVAL;
  return -1;
}

static int
hs_exfat_stream_cluster_at(const hs_exfat_t *ex, uint32_t first_cluster,
                           uint8_t flags, uint64_t cluster_delta,
                           uint32_t *cluster_out,
                           char *err, size_t err_size) {
  if(first_cluster == 0) {
    hs_set_err(err, err_size, "exFAT stream has no cluster");
    errno = EINVAL;
    return -1;
  }
  if(flags & 0x02) {
    if((uint64_t)first_cluster + cluster_delta > UINT32_MAX ||
       hs_exfat_check_cluster(ex, first_cluster + (uint32_t)cluster_delta,
                              err, err_size) != 0) {
      return -1;
    }
    *cluster_out = first_cluster + (uint32_t)cluster_delta;
    return 0;
  }
  uint32_t cluster = first_cluster;
  for(uint64_t i = 0; i < cluster_delta; i++) {
    uint32_t next = 0;
    if(hs_exfat_read_fat(ex, cluster, &next, err, err_size) != 0) return -1;
    if(next >= 0xfffffff8U || next == 0xffffffffU) {
      hs_set_err(err, err_size, "exFAT stream ended early");
      errno = EINVAL;
      return -1;
    }
    cluster = next;
  }
  if(hs_exfat_check_cluster(ex, cluster, err, err_size) != 0) return -1;
  *cluster_out = cluster;
  return 0;
}

static int
hs_exfat_stream_image_offset(const hs_exfat_t *ex, uint32_t first_cluster,
                             uint8_t flags, uint64_t stream_off,
                             uint64_t *image_off,
                             char *err, size_t err_size) {
  uint64_t cluster_delta = stream_off / ex->cluster_size;
  uint64_t cluster_off = stream_off % ex->cluster_size;
  uint32_t cluster = 0;
  if(hs_exfat_stream_cluster_at(ex, first_cluster, flags, cluster_delta,
                                &cluster, err, err_size) != 0) {
    return -1;
  }
  *image_off = hs_exfat_cluster_offset(ex, cluster) + cluster_off;
  return 0;
}

static int
hs_exfat_read_stream(const hs_exfat_t *ex, uint32_t first_cluster,
                     uint8_t flags, uint64_t stream_off, void *out,
                     size_t size, char *err, size_t err_size) {
  unsigned char *dst = out;
  while(size > 0) {
    uint64_t cluster_off = stream_off % ex->cluster_size;
    size_t avail = (size_t)(ex->cluster_size - cluster_off);
    size_t n = avail < size ? avail : size;
    uint64_t image_off = 0;
    if(hs_exfat_stream_image_offset(ex, first_cluster, flags, stream_off,
                                    &image_off, err, err_size) != 0) {
      return -1;
    }
    if(ex->read_fn(ex->read_ctx, image_off, dst, n, err, err_size) != 0) {
      return -1;
    }
    dst += n;
    stream_off += n;
    size -= n;
  }
  return 0;
}

static int
hs_exfat_decode_name(const unsigned char *set, size_t entry_count,
                     size_t name_len, char *name, size_t name_size,
                     char *err, size_t err_size) {
  if(name_len == 0 || name_len >= name_size) {
    hs_set_err(err, err_size, "invalid exFAT name length");
    errno = EINVAL;
    return -1;
  }
  size_t pos = 0;
  for(size_t e = 2; e < entry_count && pos < name_len; e++) {
    const unsigned char *fn = set + e * 32U;
    if(fn[0] != 0xC1) {
      hs_set_err(err, err_size, "invalid exFAT filename entry");
      errno = EINVAL;
      return -1;
    }
    for(size_t i = 0; i < 15 && pos < name_len; i++) {
      uint16_t ch = hs_rd16(fn + 2 + i * 2);
      if(ch == 0 || ch >= 0x80) {
        return HS_EXFAT_NAME_SKIP;
      }
      name[pos++] = (char)ch;
    }
  }
  if(pos != name_len) {
    hs_set_err(err, err_size, "truncated exFAT filename");
    errno = EINVAL;
    return -1;
  }
  name[pos] = 0;
  if(!path_segment_supported(name)) {
    return HS_EXFAT_NAME_SKIP;
  }
  return 0;
}

static int
hs_exfat_open_reader(hs_read_fn read_fn, void *read_ctx, hs_exfat_t *ex,
                     char *err, size_t err_size) {
  unsigned char boot[512];
  memset(ex, 0, sizeof(*ex));
  ex->read_fn = read_fn;
  ex->read_ctx = read_ctx;
  if(read_fn(read_ctx, 0, boot, sizeof(boot), err, err_size) != 0) return -1;
  if(memcmp(boot + 3, "EXFAT   ", 8) != 0) {
    hs_set_err(err, err_size, "nested image is not exFAT");
    errno = EINVAL;
    return -1;
  }
  uint8_t sector_shift = boot[0x6c];
  uint8_t cluster_shift = boot[0x6d];
  if(sector_shift >= 31 || cluster_shift >= 31) {
    hs_set_err(err, err_size, "unsupported exFAT sector geometry");
    errno = EINVAL;
    return -1;
  }
  ex->sector_size = 1ULL << sector_shift;
  ex->cluster_size = ex->sector_size * (1ULL << cluster_shift);
  ex->volume_length = hs_rd64(boot + 0x48);
  ex->fat_offset = hs_rd32(boot + 0x50);
  ex->fat_length = hs_rd32(boot + 0x54);
  ex->cluster_heap_offset = hs_rd32(boot + 0x58);
  ex->cluster_count = hs_rd32(boot + 0x5c);
  ex->root_cluster = hs_rd32(boot + 0x60);
  if(ex->sector_size != HS_EXFAT_SECTOR_SIZE ||
     ex->cluster_size == 0 ||
     ex->cluster_size > 1024ULL * 1024ULL ||
     boot[0x6e] != 1 ||
     ex->fat_offset == 0 ||
     ex->fat_length == 0 ||
     ex->cluster_heap_offset <= ex->fat_offset ||
     ex->cluster_count == 0 ||
     ex->volume_length == 0 ||
     hs_exfat_check_cluster(ex, ex->root_cluster, err, err_size) != 0) {
    hs_set_err(err, err_size, "unsupported exFAT layout");
    errno = EINVAL;
    return -1;
  }
  return 0;
}

static int
hs_exfat_dir_size(const hs_exfat_t *ex, uint32_t first_cluster,
                  uint64_t explicit_size, uint8_t flags, uint64_t *size_out,
                  char *err, size_t err_size) {
  if(explicit_size) {
    *size_out = explicit_size;
    return 0;
  }
  uint32_t clusters = 0;
  if(hs_exfat_chain_count(ex, first_cluster, 0, flags, &clusters,
                          err, err_size) != 0) {
    return -1;
  }
  *size_out = (uint64_t)clusters * ex->cluster_size;
  return 0;
}

static int
hs_exfat_load_root_bitmap(hs_exfat_t *ex, char *err, size_t err_size) {
  uint64_t dir_size = 0;
  if(hs_exfat_dir_size(ex, ex->root_cluster, 0, 0, &dir_size,
                       err, err_size) != 0) {
    return -1;
  }
  unsigned char entry[32];
  for(uint64_t off = 0; off + 32ULL <= dir_size;) {
    if(hs_exfat_read_stream(ex, ex->root_cluster, 0, off, entry,
                            sizeof(entry), err, err_size) != 0) {
      return -1;
    }
    if(entry[0] == 0x00) break;
    if(entry[0] == 0x81) {
      if(hs_exfat_stream_image_offset(ex, ex->root_cluster, 0, off,
                                      &ex->bitmap_entry_off,
                                      err, err_size) != 0) {
        return -1;
      }
      ex->bitmap_cluster = hs_rd32(entry + 20);
      ex->bitmap_size = hs_rd64(entry + 24);
      if(ex->bitmap_cluster == 0 || ex->bitmap_size == 0 ||
         hs_exfat_check_cluster(ex, ex->bitmap_cluster, err, err_size) != 0) {
        return -1;
      }
      ex->has_bitmap = 1;
      return 0;
    }
    if(entry[0] == 0x85) off += ((uint64_t)entry[1] + 1ULL) * 32ULL;
    else off += 32ULL;
  }
  hs_set_err(err, err_size, "exFAT allocation bitmap was not found");
  errno = EINVAL;
  return -1;
}

static int
hs_path_split(const char *path, char segs[][64], size_t *count) {
  size_t n = 0;
  const char *p = path;
  while(p && *p) {
    const char *slash = strchr(p, '/');
    size_t len = slash ? (size_t)(slash - p) : strlen(p);
    if(len == 0 || len >= 64 || n >= 8) {
      errno = EINVAL;
      return -1;
    }
    memcpy(segs[n], p, len);
    segs[n][len] = 0;
    n++;
    p = slash ? slash + 1 : NULL;
  }
  *count = n;
  return n ? 0 : -1;
}

static void
hs_index_list_free(hs_index_list_t *list) {
  if(!list) return;
  free(list->items);
  memset(list, 0, sizeof(*list));
}

static int
hs_index_list_push(hs_index_list_t *list, const char *rel,
                   uint64_t size, int64_t mtime,
                   char *err, size_t err_size) {
  if(!list || !rel || !rel[0]) {
    errno = EINVAL;
    return -1;
  }
  if(list->count == list->cap) {
    size_t next = list->cap ? list->cap * 2 : 256;
    hs_index_file_t *p = realloc(list->items, next * sizeof(*p));
    if(!p) {
      hs_set_err(err, err_size, "out of memory");
      errno = ENOMEM;
      return -1;
    }
    list->items = p;
    list->cap = next;
  }
  hs_index_file_t *item = &list->items[list->count++];
  memset(item, 0, sizeof(*item));
  if(snprintf(item->rel, sizeof(item->rel), "%s", rel) >=
     (int)sizeof(item->rel)) {
    hs_set_err(err, err_size, "AMPR index path too long");
    errno = ENAMETOOLONG;
    return -1;
  }
  item->size = size;
  item->mtime = mtime;
  return 0;
}

static int
hs_join_rel(char *out, size_t out_size, const char *prefix,
            const char *name, char *err, size_t err_size) {
  int n;
  if(!prefix || !prefix[0]) n = snprintf(out, out_size, "%s", name);
  else n = snprintf(out, out_size, "%s/%s", prefix, name);
  if(n < 0 || (size_t)n >= out_size) {
    hs_set_err(err, err_size, "AMPR index path too long");
    errno = ENAMETOOLONG;
    return -1;
  }
  return 0;
}

static int
hs_exfat_collect_index_entries(const hs_exfat_t *ex, uint32_t dir_cluster,
                               uint64_t dir_data_len, uint8_t dir_flags,
                               const char *prefix, int depth,
                               hs_index_list_t *list,
                               char *err, size_t err_size) {
  if(depth > 32) {
    hs_set_err(err, err_size, "exFAT directory tree is too deep");
    errno = EINVAL;
    return -1;
  }
  uint64_t dir_size = 0;
  if(hs_exfat_dir_size(ex, dir_cluster, dir_data_len, dir_flags, &dir_size,
                       err, err_size) != 0) {
    return -1;
  }
  unsigned char entry[32];
  for(uint64_t off = 0; off + 32ULL <= dir_size;) {
    if(hs_exfat_read_stream(ex, dir_cluster, dir_flags, off, entry,
                            sizeof(entry), err, err_size) != 0) {
      return -1;
    }
    if(entry[0] == 0x00) break;
    if(entry[0] != 0x85) {
      off += 32ULL;
      continue;
    }
    size_t secondary_count = entry[1];
    size_t entry_count = secondary_count + 1;
    if(entry_count < 3 || entry_count > 19 ||
       off + entry_count * 32ULL > dir_size) {
      hs_set_err(err, err_size, "invalid exFAT directory entry set");
      errno = EINVAL;
      return -1;
    }
    unsigned char set[32 * 19];
    if(hs_exfat_read_stream(ex, dir_cluster, dir_flags, off, set,
                            entry_count * 32U, err, err_size) != 0) {
      return -1;
    }
    const unsigned char *stream = set + 32;
    if(stream[0] != 0xC0) {
      hs_set_err(err, err_size, "invalid exFAT stream entry");
      errno = EINVAL;
      return -1;
    }
    char name[256];
    int name_rc = hs_exfat_decode_name(set, entry_count, stream[3],
                                       name, sizeof(name), err, err_size);
    if(name_rc < 0) {
      return -1;
    }
    if(name_rc == HS_EXFAT_NAME_SKIP) {
      off += entry_count * 32ULL;
      continue;
    }
    uint16_t attrs = hs_rd16(set + 4);
    uint8_t flags = stream[1];
    uint32_t first = hs_rd32(stream + 20);
    uint64_t data_len = hs_rd64(stream + 24);
    char rel[1024];
    if(hs_join_rel(rel, sizeof(rel), prefix, name, err, err_size) != 0) {
      return -1;
    }
    if(attrs & 0x10) {
      if(first != 0 &&
         hs_exfat_collect_index_entries(ex, first, data_len, flags, rel,
                                        depth + 1, list, err, err_size) != 0) {
        return -1;
      }
    } else if(hs_index_list_push(list, rel, data_len, 0, err,
                                 err_size) != 0) {
      return -1;
    }
    off += entry_count * 32ULL;
  }
  return 0;
}

static int
hs_exfat_find_in_dir(const hs_exfat_t *ex, uint32_t dir_cluster,
                     uint64_t dir_data_len, uint8_t dir_flags,
                     char segs[][64], size_t seg_count, size_t seg_index,
                     hs_exfat_file_t *file, char *err, size_t err_size) {
  uint64_t dir_size = 0;
  if(hs_exfat_dir_size(ex, dir_cluster, dir_data_len, dir_flags, &dir_size,
                       err, err_size) != 0) {
    return -1;
  }
  unsigned char entry[32];
  for(uint64_t off = 0; off + 32ULL <= dir_size;) {
    if(hs_exfat_read_stream(ex, dir_cluster, dir_flags, off, entry,
                            sizeof(entry), err, err_size) != 0) {
      return -1;
    }
    if(entry[0] == 0x00) break;
    if(entry[0] != 0x85) {
      off += 32ULL;
      continue;
    }
    size_t secondary_count = entry[1];
    size_t entry_count = secondary_count + 1;
    if(entry_count < 3 || entry_count > 19 ||
       off + entry_count * 32ULL > dir_size) {
      hs_set_err(err, err_size, "invalid exFAT directory entry set");
      errno = EINVAL;
      return -1;
    }
    unsigned char set[32 * 19];
    if(hs_exfat_read_stream(ex, dir_cluster, dir_flags, off, set,
                            entry_count * 32U, err, err_size) != 0) {
      return -1;
    }
    const unsigned char *stream = set + 32;
    if(stream[0] != 0xC0) {
      hs_set_err(err, err_size, "invalid exFAT stream entry");
      errno = EINVAL;
      return -1;
    }
    char name[64];
    int name_rc = hs_exfat_decode_name(set, entry_count, stream[3],
                                       name, sizeof(name), err, err_size);
    if(name_rc < 0) {
      return -1;
    }
    if(name_rc == HS_EXFAT_NAME_SKIP) {
      off += entry_count * 32ULL;
      continue;
    }
    if(!strcasecmp(name, segs[seg_index])) {
      uint16_t attrs = hs_rd16(set + 4);
      uint8_t flags = stream[1];
      uint32_t first = hs_rd32(stream + 20);
      uint64_t data_len = hs_rd64(stream + 24);
      if(seg_index + 1 == seg_count) {
        uint64_t entry_image_off = 0;
        uint32_t allocated = 0;
        if(attrs & 0x10) {
          hs_set_err(err, err_size, "AMPR path resolves to a directory");
          errno = EINVAL;
          return -1;
        }
        if(hs_exfat_chain_count(ex, first, data_len, flags, &allocated,
                                err, err_size) != 0) {
          return -1;
        }
        if(allocated > HS_MAX_AMPR_CLUSTERS) {
          hs_set_err(err, err_size, "AMPR allocation is too large");
          errno = EFBIG;
          return -1;
        }
        if(hs_exfat_stream_image_offset(ex, dir_cluster, dir_flags, off,
                                        &entry_image_off, err, err_size) != 0) {
          return -1;
        }
        memset(file, 0, sizeof(*file));
        snprintf(file->rel, sizeof(file->rel), "%s/%s",
                 segs[0], segs[seg_count - 1]);
        file->entry_set_off = entry_image_off;
        file->entry_count = entry_count;
        memcpy(file->entry_set, set, entry_count * 32U);
        file->stream_flags = flags;
        file->attrs = attrs;
        file->first_cluster = first;
        file->allocated_clusters = allocated;
        file->data_len = data_len;
        return 0;
      }
      if((attrs & 0x10) == 0 || first == 0) {
        hs_set_err(err, err_size, "AMPR parent path is not a directory");
        errno = EINVAL;
        return -1;
      }
      return hs_exfat_find_in_dir(ex, first, data_len, flags, segs, seg_count,
                                  seg_index + 1, file, err, err_size);
    }
    off += entry_count * 32ULL;
  }
  hs_set_err(err, err_size, "AMPR binary was not found in exFAT image");
  errno = ENOENT;
  return -1;
}

static int
hs_exfat_find_ampr(const hs_exfat_t *ex, hs_exfat_file_t *file,
                   char *err, size_t err_size) {
  static const char *targets[] = {
    "fakelib/libSceAmpr.sprx",
    "fakelib/libSceAmpr.prx",
    "sce_module/libSceAmpr.sprx",
    "sce_module/libSceAmpr.prx",
  };
  char local_err[256] = {0};
  for(size_t i = 0; i < sizeof(targets) / sizeof(targets[0]); i++) {
    char segs[8][64];
    size_t seg_count = 0;
    if(hs_path_split(targets[i], segs, &seg_count) != 0) continue;
    local_err[0] = 0;
    if(hs_exfat_find_in_dir(ex, ex->root_cluster, 0, 0, segs, seg_count, 0,
                            file, local_err, sizeof(local_err)) == 0) {
      return 0;
    }
  }
  hs_set_err(err, err_size, "%s",
             local_err[0] ? local_err : "AMPR binary was not found in exFAT image");
  errno = ENOENT;
  return -1;
}

static int
hs_bitmap_get(const unsigned char *bitmap, uint32_t cluster) {
  uint32_t bit = cluster - 2U;
  return (bitmap[bit / 8U] & (unsigned char)(1U << (bit % 8U))) != 0;
}

static void
hs_bitmap_set(unsigned char *bitmap, uint32_t cluster, int allocated) {
  uint32_t bit = cluster - 2U;
  if(allocated) bitmap[bit / 8U] |= (unsigned char)(1U << (bit % 8U));
  else bitmap[bit / 8U] &= (unsigned char)~(1U << (bit % 8U));
}

static int
hs_find_tail_run(const hs_exfat_t *ex, const unsigned char *bitmap,
                 uint32_t needed, uint32_t desired_slack,
                 uint32_t *first_out, uint32_t *count_out) {
  if(needed == 0 || needed > ex->cluster_count) return -1;
  uint64_t max_start = (uint64_t)ex->cluster_count - needed;
  for(uint64_t start = max_start + 1ULL; start > 0;) {
    start--;
    uint32_t first = (uint32_t)start + 2U;
    uint32_t run = 0;
    while(start + run < ex->cluster_count &&
          !hs_bitmap_get(bitmap, first + run) &&
          run < HS_MAX_AMPR_CLUSTERS) {
      run++;
    }
    if(run >= needed) {
      uint32_t desired = needed + desired_slack;
      if(desired < needed) desired = needed;
      if(desired > run) desired = run;
      *first_out = first;
      *count_out = desired;
      return 0;
    }
  }
  return -1;
}

static int
hs_append_data_patch(const hs_exfat_t *ex, hs_patch_list_t *patches,
                     uint32_t first_cluster, const unsigned char *replacement,
                     size_t replacement_size, char *err, size_t err_size) {
  uint64_t needed_clusters = hs_ceil_div_u64(replacement_size, ex->cluster_size);
  uint64_t patch_size64 = needed_clusters * ex->cluster_size;
  if(needed_clusters == 0 || needed_clusters > HS_MAX_AMPR_CLUSTERS ||
     patch_size64 > SIZE_MAX) {
    hs_set_err(err, err_size, "AMPR replacement is too large");
    errno = EFBIG;
    return -1;
  }
  unsigned char *data = calloc(1, (size_t)patch_size64);
  if(!data) {
    hs_set_err(err, err_size, "out of memory");
    errno = ENOMEM;
    return -1;
  }
  memcpy(data, replacement, replacement_size);
  uint64_t off = hs_exfat_cluster_offset(ex, first_cluster);
  int rc = hs_patch_append(patches, off, data, (size_t)patch_size64,
                           err, err_size);
  free(data);
  return rc;
}

static int
hs_append_fat_range_patch(const hs_exfat_t *ex, hs_patch_list_t *patches,
                          uint32_t first, uint32_t count, int clear,
                          char *err, size_t err_size) {
  if(count == 0) return 0;
  if(first < 2 || (uint64_t)first - 2ULL + count > ex->cluster_count ||
     count > HS_MAX_AMPR_CLUSTERS) {
    hs_set_err(err, err_size, "bad exFAT FAT patch range");
    errno = EINVAL;
    return -1;
  }
  size_t bytes = (size_t)count * 4U;
  unsigned char *raw = calloc(1, bytes);
  if(!raw) {
    hs_set_err(err, err_size, "out of memory");
    errno = ENOMEM;
    return -1;
  }
  if(!clear) {
    for(uint32_t i = 0; i < count; i++) {
      uint32_t cluster = first + i;
      uint32_t value = i + 1U < count ? cluster + 1U : 0xffffffffU;
      hs_le32(raw + (size_t)i * 4U, value);
    }
  }
  uint64_t off = (uint64_t)ex->fat_offset * ex->sector_size +
                 (uint64_t)first * 4ULL;
  int rc = hs_patch_append(patches, off, raw, bytes, err, err_size);
  free(raw);
  return rc;
}

static int
hs_append_fat_entry_patch(const hs_exfat_t *ex, hs_patch_list_t *patches,
                          uint32_t cluster, uint32_t value,
                          char *err, size_t err_size) {
  if(cluster < 2 || (uint64_t)cluster - 2ULL >= ex->cluster_count) {
    hs_set_err(err, err_size, "bad exFAT FAT entry patch cluster");
    errno = EINVAL;
    return -1;
  }
  unsigned char raw[4];
  hs_le32(raw, value);
  uint64_t off = (uint64_t)ex->fat_offset * ex->sector_size +
                 (uint64_t)cluster * 4ULL;
  return hs_patch_append(patches, off, raw, sizeof(raw), err, err_size);
}

static int
hs_exfat_chain_last(const hs_exfat_t *ex, uint32_t first_cluster,
                    uint32_t *last_out, uint32_t *count_out,
                    char *err, size_t err_size) {
  uint32_t cluster = first_cluster;
  uint32_t count = 0;
  if(hs_exfat_check_cluster(ex, first_cluster, err, err_size) != 0) {
    return -1;
  }
  for(;;) {
    uint32_t next = 0;
    count++;
    if(count > ex->cluster_count) break;
    if(hs_exfat_read_fat(ex, cluster, &next, err, err_size) != 0) {
      return -1;
    }
    if(next >= 0xfffffff8U || next == 0xffffffffU) {
      if(last_out) *last_out = cluster;
      if(count_out) *count_out = count;
      return 0;
    }
    if(next < 2 || (uint64_t)next - 2ULL >= ex->cluster_count ||
       next == cluster) {
      break;
    }
    cluster = next;
  }
  hs_set_err(err, err_size, "unsupported exFAT directory chain");
  errno = EINVAL;
  return -1;
}

static int
hs_exfat_find_root_index_slot(const hs_exfat_t *ex, hs_exfat_file_t *file,
                              int *found, uint64_t *insert_off,
                              char *err, size_t err_size) {
  uint64_t dir_size = 0;
  size_t needed_entries = 2 + hs_exfat_name_entry_count("ampr_emu.index");
  if(file) memset(file, 0, sizeof(*file));
  if(found) *found = 0;
  if(insert_off) *insert_off = 0;
  if(hs_exfat_dir_size(ex, ex->root_cluster, 0, 0, &dir_size,
                       err, err_size) != 0) {
    return -1;
  }
  unsigned char entry[32];
  for(uint64_t off = 0; off + 32ULL <= dir_size;) {
    if(hs_exfat_read_stream(ex, ex->root_cluster, 0, off, entry,
                            sizeof(entry), err, err_size) != 0) {
      return -1;
    }
    if(entry[0] == 0x00) {
      if(off + (needed_entries + 1ULL) * 32ULL > dir_size) {
        hs_set_err(err, err_size,
                   "exFAT root directory has no room for ampr_emu.index");
        errno = ENOSPC;
        return -1;
      }
      if(insert_off &&
         hs_exfat_stream_image_offset(ex, ex->root_cluster, 0, off,
                                      insert_off, err, err_size) != 0) {
        return -1;
      }
      return 0;
    }
    if(entry[0] != 0x85) {
      off += 32ULL;
      continue;
    }
    size_t secondary_count = entry[1];
    size_t entry_count = secondary_count + 1;
    if(entry_count < 3 || entry_count > 19 ||
       off + entry_count * 32ULL > dir_size) {
      hs_set_err(err, err_size, "invalid exFAT directory entry set");
      errno = EINVAL;
      return -1;
    }
    unsigned char set[32 * 19];
    if(hs_exfat_read_stream(ex, ex->root_cluster, 0, off, set,
                            entry_count * 32U, err, err_size) != 0) {
      return -1;
    }
    const unsigned char *stream = set + 32;
    if(stream[0] != 0xC0) {
      hs_set_err(err, err_size, "invalid exFAT stream entry");
      errno = EINVAL;
      return -1;
    }
    char name[256];
    int name_rc = hs_exfat_decode_name(set, entry_count, stream[3],
                                       name, sizeof(name), err, err_size);
    if(name_rc < 0) {
      return -1;
    }
    if(name_rc == HS_EXFAT_NAME_SKIP) {
      off += entry_count * 32ULL;
      continue;
    }
    if(!strcasecmp(name, "ampr_emu.index")) {
      uint16_t attrs = hs_rd16(set + 4);
      if(attrs & 0x10) {
        hs_set_err(err, err_size, "ampr_emu.index is a directory");
        errno = EINVAL;
        return -1;
      }
      uint8_t flags = stream[1];
      uint32_t first = hs_rd32(stream + 20);
      uint64_t data_len = hs_rd64(stream + 24);
      uint32_t allocated = 0;
      if(hs_exfat_chain_count(ex, first, data_len, flags, &allocated,
                              err, err_size) != 0) {
        return -1;
      }
      if(file) {
        uint64_t entry_image_off = 0;
        if(hs_exfat_stream_image_offset(ex, ex->root_cluster, 0, off,
                                        &entry_image_off, err,
                                        err_size) != 0) {
          return -1;
        }
        snprintf(file->rel, sizeof(file->rel), "%s", "ampr_emu.index");
        file->entry_set_off = entry_image_off;
        file->entry_count = entry_count;
        memcpy(file->entry_set, set, entry_count * 32U);
        file->stream_flags = flags;
        file->attrs = attrs;
        file->first_cluster = first;
        file->allocated_clusters = allocated;
        file->data_len = data_len;
      }
      if(found) *found = 1;
      return 0;
    }
    off += entry_count * 32ULL;
  }
  hs_set_err(err, err_size, "exFAT root directory has no free index slot");
  errno = ENOSPC;
  return -1;
}

static int
hs_exfat_find_root_index_slot_for_write(const hs_exfat_t *ex,
                                        hs_root_index_slot_t *slot,
                                        char *err, size_t err_size) {
  uint64_t dir_size = 0;
  size_t needed_entries = 2 + hs_exfat_name_entry_count("ampr_emu.index");
  if(!slot) {
    hs_set_err(err, err_size, "bad exFAT root slot output");
    errno = EINVAL;
    return -1;
  }
  memset(slot, 0, sizeof(*slot));
  if(hs_exfat_dir_size(ex, ex->root_cluster, 0, 0, &dir_size,
                       err, err_size) != 0 ||
     hs_exfat_chain_last(ex, ex->root_cluster, &slot->root_last_cluster,
                         NULL, err, err_size) != 0) {
    return -1;
  }
  slot->old_dir_size = dir_size;

  unsigned char entry[32];
  for(uint64_t off = 0; off + 32ULL <= dir_size;) {
    if(hs_exfat_read_stream(ex, ex->root_cluster, 0, off, entry,
                            sizeof(entry), err, err_size) != 0) {
      return -1;
    }
    if(entry[0] == 0x00) {
      slot->insert_stream_off = off;
      if(off + (needed_entries + 1ULL) * 32ULL <= dir_size) {
        if(hs_exfat_stream_image_offset(ex, ex->root_cluster, 0, off,
                                        &slot->insert_off,
                                        err, err_size) != 0) {
          return -1;
        }
      } else {
        slot->needs_root_extend = 1;
      }
      return 0;
    }
    if(entry[0] != 0x85) {
      off += 32ULL;
      continue;
    }
    size_t secondary_count = entry[1];
    size_t entry_count = secondary_count + 1;
    if(entry_count < 3 || entry_count > 19 ||
       off + entry_count * 32ULL > dir_size) {
      hs_set_err(err, err_size, "invalid exFAT directory entry set");
      errno = EINVAL;
      return -1;
    }
    unsigned char set[32 * 19];
    if(hs_exfat_read_stream(ex, ex->root_cluster, 0, off, set,
                            entry_count * 32U, err, err_size) != 0) {
      return -1;
    }
    const unsigned char *stream = set + 32;
    if(stream[0] != 0xC0) {
      hs_set_err(err, err_size, "invalid exFAT stream entry");
      errno = EINVAL;
      return -1;
    }
    char name[256];
    int name_rc = hs_exfat_decode_name(set, entry_count, stream[3],
                                       name, sizeof(name), err, err_size);
    if(name_rc < 0) {
      return -1;
    }
    if(name_rc == HS_EXFAT_NAME_SKIP) {
      off += entry_count * 32ULL;
      continue;
    }
    if(!strcasecmp(name, "ampr_emu.index")) {
      uint16_t attrs = hs_rd16(set + 4);
      if(attrs & 0x10) {
        hs_set_err(err, err_size, "ampr_emu.index is a directory");
        errno = EINVAL;
        return -1;
      }
      uint8_t flags = stream[1];
      uint32_t first = hs_rd32(stream + 20);
      uint64_t data_len = hs_rd64(stream + 24);
      uint32_t allocated = 0;
      if(hs_exfat_chain_count(ex, first, data_len, flags, &allocated,
                              err, err_size) != 0) {
        return -1;
      }
      uint64_t entry_image_off = 0;
      if(hs_exfat_stream_image_offset(ex, ex->root_cluster, 0, off,
                                      &entry_image_off, err, err_size) != 0) {
        return -1;
      }
      snprintf(slot->existing.rel, sizeof(slot->existing.rel), "%s",
               "ampr_emu.index");
      slot->existing.entry_set_off = entry_image_off;
      slot->existing.entry_count = entry_count;
      memcpy(slot->existing.entry_set, set, entry_count * 32U);
      slot->existing.stream_flags = flags;
      slot->existing.attrs = attrs;
      slot->existing.first_cluster = first;
      slot->existing.allocated_clusters = allocated;
      slot->existing.data_len = data_len;
      slot->found_existing = 1;
      return 0;
    }
    off += entry_count * 32ULL;
  }

  slot->insert_stream_off = dir_size;
  slot->needs_root_extend = 1;
  return 0;
}

static int
hs_build_exfat_index_blob(hs_exfat_t *ex, unsigned char **out,
                          size_t *out_size, ampr_index_stats_t *stats,
                          char *err, size_t err_size) {
  hs_index_list_t list = {0};
  ampr_index_entry_t *entries = NULL;
  int rc = -1;
  if(out) *out = NULL;
  if(out_size) *out_size = 0;
  if(hs_exfat_collect_index_entries(ex, ex->root_cluster, 0, 0, "", 0,
                                    &list, err, err_size) != 0) {
    goto done;
  }
  if(list.count == 0 || list.count > SIZE_MAX / sizeof(*entries)) {
    hs_set_err(err, err_size, "exFAT image has no files to index");
    errno = EINVAL;
    goto done;
  }
  entries = calloc(list.count, sizeof(*entries));
  if(!entries) {
    hs_set_err(err, err_size, "out of memory");
    errno = ENOMEM;
    goto done;
  }
  for(size_t i = 0; i < list.count; i++) {
    entries[i].rel = list.items[i].rel;
    entries[i].size = list.items[i].size;
    entries[i].mtime = list.items[i].mtime;
  }
  if(!ampr_index_entries_need_index(entries, list.count)) {
    hs_set_err(err, err_size, "exFAT image has no AMPR marker files to index");
    errno = ENOENT;
    goto done;
  }
  if(ampr_index_build_to_memory(entries, list.count, 0, out, out_size,
                                stats, err, err_size) != 0) {
    goto done;
  }
  rc = 0;

done:
  free(entries);
  hs_index_list_free(&list);
  return rc;
}

static int
hs_build_exfat_index_patches(hs_exfat_t *ex, hs_patch_list_t *patches,
                             pfs_ampr_hotswap_info_t *info,
                             char *err, size_t err_size) {
  unsigned char *index_data = NULL;
  size_t index_size = 0;
  ampr_index_stats_t stats;
  hs_root_index_slot_t slot;
  unsigned char *bitmap = NULL;
  uint64_t bitmap_off = 0;
  uint32_t needed_clusters = 0;
  uint32_t run_first = 0;
  uint32_t run_alloc = 0;
  uint32_t root_extra = 0;
  uint32_t root_ext_first = 0;
  uint32_t new_first = 0;
  uint32_t new_alloc = 0;
  int rc = -1;

  memset(&stats, 0, sizeof(stats));
  memset(&slot, 0, sizeof(slot));
  if(hs_build_exfat_index_blob(ex, &index_data, &index_size, &stats,
                               err, err_size) != 0) {
    goto done;
  }
  uint64_t needed64 = hs_ceil_div_u64(index_size, ex->cluster_size);
  if(needed64 == 0 || needed64 > HS_MAX_AMPR_CLUSTERS ||
     needed64 > UINT32_MAX) {
    hs_set_err(err, err_size, "AMPR index is too large for exFAT hot patch");
    errno = EFBIG;
    goto done;
  }
  needed_clusters = (uint32_t)needed64;
  if(hs_exfat_find_root_index_slot_for_write(ex, &slot, err, err_size) != 0 ||
     hs_exfat_load_root_bitmap(ex, err, err_size) != 0) {
    goto done;
  }
  if(slot.found_existing &&
     hs_exfat_require_contiguous_allocation(&slot.existing, "ampr_emu.index",
                                            err, err_size) != 0) {
    goto done;
  }
  root_extra = (!slot.found_existing && slot.needs_root_extend) ? 1U : 0U;
  uint64_t bitmap_bytes64 = hs_ceil_div_u64(ex->cluster_count, 8);
  if(ex->bitmap_size < bitmap_bytes64 || ex->bitmap_size > SIZE_MAX) {
    hs_set_err(err, err_size, "invalid exFAT allocation bitmap size");
    errno = EINVAL;
    goto done;
  }
  bitmap = malloc((size_t)ex->bitmap_size);
  if(!bitmap) {
    hs_set_err(err, err_size, "out of memory");
    errno = ENOMEM;
    goto done;
  }
  bitmap_off = hs_exfat_cluster_offset(ex, ex->bitmap_cluster);
  if(ex->read_fn(ex->read_ctx, bitmap_off, bitmap, (size_t)ex->bitmap_size,
                 err, err_size) != 0) {
    goto done;
  }
  if(slot.found_existing &&
     slot.existing.first_cluster &&
     slot.existing.allocated_clusters) {
    for(uint32_t i = 0; i < slot.existing.allocated_clusters; i++) {
      hs_bitmap_set(bitmap, slot.existing.first_cluster + i, 0);
    }
  }
  if(root_extra && needed_clusters >= UINT32_MAX) {
    hs_set_err(err, err_size, "AMPR index allocation is too large");
    errno = EFBIG;
    goto done;
  }
  if(hs_find_tail_run(ex, bitmap, needed_clusters + root_extra,
                      HS_AMPR_SLACK_CLUSTERS, &run_first,
                      &run_alloc) != 0) {
    hs_set_err(err, err_size,
               "not enough tail free clusters for ampr_emu.index");
    errno = ENOSPC;
    goto done;
  }
  if(root_extra) {
    root_ext_first = run_first;
    new_first = run_first + 1U;
    new_alloc = run_alloc - 1U;
  } else {
    new_first = run_first;
    new_alloc = run_alloc;
  }
  if(new_alloc < needed_clusters) {
    hs_set_err(err, err_size, "bad AMPR index tail allocation");
    errno = EIO;
    goto done;
  }
  for(uint32_t i = 0; i < run_alloc; i++) {
    hs_bitmap_set(bitmap, run_first + i, 1);
  }

  unsigned char entry_set[32 * 19];
  size_t entry_count = 0;
  if(hs_exfat_make_file_entry_set("ampr_emu.index", new_first,
                                  (uint64_t)index_size, entry_set,
                                  &entry_count, err, err_size) != 0) {
    goto done;
  }

  if((root_extra &&
      (hs_append_fat_entry_patch(ex, patches, slot.root_last_cluster,
                                 root_ext_first, err, err_size) != 0 ||
       hs_append_fat_range_patch(ex, patches, root_ext_first, 1, 0,
                                 err, err_size) != 0)) ||
     hs_append_fat_range_patch(ex, patches, new_first, new_alloc, 0,
                               err, err_size) != 0 ||
     (slot.found_existing &&
      slot.existing.first_cluster &&
      slot.existing.allocated_clusters &&
      hs_append_fat_range_patch(ex, patches, slot.existing.first_cluster,
                                slot.existing.allocated_clusters, 1,
                                err, err_size) != 0) ||
     hs_patch_append(patches, bitmap_off, bitmap, (size_t)ex->bitmap_size,
                     err, err_size) != 0 ||
     hs_append_data_patch(ex, patches, new_first, index_data, index_size,
                          err, err_size) != 0) {
    goto done;
  }

  if(slot.found_existing) {
    if(hs_patch_append(patches, slot.existing.entry_set_off, entry_set,
                       entry_count * 32U, err, err_size) != 0) {
      goto done;
    }
  } else if(root_extra) {
    size_t entry_bytes = (entry_count + 1U) * 32U;
    unsigned char *root_cluster = calloc(1, (size_t)ex->cluster_size);
    if(!root_cluster) {
      hs_set_err(err, err_size, "out of memory");
      errno = ENOMEM;
      goto done;
    }
    unsigned char root_entries[32 * 20];
    memset(root_entries, 0, sizeof(root_entries));
    memcpy(root_entries, entry_set, entry_count * 32U);
    uint64_t old_tail64 = slot.old_dir_size > slot.insert_stream_off
        ? slot.old_dir_size - slot.insert_stream_off
        : 0;
    size_t old_tail = old_tail64 > entry_bytes
        ? entry_bytes
        : (size_t)old_tail64;
    if(old_tail) {
      uint64_t insert_image_off = 0;
      if(hs_exfat_stream_image_offset(ex, ex->root_cluster, 0,
                                      slot.insert_stream_off,
                                      &insert_image_off,
                                      err, err_size) != 0 ||
         hs_patch_append(patches, insert_image_off, root_entries, old_tail,
                         err, err_size) != 0) {
        free(root_cluster);
        goto done;
      }
    }
    memcpy(root_cluster, root_entries + old_tail, entry_bytes - old_tail);
    if(hs_patch_append(patches, hs_exfat_cluster_offset(ex, root_ext_first),
                       root_cluster, (size_t)ex->cluster_size,
                       err, err_size) != 0) {
      free(root_cluster);
      goto done;
    }
    free(root_cluster);
  } else {
    if(hs_patch_append(patches, slot.insert_off, entry_set,
                       entry_count * 32U, err, err_size) != 0) {
      goto done;
    }
    unsigned char zero[32] = {0};
    if(hs_patch_append(patches, slot.insert_off + entry_count * 32ULL,
                       zero, sizeof(zero), err, err_size) != 0) {
      goto done;
    }
  }
  if(info) {
    snprintf(info->logical_path, sizeof(info->logical_path), "%s",
             "ampr_emu.index");
    info->old_size = slot.found_existing ? slot.existing.data_len : 0;
    info->new_size = index_size;
    info->old_first_cluster = slot.found_existing
        ? slot.existing.first_cluster
        : 0;
    info->new_first_cluster = new_first;
    info->allocated_clusters = new_alloc;
    info->tail_migrated = 1;
  }
  rc = 0;

done:
  free(index_data);
  free(bitmap);
  return rc;
}

static int
hs_build_exfat_ampr_patches(hs_exfat_t *ex,
                            const unsigned char *replacement,
                            size_t replacement_size,
                            hs_patch_list_t *patches,
                            pfs_ampr_hotswap_info_t *info,
                            char *err, size_t err_size) {
  hs_exfat_file_t file;
  uint64_t needed_clusters64 = hs_ceil_div_u64(replacement_size, ex->cluster_size);
  if(needed_clusters64 == 0 || needed_clusters64 > HS_MAX_AMPR_CLUSTERS ||
     needed_clusters64 > UINT32_MAX) {
    hs_set_err(err, err_size, "AMPR replacement is too large");
    errno = EFBIG;
    return -1;
  }
  uint32_t needed_clusters = (uint32_t)needed_clusters64;
  if(hs_exfat_find_ampr(ex, &file, err, err_size) != 0) return -1;
  if(hs_exfat_require_contiguous_allocation(&file, "AMPR binary",
                                            err, err_size) != 0) {
    return -1;
  }

  uint32_t new_first = file.first_cluster;
  uint32_t new_alloc = file.allocated_clusters;
  int tail_migrated = 0;
  unsigned char *bitmap_final = NULL;
  uint64_t bitmap_off = 0;

  if(needed_clusters > file.allocated_clusters) {
    if(hs_exfat_load_root_bitmap(ex, err, err_size) != 0) return -1;
    uint64_t bitmap_bytes64 = hs_ceil_div_u64(ex->cluster_count, 8);
    if(ex->bitmap_size < bitmap_bytes64 || ex->bitmap_size > SIZE_MAX) {
      hs_set_err(err, err_size, "invalid exFAT allocation bitmap size");
      errno = EINVAL;
      return -1;
    }
    unsigned char *bitmap = malloc((size_t)ex->bitmap_size);
    if(!bitmap) {
      hs_set_err(err, err_size, "out of memory");
      errno = ENOMEM;
      return -1;
    }
    bitmap_off = hs_exfat_cluster_offset(ex, ex->bitmap_cluster);
    if(ex->read_fn(ex->read_ctx, bitmap_off, bitmap, (size_t)ex->bitmap_size,
                   err, err_size) != 0) {
      free(bitmap);
      return -1;
    }
    if(hs_find_tail_run(ex, bitmap, needed_clusters, HS_AMPR_SLACK_CLUSTERS,
                        &new_first, &new_alloc) != 0) {
      free(bitmap);
      hs_set_err(err, err_size,
                 "new APR-EMU binary does not fit existing exFAT allocation; recompress with AMPR hot-swap layout");
      errno = ENOSPC;
      return -1;
    }
    for(uint32_t i = 0; i < new_alloc; i++) {
      hs_bitmap_set(bitmap, new_first + i, 1);
    }
    bitmap_final = malloc((size_t)ex->bitmap_size);
    if(!bitmap_final) {
      free(bitmap);
      hs_set_err(err, err_size, "out of memory");
      errno = ENOMEM;
      return -1;
    }
    memcpy(bitmap_final, bitmap, (size_t)ex->bitmap_size);
    if(file.first_cluster && file.allocated_clusters) {
      for(uint32_t i = 0; i < file.allocated_clusters; i++) {
        hs_bitmap_set(bitmap_final, file.first_cluster + i, 0);
      }
    }
    if(hs_append_fat_range_patch(ex, patches, new_first, new_alloc, 0,
                                 err, err_size) != 0 ||
       hs_patch_append(patches, bitmap_off, bitmap, (size_t)ex->bitmap_size,
                       err, err_size) != 0) {
      free(bitmap);
      free(bitmap_final);
      return -1;
    }
    free(bitmap);
    tail_migrated = 1;
  }

  if(hs_append_data_patch(ex, patches, new_first, replacement,
                          replacement_size, err, err_size) != 0) {
    free(bitmap_final);
    return -1;
  }

  unsigned char entry_set[32 * 19];
  memcpy(entry_set, file.entry_set, file.entry_count * 32U);
  unsigned char *stream = entry_set + 32;
  stream[1] = new_first ? (uint8_t)((stream[1] | 0x03U) & 0xffU) : 0;
  hs_le64(stream + 8, (uint64_t)replacement_size);
  hs_le32(stream + 20, new_first);
  hs_le64(stream + 24, (uint64_t)replacement_size);
  hs_le16(entry_set + 2, hs_exfat_entry_set_checksum(entry_set,
                                                     file.entry_count));
  if(hs_patch_append(patches, file.entry_set_off, entry_set,
                     file.entry_count * 32U, err, err_size) != 0) {
    free(bitmap_final);
    return -1;
  }
  if(tail_migrated && file.first_cluster && file.allocated_clusters) {
    if(hs_append_fat_range_patch(ex, patches, file.first_cluster,
                                 file.allocated_clusters, 1,
                                 err, err_size) != 0 ||
       hs_patch_append(patches, bitmap_off, bitmap_final,
                       (size_t)ex->bitmap_size, err, err_size) != 0) {
      free(bitmap_final);
      return -1;
    }
  }
  free(bitmap_final);

  if(info) {
    snprintf(info->logical_path, sizeof(info->logical_path), "%s", file.rel);
    info->old_size = file.data_len;
    info->new_size = replacement_size;
    info->old_first_cluster = file.first_cluster;
    info->new_first_cluster = new_first;
    info->allocated_clusters = new_alloc;
    info->tail_migrated = tail_migrated;
  }
  return 0;
}

static int
hs_apply_direct_patches(int fd, const hs_patch_list_t *patches,
                        char *err, size_t err_size) {
  for(size_t i = 0; i < patches->count; i++) {
    if(hs_write_exact_at(fd, patches->items[i].data, patches->items[i].size,
                         patches->items[i].off, err, err_size) != 0) {
      return -1;
    }
  }
  if(fsync(fd) != 0) {
    hs_set_err(err, err_size, "sync exFAT image: %s", strerror(errno));
    return -1;
  }
  return 0;
}

static int
hs_verify_exfat_ampr(hs_exfat_t *ex, const unsigned char *replacement,
                     size_t replacement_size, char *err, size_t err_size) {
  hs_exfat_file_t file;
  if(hs_exfat_find_ampr(ex, &file, err, err_size) != 0) return -1;
  if(file.data_len != (uint64_t)replacement_size) {
    hs_set_err(err, err_size, "AMPR target size verification failed");
    errno = EIO;
    return -1;
  }
  unsigned char *buf = malloc(replacement_size ? replacement_size : 1);
  if(!buf) {
    hs_set_err(err, err_size, "out of memory");
    errno = ENOMEM;
    return -1;
  }
  if(hs_exfat_read_stream(ex, file.first_cluster, file.stream_flags, 0,
                          buf, replacement_size, err, err_size) != 0) {
    free(buf);
    return -1;
  }
  int ok = memcmp(buf, replacement, replacement_size) == 0;
  free(buf);
  if(!ok) {
    hs_set_err(err, err_size, "AMPR target content verification failed");
    errno = EIO;
    return -1;
  }
  return 0;
}

static int
hs_verify_exfat_index(hs_exfat_t *ex, char *err, size_t err_size) {
  unsigned char *expected = NULL;
  size_t expected_size = 0;
  ampr_index_stats_t stats;
  hs_exfat_file_t file;
  int found = 0;
  uint64_t insert_off = 0;
  int rc = -1;
  memset(&stats, 0, sizeof(stats));
  if(hs_build_exfat_index_blob(ex, &expected, &expected_size, &stats,
                               err, err_size) != 0 ||
     hs_exfat_find_root_index_slot(ex, &file, &found, &insert_off,
                                   err, err_size) != 0) {
    goto done;
  }
  (void)insert_off;
  if(!found) {
    hs_set_err(err, err_size, "ampr_emu.index was not found after patch");
    errno = EIO;
    goto done;
  }
  if(file.data_len != (uint64_t)expected_size) {
    hs_set_err(err, err_size, "AMPR index size verification failed");
    errno = EIO;
    goto done;
  }
  unsigned char *buf = malloc(expected_size ? expected_size : 1);
  if(!buf) {
    hs_set_err(err, err_size, "out of memory");
    errno = ENOMEM;
    goto done;
  }
  if(hs_exfat_read_stream(ex, file.first_cluster, file.stream_flags, 0,
                          buf, expected_size, err, err_size) != 0) {
    free(buf);
    goto done;
  }
  int ok = memcmp(buf, expected, expected_size) == 0;
  free(buf);
  if(!ok) {
    hs_set_err(err, err_size, "AMPR index content verification failed");
    errno = EIO;
    goto done;
  }
  rc = 0;

done:
  free(expected);
  return rc;
}

static int
hs_exfat_index_growth_clusters(hs_exfat_t *ex, uint32_t *extra_out,
                               char *err, size_t err_size) {
  unsigned char *index_data = NULL;
  size_t index_size = 0;
  ampr_index_stats_t stats;
  int rc = -1;
  if(extra_out) *extra_out = 0;
  memset(&stats, 0, sizeof(stats));
  if(hs_build_exfat_index_blob(ex, &index_data, &index_size, &stats,
                               err, err_size) != 0) {
    goto done;
  }
  uint64_t needed = hs_ceil_div_u64(index_size, ex->cluster_size);
  if(needed == 0 || needed > HS_MAX_AMPR_CLUSTERS ||
     needed + HS_AMPR_SLACK_CLUSTERS + 1ULL > UINT32_MAX) {
    hs_set_err(err, err_size, "AMPR index is too large for exFAT growth");
    errno = EFBIG;
    goto done;
  }
  if(extra_out) {
    *extra_out = (uint32_t)(needed + HS_AMPR_SLACK_CLUSTERS + 1ULL);
  }
  rc = 0;

done:
  free(index_data);
  return rc;
}

static int
hs_exfat_growth_clusters_for_size(const hs_exfat_t *ex, size_t size,
                                  uint32_t *extra_out,
                                  char *err, size_t err_size) {
  uint64_t needed = hs_ceil_div_u64(size, ex->cluster_size);
  if(extra_out) *extra_out = 0;
  if(needed == 0 || needed > HS_MAX_AMPR_CLUSTERS ||
     needed + HS_AMPR_SLACK_CLUSTERS > UINT32_MAX) {
    hs_set_err(err, err_size, "AMPR replacement is too large for exFAT growth");
    errno = EFBIG;
    return -1;
  }
  if(extra_out) *extra_out = (uint32_t)(needed + HS_AMPR_SLACK_CLUSTERS);
  return 0;
}

static int
hs_resize_direct_exfat_image(int fd, hs_exfat_t *ex, uint32_t extra_clusters,
                             char *err, size_t err_size) {
  hs_patch_list_t patches = {0};
  unsigned char *boot = NULL;
  unsigned char *bitmap = NULL;
  int rc = -1;

  if(extra_clusters == 0) return 0;
  if(hs_exfat_load_root_bitmap(ex, err, err_size) != 0) goto done;
  uint64_t new_cluster_count64 =
      (uint64_t)ex->cluster_count + (uint64_t)extra_clusters;
  if(new_cluster_count64 > 0xffffff00ULL) {
    hs_set_err(err, err_size, "exFAT image is too large to grow");
    errno = EFBIG;
    goto done;
  }
  uint64_t fat_entries = ((uint64_t)ex->fat_length * ex->sector_size) / 4ULL;
  if(fat_entries < 2ULL || new_cluster_count64 + 2ULL > fat_entries) {
    hs_set_err(err, err_size,
               "cannot grow exFAT image: existing FAT has no spare capacity");
    errno = ENOSPC;
    goto done;
  }
  uint32_t bitmap_alloc_clusters = 0;
  if(hs_exfat_chain_count(ex, ex->bitmap_cluster, ex->bitmap_size, 0,
                          &bitmap_alloc_clusters, err, err_size) != 0) {
    goto done;
  }
  uint64_t bitmap_capacity =
      (uint64_t)bitmap_alloc_clusters * ex->cluster_size;
  uint64_t new_bitmap_size64 = hs_ceil_div_u64(new_cluster_count64, 8ULL);
  if(new_bitmap_size64 == 0 || new_bitmap_size64 > bitmap_capacity ||
     new_bitmap_size64 > SIZE_MAX || ex->bitmap_size > SIZE_MAX) {
    hs_set_err(err, err_size,
               "cannot grow exFAT image: allocation bitmap has no spare capacity");
    errno = ENOSPC;
    goto done;
  }
  uint64_t sectors_per_cluster = ex->cluster_size / ex->sector_size;
  uint64_t old_data_end =
      (uint64_t)ex->cluster_heap_offset +
      (uint64_t)ex->cluster_count * sectors_per_cluster;
  uint64_t tail_pad = ex->volume_length > old_data_end
      ? ex->volume_length - old_data_end
      : 0;
  if(tail_pad < 24ULL) tail_pad = 24ULL;
  uint64_t new_data_end =
      (uint64_t)ex->cluster_heap_offset +
      new_cluster_count64 * sectors_per_cluster;
  if(new_data_end > UINT64_MAX - tail_pad) {
    hs_set_err(err, err_size, "grown exFAT image size overflow");
    errno = EFBIG;
    goto done;
  }
  uint64_t new_volume_length = new_data_end + tail_pad;
  if(new_volume_length > UINT64_MAX / ex->sector_size ||
     new_volume_length * ex->sector_size > (uint64_t)LLONG_MAX) {
    hs_set_err(err, err_size, "grown exFAT image exceeds host file limits");
    errno = EFBIG;
    goto done;
  }
  uint64_t new_image_size = new_volume_length * ex->sector_size;

  size_t boot_size = (size_t)(24ULL * ex->sector_size);
  boot = malloc(boot_size);
  bitmap = calloc(1, (size_t)new_bitmap_size64);
  if(!boot || !bitmap) {
    hs_set_err(err, err_size, "out of memory");
    errno = ENOMEM;
    goto done;
  }
  if(hs_read_exact_at(fd, boot, boot_size, 0, err, err_size) != 0 ||
     hs_read_exact_at(fd, bitmap, (size_t)ex->bitmap_size,
                      hs_exfat_cluster_offset(ex, ex->bitmap_cluster),
                      err, err_size) != 0) {
    goto done;
  }

  uint64_t allocated = 0;
  for(uint32_t cluster = 2; (uint64_t)cluster - 2ULL < ex->cluster_count;
      cluster++) {
    if(hs_bitmap_get(bitmap, cluster)) allocated++;
  }
  uint8_t percent = new_cluster_count64
      ? (uint8_t)((allocated * 100ULL + new_cluster_count64 - 1ULL) /
                  new_cluster_count64)
      : 0;
  if(percent > 100) percent = 100;

  for(int copy = 0; copy < 2; copy++) {
    unsigned char *base = boot + (size_t)copy * 12U * (size_t)ex->sector_size;
    hs_le64(base + 0x48, new_volume_length);
    hs_le32(base + 0x5c, (uint32_t)new_cluster_count64);
    base[0x70] = percent;
    uint32_t checksum = hs_exfat_boot_checksum(base, (size_t)ex->sector_size);
    unsigned char *check = base + 11U * (size_t)ex->sector_size;
    for(size_t off = 0; off < ex->sector_size; off += 4U) {
      hs_le32(check + off, checksum);
    }
  }

  if(ftruncate(fd, (off_t)new_image_size) != 0) {
    hs_set_err(err, err_size, "grow exFAT image: %s", strerror(errno));
    goto done;
  }
  if(hs_patch_append(&patches, 0, boot, boot_size, err, err_size) != 0 ||
     hs_patch_append(&patches, hs_exfat_cluster_offset(ex, ex->bitmap_cluster),
                     bitmap, (size_t)new_bitmap_size64,
                     err, err_size) != 0) {
    goto done;
  }
  if(new_bitmap_size64 != ex->bitmap_size) {
    unsigned char bitmap_entry[32];
    if(hs_read_exact_at(fd, bitmap_entry, sizeof(bitmap_entry),
                        ex->bitmap_entry_off, err, err_size) != 0) {
      goto done;
    }
    hs_le64(bitmap_entry + 24, new_bitmap_size64);
    if(hs_patch_append(&patches, ex->bitmap_entry_off, bitmap_entry,
                       sizeof(bitmap_entry), err, err_size) != 0) {
      goto done;
    }
  }
  if(hs_apply_direct_patches(fd, &patches, err, err_size) != 0) {
    goto done;
  }
  rc = 0;

done:
  free(boot);
  free(bitmap);
  hs_patch_list_free(&patches);
  return rc;
}

int
pfs_ampr_index_exfat_image(const char *image_path,
                           pfs_ampr_hotswap_info_t *info,
                           char *err, size_t err_size) {
  int fd = -1;
  hs_exfat_t ex;
  hs_patch_list_t patches = {0};
  int rc = -1;

  if(info) memset(info, 0, sizeof(*info));
  if(!image_path || !image_path[0]) {
    hs_set_err(err, err_size, "bad AMPR index input");
    errno = EINVAL;
    return -1;
  }
  fd = open(image_path, O_RDWR);
  if(fd < 0) {
    hs_set_err(err, err_size, "open exFAT image: %s", strerror(errno));
    goto done;
  }
  if(hs_exfat_open_reader(hs_fd_read_cb, &fd, &ex, err, err_size) != 0) {
    goto done;
  }
  if(hs_build_exfat_index_patches(&ex, &patches, info, err, err_size) != 0) {
    int saved_errno = errno;
    hs_patch_list_free(&patches);
    memset(&patches, 0, sizeof(patches));
    if(saved_errno != ENOSPC) goto done;
    uint32_t extra_clusters = 0;
    if(hs_exfat_index_growth_clusters(&ex, &extra_clusters,
                                      err, err_size) != 0 ||
       hs_resize_direct_exfat_image(fd, &ex, extra_clusters,
                                    err, err_size) != 0 ||
       hs_exfat_open_reader(hs_fd_read_cb, &fd, &ex, err, err_size) != 0 ||
       hs_build_exfat_index_patches(&ex, &patches, info,
                                    err, err_size) != 0) {
      goto done;
    }
  }
  if(hs_apply_direct_patches(fd, &patches, err, err_size) != 0 ||
     hs_exfat_open_reader(hs_fd_read_cb, &fd, &ex, err, err_size) != 0 ||
     hs_verify_exfat_index(&ex, err, err_size) != 0) {
    goto done;
  }
  if(info) {
    snprintf(info->mode, sizeof(info->mode), "%s", "exfat-index-tail");
  }
  rc = 0;

done:
  if(fd >= 0) close(fd);
  hs_patch_list_free(&patches);
  return rc;
}

int
pfs_ampr_hotswap_exfat_image(const char *image_path,
                             const char *replacement_path,
                             pfs_ampr_hotswap_info_t *info,
                             char *err, size_t err_size) {
  int fd = -1;
  unsigned char *replacement = NULL;
  size_t replacement_size = 0;
  hs_exfat_t ex;
  hs_patch_list_t patches = {0};
  int rc = -1;

  if(info) memset(info, 0, sizeof(*info));
  if(!image_path || !replacement_path) {
    hs_set_err(err, err_size, "bad AMPR hot-swap input");
    errno = EINVAL;
    return -1;
  }
  if(hs_file_to_memory(replacement_path, &replacement, &replacement_size,
                       err, err_size) != 0) {
    return -1;
  }
  fd = open(image_path, O_RDWR);
  if(fd < 0) {
    hs_set_err(err, err_size, "open exFAT image: %s", strerror(errno));
    goto done;
  }
  if(hs_exfat_open_reader(hs_fd_read_cb, &fd, &ex, err, err_size) != 0) {
    goto done;
  }
  if(hs_build_exfat_ampr_patches(&ex, replacement, replacement_size,
                                 &patches, info, err, err_size) != 0) {
    int saved_errno = errno;
    hs_patch_list_free(&patches);
    memset(&patches, 0, sizeof(patches));
    if(saved_errno != ENOSPC) goto done;
    uint32_t extra_clusters = 0;
    if(hs_exfat_growth_clusters_for_size(&ex, replacement_size,
                                         &extra_clusters,
                                         err, err_size) != 0 ||
       hs_resize_direct_exfat_image(fd, &ex, extra_clusters,
                                    err, err_size) != 0 ||
       hs_exfat_open_reader(hs_fd_read_cb, &fd, &ex, err, err_size) != 0 ||
       hs_build_exfat_ampr_patches(&ex, replacement, replacement_size,
                                   &patches, info, err, err_size) != 0) {
      goto done;
    }
  }
  if(hs_apply_direct_patches(fd, &patches, err, err_size) != 0 ||
     hs_exfat_open_reader(hs_fd_read_cb, &fd, &ex, err, err_size) != 0 ||
     hs_verify_exfat_ampr(&ex, replacement, replacement_size,
                          err, err_size) != 0) {
    goto done;
  }
  if(info) {
    snprintf(info->mode, sizeof(info->mode), "%s",
             info->tail_migrated ? "exfat-tail" : "exfat-in-place");
  }
  rc = 0;

done:
  if(fd >= 0) close(fd);
  free(replacement);
  hs_patch_list_free(&patches);
  return rc;
}

static void
hs_parse_inode(const unsigned char *data, uint16_t *mode, uint32_t *flags,
               uint64_t *size, uint64_t *size_comp,
               uint32_t *blocks, int32_t *db0) {
  if(mode) *mode = hs_rd16(data + 0x00);
  if(flags) *flags = hs_rd32(data + 0x04);
  if(size) *size = hs_rd64(data + 0x08);
  if(size_comp) *size_comp = hs_rd64(data + 0x10);
  if(blocks) *blocks = hs_rd32(data + 0x60);
  if(db0) *db0 = (int32_t)hs_rd32(data + 0x64);
}

static int
hs_pfsc_header_span(uint64_t block_count, uint64_t *out) {
  uint64_t table_size = (block_count + 1ULL) * HS_PFSC_OFFSET_ENTRY_SIZE;
  uint64_t initial_capacity =
      HS_PFSC_INITIAL_DATA_OFFSET - HS_PFSC_BLOCK_OFFSETS_OFFSET;
  uint64_t extra = table_size > initial_capacity
      ? table_size - initial_capacity
      : 0;
  if(extra > UINT64_MAX - HS_PFSC_INITIAL_DATA_OFFSET) return -1;
  *out = HS_PFSC_INITIAL_DATA_OFFSET +
         hs_ceil_div_u64(extra, HS_BLOCK_SIZE) * HS_BLOCK_SIZE;
  return 0;
}

static void
hs_pfsc_close(hs_pfsc_image_t *img) {
  if(!img) return;
  if(img->fd >= 0) close(img->fd);
  free(img->offsets);
  memset(img, 0, sizeof(*img));
  img->fd = -1;
}

static int
hs_pfsc_read_nested_name(int fd, hs_pfsc_image_t *img,
                         char *err, size_t err_size) {
  unsigned char inode_raw[HS_PFS_INODE_SIZE];
  unsigned char *dir_block = NULL;
  uint16_t mode = 0;
  uint64_t size = 0;
  int32_t db0 = -1;
  int rc = -1;

  snprintf(img->nested_name, sizeof(img->nested_name), "pfs_image.dat");
  if(hs_read_exact_at(fd, inode_raw, sizeof(inode_raw),
                      HS_BLOCK_SIZE + 2ULL * HS_PFS_INODE_SIZE,
                      err, err_size) != 0) {
    return -1;
  }
  hs_parse_inode(inode_raw, &mode, NULL, &size, NULL, NULL, &db0);
  if((mode & HS_PFS_INODE_MODE_DIR) == 0 || db0 < 0 ||
     size == 0 || size > HS_BLOCK_SIZE) {
    hs_set_err(err, err_size, "outer PFS root directory is invalid");
    errno = EINVAL;
    return -1;
  }
  dir_block = malloc((size_t)HS_BLOCK_SIZE);
  if(!dir_block) {
    hs_set_err(err, err_size, "out of memory");
    errno = ENOMEM;
    return -1;
  }
  if(hs_read_exact_at(fd, dir_block, (size_t)size,
                      (uint64_t)db0 * HS_BLOCK_SIZE,
                      err, err_size) != 0) {
    goto done;
  }
  for(uint64_t off = 0; off + 16ULL <= size;) {
    uint32_t child_inode = hs_rd32(dir_block + off);
    uint32_t type = hs_rd32(dir_block + off + 4);
    uint32_t name_len = hs_rd32(dir_block + off + 8);
    uint32_t ent_size = hs_rd32(dir_block + off + 12);
    if(ent_size == 0) break;
    if(ent_size < 16 || ent_size > HS_BLOCK_SIZE ||
       name_len == 0 || name_len >= sizeof(img->nested_name) ||
       16ULL + name_len > ent_size || off + ent_size > size) {
      hs_set_err(err, err_size, "outer PFS root directory entry is invalid");
      errno = EINVAL;
      goto done;
    }
    if(child_inode == 3 && type == HS_PFS_DIRENT_TYPE_FILE) {
      memcpy(img->nested_name, dir_block + off + 16, name_len);
      img->nested_name[name_len] = 0;
      if(!path_segment_supported(img->nested_name)) {
        hs_set_err(err, err_size, "nested image name is unsupported");
        errno = EINVAL;
        goto done;
      }
      rc = 0;
      goto done;
    }
    off += ent_size;
  }
  hs_set_err(err, err_size, "outer PFS nested image entry was not found");
  errno = EINVAL;

done:
  free(dir_block);
  return rc;
}

static int
hs_pfsc_open(const char *path, hs_pfsc_image_t *img,
             char *err, size_t err_size) {
  unsigned char *header = NULL;
  unsigned char *table = NULL;
  unsigned char inode_raw[HS_PFS_INODE_SIZE];
  unsigned char pfsc[HS_PFSC_HEADER_SIZE];
  struct stat st;
  uint16_t mode = 0;
  uint32_t flags = 0;
  uint64_t size = 0;
  uint64_t size_comp = 0;
  int32_t db0 = -1;
  uint64_t table_offset = 0;
  uint64_t data_offset = 0;
  uint64_t expected_header = 0;
  int rc = -1;

  memset(img, 0, sizeof(*img));
  img->fd = -1;
  header = malloc((size_t)HS_BLOCK_SIZE);
  if(!header) {
    hs_set_err(err, err_size, "out of memory");
    errno = ENOMEM;
    return -1;
  }
  img->fd = open(path, O_RDONLY);
  if(img->fd < 0) {
    hs_set_err(err, err_size, "open .ffpfsc: %s", strerror(errno));
    goto done;
  }
  if(fstat(img->fd, &st) != 0 || st.st_size < 0) {
    hs_set_err(err, err_size, "stat .ffpfsc: %s", strerror(errno));
    goto done;
  }
  img->outer_size = (uint64_t)st.st_size;
  if(hs_read_exact_at(img->fd, header, (size_t)HS_BLOCK_SIZE, 0,
                      err, err_size) != 0) {
    goto done;
  }
  if(hs_rd64(header + 0x00) != HS_PFS_VERSION_PS5 ||
     hs_rd64(header + 0x08) != HS_PFS_MAGIC ||
     hs_rd32(header + 0x20) != (uint32_t)HS_BLOCK_SIZE) {
    hs_set_err(err, err_size, "not a supported outer PFS container");
    errno = EINVAL;
    goto done;
  }
  if(hs_read_exact_at(img->fd, inode_raw, sizeof(inode_raw),
                      HS_BLOCK_SIZE + 3ULL * HS_PFS_INODE_SIZE,
                      err, err_size) != 0) {
    goto done;
  }
  hs_parse_inode(inode_raw, &mode, &flags, &size, &size_comp, NULL, &db0);
  if((flags & HS_PFS_INODE_FLAG_COMPRESSED) == 0 || db0 < 0 ||
     size == 0 || size_comp == 0) {
    hs_set_err(err, err_size,
               "outer PFS does not contain a compressed nested image");
    errno = EINVAL;
    goto done;
  }
  img->file_start = (uint64_t)db0 * HS_BLOCK_SIZE;
  img->stored_size = size;
  img->nested_size = size_comp;
  if(hs_pfsc_read_nested_name(img->fd, img, err, err_size) != 0) goto done;
  if(hs_read_exact_at(img->fd, pfsc, sizeof(pfsc), img->file_start,
                      err, err_size) != 0) {
    goto done;
  }
  if(hs_rd32(pfsc + 0x00) != HS_PFSC_MAGIC ||
     hs_rd32(pfsc + 0x04) != HS_PFSC_UNK4 ||
     hs_rd32(pfsc + 0x08) != HS_PFSC_UNK8 ||
     hs_rd32(pfsc + 0x0c) != (uint32_t)HS_BLOCK_SIZE ||
     hs_rd64(pfsc + 0x10) != HS_BLOCK_SIZE) {
    hs_set_err(err, err_size, "invalid PFSC header");
    errno = EINVAL;
    goto done;
  }
  table_offset = hs_rd64(pfsc + 0x18);
  data_offset = hs_rd64(pfsc + 0x20);
  img->logical_size = hs_rd64(pfsc + 0x28);
  if(table_offset != HS_PFSC_BLOCK_OFFSETS_OFFSET ||
     data_offset < HS_PFSC_INITIAL_DATA_OFFSET ||
     img->logical_size == 0 ||
     img->logical_size % HS_BLOCK_SIZE != 0 ||
     img->nested_size == 0 ||
     img->nested_size > img->logical_size) {
    hs_set_err(err, err_size, "unsupported PFSC layout");
    errno = EINVAL;
    goto done;
  }
  img->block_count = img->logical_size / HS_BLOCK_SIZE;
  if(img->block_count == 0 ||
     img->block_count > (SIZE_MAX / sizeof(uint64_t)) - 1ULL ||
     hs_pfsc_header_span(img->block_count, &expected_header) != 0 ||
     expected_header != data_offset) {
    hs_set_err(err, err_size, "unsupported PFSC data offset");
    errno = EINVAL;
    goto done;
  }
  img->header_size = data_offset;
  uint64_t table_size = (img->block_count + 1ULL) * HS_PFSC_OFFSET_ENTRY_SIZE;
  if(table_offset + table_size > data_offset ||
     data_offset > img->stored_size ||
     table_size > SIZE_MAX) {
    hs_set_err(err, err_size, "invalid PFSC offset table");
    errno = EINVAL;
    goto done;
  }
  img->offsets = calloc((size_t)(img->block_count + 1ULL), sizeof(*img->offsets));
  table = malloc((size_t)table_size);
  if(!img->offsets || !table) {
    hs_set_err(err, err_size, "out of memory");
    errno = ENOMEM;
    goto done;
  }
  if(hs_read_exact_at(img->fd, table, (size_t)table_size,
                      img->file_start + table_offset,
                      err, err_size) != 0) {
    goto done;
  }
  for(uint64_t i = 0; i <= img->block_count; i++) {
    img->offsets[i] = hs_rd64(table + i * 8ULL);
    if(i > 0 && img->offsets[i] < img->offsets[i - 1]) {
      hs_set_err(err, err_size, "PFSC offsets are not monotonic");
      errno = EINVAL;
      goto done;
    }
  }
  if(img->offsets[0] != data_offset ||
     img->offsets[img->block_count] > img->stored_size) {
    hs_set_err(err, err_size, "PFSC offsets exceed stored size");
    errno = EINVAL;
    goto done;
  }
  rc = 0;

done:
  free(table);
  free(header);
  if(rc != 0) hs_pfsc_close(img);
  return rc;
}

static int
hs_pfsc_decode_block(const hs_pfsc_image_t *img, uint64_t index,
                     unsigned char *decoded, char *err, size_t err_size) {
  if(index >= img->block_count) {
    hs_set_err(err, err_size, "PFSC block outside image");
    errno = EINVAL;
    return -1;
  }
  uint64_t start = img->offsets[index];
  uint64_t end = img->offsets[index + 1ULL];
  if(end < start || end - start > HS_BLOCK_SIZE || end > img->stored_size) {
    hs_set_err(err, err_size, "invalid PFSC block span");
    errno = EINVAL;
    return -1;
  }
  size_t stored_len = (size_t)(end - start);
  if(stored_len == 0) {
    hs_set_err(err, err_size, "empty PFSC block span");
    errno = EINVAL;
    return -1;
  }
  unsigned char *stored = malloc(stored_len);
  if(!stored) {
    hs_set_err(err, err_size, "out of memory");
    errno = ENOMEM;
    return -1;
  }
  if(hs_read_exact_at(img->fd, stored, stored_len, img->file_start + start,
                      err, err_size) != 0) {
    free(stored);
    return -1;
  }
  if(stored_len == (size_t)HS_BLOCK_SIZE) {
    memcpy(decoded, stored, (size_t)HS_BLOCK_SIZE);
    free(stored);
    return 0;
  }
  size_t out_len = tinfl_decompress_mem_to_mem(
      decoded, (size_t)HS_BLOCK_SIZE, stored, stored_len,
      TINFL_FLAG_PARSE_ZLIB_HEADER);
  free(stored);
  if(out_len != (size_t)HS_BLOCK_SIZE) {
    hs_set_err(err, err_size, "decompress PFSC block %llu failed",
               (unsigned long long)index);
    errno = EINVAL;
    return -1;
  }
  return 0;
}

static int
hs_pfsc_read_logical_cb(void *ctx, uint64_t off, void *out, size_t size,
                        char *err, size_t err_size) {
  hs_pfsc_image_t *img = ctx;
  unsigned char *dst = out;
  if(size == 0) return 0;
  if(off >= img->logical_size || size > img->logical_size - off) {
    hs_set_err(err, err_size, "PFSC logical read outside image");
    errno = EINVAL;
    return -1;
  }
  unsigned char *block = malloc((size_t)HS_BLOCK_SIZE);
  if(!block) {
    hs_set_err(err, err_size, "out of memory");
    errno = ENOMEM;
    return -1;
  }
  while(size > 0) {
    uint64_t index = off / HS_BLOCK_SIZE;
    size_t block_off = (size_t)(off % HS_BLOCK_SIZE);
    size_t avail = (size_t)HS_BLOCK_SIZE - block_off;
    size_t n = avail < size ? avail : size;
    if(hs_pfsc_decode_block(img, index, block, err, err_size) != 0) {
      free(block);
      return -1;
    }
    memcpy(dst, block + block_off, n);
    dst += n;
    off += n;
    size -= n;
  }
  free(block);
  return 0;
}

static void
hs_changed_blocks_free(hs_changed_blocks_t *blocks) {
  if(!blocks) return;
  for(size_t i = 0; i < blocks->count; i++) {
    free(blocks->items[i].raw);
    if(blocks->items[i].stored != blocks->items[i].raw) {
      free(blocks->items[i].stored);
    }
  }
  free(blocks->items);
  memset(blocks, 0, sizeof(*blocks));
}

static hs_changed_block_t *
hs_changed_block_get(hs_changed_blocks_t *blocks, const hs_pfsc_image_t *img,
                     uint64_t index, char *err, size_t err_size) {
  for(size_t i = 0; i < blocks->count; i++) {
    if(blocks->items[i].index == index) return &blocks->items[i];
  }
  if(index >= img->block_count) {
    hs_set_err(err, err_size, "PFSC patch outside logical image");
    errno = EINVAL;
    return NULL;
  }
  if(blocks->count == blocks->cap) {
    size_t next = blocks->cap ? blocks->cap * 2 : 8;
    hs_changed_block_t *p = realloc(blocks->items, next * sizeof(*p));
    if(!p) {
      hs_set_err(err, err_size, "out of memory");
      errno = ENOMEM;
      return NULL;
    }
    blocks->items = p;
    blocks->cap = next;
  }
  hs_changed_block_t *b = &blocks->items[blocks->count++];
  memset(b, 0, sizeof(*b));
  b->index = index;
  b->raw = malloc((size_t)HS_BLOCK_SIZE);
  if(!b->raw) {
    hs_set_err(err, err_size, "out of memory");
    errno = ENOMEM;
    return NULL;
  }
  if(hs_pfsc_decode_block(img, index, b->raw, err, err_size) != 0) {
    return NULL;
  }
  return b;
}

static int
hs_changed_block_cmp(const void *a, const void *b) {
  const hs_changed_block_t *aa = a;
  const hs_changed_block_t *bb = b;
  if(aa->index < bb->index) return -1;
  if(aa->index > bb->index) return 1;
  return 0;
}

static int
hs_apply_patches_to_blocks(const hs_pfsc_image_t *img,
                           const hs_patch_list_t *patches,
                           hs_changed_blocks_t *blocks,
                           char *err, size_t err_size) {
  for(size_t i = 0; i < patches->count; i++) {
    uint64_t off = patches->items[i].off;
    size_t left = patches->items[i].size;
    const unsigned char *src = patches->items[i].data;
    if(off >= img->logical_size || left > img->logical_size - off) {
      hs_set_err(err, err_size, "exFAT patch exceeds PFSC logical image");
      errno = EINVAL;
      return -1;
    }
    while(left > 0) {
      uint64_t index = off / HS_BLOCK_SIZE;
      size_t block_off = (size_t)(off % HS_BLOCK_SIZE);
      size_t n = (size_t)HS_BLOCK_SIZE - block_off;
      if(n > left) n = left;
      hs_changed_block_t *b = hs_changed_block_get(blocks, img, index,
                                                   err, err_size);
      if(!b) return -1;
      memcpy(b->raw + block_off, src, n);
      src += n;
      off += n;
      left -= n;
    }
  }
  qsort(blocks->items, blocks->count, sizeof(blocks->items[0]),
        hs_changed_block_cmp);
  return 0;
}

static int
hs_pfsc_compressed_block_meets_gain(size_t comp_len) {
  if(comp_len >= (size_t)HS_BLOCK_SIZE) return 0;
  if(GC_PFSC_THRESHOLD_GAIN <= 0) return 1;
  return ((uint64_t)((size_t)HS_BLOCK_SIZE - comp_len) * 100ULL) >=
         ((uint64_t)HS_BLOCK_SIZE * (uint64_t)GC_PFSC_THRESHOLD_GAIN);
}

static int
hs_compress_changed_blocks(hs_changed_blocks_t *blocks,
                           char *err, size_t err_size) {
  for(size_t i = 0; i < blocks->count; i++) {
    unsigned char *comp = malloc((size_t)HS_BLOCK_SIZE);
    if(!comp) {
      hs_set_err(err, err_size, "out of memory");
      errno = ENOMEM;
      return -1;
    }
    uLongf comp_len = (uLongf)((size_t)HS_BLOCK_SIZE - 1U);
    int zrc = compress2(comp, &comp_len, blocks->items[i].raw,
                        (uLong)HS_BLOCK_SIZE, GC_PFSC_ZLIB_LEVEL);
    if(zrc == Z_OK && hs_pfsc_compressed_block_meets_gain((size_t)comp_len)) {
      blocks->items[i].stored = comp;
      blocks->items[i].stored_len = (size_t)comp_len;
    } else {
      free(comp);
      blocks->items[i].stored = blocks->items[i].raw;
      blocks->items[i].stored_len = (size_t)HS_BLOCK_SIZE;
    }
  }
  return 0;
}

static int
hs_write_pfsc_header(int fd, const hs_pfsc_image_t *img,
                     const uint64_t *offsets,
                     char *err, size_t err_size) {
  unsigned char *header = calloc(1, (size_t)img->header_size);
  if(!header) {
    hs_set_err(err, err_size, "out of memory");
    errno = ENOMEM;
    return -1;
  }
  hs_le32(header + 0x00, HS_PFSC_MAGIC);
  hs_le32(header + 0x04, HS_PFSC_UNK4);
  hs_le32(header + 0x08, HS_PFSC_UNK8);
  hs_le32(header + 0x0c, (uint32_t)HS_BLOCK_SIZE);
  hs_le64(header + 0x10, HS_BLOCK_SIZE);
  hs_le64(header + 0x18, HS_PFSC_BLOCK_OFFSETS_OFFSET);
  hs_le64(header + 0x20, img->header_size);
  hs_le64(header + 0x28, img->logical_size);
  for(uint64_t i = 0; i <= img->block_count; i++) {
    hs_le64(header + HS_PFSC_BLOCK_OFFSETS_OFFSET + i * 8ULL, offsets[i]);
  }
  int rc = hs_write_exact_at(fd, header, (size_t)img->header_size,
                             img->file_start, err, err_size);
  free(header);
  return rc;
}

static int
hs_update_outer_metadata(int fd, const hs_pfsc_image_t *img,
                         uint64_t new_stored_size,
                         char *err, size_t err_size) {
  uint64_t file_blocks = hs_ceil_div_u64(new_stored_size, HS_BLOCK_SIZE);
  if(file_blocks == 0) file_blocks = 1;
  uint64_t final_ndblock = 6ULL + file_blocks;
  uint64_t final_size = final_ndblock * HS_BLOCK_SIZE;
  unsigned char raw8[8];
  unsigned char raw4[4];
  if(ftruncate(fd, (off_t)final_size) != 0) {
    hs_set_err(err, err_size, "truncate hot-swap image: %s", strerror(errno));
    return -1;
  }
  hs_le64(raw8, final_ndblock);
  if(hs_write_exact_at(fd, raw8, sizeof(raw8), 0x38, err, err_size) != 0) {
    return -1;
  }
  uint64_t inode_off = HS_BLOCK_SIZE + 3ULL * HS_PFS_INODE_SIZE;
  hs_le64(raw8, new_stored_size);
  if(hs_write_exact_at(fd, raw8, sizeof(raw8), inode_off + 0x08,
                       err, err_size) != 0) {
    return -1;
  }
  hs_le64(raw8, img->nested_size);
  if(hs_write_exact_at(fd, raw8, sizeof(raw8), inode_off + 0x10,
                       err, err_size) != 0) {
    return -1;
  }
  hs_le32(raw4, (uint32_t)file_blocks);
  if(hs_write_exact_at(fd, raw4, sizeof(raw4), inode_off + 0x60,
                       err, err_size) != 0) {
    return -1;
  }
  return 0;
}

static int
hs_temp_path_for(const char *path, char *out, size_t out_size) {
  char parent[1024];
  char base[256];
  if(path_parent_base(path, parent, sizeof(parent), base, sizeof(base)) != 0) {
    return -1;
  }
  int n = snprintf(out, out_size, "%s/.%s.gc-ampr-patch.tmp", parent, base);
  if(n < 0 || (size_t)n >= out_size) {
    errno = ENAMETOOLONG;
    return -1;
  }
  return 0;
}

static int
hs_fsync_parent_best_effort(const char *path) {
  char parent[1024];
  char base[256];
  if(path_parent_base(path, parent, sizeof(parent), base, sizeof(base)) != 0) {
    return -1;
  }
  int fd = open(parent, O_RDONLY);
  if(fd < 0) return -1;
  fsync(fd);
  close(fd);
  return 0;
}

static hs_changed_block_t *
hs_changed_at(hs_changed_blocks_t *blocks, size_t *cursor, uint64_t index) {
  while(*cursor < blocks->count && blocks->items[*cursor].index < index) {
    (*cursor)++;
  }
  if(*cursor < blocks->count && blocks->items[*cursor].index == index) {
    return &blocks->items[*cursor];
  }
  return NULL;
}

static hs_changed_block_t *
hs_changed_find(const hs_changed_blocks_t *blocks, uint64_t index) {
  if(!blocks) return NULL;
  for(size_t i = 0; i < blocks->count; i++) {
    if(blocks->items[i].index == index) return &blocks->items[i];
  }
  return NULL;
}

static int
hs_pfsc_final_size(uint64_t stored_size, uint64_t *out,
                   char *err, size_t err_size) {
  uint64_t file_blocks = hs_ceil_div_u64(stored_size, HS_BLOCK_SIZE);
  if(file_blocks == 0) file_blocks = 1;
  if(file_blocks > UINT64_MAX / HS_BLOCK_SIZE - 6ULL) {
    hs_set_err(err, err_size, "hot-swap PFSC final size overflow");
    errno = EOVERFLOW;
    return -1;
  }
  *out = (6ULL + file_blocks) * HS_BLOCK_SIZE;
  return 0;
}

static int
hs_prepare_pfsc_rewrite(const hs_pfsc_image_t *img,
                        hs_changed_blocks_t *blocks,
                        uint64_t **new_offsets_out,
                        unsigned char **changed_out,
                        char *err, size_t err_size) {
  uint64_t *new_offsets = NULL;
  unsigned char *changed = NULL;

  if(new_offsets_out) *new_offsets_out = NULL;
  if(changed_out) *changed_out = NULL;
  if(blocks->count == 0) {
    hs_set_err(err, err_size, "no PFSC blocks changed");
    errno = EINVAL;
    return -1;
  }
  new_offsets = calloc((size_t)(img->block_count + 1ULL),
                       sizeof(*new_offsets));
  changed = calloc((size_t)img->block_count, 1);
  if(!new_offsets || !changed) {
    hs_set_err(err, err_size, "out of memory");
    errno = ENOMEM;
    goto fail;
  }
  if(hs_compress_changed_blocks(blocks, err, err_size) != 0) goto fail;
  for(size_t i = 0; i < blocks->count; i++) {
    if(blocks->items[i].index >= img->block_count) {
      hs_set_err(err, err_size, "bad changed PFSC block");
      errno = EINVAL;
      goto fail;
    }
    changed[blocks->items[i].index] = 1;
  }
  new_offsets[0] = img->header_size;
  size_t changed_cursor = 0;
  for(uint64_t i = 0; i < img->block_count; i++) {
    hs_changed_block_t *b = hs_changed_at(blocks, &changed_cursor, i);
    uint64_t old_len = img->offsets[i + 1ULL] - img->offsets[i];
    uint64_t new_len = b ? (uint64_t)b->stored_len : old_len;
    if(new_len > UINT64_MAX - new_offsets[i]) {
      hs_set_err(err, err_size, "hot-swap PFSC size overflow");
      errno = EOVERFLOW;
      goto fail;
    }
    new_offsets[i + 1ULL] = new_offsets[i] + new_len;
  }
  *new_offsets_out = new_offsets;
  *changed_out = changed;
  return 0;

fail:
  free(new_offsets);
  free(changed);
  return -1;
}

static hs_pfsc_apply_strategy_t
hs_pfsc_choose_apply_strategy(const hs_pfsc_image_t *img,
                              const uint64_t *new_offsets,
                              const unsigned char *changed) {
  int saw_left = 0;
  int saw_right = 0;
  for(uint64_t i = 0; i < img->block_count; i++) {
    if(changed[i]) continue;
    uint64_t old_start = img->offsets[i];
    uint64_t old_len = img->offsets[i + 1ULL] - img->offsets[i];
    uint64_t new_start = new_offsets[i];
    uint64_t new_len = new_offsets[i + 1ULL] - new_offsets[i];
    if(new_len != old_len) return HS_PFSC_APPLY_MIXED;
    if(new_start < old_start) saw_left = 1;
    if(new_start > old_start) saw_right = 1;
    if(saw_left && saw_right) return HS_PFSC_APPLY_MIXED;
  }
  if(saw_left) return HS_PFSC_APPLY_MOVE_LEFT;
  if(saw_right) return HS_PFSC_APPLY_MOVE_RIGHT;
  return HS_PFSC_APPLY_OVERWRITE;
}

static int
hs_copy_fd_range_overlap_progress(int fd, uint64_t src_off, uint64_t dst_off,
                                  uint64_t size, int backwards,
                                  uint64_t run_blocks,
                                  hs_rebuild_progress_t *progress,
                                  const char *current,
                                  char *err, size_t err_size) {
  if(size == 0 || src_off == dst_off) {
    hs_rebuild_advance_progress(progress, run_blocks, current);
    return 0;
  }
  unsigned char *buf = malloc(HS_IO_CHUNK);
  if(!buf) {
    hs_set_err(err, err_size, "out of memory");
    errno = ENOMEM;
    return -1;
  }
  uint64_t done = 0;
  uint64_t base_blocks = progress ? progress->done_blocks : 0;
  if(backwards) {
    uint64_t remaining = size;
    while(remaining > 0) {
      size_t chunk = remaining > HS_IO_CHUNK ? HS_IO_CHUNK : (size_t)remaining;
      uint64_t pos = remaining - (uint64_t)chunk;
      if(hs_read_exact_at(fd, buf, chunk, src_off + pos,
                          err, err_size) != 0 ||
         hs_write_exact_at(fd, buf, chunk, dst_off + pos,
                           err, err_size) != 0) {
        free(buf);
        return -1;
      }
      remaining = pos;
      done = size - remaining;
      if(progress && size > 0 && run_blocks > 0) {
        uint64_t run_done = done >= size ? run_blocks :
            (uint64_t)(((unsigned __int128)done * run_blocks) / size);
        hs_rebuild_report_progress(progress, base_blocks + run_done, current);
      }
    }
  } else {
    while(done < size) {
      size_t chunk = size - done > HS_IO_CHUNK ?
          HS_IO_CHUNK : (size_t)(size - done);
      if(hs_read_exact_at(fd, buf, chunk, src_off + done,
                          err, err_size) != 0 ||
         hs_write_exact_at(fd, buf, chunk, dst_off + done,
                           err, err_size) != 0) {
        free(buf);
        return -1;
      }
      done += chunk;
      if(progress && size > 0 && run_blocks > 0) {
        uint64_t run_done = done >= size ? run_blocks :
            (uint64_t)(((unsigned __int128)done * run_blocks) / size);
        hs_rebuild_report_progress(progress, base_blocks + run_done, current);
      }
    }
  }
  free(buf);
  hs_rebuild_report_progress(progress, base_blocks + run_blocks, current);
  return 0;
}

static int
hs_write_changed_pfsc_block(int fd, const hs_pfsc_image_t *img,
                            const uint64_t *new_offsets,
                            const hs_changed_block_t *b,
                            char *err, size_t err_size) {
  if(!b) {
    hs_set_err(err, err_size, "missing changed PFSC block");
    errno = EINVAL;
    return -1;
  }
  return hs_write_exact_at(fd, b->stored, b->stored_len,
                           img->file_start + new_offsets[b->index],
                           err, err_size);
}

static int
hs_apply_pfsc_in_place(const char *path, const hs_pfsc_image_t *img,
                       hs_changed_blocks_t *blocks,
                       const uint64_t *new_offsets,
                       const unsigned char *changed,
                       hs_pfsc_apply_strategy_t strategy,
                       int *modified,
                       char *err, size_t err_size) {
  int fd = -1;
  int rc = -1;
  uint64_t final_size = 0;
  hs_rebuild_progress_t progress = {0};

  if(modified) *modified = 0;
  if(strategy == HS_PFSC_APPLY_MIXED) {
    hs_set_err(err, err_size, "mixed PFSC movement requires copy-replace");
    errno = EINVAL;
    return -1;
  }
  if(hs_pfsc_final_size(new_offsets[img->block_count], &final_size,
                        err, err_size) != 0) {
    return -1;
  }
  fd = open(path, O_RDWR);
  if(fd < 0) {
    hs_set_err(err, err_size, "open .ffpfsc for in-place AMPR update: %s",
               strerror(errno));
    return -1;
  }
  if(final_size > img->outer_size) {
    if(ftruncate(fd, (off_t)final_size) != 0) {
      hs_set_err(err, err_size, "extend .ffpfsc for AMPR update: %s",
                 strerror(errno));
      goto done;
    }
    if(modified) *modified = 1;
  }

  progress.total_blocks = img->block_count;
  hs_rebuild_report_progress(&progress, 0,
                             "Updating compressed image in place");
  if(strategy == HS_PFSC_APPLY_MOVE_RIGHT) {
    uint64_t remaining = img->block_count;
    while(remaining > 0) {
      uint64_t i = remaining - 1ULL;
      if(changed[i]) {
        hs_changed_block_t *b = hs_changed_find(blocks, i);
        if(modified) *modified = 1;
        if(hs_write_changed_pfsc_block(fd, img, new_offsets, b,
                                       err, err_size) != 0) {
          goto done;
        }
        remaining--;
        hs_rebuild_advance_progress(&progress, 1,
                                    "Updating compressed image in place");
        continue;
      }

      uint64_t old_start = img->offsets[i];
      uint64_t new_start = new_offsets[i];
      uint64_t delta = new_start - old_start;
      uint64_t run_start = i;
      while(run_start > 0) {
        uint64_t prev = run_start - 1ULL;
        if(changed[prev]) break;
        uint64_t prev_old_start = img->offsets[prev];
        uint64_t prev_old_len = img->offsets[prev + 1ULL] - img->offsets[prev];
        uint64_t prev_new_start = new_offsets[prev];
        uint64_t prev_new_len = new_offsets[prev + 1ULL] - new_offsets[prev];
        if(prev_new_len != prev_old_len ||
           prev_new_start < prev_old_start ||
           prev_new_start - prev_old_start != delta) {
          break;
        }
        run_start = prev;
      }

      uint64_t old_range_start = img->offsets[run_start];
      uint64_t old_range_end = img->offsets[remaining];
      uint64_t new_range_start = new_offsets[run_start];
      uint64_t run_size = old_range_end - old_range_start;
      uint64_t run_blocks = remaining - run_start;
      if(delta != 0) {
        if(modified) *modified = 1;
        if(hs_copy_fd_range_overlap_progress(
               fd, img->file_start + old_range_start,
               img->file_start + new_range_start, run_size, 1,
               run_blocks, &progress,
               "Updating compressed image in place",
               err, err_size) != 0) {
          goto done;
        }
      } else {
        hs_rebuild_advance_progress(&progress, run_blocks,
                                    "Updating compressed image in place");
      }
      remaining = run_start;
    }
  } else {
    for(uint64_t block = 0; block < img->block_count;) {
      if(changed[block]) {
        hs_changed_block_t *b = hs_changed_find(blocks, block);
        if(modified) *modified = 1;
        if(hs_write_changed_pfsc_block(fd, img, new_offsets, b,
                                       err, err_size) != 0) {
          goto done;
        }
        block++;
        hs_rebuild_advance_progress(&progress, 1,
                                    "Updating compressed image in place");
        continue;
      }

      uint64_t old_start = img->offsets[block];
      uint64_t new_start = new_offsets[block];
      uint64_t delta = old_start - new_start;
      uint64_t run_start = block;
      while(block < img->block_count && !changed[block]) {
        uint64_t cur_old_start = img->offsets[block];
        uint64_t cur_old_len = img->offsets[block + 1ULL] -
                               img->offsets[block];
        uint64_t cur_new_start = new_offsets[block];
        uint64_t cur_new_len = new_offsets[block + 1ULL] -
                               new_offsets[block];
        if(cur_new_len != cur_old_len ||
           cur_new_start > cur_old_start ||
           cur_old_start - cur_new_start != delta) {
          break;
        }
        block++;
      }
      uint64_t old_range_start = img->offsets[run_start];
      uint64_t old_range_end = img->offsets[block];
      uint64_t new_range_start = new_offsets[run_start];
      uint64_t run_size = old_range_end - old_range_start;
      uint64_t run_blocks = block - run_start;
      if(delta != 0) {
        if(modified) *modified = 1;
        if(hs_copy_fd_range_overlap_progress(
               fd, img->file_start + old_range_start,
               img->file_start + new_range_start, run_size, 0,
               run_blocks, &progress,
               "Updating compressed image in place",
               err, err_size) != 0) {
          goto done;
        }
      } else {
        hs_rebuild_advance_progress(&progress, run_blocks,
                                    "Updating compressed image in place");
      }
    }
  }

  hs_rebuild_report_progress(&progress, img->block_count,
                             "Finalizing compressed image");
  if(modified) *modified = 1;
  if(hs_write_pfsc_header(fd, img, new_offsets, err, err_size) != 0 ||
     hs_update_outer_metadata(fd, img, new_offsets[img->block_count],
                              err, err_size) != 0) {
    goto done;
  }
  if(fsync(fd) != 0) {
    hs_set_err(err, err_size, "sync in-place AMPR update: %s",
               strerror(errno));
    goto done;
  }
  hs_rebuild_report_progress(&progress, img->block_count,
                             "Compressed image update complete");
  rc = 0;

done:
  if(fd >= 0) close(fd);
  return rc;
}

static int
hs_rebuild_pfsc_copy_replace(const char *path, hs_pfsc_image_t *img,
                             hs_changed_blocks_t *blocks,
                             const uint64_t *new_offsets,
                             const unsigned char *changed,
                             pfs_ampr_hotswap_info_t *info,
                             char *err, size_t err_size) {
  char temp_path[1024] = {0};
  char parent[1024];
  char base[256];
  int temp_fd = -1;
  int rc = -1;
  struct stat st;
  hs_rebuild_progress_t progress = {0};

  if(hs_temp_path_for(path, temp_path, sizeof(temp_path)) != 0) {
    hs_set_err(err, err_size, "hot-swap temp path too long");
    goto done;
  }
  if(path_parent_base(path, parent, sizeof(parent), base, sizeof(base)) != 0) {
    hs_set_err(err, err_size, "hot-swap parent path too long");
    goto done;
  }
  uint64_t final_size = 0;
  if(hs_pfsc_final_size(new_offsets[img->block_count], &final_size,
                        err, err_size) != 0) {
    goto done;
  }
  struct statvfs vfs;
  if(statvfs(parent, &vfs) == 0) {
    uint64_t free_bytes = (uint64_t)vfs.f_bavail * (uint64_t)vfs.f_frsize;
    if(free_bytes < final_size) {
      hs_set_err(err, err_size,
                 "not enough free storage for compressed AMPR hot-swap; free %llu more bytes",
                 (unsigned long long)(final_size - free_bytes));
      errno = ENOSPC;
      goto done;
    }
  }
  if(fstat(img->fd, &st) != 0) {
    hs_set_err(err, err_size, "stat .ffpfsc: %s", strerror(errno));
    goto done;
  }
  unlink(temp_path);
  temp_fd = open(temp_path, O_RDWR | O_CREAT | O_EXCL,
                 st.st_mode ? (st.st_mode & 0777) : 0666);
  if(temp_fd < 0) {
    hs_set_err(err, err_size, "create hot-swap temp: %s", strerror(errno));
    goto done;
  }
  if(hs_copy_fd_range(img->fd, temp_fd, img->file_start,
                      0, 0, err, err_size) != 0) {
    goto done;
  }

  uint64_t block = 0;
  size_t changed_cursor = 0;
  progress.total_blocks = img->block_count;
  hs_rebuild_report_progress(&progress, 0, "Rebuilding compressed image");
  while(block < img->block_count) {
    hs_changed_block_t *b = hs_changed_at(blocks, &changed_cursor, block);
    if(!b) {
      uint64_t run_start = block;
      while(block < img->block_count && !changed[block]) block++;
      uint64_t old_start = img->offsets[run_start];
      uint64_t old_end = img->offsets[block];
      uint64_t new_start = new_offsets[run_start];
      if(old_end > old_start &&
         hs_copy_fd_range_rebuild_progress(img->fd, temp_fd,
                                           old_end - old_start,
                                           img->file_start + old_start,
                                           img->file_start + new_start,
                                           run_start, block - run_start,
                                           &progress,
                                           "Rebuilding compressed image",
                                           err, err_size) != 0) {
        goto done;
      }
      hs_rebuild_report_progress(&progress, block,
                                 "Rebuilding compressed image");
      continue;
    }
    if(hs_write_exact_at(temp_fd, b->stored, b->stored_len,
                         img->file_start + new_offsets[block],
                         err, err_size) != 0) {
      goto done;
    }
    block++;
    hs_rebuild_advance_progress(&progress, 1,
                                "Rebuilding compressed image");
  }
  hs_rebuild_report_progress(&progress, img->block_count,
                             "Finalizing compressed image");
  if(hs_write_pfsc_header(temp_fd, img, new_offsets, err, err_size) != 0 ||
     hs_update_outer_metadata(temp_fd, img, new_offsets[img->block_count],
                              err, err_size) != 0) {
    goto done;
  }
  if(fsync(temp_fd) != 0) {
    hs_set_err(err, err_size, "sync hot-swap temp: %s", strerror(errno));
    goto done;
  }
  if(close(temp_fd) != 0) {
    temp_fd = -1;
    hs_set_err(err, err_size, "close hot-swap temp: %s", strerror(errno));
    goto done;
  }
  temp_fd = -1;
  hs_pfsc_close(img);
  if(rename(temp_path, path) != 0) {
    hs_set_err(err, err_size, "replace hot-swapped image: %s", strerror(errno));
    goto done;
  }
  hs_fsync_parent_best_effort(path);
  if(info) info->changed_blocks = blocks->count;
  rc = 0;

done:
  if(temp_fd >= 0) close(temp_fd);
  if(rc != 0 && temp_path[0]) unlink(temp_path);
  return rc;
}

static int
hs_rebuild_pfsc_with_changed_blocks(const char *path, hs_pfsc_image_t *img,
                                    hs_changed_blocks_t *blocks,
                                    pfs_ampr_hotswap_info_t *info,
                                    char *err, size_t err_size) {
  uint64_t *new_offsets = NULL;
  unsigned char *changed = NULL;
  hs_pfsc_apply_strategy_t strategy = HS_PFSC_APPLY_MIXED;
  int modified = 0;
  int rc = -1;

  if(hs_prepare_pfsc_rewrite(img, blocks, &new_offsets, &changed,
                             err, err_size) != 0) {
    goto done;
  }
  strategy = hs_pfsc_choose_apply_strategy(img, new_offsets, changed);
  if(strategy != HS_PFSC_APPLY_MIXED) {
    if(hs_apply_pfsc_in_place(path, img, blocks, new_offsets, changed,
                              strategy, &modified, err, err_size) == 0) {
      if(info) info->changed_blocks = blocks->count;
      hs_pfsc_close(img);
      rc = 0;
      goto done;
    }
    if(modified) goto done;
  }

  if(hs_rebuild_pfsc_copy_replace(path, img, blocks, new_offsets, changed,
                                  info, err, err_size) == 0) {
    rc = 0;
  }

done:
  free(new_offsets);
  free(changed);
  return rc;
}

int
pfs_ampr_index_ffpfsc_exfat(const char *ffpfsc_path,
                            pfs_ampr_hotswap_info_t *info,
                            char *err, size_t err_size) {
  hs_pfsc_image_t img;
  hs_exfat_t ex;
  hs_patch_list_t patches = {0};
  hs_changed_blocks_t blocks = {0};
  int rc = -1;

  memset(&img, 0, sizeof(img));
  img.fd = -1;
  if(info) memset(info, 0, sizeof(*info));
  if(!ffpfsc_path || !ffpfsc_path[0]) {
    hs_set_err(err, err_size, "bad AMPR index input");
    errno = EINVAL;
    return -1;
  }
  if(hs_pfsc_open(ffpfsc_path, &img, err, err_size) != 0 ||
     hs_exfat_open_reader(hs_pfsc_read_logical_cb, &img, &ex,
                          err, err_size) != 0 ||
     hs_build_exfat_index_patches(&ex, &patches, info, err, err_size) != 0 ||
     hs_apply_patches_to_blocks(&img, &patches, &blocks,
                                err, err_size) != 0 ||
     hs_rebuild_pfsc_with_changed_blocks(ffpfsc_path, &img, &blocks, info,
                                         err, err_size) != 0) {
    goto done;
  }
  if(hs_pfsc_open(ffpfsc_path, &img, err, err_size) != 0 ||
     hs_exfat_open_reader(hs_pfsc_read_logical_cb, &img, &ex,
                          err, err_size) != 0 ||
     hs_verify_exfat_index(&ex, err, err_size) != 0) {
    goto done;
  }
  if(info) {
    snprintf(info->mode, sizeof(info->mode), "%s", "ffpfsc-index-tail");
  }
  rc = 0;

done:
  hs_pfsc_close(&img);
  hs_patch_list_free(&patches);
  hs_changed_blocks_free(&blocks);
  return rc;
}

int
pfs_ampr_hotswap_ffpfsc_exfat(const char *ffpfsc_path,
                              const char *replacement_path,
                              pfs_ampr_hotswap_info_t *info,
                              char *err, size_t err_size) {
  hs_pfsc_image_t img;
  hs_exfat_t ex;
  hs_patch_list_t patches = {0};
  hs_changed_blocks_t blocks = {0};
  unsigned char *replacement = NULL;
  size_t replacement_size = 0;
  int rc = -1;

  memset(&img, 0, sizeof(img));
  img.fd = -1;
  if(info) memset(info, 0, sizeof(*info));
  if(!ffpfsc_path || !replacement_path) {
    hs_set_err(err, err_size, "bad AMPR hot-swap input");
    errno = EINVAL;
    return -1;
  }
  if(hs_file_to_memory(replacement_path, &replacement, &replacement_size,
                       err, err_size) != 0 ||
     hs_pfsc_open(ffpfsc_path, &img, err, err_size) != 0 ||
     hs_exfat_open_reader(hs_pfsc_read_logical_cb, &img, &ex,
                          err, err_size) != 0 ||
     hs_build_exfat_ampr_patches(&ex, replacement, replacement_size,
                                 &patches, info, err, err_size) != 0 ||
     hs_apply_patches_to_blocks(&img, &patches, &blocks,
                                err, err_size) != 0 ||
     hs_rebuild_pfsc_with_changed_blocks(ffpfsc_path, &img, &blocks, info,
                                         err, err_size) != 0) {
    goto done;
  }
  if(hs_pfsc_open(ffpfsc_path, &img, err, err_size) != 0 ||
     hs_exfat_open_reader(hs_pfsc_read_logical_cb, &img, &ex,
                          err, err_size) != 0 ||
     hs_verify_exfat_ampr(&ex, replacement, replacement_size,
                          err, err_size) != 0) {
    goto done;
  }
  if(info) {
    snprintf(info->mode, sizeof(info->mode), "%s",
             info->tail_migrated ? "ffpfsc-tail" : "ffpfsc-patch");
  }
  rc = 0;

done:
  hs_pfsc_close(&img);
  free(replacement);
  hs_patch_list_free(&patches);
  hs_changed_blocks_free(&blocks);
  return rc;
}

static int
hs_build_pfs_ampr_patches(const char *ffpfsc_path,
                          const unsigned char *replacement,
                          size_t replacement_size,
                          hs_patch_list_t *patches,
                          pfs_ampr_hotswap_info_t *info,
                          char *err, size_t err_size) {
  static const char *targets[] = {
    "fakelib/libSceAmpr.sprx",
    "fakelib/libSceAmpr.prx",
    "sce_module/libSceAmpr.sprx",
    "sce_module/libSceAmpr.prx",
  };
  pfs_nested_pfs_file_span_t span;
  unsigned char raw8[8];

  if(pfs_decompress_find_nested_pfs_file_span(
         ffpfsc_path, targets, sizeof(targets) / sizeof(targets[0]),
         &span, err, err_size) != 0) {
    return -1;
  }
  if(replacement_size > span.allocated_size) {
    hs_set_err(err, err_size,
               "replacement AMPR binary is larger than the nested PFS file allocation; recompress with AMPR hot-swap layout");
    errno = ENOSPC;
    return -1;
  }
  if(hs_patch_append(patches, span.data_offset, replacement,
                     replacement_size, err, err_size) != 0) {
    return -1;
  }
  hs_le64(raw8, (uint64_t)replacement_size);
  if(hs_patch_append(patches, span.inode_offset + 0x08, raw8, sizeof(raw8),
                     err, err_size) != 0 ||
     hs_patch_append(patches, span.inode_offset + 0x10, raw8, sizeof(raw8),
                     err, err_size) != 0) {
    return -1;
  }
  if(info) {
    snprintf(info->logical_path, sizeof(info->logical_path), "%s", span.rel);
    info->old_size = span.size;
    info->new_size = replacement_size;
    info->old_first_cluster = span.first_block;
    info->new_first_cluster = span.first_block;
    info->allocated_clusters = span.blocks;
    info->tail_migrated = 0;
  }
  return 0;
}

int
pfs_ampr_hotswap_ffpfsc_pfs(const char *ffpfsc_path,
                            const char *replacement_path,
                            pfs_ampr_hotswap_info_t *info,
                            char *err, size_t err_size) {
  hs_pfsc_image_t img;
  hs_patch_list_t patches = {0};
  hs_changed_blocks_t blocks = {0};
  unsigned char *replacement = NULL;
  unsigned char *actual = NULL;
  size_t replacement_size = 0;
  size_t actual_size = 0;
  char logical_path[128] = {0};
  int rc = -1;

  memset(&img, 0, sizeof(img));
  img.fd = -1;
  if(info) memset(info, 0, sizeof(*info));
  if(!ffpfsc_path || !replacement_path) {
    hs_set_err(err, err_size, "bad AMPR hot-swap input");
    errno = EINVAL;
    return -1;
  }
  if(hs_file_to_memory(replacement_path, &replacement, &replacement_size,
                       err, err_size) != 0 ||
     hs_pfsc_open(ffpfsc_path, &img, err, err_size) != 0 ||
     hs_build_pfs_ampr_patches(ffpfsc_path, replacement, replacement_size,
                               &patches, info, err, err_size) != 0 ||
     hs_apply_patches_to_blocks(&img, &patches, &blocks,
                                err, err_size) != 0 ||
     hs_rebuild_pfsc_with_changed_blocks(ffpfsc_path, &img, &blocks, info,
                                         err, err_size) != 0) {
    goto done;
  }
  if(info && info->logical_path[0]) {
    snprintf(logical_path, sizeof(logical_path), "%s", info->logical_path);
  } else {
    snprintf(logical_path, sizeof(logical_path), "%s",
             "fakelib/libSceAmpr.sprx");
  }
  if(pfs_decompress_read_nested_pfs_file(ffpfsc_path, logical_path,
                                         replacement_size,
                                         &actual, &actual_size,
                                         err, err_size) != 0) {
    goto done;
  }
  if(actual_size != replacement_size ||
     memcmp(actual, replacement, replacement_size) != 0) {
    hs_set_err(err, err_size, "nested PFS AMPR verification failed");
    errno = EIO;
    goto done;
  }
  if(info) {
    snprintf(info->mode, sizeof(info->mode), "%s", "ffpfsc-patch");
  }
  rc = 0;

done:
  hs_pfsc_close(&img);
  free(replacement);
  free(actual);
  hs_patch_list_free(&patches);
  hs_changed_blocks_free(&blocks);
  return rc;
}
