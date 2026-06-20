/*
 * Game Compressor - AMPR APR resolver index builder.
 */

#include "ampr_index.h"

#include <errno.h>
#include <fcntl.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

#define AMPR_INDEX_MAGIC "AMPRIDX3"
#define AMPR_INDEX_VERSION 3U
#define AMPR_INDEX_RECORD_SIZE 24U
#define AMPR_INDEX_HASH_SLOT_SIZE 16U
#define AMPR_INDEX_DUPLICATE_HASH_FLAG 1U

typedef struct ampr_row {
  uint64_t size;
  int64_t mtime;
  char path[1024];
} ampr_row_t;

typedef struct ampr_hash_slot {
  uint64_t hash;
  uint32_t index_plus_one;
  uint32_t flags;
} ampr_hash_slot_t;

typedef struct ampr_mem_buf {
  unsigned char *data;
  size_t len;
  size_t cap;
} ampr_mem_buf_t;

static void
ampr_set_err(char *err, size_t err_size, const char *fmt, ...) {
  if(!err || err_size == 0 || err[0]) return;
  va_list ap;
  va_start(ap, fmt);
  vsnprintf(err, err_size, fmt, ap);
  va_end(ap);
}

static int
ampr_ascii_tolower(int ch) {
  return ch >= 'A' && ch <= 'Z' ? ch + 32 : ch;
}

static int
ampr_ascii_casecmp(const char *a, const char *b) {
  const unsigned char *pa = (const unsigned char *)(a ? a : "");
  const unsigned char *pb = (const unsigned char *)(b ? b : "");
  while(*pa || *pb) {
    int ca = ampr_ascii_tolower(*pa);
    int cb = ampr_ascii_tolower(*pb);
    if(ca != cb) return ca - cb;
    if(*pa) pa++;
    if(*pb) pb++;
  }
  return 0;
}

static int
ampr_ascii_eq_ci(const char *a, const char *b) {
  return ampr_ascii_casecmp(a, b) == 0;
}

static int
ampr_rel_is_root_index(const char *rel) {
  return ampr_ascii_eq_ci(rel, "ampr_emu.index") ||
         ampr_ascii_eq_ci(rel, "ampr_emu.index.tmp");
}

static int
ampr_rel_is_marker(const char *rel) {
  return ampr_ascii_eq_ci(rel, "fakelib/libSceAmpr.sprx") ||
         ampr_ascii_eq_ci(rel, "fakelib/libSceAmpr.prx") ||
         ampr_ascii_eq_ci(rel, "sce_module/libSceAmpr.sprx") ||
         ampr_ascii_eq_ci(rel, "sce_module/libSceAmpr.prx");
}

static int
ampr_path_supported(const char *path) {
  const unsigned char *p = (const unsigned char *)(path ? path : "");
  if(!*p) return 0;
  for(; *p; p++) {
    if(*p == '\t' || *p == '\n' || *p == '\r') return 0;
  }
  return 1;
}

static uint64_t
ampr_fnv1a64_path_hash(const char *path) {
  uint64_t h = 1469598103934665603ULL;
  const unsigned char *p = (const unsigned char *)(path ? path : "");
  for(; *p; p++) {
    h ^= (uint64_t)(unsigned char)ampr_ascii_tolower(*p);
    h *= 1099511628211ULL;
  }
  return h ? h : 1;
}

static uint32_t
ampr_hash_slot_count(size_t entry_count) {
  uint32_t slots = 2;
  uint64_t target;
  if(entry_count == 0 || entry_count > 0x40000000ULL) return 0;
  target = (uint64_t)entry_count * 2ULL;
  while((uint64_t)slots < target) slots <<= 1;
  return slots;
}

static int
ampr_row_cmp(const void *a, const void *b) {
  const ampr_row_t *ra = (const ampr_row_t *)a;
  const ampr_row_t *rb = (const ampr_row_t *)b;
  int rc = ampr_ascii_casecmp(ra->path, rb->path);
  if(rc != 0) return rc;
  return strcmp(ra->path, rb->path);
}

static int
ampr_append_row(ampr_row_t **rows, size_t *count, size_t *cap,
                uint64_t size, int64_t mtime, const char *path,
                char *err, size_t err_size) {
  ampr_row_t *row;
  if(!ampr_path_supported(path)) {
    ampr_set_err(err, err_size, "AMPR index path contains unsupported whitespace: %s",
                 path ? path : "");
    errno = EINVAL;
    return -1;
  }
  if(*count == *cap) {
    size_t next = *cap ? *cap * 2 : 256;
    ampr_row_t *p = (ampr_row_t *)realloc(*rows, next * sizeof(*p));
    if(!p) {
      ampr_set_err(err, err_size, "out of memory");
      errno = ENOMEM;
      return -1;
    }
    *rows = p;
    *cap = next;
  }
  row = &(*rows)[(*count)++];
  memset(row, 0, sizeof(*row));
  row->size = size;
  row->mtime = mtime;
  int n = snprintf(row->path, sizeof(row->path), "%s", path);
  if(n < 0 || (size_t)n >= sizeof(row->path)) {
    ampr_set_err(err, err_size, "AMPR index path too long");
    errno = ENAMETOOLONG;
    return -1;
  }
  return 0;
}

static void
ampr_put_le32(unsigned char *p, uint32_t v) {
  p[0] = (unsigned char)(v & 0xffU);
  p[1] = (unsigned char)((v >> 8) & 0xffU);
  p[2] = (unsigned char)((v >> 16) & 0xffU);
  p[3] = (unsigned char)((v >> 24) & 0xffU);
}

static void
ampr_put_le64(unsigned char *p, uint64_t v) {
  for(int i = 0; i < 8; i++) p[i] = (unsigned char)((v >> (i * 8)) & 0xffU);
}

static int
ampr_write_full(int fd, const void *buf, size_t len) {
  const unsigned char *p = (const unsigned char *)buf;
  while(len > 0) {
    ssize_t n = write(fd, p, len);
    if(n < 0) {
      if(errno == EINTR) continue;
      return -1;
    }
    if(n == 0) {
      errno = EIO;
      return -1;
    }
    p += (size_t)n;
    len -= (size_t)n;
  }
  return 0;
}

static void
ampr_mem_buf_free(ampr_mem_buf_t *b) {
  if(!b) return;
  free(b->data);
  memset(b, 0, sizeof(*b));
}

static int
ampr_mem_buf_reserve(ampr_mem_buf_t *b, size_t extra, char *err,
                     size_t err_size) {
  if(!b || extra > SIZE_MAX - b->len) {
    ampr_set_err(err, err_size, "AMPR index is too large");
    errno = EOVERFLOW;
    return -1;
  }
  size_t need = b->len + extra;
  if(need <= b->cap) return 0;
  size_t next = b->cap ? b->cap : 4096;
  while(next < need) {
    if(next > SIZE_MAX / 2) {
      next = need;
      break;
    }
    next *= 2;
  }
  unsigned char *p = (unsigned char *)realloc(b->data, next);
  if(!p) {
    ampr_set_err(err, err_size, "out of memory");
    errno = ENOMEM;
    return -1;
  }
  b->data = p;
  b->cap = next;
  return 0;
}

static int
ampr_mem_buf_append(ampr_mem_buf_t *b, const void *data, size_t size,
                    char *err, size_t err_size) {
  if(size == 0) return 0;
  if(!data) {
    errno = EINVAL;
    return -1;
  }
  if(ampr_mem_buf_reserve(b, size, err, err_size) != 0) return -1;
  memcpy(b->data + b->len, data, size);
  b->len += size;
  return 0;
}

static int
ampr_mem_buf_append_zero(ampr_mem_buf_t *b, size_t size, char *err,
                         size_t err_size) {
  if(size == 0) return 0;
  if(ampr_mem_buf_reserve(b, size, err, err_size) != 0) return -1;
  memset(b->data + b->len, 0, size);
  b->len += size;
  return 0;
}

static int
ampr_build_hash_slots(const ampr_row_t *rows, size_t count,
                      ampr_hash_slot_t **slots_out, uint32_t *slot_count_out,
                      ampr_index_stats_t *stats, char *err, size_t err_size) {
  uint32_t slot_count = ampr_hash_slot_count(count);
  ampr_hash_slot_t *slots;
  if(slot_count == 0) {
    ampr_set_err(err, err_size, "invalid AMPR index entry count");
    errno = EINVAL;
    return -1;
  }
  slots = (ampr_hash_slot_t *)calloc(slot_count, sizeof(*slots));
  if(!slots) {
    ampr_set_err(err, err_size, "out of memory");
    errno = ENOMEM;
    return -1;
  }
  uint32_t mask = slot_count - 1U;
  for(size_t i = 0; i < count; i++) {
    uint64_t h = ampr_fnv1a64_path_hash(rows[i].path);
    uint32_t pos = (uint32_t)h & mask;
    uint32_t probe = 0;
    int duplicate = 0;
    while(slots[pos].index_plus_one != 0) {
      if(slots[pos].hash == h) {
        slots[pos].flags |= AMPR_INDEX_DUPLICATE_HASH_FLAG;
        duplicate = 1;
        if(stats) {
          stats->duplicate_hash_groups++;
          stats->duplicate_hash_entries += 2;
        }
      }
      pos = (pos + 1U) & mask;
      probe++;
    }
    if(probe && stats) {
      stats->probed_entries++;
      stats->probe_steps += probe;
      if(probe > stats->max_probe) stats->max_probe = probe;
    }
    slots[pos].hash = h;
    slots[pos].index_plus_one = (uint32_t)i + 1U;
    slots[pos].flags = duplicate ? AMPR_INDEX_DUPLICATE_HASH_FLAG : 0;
  }
  *slots_out = slots;
  *slot_count_out = slot_count;
  if(stats) stats->hash_slots = slot_count;
  return 0;
}

static int
ampr_build_index_memory_from_rows(const ampr_row_t *rows, size_t count,
                                  unsigned char **out, size_t *out_size,
                                  ampr_index_stats_t *stats,
                                  char *err, size_t err_size) {
  const size_t header_size = 48;
  size_t records_size;
  size_t path_blob_size = 0;
  size_t path_end;
  size_t hash_offset;
  size_t padding_size;
  unsigned char header[48];
  unsigned char record[24];
  unsigned char slot_buf[16];
  ampr_hash_slot_t *slots = NULL;
  uint32_t slot_count = 0;
  ampr_mem_buf_t b = {0};
  int rc = -1;

  if(out) *out = NULL;
  if(out_size) *out_size = 0;
  if(!out || !out_size || count == 0 || count > 0x7fffffffU) {
    ampr_set_err(err, err_size, "invalid AMPR index entry count");
    errno = EINVAL;
    return -1;
  }
  if(count > (SIZE_MAX / AMPR_INDEX_RECORD_SIZE)) {
    ampr_set_err(err, err_size, "AMPR index is too large");
    errno = EOVERFLOW;
    return -1;
  }
  records_size = count * AMPR_INDEX_RECORD_SIZE;
  for(size_t i = 0; i < count; i++) {
    size_t path_len = strlen(rows[i].path);
    if(path_len > 0xffffffffULL ||
       path_blob_size > 0xffffffffULL - path_len - 1ULL) {
      ampr_set_err(err, err_size, "AMPR index path blob is too large");
      errno = EOVERFLOW;
      return -1;
    }
    path_blob_size += path_len + 1;
  }
  path_end = header_size + records_size + path_blob_size;
  hash_offset = (path_end + (AMPR_INDEX_HASH_SLOT_SIZE - 1U)) &
                ~(size_t)(AMPR_INDEX_HASH_SLOT_SIZE - 1U);
  padding_size = hash_offset - path_end;

  if(ampr_build_hash_slots(rows, count, &slots, &slot_count, stats, err,
                           err_size) != 0) {
    return -1;
  }

  memset(header, 0, sizeof(header));
  memcpy(header, AMPR_INDEX_MAGIC, 8);
  ampr_put_le32(header + 8, AMPR_INDEX_VERSION);
  ampr_put_le32(header + 12, AMPR_INDEX_RECORD_SIZE);
  ampr_put_le64(header + 16, (uint64_t)count);
  ampr_put_le64(header + 24, (uint64_t)path_blob_size);
  ampr_put_le64(header + 32, (uint64_t)hash_offset);
  ampr_put_le32(header + 40, AMPR_INDEX_HASH_SLOT_SIZE);
  ampr_put_le32(header + 44, slot_count);
  if(ampr_mem_buf_append(&b, header, sizeof(header), err, err_size) != 0) {
    goto done;
  }

  uint32_t path_offset = 0;
  for(size_t i = 0; i < count; i++) {
    size_t path_len = strlen(rows[i].path);
    memset(record, 0, sizeof(record));
    ampr_put_le32(record + 0, path_offset);
    ampr_put_le32(record + 4, (uint32_t)path_len);
    ampr_put_le64(record + 8, rows[i].size);
    ampr_put_le64(record + 16, (uint64_t)rows[i].mtime);
    if(ampr_mem_buf_append(&b, record, sizeof(record), err, err_size) != 0) {
      goto done;
    }
    path_offset += (uint32_t)path_len + 1U;
  }
  for(size_t i = 0; i < count; i++) {
    size_t path_len = strlen(rows[i].path) + 1;
    if(ampr_mem_buf_append(&b, rows[i].path, path_len, err, err_size) != 0) {
      goto done;
    }
  }
  if(ampr_mem_buf_append_zero(&b, padding_size, err, err_size) != 0) {
    goto done;
  }
  for(uint32_t i = 0; i < slot_count; i++) {
    memset(slot_buf, 0, sizeof(slot_buf));
    ampr_put_le64(slot_buf + 0, slots[i].hash);
    ampr_put_le32(slot_buf + 8, slots[i].index_plus_one);
    ampr_put_le32(slot_buf + 12, slots[i].flags);
    if(ampr_mem_buf_append(&b, slot_buf, sizeof(slot_buf), err,
                           err_size) != 0) {
      goto done;
    }
  }
  if(stats) {
    stats->indexed_files = count;
    stats->output_size = (uint64_t)b.len;
  }
  *out = b.data;
  *out_size = b.len;
  memset(&b, 0, sizeof(b));
  rc = 0;

done:
  ampr_mem_buf_free(&b);
  free(slots);
  return rc;
}

static int
ampr_write_index_file(const ampr_row_t *rows, size_t count, const char *output,
                      ampr_index_stats_t *stats, char *err, size_t err_size) {
  const size_t header_size = 48;
  size_t records_size;
  size_t path_blob_size = 0;
  size_t path_end;
  size_t hash_offset;
  size_t padding_size;
  unsigned char header[48];
  unsigned char record[24];
  unsigned char slot_buf[16];
  ampr_hash_slot_t *slots = NULL;
  uint32_t slot_count = 0;
  char tmp_path[1024];
  int fd = -1;
  int rc = -1;

  if(count == 0 || count > 0x7fffffffU) {
    ampr_set_err(err, err_size, "invalid AMPR index entry count");
    errno = EINVAL;
    return -1;
  }
  if(count > (SIZE_MAX / AMPR_INDEX_RECORD_SIZE)) {
    ampr_set_err(err, err_size, "AMPR index is too large");
    errno = EOVERFLOW;
    return -1;
  }
  records_size = count * AMPR_INDEX_RECORD_SIZE;
  for(size_t i = 0; i < count; i++) {
    size_t path_len = strlen(rows[i].path);
    if(path_len > 0xffffffffULL ||
       path_blob_size > 0xffffffffULL - path_len - 1ULL) {
      ampr_set_err(err, err_size, "AMPR index path blob is too large");
      errno = EOVERFLOW;
      return -1;
    }
    path_blob_size += path_len + 1;
  }
  path_end = header_size + records_size + path_blob_size;
  hash_offset = (path_end + (AMPR_INDEX_HASH_SLOT_SIZE - 1U)) &
                ~(size_t)(AMPR_INDEX_HASH_SLOT_SIZE - 1U);
  padding_size = hash_offset - path_end;

  if(ampr_build_hash_slots(rows, count, &slots, &slot_count, stats, err,
                           err_size) != 0) {
    return -1;
  }

  if(snprintf(tmp_path, sizeof(tmp_path), "%s.tmp", output) >=
     (int)sizeof(tmp_path)) {
    ampr_set_err(err, err_size, "AMPR index temp path too long");
    errno = ENAMETOOLONG;
    goto done;
  }

  memset(header, 0, sizeof(header));
  memcpy(header, AMPR_INDEX_MAGIC, 8);
  ampr_put_le32(header + 8, AMPR_INDEX_VERSION);
  ampr_put_le32(header + 12, AMPR_INDEX_RECORD_SIZE);
  ampr_put_le64(header + 16, (uint64_t)count);
  ampr_put_le64(header + 24, (uint64_t)path_blob_size);
  ampr_put_le64(header + 32, (uint64_t)hash_offset);
  ampr_put_le32(header + 40, AMPR_INDEX_HASH_SLOT_SIZE);
  ampr_put_le32(header + 44, slot_count);

  fd = open(tmp_path, O_WRONLY | O_CREAT | O_TRUNC, 0644);
  if(fd < 0) {
    ampr_set_err(err, err_size, "create AMPR index: %s", strerror(errno));
    goto done;
  }
  if(ampr_write_full(fd, header, sizeof(header)) != 0) {
    ampr_set_err(err, err_size, "write AMPR index header: %s",
                 strerror(errno));
    goto done;
  }

  uint32_t path_offset = 0;
  for(size_t i = 0; i < count; i++) {
    size_t path_len = strlen(rows[i].path);
    memset(record, 0, sizeof(record));
    ampr_put_le32(record + 0, path_offset);
    ampr_put_le32(record + 4, (uint32_t)path_len);
    ampr_put_le64(record + 8, rows[i].size);
    ampr_put_le64(record + 16, (uint64_t)rows[i].mtime);
    if(ampr_write_full(fd, record, sizeof(record)) != 0) {
      ampr_set_err(err, err_size, "write AMPR index record: %s",
                   strerror(errno));
      goto done;
    }
    path_offset += (uint32_t)path_len + 1U;
  }
  for(size_t i = 0; i < count; i++) {
    size_t path_len = strlen(rows[i].path) + 1;
    if(ampr_write_full(fd, rows[i].path, path_len) != 0) {
      ampr_set_err(err, err_size, "write AMPR index path blob: %s",
                   strerror(errno));
      goto done;
    }
  }
  if(padding_size) {
    static const unsigned char zeroes[AMPR_INDEX_HASH_SLOT_SIZE] = {0};
    if(ampr_write_full(fd, zeroes, padding_size) != 0) {
      ampr_set_err(err, err_size, "write AMPR index padding: %s",
                   strerror(errno));
      goto done;
    }
  }
  for(uint32_t i = 0; i < slot_count; i++) {
    memset(slot_buf, 0, sizeof(slot_buf));
    ampr_put_le64(slot_buf + 0, slots[i].hash);
    ampr_put_le32(slot_buf + 8, slots[i].index_plus_one);
    ampr_put_le32(slot_buf + 12, slots[i].flags);
    if(ampr_write_full(fd, slot_buf, sizeof(slot_buf)) != 0) {
      ampr_set_err(err, err_size, "write AMPR index hash table: %s",
                   strerror(errno));
      goto done;
    }
  }
  if(close(fd) != 0) {
    fd = -1;
    ampr_set_err(err, err_size, "close AMPR index: %s", strerror(errno));
    goto done;
  }
  fd = -1;
  if(rename(tmp_path, output) != 0) {
    ampr_set_err(err, err_size, "publish AMPR index: %s", strerror(errno));
    goto done;
  }
  if(stats) {
    struct stat st;
    stats->indexed_files = count;
    if(stat(output, &st) == 0 && st.st_size > 0) {
      stats->output_size = (uint64_t)st.st_size;
    }
  }
  rc = 0;

done:
  if(fd >= 0) close(fd);
  if(rc != 0) unlink(tmp_path);
  free(slots);
  return rc;
}

int
ampr_index_entries_need_index(const ampr_index_entry_t *entries, size_t count) {
  if(!entries) return 0;
  for(size_t i = 0; i < count; i++) {
    if(ampr_rel_is_marker(entries[i].rel)) return 1;
  }
  return 0;
}

int
ampr_index_build_from_entries(const char *root,
                              const ampr_index_entry_t *entries,
                              size_t count,
                              int allow_case_collisions,
                              ampr_index_stats_t *stats,
                              char *err,
                              size_t err_size) {
  ampr_row_t *rows = NULL;
  size_t row_count = 0;
  size_t row_cap = 0;
  char output[1024];
  int rc = -1;

  if(stats) memset(stats, 0, sizeof(*stats));
  if(!root || !root[0] || !entries) {
    ampr_set_err(err, err_size, "bad AMPR index input");
    errno = EINVAL;
    return -1;
  }
  if(snprintf(output, sizeof(output), "%s/%s", root, "ampr_emu.index") >=
     (int)sizeof(output)) {
    ampr_set_err(err, err_size, "AMPR index output path too long");
    errno = ENAMETOOLONG;
    return -1;
  }

  for(size_t i = 0; i < count; i++) {
    char app0_path[1024];
    const char *rel = entries[i].rel ? entries[i].rel : "";
    if(ampr_rel_is_root_index(rel)) continue;
    if(snprintf(app0_path, sizeof(app0_path), "/app0/%s", rel) >=
       (int)sizeof(app0_path)) {
      ampr_set_err(err, err_size, "AMPR index path too long");
      errno = ENAMETOOLONG;
      goto done;
    }
    if(ampr_append_row(&rows, &row_count, &row_cap, entries[i].size,
                       entries[i].mtime, app0_path, err, err_size) != 0) {
      goto done;
    }
  }
  if(row_count == 0) {
    ampr_set_err(err, err_size, "AMPR index has no files");
    errno = EINVAL;
    goto done;
  }
  qsort(rows, row_count, sizeof(rows[0]), ampr_row_cmp);
  size_t out = 0;
  for(size_t i = 0; i < row_count; i++) {
    if(out > 0 && ampr_ascii_casecmp(rows[out - 1].path, rows[i].path) == 0) {
      if(!allow_case_collisions) {
        ampr_set_err(err, err_size,
                     "AMPR index case-insensitive path collision: %s <-> %s",
                     rows[out - 1].path, rows[i].path);
        errno = EEXIST;
        goto done;
      }
      continue;
    }
    if(out != i) rows[out] = rows[i];
    out++;
  }
  row_count = out;
  if(ampr_write_index_file(rows, row_count, output, stats, err, err_size) != 0) {
    goto done;
  }
  rc = 0;

done:
  free(rows);
  return rc;
}

int
ampr_index_build_to_memory(const ampr_index_entry_t *entries,
                           size_t count,
                           int allow_case_collisions,
                           unsigned char **out,
                           size_t *out_size,
                           ampr_index_stats_t *stats,
                           char *err,
                           size_t err_size) {
  ampr_row_t *rows = NULL;
  size_t row_count = 0;
  size_t row_cap = 0;
  int rc = -1;

  if(out) *out = NULL;
  if(out_size) *out_size = 0;
  if(stats) memset(stats, 0, sizeof(*stats));
  if(!entries || !out || !out_size) {
    ampr_set_err(err, err_size, "bad AMPR index input");
    errno = EINVAL;
    return -1;
  }

  for(size_t i = 0; i < count; i++) {
    char app0_path[1024];
    const char *rel = entries[i].rel ? entries[i].rel : "";
    if(ampr_rel_is_root_index(rel)) continue;
    if(snprintf(app0_path, sizeof(app0_path), "/app0/%s", rel) >=
       (int)sizeof(app0_path)) {
      ampr_set_err(err, err_size, "AMPR index path too long");
      errno = ENAMETOOLONG;
      goto done;
    }
    if(ampr_append_row(&rows, &row_count, &row_cap, entries[i].size,
                       entries[i].mtime, app0_path, err, err_size) != 0) {
      goto done;
    }
  }
  if(row_count == 0) {
    ampr_set_err(err, err_size, "AMPR index has no files");
    errno = EINVAL;
    goto done;
  }
  qsort(rows, row_count, sizeof(rows[0]), ampr_row_cmp);
  size_t out_rows = 0;
  for(size_t i = 0; i < row_count; i++) {
    if(out_rows > 0 &&
       ampr_ascii_casecmp(rows[out_rows - 1].path, rows[i].path) == 0) {
      if(!allow_case_collisions) {
        ampr_set_err(err, err_size,
                     "AMPR index case-insensitive path collision: %s <-> %s",
                     rows[out_rows - 1].path, rows[i].path);
        errno = EEXIST;
        goto done;
      }
      continue;
    }
    if(out_rows != i) rows[out_rows] = rows[i];
    out_rows++;
  }
  row_count = out_rows;
  rc = ampr_build_index_memory_from_rows(rows, row_count, out, out_size,
                                         stats, err, err_size);

done:
  free(rows);
  return rc;
}
