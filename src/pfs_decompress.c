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
#include "transfer_internal.h"

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
#define PFS_EXTRACT_OUTPUT_BUFFER_SIZE (16U * 1024U * 1024U)
#define PFS_EXTRACT_OUTPUT_BUFFER_MIN_SIZE (64U * 1024U)

typedef struct pfsc_reader {
  int fd;
  uint64_t file_start;
  uint64_t logical_size;
  uint64_t block_size;
  uint64_t block_count;
  char nested_name[256];
  uint64_t *offsets;
  unsigned char *stored;
  unsigned char *block;
  uint64_t cached_index;
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
path_segment_supported(const char *name) {
  if(!name || !*name || !strcmp(name, ".") || !strcmp(name, "..")) return 0;
  if(strlen(name) >= 256) return 0;
  for(const unsigned char *p = (const unsigned char *)name; *p; p++) {
    if(*p < 0x20 || *p >= 0x7f || *p == '/' || *p == '\\') return 0;
  }
  return 1;
}

static int
ffpfsc_path_supported(const char *path) {
  return path && ends_with_ci(path, ".ffpfsc");
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
  memcpy(parent, parent_len ? path : "/", parent_len);
  parent[parent_len] = 0;
  snprintf(base, base_size, "%s", slash + 1);
  return 0;
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

static long
job_long_from_u64(uint64_t v) {
  return v > (uint64_t)LONG_MAX ? LONG_MAX : (long)v;
}

static uint64_t
saturating_add_u64(uint64_t a, uint64_t b) {
  return UINT64_MAX - a < b ? UINT64_MAX : a + b;
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
  if(r->fd >= 0) close(r->fd);
  free(r->offsets);
  free(r->stored);
  free(r->block);
  memset(r, 0, sizeof(*r));
  r->fd = -1;
}

static int
pfsc_reader_load_offsets(pfsc_reader_t *r, uint64_t table_offset,
                         char *err, size_t err_size) {
  uint64_t count = r->block_count + 1;
  r->offsets = calloc((size_t)count, sizeof(*r->offsets));
  if(!r->offsets) {
    set_err(err, err_size, "out of memory");
    errno = ENOMEM;
    return -1;
  }
  unsigned char raw[8];
  for(uint64_t i = 0; i < count; i++) {
    if(read_exact_at(r->fd, raw, sizeof(raw),
                     (off_t)(r->file_start + table_offset + i * 8)) != 0) {
      set_err(err, err_size, "read PFSC offsets: %s", strerror(errno));
      return -1;
    }
    r->offsets[i] = rd64(raw);
    if(i > 0 && r->offsets[i] < r->offsets[i - 1]) {
      set_err(err, err_size, "PFSC offsets are not monotonic");
      errno = EINVAL;
      return -1;
    }
  }
  return 0;
}

static void
pfsc_reader_read_outer_nested_name(int fd, pfsc_reader_t *r) {
  unsigned char inode[PFS_INODE_SIZE];
  unsigned char dir_block[PFS_BLOCK_SIZE];
  pfs_inode_info_t root;

  snprintf(r->nested_name, sizeof(r->nested_name), "pfs_image.dat");
  if(read_exact_at(fd, inode, sizeof(inode),
                   (off_t)(PFS_BLOCK_SIZE + 2 * PFS_INODE_SIZE)) != 0) {
    return;
  }
  parse_inode_info(inode, &root);
  if((root.mode & PFS_INODE_MODE_DIR) == 0 || root.db0 < 0 ||
     root.size == 0 || root.size > PFS_BLOCK_SIZE) {
    return;
  }
  if(read_exact_at(fd, dir_block, (size_t)root.size,
                   (off_t)((uint64_t)root.db0 * PFS_BLOCK_SIZE)) != 0) {
    return;
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
      return;
    }
    off += ent_size;
  }
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
  r->cached_index = UINT64_MAX;
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
    r->offsets = NULL;
    r->stored = NULL;
    r->block = NULL;
    r->fd = -1;
  }
  return rc;
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
    memcpy(r->block, r->stored, stored_len);
  } else {
    size_t out_len = tinfl_decompress_mem_to_mem(
        r->block, (size_t)r->block_size, r->stored, stored_len,
        TINFL_FLAG_PARSE_ZLIB_HEADER);
    if(out_len != (size_t)r->block_size) {
      set_err(err, err_size, "decompress PFSC block failed");
      errno = EINVAL;
      return -1;
    }
  }
  r->cached_index = index;
  r->cache_valid = 1;
  return 0;
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
pfsc_reader_detect_nested_type(pfsc_reader_t *r,
                               char *err, size_t err_size) {
  unsigned char header[PFS_BLOCK_SIZE];
  if(pfsc_reader_read(r, 0, header, sizeof(header), err, err_size) != 0) {
    return PFS_NESTED_UNKNOWN;
  }
  if(rd64(header + 0x00) == (uint64_t)PFS_VERSION_PS5 &&
     rd64(header + 0x08) == (uint64_t)PFS_MAGIC) {
    return PFS_NESTED_PFS;
  }
  if(memcmp(header + 3, "EXFAT   ", 8) == 0) {
    return PFS_NESTED_EXFAT;
  }
  set_err(err, err_size, "unsupported nested image type");
  errno = EINVAL;
  return PFS_NESTED_UNKNOWN;
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
    unsigned char hdr[16];
    if(pfs_nested_read_inode_data(nr, &dir_ino, off, hdr, sizeof(hdr),
                                  err, err_size) != 0) {
      return -1;
    }
    uint32_t child_inode = rd32(hdr + 0);
    uint32_t type = rd32(hdr + 4);
    uint32_t name_len = rd32(hdr + 8);
    uint32_t ent_size = rd32(hdr + 12);
    if(ent_size == 0) break;
    if(ent_size < 16 || ent_size > PFS_BLOCK_SIZE ||
       name_len == 0 || name_len >= 256 ||
       16ULL + name_len > ent_size ||
       off + ent_size > dir_ino.size) {
      set_err(err, err_size, "invalid PFS dirent");
      errno = EINVAL;
      return -1;
    }

    char name[256];
    if(pfs_nested_read_inode_data(nr, &dir_ino, off + 16, name, name_len,
                                  err, err_size) != 0) {
      return -1;
    }
    name[name_len] = 0;
    if(type == wanted_type && strcmp(name, wanted) == 0) {
      *inode_out = child_inode;
      return 0;
    }
    off += ent_size;
  }

  set_err(err, err_size, "PFS superroot missing uroot");
  errno = EINVAL;
  return -1;
}

static int pfs_count_dir(pfs_nested_reader_t *nr, uint32_t inode_num,
                         uint64_t *files, uint64_t *bytes, uint64_t *blocks,
                         char *err, size_t err_size);
static int pfs_extract_file(pfs_nested_reader_t *nr, uint32_t inode_num,
                            const char *out_path, const char *rel,
                            int workers, char *err, size_t err_size);
static int pfs_extract_dir(pfs_nested_reader_t *nr, uint32_t inode_num,
                           const char *out_dir, const char *rel,
                           int workers, char *err, size_t err_size);

static void
pfs_extract_plan_free(pfs_extract_plan_t *plan) {
  if(!plan) return;
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
pfs_extract_entry_cmp_desc(const void *a, const void *b) {
  const pfs_extract_entry_t *ea = a;
  const pfs_extract_entry_t *eb = b;
  if(ea->first_block < eb->first_block) return 1;
  if(ea->first_block > eb->first_block) return -1;
  return strcmp(eb->rel, ea->rel);
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
    unsigned char hdr[16];
    if(job_cancelled()) {
      set_err(err, err_size, "cancelled");
      errno = EINTR;
      return -1;
    }
    if(pfs_nested_read_inode_data(nr, &dir_ino, off, hdr, sizeof(hdr),
                                  err, err_size) != 0) {
      return -1;
    }
    uint32_t child_inode = rd32(hdr + 0);
    uint32_t type = rd32(hdr + 4);
    uint32_t name_len = rd32(hdr + 8);
    uint32_t ent_size = rd32(hdr + 12);
    if(ent_size == 0) break;
    if(ent_size < 16 || ent_size > PFS_BLOCK_SIZE ||
       name_len == 0 || name_len >= 256 ||
       16ULL + name_len > ent_size ||
       off + ent_size > dir_ino.size) {
      set_err(err, err_size, "invalid PFS dirent");
      errno = EINVAL;
      return -1;
    }

    char name[256];
    if(pfs_nested_read_inode_data(nr, &dir_ino, off + 16, name, name_len,
                                  err, err_size) != 0) {
      return -1;
    }
    name[name_len] = 0;
    if(strcmp(name, ".") != 0 && strcmp(name, "..") != 0) {
      if(!path_segment_supported(name)) {
        set_err(err, err_size, "unsafe PFS path segment");
        errno = EINVAL;
        return -1;
      }
      char child_out[1024];
      char child_rel[1024];
      if(join_abs(child_out, sizeof(child_out), out_dir, name) != 0 ||
         join_rel(child_rel, sizeof(child_rel), rel ? rel : "", name) != 0) {
        set_err(err, err_size, "output path too long");
        return -1;
      }
      if(type == PFS_DIRENT_TYPE_DIRECTORY) {
        if(pfs_collect_extract_plan(nr, child_inode, child_out, child_rel,
                                    plan, err, err_size) != 0) {
          return -1;
        }
      } else if(type == PFS_DIRENT_TYPE_FILE) {
        pfs_inode_info_t file_ino;
        if(pfs_nested_read_inode(nr, child_inode, &file_ino,
                                 err, err_size) != 0 ||
           pfs_extract_plan_push(plan, child_inode, &file_ino, child_out,
                                 child_rel, err, err_size) != 0) {
          return -1;
        }
      } else {
        set_err(err, err_size, "unsupported PFS dirent type");
        errno = EINVAL;
        return -1;
      }
    }
    off += ent_size;
  }
  chmod(out_dir, 0777);
  return 0;
}

static int
pfsc_truncate_source_after_block(pfsc_reader_t *r, uint64_t first_block,
                                 char *err, size_t err_size) {
  if(first_block >= r->block_count) {
    set_err(err, err_size, "truncate outside PFSC image");
    errno = EINVAL;
    return -1;
  }
  uint64_t new_size = r->file_start + r->offsets[first_block];
  if(ftruncate(r->fd, (off_t)new_size) != 0) {
    set_err(err, err_size, "truncate compressed image: %s", strerror(errno));
    return -1;
  }
  r->cache_valid = 0;
  return 0;
}

static int
pfsc_stream_group_consumed(const pfsc_reader_t *r, uint64_t first_block,
                           int *consumed, char *err, size_t err_size) {
  struct stat st;
  if(consumed) *consumed = 0;
  if(!r || first_block >= r->block_count) return 0;
  if(fstat(r->fd, &st) != 0) {
    set_err(err, err_size, "stat compressed image: %s", strerror(errno));
    return -1;
  }
  uint64_t source_size = st.st_size < 0 ? 0 : (uint64_t)st.st_size;
  uint64_t committed_size = r->file_start + r->offsets[first_block];
  if(consumed) *consumed = source_size <= committed_size;
  return 0;
}

static int
extract_output_complete(const pfs_extract_entry_t *e) {
  struct stat st;
  if(!e || stat(e->out_path, &st) != 0 || !S_ISREG(st.st_mode)) return 0;
  return st.st_size >= 0 && (uint64_t)st.st_size == e->size;
}

static int
extract_group_outputs_complete(const pfs_extract_plan_t *plan, size_t first,
                               size_t last) {
  if(!plan || first >= last || last > plan->count) return 0;
  for(size_t i = first; i < last; i++) {
    if(!extract_output_complete(&plan->items[i])) return 0;
  }
  return 1;
}

static int
require_extract_group_outputs_complete(const pfs_extract_plan_t *plan,
                                       size_t first, size_t last,
                                       char *err, size_t err_size) {
  if(!plan || first >= last || last > plan->count) {
    errno = EINVAL;
    return -1;
  }
  for(size_t i = first; i < last; i++) {
    if(!extract_output_complete(&plan->items[i])) {
      set_err(err, err_size,
              "stream restore output missing after source truncation: %s",
              plan->items[i].rel[0] ? plan->items[i].rel :
                                      plan->items[i].out_path);
      errno = EIO;
      return -1;
    }
  }
  return 0;
}

static void
account_skipped_extract_group(const pfs_extract_plan_t *plan, size_t first,
                              size_t last) {
  if(!plan || first >= last || last > plan->count) return;
  for(size_t i = first; i < last; i++) {
    uint64_t size = plan->items[i].size;
    atomic_fetch_add(&g_job.copied_bytes,
                     size > (uint64_t)LONG_MAX ? LONG_MAX : (long)size);
    atomic_fetch_add(&g_job.done_files, 1);
  }
}

static int
pfs_extract_reverse_plan(pfs_nested_reader_t *nr, pfs_extract_plan_t *plan,
                         int workers, char *err, size_t err_size) {
  qsort(plan->items, plan->count, sizeof(plan->items[0]),
        pfs_extract_entry_cmp_desc);
  for(size_t i = 0; i < plan->count;) {
    size_t group_start = i;
    uint64_t group_first_block = plan->items[i].first_block;
    int truncate_group = 0;
    int group_consumed = 0;
    size_t group_end = i;
    while(group_end < plan->count &&
          plan->items[group_end].first_block == group_first_block) {
      if(plan->items[group_end].block_count > 0) truncate_group = 1;
      group_end++;
    }
    if(pfsc_stream_group_consumed(&nr->pfsc, group_first_block,
                                  &group_consumed, err, err_size) != 0) {
      return -1;
    }
    if(group_consumed) {
      if(require_extract_group_outputs_complete(plan, group_start, group_end,
                                                err, err_size) != 0) {
        return -1;
      }
      account_skipped_extract_group(plan, group_start, group_end);
      i = group_end;
      continue;
    }
    if(extract_group_outputs_complete(plan, group_start, group_end)) {
      account_skipped_extract_group(plan, group_start, group_end);
      if(truncate_group &&
         pfsc_truncate_source_after_block(&nr->pfsc, group_first_block,
                                          err, err_size) != 0) {
        return -1;
      }
      i = group_end;
      continue;
    }
    do {
      pfs_extract_entry_t *e = &plan->items[i];
      if(job_cancelled()) {
        set_err(err, err_size, "cancelled");
        errno = EINTR;
        return -1;
      }
      if(pfs_extract_file(nr, e->inode_num, e->out_path, e->rel,
                          workers, err, err_size) != 0) {
        return -1;
      }
      if(e->block_count > 0) truncate_group = 1;
      i++;
    } while(i < plan->count && plan->items[i].first_block == group_first_block);

    if(truncate_group &&
       pfsc_truncate_source_after_block(&nr->pfsc, group_first_block,
                                        err, err_size) != 0) {
      return -1;
    }
  }
  return 0;
}

static int
pfs_count_dir(pfs_nested_reader_t *nr, uint32_t inode_num,
              uint64_t *files, uint64_t *bytes, uint64_t *blocks,
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

  for(uint64_t off = 0; off + 16 <= dir_ino.size;) {
    unsigned char hdr[16];
    if(pfs_nested_read_inode_data(nr, &dir_ino, off, hdr, sizeof(hdr),
                                  err, err_size) != 0) {
      return -1;
    }
    uint32_t child_inode = rd32(hdr + 0);
    uint32_t type = rd32(hdr + 4);
    uint32_t name_len = rd32(hdr + 8);
    uint32_t ent_size = rd32(hdr + 12);
    if(ent_size == 0) break;
    if(ent_size < 16 || ent_size > PFS_BLOCK_SIZE ||
       name_len == 0 || name_len >= 256 ||
       16ULL + name_len > ent_size ||
       off + ent_size > dir_ino.size) {
      set_err(err, err_size, "invalid PFS dirent");
      errno = EINVAL;
      return -1;
    }
    char name[256];
    if(pfs_nested_read_inode_data(nr, &dir_ino, off + 16, name, name_len,
                                  err, err_size) != 0) {
      return -1;
    }
    name[name_len] = 0;
    if(strcmp(name, ".") != 0 && strcmp(name, "..") != 0) {
      if(type == PFS_DIRENT_TYPE_DIRECTORY) {
        if(pfs_count_dir(nr, child_inode, files, bytes, blocks,
                         err, err_size) != 0) {
          return -1;
        }
      } else if(type == PFS_DIRENT_TYPE_FILE) {
        pfs_inode_info_t file_ino;
        if(pfs_nested_read_inode(nr, child_inode, &file_ino,
                                 err, err_size) != 0) {
          return -1;
        }
        (*files)++;
        *bytes += file_ino.size;
        *blocks += ceil_div_u64(file_ino.size, PFS_BLOCK_SIZE);
      }
    }
    off += ent_size;
  }
  return 0;
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
      pthread_cond_wait(&pool->cond, &pool->lock);
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

  for(uint64_t off = 0; off < ino->size;) {
    if(job_cancelled()) {
      set_err(err, err_size, "cancelled");
      errno = EINTR;
      goto done;
    }
    size_t n = ino->size - off > PFS_BLOCK_SIZE
                   ? (size_t)PFS_BLOCK_SIZE
                   : (size_t)(ino->size - off);
    if(pfs_nested_read_inode_data(nr, ino, off, buf, n,
                                  err, err_size) != 0 ||
       pfs_stream_buffer_write(&outbuf, out, write_all_fd, buf, n) != 0) {
      if(!err[0]) set_err(err, err_size, "write output file: %s", strerror(errno));
      goto done;
    }
    off += n;
    atomic_fetch_add(&g_job.copied_bytes,
                     n > (size_t)LONG_MAX ? LONG_MAX : (long)n);
  }
  if(pfs_stream_buffer_flush(&outbuf, out, write_all_fd) != 0) {
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
        pthread_cond_wait(&pool.cond, &pool.lock);
      }
      if(pool.error) {
        int saved = pool.error;
        pthread_mutex_unlock(&pool.lock);
        errno = saved;
        set_err(err, err_size, "decompression task failed: %s", strerror(saved));
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
      pthread_cond_wait(&pool.cond, &pool.lock);
      job_add_wait_us(&g_job.writer_wait_us, wait_started);
    }
    if(pool.error) {
      int saved = pool.error;
      pthread_mutex_unlock(&pool.lock);
      errno = saved;
      set_err(err, err_size, "decompression task failed: %s", strerror(saved));
      goto done;
    }
    pthread_mutex_unlock(&pool.lock);

    if(pfs_stream_buffer_write(&outbuf, out, write_all_fd,
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

  if(pfs_stream_buffer_flush(&outbuf, out, write_all_fd) != 0) {
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
                 char *err, size_t err_size) {
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
  if(fsync(out) != 0) {
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
    unsigned char hdr[16];
    if(pfs_nested_read_inode_data(nr, &dir_ino, off, hdr, sizeof(hdr),
                                  err, err_size) != 0) {
      return -1;
    }
    uint32_t child_inode = rd32(hdr + 0);
    uint32_t type = rd32(hdr + 4);
    uint32_t name_len = rd32(hdr + 8);
    uint32_t ent_size = rd32(hdr + 12);
    if(ent_size == 0) break;
    if(ent_size < 16 || ent_size > PFS_BLOCK_SIZE ||
       name_len == 0 || name_len >= 256 ||
       16ULL + name_len > ent_size ||
       off + ent_size > dir_ino.size) {
      set_err(err, err_size, "invalid PFS dirent");
      errno = EINVAL;
      return -1;
    }

    char name[256];
    if(pfs_nested_read_inode_data(nr, &dir_ino, off + 16, name, name_len,
                                  err, err_size) != 0) {
      return -1;
    }
    name[name_len] = 0;
    if(strcmp(name, ".") != 0 && strcmp(name, "..") != 0) {
      if(!path_segment_supported(name)) {
        set_err(err, err_size, "unsafe PFS path segment");
        errno = EINVAL;
        return -1;
      }
      char child_out[1024];
      char child_rel[1024];
      if(join_abs(child_out, sizeof(child_out), out_dir, name) != 0 ||
         join_rel(child_rel, sizeof(child_rel), rel ? rel : "", name) != 0) {
        set_err(err, err_size, "output path too long");
        return -1;
      }
      if(type == PFS_DIRENT_TYPE_DIRECTORY) {
        if(pfs_extract_dir(nr, child_inode, child_out, child_rel, workers,
                           err, err_size) != 0) {
          return -1;
        }
      } else if(type == PFS_DIRENT_TYPE_FILE) {
        if(pfs_extract_file(nr, child_inode, child_out, child_rel, workers,
                            err, err_size) != 0) {
          return -1;
        }
      } else {
        set_err(err, err_size, "unsupported PFS dirent type");
        errno = EINVAL;
        return -1;
      }
    }
    off += ent_size;
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
                   char *err, size_t err_size) {
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
    if(pfsc_reader_read(&er->pfsc, e->image_offset + off, buf, n,
                        err, err_size) != 0 ||
       pfs_stream_buffer_write(&outbuf, out, write_all_fd, buf, n) != 0) {
      if(!err[0]) set_err(err, err_size, "write output file: %s", strerror(errno));
      goto done;
    }
    off += n;
    atomic_fetch_add(&g_job.copied_bytes,
                     n > (size_t)LONG_MAX ? LONG_MAX : (long)n);
  }
  if(pfs_stream_buffer_flush(&outbuf, out, write_all_fd) != 0) {
    set_err(err, err_size, "write output file: %s", strerror(errno));
    goto done;
  }
  chmod(e->out_path, 0777);
  if(fsync(out) != 0) {
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
                   int delete_stream, char *err, size_t err_size) {
  if(delete_stream) {
    qsort(plan->items, plan->count, sizeof(plan->items[0]),
          pfs_extract_entry_cmp_desc);
  }
  for(size_t i = 0; i < plan->count;) {
    uint64_t group_first_block = plan->items[i].first_block;
    int truncate_group = 0;
    size_t group_start = i;
    size_t group_end = i + 1;
    if(delete_stream) {
      int group_consumed = 0;
      while(group_end < plan->count &&
            plan->items[group_end].first_block == group_first_block) {
        group_end++;
      }
      for(size_t j = group_start; j < group_end; j++) {
        if(plan->items[j].block_count > 0) truncate_group = 1;
      }
      if(pfsc_stream_group_consumed(&er->pfsc, group_first_block,
                                    &group_consumed, err, err_size) != 0) {
        return -1;
      }
      if(group_consumed) {
        if(require_extract_group_outputs_complete(plan, group_start, group_end,
                                                  err, err_size) != 0) {
          return -1;
        }
        account_skipped_extract_group(plan, group_start, group_end);
        i = group_end;
        continue;
      }
      if(extract_group_outputs_complete(plan, group_start, group_end)) {
        account_skipped_extract_group(plan, group_start, group_end);
        if(truncate_group &&
           pfsc_truncate_source_after_block(&er->pfsc, group_first_block,
                                            err, err_size) != 0) {
          return -1;
        }
        i = group_end;
        continue;
      }
    }
    do {
      if(job_cancelled()) {
        set_err(err, err_size, "cancelled");
        errno = EINTR;
        return -1;
      }
      if(exfat_extract_file(er, &plan->items[i], err, err_size) != 0) {
        return -1;
      }
      if(plan->items[i].block_count > 0) truncate_group = 1;
      i++;
    } while(delete_stream && i < plan->count &&
            plan->items[i].first_block == group_first_block);
    if(delete_stream && truncate_group &&
       pfsc_truncate_source_after_block(&er->pfsc, group_first_block,
                                        err, err_size) != 0) {
      return -1;
    }
    if(!delete_stream) {
      /* In forward mode each iteration is a single file. */
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
  if(info->nested_type == PFS_NESTED_UNKNOWN) goto done;
  rc = 0;

done:
  if(reader.fd >= 0) pfsc_reader_close(&reader);
  if(fd >= 0) close(fd);
  return rc;
}

int
pfs_decompress_ffpfsc_to_app_opts(const char *path, int overwrite, int workers,
                                  int delete_policy,
                                  pfs_decompress_info_t *info,
                                  char *err, size_t err_size) {
  pfs_decompress_info_t local_info;
  pfs_nested_reader_t nr;
  exfat_reader_t er;
  pfs_extract_plan_t plan = {0};
  char tmp_path[1024];
  uint64_t file_count = 0;
  uint64_t data_bytes = 0;
  uint64_t data_blocks = 0;
  int opened = 0;
  int exfat_opened = 0;
  int rc = -1;
  int nested_type = PFS_NESTED_UNKNOWN;
  int stream_delete = delete_policy == PFS_DELETE_STREAM;

  memset(&nr, 0, sizeof(nr));
  nr.pfsc.fd = -1;
  memset(&er, 0, sizeof(er));
  er.pfsc.fd = -1;
  if(!info) info = &local_info;
  if(delete_policy != PFS_DELETE_KEEP &&
     delete_policy != PFS_DELETE_AFTER &&
     delete_policy != PFS_DELETE_STREAM) {
    set_err(err, err_size, "unsupported delete policy");
    errno = EINVAL;
    return -1;
  }
  if(pfs_decompress_probe(path, info, err, err_size) != 0) return -1;
  info->delete_policy = delete_policy;
  if(info->output_exists && !overwrite) {
    set_err(err, err_size, "output exists");
    errno = EEXIST;
    return -2;
  }

  if(workers <= 0) workers = PFS_COMPRESS_DEFAULT_WORKERS;
  if(workers > PFS_COMPRESS_MAX_WORKERS) workers = PFS_COMPRESS_MAX_WORKERS;

  if(snprintf(tmp_path, sizeof(tmp_path), "%s.tmp", info->output_path) >=
     (int)sizeof(tmp_path)) {
    set_err(err, err_size, "temporary output path too long");
    return -1;
  }

  job_set_target(info->output_path);
  job_set_current("Opening compressed image");
  int preserve_stream_tmp = stream_delete &&
      atomic_load(&g_job.rollback_requested);
  if(!preserve_stream_tmp && remove_tree_local(tmp_path) != 0) {
    set_err(err, err_size, "remove old temp output: %s", strerror(errno));
    goto done;
  }

  if(pfs_decompress_detect_nested(info->source_path, info,
                                  err, err_size) != 0) {
    goto done;
  }
  nested_type = info->nested_type;

  if(nested_type == PFS_NESTED_PFS) {
    if(pfs_nested_open(info->source_path, &nr, stream_delete,
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
    if(stream_delete) {
      if(pfs_collect_extract_plan(&nr, nr.uroot_inode, tmp_path, "", &plan,
                                  err, err_size) != 0) {
        goto done;
      }
      file_count = plan.count;
      data_bytes = plan.total_bytes;
      data_blocks = plan.total_blocks;
    } else {
      if(pfs_count_dir(&nr, nr.uroot_inode, &file_count, &data_bytes,
                       &data_blocks, err, err_size) != 0) {
        goto done;
      }
    }
  } else if(nested_type == PFS_NESTED_EXFAT) {
    if(exfat_reader_open(info->source_path, &er, stream_delete,
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

  uint64_t progress_base = 0;
  if(preserve_stream_tmp) {
    long copied_now = atomic_load(&g_job.copied_bytes);
    if(copied_now > 0) progress_base = (uint64_t)copied_now;
  }
  atomic_store(&g_job.total_bytes,
               job_long_from_u64(saturating_add_u64(progress_base,
                                                    data_bytes)));
  atomic_store(&g_job.copied_bytes, job_long_from_u64(progress_base));
  atomic_store(&g_job.compressed_output_bytes, 0);
  atomic_store(&g_job.raw_blocks, 0);
  atomic_store(&g_job.compressed_blocks, 0);
  atomic_store(&g_job.skipped_zlib_blocks, 0);
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
  if(nested_type == PFS_NESTED_PFS && !stream_delete &&
     mkdir_one_local(tmp_path) != 0) {
    set_err(err, err_size, "create temp output: %s", strerror(errno));
    goto done;
  }

  job_set_current(stream_delete ? "Converting to app folder" :
                                  "Decompressing app folder");
  if(nested_type == PFS_NESTED_PFS && stream_delete) {
    if(pfs_extract_reverse_plan(&nr, &plan, workers, err, err_size) != 0) {
      goto done;
    }
  } else if(nested_type == PFS_NESTED_PFS) {
    if(pfs_extract_dir(&nr, nr.uroot_inode, tmp_path, "", workers,
                       err, err_size) != 0) {
      goto done;
    }
  } else if(nested_type == PFS_NESTED_EXFAT) {
    if(exfat_extract_plan(&er, &plan, stream_delete, err, err_size) != 0) {
      goto done;
    }
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
  if(rc != 0 && !stream_delete) {
    remove_tree_local(tmp_path);
    if(err && err_size > 0 && !err[0]) {
      set_err(err, err_size, "%s", strerror(errno));
    }
  }
  pfs_extract_plan_free(&plan);
  return rc;
}

int
pfs_decompress_ffpfsc_to_app_ex(const char *path, int overwrite, int workers,
                                int convert, pfs_decompress_info_t *info,
                                char *err, size_t err_size) {
  return pfs_decompress_ffpfsc_to_app_opts(path, overwrite, workers,
                                           convert ? PFS_DELETE_STREAM
                                                   : PFS_DELETE_KEEP,
                                           info, err, err_size);
}

int
pfs_decompress_ffpfsc_to_app(const char *path, int overwrite,
                             pfs_decompress_info_t *info,
                             char *err, size_t err_size) {
  return pfs_decompress_ffpfsc_to_app_opts(path, overwrite,
                                           PFS_COMPRESS_DEFAULT_WORKERS,
                                           PFS_DELETE_KEEP,
                                           info, err, err_size);
}
