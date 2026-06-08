/*
 * Game Compressor - ShadowMountPlus compatibility hints.
 */

#include "gc_shadowmount.h"

#include <ctype.h>
#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <sys/stat.h>
#include <sys/sysctl.h>
#include <sys/time.h>
#include <sys/user.h>
#include <unistd.h>

#define SHADOWMOUNT_DIR "/data/shadowmount"
#define SHADOWMOUNT_CONFIG SHADOWMOUNT_DIR "/config.ini"
#define SHADOWMOUNT_CONFIG_TMP SHADOWMOUNT_DIR "/config.ini.game-compressor.tmp"
#define SHADOWMOUNT_AUTOTUNE SHADOWMOUNT_DIR "/autotune.ini"
#define SHADOWMOUNT_AUTOTUNE_TMP SHADOWMOUNT_DIR "/autotune.ini.game-compressor.tmp"
#define SHADOWMOUNT_MANUAL_LIST SHADOWMOUNT_DIR "/manual.lst"
#define SHADOWMOUNT_PFSC_SECTOR 65536U

int sceKernelLoadStartModule(const char *, size_t, const void *, uint32_t,
                             void *, int *);

static void
set_err(char *err, size_t err_size, const char *message) {
  if(err && err_size && !err[0]) {
    snprintf(err, err_size, "%s", message ? message : "shadowmount error");
  }
}

static void
set_errno_err(char *err, size_t err_size, const char *context) {
  if(err && err_size && !err[0]) {
    snprintf(err, err_size, "%s: %s", context ? context : "shadowmount",
             strerror(errno));
  }
}

static void
set_detail(char *detail, size_t detail_size, const char *message) {
  if(detail && detail_size && !detail[0]) {
    snprintf(detail, detail_size, "%s", message ? message : "shadowmount");
  }
}

static void
set_detail_errno(char *detail, size_t detail_size, const char *context) {
  if(detail && detail_size && !detail[0]) {
    snprintf(detail, detail_size, "%s: %s", context ? context : "shadowmount",
             strerror(errno));
  }
}

static int
mkdir_if_needed(const char *path) {
  if(mkdir(path, 0777) == 0) {
    chmod(path, 0777);
    return 0;
  }
  if(errno == EEXIST) return 0;
  return -1;
}

static const char *
base_name(const char *path) {
  const char *slash = strrchr(path ? path : "", '/');
  return slash ? slash + 1 : path;
}

static int
path_is_regular_elf(const char *path, char *detail, size_t detail_size) {
  struct stat st;
  unsigned char magic[4];
  ssize_t nread;
  int fd;
  if(!path || path[0] != '/') {
    set_detail(detail, detail_size, "ShadowMount executable path is not absolute");
    return 0;
  }
  if(stat(path, &st) != 0) {
    set_detail_errno(detail, detail_size, "stat ShadowMount executable");
    return 0;
  }
  if(!S_ISREG(st.st_mode)) {
    set_detail(detail, detail_size, "ShadowMount executable is not a regular file");
    return 0;
  }
  fd = open(path, O_RDONLY);
  if(fd < 0) {
    set_detail_errno(detail, detail_size, "open ShadowMount executable");
    return 0;
  }
  nread = read(fd, magic, sizeof(magic));
  close(fd);
  if(nread != (ssize_t)sizeof(magic) ||
     magic[0] != 0x7f || magic[1] != 'E' ||
     magic[2] != 'L' || magic[3] != 'F') {
    set_detail(detail, detail_size, "ShadowMount executable is not an ELF");
    return 0;
  }
  return 1;
}

static int
process_path_for_pid(pid_t pid, char *out, size_t out_size,
                     char *detail, size_t detail_size) {
  int mib[4] = { CTL_KERN, KERN_PROC, KERN_PROC_PATHNAME, pid };
  size_t len = out_size;
  if(!out || out_size == 0) return -1;
  out[0] = 0;
  if(sysctl(mib, 4, out, &len, NULL, 0) != 0) {
    set_detail_errno(detail, detail_size, "resolve ShadowMount executable path");
    return -1;
  }
  out[out_size - 1] = 0;
  if(!out[0]) {
    set_detail(detail, detail_size, "ShadowMount executable path is empty");
    return -1;
  }
  return 0;
}

static int
process_has_shadowmount_fingerprint(pid_t pid, char *detail,
                                    size_t detail_size) {
  int mib[4] = { CTL_KERN, KERN_PROC, KERN_PROC_FILEDESC, pid };
  size_t buf_size = 0;
  unsigned char *buf;
  unsigned char *ptr;
  unsigned char *end;
  int has_manual = 0;
  int has_config = 0;

  if(sysctl(mib, 4, NULL, &buf_size, NULL, 0) != 0) {
    return 0;
  }
  if(buf_size == 0) return 0;

  buf = malloc(buf_size);
  if(!buf) {
    set_detail(detail, detail_size, "allocate process file descriptor buffer");
    return -1;
  }
  if(sysctl(mib, 4, buf, &buf_size, NULL, 0) != 0) {
    free(buf);
    return 0;
  }

  ptr = buf;
  end = buf + buf_size;
  while(ptr + sizeof(int) <= end) {
    struct kinfo_file *kf = (struct kinfo_file *)ptr;
    int struct_size = kf->kf_structsize;
    if(struct_size <= 0 || ptr + struct_size > end) {
      break;
    }
    if(kf->kf_type == KF_TYPE_VNODE && kf->kf_path[0]) {
      if(strcmp(kf->kf_path, SHADOWMOUNT_MANUAL_LIST) == 0) {
        has_manual = 1;
      } else if(strcmp(kf->kf_path, SHADOWMOUNT_CONFIG) == 0) {
        has_config = 1;
      }
      if(has_manual && has_config) break;
    }
    ptr += struct_size;
  }
  free(buf);
  return has_manual && has_config ? 1 : 0;
}

static int
find_running_shadowmount_path(char *out, size_t out_size,
                              char *detail, size_t detail_size) {
  int mib[4] = { CTL_KERN, KERN_PROC, KERN_PROC_PROC, 0 };
  size_t buf_size = 0;
  unsigned char *buf;
  unsigned char *ptr;
  unsigned char *end;
  pid_t self = getpid();
  int matches = 0;
  char selected[PATH_MAX];
  char local_detail[256];

  if(out && out_size) out[0] = 0;
  if(detail && detail_size) detail[0] = 0;
  selected[0] = 0;

  if(sysctl(mib, 4, NULL, &buf_size, NULL, 0) != 0) {
    set_detail_errno(detail, detail_size, "enumerate processes");
    return -1;
  }
  if(buf_size == 0) {
    set_detail(detail, detail_size, "no processes returned");
    return -1;
  }

  buf = malloc(buf_size);
  if(!buf) {
    set_detail(detail, detail_size, "allocate process buffer");
    return -1;
  }
  if(sysctl(mib, 4, buf, &buf_size, NULL, 0) != 0) {
    free(buf);
    set_detail_errno(detail, detail_size, "read process list");
    return -1;
  }

  ptr = buf;
  end = buf + buf_size;
  while(ptr + sizeof(int) <= end) {
    struct kinfo_proc *kp = (struct kinfo_proc *)ptr;
    int struct_size = kp->ki_structsize;
    int fingerprint;
    char path[PATH_MAX];

    if(struct_size <= 0 || ptr + struct_size > end) break;
    ptr += struct_size;

    if(kp->ki_pid <= 0 || kp->ki_pid == self) continue;
    local_detail[0] = 0;
    fingerprint = process_has_shadowmount_fingerprint(kp->ki_pid,
                                                      local_detail,
                                                      sizeof(local_detail));
    if(fingerprint <= 0) {
      if(fingerprint < 0 && detail && detail_size && !detail[0]) {
        snprintf(detail, detail_size, "%s", local_detail);
      }
      continue;
    }

    path[0] = 0;
    local_detail[0] = 0;
    if(process_path_for_pid(kp->ki_pid, path, sizeof(path), local_detail,
                            sizeof(local_detail)) != 0) {
      if(detail && detail_size && !detail[0]) {
        snprintf(detail, detail_size, "%s", local_detail);
      }
      continue;
    }
    local_detail[0] = 0;
    if(!path_is_regular_elf(path, local_detail, sizeof(local_detail))) {
      if(detail && detail_size && !detail[0]) {
        snprintf(detail, detail_size, "%s", local_detail);
      }
      continue;
    }

    matches++;
    if(matches == 1) {
      snprintf(selected, sizeof(selected), "%s", path);
    }
  }
  free(buf);

  if(matches == 1) {
    snprintf(out, out_size, "%s", selected);
    if(detail && detail_size) {
      snprintf(detail, detail_size, "ShadowMount process executable: %s", selected);
    }
    return 0;
  }
  if(matches == 0) {
    set_detail(detail, detail_size,
               "no running ShadowMount process with manual.lst and config.ini open");
  } else {
    if(detail && detail_size) {
      snprintf(detail, detail_size,
               "multiple running ShadowMount-like processes found (%d)", matches);
    }
  }
  return -1;
}

static int
normalize_image_name(const char *path_or_name, char *out, size_t out_size) {
  const char *name = base_name(path_or_name);
  size_t len = name ? strlen(name) : 0;
  if(!out || out_size == 0 || len == 0 || len >= out_size) return -1;
  for(size_t i = 0; i < len; i++) {
    unsigned char ch = (unsigned char)name[i];
    if(ch < 0x20 || ch >= 0x7f || ch == ':' || ch == '/' ||
       ch == '\\') {
      return -1;
    }
  }
  memcpy(out, name, len + 1);
  return 0;
}

static int
title_id_hint_safe(const char *title_id) {
  size_t len = title_id ? strlen(title_id) : 0;
  if(len == 0 || len > 32) return 0;
  for(size_t i = 0; i < len; i++) {
    unsigned char ch = (unsigned char)title_id[i];
    if(!isalnum(ch) && ch != '_' && ch != '-') return 0;
  }
  return 1;
}

static int
build_title_hint_name(const char *title_id, const char *suffix,
                      char *out, size_t out_size) {
  if(!title_id_hint_safe(title_id) || !suffix || !out || out_size == 0) {
    return -1;
  }
  int n = snprintf(out, out_size, "%s%s", title_id, suffix);
  return (n < 0 || (size_t)n >= out_size) ? -1 : 0;
}

static char *
trim_left(char *s) {
  while(s && *s && isspace((unsigned char)*s)) s++;
  return s;
}

static void
trim_right(char *s) {
  size_t len = s ? strlen(s) : 0;
  while(len > 0 && isspace((unsigned char)s[len - 1])) {
    s[--len] = 0;
  }
}

static int
line_matches_image_sector(const char *line, const char *image_name) {
  char tmp[512];
  int n = snprintf(tmp, sizeof(tmp), "%s", line ? line : "");
  if(n < 0 || (size_t)n >= sizeof(tmp)) return 0;

  char *p = trim_left(tmp);
  if(!*p || *p == '#' || *p == ';') return 0;
  char *eq = strchr(p, '=');
  if(!eq) return 0;
  *eq = 0;
  trim_right(p);
  if(strcasecmp(p, "image_sector")) return 0;

  char *value = trim_left(eq + 1);
  char *colon = strrchr(value, ':');
  if(!colon) return 0;
  *colon = 0;
  trim_right(value);
  value = trim_left(value);
  return !strcasecmp(value, image_name);
}

static int
line_matches_image_mode(const char *line, const char *image_name) {
  char tmp[512];
  int n = snprintf(tmp, sizeof(tmp), "%s", line ? line : "");
  if(n < 0 || (size_t)n >= sizeof(tmp)) return 0;

  char *p = trim_left(tmp);
  if(!*p || *p == '#' || *p == ';') return 0;
  char *eq = strchr(p, '=');
  if(!eq) return 0;
  *eq = 0;
  trim_right(p);
  if(strcasecmp(p, "image_ro") && strcasecmp(p, "image_rw")) return 0;

  char *value = trim_left(eq + 1);
  trim_right(value);
  return !strcasecmp(value, image_name);
}

static int
upsert_image_mode_hint(const char *path_or_name, int read_only,
                       char *err, size_t err_size) {
  char image_name[256];
  char line[384];
  FILE *in = NULL;
  FILE *out = NULL;
  int found = 0;
  int rc = -1;

  if(normalize_image_name(path_or_name, image_name, sizeof(image_name)) != 0) {
    set_err(err, err_size, "bad ShadowMount image mode hint name");
    return -1;
  }
  if(mkdir_if_needed(SHADOWMOUNT_DIR) != 0) {
    set_errno_err(err, err_size, "create /data/shadowmount");
    return -1;
  }

  int n = snprintf(line, sizeof(line), "image_%s=%s\n",
                   read_only ? "ro" : "rw", image_name);
  if(n < 0 || (size_t)n >= sizeof(line)) {
    set_err(err, err_size, "ShadowMount image mode hint line too long");
    return -1;
  }

  in = fopen(SHADOWMOUNT_CONFIG, "r");
  out = fopen(SHADOWMOUNT_CONFIG_TMP, "w");
  if(!out) {
    if(in) fclose(in);
    set_errno_err(err, err_size, "open ShadowMount config temp");
    return -1;
  }

  if(in) {
    char existing[512];
    while(fgets(existing, sizeof(existing), in)) {
      if(line_matches_image_mode(existing, image_name)) {
        if(!found && fputs(line, out) == EOF) {
          set_errno_err(err, err_size, "write ShadowMount image mode hint");
          goto done;
        }
        found = 1;
        continue;
      }
      if(fputs(existing, out) == EOF) {
        set_errno_err(err, err_size, "write ShadowMount config");
        goto done;
      }
    }
    if(ferror(in)) {
      set_errno_err(err, err_size, "read ShadowMount config");
      goto done;
    }
  }

  if(!found && fputs(line, out) == EOF) {
    set_errno_err(err, err_size, "append ShadowMount image mode hint");
    goto done;
  }
  if(fflush(out) != 0 || fsync(fileno(out)) != 0) {
    set_errno_err(err, err_size, "flush ShadowMount config");
    goto done;
  }
  if(fclose(out) != 0) {
    out = NULL;
    set_errno_err(err, err_size, "close ShadowMount config temp");
    goto done;
  }
  out = NULL;
  if(rename(SHADOWMOUNT_CONFIG_TMP, SHADOWMOUNT_CONFIG) != 0) {
    set_errno_err(err, err_size, "replace ShadowMount config");
    goto done;
  }
  chmod(SHADOWMOUNT_CONFIG, 0666);
  rc = 0;

done:
  if(in) fclose(in);
  if(out) fclose(out);
  if(rc != 0) unlink(SHADOWMOUNT_CONFIG_TMP);
  return rc;
}

static int
remove_image_mode_hint(const char *path_or_name, char *err, size_t err_size) {
  char image_name[256];
  FILE *in = NULL;
  FILE *out = NULL;
  int removed = 0;
  int rc = -1;

  if(normalize_image_name(path_or_name, image_name, sizeof(image_name)) != 0) {
    set_err(err, err_size, "bad ShadowMount image mode hint name");
    return -1;
  }
  if(mkdir_if_needed(SHADOWMOUNT_DIR) != 0) {
    set_errno_err(err, err_size, "create /data/shadowmount");
    return -1;
  }

  in = fopen(SHADOWMOUNT_CONFIG, "r");
  if(!in) {
    if(errno == ENOENT) return 0;
    set_errno_err(err, err_size, "open ShadowMount config");
    return -1;
  }
  out = fopen(SHADOWMOUNT_CONFIG_TMP, "w");
  if(!out) {
    fclose(in);
    set_errno_err(err, err_size, "open ShadowMount config temp");
    return -1;
  }

  char existing[512];
  while(fgets(existing, sizeof(existing), in)) {
    if(line_matches_image_mode(existing, image_name)) {
      removed = 1;
      continue;
    }
    if(fputs(existing, out) == EOF) {
      set_errno_err(err, err_size, "write ShadowMount config");
      goto done;
    }
  }
  if(ferror(in)) {
    set_errno_err(err, err_size, "read ShadowMount config");
    goto done;
  }
  if(fflush(out) != 0 || fsync(fileno(out)) != 0) {
    set_errno_err(err, err_size, "flush ShadowMount config");
    goto done;
  }
  if(fclose(out) != 0) {
    out = NULL;
    set_errno_err(err, err_size, "close ShadowMount config temp");
    goto done;
  }
  out = NULL;
  if(rename(SHADOWMOUNT_CONFIG_TMP, SHADOWMOUNT_CONFIG) != 0) {
    set_errno_err(err, err_size, "replace ShadowMount config");
    goto done;
  }
  chmod(SHADOWMOUNT_CONFIG, 0666);
  if(removed) sync();
  rc = 0;

done:
  if(in) fclose(in);
  if(out) fclose(out);
  if(rc != 0) unlink(SHADOWMOUNT_CONFIG_TMP);
  return rc;
}

static int
upsert_sector_hint(const char *path_or_name, unsigned sector,
                   char *err, size_t err_size) {
  char image_name[256];
  char line[384];
  FILE *in = NULL;
  FILE *out = NULL;
  int found = 0;
  int rc = -1;

  if(normalize_image_name(path_or_name, image_name, sizeof(image_name)) != 0) {
    set_err(err, err_size, "bad ShadowMount image hint name");
    return -1;
  }
  if(sector != 4096U && sector != 8192U && sector != 16384U &&
     sector != 32768U && sector != 65536U) {
    set_err(err, err_size, "bad ShadowMount sector hint");
    return -1;
  }
  if(mkdir_if_needed(SHADOWMOUNT_DIR) != 0) {
    set_errno_err(err, err_size, "create /data/shadowmount");
    return -1;
  }

  int n = snprintf(line, sizeof(line), "image_sector=%s:%u\n",
                   image_name, sector);
  if(n < 0 || (size_t)n >= sizeof(line)) {
    set_err(err, err_size, "ShadowMount hint line too long");
    return -1;
  }

  in = fopen(SHADOWMOUNT_AUTOTUNE, "r");
  out = fopen(SHADOWMOUNT_AUTOTUNE_TMP, "w");
  if(!out) {
    if(in) fclose(in);
    set_errno_err(err, err_size, "open ShadowMount autotune temp");
    return -1;
  }

  if(in) {
    char existing[512];
    while(fgets(existing, sizeof(existing), in)) {
      if(line_matches_image_sector(existing, image_name)) {
        if(!found && fputs(line, out) == EOF) {
          set_errno_err(err, err_size, "write ShadowMount hint");
          goto done;
        }
        found = 1;
        continue;
      }
      if(fputs(existing, out) == EOF) {
        set_errno_err(err, err_size, "write ShadowMount autotune");
        goto done;
      }
    }
    if(ferror(in)) {
      set_errno_err(err, err_size, "read ShadowMount autotune");
      goto done;
    }
  }

  if(!found && fputs(line, out) == EOF) {
    set_errno_err(err, err_size, "append ShadowMount hint");
    goto done;
  }
  if(fflush(out) != 0 || fsync(fileno(out)) != 0) {
    set_errno_err(err, err_size, "flush ShadowMount autotune");
    goto done;
  }
  if(fclose(out) != 0) {
    out = NULL;
    set_errno_err(err, err_size, "close ShadowMount autotune temp");
    goto done;
  }
  out = NULL;
  if(rename(SHADOWMOUNT_AUTOTUNE_TMP, SHADOWMOUNT_AUTOTUNE) != 0) {
    set_errno_err(err, err_size, "replace ShadowMount autotune");
    goto done;
  }
  chmod(SHADOWMOUNT_AUTOTUNE, 0666);
  rc = 0;

done:
  if(in) fclose(in);
  if(out) fclose(out);
  if(rc != 0) unlink(SHADOWMOUNT_AUTOTUNE_TMP);
  return rc;
}

static int
remove_sector_hint(const char *path_or_name, char *err, size_t err_size) {
  char image_name[256];
  FILE *in = NULL;
  FILE *out = NULL;
  int removed = 0;
  int rc = -1;

  if(normalize_image_name(path_or_name, image_name, sizeof(image_name)) != 0) {
    set_err(err, err_size, "bad ShadowMount image hint name");
    return -1;
  }
  if(mkdir_if_needed(SHADOWMOUNT_DIR) != 0) {
    set_errno_err(err, err_size, "create /data/shadowmount");
    return -1;
  }

  in = fopen(SHADOWMOUNT_AUTOTUNE, "r");
  if(!in) {
    if(errno == ENOENT) return 0;
    set_errno_err(err, err_size, "open ShadowMount autotune");
    return -1;
  }
  out = fopen(SHADOWMOUNT_AUTOTUNE_TMP, "w");
  if(!out) {
    fclose(in);
    set_errno_err(err, err_size, "open ShadowMount autotune temp");
    return -1;
  }

  char existing[512];
  while(fgets(existing, sizeof(existing), in)) {
    if(line_matches_image_sector(existing, image_name)) {
      removed = 1;
      continue;
    }
    if(fputs(existing, out) == EOF) {
      set_errno_err(err, err_size, "write ShadowMount autotune");
      goto done;
    }
  }
  if(ferror(in)) {
    set_errno_err(err, err_size, "read ShadowMount autotune");
    goto done;
  }
  if(fflush(out) != 0 || fsync(fileno(out)) != 0) {
    set_errno_err(err, err_size, "flush ShadowMount autotune");
    goto done;
  }
  if(fclose(out) != 0) {
    out = NULL;
    set_errno_err(err, err_size, "close ShadowMount autotune temp");
    goto done;
  }
  out = NULL;
  if(rename(SHADOWMOUNT_AUTOTUNE_TMP, SHADOWMOUNT_AUTOTUNE) != 0) {
    set_errno_err(err, err_size, "replace ShadowMount autotune");
    goto done;
  }
  chmod(SHADOWMOUNT_AUTOTUNE, 0666);
  if(removed) {
    sync();
  }
  rc = 0;

done:
  if(in) fclose(in);
  if(out) fclose(out);
  if(rc != 0) unlink(SHADOWMOUNT_AUTOTUNE_TMP);
  return rc;
}

static int
remove_image_mode_and_sector_hints(const char *path_or_name,
                                   char *err, size_t err_size) {
  if(!path_or_name || !path_or_name[0]) return 0;
  if(remove_image_mode_hint(path_or_name, err, err_size) != 0) return -1;
  if(remove_sector_hint(path_or_name, err, err_size) != 0) return -1;
  return 0;
}

int
gc_shadowmount_write_pfsc_hints(const char *outer_path,
                                const char *nested_name,
                                int nested_type,
                                char *err,
                                size_t err_size) {
  if(err && err_size) err[0] = 0;
  if(upsert_sector_hint(outer_path, SHADOWMOUNT_PFSC_SECTOR,
                        err, err_size) != 0) {
    return -1;
  }
  if(nested_type == PFS_NESTED_EXFAT && nested_name && nested_name[0]) {
    if(upsert_image_mode_hint(nested_name, 1, err, err_size) != 0) {
      return -1;
    }
    if(remove_sector_hint(nested_name, err, err_size) != 0) {
      return -1;
    }
  }
  return 0;
}

int
gc_shadowmount_remove_title_pfsc_hints(const char *title_id,
                                       const char *outer_path,
                                       char *err,
                                       size_t err_size) {
  char title_image[256];

  if(err && err_size) err[0] = 0;
  if(outer_path && outer_path[0] &&
     remove_image_mode_and_sector_hints(outer_path, err, err_size) != 0) {
    return -1;
  }
  if(!title_id || !title_id[0]) return 0;
  if(!title_id_hint_safe(title_id)) {
    set_err(err, err_size, "bad ShadowMount title hint name");
    return -1;
  }

  if(build_title_hint_name(title_id, ".exfat", title_image,
                           sizeof(title_image)) != 0) {
    set_err(err, err_size, "ShadowMount title exFAT hint name too long");
    return -1;
  }
  if(remove_image_mode_and_sector_hints(title_image, err, err_size) != 0) {
    return -1;
  }

  if(build_title_hint_name(title_id, ".ffpfsc", title_image,
                           sizeof(title_image)) != 0) {
    set_err(err, err_size, "ShadowMount title PFSC hint name too long");
    return -1;
  }
  if(remove_image_mode_and_sector_hints(title_image, err, err_size) != 0) {
    return -1;
  }
  return 0;
}

int
gc_shadowmount_prepare_pfsc_hints_for_title(const char *title_id,
                                            const char *outer_path,
                                            const char *nested_name,
                                            int nested_type,
                                            char *err,
                                            size_t err_size) {
  if(err && err_size) err[0] = 0;
  if(gc_shadowmount_remove_title_pfsc_hints(title_id, outer_path,
                                           err, err_size) != 0) {
    return -1;
  }
  return gc_shadowmount_write_pfsc_hints(outer_path, nested_name,
                                         nested_type, err, err_size);
}

int
gc_shadowmount_remove_pfsc_hints(const char *outer_path,
                                 const char *nested_name,
                                 int nested_type,
                                 char *err,
                                 size_t err_size) {
  if(err && err_size) err[0] = 0;
  if(outer_path && outer_path[0] &&
     remove_sector_hint(outer_path, err, err_size) != 0) {
    return -1;
  }
  if(nested_type == PFS_NESTED_EXFAT && nested_name && nested_name[0]) {
    if(remove_image_mode_hint(nested_name, err, err_size) != 0) return -1;
    if(remove_sector_hint(nested_name, err, err_size) != 0) return -1;
  }
  return 0;
}

int
gc_shadowmount_remove_outer_sector_hint(const char *outer_path,
                                        char *err,
                                        size_t err_size) {
  if(err && err_size) err[0] = 0;
  if(!outer_path || !outer_path[0]) return 0;
  return remove_sector_hint(outer_path, err, err_size);
}

int
gc_shadowmount_request_scan(char *err, size_t err_size) {
  int fd;
  struct timeval tv[2];
  struct stat st;
  if(err && err_size) err[0] = 0;
  if(mkdir_if_needed(SHADOWMOUNT_DIR) != 0) {
    set_errno_err(err, err_size, "create /data/shadowmount");
    return -1;
  }
  fd = open(SHADOWMOUNT_MANUAL_LIST, O_WRONLY | O_CREAT | O_APPEND, 0666);
  if(fd < 0) {
    set_errno_err(err, err_size, "open ShadowMount manual.lst");
    return -1;
  }
  close(fd);
  if(gettimeofday(&tv[0], NULL) != 0) {
    set_errno_err(err, err_size, "time ShadowMount scan request");
    return -1;
  }
  if(stat(SHADOWMOUNT_MANUAL_LIST, &st) == 0 && tv[0].tv_sec <= st.st_mtime) {
    tv[0].tv_sec = st.st_mtime + 1;
    tv[0].tv_usec = 0;
  }
  tv[1] = tv[0];
  if(utimes(SHADOWMOUNT_MANUAL_LIST, tv) != 0) {
    set_errno_err(err, err_size, "touch ShadowMount manual.lst");
    return -1;
  }
  chmod(SHADOWMOUNT_MANUAL_LIST, 0666);
  return 0;
}

int
gc_shadowmount_restart_running(char *detail, size_t detail_size) {
  char path[PATH_MAX];
  int rc;
  if(detail && detail_size) detail[0] = 0;
  if(find_running_shadowmount_path(path, sizeof(path), detail, detail_size) != 0) {
    return -1;
  }
  rc = sceKernelLoadStartModule(path, 0, NULL, 0, NULL, NULL);
  if(rc <= 0) {
    if(detail && detail_size) {
      snprintf(detail, detail_size, "launch ShadowMount executable failed rc=0x%08x path=%s",
               (unsigned)rc, path);
    }
    return -1;
  }
  if(detail && detail_size) {
    snprintf(detail, detail_size, "launched ShadowMount executable rc=0x%08x path=%s",
             (unsigned)rc, path);
  }
  return 0;
}
