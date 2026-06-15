/*
 * Game Compressor - ShadowMountPlus compatibility hints.
 */

#include "gc_shadowmount.h"

#include <ctype.h>
#include <dirent.h>
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
#include <sys/types.h>
#include <sys/user.h>
#include <unistd.h>

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>

#define SHADOWMOUNT_DIR "/data/shadowmount"
#define SHADOWMOUNT_CONFIG SHADOWMOUNT_DIR "/config.ini"
#define SHADOWMOUNT_CONFIG_TMP SHADOWMOUNT_DIR "/config.ini.game-compressor.tmp"
#define SHADOWMOUNT_AUTOTUNE SHADOWMOUNT_DIR "/autotune.ini"
#define SHADOWMOUNT_AUTOTUNE_TMP SHADOWMOUNT_DIR "/autotune.ini.game-compressor.tmp"
#define SHADOWMOUNT_MANUAL_LIST SHADOWMOUNT_DIR "/manual.lst"
#define SHADOWMOUNT_MANUAL_LIST_TMP \
  SHADOWMOUNT_DIR "/manual.lst.game-compressor.tmp"
#define SHADOWMOUNT_PAYLOAD_MANAGER_ELF \
  "/data/pldmgr/payloads/ShadowMountPlus/shadowmountplus.elf"
#define SHADOWMOUNT_PAYLOAD_MANAGER_ELF_LEGACY \
  "/data/pldmgr/payloads/shadowmountplus/shadowmountplus.elf"
#define SHADOWMOUNT_PAYLOAD_MANAGER_DIR \
  "/data/pldmgr/payloads/ShadowMountPlus"
#define SHADOWMOUNT_PAYLOAD_MANAGER_DIR_LEGACY \
  "/data/pldmgr/payloads/shadowmountplus"
#define PAYLOAD_MANAGER_PORT 8084
#define LOCAL_HTTP_TIMEOUT_SECONDS 5
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

static int
replace_shadowmount_temp(FILE **out, const char *tmp_path,
                         const char *final_path, const char *flush_context,
                         const char *close_context,
                         const char *replace_context,
                         char *err, size_t err_size) {
  if(fflush(*out) != 0 || fsync(fileno(*out)) != 0) {
    set_errno_err(err, err_size, flush_context);
    return -1;
  }
  if(fclose(*out) != 0) {
    *out = NULL;
    set_errno_err(err, err_size, close_context);
    return -1;
  }
  *out = NULL;
  if(rename(tmp_path, final_path) != 0) {
    set_errno_err(err, err_size, replace_context);
    return -1;
  }
  chmod(final_path, 0666);
  return 0;
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
line_matches_config_key(const char *line, const char *key) {
  char tmp[512];
  int n = snprintf(tmp, sizeof(tmp), "%s", line ? line : "");
  if(n < 0 || (size_t)n >= sizeof(tmp)) return 0;

  char *p = trim_left(tmp);
  if(!*p || *p == '#' || *p == ';') return 0;
  char *eq = strchr(p, '=');
  if(!eq) return 0;
  *eq = 0;
  trim_right(p);
  return !strcasecmp(p, key ? key : "");
}

static int
upsert_config_key_value(const char *key, const char *value,
                        char *err, size_t err_size) {
  char line[384];
  FILE *in = NULL;
  FILE *out = NULL;
  int found = 0;
  int rc = -1;

  if(!key || !key[0] || !value) {
    set_err(err, err_size, "bad ShadowMount config key");
    return -1;
  }
  if(mkdir_if_needed(SHADOWMOUNT_DIR) != 0) {
    set_errno_err(err, err_size, "create /data/shadowmount");
    return -1;
  }
  int n = snprintf(line, sizeof(line), "%s=%s\n", key, value);
  if(n < 0 || (size_t)n >= sizeof(line)) {
    set_err(err, err_size, "ShadowMount config line too long");
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
      if(line_matches_config_key(existing, key)) {
        if(!found && fputs(line, out) == EOF) {
          set_errno_err(err, err_size, "write ShadowMount config key");
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
    set_errno_err(err, err_size, "append ShadowMount config key");
    goto done;
  }
  if(replace_shadowmount_temp(&out, SHADOWMOUNT_CONFIG_TMP,
                              SHADOWMOUNT_CONFIG,
                              "flush ShadowMount config",
                              "close ShadowMount config temp",
                              "replace ShadowMount config",
                              err, err_size) != 0) {
    goto done;
  }
  rc = 0;

done:
  if(in) fclose(in);
  if(out) fclose(out);
  if(rc != 0) unlink(SHADOWMOUNT_CONFIG_TMP);
  return rc;
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
  if(replace_shadowmount_temp(&out, SHADOWMOUNT_CONFIG_TMP,
                              SHADOWMOUNT_CONFIG,
                              "flush ShadowMount config",
                              "close ShadowMount config temp",
                              "replace ShadowMount config",
                              err, err_size) != 0) {
    goto done;
  }
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
  if(replace_shadowmount_temp(&out, SHADOWMOUNT_CONFIG_TMP,
                              SHADOWMOUNT_CONFIG,
                              "flush ShadowMount config",
                              "close ShadowMount config temp",
                              "replace ShadowMount config",
                              err, err_size) != 0) {
    goto done;
  }
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
  if(replace_shadowmount_temp(&out, SHADOWMOUNT_AUTOTUNE_TMP,
                              SHADOWMOUNT_AUTOTUNE,
                              "flush ShadowMount autotune",
                              "close ShadowMount autotune temp",
                              "replace ShadowMount autotune",
                              err, err_size) != 0) {
    goto done;
  }
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
  if(replace_shadowmount_temp(&out, SHADOWMOUNT_AUTOTUNE_TMP,
                              SHADOWMOUNT_AUTOTUNE,
                              "flush ShadowMount autotune",
                              "close ShadowMount autotune temp",
                              "replace ShadowMount autotune",
                              err, err_size) != 0) {
    goto done;
  }
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

  if(build_title_hint_name(title_id, ".pfs", title_image,
                           sizeof(title_image)) != 0) {
    set_err(err, err_size, "ShadowMount title PFS hint name too long");
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
  if(gc_shadowmount_enable_pfsc_direct_game(err, err_size) != 0) return -1;
  if(gc_shadowmount_remove_title_pfsc_hints(title_id, outer_path,
                                           err, err_size) != 0) {
    return -1;
  }
  return gc_shadowmount_write_pfsc_hints(outer_path, nested_name,
                                         nested_type, err, err_size);
}

int
gc_shadowmount_prepare_image_hints_for_title(const char *title_id,
                                             const char *image_path,
                                             int nested_type,
                                             char *err,
                                             size_t err_size) {
  if(err && err_size) err[0] = 0;
  if(nested_type != PFS_NESTED_PFS && nested_type != PFS_NESTED_EXFAT) {
    set_err(err, err_size, "bad ShadowMount image type");
    return -1;
  }
  if(gc_shadowmount_remove_title_pfsc_hints(title_id, image_path,
                                           err, err_size) != 0) {
    return -1;
  }
  if(upsert_image_mode_hint(image_path, 1, err, err_size) != 0) {
    return -1;
  }
  return remove_sector_hint(image_path, err, err_size);
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
gc_shadowmount_enable_pfsc_direct_game(char *err, size_t err_size) {
  if(err && err_size) err[0] = 0;
  return upsert_config_key_value("pfsc_direct_game", "1", err, err_size);
}

static int
shadowmount_title_id_valid(const char *title_id) {
  if(!title_id || strlen(title_id) != 9) return 0;
  for(size_t i = 0; i < 9; i++) {
    if(!isalnum((unsigned char)title_id[i])) return 0;
  }
  return 1;
}

static void
manual_list_strip_line(char *line) {
  size_t len;
  if(!line) return;
  len = strlen(line);
  while(len > 0 && (line[len - 1] == '\n' || line[len - 1] == '\r')) {
    line[--len] = 0;
  }
}

static int
manual_list_line_matches_path(char *line, const char *source_path) {
  if(!line || !source_path) return 0;
  manual_list_strip_line(line);
  return strcmp(line, source_path) == 0;
}

static int
manual_list_line_matches_title(const char *line, const char *title_id) {
  const char *name;
  if(!line || !shadowmount_title_id_valid(title_id)) return 0;
  name = base_name(line);
  return strncmp(name, title_id, 9) == 0 &&
      (name[9] == 0 || name[9] == '-' || name[9] == '.');
}

static int
manual_list_upsert_source(const char *title_id, const char *source_path,
                          char *err, size_t err_size) {
  FILE *in = NULL;
  FILE *out = NULL;
  char line[PATH_MAX + 8];
  int rc = -1;
  int replace_same_title = shadowmount_title_id_valid(title_id);

  in = fopen(SHADOWMOUNT_MANUAL_LIST, "r");
  if(!in && errno != ENOENT) {
    set_errno_err(err, err_size, "open ShadowMount manual.lst");
    goto done;
  }
  out = fopen(SHADOWMOUNT_MANUAL_LIST_TMP, "w");
  if(!out) {
    set_errno_err(err, err_size, "open ShadowMount manual temp");
    goto done;
  }
  if(in) {
    while(fgets(line, sizeof(line), in)) {
      if(manual_list_line_matches_path(line, source_path)) continue;
      if(replace_same_title &&
         manual_list_line_matches_title(line, title_id)) {
        continue;
      }
      if(line[0] == 0) continue;
      if(fprintf(out, "%s\n", line) < 0) {
        set_errno_err(err, err_size, "write ShadowMount manual source");
        goto done;
      }
    }
    if(ferror(in)) {
      set_errno_err(err, err_size, "read ShadowMount manual.lst");
      goto done;
    }
  }
  if(fprintf(out, "%s\n", source_path) < 0) {
    set_errno_err(err, err_size, "write ShadowMount manual source");
    goto done;
  }
  if(replace_shadowmount_temp(&out, SHADOWMOUNT_MANUAL_LIST_TMP,
                              SHADOWMOUNT_MANUAL_LIST,
                              "flush ShadowMount manual.lst",
                              "close ShadowMount manual.lst",
                              "replace ShadowMount manual.lst",
                              err, err_size) != 0) {
    goto done;
  }
  rc = 0;

done:
  if(in) fclose(in);
  if(out) fclose(out);
  if(rc != 0) unlink(SHADOWMOUNT_MANUAL_LIST_TMP);
  return rc;
}

static int
request_source_scan_locked(const char *title_id, const char *source_path,
                           char *err, size_t err_size) {
  if(err && err_size) err[0] = 0;
  if(!source_path || source_path[0] != '/') {
    set_err(err, err_size, "bad ShadowMount source path");
    return -1;
  }
  if(mkdir_if_needed(SHADOWMOUNT_DIR) != 0) {
    set_errno_err(err, err_size, "create /data/shadowmount");
    return -1;
  }
  if(manual_list_upsert_source(title_id, source_path, err, err_size) != 0) {
    return -1;
  }
  chmod(SHADOWMOUNT_MANUAL_LIST, 0666);
  return gc_shadowmount_request_scan(err, err_size);
}

int
gc_shadowmount_request_source_scan(const char *source_path,
                                   char *err, size_t err_size) {
  return request_source_scan_locked(NULL, source_path, err, err_size);
}

int
gc_shadowmount_request_title_source_scan(const char *title_id,
                                         const char *source_path,
                                         char *err, size_t err_size) {
  if(!shadowmount_title_id_valid(title_id)) {
    set_err(err, err_size, "bad ShadowMount title id");
    return -1;
  }
  return request_source_scan_locked(title_id, source_path, err, err_size);
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

static int
ends_with_ci_local(const char *s, const char *suffix) {
  size_t n, m, i;
  if(!s || !suffix) return 0;
  n = strlen(s);
  m = strlen(suffix);
  if(m > n) return 0;
  s += n - m;
  for(i = 0; i < m; i++) {
    if(tolower((unsigned char)s[i]) !=
       tolower((unsigned char)suffix[i])) {
      return 0;
    }
  }
  return 1;
}

static int
name_contains_shadowmount(const char *name) {
  const char *needle = "shadowmount";
  size_t n = strlen(needle);
  if(!name) return 0;
  for(; *name; name++) {
    size_t i;
    for(i = 0; i < n; i++) {
      if(!name[i] ||
         tolower((unsigned char)name[i]) != (unsigned char)needle[i]) {
        break;
      }
    }
    if(i == n) return 1;
  }
  return 0;
}

static int
find_shadowmount_payload_manager_elf(char *path, size_t path_size,
                                     char *detail, size_t detail_size) {
  const char *dirs[] = {
      SHADOWMOUNT_PAYLOAD_MANAGER_DIR,
      SHADOWMOUNT_PAYLOAD_MANAGER_DIR_LEGACY,
  };
  char first_valid[PATH_MAX];
  char candidate[PATH_MAX];
  char candidate_detail[256];
  size_t i;
  first_valid[0] = 0;
  if(path && path_size) path[0] = 0;
  if(detail && detail_size) detail[0] = 0;
  for(i = 0; i < sizeof(dirs) / sizeof(dirs[0]); i++) {
    DIR *d = opendir(dirs[i]);
    struct dirent *ent;
    if(!d) {
      if(detail && detail_size && !detail[0]) {
        snprintf(detail, detail_size, "open %s: %s", dirs[i], strerror(errno));
      }
      continue;
    }
    while((ent = readdir(d))) {
      if(ent->d_name[0] == '.' || !ends_with_ci_local(ent->d_name, ".elf")) {
        continue;
      }
      if(snprintf(candidate, sizeof(candidate), "%s/%s", dirs[i],
                  ent->d_name) >= (int)sizeof(candidate)) {
        continue;
      }
      candidate_detail[0] = 0;
      if(!path_is_regular_elf(candidate, candidate_detail,
                              sizeof(candidate_detail))) {
        if(detail && detail_size && !detail[0]) {
          snprintf(detail, detail_size, "%s",
                   candidate_detail[0] ? candidate_detail : candidate);
        }
        continue;
      }
      if(!first_valid[0]) {
        snprintf(first_valid, sizeof(first_valid), "%s", candidate);
      }
      if(name_contains_shadowmount(ent->d_name)) {
        closedir(d);
        snprintf(path, path_size, "%s", candidate);
        return 0;
      }
    }
    closedir(d);
  }
  if(first_valid[0]) {
    snprintf(path, path_size, "%s", first_valid);
    return 0;
  }
  if(detail && detail_size && !detail[0]) {
    snprintf(detail, detail_size, "%s", "no ShadowMountPlus payload ELF found");
  }
  return -1;
}

static int
local_send_all(int fd, const char *data, size_t size) {
  size_t off = 0;
  while(off < size) {
    ssize_t n = send(fd, data + off, size - off, 0);
    if(n < 0) {
      if(errno == EINTR) continue;
      return -1;
    }
    if(n == 0) return -1;
    off += (size_t)n;
  }
  return 0;
}

static int
payload_manager_launch(const char *elf_path, char *detail, size_t detail_size) {
  int fd;
  struct timeval timeout;
  struct sockaddr_in addr;
  char request[1536];
  char response[4096];
  size_t used = 0;
  int status = 0;
  int n;
  if(detail && detail_size) detail[0] = 0;
  fd = socket(AF_INET, SOCK_STREAM, 0);
  if(fd < 0) {
    set_detail_errno(detail, detail_size, "Payload Manager socket");
    return -1;
  }
  timeout.tv_sec = LOCAL_HTTP_TIMEOUT_SECONDS;
  timeout.tv_usec = 0;
  setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &timeout, sizeof(timeout));
  setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout));
  memset(&addr, 0, sizeof(addr));
  addr.sin_family = AF_INET;
  addr.sin_port = htons(PAYLOAD_MANAGER_PORT);
  addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
  if(connect(fd, (struct sockaddr *)&addr, sizeof(addr)) != 0) {
    set_detail_errno(detail, detail_size, "connect Payload Manager");
    close(fd);
    return -1;
  }
  n = snprintf(request, sizeof(request),
               "GET /loadpayload:%s HTTP/1.1\r\n"
               "Host: 127.0.0.1:%d\r\n"
               "Connection: close\r\n"
               "\r\n",
               elf_path ? elf_path : "", PAYLOAD_MANAGER_PORT);
  if(n < 0 || (size_t)n >= sizeof(request)) {
    set_detail(detail, detail_size, "Payload Manager request too long");
    close(fd);
    return -1;
  }
  if(local_send_all(fd, request, (size_t)n) != 0) {
    set_detail_errno(detail, detail_size, "send Payload Manager request");
    close(fd);
    return -1;
  }
  while(used + 1 < sizeof(response)) {
    ssize_t got = recv(fd, response + used, sizeof(response) - 1 - used, 0);
    if(got < 0 && errno == EINTR) continue;
    if(got <= 0) break;
    used += (size_t)got;
  }
  close(fd);
  response[used] = 0;
  if(sscanf(response, "HTTP/%*s %d", &status) != 1) {
    snprintf(detail, detail_size, "Payload Manager bad response: %.120s",
             response);
    return -1;
  }
  if(status >= 400) {
    snprintf(detail, detail_size, "Payload Manager launch failed HTTP %d: %.120s",
             status, response);
    return -1;
  }
  if(detail && detail_size) {
    snprintf(detail, detail_size,
             "Payload Manager launched ShadowMountPlus HTTP %d path=%s",
             status, elf_path ? elf_path : "");
  }
  return 0;
}

int
gc_shadowmount_restart_running(char *detail, size_t detail_size) {
  char path[PATH_MAX];
  char original_detail[512];
  char path_detail[256];
  int use_payload_manager = 0;
  const char *fallbacks[] = {
      SHADOWMOUNT_PAYLOAD_MANAGER_ELF,
      SHADOWMOUNT_PAYLOAD_MANAGER_ELF_LEGACY,
  };
  size_t i;
  int rc;
  if(detail && detail_size) detail[0] = 0;
  if(find_running_shadowmount_path(path, sizeof(path), detail, detail_size) != 0) {
    snprintf(original_detail, sizeof(original_detail), "%s",
             detail && detail[0] ? detail : "running ShadowMount process not identified");
    path_detail[0] = 0;
    path[0] = 0;
    for(i = 0; i < sizeof(fallbacks) / sizeof(fallbacks[0]); i++) {
      if(path_is_regular_elf(fallbacks[i], path_detail, sizeof(path_detail))) {
        snprintf(path, sizeof(path), "%s", fallbacks[i]);
        use_payload_manager = 1;
        break;
      }
    }
    if(!path[0] &&
       find_shadowmount_payload_manager_elf(path, sizeof(path), path_detail,
                                            sizeof(path_detail)) != 0) {
      if(detail && detail_size) {
        snprintf(detail, detail_size, "%s; fallback unavailable: %s",
                 original_detail,
                 path_detail[0] ? path_detail : SHADOWMOUNT_PAYLOAD_MANAGER_ELF);
      }
      return -1;
    }
    if(path[0]) use_payload_manager = 1;
  }
  if(use_payload_manager) {
    return payload_manager_launch(path, detail, detail_size);
  }
  rc = sceKernelLoadStartModule(path, 0, NULL, 0, NULL, NULL);
  if(rc <= 0) {
    if(strstr(path, "/data/pldmgr/payloads/") &&
       payload_manager_launch(path, detail, detail_size) == 0) {
      return 0;
    }
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
