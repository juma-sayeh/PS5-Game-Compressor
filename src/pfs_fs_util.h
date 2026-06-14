/*
 * Game Compressor - shared PFSC filesystem helpers.
 */

#pragma once

#include <dirent.h>
#include <errno.h>
#include <stdio.h>
#include <string.h>
#include <strings.h>
#include <sys/stat.h>
#include <unistd.h>

static inline int
ends_with_ci(const char *s, const char *suffix) {
  size_t slen = s ? strlen(s) : 0;
  size_t suffix_len = suffix ? strlen(suffix) : 0;
  if(slen < suffix_len) return 0;
  return strcasecmp(s + slen - suffix_len, suffix) == 0;
}

static inline int
path_segment_supported(const char *name) {
  if(!name || !*name || !strcmp(name, ".") || !strcmp(name, "..")) return 0;
  if(strlen(name) >= 256) return 0;
  for(const unsigned char *p = (const unsigned char *)name; *p; p++) {
    if(*p < 0x20 || *p >= 0x7f || *p == '/' || *p == '\\') return 0;
  }
  return 1;
}

static inline int
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

static inline int
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

static inline int
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
      size_t n = strlen(path);
      int written = snprintf(child, sizeof(child), "%s%s%s", path,
                             (n > 1 && path[n - 1] != '/') ? "/" : "",
                             ent->d_name);
      if(written < 0 || (size_t)written >= sizeof(child) ||
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
