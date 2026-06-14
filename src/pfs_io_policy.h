/*
 * Game Compressor - source/destination storage overlap policy.
 */

#pragma once

#include <errno.h>
#include <stdio.h>
#include <string.h>
#include <sys/param.h>
#include <sys/mount.h>
#include <sys/stat.h>
#include <sys/types.h>

#define PFS_IO_POLICY_KEY_SIZE 256
#define PFS_IO_POLICY_REASON_SIZE 128

typedef enum pfs_io_policy {
  PFS_IO_POLICY_SERIAL = 0,
  PFS_IO_POLICY_PIPELINED = 1,
} pfs_io_policy_t;

typedef struct pfs_io_policy_info {
  pfs_io_policy_t policy;
  char source_key[PFS_IO_POLICY_KEY_SIZE];
  char dest_key[PFS_IO_POLICY_KEY_SIZE];
  char reason[PFS_IO_POLICY_REASON_SIZE];
} pfs_io_policy_info_t;

typedef struct pfs_io_device_ref {
  int valid;
  int has_mount_from;
  int has_physical_key;
  int has_stat;
  int has_fsid;
  dev_t st_dev;
  long fsid0;
  long fsid1;
  char mount_from[PFS_IO_POLICY_KEY_SIZE];
  char physical_key[PFS_IO_POLICY_KEY_SIZE];
  char key[PFS_IO_POLICY_KEY_SIZE];
  char reason[PFS_IO_POLICY_REASON_SIZE];
} pfs_io_device_ref_t;

static const char *
pfs_io_policy_name(pfs_io_policy_t policy) {
  return policy == PFS_IO_POLICY_PIPELINED ? "pipelined" : "serial";
}

static int
pfs_io_policy_allows_overlap(const pfs_io_policy_info_t *info) {
  return info && info->policy == PFS_IO_POLICY_PIPELINED;
}

static int
pfs_io_parent_in_place(char *path) {
  char *slash;
  if(!path || !path[0]) return 0;
  slash = strrchr(path, '/');
  if(!slash) return 0;
  if(slash == path) {
    path[1] = 0;
    return 1;
  }
  *slash = 0;
  return path[0] != 0;
}

static int
pfs_io_path_under_root(const char *path, const char *root) {
  size_t n;
  if(!path || !root || !root[0]) return 0;
  n = strlen(root);
  if(strncmp(path, root, n) != 0) return 0;
  return path[n] == 0 || path[n] == '/';
}

static int
pfs_io_physical_key_for_path(const char *path, char *key, size_t key_size) {
  char root[32];
  if(!path || !path[0] || !key || key_size == 0) return 0;
  for(int i = 0; i < 8; i++) {
    snprintf(root, sizeof(root), "/mnt/usb%d", i);
    if(pfs_io_path_under_root(path, root)) {
      snprintf(key, key_size, "usb%d", i);
      return 1;
    }
  }
  if(path[0] == '/' && strncmp(path, "/mnt/", 5) != 0) {
    snprintf(key, key_size, "%s", "internal");
    return 1;
  }
  return 0;
}

static int
pfs_io_resolve_device_ref(const char *path, pfs_io_device_ref_t *ref) {
  char probe[1024];
  if(!ref) return -1;
  memset(ref, 0, sizeof(*ref));
  snprintf(ref->key, sizeof(ref->key), "%s", "unknown");

  if(!path || !path[0]) {
    snprintf(ref->reason, sizeof(ref->reason), "%s", "missing-path");
    errno = EINVAL;
    return -1;
  }
  if(snprintf(probe, sizeof(probe), "%s", path) >= (int)sizeof(probe)) {
    snprintf(ref->reason, sizeof(ref->reason), "%s", "path-too-long");
    errno = ENAMETOOLONG;
    return -1;
  }

  for(;;) {
    struct statfs fs;
    if(statfs(probe, &fs) == 0) {
      struct stat st;
      memset(&st, 0, sizeof(st));
      ref->valid = 1;
      ref->has_physical_key = pfs_io_physical_key_for_path(
          path, ref->physical_key, sizeof(ref->physical_key));
      ref->has_mount_from = fs.f_mntfromname[0] != 0;
      ref->has_fsid = 1;
      ref->fsid0 = (long)fs.f_fsid.val[0];
      ref->fsid1 = (long)fs.f_fsid.val[1];
      if(ref->has_mount_from) {
        snprintf(ref->mount_from, sizeof(ref->mount_from), "%s",
                 fs.f_mntfromname);
      }
      if(stat(probe, &st) == 0) {
        ref->has_stat = 1;
        ref->st_dev = st.st_dev;
      }
      if(ref->has_mount_from) {
        snprintf(ref->key, sizeof(ref->key),
                 "physical=%s mntfrom=%s fsid=%ld:%ld stdev=%llu",
                 ref->has_physical_key ? ref->physical_key : "unknown",
                 ref->mount_from, ref->fsid0, ref->fsid1,
                 (unsigned long long)ref->st_dev);
      } else {
        snprintf(ref->key, sizeof(ref->key),
                 "physical=%s fsid=%ld:%ld stdev=%llu",
                 ref->has_physical_key ? ref->physical_key : "unknown",
                 ref->fsid0, ref->fsid1,
                 (unsigned long long)ref->st_dev);
      }
      snprintf(ref->reason, sizeof(ref->reason), "%s", "ok");
      return 0;
    }

    int saved_errno = errno ? errno : ENOENT;
    if(!pfs_io_parent_in_place(probe)) {
      snprintf(ref->reason, sizeof(ref->reason), "statfs:%s",
               strerror(saved_errno));
      errno = saved_errno;
      return -1;
    }
  }
}

static pfs_io_policy_t
pfs_io_policy_for_paths(const char *source_path, const char *dest_path,
                        pfs_io_policy_info_t *info) {
  pfs_io_device_ref_t source;
  pfs_io_device_ref_t dest;
  pfs_io_policy_info_t local;
  if(!info) info = &local;
  memset(info, 0, sizeof(*info));
  info->policy = PFS_IO_POLICY_SERIAL;
  snprintf(info->source_key, sizeof(info->source_key), "%s", "unknown");
  snprintf(info->dest_key, sizeof(info->dest_key), "%s", "unknown");
  snprintf(info->reason, sizeof(info->reason), "%s", "unknown-device");

  if(pfs_io_resolve_device_ref(source_path, &source) != 0) {
    snprintf(info->source_key, sizeof(info->source_key), "%s", source.key);
    snprintf(info->reason, sizeof(info->reason), "source-%s",
             source.reason[0] ? source.reason : "unknown");
    return info->policy;
  }
  if(pfs_io_resolve_device_ref(dest_path, &dest) != 0) {
    snprintf(info->source_key, sizeof(info->source_key), "%s", source.key);
    snprintf(info->dest_key, sizeof(info->dest_key), "%s", dest.key);
    snprintf(info->reason, sizeof(info->reason), "dest-%s",
             dest.reason[0] ? dest.reason : "unknown");
    return info->policy;
  }

  snprintf(info->source_key, sizeof(info->source_key), "%s", source.key);
  snprintf(info->dest_key, sizeof(info->dest_key), "%s", dest.key);

  if(source.has_physical_key && dest.has_physical_key) {
    if(!strcmp(source.physical_key, dest.physical_key)) {
      snprintf(info->reason, sizeof(info->reason), "%s",
               "same-physical-root");
      return info->policy;
    }
    info->policy = PFS_IO_POLICY_PIPELINED;
    snprintf(info->reason, sizeof(info->reason), "%s",
             "different-physical-root");
    return info->policy;
  }

  if(source.has_mount_from && dest.has_mount_from) {
    if(!strcmp(source.mount_from, dest.mount_from)) {
      snprintf(info->reason, sizeof(info->reason), "%s",
               "same-mount-source");
      return info->policy;
    }
    info->policy = PFS_IO_POLICY_PIPELINED;
    snprintf(info->reason, sizeof(info->reason), "%s",
             "different-mount-source");
    return info->policy;
  }

  if(source.has_fsid && dest.has_fsid) {
    if(source.fsid0 == dest.fsid0 && source.fsid1 == dest.fsid1) {
      snprintf(info->reason, sizeof(info->reason), "%s", "same-fsid");
      return info->policy;
    }
    info->policy = PFS_IO_POLICY_PIPELINED;
    snprintf(info->reason, sizeof(info->reason), "%s", "different-fsid");
    return info->policy;
  }

  if(source.has_stat && dest.has_stat) {
    if(source.st_dev == dest.st_dev) {
      snprintf(info->reason, sizeof(info->reason), "%s", "same-st-dev");
      return info->policy;
    }
    info->policy = PFS_IO_POLICY_PIPELINED;
    snprintf(info->reason, sizeof(info->reason), "%s", "different-st-dev");
    return info->policy;
  }

  snprintf(info->reason, sizeof(info->reason), "%s", "unresolved-device-id");
  return info->policy;
}
