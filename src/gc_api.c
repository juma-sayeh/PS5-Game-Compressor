/*
 * Game Compressor - mounted-game discovery, queue, history, and operations.
 */

#include <ctype.h>
#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <pthread.h>
#include <stdatomic.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <sys/mount.h>
#include <sys/stat.h>
#include <sys/statvfs.h>
#include <sys/time.h>
#include <time.h>
#include <unistd.h>

#include "gc_api.h"
#include "gc_diag.h"
#include "gc_icon_thumb.h"
#include "gc_notify.h"
#include "gc_shadowmount.h"
#include "gc_size_cache.h"
#include "pfs_ampr_hotswap.h"
#include "pfs_compress.h"
#include "pfs_validate_hash.h"
#include "pfs_repair.h"
#include "transfer_internal.h"

#define GC_APP_BASE "/user/app"
#define GC_APPMETA_BASE "/user/appmeta"
#define GC_BASE "/data/GameCompressor"
#define GC_LOG_DIR GC_BASE "/logs"
#define GC_VALIDATION_DIR GC_BASE "/validations"
#define GC_HISTORY_DIR GC_BASE "/history"
#define GC_HISTORY_LOG GC_HISTORY_DIR "/history.jsonl"
#define GC_MOUNT_SWITCH_LOG GC_BASE "/mount-switch-recovery.jsonl"
#define GC_REPAIR_DIR GC_LOG_DIR "/repair"
#define GC_UI_SETTINGS_FILE GC_BASE "/ui-settings.json"
#define GC_AMPR_DIR GC_BASE "/ampr-emu"
#define GC_AMPR_BINARY_NAME "libSceAmpr.sprx"
#define GC_AMPR_LATEST_FILE GC_AMPR_DIR "/latest.json"
#define GC_AMPR_SELECTION_DIR GC_AMPR_DIR "/selections"
#define GC_AMPR_ORIGINAL_DIR GC_AMPR_DIR "/originals"
#define GC_HISTORY_KEY_SIZE 96
#define GC_MAX_GAMES 128
#define GC_MAX_SCAN_ROOTS 256
#define GC_MAX_OPS 64
#define GC_ARTIFACT_SCAN_TTL_SECONDS 10
#define GC_CLOSE_WAIT_SECONDS 20
#define GC_REPAIR_WAIT_SECONDS 180
#define GC_REPAIR_WAIT_STEP_SECONDS 2
#define GC_REMOUNT_WAIT_SECONDS 10
#define GC_REMOUNT_WAIT_STEP_SECONDS 1
#define GC_MOUNT_SCAN_REQUEST_SECONDS 2
#define GC_SHADOWMOUNT_RESTART_WAIT_SECONDS 4
#define GC_CANCEL_POLL_USEC 100000U
#define GC_FORCE_REMOUNT_PREFIX ".__gc_remount_"
#define GC_MOUNT_HIDE_PREFIX ".gc-hide-"
#define GC_WORKER_THREAD_STACK_SIZE (1024 * 1024)
#define GC_SYSTEM_APP_BASE "/system_ex/app"
#define GC_SHADOW_PFSC_BASE "/mnt/shadowmnt/pfsc"
#define GC_SHADOW_IMAGE_BASE "/mnt/shadowmnt"
#define GC_SHADOW_CONFIG_FILE "/data/shadowmount/config.ini"
#define GC_SHADOW_MANUAL_LIST_FILE "/data/shadowmount/manual.lst"
#define GC_INTERNAL_GAME_ROOT "/data/homebrew"
#define GC_USB_COUNT 8
#define GC_STORAGE_TARGET_COUNT (GC_USB_COUNT + 2)
#define GC_STORAGE_TARGET_MIN_TOTAL_BYTES (64ULL * 1024ULL * 1024ULL)
#define GC_STORAGE_TARGET_MIN_FREE_BYTES (10ULL * 1024ULL * 1024ULL)
#define GC_COPY_CHUNK_SIZE (1024 * 1024)
#define GC_GIB (1024ULL * 1024ULL * 1024ULL)
#define GC_STREAM_MIN_FREE_BYTES (1ULL * GC_GIB)
#define GC_READ_SPEED_TEST_SECONDS 60

static void
gc_job_set_cancel_disabled(const char *reason) {
  pthread_mutex_lock(&g_job.lock);
  atomic_store(&g_job.cancel_disabled, 1);
  snprintf(g_job.cancel_disabled_reason, sizeof(g_job.cancel_disabled_reason),
           "%s", reason && reason[0] ? reason : "operation cannot be cancelled");
  pthread_mutex_unlock(&g_job.lock);
}

static int
gc_cancel_requested(char *err, size_t err_size) {
  if(!job_cancelled()) return 0;
  if(err && err_size) snprintf(err, err_size, "%s", "cancelled");
  errno = EINTR;
  return 1;
}

static int
gc_sleep_cancelable_seconds(unsigned int seconds, char *err, size_t err_size) {
  unsigned int polls_per_second = 1000000U / GC_CANCEL_POLL_USEC;
  unsigned int polls = seconds * polls_per_second;
  if(polls == 0) polls = 1;
  for(unsigned int i = 0; i < polls; i++) {
    if(gc_cancel_requested(err, err_size)) return -1;
    usleep(GC_CANCEL_POLL_USEC);
  }
  return gc_cancel_requested(err, err_size) ? -1 : 0;
}

static int
gc_shadowmount_request_scan_cancelable(char *err, size_t err_size) {
  if(gc_cancel_requested(err, err_size)) return -1;
  if(gc_shadowmount_request_scan(err, err_size) != 0) return -1;
  return gc_cancel_requested(err, err_size) ? -1 : 0;
}

static int
gc_shadowmount_request_title_source_scan_cancelable(const char *title_id,
                                                    const char *source_path,
                                                    char *err,
                                                    size_t err_size) {
  if(gc_cancel_requested(err, err_size)) return -1;
  if(gc_shadowmount_request_title_source_scan(title_id, source_path, err,
                                              err_size) != 0) {
    return -1;
  }
  return gc_cancel_requested(err, err_size) ? -1 : 0;
}

typedef enum gc_source_kind {
  GC_SOURCE_UNKNOWN = 0,
  GC_SOURCE_FOLDER,
  GC_SOURCE_IMAGE,
  GC_SOURCE_COMPRESSED,
} gc_source_kind_t;

typedef enum gc_action {
  GC_ACTION_COMPRESS = 1,
  GC_ACTION_MAKE_IMAGE,
  GC_ACTION_UNCOMPRESS,
  GC_ACTION_EXTRACT_IMAGE,
  GC_ACTION_VALIDATE_REPAIR,
  GC_ACTION_VALIDATE_ONLY,
  GC_ACTION_MOVE_TO_USB,
  GC_ACTION_MOVE_TO_INTERNAL,
  GC_ACTION_COPY_TO_USB,
  GC_ACTION_COPY_TO_INTERNAL,
  GC_ACTION_REFRESH_MOUNT,
  GC_ACTION_DELETE_GAME_DATA,
  GC_ACTION_READ_SPEED_TEST,
  GC_ACTION_BUILD_AMPR_INDEX,
  GC_ACTION_SET_READ_ONLY,
  GC_ACTION_UPDATE_AMPR,
} gc_action_t;

typedef enum gc_op_status {
  GC_OP_QUEUED = 1,
  GC_OP_RUNNING,
  GC_OP_SUCCESS,
  GC_OP_FAILED,
  GC_OP_CANCELLED,
} gc_op_status_t;

typedef enum gc_validation_state {
  GC_VALIDATION_NONE = 0,
  GC_VALIDATION_VALIDATED,
} gc_validation_state_t;

typedef struct gc_game {
  char title_id[64];
  char name[256];
  char mount_path[1024];
  char image_path[1024];
  char source_path[1024];
  char output_path[1024];
  char nested_name[256];
  gc_source_kind_t source_kind;
  int nested_type;
  uint64_t source_size;
  uint64_t free_bytes;
  uint64_t required_bytes;
  uint64_t extra_needed;
  uint64_t compression_source_size;
  uint64_t compressed_size;
  uint64_t saved_bytes;
  int size_pending;
  gc_size_status_t size_status;
  int size_estimated;
  uint64_t size_measured_at;
  int size_refreshing;
  int can_stream_delete;
  int has_icon;
  uint64_t icon_size;
  uint64_t icon_mtime;
  int output_exists;
  int is_mounted;
  int apr_indexed;
  int ampr_hot_swap_optimized;
  int ampr_present;
  int ampr_update_needed;
  int ampr_update_supported;
  int ampr_original_available;
  char ampr_path[1024];
  char ampr_version[64];
  char ampr_sha256[65];
  char ampr_original_sha256[65];
  uint64_t ampr_original_size;
  char ampr_latest_version[64];
  char ampr_latest_sha256[65];
  char mount_status[32];
  gc_validation_state_t validation;
  char validation_status[32];
  char primary_action[32];
} gc_game_t;

typedef struct gc_operation {
  int used;
  uint64_t seq;
  char id[32];
  gc_action_t action;
  gc_op_status_t status;
  char title_id[64];
  char display_name[256];
  char source_path[1024];
  char output_path[1024];
  char source_kind[32];
  char format[16];
  char delete_policy[16];
  char compression_mode[16];
  char stream_order[24];
  uint64_t stream_budget_bytes;
  int skip_space_check;
  char target_root[1024];
  char preserve_original[16];
  char preserved_original_path[1024];
  char preserved_hidden_path[1024];
  char phase[32];
  char result[32];
  char error[256];
  char repair_summary[1024];
  char ampr_version[64];
  char ampr_sha256[65];
  char ampr_cache_path[1024];
  char ampr_result_mode[32];
  char ampr_intent[16];
  char read_root[1024];
  char read_storage[32];
  char read_first_error_path[1024];
  char read_first_error[256];
  uint64_t compression_source_size;
  uint64_t compressed_size;
  uint64_t saved_bytes;
  uint64_t scan_bytes;
  uint64_t scan_files;
  uint64_t scan_dirs;
  uint64_t scan_entries;
  uint64_t scan_elapsed_ms;
  uint64_t scan_workers;
  int apr_indexed;
  int ampr_hot_swap_optimized;
  uint64_t read_bytes;
  uint64_t read_files;
  uint64_t read_dirs;
  uint64_t read_elapsed_ms;
  uint64_t read_avg_bps;
  uint64_t read_min_bps;
  uint64_t read_max_bps;
  uint64_t read_errors;
  uint64_t read_skipped;
  uint64_t repaired_blocks;
  uint64_t bad_blocks_found;
  uint64_t hash_checked_blocks;
  uint64_t hash_mismatched_blocks;
  uint64_t software_compared_blocks;
  time_t created_at;
  time_t started_at;
  time_t ended_at;
  int cancel_requested;
} gc_operation_t;

typedef struct gc_usb_target {
  char id[16];
  char name[64];
  char root[64];
  char target_root[96];
  uint64_t total_bytes;
  uint64_t free_bytes;
} gc_usb_target_t;

typedef struct gc_storage_target_def {
  const char *id;
  const char *name;
  const char *root;
  const char *target_root;
} gc_storage_target_def_t;

static pthread_mutex_t g_gc_lock = PTHREAD_MUTEX_INITIALIZER;
static gc_operation_t g_ops[GC_MAX_OPS];
static uint64_t g_next_seq = 1;
static int g_worker_running = 0;
static pthread_mutex_t g_folder_size_recheck_lock = PTHREAD_MUTEX_INITIALIZER;
static int g_folder_size_initial_recheck_done = 0;
static pthread_mutex_t g_artifact_cache_lock = PTHREAD_MUTEX_INITIALIZER;
static gc_game_t g_artifact_cache[GC_MAX_GAMES];
static size_t g_artifact_cache_count = 0;
static time_t g_artifact_cache_scanned_at = 0;
static int g_artifact_cache_force = 1;

extern int sceSystemServiceGetAppIdOfRunningBigApp(void);
extern int sceSystemServiceGetAppTitleId(int app_id, char *title_id);
extern uint32_t sceLncUtilKillApp(uint32_t app_id);

static const gc_storage_target_def_t GC_STORAGE_TARGETS[GC_STORAGE_TARGET_COUNT] = {
  { "ext0", "External SSD", "/mnt/ext0", "/mnt/ext0/homebrew" },
  { "ext1", "M.2 SSD", "/mnt/ext1", "/mnt/ext1/homebrew" },
  { "usb0", "USB 0", "/mnt/usb0", "/mnt/usb0/homebrew" },
  { "usb1", "USB 1", "/mnt/usb1", "/mnt/usb1/homebrew" },
  { "usb2", "USB 2", "/mnt/usb2", "/mnt/usb2/homebrew" },
  { "usb3", "USB 3", "/mnt/usb3", "/mnt/usb3/homebrew" },
  { "usb4", "USB 4", "/mnt/usb4", "/mnt/usb4/homebrew" },
  { "usb5", "USB 5", "/mnt/usb5", "/mnt/usb5/homebrew" },
  { "usb6", "USB 6", "/mnt/usb6", "/mnt/usb6/homebrew" },
  { "usb7", "USB 7", "/mnt/usb7", "/mnt/usb7/homebrew" },
};

static void fsync_parent_dir_best_effort(const char *path);
static int copy_file_contents(const char *src, const char *dst, char *err,
                              size_t err_size);

static const char *
source_kind_name(gc_source_kind_t kind) {
  if(kind == GC_SOURCE_FOLDER) return "folder";
  if(kind == GC_SOURCE_IMAGE) return "image";
  if(kind == GC_SOURCE_COMPRESSED) return "compressed";
  return "unknown";
}

static gc_source_kind_t
source_kind_from_name(const char *name) {
  if(!strcmp(name ? name : "", "folder")) return GC_SOURCE_FOLDER;
  if(!strcmp(name ? name : "", "image")) return GC_SOURCE_IMAGE;
  if(!strcmp(name ? name : "", "compressed")) return GC_SOURCE_COMPRESSED;
  return GC_SOURCE_UNKNOWN;
}

static int
get_required_source_path_arg(const http_request_t *req, char *out,
                             size_t out_size) {
  if(!websrv_get_query_arg(req, "sourcePath", out, out_size) || !out[0]) {
    return -2;
  }
  return path_is_safe(out) ? 0 : -1;
}

static const char *
action_name(gc_action_t action) {
  if(action == GC_ACTION_COMPRESS) return "compress";
  if(action == GC_ACTION_MAKE_IMAGE) return "make-image";
  if(action == GC_ACTION_UNCOMPRESS) return "uncompress";
  if(action == GC_ACTION_EXTRACT_IMAGE) return "extract-image";
  if(action == GC_ACTION_VALIDATE_REPAIR) return "validate-repair";
  if(action == GC_ACTION_VALIDATE_ONLY) return "validate-only";
  if(action == GC_ACTION_MOVE_TO_USB) return "move-to-usb";
  if(action == GC_ACTION_MOVE_TO_INTERNAL) return "move-to-internal";
  if(action == GC_ACTION_COPY_TO_USB) return "copy-to-usb";
  if(action == GC_ACTION_COPY_TO_INTERNAL) return "copy-to-internal";
  if(action == GC_ACTION_REFRESH_MOUNT) return "refresh-mount";
  if(action == GC_ACTION_DELETE_GAME_DATA) return "delete-game-data";
  if(action == GC_ACTION_READ_SPEED_TEST) return "read-speed-test";
  if(action == GC_ACTION_BUILD_AMPR_INDEX) return "build-ampr-index";
  if(action == GC_ACTION_SET_READ_ONLY) return "set-read-only";
  if(action == GC_ACTION_UPDATE_AMPR) return "update-ampr";
  return "unknown";
}

static const char *
status_name(gc_op_status_t status) {
  if(status == GC_OP_QUEUED) return "pending";
  if(status == GC_OP_RUNNING) return "running";
  if(status == GC_OP_SUCCESS) return "success";
  if(status == GC_OP_FAILED) return "failed";
  if(status == GC_OP_CANCELLED) return "cancelled";
  return "unknown";
}

static const char *
compression_mode_or_default(const char *mode) {
  (void)mode;
  return "compressed";
}

static int
error_is_output_exists(const char *err) {
  const char *s = err ? err : "";
  return !strcasecmp(s, "output exists") ||
      !strcasecmp(s, "target already exists");
}

static const char *
operation_notification_status(gc_op_status_t status) {
  if(status == GC_OP_SUCCESS) return "success";
  if(status == GC_OP_CANCELLED) return "cancelled";
  return "failed";
}

static void
set_output_exists_error(gc_operation_t *op, const char *path) {
  const char *output = (path && path[0]) ? path :
      (op && op->output_path[0] ? op->output_path : "");
  if(!op) return;
  if(output[0]) {
    snprintf(op->error, sizeof(op->error),
             "Output already exists: %s. Delete or move it first.", output);
  } else {
    snprintf(op->error, sizeof(op->error),
             "%s", "Output already exists. Delete or move it first.");
  }
}

static int
ends_with_ci(const char *s, const char *suffix) {
  size_t sl = s ? strlen(s) : 0;
  size_t tl = suffix ? strlen(suffix) : 0;
  return sl >= tl && !strcasecmp(s + sl - tl, suffix);
}

static int
valid_title_id(const char *s) {
  if(!s || strlen(s) != 9) return 0;
  for(const unsigned char *p = (const unsigned char *)s; *p; p++) {
    if(!isalnum(*p)) return 0;
  }
  return 1;
}

static int
hex_char_value(int ch) {
  if(ch >= '0' && ch <= '9') return ch - '0';
  if(ch >= 'a' && ch <= 'f') return ch - 'a' + 10;
  if(ch >= 'A' && ch <= 'F') return ch - 'A' + 10;
  return -1;
}

static int
sha256_hex_valid(const char *hex) {
  if(!hex || strlen(hex) != 64) return 0;
  for(size_t i = 0; i < 64; i++) {
    if(hex_char_value((unsigned char)hex[i]) < 0) return 0;
  }
  return 1;
}

static void
sha256_to_hex(const unsigned char hash[PFS_VHASH_HASH_SIZE],
              char out[65]) {
  static const char hexdigits[] = "0123456789abcdef";
  for(size_t i = 0; i < PFS_VHASH_HASH_SIZE; i++) {
    out[i * 2] = hexdigits[(hash[i] >> 4) & 0xf];
    out[i * 2 + 1] = hexdigits[hash[i] & 0xf];
  }
  out[64] = 0;
}

static int
ampr_version_safe(const char *version) {
  if(!version || !*version || strlen(version) >= 64) return 0;
  for(const unsigned char *p = (const unsigned char *)version; *p; p++) {
    if(!isalnum(*p) && *p != '.' && *p != '_' && *p != '-') return 0;
  }
  return 1;
}

static int
ampr_cache_dir_for_version(const char *version, char *out, size_t out_size) {
  if(!ampr_version_safe(version)) {
    errno = EINVAL;
    return -1;
  }
  int n = snprintf(out, out_size, "%s/%s", GC_AMPR_DIR, version);
  if(n < 0 || (size_t)n >= out_size) {
    errno = ENAMETOOLONG;
    return -1;
  }
  return 0;
}

static int
ampr_cache_binary_path(const char *version, char *out, size_t out_size) {
  char dir[1024];
  if(ampr_cache_dir_for_version(version, dir, sizeof(dir)) != 0) return -1;
  int n = snprintf(out, out_size, "%s/%s", dir, GC_AMPR_BINARY_NAME);
  if(n < 0 || (size_t)n >= out_size) {
    errno = ENAMETOOLONG;
    return -1;
  }
  return 0;
}

static int
hash_file_sha256_hex(const char *path, char out[65], char *err,
                     size_t err_size) {
  int fd = open(path, O_RDONLY);
  if(fd < 0) {
    snprintf(err, err_size, "open hash input: %s", strerror(errno));
    return -1;
  }
  struct stat st;
  if(fstat(fd, &st) != 0 || st.st_size < 0 ||
     (uint64_t)st.st_size > SIZE_MAX) {
    snprintf(err, err_size, "stat hash input: %s", strerror(errno));
    close(fd);
    return -1;
  }
  size_t size = (size_t)st.st_size;
  unsigned char *buf = malloc(size ? size : 1);
  if(!buf) {
    snprintf(err, err_size, "%s", "out of memory");
    close(fd);
    return -1;
  }
  size_t done = 0;
  while(done < size) {
    ssize_t n = read(fd, buf + done, size - done);
    if(n < 0) {
      if(errno == EINTR) continue;
      snprintf(err, err_size, "read hash input: %s", strerror(errno));
      free(buf);
      close(fd);
      return -1;
    }
    if(n == 0) break;
    done += (size_t)n;
  }
  close(fd);
  if(done != size) {
    snprintf(err, err_size, "%s", "short hash input");
    free(buf);
    errno = EIO;
    return -1;
  }
  unsigned char hash[PFS_VHASH_HASH_SIZE];
  pfs_sha256(buf, size, hash);
  sha256_to_hex(hash, out);
  free(buf);
  return 0;
}

static int
read_link_file(const char *path, char *out, size_t out_size) {
  FILE *f = fopen(path, "r");
  if(!f) return -1;
  if(!fgets(out, (int)out_size, f)) {
    fclose(f);
    return -1;
  }
  fclose(f);
  out[strcspn(out, "\r\n")] = 0;
  return out[0] ? 0 : -1;
}

static int
ampr_folder_target_probe(const char *root, char *path_out, size_t path_size,
                         char sha_out[65]) {
  static const char *rels[] = {
    "fakelib/" GC_AMPR_BINARY_NAME,
    "fakelib/libSceAmpr.prx",
    "sce_module/" GC_AMPR_BINARY_NAME,
    "sce_module/libSceAmpr.prx",
  };
  struct stat st;
  if(!root || !root[0]) return 0;
  if(path_out && path_size) path_out[0] = 0;
  if(sha_out) sha_out[0] = 0;
  for(size_t i = 0; i < sizeof(rels) / sizeof(rels[0]); i++) {
    char path[1024];
    int n = snprintf(path, sizeof(path), "%s/%s", root, rels[i]);
    if(n < 0 || (size_t)n >= sizeof(path)) continue;
    if(stat(path, &st) == 0 && S_ISREG(st.st_mode)) {
      char err[128] = {0};
      if(path_out && path_size) snprintf(path_out, path_size, "%s", path);
      if(sha_out && hash_file_sha256_hex(path, sha_out, err, sizeof(err)) != 0) {
        sha_out[0] = 0;
      }
      return 1;
    }
  }
  return 0;
}

static int
ampr_folder_index_probe(const char *root) {
  char path[1024];
  struct stat st;
  int n;

  if(!root || !root[0]) return 0;
  n = snprintf(path, sizeof(path), "%s/ampr_emu.index", root);
  if(n < 0 || (size_t)n >= sizeof(path)) return 0;
  return stat(path, &st) == 0 && S_ISREG(st.st_mode) && st.st_size > 0;
}

static void
populate_apr_index_state_from_roots(gc_game_t *g) {
  if(!g || !g->ampr_present) return;
  if(g->source_kind == GC_SOURCE_FOLDER &&
     ampr_folder_index_probe(g->source_path)) {
    g->apr_indexed = 1;
    return;
  }
  if(g->is_mounted && ampr_folder_index_probe(g->mount_path)) {
    g->apr_indexed = 1;
  }
}

static void
mount_link_path_for_title(const char *title_id, const char *name,
                          char *out, size_t out_size) {
  snprintf(out, out_size, "%s/%s/%s", GC_APP_BASE, title_id, name);
}

static int
read_title_link(const char *title_id, const char *name,
                char *out, size_t out_size) {
  char path[1024];
  mount_link_path_for_title(title_id, name, path, sizeof(path));
  return read_link_file(path, out, out_size);
}

static int
path_parent(const char *path, char *out, size_t out_size) {
  const char *slash = strrchr(path ? path : "", '/');
  if(!slash) return -1;
  if(slash == path) {
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
storage_statvfs_floor_for_path(const char *path, char *out, size_t out_size) {
  if(!path || !out || out_size == 0) return 0;
  if(!strcmp(path, "/data") || !strncmp(path, "/data/", 6)) {
    snprintf(out, out_size, "%s", "/data");
    return 1;
  }
  if(!strcmp(path, "/mnt/ext0") || !strncmp(path, "/mnt/ext0/", 10)) {
    snprintf(out, out_size, "%s", "/mnt/ext0");
    return 1;
  }
  if(!strcmp(path, "/mnt/ext1") || !strncmp(path, "/mnt/ext1/", 10)) {
    snprintf(out, out_size, "%s", "/mnt/ext1");
    return 1;
  }
  for(int i = 0; i < GC_USB_COUNT; i++) {
    char root[32];
    snprintf(root, sizeof(root), "/mnt/usb%d", i);
    size_t n = strlen(root);
    if(!strcmp(path, root) || (!strncmp(path, root, n) && path[n] == '/')) {
      snprintf(out, out_size, "%s", root);
      return 1;
    }
  }
  return 0;
}

static int
statvfs_with_storage_root_fallback(const char *path, struct statvfs *sv,
                                   char *used_path, size_t used_path_size) {
  char probe[1024];
  char floor[1024] = {0};
  int have_floor;
  int first_errno = 0;
  if(!path || !path[0] || !sv) {
    errno = EINVAL;
    return -1;
  }
  if(snprintf(probe, sizeof(probe), "%s", path) >= (int)sizeof(probe)) {
    errno = ENAMETOOLONG;
    return -1;
  }
  have_floor = storage_statvfs_floor_for_path(probe, floor, sizeof(floor));
  for(;;) {
    if(statvfs(probe, sv) == 0) {
      if(used_path && used_path_size) {
        snprintf(used_path, used_path_size, "%s", probe);
      }
      return 0;
    }
    if(!first_errno) first_errno = errno ? errno : EIO;
    if(have_floor && !strcmp(probe, floor)) {
      errno = first_errno;
      return -1;
    }
    if(!strcmp(probe, "/")) {
      errno = first_errno;
      return -1;
    }
    char parent[1024];
    if(path_parent(probe, parent, sizeof(parent)) != 0 ||
       !strcmp(parent, probe)) {
      errno = first_errno;
      return -1;
    }
    snprintf(probe, sizeof(probe), "%s", parent);
  }
}

static int
hidden_sibling_temp_path(const char *output_path, const char *tag,
                         char *out, size_t out_size) {
  char parent[1024];
  const char *base = path_basename(output_path);
  const char *safe_tag = tag && tag[0] ? tag : "gc-temp";
  if(!output_path || !output_path[0] || !base || !base[0] ||
     !out || out_size == 0 ||
     path_parent(output_path, parent, sizeof(parent)) != 0) {
    errno = EINVAL;
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

static int
legacy_suffix_temp_path(const char *output_path, const char *suffix,
                        char *out, size_t out_size) {
  if(!output_path || !output_path[0] || !suffix || !suffix[0] ||
     !out || out_size == 0) {
    errno = EINVAL;
    return -1;
  }
  int n = snprintf(out, out_size, "%s.%s", output_path, suffix);
  if(n < 0 || (size_t)n >= out_size) {
    errno = ENAMETOOLONG;
    return -1;
  }
  return 0;
}

static int
path_exists_for_source_kind(const char *path, gc_source_kind_t kind) {
  struct stat st;
  if(!path || !path[0] || lstat(path, &st) != 0) return 0;
  if(kind == GC_SOURCE_FOLDER) return S_ISDIR(st.st_mode);
  if(kind == GC_SOURCE_IMAGE || kind == GC_SOURCE_COMPRESSED) {
    return S_ISREG(st.st_mode);
  }
  return S_ISDIR(st.st_mode) || S_ISREG(st.st_mode);
}

static uint64_t
saturating_mul(uint64_t a, uint64_t b) {
  if(a != 0 && b > UINT64_MAX / a) return UINT64_MAX;
  return a * b;
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

static const char *
base_name(const char *path) {
  const char *slash = strrchr(path ? path : "", '/');
  return slash ? slash + 1 : path;
}

static const char *
source_kind_name_from_path(const char *path) {
  const char *name = base_name(path);
  if(name && ends_with_ci(name, ".ffpfsc")) return "compressed";
  if(name && (ends_with_ci(name, ".exfat") ||
              ends_with_ci(name, ".ffpfs"))) return "image";
  return "unknown";
}

static void
set_game_mount_status(gc_game_t *g, int is_mounted, const char *status) {
  if(!g) return;
  g->is_mounted = is_mounted ? 1 : 0;
  snprintf(g->mount_status, sizeof(g->mount_status), "%s",
           status && status[0] ? status : (is_mounted ? "mounted" : "not-mounted"));
}

static void
game_instance_id(const gc_game_t *g, char *out, size_t out_size) {
  uint32_t source_hash;
  if(!out || out_size == 0) return;
  source_hash = fnv1a32_string(g ? g->source_path : "");
  snprintf(out, out_size, "%s:%s:%08x",
           g && g->title_id[0] ? g->title_id : "UNKNOWN",
           source_kind_name(g ? g->source_kind : GC_SOURCE_UNKNOWN),
           source_hash);
}

static int
strip_extension_base(const char *path, char *out, size_t out_size) {
  const char *name = base_name(path);
  const char *dot = strrchr(name ? name : "", '.');
  size_t len = dot ? (size_t)(dot - name) : strlen(name ? name : "");
  if(!out || out_size == 0 || len == 0 || len >= out_size) return -1;
  memcpy(out, name, len);
  out[len] = 0;
  return 0;
}

static int
shadow_image_mount_point(const char *image_path, int nested_type,
                         char *out, size_t out_size) {
  char stem[256];
  char mount_name[384];
  const char *base = nested_type == PFS_NESTED_UNKNOWN
      ? GC_SHADOW_IMAGE_BASE
      : GC_SHADOW_IMAGE_BASE;
  if(strip_extension_base(image_path, stem, sizeof(stem)) != 0) return -1;
  int n = snprintf(mount_name, sizeof(mount_name), "%s_%08x", stem,
                   fnv1a32_string(image_path));
  if(n < 0 || (size_t)n >= sizeof(mount_name)) return -1;
  n = snprintf(out, out_size, "%s/%s", base, mount_name);
  return n < 0 || (size_t)n >= out_size ? -1 : 0;
}

static int
shadow_pfsc_mount_dir_for_outer(const char *outer_path,
                                char *out, size_t out_size) {
  char stem[256];
  char mount_name[384];
  if(strip_extension_base(outer_path, stem, sizeof(stem)) != 0) return -1;
  int n = snprintf(mount_name, sizeof(mount_name), "%s_%08x", stem,
                   fnv1a32_string(outer_path));
  if(n < 0 || (size_t)n >= sizeof(mount_name)) return -1;
  n = snprintf(out, out_size, "%s/%s", GC_SHADOW_PFSC_BASE, mount_name);
  return n < 0 || (size_t)n >= out_size ? -1 : 0;
}

static int
expected_compressed_shadow_paths(const char *outer_path,
                                 const char *nested_name,
                                 int nested_type,
                                 char *nested_image_path,
                                 size_t nested_image_path_size,
                                 char *mounted_app_path,
                                 size_t mounted_app_path_size) {
  char outer_mount[1024];
  if(!outer_path || !nested_name || !nested_name[0]) return -1;
  if(shadow_pfsc_mount_dir_for_outer(outer_path, outer_mount,
                                     sizeof(outer_mount)) != 0) {
    return -1;
  }
  join_path(nested_image_path, nested_image_path_size, outer_mount,
            nested_name);
  if(shadow_image_mount_point(nested_image_path, nested_type,
                              mounted_app_path,
                              mounted_app_path_size) != 0) {
    return -1;
  }
  return 0;
}

static int
parse_hex32(const char *s, uint32_t *out) {
  uint32_t value = 0;
  if(!s || !out) return -1;
  for(int i = 0; i < 8; i++) {
    int ch = (unsigned char)s[i];
    uint32_t nibble;
    if(ch >= '0' && ch <= '9') nibble = (uint32_t)(ch - '0');
    else if(ch >= 'a' && ch <= 'f') nibble = (uint32_t)(ch - 'a' + 10);
    else if(ch >= 'A' && ch <= 'F') nibble = (uint32_t)(ch - 'A' + 10);
    else return -1;
    value = (value << 4) | nibble;
  }
  *out = value;
  return 0;
}

static int
shadow_pfsc_hash_from_path(const char *image_path, const char *title_id,
                           uint32_t *hash_out) {
  size_t prefix_len = strlen(GC_SHADOW_PFSC_BASE);
  size_t title_len = title_id ? strlen(title_id) : 0;
  const char *segment;
  if(!image_path || !title_id || !hash_out || title_len == 0) return -1;
  if(strncmp(image_path, GC_SHADOW_PFSC_BASE, prefix_len) ||
     image_path[prefix_len] != '/') {
    return -1;
  }
  segment = image_path + prefix_len + 1;
  if(strncmp(segment, title_id, title_len) || segment[title_len] != '_') {
    return -1;
  }
  if(parse_hex32(segment + title_len + 1, hash_out) != 0) return -1;
  if(segment[title_len + 9] && segment[title_len + 9] != '/') return -1;
  return 0;
}

static int
free_bytes_for_output_ex(const char *output_path, uint64_t *out,
                         char *probe_out, size_t probe_out_size) {
  char parent[1024];
  struct statvfs sv;
  if(path_parent(output_path, parent, sizeof(parent)) != 0) return -1;
  if(statvfs_with_storage_root_fallback(parent, &sv, probe_out,
                                        probe_out_size) != 0) {
    return -1;
  }
  uint64_t block = sv.f_frsize ? (uint64_t)sv.f_frsize :
      (sv.f_bsize ? (uint64_t)sv.f_bsize : 1ULL);
  if(out) *out = saturating_mul((uint64_t)sv.f_bavail, block);
  return 0;
}

static int
free_bytes_for_output(const char *output_path, uint64_t *out) {
  return free_bytes_for_output_ex(output_path, out, NULL, 0);
}

static int
space_for_path(const char *path, uint64_t *free_out, uint64_t *total_out) {
  struct statvfs sv;
  if(statvfs(path, &sv) != 0) return -1;
  uint64_t block = sv.f_frsize ? (uint64_t)sv.f_frsize :
      (sv.f_bsize ? (uint64_t)sv.f_bsize : 1ULL);
  if(free_out) *free_out = saturating_mul((uint64_t)sv.f_bavail, block);
  if(total_out) *total_out = saturating_mul((uint64_t)sv.f_blocks, block);
  return 0;
}

static int
path_under_root(const char *path, const char *root) {
  size_t len = root ? strlen(root) : 0;
  if(!path || !root || len == 0) return 0;
  return !strcmp(path, root) || (!strncmp(path, root, len) && path[len] == '/');
}

static int
path_is_system_app_path(const char *path) {
  return path_under_root(path, GC_SYSTEM_APP_BASE);
}

static int
game_uses_system_app_path(const gc_game_t *g) {
  if(!g) return 0;
  return path_is_system_app_path(g->mount_path) ||
      path_is_system_app_path(g->image_path) ||
      path_is_system_app_path(g->source_path) ||
      path_is_system_app_path(g->output_path);
}

static const char *
storage_name_for_path(const char *path) {
  if(path_under_root(path, "/data")) return "internal";
  if(path_under_root(path, "/mnt/ext0")) return "external";
  if(path_under_root(path, "/mnt/ext1")) return "m2";
  if(path_under_root(path, "/mnt/usb0") ||
     path_under_root(path, "/mnt/usb1") ||
     path_under_root(path, "/mnt/usb2") ||
     path_under_root(path, "/mnt/usb3") ||
     path_under_root(path, "/mnt/usb4") ||
     path_under_root(path, "/mnt/usb5") ||
     path_under_root(path, "/mnt/usb6") ||
     path_under_root(path, "/mnt/usb7")) {
    return "usb";
  }
  return "other";
}

static const gc_storage_target_def_t *
storage_target_def_for_id(const char *id) {
  if(!id || !id[0]) return NULL;
  for(size_t i = 0; i < GC_STORAGE_TARGET_COUNT; i++) {
    if(!strcmp(id, GC_STORAGE_TARGETS[i].id)) return &GC_STORAGE_TARGETS[i];
  }
  return NULL;
}

static int
storage_target_root_for_id(const char *id, char *root, size_t root_size,
                           const char **name_out) {
  const gc_storage_target_def_t *def = storage_target_def_for_id(id);
  int n;
  if(!def || !root || root_size == 0) return -1;
  n = snprintf(root, root_size, "%s", def->target_root);
  if(n < 0 || (size_t)n >= root_size) return -1;
  if(name_out) *name_out = def->name;
  return 0;
}

static int
usb_target_for_id(const char *id, gc_usb_target_t *out) {
  const gc_storage_target_def_t *def = storage_target_def_for_id(id);
  struct stat st;
  if(!def || !out) return -1;
  if(stat(def->root, &st) != 0 || !S_ISDIR(st.st_mode)) return -1;
  memset(out, 0, sizeof(*out));
  snprintf(out->id, sizeof(out->id), "%s", def->id);
  snprintf(out->name, sizeof(out->name), "%s", def->name);
  snprintf(out->root, sizeof(out->root), "%s", def->root);
  snprintf(out->target_root, sizeof(out->target_root), "%s", def->target_root);
  if(space_for_path(def->root, &out->free_bytes, &out->total_bytes) != 0) {
    return -1;
  }
  if(out->total_bytes < GC_STORAGE_TARGET_MIN_TOTAL_BYTES) {
    return -1;
  }
  if(out->free_bytes < GC_STORAGE_TARGET_MIN_FREE_BYTES) {
    return -1;
  }
  return 0;
}

static int
discover_usb_targets(gc_usb_target_t *targets, size_t max, size_t *count_out) {
  size_t count = 0;
  for(size_t i = 0; i < GC_STORAGE_TARGET_COUNT && count < max; i++) {
    if(usb_target_for_id(GC_STORAGE_TARGETS[i].id, &targets[count]) == 0) {
      count++;
    }
  }
  if(count_out) *count_out = count;
  return 0;
}

static int
can_move_to_external_storage(const char *source_path) {
  gc_usb_target_t targets[GC_STORAGE_TARGET_COUNT];
  size_t count = 0;
  if(!source_path || !source_path[0]) return 0;
  discover_usb_targets(targets, GC_STORAGE_TARGET_COUNT, &count);
  for(size_t i = 0; i < count; i++) {
    if(!path_under_root(source_path, targets[i].root)) return 1;
  }
  return 0;
}

static int paths_equal_ignoring_trailing_slash(const char *a, const char *b);
static int unmountable_game_folder_segment(const char *name);
static int path_has_unmountable_game_folder_segment(const char *path);

typedef struct gc_source_roots {
  char roots[GC_MAX_SCAN_ROOTS][1024];
  size_t count;
  uint32_t scan_depth;
  int custom_scanpaths;
} gc_source_roots_t;

static const char *const GC_SHADOW_DEFAULT_SOURCE_ROOTS[] = {
  "/data/homebrew",
  "/data/etaHEN/games",
  "/mnt/ext0",
  "/mnt/ext0/homebrew",
  "/mnt/ext0/etaHEN/games",
  "/mnt/ext1",
  "/mnt/ext1/homebrew",
  "/mnt/ext1/etaHEN/games",
  "/mnt/usb0",
  "/mnt/usb0/homebrew",
  "/mnt/usb0/etaHEN/games",
  "/mnt/usb1",
  "/mnt/usb1/homebrew",
  "/mnt/usb1/etaHEN/games",
  "/mnt/usb2",
  "/mnt/usb2/homebrew",
  "/mnt/usb2/etaHEN/games",
  "/mnt/usb3",
  "/mnt/usb3/homebrew",
  "/mnt/usb3/etaHEN/games",
  "/mnt/usb4",
  "/mnt/usb4/homebrew",
  "/mnt/usb4/etaHEN/games",
  "/mnt/usb5",
  "/mnt/usb5/homebrew",
  "/mnt/usb5/etaHEN/games",
  "/mnt/usb6",
  "/mnt/usb6/homebrew",
  "/mnt/usb6/etaHEN/games",
  "/mnt/usb7",
  "/mnt/usb7/homebrew",
  "/mnt/usb7/etaHEN/games",
  NULL,
};

static char *
trim_ascii_inplace(char *s) {
  char *end;
  if(!s) return s;
  while(*s && isspace((unsigned char)*s)) s++;
  end = s + strlen(s);
  while(end > s && isspace((unsigned char)end[-1])) end--;
  *end = 0;
  if((s[0] == '"' || s[0] == '\'') && end > s + 1 && end[-1] == s[0]) {
    s++;
    end[-1] = 0;
  }
  return s;
}

static int
parse_ini_line_gc(char *line, char **key_out, char **value_out) {
  char *s = trim_ascii_inplace(line);
  char *eq;
  if(!s || !s[0] || s[0] == '#' || s[0] == ';') return 0;
  eq = strchr(s, '=');
  if(!eq) return 0;
  *eq++ = 0;
  if(key_out) *key_out = trim_ascii_inplace(s);
  if(value_out) *value_out = trim_ascii_inplace(eq);
  return key_out && *key_out && (*key_out)[0] && value_out && *value_out;
}

static int
parse_bool_ini_gc(const char *value, int *out) {
  if(!value || !out) return 0;
  if(!strcasecmp(value, "1") || !strcasecmp(value, "true") ||
     !strcasecmp(value, "yes") || !strcasecmp(value, "on")) {
    *out = 1;
    return 1;
  }
  if(!strcasecmp(value, "0") || !strcasecmp(value, "false") ||
     !strcasecmp(value, "no") || !strcasecmp(value, "off")) {
    *out = 0;
    return 1;
  }
  return 0;
}

static int
parse_scan_depth_ini_gc(const char *value, uint32_t *out) {
  char *end = NULL;
  unsigned long parsed;
  if(!value || !value[0] || !out) return 0;
  errno = 0;
  parsed = strtoul(value, &end, 10);
  if(errno != 0 || !end || *end != 0 || parsed < 1 || parsed > 2) return 0;
  *out = (uint32_t)parsed;
  return 1;
}

static void
source_roots_add(gc_source_roots_t *roots, const char *path) {
  char normalized[1024];
  size_t len;
  if(!roots || !path || !path[0] || roots->count >= GC_MAX_SCAN_ROOTS ||
     !path_is_safe(path) || path_is_system_app_path(path) ||
     path_has_unmountable_game_folder_segment(path) ||
     path_under_root(path, GC_SHADOW_IMAGE_BASE)) {
    return;
  }
  if(snprintf(normalized, sizeof(normalized), "%s", path) >=
     (int)sizeof(normalized)) {
    return;
  }
  len = strlen(normalized);
  while(len > 1 && normalized[len - 1] == '/') normalized[--len] = 0;
  for(size_t i = 0; i < roots->count; i++) {
    if(paths_equal_ignoring_trailing_slash(roots->roots[i], normalized)) return;
  }
  snprintf(roots->roots[roots->count++], sizeof(roots->roots[0]), "%s",
           normalized);
}

static void
source_roots_add_parent(gc_source_roots_t *roots, const char *path) {
  char parent[1024];
  if(!path || !path[0] || path_parent(path, parent, sizeof(parent)) != 0) {
    return;
  }
  source_roots_add(roots, parent);
}

static void
source_roots_add_default(gc_source_roots_t *roots) {
  for(int i = 0; GC_SHADOW_DEFAULT_SOURCE_ROOTS[i]; i++) {
    source_roots_add(roots, GC_SHADOW_DEFAULT_SOURCE_ROOTS[i]);
  }
}

static void
source_roots_expand_one_child_level(gc_source_roots_t *roots) {
  size_t initial_count = roots ? roots->count : 0;
  for(size_t i = 0; roots && i < initial_count; i++) {
    DIR *d = opendir(roots->roots[i]);
    struct dirent *ent;
    if(!d) continue;
    while((ent = readdir(d)) && roots->count < GC_MAX_SCAN_ROOTS) {
      char child[1024];
      struct stat st;
      if(!strcmp(ent->d_name, ".") || !strcmp(ent->d_name, "..")) continue;
      if(!upload_segment_safe(ent->d_name)) continue;
      if(unmountable_game_folder_segment(ent->d_name)) continue;
      if(snprintf(child, sizeof(child), "%s/%s", roots->roots[i],
                  ent->d_name) >= (int)sizeof(child)) {
        continue;
      }
      if(stat(child, &st) != 0 || !S_ISDIR(st.st_mode)) continue;
      source_roots_add(roots, child);
    }
    closedir(d);
  }
}

static void
source_roots_add_manual_entries(gc_source_roots_t *roots) {
  FILE *f = fopen(GC_SHADOW_MANUAL_LIST_FILE, "r");
  char line[1024];
  if(!f) return;
  while(fgets(line, sizeof(line), f)) {
    char *path = trim_ascii_inplace(line);
    if(!path || !path[0] || path[0] == '#' || path[0] == ';') continue;
    if(!ends_with_ci(path, ".ffpfsc")) continue;
    source_roots_add_parent(roots, path);
  }
  fclose(f);
}

static void
shadow_source_roots_build(gc_source_roots_t *roots) {
  FILE *f;
  char line[1024];
  uint32_t scan_depth = 1;
  int custom_scanpaths = 0;

  if(!roots) return;
  memset(roots, 0, sizeof(*roots));
  roots->scan_depth = scan_depth;

  f = fopen(GC_SHADOW_CONFIG_FILE, "r");
  if(f) {
    while(fgets(line, sizeof(line), f)) {
      char *key = NULL;
      char *value = NULL;
      if(!parse_ini_line_gc(line, &key, &value)) continue;
      if(!strcasecmp(key, "scanpath")) {
        if(!custom_scanpaths) {
          roots->count = 0;
          custom_scanpaths = 1;
        }
        source_roots_add(roots, value);
      } else if(!strcasecmp(key, "scan_depth")) {
        (void)parse_scan_depth_ini_gc(value, &scan_depth);
      } else if(!strcasecmp(key, "recursive_scan")) {
        int enabled = 0;
        if(parse_bool_ini_gc(value, &enabled) && enabled) scan_depth = 2;
      }
    }
    fclose(f);
  }

  roots->custom_scanpaths = custom_scanpaths;
  roots->scan_depth = scan_depth;
  if(!custom_scanpaths || roots->count == 0) {
    source_roots_add_default(roots);
    roots->custom_scanpaths = 0;
  }
  if(scan_depth > 1) {
    source_roots_expand_one_child_level(roots);
  }
  source_roots_add_manual_entries(roots);
}

static int
unmountable_game_folder_segment(const char *name) {
  if(!name || !name[0]) return 0;
  if(name[0] == '.') return 1;
  return !strcasecmp(name, "$RECYCLE.BIN") ||
      !strcasecmp(name, "System Volume Information") ||
      !strcasecmp(name, "RECYCLER") ||
      !strcasecmp(name, "RECYCLED") ||
      !strcasecmp(name, "found.000") ||
      !strcasecmp(name, "lost+found");
}

static int
path_has_unmountable_game_folder_segment(const char *path) {
  const char *p = path;
  if(!p || !p[0]) return 1;
  while(*p) {
    char segment[256];
    size_t len = 0;
    while(*p == '/') p++;
    while(p[len] && p[len] != '/') len++;
    if(len >= sizeof(segment)) return 1;
    if(len > 0) {
      memcpy(segment, p, len);
      segment[len] = 0;
      if(unmountable_game_folder_segment(segment)) return 1;
    }
    p += len;
  }
  return 0;
}

static int
path_is_shadowmount_game_root(const char *path) {
  gc_source_roots_t roots;
  if(!path || !path[0] || path_has_unmountable_game_folder_segment(path)) {
    return 0;
  }
  shadow_source_roots_build(&roots);
  for(size_t i = 0; i < roots.count; i++) {
    if(paths_equal_ignoring_trailing_slash(path, roots.roots[i])) {
      return 1;
    }
  }
  return 0;
}

static uint64_t
source_size_bytes_exact_ex(const char *path, gc_source_kind_t kind,
                           int honor_cancel, int *cancelled_out) {
  struct stat st;
  if(cancelled_out) *cancelled_out = 0;
  if(honor_cancel && job_cancelled()) {
    if(cancelled_out) *cancelled_out = 1;
    errno = EINTR;
    return 0;
  }
  if(kind == GC_SOURCE_FOLDER) {
    du_state_t du;
    if(honor_cancel) du_walk_cancelable(path, &du);
    else du_walk(path, &du);
    if(du.cancelled && cancelled_out) *cancelled_out = 1;
    return du.bytes;
  }
  if(stat(path, &st) == 0 && st.st_size > 0) {
    if(honor_cancel && job_cancelled()) {
      if(cancelled_out) *cancelled_out = 1;
      errno = EINTR;
      return 0;
    }
    return (uint64_t)st.st_size;
  }
  return 0;
}

static uint64_t
source_size_bytes_exact(const char *path, gc_source_kind_t kind) {
  return source_size_bytes_exact_ex(path, kind, 0, NULL);
}

static void
populate_game_size_ex(gc_game_t *g, int exact_folder_size, int honor_cancel) {
  if(!g) return;
  g->source_size = 0;
  g->required_bytes = 0;
  g->extra_needed = 0;
  g->size_pending = 0;
  g->size_status = GC_SIZE_STATUS_UNKNOWN;
  g->size_estimated = 0;
  g->size_measured_at = 0;
  g->size_refreshing = 0;

  if(g->source_kind == GC_SOURCE_FOLDER) {
    uint64_t cached_size = 0;
    time_t measured_at = 0;
    gc_size_status_t status = GC_SIZE_STATUS_UNKNOWN;
    if(exact_folder_size) {
      int cancelled = 0;
      g->source_size = source_size_bytes_exact_ex(
          g->source_path, g->source_kind, honor_cancel, &cancelled);
      if(cancelled) {
        g->size_pending = 1;
        g->size_status = GC_SIZE_STATUS_UNKNOWN;
        return;
      }
      g->required_bytes = g->source_size;
      g->size_status = GC_SIZE_STATUS_DONE;
      g->size_measured_at = (uint64_t)time(NULL);
      gc_size_cache_store(g->source_path, g->source_size);
    } else if(gc_size_cache_lookup(g->source_path, &cached_size, &measured_at,
                                &status) == 0) {
      g->source_size = cached_size;
      g->required_bytes = 0;
      g->size_estimated = 1;
      g->size_measured_at = measured_at > 0 ? (uint64_t)measured_at : 0;
      g->size_status = status;
      g->size_refreshing = status == GC_SIZE_STATUS_REFRESHING;
    } else {
      /*
       * Library scans must stay shallow, especially on USB. Exact operation
       * sizes are measured only by explicit operations. The size cache is a
       * display estimate and must not drive free-space safety checks.
       */
      g->size_pending = 1;
      g->size_status = status;
    }
  } else {
    int cancelled = 0;
    g->source_size = source_size_bytes_exact_ex(
        g->source_path, g->source_kind, honor_cancel, &cancelled);
    if(cancelled) {
      g->size_pending = 1;
      g->size_status = GC_SIZE_STATUS_UNKNOWN;
      return;
    }
    g->required_bytes = g->source_size;
    g->size_status = GC_SIZE_STATUS_DONE;
  }

  if(g->output_path[0] && free_bytes_for_output(g->output_path,
                                                &g->free_bytes) == 0 &&
     !g->size_pending && !g->size_estimated) {
    g->extra_needed = g->free_bytes >= g->required_bytes
        ? 0
        : g->required_bytes - g->free_bytes;
  }
}

static void
size_cache_apply_display_to_game(gc_game_t *g) {
  if(!g || g->source_kind != GC_SOURCE_FOLDER) return;
  uint64_t cached_size = 0;
  time_t measured_at = 0;
  gc_size_status_t status = GC_SIZE_STATUS_UNKNOWN;
  if(gc_size_cache_lookup(g->source_path, &cached_size, &measured_at,
                       &status) == 0) {
    g->source_size = cached_size;
    g->required_bytes = 0;
    g->extra_needed = 0;
    g->size_pending = 0;
    g->size_estimated = 1;
    g->size_measured_at = measured_at > 0 ? (uint64_t)measured_at : 0;
    g->size_status = status;
    g->size_refreshing = status == GC_SIZE_STATUS_REFRESHING;
  } else {
    g->source_size = 0;
    g->required_bytes = 0;
    g->extra_needed = 0;
    g->size_pending = 1;
    g->size_estimated = 0;
    g->size_measured_at = 0;
    g->size_status = status;
    g->size_refreshing = 0;
  }
}

static void
queue_folder_game_size_rechecks(gc_game_t *games, size_t count) {
  if(!games || count == 0) return;
  pthread_mutex_lock(&g_folder_size_recheck_lock);
  if(g_folder_size_initial_recheck_done) {
    pthread_mutex_unlock(&g_folder_size_recheck_lock);
    return;
  }
  g_folder_size_initial_recheck_done = 1;
  pthread_mutex_unlock(&g_folder_size_recheck_lock);
  for(size_t i = 0; i < count; i++) {
    if(games[i].source_kind == GC_SOURCE_FOLDER && games[i].source_path[0]) {
      if(!strcmp(storage_name_for_path(games[i].source_path), "usb")) {
        gc_log("size estimate auto-skip usb path=%s", games[i].source_path);
        continue;
      }
      gc_size_cache_queue_recheck(games[i].source_path);
    }
  }
}

static uint64_t
stream_min_free_bytes_for_budget(uint64_t source_size, uint64_t budget_bytes) {
  uint64_t budget = budget_bytes ? budget_bytes : GC_STREAM_MIN_FREE_BYTES;
  if(source_size > 0 && source_size < budget) {
    return source_size;
  }
  return budget;
}

static uint64_t
stream_min_free_bytes(uint64_t source_size) {
  return stream_min_free_bytes_for_budget(source_size, 0);
}

static int
stream_delete_allowed_by_space_budget(const gc_game_t *game,
                                      uint64_t budget_bytes) {
  if(!game || !game->can_stream_delete || game->source_size == 0) return 0;
  return game->free_bytes >=
      stream_min_free_bytes_for_budget(game->source_size, budget_bytes);
}

static uint64_t
stream_extra_needed(const gc_game_t *game) {
  if(!game) return 0;
  uint64_t min_free = stream_min_free_bytes(game->source_size);
  return game->free_bytes >= min_free ? 0 : min_free - game->free_bytes;
}

static int
parse_u64_arg(const char *s, uint64_t *out) {
  char *end = NULL;
  unsigned long long value;
  if(!s || !s[0] || !out) return -1;
  errno = 0;
  value = strtoull(s, &end, 10);
  if(errno != 0 || !end || *end != '\0') return -1;
  *out = (uint64_t)value;
  return 0;
}

static int
find_outer_pfsc_candidate(const char *root, const char *title_id,
                          uint32_t shadow_hash, char *out,
                          size_t out_size) {
  char candidate[1024];
  struct stat st;
  int n;
  if(!root || !title_id || !out) return -1;
  n = snprintf(candidate, sizeof(candidate), "%s/%s.ffpfsc", root, title_id);
  if(n < 0 || (size_t)n >= sizeof(candidate)) return -1;
  if(fnv1a32_string(candidate) != shadow_hash) return -1;
  if(stat(candidate, &st) != 0 || !S_ISREG(st.st_mode)) return -1;
  snprintf(out, out_size, "%s", candidate);
  return 0;
}

static int
find_outer_pfsc_by_shadow_hash(const char *title_id, uint32_t shadow_hash,
                               char *out, size_t out_size) {
  gc_source_roots_t roots;
  shadow_source_roots_build(&roots);
  for(size_t i = 0; i < roots.count; i++) {
    if(find_outer_pfsc_candidate(roots.roots[i], title_id,
                                 shadow_hash, out, out_size) == 0) {
      return 0;
    }
  }
  return -1;
}

static int
find_exact_pfsc_by_title(const char *title_id, char *out, size_t out_size) {
  char candidate[1024];
  char err[256] = {0};
  struct stat st;
  gc_source_roots_t roots;
  if(!valid_title_id(title_id) || !out || out_size == 0) return -1;
  shadow_source_roots_build(&roots);
  for(size_t i = 0; i < roots.count; i++) {
    int n = snprintf(candidate, sizeof(candidate), "%s/%s.ffpfsc",
                     roots.roots[i], title_id);
    if(n < 0 || (size_t)n >= sizeof(candidate)) continue;
    if(stat(candidate, &st) != 0 || !S_ISREG(st.st_mode)) continue;
    pfs_decompress_info_t info = {0};
    err[0] = 0;
    if(pfs_decompress_probe(candidate, &info, err, sizeof(err)) != 0) {
      continue;
    }
    snprintf(out, out_size, "%s", candidate);
    return 0;
  }
  return -1;
}

static int remove_tree_gc(const char *path);
static void artifact_cache_invalidate(void);

static const char *
force_remount_visible_suffix(const char *name, size_t *suffix_len) {
  const char *suffix = "";
  if(!name) return NULL;
  if(ends_with_ci(name, ".ffpfsc")) {
    suffix = ".ffpfsc";
  } else if(ends_with_ci(name, ".exfat")) {
    suffix = ".exfat";
  } else if(ends_with_ci(name, ".ffpfs")) {
    suffix = ".ffpfs";
  } else if(ends_with_ci(name, "-app")) {
    suffix = "-app";
  } else {
    return NULL;
  }
  if(suffix_len) *suffix_len = strlen(suffix);
  return suffix;
}

static int
force_remount_original_name_for_temp(const char *name,
                                     char *out, size_t out_size) {
  const char *marker;
  const char *visible_name = name;
  const char *suffix;
  char title_id[64];
  size_t title_len;
  size_t prefix_len = strlen(GC_FORCE_REMOUNT_PREFIX);
  size_t name_len;
  size_t suffix_len = 0;

  if(!name || !out || out_size == 0) {
    return -1;
  }
  if(visible_name[0] == '.') visible_name++;
  name_len = strlen(visible_name);
  suffix = force_remount_visible_suffix(visible_name, &suffix_len);
  if(!suffix) return -1;
  marker = strstr(visible_name, GC_FORCE_REMOUNT_PREFIX);
  if(!marker) return -1;
  title_len = (size_t)(marker - visible_name);
  if(title_len != 9 || title_len >= sizeof(title_id)) return -1;
  if(name_len <= title_len + prefix_len + suffix_len) {
    return -1;
  }
  memcpy(title_id, visible_name, title_len);
  title_id[title_len] = 0;
  if(!valid_title_id(title_id)) return -1;
  if(snprintf(out, out_size, "%s%s", title_id, suffix) >= (int)out_size) {
    return -1;
  }
  return 0;
}

static int
cleanup_force_remount_temps_in_root(const char *root) {
  DIR *d = opendir(root);
  struct dirent *ent;
  int restored = 0;

  if(!d) return 0;
  while((ent = readdir(d))) {
    char original_name[64];
    char temp_path[1024];
    char original_path[1024];
    struct stat st;

    if(force_remount_original_name_for_temp(ent->d_name, original_name,
                                           sizeof(original_name)) != 0) {
      continue;
    }
    join_path(temp_path, sizeof(temp_path), root, ent->d_name);
    if(stat(temp_path, &st) != 0 ||
       !(S_ISREG(st.st_mode) || S_ISDIR(st.st_mode))) {
      continue;
    }
    join_path(original_path, sizeof(original_path), root, original_name);

    if(stat(original_path, &st) == 0) {
      gc_log("force remount cleanup kept temp=%s original exists=%s",
             temp_path, original_path);
      continue;
    }
    if(errno != ENOENT) {
      gc_log("force remount cleanup skipped temp=%s original=%s err=%s",
             temp_path, original_path, strerror(errno));
      continue;
    }
    if(rename(temp_path, original_path) != 0) {
      gc_log("force remount cleanup restore failed temp=%s original=%s err=%s",
             temp_path, original_path, strerror(errno));
      continue;
    }
    restored++;
    gc_log("force remount cleanup restored temp=%s original=%s",
           temp_path, original_path);
  }
  closedir(d);
  return restored;
}

static void
cleanup_force_remount_temps_on_startup(void) {
  int restored = 0;
  gc_source_roots_t roots;

  shadow_source_roots_build(&roots);
  for(size_t i = 0; i < roots.count; i++) {
    restored += cleanup_force_remount_temps_in_root(roots.roots[i]);
  }
  if(restored > 0) {
    char err[256] = {0};
    if(gc_shadowmount_request_scan(err, sizeof(err)) != 0) {
      gc_log("force remount cleanup scan request failed restored=%d err=%s",
             restored, err[0] ? err : "unknown");
    } else {
      gc_log("force remount cleanup requested scan restored=%d", restored);
    }
  }
}

static int
cleanup_delete_pending_temps_in_root(const char *root) {
  DIR *d = opendir(root);
  struct dirent *ent;
  int removed = 0;

  if(!d) return 0;
  while((ent = readdir(d))) {
    char temp_path[1024];
    struct stat st;

    if(ent->d_name[0] != '.' ||
       !ends_with_ci(ent->d_name, ".gc-delete.tmp")) {
      continue;
    }
    join_path(temp_path, sizeof(temp_path), root, ent->d_name);
    if(!path_is_safe(temp_path) || lstat(temp_path, &st) != 0) {
      continue;
    }
    if(remove_tree_gc(temp_path) != 0) {
      gc_log("delete pending cleanup failed path=%s err=%s",
             temp_path, strerror(errno));
      continue;
    }
    removed++;
    gc_log("delete pending cleanup removed path=%s", temp_path);
  }
  closedir(d);
  return removed;
}

static void
cleanup_delete_pending_temps_on_startup(void) {
  int removed = 0;
  gc_source_roots_t roots;

  shadow_source_roots_build(&roots);
  for(size_t i = 0; i < roots.count; i++) {
    removed += cleanup_delete_pending_temps_in_root(roots.roots[i]);
  }
  removed += cleanup_delete_pending_temps_in_root(GC_INTERNAL_GAME_ROOT);
  for(size_t i = 0; i < GC_STORAGE_TARGET_COUNT; i++) {
    removed += cleanup_delete_pending_temps_in_root(GC_STORAGE_TARGETS[i].root);
    removed += cleanup_delete_pending_temps_in_root(
        GC_STORAGE_TARGETS[i].target_root);
  }
  if(removed > 0) {
    char err[256] = {0};
    artifact_cache_invalidate();
    if(gc_shadowmount_request_scan(err, sizeof(err)) != 0) {
      gc_log("delete pending cleanup scan request failed removed=%d err=%s",
             removed, err[0] ? err : "unknown");
    } else {
      gc_log("delete pending cleanup requested scan removed=%d", removed);
    }
  }
}

static int
read_file_limited(const char *path, char **out, size_t *out_size,
                  size_t limit) {
  int fd = open(path, O_RDONLY);
  if(fd < 0) return -1;
  struct stat st;
  if(fstat(fd, &st) != 0 || st.st_size < 0 || (uint64_t)st.st_size > limit) {
    close(fd);
    return -1;
  }
  char *buf = calloc(1, (size_t)st.st_size + 1);
  if(!buf) {
    close(fd);
    return -1;
  }
  size_t got = 0;
  while(got < (size_t)st.st_size) {
    ssize_t n = read(fd, buf + got, (size_t)st.st_size - got);
    if(n < 0) {
      if(errno == EINTR) continue;
      free(buf);
      close(fd);
      return -1;
    }
    if(n == 0) break;
    got += (size_t)n;
  }
  close(fd);
  buf[got] = 0;
  if(out) *out = buf;
  else free(buf);
  if(out_size) *out_size = got;
  return 0;
}

static int
json_find_string_value(const char *json, const char *key,
                       char *out, size_t out_size) {
  char pattern[96];
  snprintf(pattern, sizeof(pattern), "\"%s\"", key);
  const char *p = strstr(json ? json : "", pattern);
  if(!p) return 0;
  p += strlen(pattern);
  while(*p && isspace((unsigned char)*p)) p++;
  if(*p != ':') return 0;
  p++;
  while(*p && isspace((unsigned char)*p)) p++;
  if(*p != '"') return 0;
  p++;
  size_t pos = 0;
  while(*p && *p != '"' && pos + 1 < out_size) {
    if(*p == '\\' && p[1]) p++;
    out[pos++] = *p++;
  }
  out[pos] = 0;
  return pos > 0;
}

static int
ampr_latest_cached(char *version, size_t version_size,
                   char sha[65]) {
  char *json = NULL;
  size_t json_size = 0;
  char local_version[64] = {0};
  char local_sha[65] = {0};
  if(version && version_size) version[0] = 0;
  if(sha) sha[0] = 0;
  if(read_file_limited(GC_AMPR_LATEST_FILE, &json, &json_size, 64 * 1024) != 0) {
    return 0;
  }
  (void)json_size;
  json_find_string_value(json, "version", local_version, sizeof(local_version));
  json_find_string_value(json, "sha256", local_sha, sizeof(local_sha));
  free(json);
  if(!ampr_version_safe(local_version) || !sha256_hex_valid(local_sha)) {
    return 0;
  }
  if(version && version_size) snprintf(version, version_size, "%s", local_version);
  if(sha) snprintf(sha, 65, "%s", local_sha);
  return 1;
}

static int
ampr_cached_version_for_sha(const char sha[65], char *version,
                            size_t version_size) {
  DIR *d;
  int found = 0;
  if(version && version_size) version[0] = 0;
  if(!sha256_hex_valid(sha) || !version || version_size == 0) return 0;
  d = opendir(GC_AMPR_DIR);
  if(!d) return 0;
  struct dirent *ent;
  while((ent = readdir(d)) != NULL) {
    char bin[1024];
    char hash[65] = {0};
    char err[128] = {0};
    struct stat st;
    if(!ampr_version_safe(ent->d_name)) continue;
    if(ampr_cache_binary_path(ent->d_name, bin, sizeof(bin)) != 0) continue;
    if(stat(bin, &st) != 0 || !S_ISREG(st.st_mode)) continue;
    if(hash_file_sha256_hex(bin, hash, err, sizeof(err)) != 0) continue;
    if(strcasecmp(hash, sha) != 0) continue;
    if(!found || strcmp(ent->d_name, version) > 0) {
      snprintf(version, version_size, "%s", ent->d_name);
      found = 1;
    }
  }
  closedir(d);
  return found;
}

static int
ampr_intent_valid(const char *intent) {
  return !strcmp(intent ? intent : "", "latest") ||
      !strcmp(intent ? intent : "", "manual") ||
      !strcmp(intent ? intent : "", "custom");
}

static const char *
ampr_normalized_intent(const char *intent, const char *version) {
  if(version && !strncmp(version, "custom-", 7)) return "custom";
  return ampr_intent_valid(intent) ? intent : "manual";
}

static int
ampr_selection_path_for_title_source(const char *title_id,
                                     const char *source_path,
                                     char *out, size_t out_size) {
  if(!valid_title_id(title_id) || !source_path || !source_path[0] ||
     !out || out_size == 0) {
    return -1;
  }
  int n = snprintf(out, out_size, "%s/%s-%08x.json",
                   GC_AMPR_SELECTION_DIR, title_id,
                   fnv1a32_string(source_path));
  return n < 0 || (size_t)n >= out_size ? -1 : 0;
}

static int
ampr_selection_read(const char *title_id, const char *source_path,
                    char *intent, size_t intent_size,
                    char *version, size_t version_size,
                    char sha[65]) {
  char path[1024];
  char marker_path[1024] = {0};
  char marker_intent[16] = {0};
  char marker_version[64] = {0};
  char marker_sha[65] = {0};
  char *json = NULL;
  size_t json_size = 0;
  if(intent && intent_size) intent[0] = 0;
  if(version && version_size) version[0] = 0;
  if(sha) sha[0] = 0;
  if(ampr_selection_path_for_title_source(title_id, source_path,
                                          path, sizeof(path)) != 0 ||
     read_file_limited(path, &json, &json_size, 64 * 1024) != 0) {
    return 0;
  }
  (void)json_size;
  json_find_string_value(json, "path", marker_path, sizeof(marker_path));
  json_find_string_value(json, "intent", marker_intent,
                         sizeof(marker_intent));
  json_find_string_value(json, "version", marker_version,
                         sizeof(marker_version));
  json_find_string_value(json, "sha256", marker_sha, sizeof(marker_sha));
  free(json);
  if(marker_path[0] && strcmp(marker_path, source_path)) return 0;
  if(!ampr_intent_valid(marker_intent) || !sha256_hex_valid(marker_sha)) {
    return 0;
  }
  if(marker_version[0] && !ampr_version_safe(marker_version)) return 0;
  if(intent && intent_size) snprintf(intent, intent_size, "%s", marker_intent);
  if(version && version_size) {
    snprintf(version, version_size, "%s", marker_version);
  }
  if(sha) snprintf(sha, 65, "%s", marker_sha);
  return 1;
}

static int
ampr_selection_write(const char *title_id, const char *source_path,
                     const char *intent, const char *version,
                     const char sha[65], const char *result_mode) {
  char path[1024];
  char tmp[1024];
  json_buf_t b = {0};
  int fd = -1;
  const char *normalized = ampr_normalized_intent(intent, version);
  if(!valid_title_id(title_id) || !source_path || !source_path[0] ||
     !ampr_intent_valid(normalized) || !ampr_version_safe(version) ||
     !sha256_hex_valid(sha)) {
    return -1;
  }
  if(mkdirs(GC_AMPR_SELECTION_DIR) != 0 ||
     ampr_selection_path_for_title_source(title_id, source_path,
                                          path, sizeof(path)) != 0) {
    return -1;
  }
  int n = snprintf(tmp, sizeof(tmp), "%s.tmp", path);
  if(n < 0 || (size_t)n >= sizeof(tmp)) return -1;
  if(json_append(&b, "{\"titleId\":") != 0 ||
     json_string(&b, title_id) != 0 ||
     json_append(&b, ",\"path\":") != 0 ||
     json_string(&b, source_path) != 0 ||
     json_append(&b, ",\"intent\":") != 0 ||
     json_string(&b, normalized) != 0 ||
     json_append(&b, ",\"version\":") != 0 ||
     json_string(&b, version) != 0 ||
     json_append(&b, ",\"sha256\":") != 0 ||
     json_string(&b, sha) != 0 ||
     json_append(&b, ",\"resultMode\":") != 0 ||
     json_string(&b, result_mode ? result_mode : "") != 0 ||
     json_appendf(&b, ",\"updatedAt\":%ld}\n", (long)time(NULL)) != 0) {
    free(b.data);
    return -1;
  }
  fd = open(tmp, O_WRONLY | O_CREAT | O_TRUNC, 0666);
  if(fd < 0) {
    free(b.data);
    return -1;
  }
  if(write_all_fd(fd, b.data, b.len) != 0 || fsync(fd) != 0) {
    close(fd);
    unlink(tmp);
    free(b.data);
    return -1;
  }
  close(fd);
  fd = -1;
  if(rename(tmp, path) != 0) {
    unlink(tmp);
    free(b.data);
    return -1;
  }
  fsync_parent_dir_best_effort(path);
  free(b.data);
  return 0;
}

static int
ampr_original_dir_for_title_source(const char *title_id,
                                   const char *source_path,
                                   char *out, size_t out_size) {
  if(!valid_title_id(title_id) || !source_path || !source_path[0] ||
     !out || out_size == 0) {
    return -1;
  }
  int n = snprintf(out, out_size, "%s/%s-%08x",
                   GC_AMPR_ORIGINAL_DIR, title_id,
                   fnv1a32_string(source_path));
  return n < 0 || (size_t)n >= out_size ? -1 : 0;
}

static int
ampr_original_binary_path(const char *title_id, const char *source_path,
                          char *out, size_t out_size) {
  char dir[1024];
  if(ampr_original_dir_for_title_source(title_id, source_path,
                                        dir, sizeof(dir)) != 0) {
    return -1;
  }
  int n = snprintf(out, out_size, "%s/%s", dir, GC_AMPR_BINARY_NAME);
  return n < 0 || (size_t)n >= out_size ? -1 : 0;
}

static int
ampr_original_metadata_path(const char *title_id, const char *source_path,
                            char *out, size_t out_size) {
  char dir[1024];
  if(ampr_original_dir_for_title_source(title_id, source_path,
                                        dir, sizeof(dir)) != 0) {
    return -1;
  }
  int n = snprintf(out, out_size, "%s/metadata.json", dir);
  return n < 0 || (size_t)n >= out_size ? -1 : 0;
}

static int
ampr_original_lookup(const char *title_id, const char *source_path,
                     char path_out[1024], char sha_out[65],
                     uint64_t *size_out) {
  char path[1024];
  char sha[65] = {0};
  char err[128] = {0};
  struct stat st;
  if(path_out) path_out[0] = 0;
  if(sha_out) sha_out[0] = 0;
  if(size_out) *size_out = 0;
  if(ampr_original_binary_path(title_id, source_path, path,
                               sizeof(path)) != 0 ||
     stat(path, &st) != 0 || !S_ISREG(st.st_mode) || st.st_size <= 0) {
    return 0;
  }
  if(hash_file_sha256_hex(path, sha, err, sizeof(err)) != 0 ||
     !sha256_hex_valid(sha)) {
    return 0;
  }
  if(path_out) snprintf(path_out, 1024, "%s", path);
  if(sha_out) snprintf(sha_out, 65, "%s", sha);
  if(size_out) *size_out = (uint64_t)st.st_size;
  return 1;
}

static int
ampr_write_original_metadata(const gc_game_t *game, const char *backup_path,
                             const char sha[65], uint64_t size,
                             char *err, size_t err_size) {
  char meta[1024];
  char tmp[1024];
  json_buf_t b = {0};
  int fd = -1;
  if(ampr_original_metadata_path(game->title_id, game->source_path,
                                 meta, sizeof(meta)) != 0) {
    snprintf(err, err_size, "%s", "original AMPR metadata path too long");
    return -1;
  }
  int n = snprintf(tmp, sizeof(tmp), "%s.tmp", meta);
  if(n < 0 || (size_t)n >= sizeof(tmp)) {
    snprintf(err, err_size, "%s", "original AMPR metadata temp path too long");
    return -1;
  }
  if(json_append(&b, "{\"titleId\":") != 0 ||
     json_string(&b, game->title_id) != 0 ||
     json_append(&b, ",\"sourcePath\":") != 0 ||
     json_string(&b, game->source_path) != 0 ||
     json_append(&b, ",\"amprPath\":") != 0 ||
     json_string(&b, game->ampr_path) != 0 ||
     json_append(&b, ",\"path\":") != 0 ||
     json_string(&b, backup_path) != 0 ||
     json_append(&b, ",\"sha256\":") != 0 ||
     json_string(&b, sha) != 0 ||
     json_appendf(&b, ",\"size\":%llu,\"savedAt\":%ld}\n",
                  (unsigned long long)size, (long)time(NULL)) != 0) {
    snprintf(err, err_size, "%s", "build original AMPR metadata failed");
    free(b.data);
    return -1;
  }
  fd = open(tmp, O_WRONLY | O_CREAT | O_TRUNC, 0666);
  if(fd < 0) {
    snprintf(err, err_size, "open original AMPR metadata: %s",
             strerror(errno));
    free(b.data);
    return -1;
  }
  if(write_all_fd(fd, b.data, b.len) != 0 || fsync(fd) != 0) {
    snprintf(err, err_size, "write original AMPR metadata: %s",
             strerror(errno));
    close(fd);
    unlink(tmp);
    free(b.data);
    return -1;
  }
  close(fd);
  fd = -1;
  if(rename(tmp, meta) != 0) {
    snprintf(err, err_size, "publish original AMPR metadata: %s",
             strerror(errno));
    unlink(tmp);
    free(b.data);
    return -1;
  }
  fsync_parent_dir_best_effort(meta);
  free(b.data);
  return 0;
}

static int
ampr_original_backup_prepare(const gc_game_t *game, char *err,
                             size_t err_size) {
  char existing_path[1024];
  char backup_path[1024];
  char backup_dir[1024];
  char tmp[1024];
  char backup_sha[65] = {0};
  char cached_version[64] = {0};
  struct stat st;
  uint64_t backup_size = 0;
  if(!game || !game->ampr_present || !sha256_hex_valid(game->ampr_sha256)) {
    return 0;
  }
  if(ampr_original_lookup(game->title_id, game->source_path,
                          existing_path, NULL, NULL)) {
    return 0;
  }
  if(ampr_cached_version_for_sha(game->ampr_sha256, cached_version,
                                 sizeof(cached_version))) {
    return 0;
  }
  if(!game->ampr_path[0]) {
    snprintf(err, err_size, "%s", "original AMPR source path is unavailable");
    return -1;
  }
  if(stat(game->ampr_path, &st) != 0 || !S_ISREG(st.st_mode) ||
     st.st_size <= 0) {
    snprintf(err, err_size, "original AMPR source is not readable: %s",
             game->ampr_path);
    return -1;
  }
  if(ampr_original_dir_for_title_source(game->title_id, game->source_path,
                                        backup_dir,
                                        sizeof(backup_dir)) != 0 ||
     ampr_original_binary_path(game->title_id, game->source_path,
                               backup_path, sizeof(backup_path)) != 0) {
    snprintf(err, err_size, "%s", "original AMPR backup path too long");
    return -1;
  }
  if(mkdirs(backup_dir) != 0) {
    snprintf(err, err_size, "create original AMPR backup dir: %s",
             strerror(errno));
    return -1;
  }
  int n = snprintf(tmp, sizeof(tmp), "%s/.%s.tmp",
                   backup_dir, GC_AMPR_BINARY_NAME);
  if(n < 0 || (size_t)n >= sizeof(tmp)) {
    snprintf(err, err_size, "%s", "original AMPR temp path too long");
    return -1;
  }
  unlink(tmp);
  if(copy_file_contents(game->ampr_path, tmp, err, err_size) != 0) {
    unlink(tmp);
    return -1;
  }
  if(hash_file_sha256_hex(tmp, backup_sha, err, err_size) != 0 ||
     strcasecmp(backup_sha, game->ampr_sha256) != 0) {
    unlink(tmp);
    snprintf(err, err_size, "%s", "original AMPR backup verification failed");
    return -1;
  }
  if(rename(tmp, backup_path) != 0) {
    unlink(tmp);
    snprintf(err, err_size, "publish original AMPR backup: %s",
             strerror(errno));
    return -1;
  }
  fsync_parent_dir_best_effort(backup_path);
  backup_size = (uint64_t)st.st_size;
  if(ampr_write_original_metadata(game, backup_path, backup_sha,
                                  backup_size, err, err_size) != 0) {
    return -1;
  }
  gc_log("original AMPR backup saved title=%s source=%s sha=%s path=%s",
         game->title_id, game->source_path, backup_sha, backup_path);
  return 0;
}

static int
ampr_latest_update_needed_for_game(const gc_game_t *g, int have_latest,
                                   int current_sha_cached) {
  char intent[16] = {0};
  char version[64] = {0};
  char selected_sha[65] = {0};
  if(!g || !have_latest || !g->ampr_present ||
     !sha256_hex_valid(g->ampr_sha256) ||
     !sha256_hex_valid(g->ampr_latest_sha256)) {
    return 0;
  }
  if(strcasecmp(g->ampr_sha256, g->ampr_latest_sha256) == 0) return 0;

  if(ampr_selection_read(g->title_id, g->source_path,
                         intent, sizeof(intent),
                         version, sizeof(version),
                         selected_sha)) {
    return !strcmp(intent, "latest") &&
        strcasecmp(selected_sha, g->ampr_sha256) == 0;
  }

  /*
   * No marker means this predates intent tracking. If the current SHA already
   * exists in the local AMPR cache, assume the user intentionally installed or
   * selected it and keep the primary action quiet. Unknown AMPR SHA means this
   * is the first GameCompressor-managed update opportunity for the title.
   */
  return current_sha_cached ? 0 : 1;
}

static const char *
json_skip_ws(const char *p, const char *end) {
  while(p && *p && (!end || p < end) && isspace((unsigned char)*p)) p++;
  return p;
}

static const char *
json_skip_string_span(const char *p, const char *end) {
  if(!p || (!end && !*p) || (end && p >= end) || *p != '"') return p;
  p++;
  while(*p && (!end || p < end)) {
    if(*p == '\\' && p[1]) {
      p += 2;
      continue;
    }
    if(*p == '"') return p + 1;
    p++;
  }
  return p;
}

static const char *
json_skip_value_span(const char *p, const char *end) {
  char stack[32];
  size_t depth = 0;
  p = json_skip_ws(p, end);
  if(!p || (!end && !*p) || (end && p >= end)) return p;
  if(*p == '"') return json_skip_string_span(p, end);
  if(*p != '{' && *p != '[') {
    while(*p && (!end || p < end) && *p != ',' && *p != '}' && *p != ']') p++;
    return p;
  }
  while(*p && (!end || p < end)) {
    if(*p == '"') {
      p = json_skip_string_span(p, end);
      continue;
    }
    if(*p == '{' || *p == '[') {
      if(depth < sizeof(stack)) stack[depth++] = *p == '{' ? '}' : ']';
      p++;
      continue;
    }
    if((*p == '}' || *p == ']') && depth > 0) {
      char expected = stack[depth - 1];
      char actual = *p;
      depth--;
      p++;
      if(actual != expected) return p;
      if(depth == 0) return p;
      continue;
    }
    p++;
  }
  return p;
}

static int
json_read_string_span(const char *p, const char *end,
                      char *out, size_t out_size,
                      const char **after_out) {
  if(after_out) *after_out = p;
  if(!p || out_size == 0 || (!end && !*p) || (end && p >= end) || *p != '"') {
    return 0;
  }
  p++;
  size_t pos = 0;
  while(*p && (!end || p < end) && *p != '"') {
    if(*p == '\\' && p[1] && (!end || p + 1 < end)) p++;
    if(pos + 1 < out_size) out[pos++] = *p;
    p++;
  }
  if(!*p || (end && p >= end) || *p != '"') {
    if(out_size) out[0] = 0;
    return 0;
  }
  out[pos] = 0;
  if(after_out) *after_out = p + 1;
  return pos > 0;
}

static int
json_find_value_span(const char *start, const char *end, const char *key,
                     const char **value_start, const char **value_end) {
  char found[96];
  const char *p = start;
  if(value_start) *value_start = NULL;
  if(value_end) *value_end = NULL;
  if(!start || !key || !key[0]) return 0;
  while(*p && (!end || p < end)) {
    p = json_skip_ws(p, end);
    if(!*p || (end && p >= end)) break;
    if(*p != '"') {
      p++;
      continue;
    }
    const char *after_key = p;
    if(!json_read_string_span(p, end, found, sizeof(found), &after_key)) {
      p = json_skip_string_span(p, end);
      continue;
    }
    const char *colon = json_skip_ws(after_key, end);
    if(!*colon || (end && colon >= end) || *colon != ':') {
      p = after_key;
      continue;
    }
    const char *value = json_skip_ws(colon + 1, end);
    const char *after_value = json_skip_value_span(value, end);
    if(!strcmp(found, key)) {
      if(value_start) *value_start = value;
      if(value_end) *value_end = after_value;
      return 1;
    }
    p = after_value;
  }
  return 0;
}

static int
json_find_string_span(const char *start, const char *end, const char *key,
                      char *out, size_t out_size) {
  const char *value = NULL;
  const char *after = NULL;
  return json_find_value_span(start, end, key, &value, &after) &&
         json_read_string_span(value, after, out, out_size, NULL);
}

static int
json_find_localized_title_for_locale(const char *localized,
                                     const char *localized_end,
                                     const char *locale,
                                     char *out, size_t out_size) {
  const char *entry = NULL;
  const char *entry_end = NULL;
  if(!locale || !locale[0]) return 0;
  if(!json_find_value_span(localized, localized_end, locale, &entry,
                           &entry_end) ||
     !entry || *entry != '{') {
    return 0;
  }
  return json_find_string_span(entry, entry_end, "titleName", out, out_size) ||
         json_find_string_span(entry, entry_end, "name", out, out_size);
}

static int
json_find_localized_title_by_prefix(const char *localized,
                                    const char *localized_end,
                                    const char *prefix,
                                    char *out, size_t out_size) {
  char key[96];
  const char *p = localized;
  size_t prefix_len = strlen(prefix ? prefix : "");
  while(*p && (!localized_end || p < localized_end)) {
    p = json_skip_ws(p, localized_end);
    if(!*p || (localized_end && p >= localized_end)) break;
    if(*p != '"') {
      p++;
      continue;
    }
    const char *after_key = p;
    if(!json_read_string_span(p, localized_end, key, sizeof(key),
                              &after_key)) {
      p = json_skip_string_span(p, localized_end);
      continue;
    }
    const char *colon = json_skip_ws(after_key, localized_end);
    if(!*colon || (localized_end && colon >= localized_end) || *colon != ':') {
      p = after_key;
      continue;
    }
    const char *value = json_skip_ws(colon + 1, localized_end);
    const char *after_value = json_skip_value_span(value, localized_end);
    if(value && *value == '{' &&
       (!prefix || !prefix[0] || !strncasecmp(key, prefix, prefix_len)) &&
       (json_find_string_span(value, after_value, "titleName", out, out_size) ||
        json_find_string_span(value, after_value, "name", out, out_size))) {
      return 1;
    }
    p = after_value;
  }
  return 0;
}

static int
json_find_localized_title(const char *json, char *out, size_t out_size) {
  const char *localized = NULL;
  const char *localized_end = NULL;
  char default_language[64];
  if(!json_find_value_span(json, NULL, "localizedParameters", &localized,
                           &localized_end) ||
     !localized || *localized != '{') {
    return 0;
  }
  if(json_find_string_span(localized, localized_end, "defaultLanguage",
                           default_language, sizeof(default_language)) &&
     json_find_localized_title_for_locale(localized, localized_end,
                                          default_language, out, out_size)) {
    return 1;
  }
  if(json_find_localized_title_for_locale(localized, localized_end, "en-US",
                                          out, out_size) ||
     json_find_localized_title_for_locale(localized, localized_end, "en-GB",
                                          out, out_size) ||
     json_find_localized_title_by_prefix(localized, localized_end, "en-",
                                         out, out_size) ||
     json_find_localized_title_by_prefix(localized, localized_end, NULL,
                                         out, out_size)) {
    return 1;
  }
  return 0;
}

static uint64_t
json_find_u64_value(const char *json, const char *key, uint64_t fallback) {
  char pattern[96];
  snprintf(pattern, sizeof(pattern), "\"%s\"", key);
  const char *p = strstr(json ? json : "", pattern);
  if(!p) return fallback;
  p += strlen(pattern);
  while(*p && isspace((unsigned char)*p)) p++;
  if(*p != ':') return fallback;
  p++;
  while(*p && isspace((unsigned char)*p)) p++;
  return (uint64_t)strtoull(p, NULL, 10);
}

static int
json_find_bool_value(const char *json, const char *key, int fallback) {
  const char *value = NULL;
  const char *after = NULL;
  if(!json_find_value_span(json, NULL, key, &value, &after) || !value) {
    return fallback;
  }
  value = json_skip_ws(value, after);
  if(!strncmp(value, "true", 4)) return 1;
  if(!strncmp(value, "false", 5)) return 0;
  return fallback;
}

static void
load_game_name(gc_game_t *g) {
  char path[1024];
  char *json = NULL;
  size_t json_size = 0;
  snprintf(g->name, sizeof(g->name), "%s", g->title_id);
  snprintf(path, sizeof(path), "%s/%s/param.json", GC_APPMETA_BASE,
           g->title_id);
  if(read_file_limited(path, &json, &json_size, 256 * 1024) != 0) return;
  (void)json_size;
  if(!json_find_localized_title(json, g->name, sizeof(g->name)) &&
     !json_find_string_value(json, "titleName", g->name, sizeof(g->name)) &&
     !json_find_string_value(json, "name", g->name, sizeof(g->name))) {
    snprintf(g->name, sizeof(g->name), "%s", g->title_id);
  }
  free(json);
}

static void
load_game_icon(gc_game_t *g) {
  char icon_path[1024];
  struct stat st;

  g->has_icon = 0;
  g->icon_size = 0;
  g->icon_mtime = 0;

  snprintf(icon_path, sizeof(icon_path), "%s/%s/icon0.png", GC_APP_BASE,
           g->title_id);
  g->has_icon = stat(icon_path, &st) == 0 && S_ISREG(st.st_mode);
  if(!g->has_icon) {
    snprintf(icon_path, sizeof(icon_path), "%s/%s/icon0.png",
             GC_APPMETA_BASE, g->title_id);
    g->has_icon = stat(icon_path, &st) == 0 && S_ISREG(st.st_mode);
  }
  if(g->has_icon) {
    g->icon_size = (uint64_t)st.st_size;
    g->icon_mtime = (uint64_t)st.st_mtime;
  }
}

static void
marker_path_for_title(const char *title_id, char *out, size_t out_size) {
  snprintf(out, out_size, "%s/%s.json", GC_VALIDATION_DIR, title_id);
}

static int
marker_path_for_title_source(const char *title_id, const char *source_path,
                             char *out, size_t out_size) {
  if(!valid_title_id(title_id) || !source_path || !source_path[0] ||
     !out || out_size == 0) {
    return -1;
  }
  int n = snprintf(out, out_size, "%s/%s-%08x.json",
                   GC_VALIDATION_DIR, title_id,
                   fnv1a32_string(source_path));
  return n < 0 || (size_t)n >= out_size ? -1 : 0;
}

static void
delete_validation_marker(const char *title_id) {
  char marker[1024];
  if(!title_id || !title_id[0]) return;
  marker_path_for_title(title_id, marker, sizeof(marker));
  if(unlink(marker) != 0 && errno != ENOENT) {
    gc_log("validation marker delete failed title=%s err=%s",
           title_id, strerror(errno));
  }
}

static void
delete_validation_marker_for_path(const char *title_id,
                                  const char *source_path) {
  char marker[1024];
  char legacy[1024];
  char *json = NULL;
  size_t json_size = 0;
  char marker_path[1024] = {0};

  if(!valid_title_id(title_id) || !source_path || !source_path[0]) {
    delete_validation_marker(title_id);
    return;
  }

  if(marker_path_for_title_source(title_id, source_path, marker,
                                  sizeof(marker)) == 0 &&
     unlink(marker) != 0 && errno != ENOENT) {
    gc_log("validation marker delete failed title=%s path=%s err=%s",
           title_id, marker, strerror(errno));
  }

  marker_path_for_title(title_id, legacy, sizeof(legacy));
  if(read_file_limited(legacy, &json, &json_size, 64 * 1024) == 0) {
    (void)json_size;
    json_find_string_value(json, "path", marker_path, sizeof(marker_path));
    free(json);
    if(!strcmp(marker_path, source_path) &&
       unlink(legacy) != 0 && errno != ENOENT) {
      gc_log("legacy validation marker delete failed title=%s err=%s",
             title_id, strerror(errno));
    }
  }
}

static int
load_validation_state(gc_game_t *g) {
  if(g->source_kind != GC_SOURCE_COMPRESSED) {
    snprintf(g->validation_status, sizeof(g->validation_status), "%s", "n/a");
    g->validation = GC_VALIDATION_NONE;
    g->apr_indexed = 0;
    g->ampr_hot_swap_optimized = 0;
    return 0;
  }

  char path[1024];
  char *json = NULL;
  size_t json_size = 0;
  struct stat st;
  int legacy_marker = 0;
  int have_marker = 0;
  if(marker_path_for_title_source(g->title_id, g->source_path, path,
                                  sizeof(path)) == 0 &&
     read_file_limited(path, &json, &json_size, 64 * 1024) == 0) {
    have_marker = 1;
  } else {
    marker_path_for_title(g->title_id, path, sizeof(path));
    legacy_marker = 1;
    if(read_file_limited(path, &json, &json_size, 64 * 1024) == 0) {
      have_marker = 1;
    }
  }
  if(!have_marker) {
    snprintf(g->validation_status, sizeof(g->validation_status), "%s",
             "not-validated");
    g->validation = GC_VALIDATION_NONE;
    g->apr_indexed = 0;
    g->ampr_hot_swap_optimized = 0;
    return 0;
  }
  (void)json_size;
  char image_path[1024];
  char marker_status[32];
  image_path[0] = 0;
  marker_status[0] = 0;
  json_find_string_value(json, "path", image_path, sizeof(image_path));
  json_find_string_value(json, "status", marker_status, sizeof(marker_status));
  uint64_t old_size = json_find_u64_value(json, "sourceSize", 0);
  int stat_ok = stat(g->source_path, &st) == 0;
  int marker_matches_path = stat_ok &&
      strcmp(image_path, g->source_path) == 0 &&
      (uint64_t)st.st_size == old_size;
  int stats_only_marker = !strcmp(marker_status, "compression-stats") ||
      !strcmp(marker_status, "not-mounted");
  if(marker_matches_path) {
    g->compression_source_size =
        json_find_u64_value(json, "compressionSourceSize", 0);
    g->compressed_size =
        json_find_u64_value(json, "compressionCompressedSize", 0);
    g->saved_bytes =
        json_find_u64_value(json, "compressionSavedBytes", 0);
    g->apr_indexed = json_find_bool_value(json, "aprIndexed", 0);
    g->ampr_hot_swap_optimized =
        json_find_bool_value(json, "amprHotSwapOptimized", 0);
  } else {
    g->apr_indexed = 0;
    g->ampr_hot_swap_optimized = 0;
  }
  if(!marker_matches_path ||
     !strcmp(marker_status, "bad-blocks-found") ||
     !strcmp(marker_status, "bad-blocks-found-not-mounted")) {
    snprintf(g->validation_status, sizeof(g->validation_status), "%s",
             "not-validated");
    g->validation = GC_VALIDATION_NONE;
    if(marker_matches_path || !legacy_marker) {
      (void)unlink(path);
    }
  } else {
    if(stats_only_marker) {
      snprintf(g->validation_status, sizeof(g->validation_status), "%s",
               "not-validated");
      g->validation = GC_VALIDATION_NONE;
    } else {
      snprintf(g->validation_status, sizeof(g->validation_status), "%s",
               "validated");
      g->validation = GC_VALIDATION_VALIDATED;
    }
  }
  free(json);
  return 0;
}

static int
write_validation_marker_file(const char *path, const char *data, size_t len) {
  int fd = open(path, O_WRONLY | O_CREAT | O_TRUNC, 0666);
  if(fd < 0) return -1;
  int rc = write_all_fd(fd, data, len);
  fsync(fd);
  close(fd);
  return rc;
}

static uint64_t
folder_size_for_compression_stats(const char *path) {
  struct stat st;
  if(!path || !path[0]) return 0;
  if(lstat(path, &st) != 0 || !S_ISDIR(st.st_mode)) return 0;
  uint64_t exact = source_size_bytes_exact(path, GC_SOURCE_FOLDER);
  if(exact > 0) gc_size_cache_store(path, exact);
  return exact;
}

static uint64_t
infer_compression_source_size_from_known_roots(const char *title_id,
                                               const char *image_path,
                                               uint64_t compressed_size) {
  char parent[1024];
  char candidate[1024];
  uint64_t best = 0;
  gc_source_roots_t roots;

  if(!valid_title_id(title_id) || !image_path || !image_path[0]) return 0;

  if(path_parent(image_path, parent, sizeof(parent)) == 0) {
    int n = snprintf(candidate, sizeof(candidate), "%s/%s-app", parent,
                     title_id);
    if(n >= 0 && (size_t)n < sizeof(candidate)) {
      best = folder_size_for_compression_stats(candidate);
      if(best > compressed_size) return best;
    }
  }

  shadow_source_roots_build(&roots);
  for(size_t i = 0; i < roots.count; i++) {
    int n = snprintf(candidate, sizeof(candidate), "%s/%s-app",
                     roots.roots[i], title_id);
    uint64_t size = 0;
    if(n < 0 || (size_t)n >= sizeof(candidate)) continue;
    size = folder_size_for_compression_stats(candidate);
    if(size > best) best = size;
    if(best > compressed_size) return best;
  }
  return best > compressed_size ? best : 0;
}

static int
write_validation_marker_ex(const char *title_id, const char *image_path,
                           const pfs_repair_info_t *info, const char *result,
                           uint64_t compression_source_size,
                           int apr_indexed,
                           int ampr_hot_swap_optimized) {
  if(mkdirs(GC_VALIDATION_DIR) != 0) return -1;
  char marker[1024];
  char legacy_marker[1024];
  marker_path_for_title(title_id, marker, sizeof(marker));
  struct stat st;
  if(stat(image_path, &st) != 0) return -1;
  uint64_t compressed_size = st.st_size > 0 ? (uint64_t)st.st_size : 0;
  uint64_t saved_bytes = 0;
  int marker_apr_indexed = apr_indexed ? 1 : 0;
  int marker_ampr_hot_swap_optimized = ampr_hot_swap_optimized ? 1 : 0;
  char *old_json = NULL;
  size_t old_json_size = 0;
  char old_path[1024] = {0};

  if(marker_path_for_title_source(title_id, image_path, marker,
                                  sizeof(marker)) != 0 ||
     read_file_limited(marker, &old_json, &old_json_size, 64 * 1024) != 0) {
    marker_path_for_title(title_id, marker, sizeof(marker));
    (void)read_file_limited(marker, &old_json, &old_json_size, 64 * 1024);
  }
  if(old_json) {
    (void)old_json_size;
    json_find_string_value(old_json, "path", old_path, sizeof(old_path));
    if(!strcmp(old_path, image_path)) {
      if(json_find_bool_value(old_json, "aprIndexed", 0)) {
        marker_apr_indexed = 1;
      }
      if(json_find_bool_value(old_json, "amprHotSwapOptimized", 0)) {
        marker_ampr_hot_swap_optimized = 1;
      }
      if(compression_source_size == 0) {
        compression_source_size =
            json_find_u64_value(old_json, "compressionSourceSize", 0);
      }
    }
    free(old_json);
  }

  if(compression_source_size > 0) {
    saved_bytes = compression_source_size > compressed_size
        ? compression_source_size - compressed_size
        : 0;
  } else {
    if(compression_source_size == 0) {
      compression_source_size =
          infer_compression_source_size_from_known_roots(title_id, image_path,
                                                        compressed_size);
    }
    if(compression_source_size > compressed_size) {
      saved_bytes = compression_source_size - compressed_size;
    }
  }

  json_buf_t b = {0};
  int ok = json_append(&b, "{\n  \"titleId\":") == 0 &&
      json_string(&b, title_id) == 0 &&
      json_append(&b, ",\n  \"path\":") == 0 &&
      json_string(&b, image_path) == 0 &&
      json_appendf(&b,
                   ",\n  \"sourceSize\":%llu,\n  \"sourceMtime\":%llu,"
                   "\n  \"compressionSourceSize\":%llu,"
                   "\n  \"compressionCompressedSize\":%llu,"
                   "\n  \"compressionSavedBytes\":%llu,"
                   "\n  \"aprIndexed\":%s,"
                   "\n  \"amprHotSwapOptimized\":%s,"
                   "\n  \"status\":",
                   (unsigned long long)(st.st_size > 0 ? st.st_size : 0),
                   (unsigned long long)st.st_mtime,
                   (unsigned long long)compression_source_size,
                   (unsigned long long)compressed_size,
                   (unsigned long long)saved_bytes,
                   marker_apr_indexed ? "true" : "false",
                   marker_ampr_hot_swap_optimized ? "true" : "false") == 0 &&
      json_string(&b, result ? result : "") == 0 &&
      json_appendf(&b,
                   ",\n  \"nestedName\":") == 0 &&
      json_string(&b, info && info->nested_name[0] ? info->nested_name : "") == 0 &&
      json_appendf(&b,
                   ",\n  \"blockCount\":%llu,"
                   "\n  \"logicalSize\":%llu,"
                   "\n  \"oldStoredSize\":%llu,"
                   "\n  \"newStoredSize\":%llu,"
                   "\n  \"repairedBlocks\":%llu,"
                   "\n  \"hashMode\":",
                   (unsigned long long)(info ? info->block_count : 0),
                   (unsigned long long)(info ? info->logical_size : 0),
                   (unsigned long long)(info ? info->old_stored_size : 0),
                   (unsigned long long)(info ? info->new_stored_size : 0),
                   (unsigned long long)(info ? info->repaired_blocks : 0)) == 0 &&
      json_string(&b, info && info->hash_mode[0] ? info->hash_mode : "") == 0 &&
      json_appendf(&b,
                   ",\n  \"hashCheckedBlocks\":%llu,"
                   "\n  \"hashMatchedBlocks\":%llu,"
                   "\n  \"hashMismatchedBlocks\":%llu,"
                   "\n  \"softwareComparedBlocks\":%llu,"
                   "\n  \"postVerifyBlocks\":%llu,"
                   "\n  \"postVerifyMountBlocks\":%llu,"
                   "\n  \"updatedAt\":%llu\n}\n",
                   (unsigned long long)(info ? info->hash_checked_blocks : 0),
                   (unsigned long long)(info ? info->hash_matched_blocks : 0),
                   0ULL,
                   (unsigned long long)(info ? info->software_compared_blocks : 0),
                   (unsigned long long)(info ? info->post_verify_blocks : 0),
                   (unsigned long long)(info ? info->post_verify_mount_blocks : 0),
                   (unsigned long long)time(NULL)) == 0;
  if(!ok) {
    free(b.data);
    return -1;
  }
  if(marker_path_for_title_source(title_id, image_path, marker,
                                  sizeof(marker)) != 0 ||
     write_validation_marker_file(marker, b.data, b.len) != 0) {
    free(b.data);
    return -1;
  }
  marker_path_for_title(title_id, legacy_marker, sizeof(legacy_marker));
  (void)write_validation_marker_file(legacy_marker, b.data, b.len);
  free(b.data);
  return 0;
}

static int
rewrite_moved_validation_marker(const char *title_id, const char *old_path,
                                const char *new_path) {
  char marker[1024];
  char *json = NULL;
  size_t json_size = 0;
  char marker_path[1024];
  struct stat st;
  uint64_t compression_source_size = 0;
  uint64_t compression_compressed_size = 0;
  uint64_t compression_saved_bytes = 0;

  if(marker_path_for_title_source(title_id, old_path, marker,
                                  sizeof(marker)) != 0 ||
     read_file_limited(marker, &json, &json_size, 64 * 1024) != 0) {
    marker_path_for_title(title_id, marker, sizeof(marker));
    if(read_file_limited(marker, &json, &json_size, 64 * 1024) != 0) {
      return 0;
    }
  }
  (void)json_size;
  marker_path[0] = 0;
  json_find_string_value(json, "path", marker_path, sizeof(marker_path));
  compression_source_size =
      json_find_u64_value(json, "compressionSourceSize", 0);
  compression_compressed_size =
      json_find_u64_value(json, "compressionCompressedSize", 0);
  compression_saved_bytes =
      json_find_u64_value(json, "compressionSavedBytes", 0);
  free(json);
  if(strcmp(marker_path, old_path) || stat(new_path, &st) != 0) return 0;
  if(compression_compressed_size == 0 && st.st_size > 0) {
    compression_compressed_size = (uint64_t)st.st_size;
  }
  if(compression_saved_bytes == 0 &&
     compression_source_size > compression_compressed_size) {
    compression_saved_bytes = compression_source_size - compression_compressed_size;
  }

  json_buf_t b = {0};
  int ok = json_append(&b, "{\n  \"titleId\":") == 0 &&
      json_string(&b, title_id) == 0 &&
      json_append(&b, ",\n  \"path\":") == 0 &&
      json_string(&b, new_path) == 0 &&
      json_appendf(&b,
                   ",\n  \"sourceSize\":%llu,\n  \"sourceMtime\":%llu,"
                   "\n  \"compressionSourceSize\":%llu,"
                   "\n  \"compressionCompressedSize\":%llu,"
                   "\n  \"compressionSavedBytes\":%llu,"
                   "\n  \"status\":\"moved\","
                   "\n  \"nestedName\":\"\","
                   "\n  \"blockCount\":0,"
                   "\n  \"logicalSize\":0,"
                   "\n  \"oldStoredSize\":0,"
                   "\n  \"newStoredSize\":0,"
                   "\n  \"repairedBlocks\":0,"
                   "\n  \"updatedAt\":%llu\n}\n",
                   (unsigned long long)(st.st_size > 0 ? st.st_size : 0),
                   (unsigned long long)st.st_mtime,
                   (unsigned long long)compression_source_size,
                   (unsigned long long)compression_compressed_size,
                   (unsigned long long)compression_saved_bytes,
                   (unsigned long long)time(NULL)) == 0;
  if(!ok) {
    free(b.data);
    return -1;
  }
  char old_marker[1024];
  if(marker_path_for_title_source(title_id, old_path, old_marker,
                                  sizeof(old_marker)) == 0) {
    (void)unlink(old_marker);
  }
  if(marker_path_for_title_source(title_id, new_path, marker,
                                  sizeof(marker)) != 0 ||
     write_validation_marker_file(marker, b.data, b.len) != 0) {
    free(b.data);
    return -1;
  }
  marker_path_for_title(title_id, marker, sizeof(marker));
  (void)write_validation_marker_file(marker, b.data, b.len);
  free(b.data);
  return 0;
}

static int
paths_equal_ignoring_trailing_slash(const char *a, const char *b) {
  size_t alen;
  size_t blen;

  if(!a || !b) return 0;
  alen = strlen(a);
  blen = strlen(b);
  while(alen > 1 && a[alen - 1] == '/') alen--;
  while(blen > 1 && b[blen - 1] == '/') blen--;
  return alen == blen && memcmp(a, b, alen) == 0;
}

static int
system_ex_title_bound_to(const char *title_id,
                         const char *expected_mount_source,
                         char *actual_type, size_t actual_type_size,
                         char *actual_source, size_t actual_source_size,
                         char *actual_mountpoint,
                         size_t actual_mountpoint_size) {
  char mountpoint[1024];
  char eboot[1024];
  struct stat st;
  struct statfs fs;
  int statfs_ok;

  if(actual_type && actual_type_size) actual_type[0] = 0;
  if(actual_source && actual_source_size) actual_source[0] = 0;
  if(actual_mountpoint && actual_mountpoint_size) actual_mountpoint[0] = 0;

  snprintf(mountpoint, sizeof(mountpoint), "%s/%s", GC_SYSTEM_APP_BASE,
           title_id ? title_id : "");
  snprintf(eboot, sizeof(eboot), "%s/eboot.bin", mountpoint);

  statfs_ok = statfs(mountpoint, &fs) == 0;
  if(statfs_ok) {
    if(actual_type && actual_type_size) {
      snprintf(actual_type, actual_type_size, "%s", fs.f_fstypename);
    }
    if(actual_source && actual_source_size) {
      snprintf(actual_source, actual_source_size, "%s", fs.f_mntfromname);
    }
    if(actual_mountpoint && actual_mountpoint_size) {
      snprintf(actual_mountpoint, actual_mountpoint_size, "%s",
               fs.f_mntonname);
    }
  } else {
    if(actual_type && actual_type_size) {
      snprintf(actual_type, actual_type_size, "%s", "statfs-failed");
    }
    if(actual_source && actual_source_size) {
      snprintf(actual_source, actual_source_size, "%s", strerror(errno));
    }
  }

  if(stat(eboot, &st) != 0 || !S_ISREG(st.st_mode)) return 0;
  if(!statfs_ok) return 0;
  if(strcmp(fs.f_fstypename, "nullfs") != 0) return 0;
  if(expected_mount_source && expected_mount_source[0] &&
     !paths_equal_ignoring_trailing_slash(fs.f_mntfromname,
                                          expected_mount_source)) {
    return 0;
  }
  return 1;
}

static int
wait_for_shadowmount_links(const char *title_id,
                           const char *expected_mount_link,
                           const char *expected_image_link,
                           char *err, size_t err_size) {
  time_t deadline = time(NULL) + GC_REMOUNT_WAIT_SECONDS;
  char mount_link[1024];
  char image_link[1024];
  char actual_type[64];
  char actual_source[1024];
  char actual_mountpoint[1024];
  char scan_err[256];
  time_t next_scan_at = 0;
  int stale_logged = 0;
  int restart_recovery_attempted = 0;

  if(err && err_size) err[0] = 0;
  gc_log("shadowmount wait title=%s mount=%s image=%s",
         title_id ? title_id : "",
         expected_mount_link ? expected_mount_link : "",
         expected_image_link ? expected_image_link : "");

  while(1) {
    if(gc_cancel_requested(err, err_size)) {
      gc_log("shadowmount wait cancelled title=%s",
             title_id ? title_id : "");
      return -1;
    }
    int has_mount = read_title_link(title_id, "mount.lnk", mount_link,
                                    sizeof(mount_link)) == 0;
    int has_image = read_title_link(title_id, "mount_img.lnk", image_link,
                                    sizeof(image_link)) == 0;
    int mount_ok = has_mount && expected_mount_link &&
        strcmp(mount_link, expected_mount_link) == 0;
    int image_ok = expected_image_link && expected_image_link[0]
        ? (has_image && strcmp(image_link, expected_image_link) == 0)
        : !has_image;
    int system_ex_ok = system_ex_title_bound_to(
        title_id, expected_mount_link, actual_type, sizeof(actual_type),
        actual_source, sizeof(actual_source), actual_mountpoint,
        sizeof(actual_mountpoint));

    if(mount_ok && image_ok && system_ex_ok) {
      gc_log("shadowmount ready title=%s mount=%s image=%s system_ex=%s:%s",
             title_id ? title_id : "", mount_link,
             has_image ? image_link : "",
             actual_type[0] ? actual_type : "(unknown)",
             actual_source[0] ? actual_source : "(unknown)");
      return 0;
    }

    if(mount_ok && image_ok && !system_ex_ok && !stale_logged) {
      gc_log("shadowmount system_ex not ready title=%s type=%s from=%s on=%s "
             "expected=%s",
             title_id ? title_id : "",
             actual_type[0] ? actual_type : "(unknown)",
             actual_source[0] ? actual_source : "(unknown)",
             actual_mountpoint[0] ? actual_mountpoint : "(unknown)",
             expected_mount_link ? expected_mount_link : "");
      stale_logged = 1;
    }

    time_t now = time(NULL);
    if(gc_cancel_requested(err, err_size)) {
      gc_log("shadowmount wait cancelled title=%s",
             title_id ? title_id : "");
      return -1;
    }
    if(now >= next_scan_at && now + GC_REMOUNT_WAIT_STEP_SECONDS < deadline) {
      scan_err[0] = 0;
      if(gc_shadowmount_request_scan_cancelable(scan_err,
                                                sizeof(scan_err)) == 0) {
        next_scan_at = now + GC_MOUNT_SCAN_REQUEST_SECONDS;
      } else if(job_cancelled()) {
        snprintf(err, err_size, "%s", "cancelled");
        gc_log("shadowmount wait cancelled title=%s",
               title_id ? title_id : "");
        return -1;
      } else {
        gc_log("shadowmount scan request failed title=%s err=%s",
               title_id ? title_id : "", scan_err[0] ? scan_err : "unknown");
        next_scan_at = now + GC_MOUNT_SCAN_REQUEST_SECONDS;
      }
    }

    if(gc_cancel_requested(err, err_size)) {
      gc_log("shadowmount wait cancelled title=%s",
             title_id ? title_id : "");
      return -1;
    }
    if(now >= deadline) {
      if(!restart_recovery_attempted) {
        char restart_detail[512] = {0};
        restart_recovery_attempted = 1;
        job_set_phase("mounting", 0, 0, "Restarting ShadowMountPlus");
        if(gc_shadowmount_restart_running(restart_detail,
                                          sizeof(restart_detail)) == 0) {
          gc_log("shadowmount restart recovery title=%s detail=%s",
                 title_id ? title_id : "",
                 restart_detail[0] ? restart_detail : "started");
          if(gc_sleep_cancelable_seconds(GC_SHADOWMOUNT_RESTART_WAIT_SECONDS,
                                         err, err_size) != 0) {
            gc_log("shadowmount restart wait cancelled title=%s",
                   title_id ? title_id : "");
            return -1;
          }
          scan_err[0] = 0;
          if(gc_shadowmount_request_scan_cancelable(scan_err,
                                                    sizeof(scan_err)) != 0) {
            if(job_cancelled()) return -1;
            gc_log("shadowmount post-restart scan failed title=%s err=%s",
                   title_id ? title_id : "",
                   scan_err[0] ? scan_err : "unknown");
          }
          deadline = time(NULL) + GC_REMOUNT_WAIT_SECONDS;
          next_scan_at = 0;
          stale_logged = 0;
          job_set_phase("mounting", 0, 0,
                        "Waiting for ShadowMountPlus after restart");
          continue;
        }
        gc_log("shadowmount restart recovery unavailable title=%s detail=%s",
               title_id ? title_id : "",
               restart_detail[0] ? restart_detail : "unknown");
      }
      snprintf(err, err_size,
               "ShadowMountPlus did not remount %s; mount.lnk=%s%s%s "
               "mount_img.lnk=%s%s%s system_ex=%s:%s%s%s",
               title_id ? title_id : "",
               has_mount ? mount_link : "(missing)",
               expected_mount_link ? " expected=" : "",
               expected_mount_link ? expected_mount_link : "",
               has_image ? image_link : "(missing)",
               expected_image_link && expected_image_link[0] ? " expected=" : "",
               expected_image_link && expected_image_link[0]
                   ? expected_image_link
                   : "(absent)",
               actual_type[0] ? actual_type : "(unknown)",
               actual_source[0] ? actual_source : "(unknown)",
               expected_mount_link ? " expectedFrom=" : "",
               expected_mount_link ? expected_mount_link : "");
      gc_log("shadowmount wait failed title=%s err=%s",
             title_id ? title_id : "", err && err[0] ? err : "unknown");
      return -1;
    }
    if(gc_sleep_cancelable_seconds(GC_REMOUNT_WAIT_STEP_SECONDS,
                                   err, err_size) != 0) {
      gc_log("shadowmount wait sleep cancelled title=%s",
             title_id ? title_id : "");
      return -1;
    }
  }
}

static int
get_running_big_app(char *title_id, size_t title_id_size, int *app_id_out) {
  int app_id;
  char tid[64] = {0};
  int rc;

  app_id = sceSystemServiceGetAppIdOfRunningBigApp();
  if(app_id < 0) return 0;

  rc = sceSystemServiceGetAppTitleId(app_id, tid);
  if(rc != 0 || !valid_title_id(tid)) {
    gc_log("running app title lookup failed appId=0x%08X rc=0x%08X tid=%s",
           (unsigned)app_id, (unsigned)rc, tid[0] ? tid : "(empty)");
    return -1;
  }

  if(title_id && title_id_size) {
    snprintf(title_id, title_id_size, "%s", tid);
  }
  if(app_id_out) *app_id_out = app_id;
  return 1;
}

static int
close_title_if_running(const char *title_id, char *err, size_t err_size) {
  char running_title[64] = {0};
  int app_id = 0;
  int state;
  uint32_t kill_rc = 0;

  if(err && err_size) err[0] = 0;
  if(!valid_title_id(title_id)) return 0;

  state = get_running_big_app(running_title, sizeof(running_title), &app_id);
  if(state < 0) {
    snprintf(err, err_size, "%s",
             "could not identify the running game before operation");
    return -1;
  }
  if(state == 0) return 0;
  if(strcmp(running_title, title_id)) return 0;

  gc_checkpoint("close running game");
  gc_log("closing running game title=%s appId=0x%08X", title_id,
         (unsigned)app_id);
  job_set_phase("closing", 0, 0, "Closing game");

  kill_rc = sceLncUtilKillApp((uint32_t)app_id);
  for(int i = 0; i < GC_CLOSE_WAIT_SECONDS; i++) {
    if(job_cancelled()) {
      snprintf(err, err_size, "%s", "cancelled while closing game");
      return -1;
    }
    if(gc_sleep_cancelable_seconds(1, err, err_size) != 0) return -1;
    running_title[0] = 0;
    state = get_running_big_app(running_title, sizeof(running_title),
                                &app_id);
    if(state == 0 || (state > 0 && strcmp(running_title, title_id))) {
      gc_log("closed running game title=%s killRc=0x%08X", title_id,
             (unsigned)kill_rc);
      return 0;
    }
    job_set_phase("closing", 0, 0, "Closing game");
  }

  snprintf(err, err_size,
           "could not close %s before operation; kill rc=0x%08X",
           title_id, (unsigned)kill_rc);
  gc_log("close running game failed title=%s appId=0x%08X killRc=0x%08X",
         title_id, (unsigned)app_id, (unsigned)kill_rc);
  return -1;
}

static void
detect_game_source_ex(gc_game_t *g, int exact_folder_size, int honor_cancel) {
  pfs_app_info_t app_info = {0};
  pfs_decompress_info_t dec_info = {0};
  char err[256] = {0};
  char outer_pfsc[1024] = {0};
  struct stat st;

  if(g->image_path[0]) {
    snprintf(g->source_path, sizeof(g->source_path), "%s", g->image_path);
  } else {
    snprintf(g->source_path, sizeof(g->source_path), "%s", g->mount_path);
  }

  uint32_t shadow_hash = 0;
  if(shadow_pfsc_hash_from_path(g->source_path, g->title_id,
                                &shadow_hash) == 0 &&
     find_outer_pfsc_by_shadow_hash(g->title_id, shadow_hash, outer_pfsc,
                                    sizeof(outer_pfsc)) == 0 &&
     pfs_decompress_probe(outer_pfsc, &dec_info, err, sizeof(err)) == 0) {
    g->source_kind = GC_SOURCE_COMPRESSED;
    snprintf(g->source_path, sizeof(g->source_path), "%s", outer_pfsc);
    snprintf(g->output_path, sizeof(g->output_path), "%s",
             dec_info.output_path);
    snprintf(g->nested_name, sizeof(g->nested_name), "%s",
             dec_info.nested_name);
    g->nested_type = dec_info.nested_type;
    g->output_exists = dec_info.output_exists;
  } else if(ends_with_ci(g->source_path, ".ffpfsc") &&
     pfs_decompress_probe(g->source_path, &dec_info, err,
                          sizeof(err)) == 0) {
    g->source_kind = GC_SOURCE_COMPRESSED;
    snprintf(g->output_path, sizeof(g->output_path), "%s",
             dec_info.output_path);
    snprintf(g->nested_name, sizeof(g->nested_name), "%s",
             dec_info.nested_name);
    g->nested_type = dec_info.nested_type;
    g->output_exists = dec_info.output_exists;
  } else if(stat(g->source_path, &st) == 0 && S_ISREG(st.st_mode) &&
            pfs_image_probe(g->source_path, &app_info, err,
                            sizeof(err)) == 0) {
    g->source_kind = GC_SOURCE_IMAGE;
    snprintf(g->output_path, sizeof(g->output_path), "%s",
             app_info.output_path);
    snprintf(g->nested_name, sizeof(g->nested_name), "%s",
             app_info.nested_name);
    g->nested_type = app_info.nested_type;
    g->output_exists = app_info.output_exists;
  } else if(stat(g->source_path, &st) == 0 && S_ISDIR(st.st_mode) &&
            pfs_app_probe(g->source_path, &app_info, err,
                          sizeof(err)) == 0) {
    g->source_kind = GC_SOURCE_FOLDER;
    snprintf(g->output_path, sizeof(g->output_path), "%s",
             app_info.output_path);
    g->nested_type = PFS_NESTED_EXFAT;
    g->output_exists = app_info.output_exists;
  } else {
    g->source_kind = GC_SOURCE_UNKNOWN;
  }

  populate_game_size_ex(g, exact_folder_size, honor_cancel);
  g->can_stream_delete =
      g->source_kind == GC_SOURCE_FOLDER ||
      g->source_kind == GC_SOURCE_COMPRESSED;
  if(ampr_folder_target_probe(g->source_path, g->ampr_path,
                              sizeof(g->ampr_path), g->ampr_sha256) ||
     (g->is_mounted &&
      ampr_folder_target_probe(g->mount_path, g->ampr_path,
                               sizeof(g->ampr_path), g->ampr_sha256))) {
    g->ampr_present = 1;
  } else {
    g->ampr_present = 0;
    g->ampr_path[0] = 0;
    g->ampr_sha256[0] = 0;
  }
  g->ampr_version[0] = 0;
  int ampr_have_latest =
      ampr_latest_cached(g->ampr_latest_version,
                         sizeof(g->ampr_latest_version),
                         g->ampr_latest_sha256);
  int ampr_current_cached = 0;
  if(g->ampr_present && sha256_hex_valid(g->ampr_sha256)) {
    if(ampr_have_latest &&
       strcasecmp(g->ampr_sha256, g->ampr_latest_sha256) == 0) {
      snprintf(g->ampr_version, sizeof(g->ampr_version), "%s",
               g->ampr_latest_version);
      ampr_current_cached = 1;
    } else {
      ampr_current_cached =
          ampr_cached_version_for_sha(g->ampr_sha256, g->ampr_version,
                                      sizeof(g->ampr_version));
    }
    if(!g->ampr_version[0]) {
      char selected_intent[16] = {0};
      char selected_version[64] = {0};
      char selected_sha[65] = {0};
      if(ampr_selection_read(g->title_id, g->source_path,
                             selected_intent, sizeof(selected_intent),
                             selected_version, sizeof(selected_version),
                             selected_sha) &&
         strcasecmp(selected_sha, g->ampr_sha256) == 0 &&
         ampr_version_safe(selected_version)) {
        snprintf(g->ampr_version, sizeof(g->ampr_version), "%s",
                 selected_version);
      }
    }
  }
  g->ampr_original_available =
      ampr_original_lookup(g->title_id, g->source_path, NULL,
                           g->ampr_original_sha256,
                           &g->ampr_original_size);
  g->ampr_update_needed =
      ampr_latest_update_needed_for_game(g, ampr_have_latest,
                                         ampr_current_cached);
  g->ampr_update_supported = g->ampr_present;
  load_validation_state(g);
  populate_apr_index_state_from_roots(g);

  if(g->source_kind == GC_SOURCE_COMPRESSED) {
    snprintf(g->primary_action, sizeof(g->primary_action), "%s",
             g->validation == GC_VALIDATION_VALIDATED
                 ? "Revalidate and Repair"
                 : "Validate and Repair");
  } else if(g->source_kind == GC_SOURCE_FOLDER ||
            g->source_kind == GC_SOURCE_IMAGE) {
    snprintf(g->primary_action, sizeof(g->primary_action), "%s", "Compress");
  } else {
    snprintf(g->primary_action, sizeof(g->primary_action), "%s",
             "Unavailable");
  }
}

static void
detect_game_source(gc_game_t *g, int exact_folder_size) {
  detect_game_source_ex(g, exact_folder_size, 0);
}

typedef struct gc_scan_roots {
  char roots[GC_MAX_SCAN_ROOTS][1024];
  size_t count;
} gc_scan_roots_t;

typedef struct gc_hidden_instance {
  char original_path[1024];
  char hidden_path[1024];
  gc_source_kind_t source_kind;
  int hidden;
} gc_hidden_instance_t;

typedef struct gc_mount_link_backup {
  char mount_link_path[1024];
  char image_link_path[1024];
  char mount_value[1024];
  char image_value[1024];
  int had_mount;
  int had_image;
  int cleared;
} gc_mount_link_backup_t;

static int discover_games(gc_game_t *games, size_t max_games,
                          size_t *count_out, int exact_folder_sizes);

static int
game_source_matches(const gc_game_t *g, const char *source_path) {
  return g && source_path && source_path[0] && g->source_path[0] &&
      paths_equal_ignoring_trailing_slash(g->source_path, source_path);
}

static int
compressed_source_shadow_hash(const gc_game_t *g, uint32_t *hash_out) {
  if(!g || !hash_out || g->source_kind != GC_SOURCE_COMPRESSED ||
     !ends_with_ci(g->source_path, ".ffpfsc")) {
    return -1;
  }
  *hash_out = fnv1a32_string(g->source_path);
  return 0;
}

static int
mounted_shadow_pfsc_hash(const gc_game_t *g, uint32_t *hash_out) {
  if(!g || !hash_out || !valid_title_id(g->title_id)) return -1;
  return shadow_pfsc_hash_from_path(g->source_path, g->title_id, hash_out);
}

static int
games_reference_same_shadow_pfsc(const gc_game_t *a, const gc_game_t *b) {
  uint32_t a_hash = 0;
  uint32_t b_hash = 0;
  if(!a || !b || !valid_title_id(a->title_id) ||
     strcmp(a->title_id, b->title_id)) {
    return 0;
  }
  if(compressed_source_shadow_hash(a, &a_hash) == 0 &&
     mounted_shadow_pfsc_hash(b, &b_hash) == 0 &&
     a_hash == b_hash) {
    return 1;
  }
  if(compressed_source_shadow_hash(b, &b_hash) == 0 &&
     mounted_shadow_pfsc_hash(a, &a_hash) == 0 &&
     a_hash == b_hash) {
    return 1;
  }
  return 0;
}

static void
merge_mount_state(gc_game_t *dst, const gc_game_t *src) {
  if(!dst || !src) return;
  if(src->is_mounted) {
    dst->is_mounted = 1;
    snprintf(dst->mount_status, sizeof(dst->mount_status), "%s",
             src->mount_status[0] ? src->mount_status : "mounted");
  }
  if(!dst->mount_path[0] && src->mount_path[0]) {
    snprintf(dst->mount_path, sizeof(dst->mount_path), "%s", src->mount_path);
  }
  if(!dst->image_path[0] && src->image_path[0]) {
    snprintf(dst->image_path, sizeof(dst->image_path), "%s", src->image_path);
  }
}

static int
candidate_preferred_for_existing_instance(const gc_game_t *existing,
                                          const gc_game_t *candidate) {
  int same_shadow_pfsc;
  if(!existing || !candidate) return 0;
  same_shadow_pfsc = games_reference_same_shadow_pfsc(existing, candidate);
  if(same_shadow_pfsc) {
    return existing->source_kind != GC_SOURCE_COMPRESSED &&
        candidate->source_kind == GC_SOURCE_COMPRESSED;
  }
  return candidate->is_mounted && !existing->is_mounted;
}

static int
game_list_find_instance(const gc_game_t *games, size_t count,
                        const gc_game_t *candidate) {
  if(!games || !candidate || !candidate->source_path[0]) return -1;
  for(size_t i = 0; i < count; i++) {
    if((games[i].source_kind == candidate->source_kind &&
        game_source_matches(&games[i], candidate->source_path)) ||
       games_reference_same_shadow_pfsc(&games[i], candidate)) {
      return (int)i;
    }
  }
  return -1;
}

static int
lookup_game_by_source_path(const char *source_path, gc_game_t *out) {
  gc_game_t *games;
  size_t count = 0;

  if(!source_path || !source_path[0] || !out) return -1;
  games = calloc(GC_MAX_GAMES, sizeof(*games));
  if(!games) return -1;
  if(discover_games(games, GC_MAX_GAMES, &count, 0) != 0) {
    free(games);
    return -1;
  }
  for(size_t i = 0; i < count; i++) {
    if(game_source_matches(&games[i], source_path)) {
      *out = games[i];
      free(games);
      return 0;
    }
  }
  free(games);
  return -1;
}

static int
game_source_still_exists(const gc_game_t *g) {
  return g && path_exists_for_source_kind(g->source_path, g->source_kind);
}

static int
append_game_unique_by_source(gc_game_t *games, size_t max_games,
                             size_t *count, const gc_game_t *candidate) {
  int existing;
  if(!games || !count || !candidate || *count >= max_games ||
     candidate->source_kind == GC_SOURCE_UNKNOWN ||
     !candidate->source_path[0]) {
    return 0;
  }
  existing = game_list_find_instance(games, *count, candidate);
  if(existing >= 0) {
    if(candidate_preferred_for_existing_instance(&games[existing],
                                                 candidate)) {
      gc_game_t merged = *candidate;
      merge_mount_state(&merged, &games[existing]);
      games[existing] = merged;
    } else {
      merge_mount_state(&games[existing], candidate);
    }
    return 0;
  }
  games[(*count)++] = *candidate;
  return 1;
}

static void
scan_roots_add(gc_scan_roots_t *roots, const char *path) {
  struct stat st;
  if(!roots || !path || !path[0] || roots->count >= GC_MAX_SCAN_ROOTS ||
     !path_is_safe(path) || path_is_system_app_path(path) ||
     path_has_unmountable_game_folder_segment(path) ||
     stat(path, &st) != 0 || !S_ISDIR(st.st_mode)) {
    return;
  }
  for(size_t i = 0; i < roots->count; i++) {
    if(paths_equal_ignoring_trailing_slash(roots->roots[i], path)) return;
  }
  snprintf(roots->roots[roots->count++], sizeof(roots->roots[0]), "%s", path);
}

static void
scan_roots_add_parent(gc_scan_roots_t *roots, const char *path) {
  char parent[1024];
  if(!path || !path[0] || path_parent(path, parent, sizeof(parent)) != 0) {
    return;
  }
  scan_roots_add(roots, parent);
}

static void
scan_roots_build(const gc_game_t *mounted_games, size_t mounted_count,
                 gc_scan_roots_t *roots) {
  gc_usb_target_t usb[GC_STORAGE_TARGET_COUNT];
  size_t usb_count = 0;
  gc_source_roots_t source_roots;
  memset(roots, 0, sizeof(*roots));

  shadow_source_roots_build(&source_roots);
  for(size_t i = 0; i < source_roots.count; i++) {
    scan_roots_add(roots, source_roots.roots[i]);
  }
  scan_roots_add(roots, GC_INTERNAL_GAME_ROOT);
  discover_usb_targets(usb, GC_STORAGE_TARGET_COUNT, &usb_count);
  for(size_t i = 0; i < usb_count; i++) {
    scan_roots_add(roots, usb[i].root);
    scan_roots_add(roots, usb[i].target_root);
  }
  for(size_t i = 0; i < mounted_count; i++) {
    scan_roots_add_parent(roots, mounted_games[i].source_path);
    scan_roots_add_parent(roots, mounted_games[i].output_path);
  }
}

static int
candidate_game_from_path(const char *source_path, const char *name,
                         gc_game_t *out, int exact_folder_size,
                         int is_mounted) {
  struct stat st;
  char title_id[64] = {0};
  int expect_kind = GC_SOURCE_UNKNOWN;
  pfs_app_info_t app_info = {0};
  char err[256] = {0};

  if(!source_path || !name || !out) return -1;
  if(path_is_system_app_path(source_path)) return -1;
  if(path_has_unmountable_game_folder_segment(source_path)) return -1;
  if(lstat(source_path, &st) != 0) return -1;
  if(name[0] == '.') return -1;
  if(unmountable_game_folder_segment(name)) return -1;
  if(strstr(name, GC_FORCE_REMOUNT_PREFIX)) return -1;

  if(S_ISREG(st.st_mode) && ends_with_ci(name, ".ffpfsc")) {
    if(strip_extension_base(name, title_id, sizeof(title_id)) != 0 ||
       !valid_title_id(title_id)) {
      return -1;
    }
    expect_kind = GC_SOURCE_COMPRESSED;
  } else if(S_ISDIR(st.st_mode) && valid_title_id(name)) {
    snprintf(title_id, sizeof(title_id), "%s", name);
    expect_kind = GC_SOURCE_FOLDER;
  } else if(S_ISDIR(st.st_mode) && strlen(name) == 13 &&
            !strcasecmp(name + 9, "-app")) {
    memcpy(title_id, name, 9);
    title_id[9] = 0;
    if(!valid_title_id(title_id)) return -1;
    expect_kind = GC_SOURCE_FOLDER;
  } else if(S_ISDIR(st.st_mode) &&
            pfs_app_probe(source_path, &app_info, err, sizeof(err)) == 0 &&
            valid_title_id(app_info.title_id)) {
    snprintf(title_id, sizeof(title_id), "%s", app_info.title_id);
    expect_kind = GC_SOURCE_FOLDER;
  } else if(S_ISREG(st.st_mode) &&
            pfs_image_probe(source_path, &app_info, err, sizeof(err)) == 0 &&
            valid_title_id(app_info.title_id)) {
    snprintf(title_id, sizeof(title_id), "%s", app_info.title_id);
    expect_kind = GC_SOURCE_IMAGE;
  } else {
    return -1;
  }

  memset(out, 0, sizeof(*out));
  snprintf(out->title_id, sizeof(out->title_id), "%s", title_id);
  if(expect_kind == GC_SOURCE_COMPRESSED || expect_kind == GC_SOURCE_IMAGE) {
    snprintf(out->image_path, sizeof(out->image_path), "%s", source_path);
  } else {
    snprintf(out->mount_path, sizeof(out->mount_path), "%s", source_path);
  }
  set_game_mount_status(out, is_mounted,
                        is_mounted ? "mounted" : "not-mounted");
  load_game_name(out);
  load_game_icon(out);
  detect_game_source(out, exact_folder_size);
  if(game_uses_system_app_path(out)) return -1;
  if(out->source_kind == GC_SOURCE_UNKNOWN) return -1;
  if(expect_kind != GC_SOURCE_UNKNOWN && out->source_kind != expect_kind) {
    return -1;
  }
  if(!game_source_still_exists(out)) return -1;
  return 0;
}

static void
scan_artifacts_uncached(const gc_game_t *mounted_games, size_t mounted_count,
                        gc_game_t *games, size_t max_games,
                        size_t *count_out) {
  gc_scan_roots_t roots;
  size_t count = 0;
  scan_roots_build(mounted_games, mounted_count, &roots);
  for(size_t i = 0; i < roots.count && count < max_games; i++) {
    DIR *root = opendir(roots.roots[i]);
    struct dirent *ent;
    if(!root) continue;
    while((ent = readdir(root)) && count < max_games) {
      char source_path[1024];
      gc_game_t candidate;
      if(!strcmp(ent->d_name, ".") || !strcmp(ent->d_name, "..")) continue;
      if(!upload_segment_safe(ent->d_name)) continue;
      if(unmountable_game_folder_segment(ent->d_name)) continue;
      if(snprintf(source_path, sizeof(source_path), "%s/%s",
                  roots.roots[i], ent->d_name) >= (int)sizeof(source_path)) {
        continue;
      }
      if(candidate_game_from_path(source_path, ent->d_name, &candidate,
                                  0, 0) != 0) {
        continue;
      }
      append_game_unique_by_source(games, max_games, &count, &candidate);
    }
    closedir(root);
  }
  if(count_out) *count_out = count;
}

static void
artifact_cache_invalidate(void) {
  pthread_mutex_lock(&g_artifact_cache_lock);
  g_artifact_cache_force = 1;
  g_artifact_cache_scanned_at = 0;
  pthread_mutex_unlock(&g_artifact_cache_lock);
}

static void
artifact_cache_get(const gc_game_t *mounted_games, size_t mounted_count,
                   gc_game_t *games, size_t max_games, size_t *count_out) {
  time_t now = time(NULL);
  gc_game_t *local;
  size_t local_count = 0;
  pthread_mutex_lock(&g_artifact_cache_lock);
  if(!g_artifact_cache_force &&
     g_artifact_cache_scanned_at > 0 &&
     now - g_artifact_cache_scanned_at < GC_ARTIFACT_SCAN_TTL_SECONDS) {
    size_t copy_count = g_artifact_cache_count < max_games
        ? g_artifact_cache_count : max_games;
    memcpy(games, g_artifact_cache, copy_count * sizeof(games[0]));
    if(count_out) *count_out = copy_count;
    pthread_mutex_unlock(&g_artifact_cache_lock);
    return;
  }
  pthread_mutex_unlock(&g_artifact_cache_lock);

  local = calloc(GC_MAX_GAMES, sizeof(*local));
  if(!local) {
    if(count_out) *count_out = 0;
    return;
  }
  scan_artifacts_uncached(mounted_games, mounted_count, local, GC_MAX_GAMES,
                          &local_count);

  pthread_mutex_lock(&g_artifact_cache_lock);
  memcpy(g_artifact_cache, local, local_count * sizeof(local[0]));
  g_artifact_cache_count = local_count;
  g_artifact_cache_scanned_at = time(NULL);
  g_artifact_cache_force = 0;
  size_t copy_count = local_count < max_games ? local_count : max_games;
  memcpy(games, g_artifact_cache, copy_count * sizeof(games[0]));
  if(count_out) *count_out = copy_count;
  pthread_mutex_unlock(&g_artifact_cache_lock);
  free(local);
}

static int
discover_games_ex(gc_game_t *games, size_t max_games, size_t *count_out,
                  int exact_folder_sizes, int honor_cancel) {
  DIR *d = opendir(GC_APP_BASE);
  size_t count = 0;
  struct dirent *ent;
  gc_game_t *artifacts = calloc(GC_MAX_GAMES, sizeof(*artifacts));
  size_t artifact_count = 0;
  if(!artifacts) return -1;

  if(d) {
    while(count < max_games) {
      if(honor_cancel && job_cancelled()) break;
      ent = readdir(d);
      if(!ent) break;
      if(!valid_title_id(ent->d_name)) continue;
      gc_game_t *g = &games[count];
      memset(g, 0, sizeof(*g));
      snprintf(g->title_id, sizeof(g->title_id), "%s", ent->d_name);

      char link_path[1024];
      int has_mount_link;
      snprintf(link_path, sizeof(link_path), "%s/%s/mount.lnk", GC_APP_BASE,
               g->title_id);
      has_mount_link =
          read_link_file(link_path, g->mount_path, sizeof(g->mount_path)) == 0;
      if(!has_mount_link) {
        if(find_exact_pfsc_by_title(g->title_id, g->image_path,
                                    sizeof(g->image_path)) != 0) {
          continue;
        }
        snprintf(g->mount_path, sizeof(g->mount_path), "%s", g->image_path);
      } else {
        snprintf(link_path, sizeof(link_path), "%s/%s/mount_img.lnk",
                 GC_APP_BASE, g->title_id);
        (void)read_link_file(link_path, g->image_path, sizeof(g->image_path));
      }
      if(game_uses_system_app_path(g)) continue;
      set_game_mount_status(g, has_mount_link,
                            has_mount_link ? "mounted" : "not-mounted");
      load_game_name(g);
      load_game_icon(g);
      detect_game_source_ex(g, exact_folder_sizes, honor_cancel);
      if(!game_uses_system_app_path(g) &&
         g->source_kind != GC_SOURCE_UNKNOWN && game_source_still_exists(g)) {
        count++;
      }
    }
    closedir(d);
  }

  artifact_cache_get(games, count, artifacts, GC_MAX_GAMES, &artifact_count);
  for(size_t i = 0; i < artifact_count && count < max_games; i++) {
    append_game_unique_by_source(games, max_games, &count, &artifacts[i]);
  }
  free(artifacts);

  if(count_out) *count_out = count;
  return 0;
}

static int
discover_games(gc_game_t *games, size_t max_games, size_t *count_out,
               int exact_folder_sizes) {
  return discover_games_ex(games, max_games, count_out, exact_folder_sizes, 0);
}

static int
preferred_transfer_root_score(const char *parent, const char *storage_root) {
  if(!parent || !parent[0] || !storage_root || !storage_root[0]) return 1000;
  if(!path_is_shadowmount_game_root(parent)) return 1000;
  if(ends_with_ci(parent, "/homebrew")) return 0;
  if(ends_with_ci(parent, "/etaHEN/games")) return 10;
  if(paths_equal_ignoring_trailing_slash(parent, storage_root)) return 30;
  return 1000;
}

static int
resolve_transfer_target_root(const char *storage_root,
                             const char *fallback_root,
                             char *out,
                             size_t out_size) {
  gc_game_t *games;
  size_t count = 0;
  int best_score = 1000;
  char best[1024] = {0};

  if(!storage_root || !storage_root[0] || !fallback_root ||
     !fallback_root[0] || !out || out_size == 0) {
    errno = EINVAL;
    return -1;
  }

  games = calloc(GC_MAX_GAMES, sizeof(*games));
  if(games) {
    if(discover_games(games, GC_MAX_GAMES, &count, 0) == 0) {
      for(size_t i = 0; i < count; i++) {
        char parent[1024];
        int score;
        if(!games[i].source_path[0] ||
           !path_under_root(games[i].source_path, storage_root) ||
           path_parent(games[i].source_path, parent, sizeof(parent)) != 0 ||
           !path_is_safe(parent) ||
           !path_under_root(parent, storage_root) ||
           !path_is_shadowmount_game_root(parent)) {
          continue;
        }
        score = preferred_transfer_root_score(parent, storage_root);
        if(score >= 1000) continue;
        if(score < best_score) {
          best_score = score;
          snprintf(best, sizeof(best), "%s", parent);
          if(score == 0) break;
        }
      }
    }
    free(games);
  }

  if(!best[0]) snprintf(best, sizeof(best), "%s", fallback_root);
  if(snprintf(out, out_size, "%s", best) >= (int)out_size) {
    errno = ENAMETOOLONG;
    return -1;
  }
  return 0;
}

static int
find_game_for_operation_source_path(const gc_operation_t *op, gc_game_t *out,
                                    int exact_folder_sizes) {
  gc_game_t candidate;
  pfs_decompress_info_t dec = {0};
  pfs_app_info_t app = {0};
  struct stat st;
  char title_id[64] = {0};
  char err[256] = {0};
  const char *name;

  if(!op) return -1;
  if(!op->source_path[0]) {
    errno = EINVAL;
    return -1;
  }

  name = base_name(op->source_path);
  if(!name || !name[0]) return -1;
  if(lstat(op->source_path, &st) != 0) return -1;

  memset(&candidate, 0, sizeof(candidate));
  snprintf(candidate.title_id, sizeof(candidate.title_id), "%s",
           op->title_id);
  snprintf(candidate.name, sizeof(candidate.name), "%s",
           op->display_name[0] ? op->display_name : op->title_id);
  snprintf(candidate.source_path, sizeof(candidate.source_path), "%s",
           op->source_path);
  set_game_mount_status(&candidate, 0, "not-mounted");

  if(S_ISREG(st.st_mode) && ends_with_ci(name, ".ffpfsc")) {
    if(strip_extension_base(name, title_id, sizeof(title_id)) != 0 ||
       strcmp(title_id, op->title_id) ||
       pfs_decompress_detect_nested(op->source_path, &dec,
                                    err, sizeof(err)) != 0) {
      return -1;
    }
    candidate.source_kind = GC_SOURCE_COMPRESSED;
    snprintf(candidate.image_path, sizeof(candidate.image_path), "%s",
             dec.source_path);
    snprintf(candidate.source_path, sizeof(candidate.source_path), "%s",
             dec.source_path);
    snprintf(candidate.output_path, sizeof(candidate.output_path), "%s",
             dec.output_path);
    snprintf(candidate.nested_name, sizeof(candidate.nested_name), "%s",
             dec.nested_name);
    candidate.nested_type = dec.nested_type;
    candidate.output_exists = dec.output_exists;
    candidate.source_size = st.st_size > 0 ? (uint64_t)st.st_size : 0;
    candidate.required_bytes = candidate.source_size;
  } else if(S_ISREG(st.st_mode) &&
            pfs_image_probe(op->source_path, &app, err, sizeof(err)) == 0) {
    if(strcmp(app.title_id, op->title_id)) return -1;
    candidate.source_kind = GC_SOURCE_IMAGE;
    snprintf(candidate.image_path, sizeof(candidate.image_path), "%s",
             op->source_path);
    snprintf(candidate.output_path, sizeof(candidate.output_path), "%s",
             app.output_path);
    snprintf(candidate.nested_name, sizeof(candidate.nested_name), "%s",
             app.nested_name);
    candidate.nested_type = app.nested_type;
    candidate.output_exists = app.output_exists;
    candidate.source_size = st.st_size > 0 ? (uint64_t)st.st_size : 0;
    candidate.required_bytes = candidate.source_size;
  } else if(S_ISDIR(st.st_mode) &&
            pfs_app_probe(op->source_path, &app, err, sizeof(err)) == 0) {
    if(strcmp(app.title_id, op->title_id)) return -1;
    candidate.source_kind = GC_SOURCE_FOLDER;
    snprintf(candidate.mount_path, sizeof(candidate.mount_path), "%s",
             op->source_path);
    snprintf(candidate.output_path, sizeof(candidate.output_path), "%s",
             app.output_path);
    candidate.nested_type = PFS_NESTED_EXFAT;
    candidate.output_exists = app.output_exists;
    candidate.size_pending = exact_folder_sizes ? 0 : 1;
    if(exact_folder_sizes) {
      int cancelled = 0;
      candidate.source_size =
          source_size_bytes_exact_ex(op->source_path, candidate.source_kind,
                                     1, &cancelled);
      if(cancelled) return -1;
      candidate.required_bytes = candidate.source_size;
    }
  } else {
    return -1;
  }

  if(game_uses_system_app_path(&candidate) ||
     !game_source_matches(&candidate, op->source_path) ||
     !game_source_still_exists(&candidate)) {
    return -1;
  }
  {
    gc_game_t mounted;
    char link_path[1024];
    memset(&mounted, 0, sizeof(mounted));
    snprintf(mounted.title_id, sizeof(mounted.title_id), "%s", op->title_id);
    snprintf(link_path, sizeof(link_path), "%s/%s/mount.lnk", GC_APP_BASE,
             op->title_id);
    if(read_link_file(link_path, mounted.mount_path,
                      sizeof(mounted.mount_path)) == 0) {
      snprintf(link_path, sizeof(link_path), "%s/%s/mount_img.lnk",
               GC_APP_BASE, op->title_id);
      (void)read_link_file(link_path, mounted.image_path,
                           sizeof(mounted.image_path));
      set_game_mount_status(&mounted, 1, "mounted");
      detect_game_source_ex(&mounted, 0, 0);
      if(game_source_matches(&mounted, candidate.source_path)) {
        candidate.is_mounted = 1;
        snprintf(candidate.mount_status, sizeof(candidate.mount_status), "%s",
                 "mounted");
        snprintf(candidate.mount_path, sizeof(candidate.mount_path), "%s",
                 mounted.mount_path);
        if(mounted.image_path[0]) {
          snprintf(candidate.image_path, sizeof(candidate.image_path), "%s",
                   mounted.image_path);
        }
        if(mounted.ampr_present) {
          candidate.ampr_present = 1;
          snprintf(candidate.ampr_path, sizeof(candidate.ampr_path), "%s",
                   mounted.ampr_path);
          snprintf(candidate.ampr_sha256, sizeof(candidate.ampr_sha256), "%s",
                   mounted.ampr_sha256);
          snprintf(candidate.ampr_version, sizeof(candidate.ampr_version), "%s",
                   mounted.ampr_version);
        }
      }
    }
  }
  candidate.can_stream_delete =
      candidate.source_kind == GC_SOURCE_FOLDER ||
      candidate.source_kind == GC_SOURCE_COMPRESSED;
  if(candidate.source_kind == GC_SOURCE_COMPRESSED) {
    snprintf(candidate.primary_action, sizeof(candidate.primary_action), "%s",
             "Validate and Repair");
  } else {
    snprintf(candidate.primary_action, sizeof(candidate.primary_action), "%s",
             "Compress");
  }
  load_validation_state(&candidate);
  populate_apr_index_state_from_roots(&candidate);
  if(out) *out = candidate;
  return 0;
}

static gc_operation_t *
find_op_locked(const char *op_id) {
  if(!op_id || !*op_id) return NULL;
  for(size_t i = 0; i < GC_MAX_OPS; i++) {
    if(g_ops[i].used && !strcmp(g_ops[i].id, op_id)) return &g_ops[i];
  }
  return NULL;
}

static gc_operation_t *
active_op_locked(void) {
  for(size_t i = 0; i < GC_MAX_OPS; i++) {
    if(g_ops[i].used && g_ops[i].status == GC_OP_RUNNING) return &g_ops[i];
  }
  return NULL;
}

static gc_operation_t *
pending_op_for_title_locked(const char *title_id) {
  for(size_t i = 0; i < GC_MAX_OPS; i++) {
    if(g_ops[i].used && g_ops[i].status == GC_OP_QUEUED &&
       !strcmp(g_ops[i].title_id, title_id)) {
      return &g_ops[i];
    }
  }
  return NULL;
}

static gc_operation_t *
active_op_for_title_locked(const char *title_id) {
  for(size_t i = 0; i < GC_MAX_OPS; i++) {
    if(g_ops[i].used && g_ops[i].status == GC_OP_RUNNING &&
       !strcmp(g_ops[i].title_id, title_id)) {
      return &g_ops[i];
    }
  }
  return NULL;
}

static gc_operation_t *
alloc_op_locked(void) {
  for(size_t i = 0; i < GC_MAX_OPS; i++) {
    if(!g_ops[i].used) return &g_ops[i];
  }
  gc_operation_t *oldest = NULL;
  for(size_t i = 0; i < GC_MAX_OPS; i++) {
    if(g_ops[i].status != GC_OP_RUNNING && g_ops[i].status != GC_OP_QUEUED &&
       (!oldest || g_ops[i].seq < oldest->seq)) {
      oldest = &g_ops[i];
    }
  }
  return oldest;
}

static void start_next_locked(void);

static void
operation_store_repair_counters(gc_operation_t *op,
                                const pfs_repair_info_t *repair) {
  if(!op || !repair) return;
  op->hash_checked_blocks = repair->hash_checked_blocks;
  op->hash_mismatched_blocks = repair->hash_mismatched_blocks;
  op->software_compared_blocks = repair->software_compared_blocks;
}

static void
operation_store_compression_stats(gc_operation_t *op, uint64_t source_size,
                                  const char *output_path) {
  struct stat st;
  uint64_t compressed_size = 0;
  if(!op) return;
  if(source_size > 0) op->compression_source_size = source_size;
  if(output_path && output_path[0] &&
     stat(output_path, &st) == 0 && S_ISREG(st.st_mode) && st.st_size > 0) {
    compressed_size = (uint64_t)st.st_size;
  }
  if(compressed_size > 0) op->compressed_size = compressed_size;
  if(op->compression_source_size > 0 && op->compressed_size > 0) {
    op->saved_bytes = op->compression_source_size > op->compressed_size
        ? op->compression_source_size - op->compressed_size
        : 0;
  }
}

static void
operation_store_scan_stats(gc_operation_t *op, const pfs_app_info_t *info) {
  if(!op || !info) return;
  op->scan_bytes = info->scan_bytes;
  op->scan_files = info->scan_files;
  op->scan_dirs = info->scan_dirs;
  op->scan_entries = info->scan_entries;
  op->scan_elapsed_ms = info->scan_elapsed_ms;
  op->scan_workers = info->scan_workers;
  if(info->apr_indexed) op->apr_indexed = 1;
  if(info->ampr_hot_swap_optimized) op->ampr_hot_swap_optimized = 1;
}

static void
operation_store_repair_success(gc_operation_t *op,
                               const pfs_repair_info_t *repair) {
  if(!op || !repair) return;
  op->repaired_blocks = repair->repaired_blocks;
  op->bad_blocks_found = 0;
  operation_store_repair_counters(op, repair);
  if(repair->repaired_blocks > 0) {
    op->hash_mismatched_blocks = 0;
    op->software_compared_blocks = 0;
  }
  if(repair->outdir[0]) {
    snprintf(op->repair_summary, sizeof(op->repair_summary), "%s",
             repair->outdir);
  }
  snprintf(op->result, sizeof(op->result), "%s",
           repair->repaired_blocks > 0 ? "repaired" :
           (repair->noop ? "noop" : "repaired"));
}

static int
shadowmount_mount_missed(const char *err) {
  if(!err) return 0;
  return strstr(err, "ShadowMountPlus did not remount") ||
         strstr(err, "not mounted by ShadowMountPlus");
}

static void
operation_mark_verified_not_mounted(gc_operation_t *op, const char *detail) {
  if(!op) return;
  snprintf(op->result, sizeof(op->result), "%s", "verified-not-mounted");
  op->error[0] = 0;
  gc_log("operation verified but not mounted title=%s detail=%s",
         op->title_id, detail && detail[0] ? detail : "ShadowMountPlus did not mount");
}

static void
operation_mark_not_mounted(gc_operation_t *op, const char *detail) {
  if(!op) return;
  snprintf(op->result, sizeof(op->result), "%s", "not-mounted");
  op->error[0] = 0;
  gc_log("operation complete but not mounted title=%s detail=%s",
         op->title_id, detail && detail[0] ? detail : "ShadowMountPlus did not mount");
}

static int
append_operation_json(json_buf_t *b, const gc_operation_t *op,
                      const char *id, int newline) {
  uint64_t bad_blocks_found = op->bad_blocks_found;
  uint64_t hash_mismatched_blocks = op->hash_mismatched_blocks;
  uint64_t software_compared_blocks = op->software_compared_blocks;
  if(op->status == GC_OP_SUCCESS && op->repaired_blocks > 0) {
    bad_blocks_found = 0;
    software_compared_blocks = 0;
  }
  return json_append(b, "{\"id\":") == 0 &&
      json_string(b, id) == 0 &&
      json_append(b, ",\"titleId\":") == 0 &&
      json_string(b, op->title_id) == 0 &&
      json_append(b, ",\"displayName\":") == 0 &&
      json_string(b, op->display_name) == 0 &&
      json_append(b, ",\"action\":") == 0 &&
      json_string(b, action_name(op->action)) == 0 &&
      json_append(b, ",\"status\":") == 0 &&
      json_string(b, status_name(op->status)) == 0 &&
      json_append(b, ",\"phase\":") == 0 &&
      json_string(b, op->phase) == 0 &&
      json_append(b, ",\"result\":") == 0 &&
      json_string(b, op->result) == 0 &&
      json_append(b, ",\"error\":") == 0 &&
      json_string(b, op->error) == 0 &&
      json_append(b, ",\"sourcePath\":") == 0 &&
      json_string(b, op->source_path) == 0 &&
      json_append(b, ",\"outputPath\":") == 0 &&
      json_string(b, op->output_path) == 0 &&
      json_append(b, ",\"sourceKind\":") == 0 &&
      json_string(b, op->source_kind) == 0 &&
      json_append(b, ",\"format\":") == 0 &&
      json_string(b, op->format) == 0 &&
      json_append(b, ",\"deletePolicy\":") == 0 &&
      json_string(b, op->delete_policy) == 0 &&
      json_append(b, ",\"compressionMode\":") == 0 &&
      json_string(b, compression_mode_or_default(op->compression_mode)) == 0 &&
      json_append(b, ",\"streamOrder\":") == 0 &&
      json_string(b, op->stream_order[0] ? op->stream_order : "budgeted-gain") == 0 &&
      json_append(b, ",\"targetRoot\":") == 0 &&
      json_string(b, op->target_root) == 0 &&
      json_appendf(b, ",\"skipSpaceCheck\":%s",
                   op->skip_space_check ? "true" : "false") == 0 &&
      json_append(b, ",\"preserveOriginal\":") == 0 &&
      json_string(b, op->preserve_original) == 0 &&
      json_append(b, ",\"preservedOriginalPath\":") == 0 &&
      json_string(b, op->preserved_original_path) == 0 &&
      json_append(b, ",\"preservedHiddenPath\":") == 0 &&
      json_string(b, op->preserved_hidden_path) == 0 &&
      json_append(b, ",\"repairSummary\":") == 0 &&
      json_string(b, op->repair_summary) == 0 &&
      json_append(b, ",\"amprVersion\":") == 0 &&
      json_string(b, op->ampr_version) == 0 &&
      json_append(b, ",\"amprSha256\":") == 0 &&
      json_string(b, op->ampr_sha256) == 0 &&
      json_append(b, ",\"amprCachePath\":") == 0 &&
      json_string(b, op->ampr_cache_path) == 0 &&
      json_append(b, ",\"amprResultMode\":") == 0 &&
      json_string(b, op->ampr_result_mode) == 0 &&
      json_append(b, ",\"amprIntent\":") == 0 &&
      json_string(b, op->ampr_intent) == 0 &&
      json_append(b, ",\"readRoot\":") == 0 &&
      json_string(b, op->read_root) == 0 &&
      json_append(b, ",\"readStorage\":") == 0 &&
      json_string(b, op->read_storage) == 0 &&
      json_append(b, ",\"readFirstErrorPath\":") == 0 &&
      json_string(b, op->read_first_error_path) == 0 &&
      json_append(b, ",\"readFirstError\":") == 0 &&
      json_string(b, op->read_first_error) == 0 &&
      json_appendf(b,
                   ",\"streamBudgetBytes\":%llu,"
                   "\"compressionSourceSize\":%llu,"
                   "\"compressedSize\":%llu,"
                   "\"savedBytes\":%llu,"
                   "\"scanBytes\":%llu,\"scanFiles\":%llu,"
                   "\"scanDirs\":%llu,\"scanEntries\":%llu,"
                   "\"scanElapsedMs\":%llu,\"scanWorkers\":%llu,"
                   "\"aprIndexed\":%s,"
                   "\"amprHotSwapOptimized\":%s,"
                   "\"readBytes\":%llu,\"readFiles\":%llu,"
                   "\"readDirs\":%llu,\"readElapsedMs\":%llu,"
                   "\"readAvgBps\":%llu,\"readMinBps\":%llu,"
                   "\"readMaxBps\":%llu,\"readErrors\":%llu,"
                   "\"readSkipped\":%llu,"
                   "\"createdAt\":%ld,\"startedAt\":%ld,\"endedAt\":%ld,"
                   "\"repairedBlocks\":%llu,\"badBlocksFound\":%llu,"
                   "\"hashCheckedBlocks\":%llu,"
                   "\"hashMismatchedBlocks\":%llu,"
                   "\"softwareComparedBlocks\":%llu}%s",
                   (unsigned long long)op->stream_budget_bytes,
                   (unsigned long long)op->compression_source_size,
                   (unsigned long long)op->compressed_size,
                   (unsigned long long)op->saved_bytes,
                   (unsigned long long)op->scan_bytes,
                   (unsigned long long)op->scan_files,
                   (unsigned long long)op->scan_dirs,
                   (unsigned long long)op->scan_entries,
                   (unsigned long long)op->scan_elapsed_ms,
                   (unsigned long long)op->scan_workers,
                   op->apr_indexed ? "true" : "false",
                   op->ampr_hot_swap_optimized ? "true" : "false",
                   (unsigned long long)op->read_bytes,
                   (unsigned long long)op->read_files,
                   (unsigned long long)op->read_dirs,
                   (unsigned long long)op->read_elapsed_ms,
                   (unsigned long long)op->read_avg_bps,
                   (unsigned long long)op->read_min_bps,
                   (unsigned long long)op->read_max_bps,
                   (unsigned long long)op->read_errors,
                   (unsigned long long)op->read_skipped,
                   (long)op->created_at, (long)op->started_at,
                   (long)op->ended_at,
                   (unsigned long long)op->repaired_blocks,
                   (unsigned long long)bad_blocks_found,
                   (unsigned long long)op->hash_checked_blocks,
                   (unsigned long long)hash_mismatched_blocks,
                   (unsigned long long)software_compared_blocks,
                   newline ? "\n" : "") == 0 ? 0 : -1;
}

static void
append_operation_log_file(const gc_operation_t *op, const char *dir,
                          const char *path) {
  if(!op || !dir || !path) return;
  if(mkdirs(dir) != 0) return;
  int fd = open(path, O_WRONLY | O_CREAT | O_APPEND, 0666);
  if(fd < 0) return;
  json_buf_t b = {0};
  char row_id[64];
  if(op->status == GC_OP_RUNNING && op->phase[0]) {
    snprintf(row_id, sizeof(row_id), "%s:%s", op->id, op->phase);
  } else {
    snprintf(row_id, sizeof(row_id), "%s", op->id);
  }
  if(append_operation_json(&b, op, row_id, 1) == 0) {
    write_all_fd(fd, b.data, b.len);
    fsync(fd);
  }
  free(b.data);
  close(fd);
}

static void
append_history_log(const gc_operation_t *op) {
  append_operation_log_file(op, GC_HISTORY_DIR, GC_HISTORY_LOG);
}

static void
append_operation_logs(const gc_operation_t *op) {
  append_history_log(op);
}

static int
operation_result_is_intermediate_phase(const char *phase) {
  return !strcmp(phase ? phase : "", "compressing") ||
         !strcmp(phase ? phase : "", "unpacking") ||
         !strcmp(phase ? phase : "", "copying") ||
         !strcmp(phase ? phase : "", "compressed") ||
         !strcmp(phase ? phase : "", "source-deleted") ||
         !strcmp(phase ? phase : "", "validating") ||
         !strcmp(phase ? phase : "", "repairing");
}

static void
append_operation_phase(gc_operation_t *op, const char *phase) {
  if(!op || !phase || !phase[0]) return;
  pthread_mutex_lock(&g_gc_lock);
  snprintf(op->phase, sizeof(op->phase), "%s", phase);
  snprintf(op->result, sizeof(op->result), "%s", phase);
  if(op->status != GC_OP_FAILED && op->status != GC_OP_CANCELLED &&
     op->status != GC_OP_SUCCESS) {
    op->status = GC_OP_RUNNING;
  }
  append_operation_logs(op);
  pthread_mutex_unlock(&g_gc_lock);
}

static int
remove_tree_gc(const char *path) {
  struct stat st;
  int job_busy = atomic_load(&g_job.busy);
  if(job_busy) job_set_current(path);
  if(lstat(path, &st) != 0) return errno == ENOENT ? 0 : -1;
  if(!S_ISDIR(st.st_mode)) {
    int rc = unlink(path);
    if(rc == 0 && job_busy) atomic_fetch_add(&g_job.done_files, 1);
    return rc;
  }
  DIR *d = opendir(path);
  if(!d) return -1;
  struct dirent *ent;
  while((ent = readdir(d))) {
    if(!strcmp(ent->d_name, ".") || !strcmp(ent->d_name, "..")) continue;
    char child[1024];
    join_path(child, sizeof(child), path, ent->d_name);
    if(remove_tree_gc(child) != 0) {
      closedir(d);
      return -1;
    }
  }
  closedir(d);
  int rc = rmdir(path);
  if(rc == 0 && job_busy) atomic_fetch_add(&g_job.done_files, 1);
  return rc;
}

static int
preserve_mode_time(const char *path, const struct stat *st) {
  if(!path || !st) return -1;
  chmod(path, st->st_mode & 0777);
  struct timeval times[2];
  times[0].tv_sec = st->st_atime;
  times[0].tv_usec = 0;
  times[1].tv_sec = st->st_mtime;
  times[1].tv_usec = 0;
  return utimes(path, times);
}

static void
job_store_u64(atomic_long *field, uint64_t value) {
  atomic_store(field, value > (uint64_t)LONG_MAX ? LONG_MAX : (long)value);
}

static uint64_t
monotonic_millis_gc(void) {
  struct timeval tv;
  gettimeofday(&tv, NULL);
  return (uint64_t)tv.tv_sec * 1000ULL + (uint64_t)(tv.tv_usec / 1000);
}

typedef struct gc_read_speed_ctx {
  uint64_t started_ms;
  uint64_t deadline_ms;
  uint64_t bytes_read;
  uint64_t files_opened;
  char *buf;
} gc_read_speed_ctx_t;

static int
read_speed_time_expired(const gc_read_speed_ctx_t *ctx) {
  return !ctx || monotonic_millis_gc() >= ctx->deadline_ms;
}

static void
read_speed_update_progress(gc_read_speed_ctx_t *ctx) {
  uint64_t now = monotonic_millis_gc();
  uint64_t elapsed = now > ctx->started_ms ? (now - ctx->started_ms) / 1000ULL : 0;
  if(elapsed > GC_READ_SPEED_TEST_SECONDS) elapsed = GC_READ_SPEED_TEST_SECONDS;
  atomic_store(&g_job.phase_step, elapsed > (uint64_t)LONG_MAX ? LONG_MAX :
               (long)elapsed);
  job_store_u64(&g_job.copied_bytes, ctx->bytes_read);
}

static int
read_speed_file(const char *path, gc_read_speed_ctx_t *ctx,
                char *err, size_t err_size) {
  if(gc_cancel_requested(err, err_size)) return -1;
  int fd = open(path, O_RDONLY);
  if(fd < 0) return 0;
  ctx->files_opened++;
  job_set_current(path);
  while(!read_speed_time_expired(ctx)) {
    if(gc_cancel_requested(err, err_size)) {
      close(fd);
      return -1;
    }
    ssize_t n = read(fd, ctx->buf, GC_COPY_CHUNK_SIZE);
    if(n < 0) {
      if(errno == EINTR) continue;
      snprintf(err, err_size, "read speed source: %s", strerror(errno));
      close(fd);
      return -1;
    }
    if(n == 0) break;
    ctx->bytes_read += (uint64_t)n;
    read_speed_update_progress(ctx);
  }
  close(fd);
  return 0;
}

static int
read_speed_walk(const char *path, gc_read_speed_ctx_t *ctx,
                char *err, size_t err_size) {
  struct stat st;
  if(read_speed_time_expired(ctx)) return 0;
  if(gc_cancel_requested(err, err_size)) return -1;
  if(lstat(path, &st) != 0) return 0;
  if(gc_cancel_requested(err, err_size)) return -1;
  if(S_ISREG(st.st_mode)) {
    return read_speed_file(path, ctx, err, err_size);
  }
  if(!S_ISDIR(st.st_mode)) return 0;

  if(gc_cancel_requested(err, err_size)) return -1;
  DIR *d = opendir(path);
  if(!d) return 0;
  if(gc_cancel_requested(err, err_size)) {
    closedir(d);
    return -1;
  }
  int rc = 0;
  struct dirent *ent;
  while(1) {
    if(gc_cancel_requested(err, err_size)) {
      rc = -1;
      break;
    }
    ent = readdir(d);
    if(!ent) break;
    if(gc_cancel_requested(err, err_size)) {
      rc = -1;
      break;
    }
    if(read_speed_time_expired(ctx)) break;
    if(!strcmp(ent->d_name, ".") || !strcmp(ent->d_name, "..")) continue;
    char child[1024];
    int n = snprintf(child, sizeof(child), "%s/%s", path, ent->d_name);
    if(n < 0 || (size_t)n >= sizeof(child)) continue;
    if(read_speed_walk(child, ctx, err, err_size) != 0) {
      rc = -1;
      break;
    }
  }
  closedir(d);
  return rc;
}

static int
write_all_fd_cancelable(int fd, const void *data, size_t size,
                        char *err, size_t err_size) {
  const char *p = data;
  while(size > 0) {
    if(gc_cancel_requested(err, err_size)) return -1;
    size_t want = size > GC_COPY_CHUNK_SIZE ? GC_COPY_CHUNK_SIZE : size;
    ssize_t n = write(fd, p, want);
    if(n < 0) {
      if(errno == EINTR) continue;
      snprintf(err, err_size, "write target: %s", strerror(errno));
      return -1;
    }
    if(n == 0) {
      errno = EIO;
      snprintf(err, err_size, "%s", "write target: short write");
      return -1;
    }
    p += n;
    size -= (size_t)n;
  }
  return 0;
}

static int
copy_regular_file_gc(const char *src, const char *dst,
                     const struct stat *st, uint64_t *copied,
                     char *err, size_t err_size) {
  int in = -1;
  int out = -1;
  char *buf = NULL;
  int rc = -1;

  if(gc_cancel_requested(err, err_size)) goto done;
  in = open(src, O_RDONLY);
  if(in < 0) {
    snprintf(err, err_size, "open source: %s", strerror(errno));
    goto done;
  }
  if(gc_cancel_requested(err, err_size)) goto done;
  out = open(dst, O_WRONLY | O_CREAT | O_EXCL, st->st_mode & 0777);
  if(out < 0) {
    snprintf(err, err_size, "create target: %s", strerror(errno));
    goto done;
  }
  if(gc_cancel_requested(err, err_size)) goto done;
  buf = malloc(GC_COPY_CHUNK_SIZE);
  if(!buf) {
    snprintf(err, err_size, "%s", "out of memory");
    goto done;
  }

  while(1) {
    if(gc_cancel_requested(err, err_size)) goto done;
    ssize_t n = read(in, buf, GC_COPY_CHUNK_SIZE);
    if(n < 0) {
      if(errno == EINTR) continue;
      snprintf(err, err_size, "read source: %s", strerror(errno));
      goto done;
    }
    if(n == 0) break;
    if(gc_cancel_requested(err, err_size)) goto done;
    if(write_all_fd_cancelable(out, buf, (size_t)n,
                               err, err_size) != 0) goto done;
    if(copied) {
      *copied += (uint64_t)n;
      job_store_u64(&g_job.copied_bytes, *copied);
    }
  }
  if(gc_cancel_requested(err, err_size)) goto done;
  fsync(out);
  if(gc_cancel_requested(err, err_size)) goto done;
  if(close(out) != 0) {
    out = -1;
    snprintf(err, err_size, "close target: %s", strerror(errno));
    goto done;
  }
  out = -1;
  if(gc_cancel_requested(err, err_size)) goto done;
  preserve_mode_time(dst, st);
  rc = 0;

done:
  if(out >= 0) close(out);
  if(in >= 0) close(in);
  free(buf);
  return rc;
}

static int
copy_tree_gc(const char *src, const char *dst, uint64_t *copied,
             char *err, size_t err_size) {
  struct stat st;
  if(gc_cancel_requested(err, err_size)) return -1;
  if(lstat(src, &st) != 0) {
    snprintf(err, err_size, "stat source: %s", strerror(errno));
    return -1;
  }
  if(gc_cancel_requested(err, err_size)) return -1;
  job_set_current(path_basename(src));

  if(S_ISDIR(st.st_mode)) {
    if(gc_cancel_requested(err, err_size)) return -1;
    if(mkdir(dst, st.st_mode & 0777) != 0) {
      snprintf(err, err_size, "create target folder: %s", strerror(errno));
      return -1;
    }
    if(gc_cancel_requested(err, err_size)) return -1;
    DIR *d = opendir(src);
    if(!d) {
      snprintf(err, err_size, "open source folder: %s", strerror(errno));
      return -1;
    }
    struct dirent *ent;
    while(1) {
      if(gc_cancel_requested(err, err_size)) {
        closedir(d);
        return -1;
      }
      ent = readdir(d);
      if(!ent) break;
      if(gc_cancel_requested(err, err_size)) {
        closedir(d);
        return -1;
      }
      if(!strcmp(ent->d_name, ".") || !strcmp(ent->d_name, "..")) continue;
      char src_child[1024];
      char dst_child[1024];
      join_path(src_child, sizeof(src_child), src, ent->d_name);
      join_path(dst_child, sizeof(dst_child), dst, ent->d_name);
      if(gc_cancel_requested(err, err_size)) {
        closedir(d);
        return -1;
      }
      if(copy_tree_gc(src_child, dst_child, copied, err, err_size) != 0) {
        closedir(d);
        return -1;
      }
    }
    closedir(d);
    if(gc_cancel_requested(err, err_size)) return -1;
    preserve_mode_time(dst, &st);
    return 0;
  }

  if(S_ISREG(st.st_mode)) {
    return copy_regular_file_gc(src, dst, &st, copied, err, err_size);
  }

  if(S_ISLNK(st.st_mode)) {
    char target[1024];
    if(gc_cancel_requested(err, err_size)) return -1;
    ssize_t n = readlink(src, target, sizeof(target) - 1);
    if(n < 0) {
      snprintf(err, err_size, "read symlink: %s", strerror(errno));
      return -1;
    }
    target[n] = 0;
    if(gc_cancel_requested(err, err_size)) return -1;
    if(symlink(target, dst) != 0) {
      snprintf(err, err_size, "create symlink: %s", strerror(errno));
      return -1;
    }
    return 0;
  }

  snprintf(err, err_size, "%s", "unsupported source entry type");
  errno = EINVAL;
  return -1;
}

static int
delete_source_after_success(const char *path, gc_source_kind_t kind,
                            char *err, size_t err_size) {
  if(gc_cancel_requested(err, err_size)) return -1;
  if(kind == GC_SOURCE_FOLDER) {
    if(remove_tree_gc(path) != 0) {
      snprintf(err, err_size, "remove source folder: %s", strerror(errno));
      return -1;
    }
    gc_size_cache_forget(path);
    return 0;
  }
  if(unlink(path) != 0 && errno != ENOENT) {
    snprintf(err, err_size, "remove source image: %s", strerror(errno));
    return -1;
  }
  return 0;
}

static void
delete_vhash_sidecar_if_present(const char *path, const char *context,
                                const char *title_id) {
  char sidecar[PATH_MAX];
  if(!path || !path[0]) return;
  if(pfs_vhash_sidecar_path(path, sidecar, sizeof(sidecar)) != 0) return;
  if(unlink(sidecar) == 0) {
    gc_log("%s removed validation hash title=%s path=%s",
           context ? context : "cleanup", title_id ? title_id : "", sidecar);
  } else if(errno != ENOENT) {
    gc_log("%s could not remove validation hash title=%s path=%s err=%s",
           context ? context : "cleanup", title_id ? title_id : "", sidecar,
           strerror(errno));
  }
}

static int
delete_source_after_success_with_title(const char *path, gc_source_kind_t kind,
                                       const char *title_id,
                                       char *err, size_t err_size) {
  int rc = delete_source_after_success(path, kind, err, err_size);
  if(rc == 0 && kind != GC_SOURCE_FOLDER) {
    delete_vhash_sidecar_if_present(path, "delete source", title_id);
  }
  return rc;
}

static int
game_source_delete_allowed(const gc_game_t *game) {
  gc_source_roots_t roots;
  if(!game || game->source_kind == GC_SOURCE_UNKNOWN ||
     !path_is_safe(game->source_path) ||
     path_under_root(game->source_path, GC_SHADOW_IMAGE_BASE) ||
     path_under_root(game->source_path, "/user/app") ||
     path_is_system_app_path(game->source_path)) {
    return 0;
  }
  shadow_source_roots_build(&roots);
  for(size_t i = 0; i < roots.count; i++) {
    if(path_under_root(game->source_path, roots.roots[i])) {
      return 1;
    }
  }
  return 0;
}

static void
cleanup_shadowmount_hints_for_deleted_source(const gc_operation_t *op,
                                             const gc_game_t *game) {
  char hint_err[256] = {0};
  if(!op || !game) return;
  if(game->source_kind == GC_SOURCE_COMPRESSED) {
    pfs_decompress_info_t info = {0};
    if(pfs_decompress_detect_nested(game->source_path, &info, hint_err,
                                    sizeof(hint_err)) == 0) {
      hint_err[0] = 0;
      if(gc_shadowmount_remove_pfsc_hints(game->source_path,
                                          info.nested_name,
                                          info.nested_type,
                                          hint_err,
                                          sizeof(hint_err)) != 0) {
        gc_log("delete pfsc hint cleanup failed title=%s path=%s err=%s",
               op->title_id, game->source_path,
               hint_err[0] ? hint_err : "unknown");
      }
    } else {
      gc_log("delete nested inspect skipped title=%s path=%s err=%s",
             op->title_id, game->source_path,
             hint_err[0] ? hint_err : "unknown");
    }
  }
  if(game->source_kind == GC_SOURCE_COMPRESSED ||
     game->source_kind == GC_SOURCE_IMAGE) {
    hint_err[0] = 0;
    if(gc_shadowmount_remove_title_pfsc_hints(op->title_id,
                                              game->source_path,
                                              hint_err,
                                              sizeof(hint_err)) != 0) {
      gc_log("delete title hint cleanup failed title=%s path=%s err=%s",
             op->title_id, game->source_path,
             hint_err[0] ? hint_err : "unknown");
    }
  }
}

typedef struct gc_source_quarantine {
  char original_path[1024];
  char quarantine_path[1024];
  int active;
} gc_source_quarantine_t;

static int
build_uncompress_quarantine_path(const char *source_path,
                                 const char *title_id,
                                 const char *op_id,
                                 char *out,
                                 size_t out_size) {
  char parent[1024];
  struct stat st;
  if(!source_path || !source_path[0] || !out || out_size == 0 ||
     !valid_title_id(title_id) ||
     path_parent(source_path, parent, sizeof(parent)) != 0) {
    errno = EINVAL;
    return -1;
  }
  for(int attempt = 0; attempt < 32; attempt++) {
    int n = snprintf(out, out_size, "%s%s.gc-uncompress-%s-%s-%ld-%u-%02d.source",
                     parent, parent[1] ? "/" : "", title_id,
                     op_id && op_id[0] ? op_id : "op",
                     (long)time(NULL), (unsigned)getpid(), attempt);
    if(n < 0 || (size_t)n >= out_size) {
      errno = ENAMETOOLONG;
      return -1;
    }
    if(lstat(out, &st) != 0) {
      if(errno == ENOENT) return 0;
      return -1;
    }
  }
  errno = EEXIST;
  return -1;
}

static int
quarantine_uncompress_source(const char *source_path,
                             const char *title_id,
                             const char *op_id,
                             gc_source_quarantine_t *q,
                             char *err,
                             size_t err_size) {
  char quarantine_path[1024];
  struct stat st;
  if(!q) {
    snprintf(err, err_size, "%s", "bad source quarantine state");
    errno = EINVAL;
    return -1;
  }
  memset(q, 0, sizeof(*q));
  if(lstat(source_path, &st) != 0 || !S_ISREG(st.st_mode)) {
    snprintf(err, err_size, "compressed source unavailable for quarantine: %s",
             strerror(errno));
    return -1;
  }
  if(build_uncompress_quarantine_path(source_path, title_id, op_id,
                                      quarantine_path,
                                      sizeof(quarantine_path)) != 0) {
    snprintf(err, err_size, "build source quarantine path: %s",
             strerror(errno));
    return -1;
  }
  if(rename(source_path, quarantine_path) != 0) {
    snprintf(err, err_size, "quarantine compressed source: %s",
             strerror(errno));
    return -1;
  }
  snprintf(q->original_path, sizeof(q->original_path), "%s", source_path);
  snprintf(q->quarantine_path, sizeof(q->quarantine_path), "%s",
           quarantine_path);
  q->active = 1;
  gc_size_cache_forget(source_path);
  gc_log("uncompress source quarantined title=%s original=%s quarantine=%s",
         title_id ? title_id : "", source_path, quarantine_path);
  return 0;
}

static int
delete_quarantined_uncompress_source(gc_source_quarantine_t *q,
                                     const char *title_id,
                                     char *err,
                                     size_t err_size) {
  if(!q || !q->active) return 0;
  if(unlink(q->quarantine_path) != 0 && errno != ENOENT) {
    snprintf(err, err_size, "remove quarantined compressed source: %s",
             strerror(errno));
    return -1;
  }
  gc_log("uncompress quarantined source deleted title=%s original=%s quarantine=%s",
         title_id ? title_id : "", q->original_path, q->quarantine_path);
  q->active = 0;
  return 0;
}

static int
uncompress_delete_quarantined_source_or_fail(gc_operation_t *op,
                                             gc_source_quarantine_t *q,
                                             char *err,
                                             size_t err_size) {
  if(delete_quarantined_uncompress_source(q, op->title_id,
                                          err, err_size) != 0) {
    snprintf(op->error, sizeof(op->error), "%s", err);
    gc_log("uncompress source delete failed title=%s err=%s", op->title_id,
           op->error);
    return -1;
  }
  return 0;
}

static int
uncompress_complete_not_mounted(gc_operation_t *op,
                                gc_source_quarantine_t *q,
                                const pfs_decompress_info_t *info,
                                int as_image,
                                const char *detail,
                                char *err,
                                size_t err_size) {
  operation_mark_not_mounted(op, detail);
  if(uncompress_delete_quarantined_source_or_fail(op, q, err, err_size) != 0) {
    return -1;
  }
  gc_size_cache_queue_measure(info->output_path);
  if(as_image) {
    gc_log("uncompress image complete but not mounted title=%s output=%s detail=%s",
           op->title_id, op->output_path, detail && detail[0] ? detail : "");
  } else {
    gc_log("uncompress complete but not mounted title=%s output=%s detail=%s",
           op->title_id, op->output_path, detail && detail[0] ? detail : "");
  }
  return 0;
}

static void
restore_quarantined_uncompress_source(gc_source_quarantine_t *q,
                                      const char *title_id) {
  struct stat st;
  if(!q || !q->active) return;
  if(lstat(q->original_path, &st) == 0) {
    gc_log("uncompress source quarantine restore skipped title=%s original exists=%s quarantine=%s",
           title_id ? title_id : "", q->original_path, q->quarantine_path);
    return;
  }
  if(errno != ENOENT) {
    gc_log("uncompress source quarantine restore stat failed title=%s original=%s err=%s",
           title_id ? title_id : "", q->original_path, strerror(errno));
    return;
  }
  if(rename(q->quarantine_path, q->original_path) != 0) {
    gc_log("uncompress source quarantine restore failed title=%s original=%s quarantine=%s err=%s",
           title_id ? title_id : "", q->original_path, q->quarantine_path,
           strerror(errno));
    return;
  }
  gc_log("uncompress source quarantine restored title=%s original=%s quarantine=%s",
         title_id ? title_id : "", q->original_path, q->quarantine_path);
  q->active = 0;
}

static void
cleanup_failed_safe_compress_output(const char *path, const char *title_id) {
  if(!path || !path[0]) return;
  if(unlink(path) == 0 || errno == ENOENT) {
    gc_log("compress cleanup removed output title=%s path=%s",
           title_id ? title_id : "", path);
    delete_vhash_sidecar_if_present(path, "compress cleanup", title_id);
  } else {
    gc_log("compress cleanup could not remove output title=%s path=%s err=%s",
           title_id ? title_id : "", path, strerror(errno));
  }
}

static void
cleanup_failed_uncompress_output(const char *path, const char *title_id) {
  if(!path || !path[0]) return;
  if(remove_tree_gc(path) == 0) {
    gc_size_cache_forget(path);
    gc_log("uncompress cleanup removed invalid output title=%s path=%s",
           title_id ? title_id : "", path);
  } else {
    gc_log("uncompress cleanup could not remove invalid output title=%s path=%s err=%s",
           title_id ? title_id : "", path, strerror(errno));
  }
}

static int
planned_compress_nested(const gc_game_t *game, int format,
                        char *nested_name, size_t nested_name_size,
                        int *nested_type) {
  if(!game || !nested_name || nested_name_size == 0 || !nested_type) return -1;
  nested_name[0] = 0;
  *nested_type = PFS_NESTED_UNKNOWN;

  if(game->source_kind == GC_SOURCE_IMAGE) {
    snprintf(nested_name, nested_name_size, "%s", game->nested_name);
    *nested_type = game->nested_type;
    return nested_name[0] && *nested_type != PFS_NESTED_UNKNOWN ? 0 : -1;
  }

  if(format == PFS_COMPRESS_FORMAT_EXFAT) {
    int n = snprintf(nested_name, nested_name_size, "%s.exfat",
                     game->title_id);
    if(n < 0 || (size_t)n >= nested_name_size) return -1;
    *nested_type = PFS_NESTED_EXFAT;
    return 0;
  }

  snprintf(nested_name, nested_name_size, "%s", "pfs_image.dat");
  *nested_type = PFS_NESTED_PFS;
  return 0;
}

static int
prepare_uncompress_plan(gc_game_t *game, int as_image,
                        const char *output_path,
                        pfs_decompress_info_t *info,
                        char *err, size_t err_size) {
  pfs_decompress_info_t dec = {0};
  uint64_t free_bytes = 0;
  struct stat st;
  if(!game || game->source_kind != GC_SOURCE_COMPRESSED) {
    snprintf(err, err_size, "%s", "compressed game is unavailable");
    return -1;
  }
  err[0] = 0;
  int rc = as_image
      ? pfs_decompress_probe_image(game->source_path, &dec, err, err_size)
      : pfs_decompress_detect_nested(game->source_path, &dec, err, err_size);
  if(rc != 0) return -1;

  if(output_path && output_path[0]) {
    int n = snprintf(dec.output_path, sizeof(dec.output_path), "%s",
                     output_path);
    if(n < 0 || (size_t)n >= sizeof(dec.output_path)) {
      snprintf(err, err_size, "%s", "uncompress target path too long");
      errno = ENAMETOOLONG;
      return -1;
    }
    dec.output_exists = stat(dec.output_path, &st) == 0;
  }

  snprintf(game->output_path, sizeof(game->output_path), "%s",
           dec.output_path);
  snprintf(game->nested_name, sizeof(game->nested_name), "%s",
           dec.nested_name);
  game->nested_type = dec.nested_type;
  game->output_exists = dec.output_exists;
  if(dec.nested_size > 0) game->required_bytes = dec.nested_size;
  if(free_bytes_for_output(dec.output_path, &free_bytes) == 0) {
    game->free_bytes = free_bytes;
  }
  game->extra_needed = game->free_bytes >= game->required_bytes
      ? 0
      : game->required_bytes - game->free_bytes;
  if(as_image) game->can_stream_delete = 0;
  if(info) *info = dec;
  return 0;
}

static int
repair_with_wait(const char *title_id, const char *path,
                 pfs_repair_info_t *info, char *err, size_t err_size) {
  time_t deadline = time(NULL) + GC_REPAIR_WAIT_SECONDS;
  gc_checkpoint("repair waiting for mount");
  gc_log("repair wait title=%s path=%s", title_id ? title_id : "",
         path ? path : "");
  while(1) {
    if(gc_cancel_requested(err, err_size)) return -1;
    err[0] = 0;
    memset(info, 0, sizeof(*info));
    gc_checkpoint("repair attempt");
    int rc = pfs_repair_ffpfsc_auto(path, info, err, err_size);
    if(rc == 0) {
      const char *result = info->noop ? "noop" : "repaired";
      gc_log("repair success title=%s result=%s repaired=%llu blocks=%llu",
             title_id ? title_id : "", result,
             (unsigned long long)info->repaired_blocks,
             (unsigned long long)info->block_count);
      return 0;
    }
    gc_log("repair attempt failed title=%s err=%s",
           title_id ? title_id : "", err[0] ? err : "unknown");
    if(!strstr(err, "not mounted by ShadowMountPlus") ||
       time(NULL) >= deadline || job_cancelled()) {
      return -1;
    }
    {
      char scan_err[256] = {0};
      if(gc_shadowmount_request_title_source_scan_cancelable(
             title_id, path, scan_err, sizeof(scan_err)) != 0) {
        if(job_cancelled()) return -1;
        gc_log("repair wait scan request failed title=%s err=%s",
               title_id ? title_id : "",
               scan_err[0] ? scan_err : "unknown");
      }
    }
    job_set_phase("mounting", 0, 0, "Waiting for ShadowMountPlus");
    if(gc_sleep_cancelable_seconds(GC_REPAIR_WAIT_STEP_SECONDS,
                                   err, err_size) != 0) {
      return -1;
    }
  }
}

static int
repair_scan_only_with_wait(const char *title_id, const char *path,
                           pfs_repair_info_t *info, int *scan_rc_out,
                           char *err, size_t err_size) {
  time_t deadline = time(NULL) + GC_REPAIR_WAIT_SECONDS;
  gc_checkpoint("validate-only waiting for mount");
  gc_log("validate-only wait title=%s path=%s", title_id ? title_id : "",
         path ? path : "");
  while(1) {
    if(gc_cancel_requested(err, err_size)) return -1;
    err[0] = 0;
    memset(info, 0, sizeof(*info));
    gc_checkpoint("validate-only attempt");
    int rc = pfs_repair_ffpfsc_scan_only(path, info, err, err_size);
    if(scan_rc_out) *scan_rc_out = rc;
    if(rc == 0 || rc == PFS_REPAIR_SCAN_REPAIR_NEEDED) {
      gc_log("validate-only success title=%s result=%s bad=%llu blocks=%llu",
             title_id ? title_id : "",
             rc == PFS_REPAIR_SCAN_REPAIR_NEEDED ? "bad-blocks-found" : "clean",
             (unsigned long long)info->repaired_blocks,
             (unsigned long long)info->block_count);
      return 0;
    }
    gc_log("validate-only attempt failed title=%s err=%s",
           title_id ? title_id : "", err[0] ? err : "unknown");
    if(!strstr(err, "not mounted by ShadowMountPlus") ||
       time(NULL) >= deadline || job_cancelled()) {
      return -1;
    }
    {
      char scan_err[256] = {0};
      if(gc_shadowmount_request_title_source_scan_cancelable(
             title_id, path, scan_err, sizeof(scan_err)) != 0) {
        if(job_cancelled()) return -1;
        gc_log("validate-only wait scan request failed title=%s err=%s",
               title_id ? title_id : "",
               scan_err[0] ? scan_err : "unknown");
      }
    }
    job_set_phase("mounting", 0, 0, "Waiting for ShadowMountPlus");
    if(gc_sleep_cancelable_seconds(GC_REPAIR_WAIT_STEP_SECONDS,
                                   err, err_size) != 0) {
      return -1;
    }
  }
}

static int
post_repair_smoke_verify(const char *title_id, const char *path,
                         pfs_repair_info_t *info, int remount_ready,
                         char *err, size_t err_size) {
  if(!info || info->repaired_blocks == 0) return 0;
  if(info->repair_mode == PFS_REPAIR_MODE_COPY_REPLACE && !remount_ready) {
    char expected_image[1024];
    char expected_mount[1024];
    if(expected_compressed_shadow_paths(path, info->nested_name,
                                        info->nested_type,
                                        expected_image,
                                        sizeof(expected_image),
                                        expected_mount,
                                        sizeof(expected_mount)) != 0) {
      snprintf(err, err_size, "%s", "could not derive repair smoke mount paths");
      return -1;
    }
    job_set_phase("mounting", 0, 0, "Waiting for repair remount");
    if(wait_for_shadowmount_links(title_id, expected_mount, expected_image,
                                  err, err_size) != 0) {
      return -1;
    }
  }
  job_set_phase("validating", 0, 0, "Smoke verifying repaired blocks");
  if(pfs_repair_ffpfsc_smoke_verify(path, info, err, err_size) != 0) {
    gc_log("repair smoke verify failed title=%s err=%s",
           title_id ? title_id : "", err && err[0] ? err : "unknown");
    return -1;
  }
  gc_log("repair smoke verify title=%s blocks=%llu mounted=%llu",
         title_id ? title_id : "",
         (unsigned long long)info->post_verify_blocks,
         (unsigned long long)info->post_verify_mount_blocks);
  return 0;
}

static int
build_force_remount_temp_path(const char *path, const char *title_id,
                              char *out, size_t out_size) {
  char parent[1024];
  const char *name = base_name(path);
  const char *suffix = NULL;
  size_t suffix_len = 0;
  if(!path || !title_id || !valid_title_id(title_id) || !name ||
     path_parent(path, parent, sizeof(parent)) != 0) {
    return -1;
  }
  suffix = force_remount_visible_suffix(name, &suffix_len);
  if(!suffix) return -1;
  size_t name_len = strlen(name);
  if(name_len <= suffix_len) return -1;
  size_t stem_len = name_len - suffix_len;
  if(stem_len != 9 || strncmp(name, title_id, stem_len) != 0) return -1;
  int n = snprintf(out, out_size, "%s%s.%.*s%s%ld_%u%s",
                   parent, parent[1] ? "/" : "",
                   (int)stem_len, name, GC_FORCE_REMOUNT_PREFIX,
                   (long)time(NULL), (unsigned)getpid(), suffix);
  return n < 0 || (size_t)n >= out_size ? -1 : 0;
}

static int
wait_for_compressed_shadowmount(const char *title_id, const char *path,
                                const char *nested_name, int nested_type,
                                const char *current,
                                char *err, size_t err_size) {
  char expected_image[1024];
  char expected_mount[1024];
  char scan_err[256] = {0};
  if(expected_compressed_shadow_paths(path, nested_name, nested_type,
                                      expected_image, sizeof(expected_image),
                                      expected_mount, sizeof(expected_mount)) != 0) {
    snprintf(err, err_size, "%s", "could not derive compressed remount path");
    return -1;
  }
  if(gc_cancel_requested(err, err_size)) return -1;
  job_set_phase("mounting", 0, 0, current ? current : "Waiting for remount");
  if(gc_shadowmount_request_title_source_scan_cancelable(
         title_id, path, scan_err, sizeof(scan_err)) != 0) {
    if(job_cancelled()) return -1;
    gc_log("compressed remount scan request failed title=%s err=%s",
           title_id ? title_id : "", scan_err[0] ? scan_err : "unknown");
  }
  return wait_for_shadowmount_links(title_id, expected_mount, expected_image,
                                    err, err_size);
}

static int
wait_for_image_shadowmount(const char *title_id, const char *path,
                           int nested_type, const char *current,
                           char *err, size_t err_size) {
  char expected_mount[1024];
  char scan_err[256] = {0};
  if(!path || !path[0] ||
     shadow_image_mount_point(path, nested_type, expected_mount,
                              sizeof(expected_mount)) != 0) {
    snprintf(err, err_size, "%s", "could not derive image remount path");
    return -1;
  }
  if(gc_cancel_requested(err, err_size)) return -1;
  job_set_phase("mounting", 0, 0, current ? current : "Waiting for remount");
  if(gc_shadowmount_request_title_source_scan_cancelable(
         title_id, path, scan_err, sizeof(scan_err)) != 0) {
    if(job_cancelled()) return -1;
    gc_log("image remount scan request failed title=%s err=%s",
           title_id ? title_id : "", scan_err[0] ? scan_err : "unknown");
  }
  return wait_for_shadowmount_links(title_id, expected_mount, path,
                                    err, err_size);
}

static int
force_compressed_path_bounce_remount(const char *title_id,
                                     const char *original_path,
                                     const char *nested_name,
                                     int nested_type,
                                     char *err, size_t err_size) {
  char temp_path[1024];
  char hint_err[256] = {0};
  char scan_err[256] = {0};
  int cancelled = 0;

  if(gc_cancel_requested(err, err_size)) return -1;
  if(build_force_remount_temp_path(original_path, title_id, temp_path,
                                   sizeof(temp_path)) != 0) {
    snprintf(err, err_size, "%s", "could not build compressed remount temp path");
    return -1;
  }
  if(rename(original_path, temp_path) != 0) {
    snprintf(err, err_size, "rename compressed image for remount: %s",
             strerror(errno));
    return -1;
  }
  gc_log("compressed remount bounce title=%s original=%s temp=%s",
         title_id ? title_id : "", original_path, temp_path);

  if(gc_shadowmount_write_pfsc_hints(temp_path, nested_name, nested_type,
                                     hint_err, sizeof(hint_err)) != 0) {
    gc_log("compressed temp remount hint failed title=%s temp=%s err=%s",
           title_id ? title_id : "", temp_path,
           hint_err[0] ? hint_err : "unknown");
  }
  job_set_phase("mounting", 0, 0, "Refreshing temporary mount state");
  if(gc_shadowmount_request_scan_cancelable(scan_err, sizeof(scan_err)) != 0) {
    if(job_cancelled()) cancelled = 1;
  }
  if(!cancelled && scan_err[0]) {
    gc_log("compressed temp remount scan failed title=%s err=%s",
           title_id ? title_id : "", scan_err[0] ? scan_err : "unknown");
  }
  if(!cancelled &&
     gc_sleep_cancelable_seconds(GC_MOUNT_SCAN_REQUEST_SECONDS,
                                 err, err_size) != 0) {
    cancelled = 1;
  }
  if(rename(temp_path, original_path) != 0) {
    snprintf(err, err_size, "restore compressed image name: %s",
             strerror(errno));
    gc_log("compressed remount restore failed title=%s temp=%s original=%s err=%s",
           title_id ? title_id : "", temp_path,
           original_path ? original_path : "", err && err[0] ? err : "");
    return -1;
  }
  gc_log("compressed remount restore title=%s temp=%s original=%s",
         title_id ? title_id : "", temp_path, original_path);
  hint_err[0] = 0;
  if(gc_shadowmount_remove_outer_sector_hint(temp_path, hint_err,
                                            sizeof(hint_err)) != 0) {
    gc_log("compressed temp remount hint cleanup failed title=%s temp=%s err=%s",
           title_id ? title_id : "", temp_path,
           hint_err[0] ? hint_err : "unknown");
  }
  hint_err[0] = 0;
  if(gc_shadowmount_prepare_pfsc_hints_for_title(title_id, original_path,
                                                nested_name, nested_type,
                                                hint_err,
                                                sizeof(hint_err)) != 0) {
    gc_log("compressed final remount hint failed title=%s path=%s err=%s",
           title_id ? title_id : "", original_path,
           hint_err[0] ? hint_err : "unknown");
  }
  if(cancelled || gc_cancel_requested(err, err_size)) {
    snprintf(err, err_size, "%s", "cancelled");
    return -1;
  }
  return wait_for_compressed_shadowmount(title_id, original_path, nested_name,
                                         nested_type,
                                         "Waiting for final remount",
                                         err, err_size);
}

static int
force_image_path_bounce_remount(const char *title_id,
                                const char *original_path,
                                int nested_type,
                                char *err, size_t err_size) {
  char temp_path[1024];
  char hint_err[256] = {0};
  char scan_err[256] = {0};
  int cancelled = 0;

  if(gc_cancel_requested(err, err_size)) return -1;
  if(build_force_remount_temp_path(original_path, title_id, temp_path,
                                   sizeof(temp_path)) != 0) {
    snprintf(err, err_size, "%s", "could not build image remount temp path");
    return -1;
  }
  if(rename(original_path, temp_path) != 0) {
    snprintf(err, err_size, "rename image for remount: %s", strerror(errno));
    return -1;
  }
  gc_log("image remount bounce title=%s original=%s temp=%s",
         title_id ? title_id : "", original_path, temp_path);

  job_set_phase("mounting", 0, 0, "Refreshing temporary mount state");
  if(gc_shadowmount_request_scan_cancelable(scan_err, sizeof(scan_err)) != 0) {
    if(job_cancelled()) cancelled = 1;
  }
  if(!cancelled && scan_err[0]) {
    gc_log("image temp remount scan failed title=%s err=%s",
           title_id ? title_id : "", scan_err[0] ? scan_err : "unknown");
  }
  if(!cancelled &&
     gc_sleep_cancelable_seconds(GC_MOUNT_SCAN_REQUEST_SECONDS,
                                 err, err_size) != 0) {
    cancelled = 1;
  }
  if(rename(temp_path, original_path) != 0) {
    snprintf(err, err_size, "restore image name: %s", strerror(errno));
    gc_log("image remount restore failed title=%s temp=%s original=%s err=%s",
           title_id ? title_id : "", temp_path,
           original_path ? original_path : "", err && err[0] ? err : "");
    return -1;
  }
  gc_log("image remount restore title=%s temp=%s original=%s",
         title_id ? title_id : "", temp_path, original_path);
  if(gc_shadowmount_prepare_image_hints_for_title(title_id, original_path,
                                                  nested_type, hint_err,
                                                  sizeof(hint_err)) != 0) {
    gc_log("image final remount hint failed title=%s path=%s err=%s",
           title_id ? title_id : "", original_path,
           hint_err[0] ? hint_err : "unknown");
  }
  if(cancelled || gc_cancel_requested(err, err_size)) {
    snprintf(err, err_size, "%s", "cancelled");
    return -1;
  }
  return wait_for_image_shadowmount(title_id, original_path, nested_type,
                                    "Waiting for final remount",
                                    err, err_size);
}

static int
repair_force_path_bounce_remount(const char *title_id,
                                 const char *original_path,
                                 pfs_repair_info_t *info,
                                 char *err, size_t err_size) {
  if(!info) return 0;
  if(force_compressed_path_bounce_remount(title_id, original_path,
                                          info->nested_name,
                                          info->nested_type,
                                          err, err_size) != 0) {
    return -1;
  }
  return post_repair_smoke_verify(title_id, original_path, info, 1,
                                  err, err_size);
}

static int
mount_switch_recovery_append(const char *op_id, const char *title_id,
                             const char *original_path,
                             const char *hidden_path,
                             const char *state) {
  json_buf_t b = {0};
  int fd;
  int rc = -1;

  if(mkdirs(GC_BASE) != 0) return -1;
  if(json_append(&b, "{\"opId\":") != 0 ||
     json_string(&b, op_id ? op_id : "") != 0 ||
     json_append(&b, ",\"titleId\":") != 0 ||
     json_string(&b, title_id ? title_id : "") != 0 ||
     json_append(&b, ",\"originalPath\":") != 0 ||
     json_string(&b, original_path ? original_path : "") != 0 ||
     json_append(&b, ",\"hiddenPath\":") != 0 ||
     json_string(&b, hidden_path ? hidden_path : "") != 0 ||
     json_append(&b, ",\"state\":") != 0 ||
     json_string(&b, state ? state : "") != 0 ||
     json_appendf(&b, ",\"createdAt\":%ld}\n", (long)time(NULL)) != 0) {
    free(b.data);
    return -1;
  }

  fd = open(GC_MOUNT_SWITCH_LOG, O_WRONLY | O_CREAT | O_APPEND, 0666);
  if(fd < 0) {
    free(b.data);
    return -1;
  }
  rc = write_all_fd(fd, b.data, b.len);
  fsync(fd);
  close(fd);
  free(b.data);
  return rc;
}

static int
mount_switch_restore_recovery_log(void) {
  FILE *f = fopen(GC_MOUNT_SWITCH_LOG, "r");
  char line[16384];
  int restored = 0;
  if(!f) return 0;
  while(fgets(line, sizeof(line), f)) {
    char original_path[1024] = {0};
    char hidden_path[1024] = {0};
    struct stat st;
    line[strcspn(line, "\r\n")] = 0;
    json_find_string_value(line, "originalPath", original_path,
                           sizeof(original_path));
    json_find_string_value(line, "hiddenPath", hidden_path,
                           sizeof(hidden_path));
    if(!path_is_safe(original_path) || !path_is_safe(hidden_path)) {
      continue;
    }
    if(lstat(hidden_path, &st) != 0) continue;
    if(lstat(original_path, &st) == 0) {
      gc_log("mount switch recovery kept hidden path because original exists hidden=%s original=%s",
             hidden_path, original_path);
      continue;
    }
    if(errno != ENOENT) {
      gc_log("mount switch recovery skipped hidden=%s original=%s err=%s",
             hidden_path, original_path, strerror(errno));
      continue;
    }
    if(rename(hidden_path, original_path) != 0) {
      gc_log("mount switch recovery restore failed hidden=%s original=%s err=%s",
             hidden_path, original_path, strerror(errno));
      continue;
    }
    restored++;
    gc_log("mount switch recovery restored hidden=%s original=%s",
           hidden_path, original_path);
  }
  fclose(f);
  if(restored > 0) {
    char scan_err[256] = {0};
    artifact_cache_invalidate();
    if(gc_shadowmount_request_scan(scan_err, sizeof(scan_err)) != 0) {
      gc_log("mount switch recovery scan request failed restored=%d err=%s",
             restored, scan_err[0] ? scan_err : "unknown");
    } else {
      gc_log("mount switch recovery requested scan restored=%d", restored);
    }
  }
  return restored;
}

static int
build_mount_switch_hidden_path(const gc_operation_t *op,
                               const char *original_path,
                               char *out, size_t out_size) {
  char parent[1024];
  const char *op_id = op && op->id[0] ? op->id : "op";
  const char *title_id = op && op->title_id[0] ? op->title_id : "UNKNOWN";
  uint32_t hash = fnv1a32_string(original_path);
  if(path_parent(original_path, parent, sizeof(parent)) != 0) return -1;
  for(int attempt = 0; attempt < 100; attempt++) {
    char name[128];
    struct stat st;
    int n = snprintf(name, sizeof(name), "%s%s-%s-%08x-%02d",
                     GC_MOUNT_HIDE_PREFIX, op_id, title_id, hash, attempt);
    if(n < 0 || (size_t)n >= sizeof(name)) return -1;
    join_path(out, out_size, parent, name);
    if(!path_is_safe(out)) return -1;
    if(lstat(out, &st) != 0) {
      return errno == ENOENT ? 0 : -1;
    }
  }
  errno = EEXIST;
  return -1;
}

static int
mount_switch_source_can_be_hidden(const gc_game_t *game) {
  return game &&
      game->source_kind != GC_SOURCE_UNKNOWN &&
      game->source_path[0] &&
      !path_under_root(game->source_path, GC_SHADOW_IMAGE_BASE) &&
      !path_is_system_app_path(game->source_path) &&
      path_exists_for_source_kind(game->source_path, game->source_kind);
}

static int
mount_switch_hide_competitors(gc_operation_t *op, const gc_game_t *selected,
                              gc_hidden_instance_t *hidden,
                              size_t max_hidden, size_t *hidden_count,
                              char *err, size_t err_size) {
  gc_game_t *games = calloc(GC_MAX_GAMES, sizeof(*games));
  gc_scan_roots_t *roots = calloc(1, sizeof(*roots));
  gc_source_roots_t *source_roots = calloc(1, sizeof(*source_roots));
  size_t count = 0;
  size_t hidden_used = 0;
  int broad_scan = selected && selected->source_kind != GC_SOURCE_IMAGE;
  int rc = -1;

  if(hidden_count) *hidden_count = 0;
  if(!games || !roots || !source_roots) {
    snprintf(err, err_size, "%s", "out of memory");
    goto done;
  }
  if(gc_cancel_requested(err, err_size)) goto done;
  if(broad_scan) {
    shadow_source_roots_build(source_roots);
    for(size_t i = 0; i < source_roots->count; i++) {
      scan_roots_add(roots, source_roots->roots[i]);
    }
    for(size_t i = 0; i < GC_STORAGE_TARGET_COUNT; i++) {
      scan_roots_add(roots, GC_STORAGE_TARGETS[i].root);
      scan_roots_add(roots, GC_STORAGE_TARGETS[i].target_root);
    }
  }
  scan_roots_add_parent(roots, selected->source_path);
  scan_roots_add_parent(roots, selected->output_path);
  if(op && op->source_path[0] &&
     !paths_equal_ignoring_trailing_slash(op->source_path,
                                          selected->source_path)) {
    gc_game_t known;
    memset(&known, 0, sizeof(known));
    if(candidate_game_from_path(op->source_path, base_name(op->source_path),
                                &known, 0, 0) == 0 &&
       !strcmp(known.title_id, selected->title_id)) {
      scan_roots_add_parent(roots, known.source_path);
      append_game_unique_by_source(games, GC_MAX_GAMES, &count, &known);
    }
  }
  if(broad_scan) {
    char link_target[1024];
    if(read_title_link(selected->title_id, "mount.lnk", link_target,
                       sizeof(link_target)) == 0) {
      scan_roots_add_parent(roots, link_target);
      gc_game_t mounted;
      memset(&mounted, 0, sizeof(mounted));
      snprintf(mounted.title_id, sizeof(mounted.title_id), "%s",
               selected->title_id);
      snprintf(mounted.mount_path, sizeof(mounted.mount_path), "%s",
               link_target);
      (void)read_title_link(selected->title_id, "mount_img.lnk",
                            mounted.image_path, sizeof(mounted.image_path));
      set_game_mount_status(&mounted, 1, "mounted");
      detect_game_source_ex(&mounted, 0, 1);
      if(mounted.source_kind != GC_SOURCE_UNKNOWN &&
         !game_uses_system_app_path(&mounted) &&
         game_source_still_exists(&mounted)) {
        append_game_unique_by_source(games, GC_MAX_GAMES, &count, &mounted);
      }
    }
  }
  gc_log("mount switch scan title=%s selected=%s kind=%s broad=%d roots=%llu candidates=%llu",
         selected->title_id, selected->source_path,
         source_kind_name(selected->source_kind), broad_scan,
         (unsigned long long)roots->count, (unsigned long long)count);
  for(size_t i = 0; i < roots->count && count < GC_MAX_GAMES; i++) {
    char path[1024];
    gc_game_t candidate;
    int n = snprintf(path, sizeof(path), "%s/%s.ffpfsc",
                     roots->roots[i], selected->title_id);
    if(n >= 0 && (size_t)n < sizeof(path) &&
       candidate_game_from_path(path, base_name(path), &candidate, 0, 0) == 0 &&
       !strcmp(candidate.title_id, selected->title_id)) {
      append_game_unique_by_source(games, GC_MAX_GAMES, &count, &candidate);
    }
    n = snprintf(path, sizeof(path), "%s/%s", roots->roots[i],
                 selected->title_id);
    if(n >= 0 && (size_t)n < sizeof(path) &&
       candidate_game_from_path(path, base_name(path), &candidate, 0, 0) == 0 &&
       !strcmp(candidate.title_id, selected->title_id)) {
      append_game_unique_by_source(games, GC_MAX_GAMES, &count, &candidate);
    }
    n = snprintf(path, sizeof(path), "%s/%s-app", roots->roots[i],
                 selected->title_id);
    if(n >= 0 && (size_t)n < sizeof(path) &&
       candidate_game_from_path(path, base_name(path), &candidate, 0, 0) == 0 &&
       !strcmp(candidate.title_id, selected->title_id)) {
      append_game_unique_by_source(games, GC_MAX_GAMES, &count, &candidate);
    }
  }
  if(gc_cancel_requested(err, err_size)) goto done;
  gc_log("mount switch candidates title=%s count=%llu",
         selected->title_id, (unsigned long long)count);

  for(size_t i = 0; i < count; i++) {
    gc_game_t *g = &games[i];
    gc_hidden_instance_t *entry;
    if(gc_cancel_requested(err, err_size)) goto done;
    if(strcmp(g->title_id, selected->title_id)) continue;
    if(game_source_matches(g, selected->source_path)) continue;
    if(!mount_switch_source_can_be_hidden(g)) continue;
    if(hidden_used >= max_hidden) {
      snprintf(err, err_size, "%s", "too many duplicate instances to hide");
      goto done;
    }

    entry = &hidden[hidden_used];
    memset(entry, 0, sizeof(*entry));
    snprintf(entry->original_path, sizeof(entry->original_path), "%s",
             g->source_path);
    entry->source_kind = g->source_kind;
    if(build_mount_switch_hidden_path(op, entry->original_path,
                                      entry->hidden_path,
                                      sizeof(entry->hidden_path)) != 0) {
      snprintf(err, err_size, "build hidden path: %s", strerror(errno));
      goto done;
    }
    if(mount_switch_recovery_append(op->id, op->title_id,
                                    entry->original_path,
                                    entry->hidden_path, "planned") != 0) {
      snprintf(err, err_size, "%s", "write mount switch recovery record");
      goto done;
    }
    if(rename(entry->original_path, entry->hidden_path) != 0) {
      snprintf(err, err_size, "hide duplicate instance: %s", strerror(errno));
      goto done;
    }
    entry->hidden = 1;
    hidden_used++;
    gc_size_cache_forget(entry->original_path);
    gc_log("mount switch hidden title=%s original=%s hidden=%s",
           op->title_id, entry->original_path, entry->hidden_path);
    if(gc_cancel_requested(err, err_size)) goto done;
  }

  rc = 0;
done:
  if(hidden_count) *hidden_count = hidden_used;
  free(source_roots);
  free(roots);
  free(games);
  return rc;
}

static int
mount_switch_restore_hidden(gc_operation_t *op, gc_hidden_instance_t *hidden,
                            size_t hidden_count, char *err, size_t err_size) {
  int rc = 0;
  for(size_t i = hidden_count; i > 0; i--) {
    gc_hidden_instance_t *entry = &hidden[i - 1];
    struct stat st;
    if(!entry->hidden) continue;
    if(lstat(entry->hidden_path, &st) != 0) {
      if(lstat(entry->original_path, &st) == 0) {
        entry->hidden = 0;
        continue;
      }
      if(rc == 0) {
        snprintf(err, err_size, "restore duplicate instance: %s",
                 strerror(errno));
      }
      rc = -1;
      continue;
    }
    if(lstat(entry->original_path, &st) == 0) {
      if(rc == 0) {
        snprintf(err, err_size,
                 "restore duplicate instance: original path already exists");
      }
      rc = -1;
      continue;
    }
    if(errno != ENOENT) {
      if(rc == 0) {
        snprintf(err, err_size, "restore duplicate instance: %s",
                 strerror(errno));
      }
      rc = -1;
      continue;
    }
    if(rename(entry->hidden_path, entry->original_path) != 0) {
      if(rc == 0) {
        snprintf(err, err_size, "restore duplicate instance: %s",
                 strerror(errno));
      }
      rc = -1;
      continue;
    }
    entry->hidden = 0;
    (void)mount_switch_recovery_append(op->id, op->title_id,
                                       entry->original_path,
                                       entry->hidden_path, "restored");
    gc_log("mount switch restored title=%s original=%s hidden=%s",
           op->title_id, entry->original_path, entry->hidden_path);
  }
  return rc;
}

static int
prepare_shadowmount_for_selected_source(const gc_game_t *game,
                                        char *err, size_t err_size) {
  char hint_err[256] = {0};
  if(!game) return -1;
  if(game->source_kind == GC_SOURCE_COMPRESSED) {
    pfs_decompress_info_t dec = {0};
    if(pfs_decompress_detect_nested(game->source_path, &dec, err,
                                    err_size) != 0) {
      return -1;
    }
    if(gc_shadowmount_prepare_pfsc_hints_for_title(game->title_id,
                                                   game->source_path,
                                                   dec.nested_name,
                                                   dec.nested_type,
                                                   hint_err,
                                                   sizeof(hint_err)) != 0) {
      gc_log("mount switch compressed hint failed title=%s path=%s err=%s",
             game->title_id, game->source_path,
             hint_err[0] ? hint_err : "unknown");
    }
    return 0;
  }
  if(game->source_kind == GC_SOURCE_IMAGE) {
    pfs_app_info_t image = {0};
    if(pfs_image_probe(game->source_path, &image, err, err_size) != 0) {
      return -1;
    }
    if(gc_shadowmount_prepare_image_hints_for_title(game->title_id,
                                                    game->source_path,
                                                    image.nested_type,
                                                    hint_err,
                                                    sizeof(hint_err)) != 0) {
      gc_log("mount switch image hint failed title=%s path=%s err=%s",
             game->title_id, game->source_path,
             hint_err[0] ? hint_err : "unknown");
    }
  }
  return 0;
}

static int
build_move_target_path(const char *target_root, const char *source_path,
                       char *out, size_t out_size,
                       char *err, size_t err_size) {
  const char *name = path_basename(source_path);
  if(!target_root || !target_root[0] || !name || !upload_segment_safe(name)) {
    snprintf(err, err_size, "%s", "bad move target");
    errno = EINVAL;
    return -1;
  }
  if(!path_is_shadowmount_game_root(target_root)) {
    snprintf(err, err_size, "%s",
             "target folder is not a ShadowMountPlus game folder");
    errno = EINVAL;
    return -1;
  }
  int n = snprintf(out, out_size, "%s/%s", target_root, name);
  if(n < 0 || (size_t)n >= out_size) {
    snprintf(err, err_size, "%s", "move target path too long");
    errno = ENAMETOOLONG;
    return -1;
  }
  return 0;
}

static int
build_compress_target_path(const char *target_root, const gc_game_t *game,
                           char *out, size_t out_size,
                           char *err, size_t err_size) {
  const char *name = NULL;
  if(!target_root || !target_root[0] || !game || !out || out_size == 0) {
    snprintf(err, err_size, "%s", "bad compression target");
    errno = EINVAL;
    return -1;
  }
  if(!path_is_shadowmount_game_root(target_root)) {
    snprintf(err, err_size, "%s",
             "compression target is not a ShadowMountPlus game folder");
    errno = EINVAL;
    return -1;
  }
  if(game->source_kind == GC_SOURCE_FOLDER) {
    char file_name[96];
    int n = snprintf(file_name, sizeof(file_name), "%s.ffpfsc",
                     game->title_id);
    if(n < 0 || (size_t)n >= sizeof(file_name)) {
      snprintf(err, err_size, "%s", "compression target path too long");
      errno = ENAMETOOLONG;
      return -1;
    }
    name = file_name;
    n = snprintf(out, out_size, "%s/%s", target_root, name);
    if(n < 0 || (size_t)n >= out_size) {
      snprintf(err, err_size, "%s", "compression target path too long");
      errno = ENAMETOOLONG;
      return -1;
    }
    return 0;
  }
  if(game->source_kind == GC_SOURCE_IMAGE) {
    name = path_basename(game->output_path);
    if(!name || !upload_segment_safe(name) || !ends_with_ci(name, ".ffpfsc")) {
      snprintf(err, err_size, "%s", "bad compression target name");
      errno = EINVAL;
      return -1;
    }
    int n = snprintf(out, out_size, "%s/%s", target_root, name);
    if(n < 0 || (size_t)n >= out_size) {
      snprintf(err, err_size, "%s", "compression target path too long");
      errno = ENAMETOOLONG;
      return -1;
    }
    return 0;
  }
  snprintf(err, err_size, "%s", "game is not compressible");
  errno = EINVAL;
  return -1;
}

static int
build_make_image_target_path(const char *target_root, const gc_game_t *game,
                             int format, char *out, size_t out_size,
                             char *err, size_t err_size) {
  const char *ext = format == PFS_COMPRESS_FORMAT_EXFAT ? ".exfat" : ".ffpfs";
  char root[1024];
  if(!game || !game->title_id[0] || !out || out_size == 0) {
    snprintf(err, err_size, "%s", "bad image target");
    errno = EINVAL;
    return -1;
  }
  if(game->source_kind != GC_SOURCE_FOLDER) {
    snprintf(err, err_size, "%s", "only app folders can be made into images");
    errno = EINVAL;
    return -1;
  }
  if(target_root && target_root[0]) {
    if(!path_is_shadowmount_game_root(target_root)) {
      snprintf(err, err_size, "%s",
               "image target is not a ShadowMountPlus game folder");
      errno = EINVAL;
      return -1;
    }
    snprintf(root, sizeof(root), "%s", target_root);
  } else if(path_parent(game->source_path, root, sizeof(root)) != 0 ||
            !root[0]) {
    snprintf(err, err_size, "%s", "bad image target folder");
    errno = EINVAL;
    return -1;
  }
  int n = snprintf(out, out_size, "%s/%s%s", root, game->title_id, ext);
  if(n < 0 || (size_t)n >= out_size) {
    snprintf(err, err_size, "%s", "image target path too long");
    errno = ENAMETOOLONG;
    return -1;
  }
  return 0;
}

static int
build_compress_stage_path(const gc_operation_t *op, const gc_game_t *game,
                          char *out, size_t out_size,
                          char *err, size_t err_size) {
  const char *name = NULL;
  char root[1024];
  if(!op || !game || !out || out_size == 0) {
    snprintf(err, err_size, "%s", "bad compression stage path");
    errno = EINVAL;
    return -1;
  }
  name = path_basename(game->output_path);
  if(!name || !upload_segment_safe(name) || !ends_with_ci(name, ".ffpfsc")) {
    snprintf(err, err_size, "%s", "bad compression stage name");
    errno = EINVAL;
    return -1;
  }
  if(path_under_root(game->source_path, "/data")) {
    snprintf(root, sizeof(root), "%s", GC_INTERNAL_GAME_ROOT);
  } else {
    snprintf(root, sizeof(root), "%s", "/data/GameCompressor/staging");
    if(mkdirs(root) != 0) {
      snprintf(err, err_size, "create compression staging root: %s",
               strerror(errno));
      return -1;
    }
  }
  int n = snprintf(out, out_size, "%s/.%s.%s.stage.ffpfsc",
                   root, game->title_id, op->id);
  if(n < 0 || (size_t)n >= out_size) {
    snprintf(err, err_size, "%s", "compression stage path too long");
    errno = ENAMETOOLONG;
    return -1;
  }
  return 0;
}

static int
publish_staged_compress_output(gc_operation_t *op,
                               const char *stage_path,
                               const char *final_path,
                               char *err,
                               size_t err_size) {
  char stage_vhash[1024];
  char final_vhash[1024];
  char final_tmp[1024];
  char final_vhash_tmp[1024];
  struct stat st;
  struct stat vh_st;
  uint64_t copied = 0;

  if(!op || !stage_path || !stage_path[0] ||
     !final_path || !final_path[0]) {
    snprintf(err, err_size, "%s", "bad staged compression publish");
    errno = EINVAL;
    return -1;
  }
  if(stat(stage_path, &st) != 0 || !S_ISREG(st.st_mode)) {
    snprintf(err, err_size, "stat staged output: %s", strerror(errno));
    return -1;
  }
  if(pfs_vhash_sidecar_path(stage_path, stage_vhash, sizeof(stage_vhash)) != 0 ||
     pfs_vhash_sidecar_path(final_path, final_vhash, sizeof(final_vhash)) != 0) {
    snprintf(err, err_size, "%s", "validation hash path too long");
    errno = ENAMETOOLONG;
    return -1;
  }
  if(stat(stage_vhash, &vh_st) != 0 || !S_ISREG(vh_st.st_mode)) {
    snprintf(err, err_size, "stat staged validation hash: %s",
             strerror(errno));
    return -1;
  }
  if(legacy_suffix_temp_path(final_path, "publishing",
                             final_tmp, sizeof(final_tmp)) != 0 ||
     legacy_suffix_temp_path(final_vhash, "publishing",
                             final_vhash_tmp, sizeof(final_vhash_tmp)) != 0) {
    snprintf(err, err_size, "%s", "publish temp path too long");
    errno = ENAMETOOLONG;
    return -1;
  }
  unlink(final_tmp);
  unlink(final_vhash_tmp);
  append_operation_phase(op, "copying");
  job_set_phase("copying", 0, 0, "Publishing compressed image");
  job_set_current(path_basename(final_path));
  atomic_store(&g_job.total_bytes, st.st_size > LONG_MAX ? LONG_MAX :
               (long)st.st_size);
  atomic_store(&g_job.copied_bytes, 0);
  if(copy_regular_file_gc(stage_path, final_tmp, &st, &copied,
                          err, err_size) != 0) {
    unlink(final_tmp);
    return -1;
  }
  job_set_current("Publishing validation hash");
  if(copy_regular_file_gc(stage_vhash, final_vhash_tmp, &vh_st, NULL,
                          err, err_size) != 0) {
    unlink(final_tmp);
    unlink(final_vhash_tmp);
    return -1;
  }
  job_set_current("Finalizing USB compressed image");
  if(rename(final_tmp, final_path) != 0) {
    snprintf(err, err_size, "rename published output: %s", strerror(errno));
    unlink(final_tmp);
    unlink(final_vhash_tmp);
    return -1;
  }
  fsync_parent_dir_best_effort(final_path);
  if(rename(final_vhash_tmp, final_vhash) != 0) {
    snprintf(err, err_size, "rename published validation hash: %s",
             strerror(errno));
    unlink(final_path);
    unlink(final_vhash_tmp);
    return -1;
  }
  fsync_parent_dir_best_effort(final_vhash);
  unlink(stage_path);
  unlink(stage_vhash);
  return 0;
}

static int
build_uncompress_target_path(const char *target_root, const gc_game_t *game,
                             char *out, size_t out_size,
                             char *err, size_t err_size) {
  const char *name = NULL;
  if(!target_root || !target_root[0] || !game || !out || out_size == 0) {
    snprintf(err, err_size, "%s", "bad uncompress target");
    errno = EINVAL;
    return -1;
  }
  if(!path_is_shadowmount_game_root(target_root)) {
    snprintf(err, err_size, "%s",
             "uncompress target is not a ShadowMountPlus game folder");
    errno = EINVAL;
    return -1;
  }
  name = path_basename(game->output_path);
  if(!name || !upload_segment_safe(name)) {
    snprintf(err, err_size, "%s", "bad uncompress target name");
    errno = EINVAL;
    return -1;
  }
  int n = snprintf(out, out_size, "%s/%s", target_root, name);
  if(n < 0 || (size_t)n >= out_size) {
    snprintf(err, err_size, "%s", "uncompress target path too long");
    errno = ENAMETOOLONG;
    return -1;
  }
  return 0;
}

static int
build_extract_image_target_path(const gc_game_t *game,
                                char *out, size_t out_size,
                                char *err, size_t err_size) {
  char parent[1024];
  if(!game || !game->source_path[0] || !game->title_id[0] ||
     !out || out_size == 0) {
    snprintf(err, err_size, "%s", "bad extract target");
    errno = EINVAL;
    return -1;
  }
  if(path_parent(game->source_path, parent, sizeof(parent)) != 0) {
    snprintf(err, err_size, "%s", "bad image path");
    return -1;
  }
  if(!path_is_shadowmount_game_root(parent)) {
    snprintf(err, err_size, "%s",
             "extract target is not a ShadowMountPlus game folder");
    errno = EINVAL;
    return -1;
  }
  if(!upload_segment_safe(game->title_id)) {
    snprintf(err, err_size, "%s", "bad title folder name");
    errno = EINVAL;
    return -1;
  }
  int n = snprintf(out, out_size, "%s/%s", parent, game->title_id);
  if(n < 0 || (size_t)n >= out_size) {
    snprintf(err, err_size, "%s", "extract target path too long");
    errno = ENAMETOOLONG;
    return -1;
  }
  return 0;
}

static int
move_remount_expectations(const gc_game_t *game, const char *target_path,
                          char *expected_mount, size_t expected_mount_size,
                          char *expected_image, size_t expected_image_size,
                          char *err, size_t err_size) {
  expected_mount[0] = 0;
  expected_image[0] = 0;
  if(game->source_kind == GC_SOURCE_FOLDER) {
    snprintf(expected_mount, expected_mount_size, "%s", target_path);
    return 0;
  }
  if(game->source_kind == GC_SOURCE_COMPRESSED) {
    pfs_decompress_info_t dec = {0};
    if(pfs_decompress_detect_nested(target_path, &dec, err, err_size) != 0) {
      return -1;
    }
    return expected_compressed_shadow_paths(target_path, dec.nested_name,
                                            dec.nested_type,
                                            expected_image,
                                            expected_image_size,
                                            expected_mount,
                                            expected_mount_size);
  }
  if(game->source_kind == GC_SOURCE_IMAGE) {
    pfs_app_info_t image = {0};
    if(pfs_image_probe(target_path, &image, err, err_size) != 0) {
      return -1;
    }
    snprintf(expected_image, expected_image_size, "%s", target_path);
    if(shadow_image_mount_point(target_path, image.nested_type,
                                expected_mount,
                                expected_mount_size) != 0) {
      snprintf(err, err_size, "%s", "could not derive moved image mount path");
      return -1;
    }
    return 0;
  }
  snprintf(err, err_size, "%s", "game source is not movable");
  errno = EINVAL;
  return -1;
}

static int
write_title_link_file(const char *path, const char *value,
                      char *err, size_t err_size) {
  int fd;
  size_t len;

  if(!path || !path[0] || !value || !value[0]) {
    snprintf(err, err_size, "%s", "bad mount link restore");
    errno = EINVAL;
    return -1;
  }

  fd = open(path, O_WRONLY | O_CREAT | O_TRUNC, 0666);
  if(fd < 0) {
    snprintf(err, err_size, "open mount link: %s", strerror(errno));
    return -1;
  }

  len = strlen(value);
  if(write_all_fd(fd, value, len) != 0 ||
     write_all_fd(fd, "\n", 1) != 0) {
    snprintf(err, err_size, "write mount link: %s", strerror(errno));
    close(fd);
    return -1;
  }
  if(fsync(fd) != 0) {
    snprintf(err, err_size, "flush mount link: %s", strerror(errno));
    close(fd);
    return -1;
  }
  if(close(fd) != 0) {
    snprintf(err, err_size, "close mount link: %s", strerror(errno));
    return -1;
  }
  chmod(path, 0666);
  fsync_parent_dir_best_effort(path);
  return 0;
}

static int
mount_switch_clear_stale_links(const char *title_id,
                               const char *expected_mount,
                               const char *expected_image,
                               gc_mount_link_backup_t *backup,
                               char *err, size_t err_size) {
  int mount_matches;
  int image_matches;

  if(backup) memset(backup, 0, sizeof(*backup));
  if(!valid_title_id(title_id) || !expected_mount || !expected_mount[0] ||
     !backup) {
    snprintf(err, err_size, "%s", "bad mount switch link state");
    errno = EINVAL;
    return -1;
  }

  mount_link_path_for_title(title_id, "mount.lnk",
                            backup->mount_link_path,
                            sizeof(backup->mount_link_path));
  mount_link_path_for_title(title_id, "mount_img.lnk",
                            backup->image_link_path,
                            sizeof(backup->image_link_path));
  backup->had_mount =
      read_link_file(backup->mount_link_path, backup->mount_value,
                     sizeof(backup->mount_value)) == 0;
  backup->had_image =
      read_link_file(backup->image_link_path, backup->image_value,
                     sizeof(backup->image_value)) == 0;

  if(!backup->had_mount) return 0;

  mount_matches =
      paths_equal_ignoring_trailing_slash(backup->mount_value,
                                          expected_mount);
  image_matches = expected_image && expected_image[0]
      ? (backup->had_image &&
         paths_equal_ignoring_trailing_slash(backup->image_value,
                                             expected_image))
      : !backup->had_image;
  if(mount_matches && image_matches) return 0;

  if(unlink(backup->mount_link_path) != 0 && errno != ENOENT) {
    snprintf(err, err_size, "remove stale mount link: %s", strerror(errno));
    return -1;
  }
  if(unlink(backup->image_link_path) != 0 && errno != ENOENT) {
    snprintf(err, err_size, "remove stale image mount link: %s",
             strerror(errno));
    return -1;
  }
  backup->cleared = 1;
  fsync_parent_dir_best_effort(backup->mount_link_path);
  gc_log("mount switch cleared stale links title=%s oldMount=%s oldImage=%s "
         "expectedMount=%s expectedImage=%s",
         title_id, backup->mount_value,
         backup->had_image ? backup->image_value : "",
         expected_mount, expected_image ? expected_image : "");
  return 0;
}

static int
mount_switch_clear_live_title_mount(const char *title_id,
                                    char *err, size_t err_size) {
  char mountpoint[1024];
  int rc;
  int saved_errno;

  if(!valid_title_id(title_id)) {
    snprintf(err, err_size, "%s", "bad title mount state");
    errno = EINVAL;
    return -1;
  }

  int n = snprintf(mountpoint, sizeof(mountpoint), "%s/%s",
                   GC_SYSTEM_APP_BASE, title_id);
  if(n < 0 || (size_t)n >= sizeof(mountpoint)) {
    snprintf(err, err_size, "%s", "title mount path too long");
    errno = ENAMETOOLONG;
    return -1;
  }

  rc = unmount(mountpoint, 0);
  if(rc == 0) {
    gc_log("mount switch unmounted live title mount title=%s path=%s",
           title_id, mountpoint);
    return 0;
  }
  saved_errno = errno;
  rc = unmount(mountpoint, MNT_FORCE);
  if(rc == 0) {
    gc_log("mount switch force-unmounted live title mount title=%s path=%s",
           title_id, mountpoint);
    return 0;
  }

  if(errno == ENOENT || errno == EINVAL || saved_errno == ENOENT ||
     saved_errno == EINVAL) {
    return 0;
  }

  snprintf(err, err_size, "unmount live title mount: %s", strerror(errno));
  return -1;
}

static int
mount_switch_restore_cleared_links(gc_mount_link_backup_t *backup,
                                   char *err, size_t err_size) {
  char title_dir[1024];

  if(!backup || !backup->cleared) return 0;
  if(path_parent(backup->mount_link_path, title_dir, sizeof(title_dir)) != 0 ||
     mkdirs(title_dir) != 0) {
    snprintf(err, err_size, "restore mount link dir: %s", strerror(errno));
    return -1;
  }

  if(backup->had_mount &&
     write_title_link_file(backup->mount_link_path, backup->mount_value,
                           err, err_size) != 0) {
    return -1;
  }
  if(!backup->had_mount &&
     unlink(backup->mount_link_path) != 0 && errno != ENOENT) {
    snprintf(err, err_size, "restore absent mount link: %s", strerror(errno));
    return -1;
  }
  if(backup->had_image &&
     write_title_link_file(backup->image_link_path, backup->image_value,
                           err, err_size) != 0) {
    return -1;
  }
  if(!backup->had_image &&
     unlink(backup->image_link_path) != 0 && errno != ENOENT) {
    snprintf(err, err_size, "restore absent image mount link: %s",
             strerror(errno));
    return -1;
  }
  backup->cleared = 0;
  gc_log("mount switch restored previous links mount=%s image=%s",
         backup->mount_value, backup->had_image ? backup->image_value : "");
  return 0;
}

static void
init_compressed_output_game_for_mount(const gc_operation_t *op,
                                      const gc_game_t *source_game,
                                      const pfs_app_info_t *info,
                                      gc_game_t *out) {
  pfs_decompress_info_t dec = {0};
  char err[256] = {0};
  if(!out) return;
  memset(out, 0, sizeof(*out));
  snprintf(out->title_id, sizeof(out->title_id), "%s",
           op && op->title_id[0] ? op->title_id :
           (info && info->title_id[0] ? info->title_id :
            (source_game ? source_game->title_id : "")));
  snprintf(out->name, sizeof(out->name), "%s",
           source_game && source_game->name[0] ? source_game->name :
           (out->title_id[0] ? out->title_id : "Compressed game"));
  if(info && info->output_path[0]) {
    snprintf(out->image_path, sizeof(out->image_path), "%s", info->output_path);
    snprintf(out->source_path, sizeof(out->source_path), "%s", info->output_path);
  }
  out->source_kind = GC_SOURCE_COMPRESSED;
  if(info) {
    snprintf(out->nested_name, sizeof(out->nested_name), "%s",
             info->nested_name);
    out->nested_type = info->nested_type;
  }
  if(out->source_path[0] &&
     pfs_decompress_detect_nested(out->source_path, &dec, err,
                                  sizeof(err)) == 0) {
    snprintf(out->output_path, sizeof(out->output_path), "%s",
             dec.output_path);
    snprintf(out->nested_name, sizeof(out->nested_name), "%s",
             dec.nested_name);
    out->nested_type = dec.nested_type;
    out->output_exists = dec.output_exists;
  }
  out->source_size = source_size_bytes_exact(out->source_path,
                                             out->source_kind);
  if(out->output_path[0]) {
    (void)free_bytes_for_output(out->output_path, &out->free_bytes);
  } else if(out->source_path[0]) {
    (void)free_bytes_for_output(out->source_path, &out->free_bytes);
  }
  set_game_mount_status(out, 0, "not-mounted");
  snprintf(out->primary_action, sizeof(out->primary_action), "%s",
           "Validate and Repair");
}

static void
init_image_output_game_for_mount(const gc_operation_t *op,
                                 const gc_game_t *source_game,
                                 const pfs_app_info_t *info,
                                 gc_game_t *out) {
  if(!out) return;
  memset(out, 0, sizeof(*out));
  snprintf(out->title_id, sizeof(out->title_id), "%s",
           op && op->title_id[0] ? op->title_id :
           (info && info->title_id[0] ? info->title_id :
            (source_game ? source_game->title_id : "")));
  snprintf(out->name, sizeof(out->name), "%s",
           source_game && source_game->name[0] ? source_game->name :
           (out->title_id[0] ? out->title_id : "Image game"));
  if(info && info->output_path[0]) {
    snprintf(out->image_path, sizeof(out->image_path), "%s", info->output_path);
    snprintf(out->source_path, sizeof(out->source_path), "%s", info->output_path);
    snprintf(out->output_path, sizeof(out->output_path), "%s", info->output_path);
  }
  out->source_kind = GC_SOURCE_IMAGE;
  if(info) {
    snprintf(out->nested_name, sizeof(out->nested_name), "%s",
             info->nested_name);
    out->nested_type = info->nested_type;
  }
  out->source_size = source_size_bytes_exact(out->source_path,
                                             out->source_kind);
  if(out->source_path[0]) {
    (void)free_bytes_for_output(out->source_path, &out->free_bytes);
  }
  set_game_mount_status(out, 0, "not-mounted");
  snprintf(out->primary_action, sizeof(out->primary_action), "%s", "Compress");
}

static int
mount_selected_instance_hidden_exclusive(gc_operation_t *op,
                                         const gc_game_t *selected,
                                         gc_hidden_instance_t *hidden,
                                         size_t max_hidden,
                                         size_t *hidden_count,
                                         int *mount_missed,
                                         char *err, size_t err_size) {
  char expected_mount[1024] = {0};
  char expected_image[1024] = {0};
  char scan_err[256] = {0};
  gc_mount_link_backup_t link_backup;

  memset(&link_backup, 0, sizeof(link_backup));
  if(hidden_count) *hidden_count = 0;
  if(mount_missed) *mount_missed = 0;
  if(!op || !selected || !selected->source_path[0]) {
    snprintf(err, err_size, "%s", "selected mount source is unavailable");
    errno = EINVAL;
    return -1;
  }
  if(gc_cancel_requested(err, err_size)) return -1;

  if(move_remount_expectations(selected, selected->source_path,
                               expected_mount, sizeof(expected_mount),
                               expected_image, sizeof(expected_image),
                               err, err_size) != 0) {
    return -1;
  }
  if(prepare_shadowmount_for_selected_source(selected, err, err_size) != 0) {
    return -1;
  }
  if(gc_cancel_requested(err, err_size)) return -1;

  gc_checkpoint("mount selected hide competitors");
  append_operation_phase(op, "hiding");
  job_set_phase("hiding", 0, 0, "Hiding other instances");
  artifact_cache_invalidate();
  if(mount_switch_hide_competitors(op, selected, hidden, max_hidden,
                                   hidden_count, err, err_size) != 0) {
    return -1;
  }
  artifact_cache_invalidate();
  if(gc_cancel_requested(err, err_size)) return -1;
  if(mount_switch_clear_stale_links(op->title_id, expected_mount,
                                    expected_image, &link_backup,
                                    err, err_size) != 0) {
    return -1;
  }
  if(mount_switch_clear_live_title_mount(op->title_id, err, err_size) != 0) {
    (void)mount_switch_restore_cleared_links(&link_backup, scan_err,
                                             sizeof(scan_err));
    return -1;
  }

  gc_checkpoint("mount selected source scan");
  append_operation_phase(op, "mounting");
  job_set_phase("mounting", 0, 0, "Mounting");
  if(gc_shadowmount_request_title_source_scan_cancelable(
         selected->title_id, selected->source_path, scan_err,
         sizeof(scan_err)) != 0) {
    snprintf(err, err_size, "%s",
             scan_err[0] ? scan_err : "could not request ShadowMount scan");
    (void)mount_switch_restore_cleared_links(&link_backup, scan_err,
                                             sizeof(scan_err));
    return -1;
  }
  if(gc_cancel_requested(err, err_size)) {
    (void)mount_switch_restore_cleared_links(&link_backup, scan_err,
                                             sizeof(scan_err));
    return -1;
  }

  err[0] = 0;
  if(wait_for_shadowmount_links(op->title_id, expected_mount, expected_image,
                                err, err_size) == 0) {
    return 0;
  }
  if(shadowmount_mount_missed(err)) {
    (void)mount_switch_restore_cleared_links(&link_backup, scan_err,
                                             sizeof(scan_err));
    if(mount_missed) *mount_missed = 1;
    return 0;
  }
  (void)mount_switch_restore_cleared_links(&link_backup, scan_err,
                                           sizeof(scan_err));
  return -1;
}

static int
update_ampr_remount_source(gc_operation_t *op, const gc_game_t *game,
                           char *expected_mount, size_t expected_mount_size,
                           char *err, size_t err_size) {
  char expected_image[1024] = {0};
  char scan_err[256] = {0};
  gc_mount_link_backup_t link_backup;

  if(!op || !game || !game->source_path[0]) {
    snprintf(err, err_size, "%s", "AMPR source is unavailable");
    errno = EINVAL;
    return -1;
  }
  if(expected_mount && expected_mount_size) expected_mount[0] = 0;
  memset(&link_backup, 0, sizeof(link_backup));
  if(move_remount_expectations(game, game->source_path,
                               expected_mount, expected_mount_size,
                               expected_image, sizeof(expected_image),
                               err, err_size) != 0) {
    return -1;
  }
  if(prepare_shadowmount_for_selected_source(game, err, err_size) != 0) {
    return -1;
  }
  if(gc_cancel_requested(err, err_size)) return -1;
  artifact_cache_invalidate();
  if(mount_switch_clear_stale_links(op->title_id, expected_mount,
                                    expected_image, &link_backup,
                                    err, err_size) != 0) {
    return -1;
  }
  if(mount_switch_clear_live_title_mount(op->title_id, err, err_size) != 0) {
    (void)mount_switch_restore_cleared_links(&link_backup, scan_err,
                                             sizeof(scan_err));
    return -1;
  }

  if(game->source_kind == GC_SOURCE_COMPRESSED) {
    pfs_decompress_info_t dec = {0};
    if(pfs_decompress_detect_nested(game->source_path, &dec,
                                    err, err_size) != 0) {
      (void)mount_switch_restore_cleared_links(&link_backup, scan_err,
                                               sizeof(scan_err));
      return -1;
    }
    if(force_compressed_path_bounce_remount(op->title_id, game->source_path,
                                            dec.nested_name, dec.nested_type,
                                            err, err_size) != 0) {
      (void)mount_switch_restore_cleared_links(&link_backup, scan_err,
                                               sizeof(scan_err));
      return -1;
    }
  } else if(game->source_kind == GC_SOURCE_IMAGE) {
    if(force_image_path_bounce_remount(op->title_id, game->source_path,
                                       game->nested_type,
                                       err, err_size) != 0) {
      (void)mount_switch_restore_cleared_links(&link_backup, scan_err,
                                               sizeof(scan_err));
      return -1;
    }
  } else {
    gc_checkpoint("update-ampr mount selected source");
    job_set_phase("mounting", 0, 0, "Mounting APR-EMU update");
    if(gc_shadowmount_request_title_source_scan_cancelable(
           game->title_id, game->source_path, scan_err,
           sizeof(scan_err)) != 0) {
      snprintf(err, err_size, "%s",
               scan_err[0] ? scan_err : "could not request ShadowMount scan");
      (void)mount_switch_restore_cleared_links(&link_backup, scan_err,
                                               sizeof(scan_err));
      return -1;
    }
    if(wait_for_shadowmount_links(op->title_id, expected_mount,
                                  expected_image, err, err_size) != 0) {
      (void)mount_switch_restore_cleared_links(&link_backup, scan_err,
                                               sizeof(scan_err));
      return -1;
    }
  }
  artifact_cache_invalidate();
  return 0;
}

static int
update_ampr_verify_mounted_hash(const char *title_id,
                                const char *mounted_root,
                                const char *expected_sha,
                                char *err, size_t err_size) {
  char ampr_path[1024];
  char mounted_sha[65];

  if(!mounted_root || !mounted_root[0] || !sha256_hex_valid(expected_sha)) {
    snprintf(err, err_size, "%s", "bad AMPR mounted hash verification input");
    errno = EINVAL;
    return -1;
  }
  if(!ampr_folder_target_probe(mounted_root, ampr_path, sizeof(ampr_path),
                               mounted_sha)) {
    snprintf(err, err_size,
             "mounted AMPR binary was not found after hot-swap at %s",
             mounted_root);
    return -1;
  }
  if(!sha256_hex_valid(mounted_sha) ||
     strcasecmp(mounted_sha, expected_sha) != 0) {
    snprintf(err, err_size,
             "mounted AMPR hash mismatch after hot-swap: expected %.64s got %.64s",
             expected_sha, mounted_sha[0] ? mounted_sha : "(unreadable)");
    return -1;
  }
  gc_log("update-ampr mounted verify title=%s path=%s sha=%s",
         title_id ? title_id : "", ampr_path, mounted_sha);
  return 0;
}

static int
mount_switch_delete_hidden_source(gc_operation_t *op,
                                  gc_hidden_instance_t *hidden,
                                  size_t hidden_count,
                                  const char *source_path,
                                  gc_source_kind_t source_kind,
                                  char *err, size_t err_size) {
  for(size_t i = 0; i < hidden_count; i++) {
    gc_hidden_instance_t *entry = &hidden[i];
    if(!entry->hidden ||
       !paths_equal_ignoring_trailing_slash(entry->original_path,
                                            source_path)) {
      continue;
    }
    if(delete_source_after_success_with_title(entry->hidden_path,
                                              entry->source_kind,
                                              op ? op->title_id : "",
                                              err, err_size) != 0) {
      return -1;
    }
    entry->hidden = 0;
    (void)mount_switch_recovery_append(op ? op->id : "",
                                       op ? op->title_id : "",
                                       entry->original_path,
                                       entry->hidden_path, "deleted");
    gc_size_cache_forget(entry->original_path);
    gc_log("mount switch deleted hidden source title=%s original=%s hidden=%s",
           op ? op->title_id : "", entry->original_path, entry->hidden_path);
    return 0;
  }
  return delete_source_after_success_with_title(source_path, source_kind,
                                                op ? op->title_id : "",
                                                err, err_size);
}

static int
mount_switch_restore_after_operation_ex(gc_operation_t *op,
                                        gc_hidden_instance_t *hidden,
                                        size_t hidden_count,
                                        int request_scan,
                                        char *err, size_t err_size) {
  char scan_err[256] = {0};
  if(hidden_count > 0) {
    append_operation_phase(op, "restoring");
    job_set_phase("restoring", 0, 0, "Restoring other instances");
  }
  if(mount_switch_restore_hidden(op, hidden, hidden_count, err,
                                 err_size) != 0) {
    artifact_cache_invalidate();
    return -1;
  }
  artifact_cache_invalidate();
  if(!request_scan) {
    gc_log("mount switch post-restore scan skipped after selected mount title=%s",
           op ? op->title_id : "");
    return 0;
  }
  if(job_cancelled()) {
    gc_log("mount switch post-restore scan skipped after cancel title=%s",
           op ? op->title_id : "");
    return 0;
  }
  if(gc_shadowmount_request_scan_cancelable(scan_err, sizeof(scan_err)) != 0) {
    if(job_cancelled()) {
      gc_log("mount switch post-restore scan skipped after cancel title=%s",
             op ? op->title_id : "");
      return 0;
    }
    gc_log("mount switch post-restore scan failed title=%s err=%s",
           op ? op->title_id : "", scan_err[0] ? scan_err : "unknown");
  }
  return 0;
}

static int
mount_switch_restore_after_operation(gc_operation_t *op,
                                     gc_hidden_instance_t *hidden,
                                     size_t hidden_count,
                                     char *err, size_t err_size) {
  return mount_switch_restore_after_operation_ex(op, hidden, hidden_count, 1,
                                                 err, err_size);
}

static void
fsync_parent_dir_best_effort(const char *path) {
  char parent[1024];
  if(!path || path_parent(path, parent, sizeof(parent)) != 0) return;
  int fd = open(parent, O_RDONLY);
  if(fd >= 0) {
    fsync(fd);
    close(fd);
  }
}

static int
build_preserved_original_path(const char *original_path,
                              char *out,
                              size_t out_size) {
  char parent[1024];
  const char *name = path_basename(original_path);
  if(!original_path || !original_path[0] || !name || !name[0] ||
     !out || out_size == 0 ||
     path_parent(original_path, parent, sizeof(parent)) != 0) {
    errno = EINVAL;
    return -1;
  }
  int n = snprintf(out, out_size, "%s%s.%s.original",
                   parent, parent[1] ? "/" : "", name);
  if(n < 0 || (size_t)n >= out_size) {
    errno = ENAMETOOLONG;
    return -1;
  }
  if(!path_is_safe(out)) {
    errno = EINVAL;
    return -1;
  }
  return 0;
}

static int
compress_delete_source_after_success(gc_operation_t *op, const gc_game_t *game,
                                     gc_hidden_instance_t *hidden,
                                     size_t hidden_count) {
  char delete_err[256] = {0};
  gc_checkpoint("compress delete source");
  job_set_current("Deleting original source");
  if(mount_switch_delete_hidden_source(op, hidden, hidden_count,
                                       game->source_path,
                                       game->source_kind,
                                       delete_err,
                                       sizeof(delete_err)) != 0) {
    char restore_err[256] = {0};
    snprintf(op->error, sizeof(op->error), "%s", delete_err);
    gc_log("compress delete source failed title=%s err=%s", op->title_id,
           op->error);
    (void)mount_switch_restore_after_operation(op, hidden, hidden_count,
                                               restore_err,
                                               sizeof(restore_err));
    return -1;
  }
  gc_log("compress source deleted title=%s path=%s", op->title_id,
         game->source_path);
  append_operation_phase(op, "source-deleted");
  return 0;
}

static int
compress_restore_duplicates_after_success(gc_operation_t *op,
                                          gc_hidden_instance_t *hidden,
                                          size_t hidden_count,
                                          int request_scan) {
  char restore_err[256] = {0};
  if(mount_switch_restore_after_operation_ex(op, hidden, hidden_count,
                                             request_scan, restore_err,
                                             sizeof(restore_err)) != 0) {
    snprintf(op->error, sizeof(op->error), "%s",
             restore_err[0] ? restore_err :
             "could not restore duplicate instances");
    gc_log("compress restore failed title=%s err=%s", op->title_id,
           op->error);
    return -1;
  }
  return 0;
}

static int
preserve_original_source_after_success(gc_operation_t *op,
                                       const gc_game_t *game,
                                       gc_hidden_instance_t *hidden,
                                       size_t hidden_count,
                                       char *err,
                                       size_t err_size) {
  char hidden_path[1024];
  const char *rename_from = NULL;
  gc_hidden_instance_t *hidden_entry = NULL;
  struct stat st;

  if(!op || !game || !game->source_path[0]) {
    snprintf(err, err_size, "%s", "source is unavailable for preserve");
    errno = EINVAL;
    return -1;
  }
  if(build_preserved_original_path(game->source_path, hidden_path,
                                   sizeof(hidden_path)) != 0) {
    snprintf(err, err_size, "build preserved original path: %s",
             strerror(errno));
    return -1;
  }
  if(lstat(hidden_path, &st) == 0) {
    snprintf(err, err_size, "preserved original already exists: %s",
             hidden_path);
    errno = EEXIST;
    return -1;
  }
  if(errno != ENOENT) {
    snprintf(err, err_size, "check preserved original path: %s",
             strerror(errno));
    return -1;
  }

  for(size_t i = 0; i < hidden_count; i++) {
    gc_hidden_instance_t *entry = &hidden[i];
    if(entry->hidden &&
       paths_equal_ignoring_trailing_slash(entry->original_path,
                                           game->source_path)) {
      hidden_entry = entry;
      rename_from = entry->hidden_path;
      break;
    }
  }
  if(!rename_from) rename_from = game->source_path;

  if(rename(rename_from, hidden_path) != 0) {
    snprintf(err, err_size, "preserve original source: %s", strerror(errno));
    return -1;
  }
  if(hidden_entry) hidden_entry->hidden = 0;
  fsync_parent_dir_best_effort(hidden_path);

  snprintf(op->preserve_original, sizeof(op->preserve_original), "%s", "hide");
  snprintf(op->preserved_original_path, sizeof(op->preserved_original_path),
           "%s", game->source_path);
  snprintf(op->preserved_hidden_path, sizeof(op->preserved_hidden_path),
           "%s", hidden_path);
  gc_size_cache_forget(game->source_path);
  artifact_cache_invalidate();
  gc_log("compress preserved original title=%s original=%s hidden=%s from=%s",
         op->title_id, game->source_path, hidden_path, rename_from);
  append_operation_phase(op, "source-preserved");

  char scan_err[256] = {0};
  if(job_cancelled()) {
    gc_log("preserve original scan skipped after cancel title=%s",
           op->title_id);
  } else if(gc_shadowmount_request_scan_cancelable(scan_err,
                                                   sizeof(scan_err)) != 0 &&
            !job_cancelled()) {
    gc_log("preserve original scan request failed title=%s err=%s",
           op->title_id, scan_err[0] ? scan_err : "unknown");
  }
  return 0;
}

static int
transfer_action_to_external(gc_action_t action) {
  return action == GC_ACTION_MOVE_TO_USB || action == GC_ACTION_COPY_TO_USB;
}

static int
transfer_action_to_internal(gc_action_t action) {
  return action == GC_ACTION_MOVE_TO_INTERNAL ||
         action == GC_ACTION_COPY_TO_INTERNAL;
}

static int
transfer_action_copy_only(gc_action_t action) {
  return action == GC_ACTION_COPY_TO_USB ||
         action == GC_ACTION_COPY_TO_INTERNAL;
}

static void operation_append_error_detail(gc_operation_t *op,
                                          const char *detail);

static int
run_move_op(gc_operation_t *op) {
  gc_game_t game = {0};
  gc_game_t moved_game = {0};
  gc_hidden_instance_t hidden[GC_MAX_GAMES];
  char err[256] = {0};
  char restore_err[256] = {0};
  char target_path[1024] = {0};
  char temp_path[1024] = {0};
  struct stat st;
  uint64_t copied = 0;
  uint64_t free_bytes = 0;
  size_t hidden_count = 0;
  int mount_missed = 0;
  int copy_only = transfer_action_copy_only(op->action);
  const char *verb = copy_only ? "copy" : "move";

  gc_checkpoint("move find game");
  gc_log("%s start op=%s title=%s action=%s targetRoot=%s", verb, op->id,
         op->title_id, action_name(op->action), op->target_root);
  append_operation_phase(op, "resolving");
  job_set_phase("resolving", 0, 0, "Resolving selected source");
  if(find_game_for_operation_source_path(op, &game, 0) != 0 ||
     game.source_kind == GC_SOURCE_UNKNOWN) {
    snprintf(op->error, sizeof(op->error), "%s", "game is unavailable");
    gc_log("%s failed title=%s err=%s", verb, op->title_id, op->error);
    return -1;
  }
  if(gc_cancel_requested(err, sizeof(err))) {
    snprintf(op->error, sizeof(op->error), "%s", err);
    return -1;
  }
  snprintf(op->source_path, sizeof(op->source_path), "%s", game.source_path);
  if(game.source_kind == GC_SOURCE_FOLDER && game.size_pending) {
    int cancelled = 0;
    append_operation_phase(op, "measuring");
    job_set_phase("measuring", 0, 0, "Measuring selected source");
    game.source_size = source_size_bytes_exact_ex(
        game.source_path, game.source_kind, 1, &cancelled);
    if(cancelled) {
      snprintf(op->error, sizeof(op->error), "%s", "cancelled");
      return -1;
    }
    game.required_bytes = game.source_size;
    gc_size_cache_store(game.source_path, game.source_size);
  }
  if(close_title_if_running(op->title_id, err, sizeof(err)) != 0) {
    snprintf(op->error, sizeof(op->error), "%s",
             err[0] ? err : "could not close game");
    gc_log("%s close game failed title=%s err=%s", verb, op->title_id,
           op->error);
    return -1;
  }
  if(gc_cancel_requested(err, sizeof(err))) {
    snprintf(op->error, sizeof(op->error), "%s", err);
    return -1;
  }
  if(build_move_target_path(op->target_root, game.source_path, target_path,
                            sizeof(target_path), err, sizeof(err)) != 0) {
    snprintf(op->error, sizeof(op->error), "%s", err);
    gc_log("%s failed title=%s err=%s", verb, op->title_id, op->error);
    return -1;
  }
  snprintf(op->output_path, sizeof(op->output_path), "%s", target_path);
  if(!strcmp(game.source_path, target_path)) {
    snprintf(op->error, sizeof(op->error), "%s", "game is already in that location");
    gc_log("%s failed title=%s err=%s", verb, op->title_id, op->error);
    return -1;
  }
  if(stat(target_path, &st) == 0) {
    snprintf(op->error, sizeof(op->error), "%s", "target already exists");
    gc_log("%s failed title=%s err=%s", verb, op->title_id, op->error);
    return -1;
  }
  if(transfer_action_to_external(op->action)) {
    char usb_root[1024];
    if(path_parent(op->target_root, usb_root, sizeof(usb_root)) != 0 ||
       stat(usb_root, &st) != 0 || !S_ISDIR(st.st_mode)) {
      snprintf(op->error, sizeof(op->error), "%s",
               "selected external storage is unavailable");
      gc_log("%s failed title=%s err=%s", verb, op->title_id, op->error);
      return -1;
    }
  }
  if(mkdirs(op->target_root) != 0) {
    snprintf(op->error, sizeof(op->error), "create target root: %s",
             strerror(errno));
    gc_log("%s failed title=%s err=%s", verb, op->title_id, op->error);
    return -1;
  }
  if(space_for_path(op->target_root, &free_bytes, NULL) != 0 ||
     free_bytes < game.source_size) {
    uint64_t need = free_bytes < game.source_size
        ? game.source_size - free_bytes
        : game.source_size;
    snprintf(op->error, sizeof(op->error),
             "not enough free storage; free %llu more bytes",
             (unsigned long long)need);
    gc_log("%s failed title=%s err=%s", verb, op->title_id, op->error);
    return -1;
  }
  if(hidden_sibling_temp_path(target_path,
                              copy_only ? "gc-copying" : "gc-moving",
                              temp_path, sizeof(temp_path)) != 0) {
    snprintf(op->error, sizeof(op->error), "temporary %s path too long",
             verb);
    return -1;
  }
  if(gc_cancel_requested(err, sizeof(err))) {
    snprintf(op->error, sizeof(op->error), "%s", err);
    return -1;
  }
  remove_tree_gc(temp_path);
  char legacy_temp_path[1024] = {0};
  if(legacy_suffix_temp_path(target_path, copy_only ? "copying" : "moving",
                             legacy_temp_path,
                             sizeof(legacy_temp_path)) == 0) {
    remove_tree_gc(legacy_temp_path);
  }
  if(gc_cancel_requested(err, sizeof(err))) {
    snprintf(op->error, sizeof(op->error), "%s", err);
    return -1;
  }

  job_set_target(target_path);
  append_operation_phase(op, "copying");
  job_set_phase("copying", 0, 0, copy_only ? "Copying game" :
                "Transferring game");
  job_store_u64(&g_job.total_bytes, game.source_size);
  atomic_store(&g_job.copied_bytes, 0);
  gc_checkpoint("move copy");
  if(copy_tree_gc(game.source_path, temp_path, &copied, err, sizeof(err)) != 0) {
    snprintf(op->error, sizeof(op->error), "%s",
             err[0] ? err : (copy_only ? "copy failed" : "move failed"));
    remove_tree_gc(temp_path);
    gc_log("%s copy failed title=%s err=%s", verb, op->title_id, op->error);
    return -1;
  }

  job_set_current(copy_only ? "Finalizing copy" : "Finalizing move");
  if(rename(temp_path, target_path) != 0) {
    snprintf(op->error, sizeof(op->error), "rename target: %s",
             strerror(errno));
    remove_tree_gc(temp_path);
    gc_log("%s finalize failed title=%s err=%s", verb, op->title_id,
           op->error);
    return -1;
  }
  if(game.source_kind == GC_SOURCE_FOLDER) {
    gc_size_cache_store(target_path, game.source_size);
  }
  if(copy_only && game.source_kind == GC_SOURCE_COMPRESSED) {
    (void)write_validation_marker_ex(
        op->title_id, target_path, NULL, "compression-stats",
        game.compression_source_size, game.apr_indexed,
        game.ampr_hot_swap_optimized);
  }

  if(copy_only) {
    err[0] = 0;
    job_set_phase("mounting", 0, 0, "Requesting ShadowMount scan");
    if(job_cancelled()) {
      gc_log("copy scan skipped after cancel title=%s", op->title_id);
    } else if(gc_shadowmount_request_scan_cancelable(err, sizeof(err)) != 0 &&
              !job_cancelled()) {
      gc_log("copy scan request failed title=%s err=%s", op->title_id,
             err[0] ? err : "unknown");
    }
    snprintf(op->result, sizeof(op->result), "%s", "success");
    gc_log("copy complete title=%s output=%s", op->title_id, target_path);
    return 0;
  }

  gc_checkpoint("move delete source");
  if(delete_source_after_success_with_title(game.source_path, game.source_kind,
                                            op->title_id, err,
                                            sizeof(err)) != 0) {
    snprintf(op->error, sizeof(op->error), "%s", err);
    gc_log("move delete source failed title=%s err=%s", op->title_id,
           op->error);
    return -1;
  }
  if(game.source_kind == GC_SOURCE_COMPRESSED) {
    (void)rewrite_moved_validation_marker(op->title_id, game.source_path,
                                          target_path);
  }

  moved_game = game;
  snprintf(moved_game.source_path, sizeof(moved_game.source_path), "%s",
           target_path);
  snprintf(moved_game.output_path, sizeof(moved_game.output_path), "%s",
           target_path);
  if(moved_game.source_kind == GC_SOURCE_COMPRESSED) {
    snprintf(moved_game.image_path, sizeof(moved_game.image_path), "%s",
             target_path);
  }
  set_game_mount_status(&moved_game, 0, "not-mounted");

  gc_checkpoint("move mount target");
  err[0] = 0;
  if(mount_selected_instance_hidden_exclusive(
         op, &moved_game, hidden, GC_MAX_GAMES, &hidden_count, &mount_missed,
         err, sizeof(err)) != 0) {
    if(mount_switch_restore_after_operation(op, hidden, hidden_count,
                                            restore_err,
                                            sizeof(restore_err)) != 0) {
      gc_log("move mount restore failed title=%s err=%s", op->title_id,
             restore_err[0] ? restore_err : "unknown");
      operation_append_error_detail(op, restore_err);
    }
    snprintf(op->error, sizeof(op->error), "%s",
             err[0] ? err : "ShadowMountPlus remount failed");
    gc_log("move remount failed title=%s err=%s", op->title_id, op->error);
    return -1;
  }
  if(mount_missed) {
    if(mount_switch_restore_after_operation(op, hidden, hidden_count,
                                            restore_err,
                                            sizeof(restore_err)) != 0) {
      snprintf(op->error, sizeof(op->error), "%s",
               restore_err[0] ? restore_err :
               "could not restore duplicate instances");
      gc_log("move mount-missed restore failed title=%s err=%s",
             op->title_id, op->error);
      return -1;
    }
    operation_mark_not_mounted(op, err);
    gc_log("move complete not mounted title=%s output=%s detail=%s",
           op->title_id, target_path, err[0] ? err : "");
    return 0;
  }
  if(mount_switch_restore_after_operation_ex(op, hidden, hidden_count, 0,
                                             restore_err,
                                             sizeof(restore_err)) != 0) {
    snprintf(op->error, sizeof(op->error), "%s",
             restore_err[0] ? restore_err :
             "could not restore duplicate instances");
    gc_log("move restore failed title=%s err=%s", op->title_id, op->error);
    return -1;
  }

  snprintf(op->result, sizeof(op->result), "%s", "success");
  gc_log("move complete title=%s output=%s", op->title_id, target_path);
  return 0;
}

static int
run_compress_op(gc_operation_t *op) {
  gc_game_t game = {0};
  char err[256] = {0};
  pfs_app_info_t info = {0};
  pfs_compress_plan_t *compress_plan = NULL;
  pfs_repair_info_t repair = {0};
  gc_game_t compressed_game = {0};
  gc_hidden_instance_t hidden[GC_MAX_GAMES];
  size_t hidden_count = 0;
  int make_image = op->action == GC_ACTION_MAKE_IMAGE;
  const char *op_log = make_image ? "make-image" : "compress";
  int format = !strcmp(op->format, "exfat") ?
      PFS_COMPRESS_FORMAT_EXFAT : PFS_COMPRESS_FORMAT_PFS;
  int stream_delete = !strcmp(op->delete_policy, "stream");
  int delete_after = !strcmp(op->delete_policy, "after");
  int preserve_hide = !strcmp(op->preserve_original, "hide");
  int skip_space_check = op->skip_space_check && !stream_delete;
  int raw_only = 0;
  int delete_policy = stream_delete ?
      PFS_DELETE_STREAM : PFS_DELETE_KEEP;
  int pfs_delete_policy = delete_policy;
  uint64_t stream_budget_bytes = op->stream_budget_bytes ?
      op->stream_budget_bytes : PFS_STREAM_DEFAULT_BUDGET_BYTES;
  pfs_stream_options_t stream_opts = {
    .budget_bytes = stream_budget_bytes,
    .reserve_bytes = 0,
    .order = (!strcmp(op->stream_order, "path")) ?
        PFS_STREAM_ORDER_PATH : PFS_STREAM_ORDER_BUDGETED_GAIN,
  };
  if(stream_delete) {
    if(!op->stream_order[0]) {
      snprintf(op->stream_order, sizeof(op->stream_order), "%s",
               "budgeted-gain");
    }
    op->stream_budget_bytes = stream_budget_bytes;
  }
  int compressed_output_committed = 0;
  char planned_nested_name[256] = {0};
  int planned_nested_type = PFS_NESTED_UNKNOWN;
  char compress_output_path[1024] = {0};
  char compress_write_path[1024] = {0};
  char staged_compress_path[1024] = {0};
  char output_parent[1024] = {0};
  char space_probe_path[1024] = {0};
  uint64_t target_free_bytes = 0;
  uint64_t stage_free_bytes = 0;
  int moving_to_target = 0;
  int stage_usb_target = 0;
  int mount_missed = 0;
  struct stat st;

#define COMPRESS_FAIL_RETURN() do { \
    pfs_compress_plan_free(compress_plan); \
    return -1; \
  } while(0)

  gc_checkpoint(make_image ? "make-image find game" : "compress find game");
  snprintf(op->format, sizeof(op->format), "%s",
           format == PFS_COMPRESS_FORMAT_EXFAT ? "exfat" : "pfs");
  snprintf(op->compression_mode, sizeof(op->compression_mode), "%s",
           compression_mode_or_default(op->compression_mode));
  gc_log("%s start op=%s title=%s format=%s policy=%s skipSpaceCheck=%d",
         op_log, op->id, op->title_id, op->format, op->delete_policy,
         skip_space_check);
  append_operation_phase(op, "resolving");
  job_set_phase("resolving", 0, 0, "Resolving selected source");
  if(find_game_for_operation_source_path(op, &game, 0) != 0) {
    snprintf(op->error, sizeof(op->error), "%s", "game is no longer mounted");
    gc_log("%s failed title=%s err=%s", op_log, op->title_id, op->error);
    COMPRESS_FAIL_RETURN();
  }
  if(make_image && game.source_kind != GC_SOURCE_FOLDER) {
    snprintf(op->error, sizeof(op->error), "%s",
             "only app folders can be made into images");
    gc_log("%s failed title=%s err=%s", op_log, op->title_id, op->error);
    COMPRESS_FAIL_RETURN();
  }
  if(make_image && stream_delete) {
    snprintf(op->error, sizeof(op->error), "%s",
             "destructive image creation is not supported");
    gc_log("%s failed title=%s err=%s", op_log, op->title_id, op->error);
    COMPRESS_FAIL_RETURN();
  }
  if(game.name[0]) {
    snprintf(op->display_name, sizeof(op->display_name), "%s", game.name);
  }
  snprintf(op->source_path, sizeof(op->source_path), "%s", game.source_path);
  if(op->target_root[0]) {
    moving_to_target = 1;
    if(stream_delete) {
      snprintf(op->error, sizeof(op->error), "%s",
               make_image ?
               "destructive image creation is not available while writing to target storage" :
               "destructive compression is not available while writing to target storage");
      gc_log("%s failed title=%s err=%s", op_log, op->title_id, op->error);
      COMPRESS_FAIL_RETURN();
    }
    int target_rc = make_image
        ? build_make_image_target_path(op->target_root, &game, format,
                                       compress_output_path,
                                       sizeof(compress_output_path),
                                       err, sizeof(err))
        : build_compress_target_path(op->target_root, &game,
                                     compress_output_path,
                                     sizeof(compress_output_path),
                                     err, sizeof(err));
    if(target_rc != 0) {
      snprintf(op->error, sizeof(op->error), "%s", err);
      gc_log("%s failed title=%s err=%s", op_log, op->title_id, op->error);
      COMPRESS_FAIL_RETURN();
    }
    snprintf(op->output_path, sizeof(op->output_path), "%s",
             compress_output_path);
    if(!make_image && !strcmp(storage_name_for_path(compress_output_path), "usb")) {
      stage_usb_target = 1;
      if(build_compress_stage_path(op, &game, staged_compress_path,
                                   sizeof(staged_compress_path),
                                   err, sizeof(err)) != 0) {
        snprintf(op->error, sizeof(op->error), "%s", err);
        gc_log("compress failed title=%s err=%s", op->title_id, op->error);
        COMPRESS_FAIL_RETURN();
      }
      snprintf(compress_write_path, sizeof(compress_write_path), "%s",
               staged_compress_path);
    } else {
      snprintf(compress_write_path, sizeof(compress_write_path), "%s",
               compress_output_path);
    }
  } else {
    if(make_image) {
      if(build_make_image_target_path(NULL, &game, format,
                                      compress_output_path,
                                      sizeof(compress_output_path),
                                      err, sizeof(err)) != 0) {
        snprintf(op->error, sizeof(op->error), "%s", err);
        gc_log("%s failed title=%s err=%s", op_log, op->title_id, op->error);
        COMPRESS_FAIL_RETURN();
      }
    } else {
      snprintf(compress_output_path, sizeof(compress_output_path), "%s",
               game.output_path);
    }
    snprintf(compress_write_path, sizeof(compress_write_path), "%s",
             compress_output_path);
    snprintf(op->output_path, sizeof(op->output_path), "%s",
             compress_output_path);
  }
  snprintf(op->source_kind, sizeof(op->source_kind), "%s",
           source_kind_name(game.source_kind));
  if(close_title_if_running(op->title_id, err, sizeof(err)) != 0) {
    snprintf(op->error, sizeof(op->error), "%s",
             err[0] ? err : "could not close game");
    gc_log("%s close game failed title=%s err=%s", op_log, op->title_id,
           op->error);
    COMPRESS_FAIL_RETURN();
  }
  append_operation_phase(op, make_image ? "making-image" : "compressing");
  job_set_phase(make_image ? "making-image" : "compressing", 0, 0,
                make_image ? "Making image" : "Compressing game");
  pfs_delete_policy = delete_policy;
  if(delete_after && game.source_kind == GC_SOURCE_FOLDER) {
    pfs_delete_policy = PFS_DELETE_AFTER;
  }
  if(game.source_kind == GC_SOURCE_FOLDER) {
    int prepare_rc;
    gc_checkpoint(make_image ? "make-image prepare scan" : "compress prepare scan");
    if(make_image) {
      prepare_rc = pfs_make_image_prepare_source_opts_output_ex(
          game.source_path, 0, format, compress_write_path,
          &compress_plan, &info, err, sizeof(err));
    } else {
      prepare_rc = pfs_compress_prepare_source_to_ffpfsc_opts_output_ex(
          game.source_path, 0, format, pfs_delete_policy, raw_only,
          moving_to_target ? compress_write_path : NULL,
          stream_delete ? &stream_opts : NULL,
          &compress_plan, &info, err, sizeof(err));
    }
    if(prepare_rc != 0) {
      if(prepare_rc == -2 || errno == EEXIST || error_is_output_exists(err)) {
        set_output_exists_error(op, compress_write_path);
      } else {
        snprintf(op->error, sizeof(op->error), "%s",
                 err[0] ? err :
                 (make_image ? "image scan failed" : "compression scan failed"));
      }
      gc_log("%s scan failed title=%s err=%s", op_log, op->title_id, op->error);
      COMPRESS_FAIL_RETURN();
    }
    operation_store_scan_stats(op, &info);
    game.source_size = info.scan_bytes;
    game.required_bytes = make_image && info.nested_size
        ? info.nested_size : info.scan_bytes;
    game.size_pending = 0;
    gc_size_cache_store(game.source_path, info.scan_bytes);
    gc_log("compress scan title=%s storage=%s workers=%llu bytes=%llu files=%llu dirs=%llu entries=%llu elapsedMs=%llu path=%s",
           op->title_id, storage_name_for_path(game.source_path),
           (unsigned long long)info.scan_workers,
           (unsigned long long)info.scan_bytes,
           (unsigned long long)info.scan_files,
           (unsigned long long)info.scan_dirs,
           (unsigned long long)info.scan_entries,
           (unsigned long long)info.scan_elapsed_ms,
           game.source_path);
  }
  if(!moving_to_target) {
    if(path_parent(compress_output_path, output_parent,
                   sizeof(output_parent)) != 0) {
      errno = EINVAL;
    }
    errno = 0;
    if(!output_parent[0] || stat(output_parent, &st) != 0 ||
       !S_ISDIR(st.st_mode)) {
      int saved_errno = !output_parent[0] ? EINVAL :
          (errno ? errno : ENOTDIR);
      snprintf(op->error, sizeof(op->error), "%s",
               "compression target folder is unavailable");
      gc_log("compress target folder unavailable title=%s output=%s parent=%s err=%s",
             op->title_id, compress_output_path, output_parent,
             strerror(saved_errno));
      COMPRESS_FAIL_RETURN();
    }
    if(!path_is_shadowmount_game_root(output_parent)) {
      snprintf(op->error, sizeof(op->error), "%s",
               "compression target folder is not a ShadowMountPlus game folder");
      gc_log("compress target folder rejected title=%s output=%s parent=%s",
             op->title_id, compress_output_path, output_parent);
      COMPRESS_FAIL_RETURN();
    }
    if(free_bytes_for_output_ex(compress_output_path, &game.free_bytes,
                                space_probe_path,
                                sizeof(space_probe_path)) != 0) {
      int saved_errno = errno ? errno : EIO;
      if(skip_space_check) {
        game.free_bytes = game.required_bytes;
        gc_log("compress target free probe bypassed title=%s output=%s parent=%s err=%s",
               op->title_id, compress_output_path, output_parent,
               strerror(saved_errno));
      } else {
        snprintf(op->error, sizeof(op->error),
                 "could not check free storage for compression target: %s",
                 strerror(saved_errno));
        gc_log("compress target free probe failed title=%s output=%s parent=%s err=%s",
               op->title_id, compress_output_path, output_parent,
               strerror(saved_errno));
        COMPRESS_FAIL_RETURN();
      }
    }
    game.extra_needed = game.free_bytes >= game.required_bytes
        ? 0 : game.required_bytes - game.free_bytes;
    if(space_probe_path[0] && strcmp(space_probe_path, output_parent)) {
      gc_log("compress target free probe fallback title=%s output=%s parent=%s probe=%s free=%llu",
             op->title_id, compress_output_path, output_parent,
             space_probe_path, (unsigned long long)game.free_bytes);
    }
  }
  gc_log("compress source title=%s kind=%s size=%llu free=%llu required=%llu path=%s output=%s targetRoot=%s",
         op->title_id, source_kind_name(game.source_kind),
         (unsigned long long)game.source_size,
         (unsigned long long)game.free_bytes,
         (unsigned long long)game.required_bytes,
         game.source_path, compress_output_path, op->target_root);
  gc_checkpoint("compress preflight");
  if(game.source_size == 0 || game.required_bytes == 0) {
    snprintf(op->error, sizeof(op->error), "%s",
             "could not measure game size");
    gc_log("compress failed title=%s err=%s", op->title_id, op->error);
    COMPRESS_FAIL_RETURN();
  }
  if(moving_to_target) {
    if(path_parent(op->target_root, err, sizeof(err)) != 0 ||
       stat(err, &st) != 0 || !S_ISDIR(st.st_mode)) {
      snprintf(op->error, sizeof(op->error), "%s",
               "selected target storage is unavailable");
      gc_log("compress failed title=%s err=%s", op->title_id, op->error);
      COMPRESS_FAIL_RETURN();
    }
    if(mkdirs(op->target_root) != 0) {
      snprintf(op->error, sizeof(op->error), "create target root: %s",
               strerror(errno));
      gc_log("compress failed title=%s err=%s", op->title_id, op->error);
      COMPRESS_FAIL_RETURN();
    }
    if(stat(compress_output_path, &st) == 0) {
      set_output_exists_error(op, compress_output_path);
      gc_log("compress failed title=%s err=%s", op->title_id, op->error);
      COMPRESS_FAIL_RETURN();
    }
    if(stage_usb_target && stat(staged_compress_path, &st) == 0) {
      snprintf(op->error, sizeof(op->error), "%s",
               "staged compression target already exists");
      gc_log("compress failed title=%s err=%s", op->title_id, op->error);
      COMPRESS_FAIL_RETURN();
    }
    if(space_for_path(op->target_root, &target_free_bytes, NULL) != 0) {
      int saved_errno = errno ? errno : EIO;
      if(skip_space_check) {
        target_free_bytes = game.required_bytes;
        gc_log("compress target free probe bypassed title=%s targetRoot=%s err=%s",
               op->title_id, op->target_root, strerror(saved_errno));
      } else {
        snprintf(op->error, sizeof(op->error),
                 "could not check free storage on selected target storage: %s",
                 strerror(saved_errno));
        gc_log("compress target free probe failed title=%s targetRoot=%s err=%s",
               op->title_id, op->target_root, strerror(saved_errno));
        COMPRESS_FAIL_RETURN();
      }
    }
    if(target_free_bytes < game.required_bytes) {
      uint64_t need = target_free_bytes < game.required_bytes
          ? game.required_bytes - target_free_bytes
          : game.required_bytes;
      snprintf(op->error, sizeof(op->error),
               "not enough free storage on selected target storage; free %llu more bytes",
               (unsigned long long)need);
      gc_log("compress failed title=%s err=%s", op->title_id, op->error);
      COMPRESS_FAIL_RETURN();
    }
    if(stage_usb_target &&
       free_bytes_for_output_ex(staged_compress_path, &stage_free_bytes,
                                space_probe_path,
                                sizeof(space_probe_path)) != 0) {
      int saved_errno = errno ? errno : EIO;
      if(skip_space_check) {
        stage_free_bytes = game.required_bytes;
        gc_log("compress staging free probe bypassed title=%s stage=%s err=%s",
               op->title_id, staged_compress_path, strerror(saved_errno));
      } else {
        snprintf(op->error, sizeof(op->error),
                 "could not check internal staging space for USB compression: %s",
                 strerror(saved_errno));
        gc_log("compress staging free probe failed title=%s stage=%s err=%s",
               op->title_id, staged_compress_path, strerror(saved_errno));
        COMPRESS_FAIL_RETURN();
      }
    }
    if(stage_usb_target && stage_free_bytes < game.required_bytes) {
      uint64_t need = stage_free_bytes < game.required_bytes
          ? game.required_bytes - stage_free_bytes
          : game.required_bytes;
      snprintf(op->error, sizeof(op->error),
               "not enough internal staging space for USB compression; free %llu more bytes",
               (unsigned long long)need);
      gc_log("compress failed title=%s err=%s", op->title_id, op->error);
      COMPRESS_FAIL_RETURN();
    }
  }
  if(!moving_to_target && !stream_delete &&
     game.free_bytes < game.required_bytes) {
    snprintf(op->error, sizeof(op->error),
             "not enough free storage; free %llu more bytes",
             (unsigned long long)(game.required_bytes - game.free_bytes));
    gc_log("compress failed title=%s err=%s", op->title_id, op->error);
    COMPRESS_FAIL_RETURN();
  }
  if(stream_delete &&
     !stream_delete_allowed_by_space_budget(&game, stream_budget_bytes)) {
    uint64_t min_free =
        stream_min_free_bytes_for_budget(game.source_size,
                                         stream_budget_bytes);
    uint64_t need = game.free_bytes < min_free ? min_free - game.free_bytes : 0;
    snprintf(op->error, sizeof(op->error),
             "not enough free storage for delete-while-processing; free %llu more bytes",
             (unsigned long long)need);
    gc_log("compress failed title=%s err=%s", op->title_id, op->error);
    COMPRESS_FAIL_RETURN();
  }
  if(stream_delete) {
    atomic_store(&g_job.stream_min_free_bytes,
                 (long)stream_min_free_bytes_for_budget(game.source_size,
                                                        stream_budget_bytes));
    atomic_store(&g_job.stream_budget_bytes,
                 stream_budget_bytes > (uint64_t)LONG_MAX ? LONG_MAX :
                 (long)stream_budget_bytes);
  }
  gc_checkpoint(make_image ? "make-image shadowmount hints" :
                "compress shadowmount hints");
  if(make_image) {
    if(gc_shadowmount_prepare_image_hints_for_title(op->title_id,
                                                   compress_output_path,
                                                   info.nested_type,
                                                   err, sizeof(err)) != 0) {
      snprintf(op->error, sizeof(op->error), "ShadowMount hint failed: %s",
               err[0] ? err : "unknown");
      gc_log("%s failed title=%s err=%s", op_log, op->title_id, op->error);
      COMPRESS_FAIL_RETURN();
    }
    gc_log("make-image shadowmount hints title=%s image=%s type=%d",
           op->title_id, compress_output_path, info.nested_type);
  } else {
    if(planned_compress_nested(&game, format, planned_nested_name,
                               sizeof(planned_nested_name),
                               &planned_nested_type) != 0) {
      snprintf(op->error, sizeof(op->error), "%s",
               "could not derive ShadowMount hints");
      gc_log("compress failed title=%s err=%s", op->title_id, op->error);
      COMPRESS_FAIL_RETURN();
    }
    if(gc_shadowmount_prepare_pfsc_hints_for_title(op->title_id,
                                                  compress_output_path,
                                                  planned_nested_name,
                                                  planned_nested_type,
                                                  err, sizeof(err)) != 0) {
      snprintf(op->error, sizeof(op->error), "ShadowMount hint failed: %s",
               err[0] ? err : "unknown");
      gc_log("compress failed title=%s err=%s", op->title_id, op->error);
      COMPRESS_FAIL_RETURN();
    }
    gc_log("compress shadowmount hints title=%s outer=%s nested=%s type=%d",
           op->title_id, compress_output_path, planned_nested_name,
           planned_nested_type);
  }

  gc_checkpoint(make_image ? "make-image writing image" :
                "compress writing ffpfsc");
  int compress_rc;
  if(compress_plan) {
    compress_rc = make_image
        ? pfs_make_image_execute_prepared(compress_plan, &info, err, sizeof(err))
        : pfs_compress_execute_prepared_to_ffpfsc(
            compress_plan, PFS_COMPRESS_DEFAULT_WORKERS, &info, err, sizeof(err));
    pfs_compress_plan_free(compress_plan);
    compress_plan = NULL;
  } else {
    compress_rc = pfs_compress_source_to_ffpfsc_opts_output_ex(
        game.source_path, 0, PFS_COMPRESS_DEFAULT_WORKERS,
        format, pfs_delete_policy, raw_only,
        moving_to_target ? compress_write_path : NULL,
        stream_delete ? &stream_opts : NULL,
        &info, err, sizeof(err));
  }
  if(compress_rc != 0) {
    if(errno == EEXIST || error_is_output_exists(err)) {
      set_output_exists_error(op, compress_write_path);
    } else {
      snprintf(op->error, sizeof(op->error), "%s",
               err[0] ? err :
               (make_image ? "image creation failed" : "compression failed"));
    }
    gc_log("%s failed title=%s err=%s", op_log, op->title_id, op->error);
    COMPRESS_FAIL_RETURN();
  }
  operation_store_scan_stats(op, &info);
  if(stage_usb_target) {
    gc_checkpoint("compress publish staged output");
    err[0] = 0;
    if(publish_staged_compress_output(op, info.output_path,
                                      compress_output_path,
                                      err, sizeof(err)) != 0) {
      snprintf(op->error, sizeof(op->error), "%s",
               err[0] ? err : "publish compressed image failed");
      gc_log("compress publish failed title=%s err=%s", op->title_id,
             op->error);
      cleanup_failed_safe_compress_output(info.output_path, op->title_id);
      COMPRESS_FAIL_RETURN();
    }
    snprintf(info.output_path, sizeof(info.output_path), "%s",
             compress_output_path);
    info.output_exists = 1;
  }
  snprintf(op->output_path, sizeof(op->output_path), "%s", info.output_path);
  if(!make_image) {
    operation_store_compression_stats(op, game.source_size, info.output_path);
  }
  gc_log("%s wrote title=%s output=%s nested=%s nestedSize=%llu storedSize=%llu",
         op_log, op->title_id, info.output_path, info.nested_name,
         (unsigned long long)info.nested_size,
         (unsigned long long)info.stored_size);
  compressed_output_committed = 1;
  if(make_image) {
    append_operation_phase(op, "image-created");
    init_image_output_game_for_mount(op, &game, &info, &compressed_game);
    gc_checkpoint("make-image mount output");
    err[0] = 0;
    if(mount_selected_instance_hidden_exclusive(
           op, &compressed_game, hidden, GC_MAX_GAMES, &hidden_count,
           &mount_missed, err, sizeof(err)) != 0) {
      char restore_err[256] = {0};
      if(hidden_count > 0 &&
         mount_switch_restore_after_operation(op, hidden, hidden_count,
                                              restore_err,
                                              sizeof(restore_err)) != 0) {
        gc_log("make-image mount restore failed title=%s err=%s", op->title_id,
               restore_err[0] ? restore_err : "unknown");
      }
      snprintf(op->error, sizeof(op->error), "%s",
               err[0] ? err : "ShadowMountPlus mount failed");
      gc_log("make-image mount failed title=%s err=%s", op->title_id,
             op->error);
      COMPRESS_FAIL_RETURN();
    }
    if(mount_missed) {
      char restore_err[256] = {0};
      if(mount_switch_restore_after_operation(op, hidden, hidden_count,
                                              restore_err,
                                              sizeof(restore_err)) != 0) {
        gc_log("make-image mount-missed restore failed title=%s err=%s",
               op->title_id, restore_err[0] ? restore_err : "unknown");
      }
      operation_mark_not_mounted(op,
          err[0] ? err : "ShadowMountPlus did not mount image output");
      gc_log("make-image complete not mounted title=%s output=%s detail=%s",
             op->title_id, info.output_path, err[0] ? err : "");
      return 0;
    }
    if(delete_after) {
      if(compress_delete_source_after_success(op, &game, hidden,
                                              hidden_count) != 0) {
        COMPRESS_FAIL_RETURN();
      }
    } else if(preserve_hide) {
      char preserve_err[256] = {0};
      gc_checkpoint("make-image preserve original");
      job_set_current("Preserving original source");
      if(preserve_original_source_after_success(op, &game, hidden, hidden_count,
                                                preserve_err,
                                                sizeof(preserve_err)) != 0) {
        char restore_err[256] = {0};
        snprintf(op->error, sizeof(op->error), "%s",
                 preserve_err[0] ? preserve_err :
                 "could not preserve original source");
        gc_log("make-image preserve original failed title=%s err=%s",
               op->title_id, op->error);
        (void)mount_switch_restore_after_operation(op, hidden, hidden_count,
                                                   restore_err,
                                                   sizeof(restore_err));
        COMPRESS_FAIL_RETURN();
      }
    }
    if(compress_restore_duplicates_after_success(op, hidden,
                                                 hidden_count, 0) != 0) {
      COMPRESS_FAIL_RETURN();
    }
    snprintf(op->result, sizeof(op->result), "%s", "success");
    gc_log("make-image complete title=%s output=%s", op->title_id,
           info.output_path);
    return 0;
  }
  if(stream_delete) {
    append_operation_phase(op, "source-deleted");
  } else {
    append_operation_phase(op, "compressed");
  }
  init_compressed_output_game_for_mount(op, &game, &info, &compressed_game);
  gc_checkpoint("compress mount output");
  err[0] = 0;
  if(mount_selected_instance_hidden_exclusive(
         op, &compressed_game, hidden, GC_MAX_GAMES, &hidden_count,
         &mount_missed, err, sizeof(err)) != 0) {
    char restore_err[256] = {0};
    if(hidden_count > 0 &&
       mount_switch_restore_after_operation(op, hidden, hidden_count,
                                            restore_err,
                                            sizeof(restore_err)) != 0) {
      gc_log("compress mount restore failed title=%s err=%s", op->title_id,
             restore_err[0] ? restore_err : "unknown");
    }
    snprintf(op->error, sizeof(op->error), "%s",
             err[0] ? err : "ShadowMountPlus mount failed");
    gc_log("compress mount failed title=%s err=%s", op->title_id,
           op->error);
    if(!compressed_output_committed) {
      cleanup_failed_safe_compress_output(info.output_path, op->title_id);
    }
    COMPRESS_FAIL_RETURN();
  }
  if(mount_missed) {
    char restore_err[256] = {0};
    char remount_err[256] = {0};
    gc_log("compress mount missed title=%s output=%s detail=%s; retrying bounce remount",
           op->title_id, info.output_path, err[0] ? err : "");
    if(force_compressed_path_bounce_remount(
           op->title_id, info.output_path, compressed_game.nested_name,
           compressed_game.nested_type, remount_err,
           sizeof(remount_err)) != 0) {
      if(mount_switch_restore_after_operation(op, hidden, hidden_count,
                                              restore_err,
                                              sizeof(restore_err)) != 0) {
        gc_log("compress mount-missed restore failed title=%s err=%s",
               op->title_id, restore_err[0] ? restore_err : "unknown");
      }
      snprintf(op->error, sizeof(op->error), "%s",
               remount_err[0] ? remount_err :
               (err[0] ? err : "ShadowMountPlus did not mount compressed output"));
      gc_log("compress mount retry failed title=%s output=%s err=%s",
             op->title_id, info.output_path, op->error);
      operation_mark_not_mounted(op, op->error);
      operation_store_compression_stats(op, game.source_size, info.output_path);
      (void)write_validation_marker_ex(op->title_id, info.output_path, NULL,
                                       "compression-stats", game.source_size,
                                       op->apr_indexed,
                                       op->ampr_hot_swap_optimized);
      return 0;
    }
    gc_log("compress mount retry succeeded title=%s output=%s",
           op->title_id, info.output_path);
  }

  gc_checkpoint("compress wait repair");
  append_operation_phase(op, "repairing");
  if(repair_with_wait(op->title_id, info.output_path, &repair, err,
                      sizeof(err)) != 0) {
    char restore_err[256] = {0};
    op->bad_blocks_found = repair.repaired_blocks;
    op->repaired_blocks = 0;
    operation_store_repair_counters(op, &repair);
    if(repair.outdir[0]) {
      snprintf(op->repair_summary, sizeof(op->repair_summary), "%s",
               repair.outdir);
    }
    snprintf(op->error, sizeof(op->error), "%s",
             err[0] ? err : "validate and repair failed");
    gc_log("compress repair failed title=%s err=%s", op->title_id, op->error);
    if(mount_switch_restore_after_operation(op, hidden, hidden_count,
                                            restore_err,
                                            sizeof(restore_err)) != 0) {
      gc_log("compress repair restore failed title=%s err=%s", op->title_id,
             restore_err[0] ? restore_err : "unknown");
    }
    if(!compressed_output_committed) {
      cleanup_failed_safe_compress_output(info.output_path, op->title_id);
    }
    COMPRESS_FAIL_RETURN();
  }

  gc_checkpoint("compress force remount");
  operation_store_repair_success(op, &repair);
  if(repair_force_path_bounce_remount(op->title_id, info.output_path, &repair,
                                      err, sizeof(err)) != 0) {
    if(shadowmount_mount_missed(err)) {
      if(delete_after) {
        if(compress_delete_source_after_success(op, &game, hidden,
                                                hidden_count) != 0) {
          COMPRESS_FAIL_RETURN();
        }
      }
      if(compress_restore_duplicates_after_success(op, hidden,
                                                   hidden_count, 1) != 0) {
        COMPRESS_FAIL_RETURN();
      }
      operation_mark_verified_not_mounted(op, err);
      (void)write_validation_marker_ex(op->title_id, info.output_path, &repair,
                                       op->result, game.source_size,
                                       op->apr_indexed,
                                       op->ampr_hot_swap_optimized);
      return 0;
    }
    {
      char restore_err[256] = {0};
      if(mount_switch_restore_after_operation(op, hidden, hidden_count,
                                              restore_err,
                                              sizeof(restore_err)) != 0) {
        gc_log("compress smoke restore failed title=%s err=%s", op->title_id,
               restore_err[0] ? restore_err : "unknown");
      }
    }
    snprintf(op->error, sizeof(op->error), "%s",
             err[0] ? err : "repair smoke verification failed");
    gc_log("compress smoke verify failed title=%s err=%s", op->title_id,
           op->error);
    COMPRESS_FAIL_RETURN();
  }

  (void)write_validation_marker_ex(op->title_id, info.output_path, &repair,
                                   op->result, game.source_size,
                                   op->apr_indexed,
                                   op->ampr_hot_swap_optimized);
  char final_result[32];
  snprintf(final_result, sizeof(final_result), "%s", op->result);
  if(delete_after) {
    if(compress_delete_source_after_success(op, &game, hidden,
                                            hidden_count) != 0) {
      COMPRESS_FAIL_RETURN();
    }
    snprintf(final_result, sizeof(final_result), "%s",
             repair.repaired_blocks > 0 ? "repaired" : "success");
  } else if(preserve_hide) {
    char preserve_err[256] = {0};
    gc_checkpoint("compress preserve original");
    job_set_current("Preserving original source");
    if(preserve_original_source_after_success(op, &game, hidden, hidden_count,
                                              preserve_err,
                                              sizeof(preserve_err)) != 0) {
      char restore_err[256] = {0};
      snprintf(op->error, sizeof(op->error), "%s",
               preserve_err[0] ? preserve_err :
               "could not preserve original source");
      gc_log("compress preserve original failed title=%s err=%s",
             op->title_id, op->error);
      (void)mount_switch_restore_after_operation(op, hidden, hidden_count,
                                                 restore_err,
                                                 sizeof(restore_err));
      COMPRESS_FAIL_RETURN();
    }
  }
  if(compress_restore_duplicates_after_success(op, hidden,
                                               hidden_count, 0) != 0) {
    COMPRESS_FAIL_RETURN();
  }
  snprintf(op->result, sizeof(op->result), "%s", final_result);
  uint64_t final_compressed_size =
      source_size_bytes_exact(info.output_path, GC_SOURCE_COMPRESSED);
  operation_store_compression_stats(op, game.source_size, info.output_path);
  uint64_t saved_bytes = game.source_size > final_compressed_size
      ? game.source_size - final_compressed_size
      : 0;
  gc_log("compress complete title=%s result=%s repaired=%llu saved=%llu",
         op->title_id, op->result,
         (unsigned long long)op->repaired_blocks,
         (unsigned long long)saved_bytes);
  return 0;
}
#undef COMPRESS_FAIL_RETURN

static int
run_extract_image_op(gc_operation_t *op) {
  gc_game_t game = {0};
  gc_game_t output_game = {0};
  gc_hidden_instance_t hidden[GC_MAX_GAMES];
  char err[256] = {0};
  char restore_err[256] = {0};
  char target_path[1024] = {0};
  char temp_path[1024] = {0};
  struct stat st;
  pfs_app_info_t output_probe = {0};
  uint64_t free_bytes = 0;
  uint64_t copied = 0;
  size_t hidden_count = 0;
  int mount_missed = 0;

  gc_checkpoint("extract find game");
  gc_log("extract start op=%s title=%s", op->id, op->title_id);
  append_operation_phase(op, "resolving");
  job_set_phase("resolving", 0, 0, "Resolving mounted image");
  if(find_game_for_operation_source_path(op, &game, 0) != 0 ||
     game.source_kind != GC_SOURCE_IMAGE) {
    snprintf(op->error, sizeof(op->error), "%s",
             "mounted image is unavailable");
    gc_log("extract failed title=%s err=%s", op->title_id, op->error);
    return -1;
  }
  snprintf(op->source_path, sizeof(op->source_path), "%s", game.source_path);
  snprintf(op->source_kind, sizeof(op->source_kind), "%s", "image");
  if(!game.is_mounted || !game.mount_path[0] ||
     stat(game.mount_path, &st) != 0 || !S_ISDIR(st.st_mode)) {
    snprintf(op->error, sizeof(op->error), "%s",
             "image must be mounted before extract");
    gc_log("extract failed title=%s err=%s", op->title_id, op->error);
    return -1;
  }
  if(build_extract_image_target_path(&game, target_path, sizeof(target_path),
                                     err, sizeof(err)) != 0) {
    snprintf(op->error, sizeof(op->error), "%s", err);
    gc_log("extract failed title=%s err=%s", op->title_id, op->error);
    return -1;
  }
  snprintf(op->output_path, sizeof(op->output_path), "%s", target_path);
  if(stat(target_path, &st) == 0) {
    snprintf(op->error, sizeof(op->error), "%s", "target folder already exists");
    gc_log("extract failed title=%s err=%s", op->title_id, op->error);
    return -1;
  }
  if(errno != ENOENT) {
    snprintf(op->error, sizeof(op->error), "check target folder: %s",
             strerror(errno));
    gc_log("extract failed title=%s err=%s", op->title_id, op->error);
    return -1;
  }
  if(game.source_size == 0) {
    snprintf(op->error, sizeof(op->error), "%s",
             "could not measure mounted image size");
    gc_log("extract failed title=%s err=%s", op->title_id, op->error);
    return -1;
  }
  if(free_bytes_for_output(target_path, &free_bytes) != 0 ||
     free_bytes < game.source_size) {
    uint64_t need = free_bytes < game.source_size
        ? game.source_size - free_bytes
        : game.source_size;
    snprintf(op->error, sizeof(op->error),
             "not enough free storage; free %llu more bytes",
             (unsigned long long)need);
    gc_log("extract failed title=%s err=%s", op->title_id, op->error);
    return -1;
  }
  if(close_title_if_running(op->title_id, err, sizeof(err)) != 0) {
    snprintf(op->error, sizeof(op->error), "%s",
             err[0] ? err : "could not close game");
    gc_log("extract close game failed title=%s err=%s", op->title_id,
           op->error);
    return -1;
  }
  if(hidden_sibling_temp_path(target_path, "gc-extracting",
                              temp_path, sizeof(temp_path)) != 0) {
    snprintf(op->error, sizeof(op->error), "%s",
             "temporary extract path too long");
    return -1;
  }
  remove_tree_gc(temp_path);
  append_operation_phase(op, "copying");
  job_set_phase("copying", 0, 0, "Extracting mounted image");
  job_store_u64(&g_job.total_bytes, game.source_size);
  atomic_store(&g_job.copied_bytes, 0);
  gc_checkpoint("extract copy");
  if(copy_tree_gc(game.mount_path, temp_path, &copied, err, sizeof(err)) != 0) {
    snprintf(op->error, sizeof(op->error), "%s",
             err[0] ? err : "extract failed");
    remove_tree_gc(temp_path);
    gc_log("extract copy failed title=%s err=%s", op->title_id, op->error);
    return -1;
  }
  job_set_current("Finalizing extracted folder");
  if(rename(temp_path, target_path) != 0) {
    snprintf(op->error, sizeof(op->error), "rename extracted folder: %s",
             strerror(errno));
    remove_tree_gc(temp_path);
    gc_log("extract finalize failed title=%s err=%s", op->title_id,
           op->error);
    return -1;
  }
  fsync_parent_dir_best_effort(target_path);
  if(pfs_app_probe(target_path, &output_probe, err, sizeof(err)) != 0) {
    snprintf(op->error, sizeof(op->error), "%s",
             err[0] ? err : "extracted folder is not valid");
    cleanup_failed_uncompress_output(target_path, op->title_id);
    gc_log("extract verify failed title=%s err=%s", op->title_id, op->error);
    return -1;
  }
  if(strcmp(output_probe.title_id, op->title_id)) {
    snprintf(op->error, sizeof(op->error), "%s",
             "extracted folder has wrong title id");
    cleanup_failed_uncompress_output(target_path, op->title_id);
    gc_log("extract verify failed title=%s err=%s", op->title_id, op->error);
    return -1;
  }

  output_game = game;
  snprintf(output_game.source_path, sizeof(output_game.source_path), "%s",
           target_path);
  snprintf(output_game.output_path, sizeof(output_game.output_path), "%s",
           output_probe.output_path);
  snprintf(output_game.mount_path, sizeof(output_game.mount_path), "%s",
           target_path);
  output_game.source_kind = GC_SOURCE_FOLDER;
  output_game.source_size = copied;
  output_game.required_bytes = copied;
  output_game.output_exists = output_probe.output_exists;
  set_game_mount_status(&output_game, 0, "not-mounted");

  gc_checkpoint("extract mount output");
  err[0] = 0;
  if(mount_selected_instance_hidden_exclusive(
         op, &output_game, hidden, GC_MAX_GAMES, &hidden_count, &mount_missed,
         err, sizeof(err)) != 0) {
    if(mount_switch_restore_after_operation(op, hidden, hidden_count,
                                            restore_err,
                                            sizeof(restore_err)) != 0) {
      gc_log("extract mount restore failed title=%s err=%s", op->title_id,
             restore_err[0] ? restore_err : "unknown");
      operation_append_error_detail(op, restore_err);
    }
    snprintf(op->error, sizeof(op->error), "%s",
             err[0] ? err : "ShadowMountPlus remount failed");
    gc_log("extract remount failed title=%s err=%s", op->title_id, op->error);
    return -1;
  }
  if(mount_missed) {
    if(mount_switch_restore_after_operation(op, hidden, hidden_count,
                                            restore_err,
                                            sizeof(restore_err)) != 0) {
      snprintf(op->error, sizeof(op->error), "%s",
               restore_err[0] ? restore_err :
               "could not restore duplicate instances");
      gc_log("extract mount-missed restore failed title=%s err=%s",
             op->title_id, op->error);
      return -1;
    }
    operation_mark_not_mounted(op, err);
    gc_size_cache_store(target_path, copied);
    gc_log("extract complete not mounted title=%s output=%s detail=%s",
           op->title_id, target_path, err[0] ? err : "");
    return 0;
  }
  if(mount_switch_restore_after_operation_ex(op, hidden, hidden_count, 0,
                                             restore_err,
                                             sizeof(restore_err)) != 0) {
    snprintf(op->error, sizeof(op->error), "%s",
             restore_err[0] ? restore_err :
             "could not restore duplicate instances");
    gc_log("extract restore failed title=%s err=%s", op->title_id, op->error);
    return -1;
  }
  gc_size_cache_store(target_path, copied);
  snprintf(op->result, sizeof(op->result), "%s", "success");
  gc_log("extract complete title=%s output=%s copied=%llu",
         op->title_id, target_path, (unsigned long long)copied);
  return 0;
}

static void operation_append_error_detail(gc_operation_t *op,
                                          const char *detail);

static int
run_uncompress_op(gc_operation_t *op) {
  gc_game_t game = {0};
  gc_game_t output_game = {0};
  gc_hidden_instance_t hidden[GC_MAX_GAMES];
  char err[256] = {0};
  char restore_err[256] = {0};
  pfs_decompress_info_t info = {0};
  gc_source_quarantine_t source_quarantine = {0};
  char target_path[1024] = {0};
  int as_image = !strcmp(op->format, "image");
  int delete_after = !strcmp(op->delete_policy, "after");
  int pfs_delete_policy = PFS_DELETE_KEEP;
  size_t hidden_count = 0;
  int mount_missed = 0;

  gc_checkpoint("uncompress find game");
  gc_log("uncompress start op=%s title=%s policy=%s mode=%s", op->id,
         op->title_id, op->delete_policy, as_image ? "image" : "app");
  append_operation_phase(op, "resolving");
  job_set_phase("resolving", 0, 0, "Resolving selected source");
  if(find_game_for_operation_source_path(op, &game, 0) != 0 ||
     game.source_kind != GC_SOURCE_COMPRESSED) {
    snprintf(op->error, sizeof(op->error), "%s", "compressed game is unavailable");
    gc_log("uncompress failed title=%s err=%s", op->title_id, op->error);
    return -1;
  }
  snprintf(op->source_path, sizeof(op->source_path), "%s", game.source_path);
  snprintf(op->source_kind, sizeof(op->source_kind), "%s", "compressed");
  if(!strcmp(op->delete_policy, "stream")) {
    snprintf(op->error, sizeof(op->error), "%s",
             "stream unpack is not supported");
    gc_log("uncompress failed title=%s err=%s", op->title_id, op->error);
    return -1;
  }
  if(op->target_root[0] && mkdirs(op->target_root) != 0) {
    snprintf(op->error, sizeof(op->error), "%s",
             "create uncompress target root failed");
    gc_log("uncompress failed title=%s err=%s", op->title_id, op->error);
    return -1;
  }
  if(op->target_root[0]) {
    if(build_uncompress_target_path(op->target_root, &game, target_path,
                                    sizeof(target_path), err,
                                    sizeof(err)) != 0) {
      snprintf(op->error, sizeof(op->error), "%s",
               err[0] ? err : "bad uncompress target");
      gc_log("uncompress failed title=%s err=%s", op->title_id, op->error);
      return -1;
    }
    if(!strcmp(game.output_path, target_path)) {
      snprintf(op->error, sizeof(op->error), "%s",
               "game is already in that location");
      gc_log("uncompress failed title=%s err=%s", op->title_id, op->error);
      return -1;
    }
  } else if(op->output_path[0]) {
    snprintf(target_path, sizeof(target_path), "%s", op->output_path);
  }
  append_operation_phase(op, "preflight");
  job_set_phase("preflight", 0, 0, "Inspecting compressed image");
  if(prepare_uncompress_plan(&game, as_image,
                             target_path[0] ? target_path : NULL,
                             NULL,
                             err, sizeof(err)) != 0) {
    snprintf(op->error, sizeof(op->error), "%s",
             err[0] ? err : "could not inspect compressed image");
    gc_log("uncompress failed title=%s err=%s", op->title_id, op->error);
    return -1;
  }
  snprintf(op->output_path, sizeof(op->output_path), "%s", game.output_path);
  if(close_title_if_running(op->title_id, err, sizeof(err)) != 0) {
    snprintf(op->error, sizeof(op->error), "%s",
             err[0] ? err : "could not close game");
    gc_log("uncompress close game failed title=%s err=%s", op->title_id,
           op->error);
    return -1;
  }
  append_operation_phase(op, "unpacking");
  job_set_phase("unpacking", 0, 0,
                as_image ? "Unpacking image" : "Unpacking game");
  gc_log("uncompress source title=%s size=%llu free=%llu required=%llu path=%s output=%s",
         op->title_id, (unsigned long long)game.source_size,
         (unsigned long long)game.free_bytes,
         (unsigned long long)game.required_bytes,
         game.source_path, game.output_path);
  gc_checkpoint("uncompress preflight");
  if(game.source_size == 0 || game.required_bytes == 0) {
    snprintf(op->error, sizeof(op->error), "%s",
             "could not measure game size");
    gc_log("uncompress failed title=%s err=%s", op->title_id, op->error);
    return -1;
  }
  if(game.free_bytes < game.required_bytes) {
    snprintf(op->error, sizeof(op->error),
             "not enough free storage; free %llu more bytes",
             (unsigned long long)(game.required_bytes - game.free_bytes));
    gc_log("uncompress failed title=%s err=%s", op->title_id, op->error);
    return -1;
  }
  gc_checkpoint("uncompress writing source");
  int decompress_rc = as_image
      ? pfs_decompress_ffpfsc_to_image_opts_output(game.source_path, 0,
                                            PFS_DECOMPRESS_DEFAULT_WORKERS,
                                            pfs_delete_policy,
                                            game.output_path,
                                            &info, err,
                                            sizeof(err))
      : pfs_decompress_ffpfsc_to_app_opts_output(game.source_path, 0,
                                          PFS_DECOMPRESS_DEFAULT_WORKERS,
                                          pfs_delete_policy,
                                          game.output_path,
	                                          &info, err,
	                                          sizeof(err));
  if(decompress_rc != 0) {
    snprintf(op->error, sizeof(op->error), "%s",
             err[0] ? err : "uncompress failed");
    gc_log("uncompress failed title=%s err=%s", op->title_id, op->error);
    return -1;
  }
  snprintf(op->output_path, sizeof(op->output_path), "%s", info.output_path);
  gc_log("uncompress wrote title=%s output=%s nested=%s mode=%s",
         op->title_id, info.output_path, info.nested_name,
         as_image ? "image" : "app");
  struct stat output_st;
  pfs_app_info_t output_probe = {0};
  char hint_err[256] = {0};
  err[0] = 0;
  if(as_image) {
    if(stat(info.output_path, &output_st) != 0 ||
       !S_ISREG(output_st.st_mode) ||
       pfs_image_probe(info.output_path, &output_probe, err,
                       sizeof(err)) != 0) {
      snprintf(op->error, sizeof(op->error), "%s",
               err[0] ? err : "uncompressed image is not valid");
      gc_log("uncompress output verify failed title=%s path=%s err=%s",
             op->title_id, info.output_path, op->error);
      cleanup_failed_uncompress_output(info.output_path, op->title_id);
      return -1;
    }
  } else {
    if(stat(info.output_path, &output_st) != 0 ||
       !S_ISDIR(output_st.st_mode) ||
       pfs_app_probe(info.output_path, &output_probe, err, sizeof(err)) != 0) {
      snprintf(op->error, sizeof(op->error), "%s",
               err[0] ? err : "uncompressed app folder is not valid");
      gc_log("uncompress output verify failed title=%s path=%s err=%s",
             op->title_id, info.output_path, op->error);
      cleanup_failed_uncompress_output(info.output_path, op->title_id);
      return -1;
    }
  }

  if(delete_after) {
    gc_checkpoint("uncompress quarantine source");
    job_set_current("Hiding compressed source");
    err[0] = 0;
    if(quarantine_uncompress_source(game.source_path, op->title_id, op->id,
                                    &source_quarantine, err,
                                    sizeof(err)) != 0) {
      snprintf(op->error, sizeof(op->error), "%s",
               err[0] ? err : "could not quarantine compressed source");
      gc_log("uncompress source quarantine failed title=%s err=%s",
             op->title_id, op->error);
      return -1;
    }
  }

  if(delete_after) {
    gc_checkpoint("uncompress shadowmount hint cleanup");
    if(gc_shadowmount_remove_pfsc_hints(game.source_path, info.nested_name,
                                        info.nested_type, hint_err,
                                        sizeof(hint_err)) != 0) {
      gc_log("uncompress pfsc hint cleanup failed title=%s path=%s err=%s",
             op->title_id, game.source_path,
             hint_err[0] ? hint_err : "unknown");
    }
    hint_err[0] = 0;
    if(gc_shadowmount_remove_title_pfsc_hints(op->title_id, game.source_path,
                                              hint_err,
                                              sizeof(hint_err)) != 0) {
      gc_log("uncompress title hint cleanup failed title=%s path=%s err=%s",
             op->title_id, game.source_path,
             hint_err[0] ? hint_err : "unknown");
    }
  }

  output_game = game;
  snprintf(output_game.source_path, sizeof(output_game.source_path), "%s",
           info.output_path);
  snprintf(output_game.output_path, sizeof(output_game.output_path), "%s",
           info.output_path);
  output_game.source_kind = as_image ? GC_SOURCE_IMAGE : GC_SOURCE_FOLDER;
  if(as_image) {
    snprintf(output_game.image_path, sizeof(output_game.image_path), "%s",
             info.output_path);
    output_game.nested_type = output_probe.nested_type;
  }
  set_game_mount_status(&output_game, 0, "not-mounted");

  gc_checkpoint("uncompress mount output");
  err[0] = 0;
  if(mount_selected_instance_hidden_exclusive(
         op, &output_game, hidden, GC_MAX_GAMES, &hidden_count, &mount_missed,
         err, sizeof(err)) != 0) {
    if(mount_switch_restore_after_operation(op, hidden, hidden_count,
                                            restore_err,
                                            sizeof(restore_err)) != 0) {
      gc_log("uncompress mount restore failed title=%s err=%s",
             op->title_id, restore_err[0] ? restore_err : "unknown");
      operation_append_error_detail(op, restore_err);
    }
    snprintf(op->error, sizeof(op->error), "%s",
             err[0] ? err : "ShadowMountPlus remount failed");
    gc_log("uncompress remount failed title=%s err=%s", op->title_id,
           op->error);
    restore_quarantined_uncompress_source(&source_quarantine, op->title_id);
    return -1;
  }
  if(mount_missed) {
    if(mount_switch_restore_after_operation(op, hidden, hidden_count,
                                            restore_err,
                                            sizeof(restore_err)) != 0) {
      restore_quarantined_uncompress_source(&source_quarantine, op->title_id);
      snprintf(op->error, sizeof(op->error), "%s",
               restore_err[0] ? restore_err :
               "could not restore duplicate instances");
	    gc_log("uncompress mount-missed restore failed title=%s err=%s",
	           op->title_id, op->error);
	    return -1;
	  }
    return uncompress_complete_not_mounted(
        op, &source_quarantine, &info, as_image,
        err[0] ? err : "ShadowMountPlus did not mount uncompressed output",
        err, sizeof(err));
  }
  if(mount_switch_restore_after_operation_ex(op, hidden, hidden_count, 0,
                                             restore_err,
                                             sizeof(restore_err)) != 0) {
    restore_quarantined_uncompress_source(&source_quarantine, op->title_id);
    snprintf(op->error, sizeof(op->error), "%s",
             restore_err[0] ? restore_err :
             "could not restore duplicate instances");
    gc_log("uncompress restore failed title=%s err=%s", op->title_id,
           op->error);
    return -1;
  }
  if(uncompress_delete_quarantined_source_or_fail(op, &source_quarantine,
                                                  err, sizeof(err)) != 0) {
    return -1;
  }
  gc_size_cache_queue_measure(info.output_path);
  snprintf(op->result, sizeof(op->result), "%s", "success");
  gc_log("uncompress complete title=%s output=%s mode=%s",
         op->title_id, op->output_path, as_image ? "image" : "app");
  return 0;
}

static int
run_validate_repair_op(gc_operation_t *op) {
  gc_game_t game = {0};
  gc_hidden_instance_t hidden[GC_MAX_GAMES];
  char err[256] = {0};
  char restore_err[256] = {0};
  pfs_repair_info_t repair = {0};
  const char *repair_path = NULL;
  char final_result[32] = {0};
  uint64_t source_size = 0;
  uint64_t free_bytes = 0;
  uint64_t outer_fixed_bytes = 0;
  size_t hidden_count = 0;
  int mount_missed = 0;

  memset(hidden, 0, sizeof(hidden));
  gc_checkpoint("validate find game");
  gc_log("validate start op=%s title=%s", op->id, op->title_id);
  append_operation_phase(op, "resolving");
  job_set_phase("resolving", 0, 0, "Resolving selected source");
  if(find_game_for_operation_source_path(op, &game, 0) != 0 ||
     game.source_kind != GC_SOURCE_COMPRESSED) {
    snprintf(op->error, sizeof(op->error), "%s",
             "compressed game is unavailable");
    gc_log("validate failed title=%s err=%s", op->title_id, op->error);
    return -1;
  }
  snprintf(op->source_path, sizeof(op->source_path), "%s", game.source_path);
  snprintf(op->output_path, sizeof(op->output_path), "%s", game.source_path);
  snprintf(op->source_kind, sizeof(op->source_kind), "%s", "compressed");
  repair_path = game.source_path;
  source_size = game.source_size;
  free_bytes = game.free_bytes;
  if(close_title_if_running(op->title_id, err, sizeof(err)) != 0) {
    snprintf(op->error, sizeof(op->error), "%s",
             err[0] ? err : "could not close game");
    gc_log("validate close game failed title=%s err=%s", op->title_id,
           op->error);
    return -1;
  }
  append_operation_phase(op, "repairing");
  job_set_phase("validating", 0, 0, "Preparing validation");
  gc_log("validate source title=%s path=%s size=%llu free=%llu validation=%s",
         op->title_id, repair_path ? repair_path : "",
         (unsigned long long)source_size,
         (unsigned long long)free_bytes,
         game.validation_status);
  gc_checkpoint("validate outer slack cleanup");
  if(pfs_repair_ffpfsc_outer_slack(repair_path, &outer_fixed_bytes,
                                   err, sizeof(err)) != 0) {
    snprintf(op->error, sizeof(op->error), "%s",
             err[0] ? err : "outer PFS slack cleanup failed");
    gc_log("validate outer slack cleanup failed title=%s err=%s",
           op->title_id, op->error);
    return -1;
  }
  gc_log("validate outer slack cleanup title=%s path=%s bytes=%llu",
         op->title_id, repair_path ? repair_path : "",
         (unsigned long long)outer_fixed_bytes);
  if(mount_selected_instance_hidden_exclusive(
         op, &game, hidden, GC_MAX_GAMES, &hidden_count, &mount_missed,
         err, sizeof(err)) != 0) {
    snprintf(op->error, sizeof(op->error), "%s",
             err[0] ? err : "could not mount selected source");
    gc_log("validate mount failed title=%s err=%s", op->title_id, op->error);
    if(mount_switch_restore_after_operation(op, hidden, hidden_count,
                                            restore_err,
                                            sizeof(restore_err)) != 0) {
      gc_log("validate mount restore failed title=%s err=%s",
             op->title_id, restore_err[0] ? restore_err : "unknown");
      operation_append_error_detail(op, restore_err);
    }
    return -1;
  }
  if(mount_missed) {
    snprintf(op->error, sizeof(op->error), "%s",
             err[0] ? err : "selected source was not mounted");
    gc_log("validate mount missed title=%s path=%s detail=%s",
           op->title_id, repair_path ? repair_path : "", op->error);
    if(mount_switch_restore_after_operation(op, hidden, hidden_count,
                                            restore_err,
                                            sizeof(restore_err)) != 0) {
      gc_log("validate mount-missed restore failed title=%s err=%s",
             op->title_id, restore_err[0] ? restore_err : "unknown");
      operation_append_error_detail(op, restore_err);
    }
    return -1;
  }
  gc_checkpoint("validate repair");
  if(repair_with_wait(op->title_id, repair_path, &repair, err,
                      sizeof(err)) != 0) {
    op->bad_blocks_found = repair.repaired_blocks;
    op->repaired_blocks = 0;
    operation_store_repair_counters(op, &repair);
    if(repair.outdir[0]) {
      snprintf(op->repair_summary, sizeof(op->repair_summary), "%s",
               repair.outdir);
    }
    snprintf(op->error, sizeof(op->error), "%s",
             err[0] ? err : "validate and repair failed");
    gc_log("validate repair failed title=%s err=%s", op->title_id, op->error);
    if(mount_switch_restore_after_operation(op, hidden, hidden_count,
                                            restore_err,
                                            sizeof(restore_err)) != 0) {
      gc_log("validate repair restore failed title=%s err=%s",
             op->title_id, restore_err[0] ? restore_err : "unknown");
      operation_append_error_detail(op, restore_err);
    }
    return -1;
  }
  operation_store_repair_success(op, &repair);
  snprintf(final_result, sizeof(final_result), "%s", op->result);
  if(repair_force_path_bounce_remount(op->title_id, repair_path, &repair,
                                      err, sizeof(err)) != 0) {
    if(shadowmount_mount_missed(err)) {
      operation_mark_verified_not_mounted(op, err);
      snprintf(final_result, sizeof(final_result), "%s", op->result);
      (void)write_validation_marker_ex(op->title_id, repair_path, &repair,
                                       op->result, 0, 0, 0);
      if(mount_switch_restore_after_operation_ex(op, hidden, hidden_count, 0,
                                                 restore_err,
                                                 sizeof(restore_err)) != 0) {
        snprintf(op->error, sizeof(op->error), "%s",
                 restore_err[0] ? restore_err :
                 "could not restore duplicate instances");
        gc_log("validate verified-not-mounted restore failed title=%s err=%s",
               op->title_id, op->error);
        return -1;
      }
      snprintf(op->result, sizeof(op->result), "%s", final_result);
      return 0;
    }
    snprintf(op->error, sizeof(op->error), "%s",
             err[0] ? err : "repair smoke verification failed");
    gc_log("validate smoke verify failed title=%s err=%s", op->title_id,
           op->error);
    if(mount_switch_restore_after_operation(op, hidden, hidden_count,
                                            restore_err,
                                            sizeof(restore_err)) != 0) {
      gc_log("validate smoke restore failed title=%s err=%s",
             op->title_id, restore_err[0] ? restore_err : "unknown");
      operation_append_error_detail(op, restore_err);
    }
    return -1;
  }
  (void)write_validation_marker_ex(op->title_id, repair_path, &repair,
                                   op->result, 0, 0, 0);
  if(mount_switch_restore_after_operation_ex(op, hidden, hidden_count, 0,
                                             restore_err,
                                             sizeof(restore_err)) != 0) {
    snprintf(op->error, sizeof(op->error), "%s",
             restore_err[0] ? restore_err :
             "could not restore duplicate instances");
    gc_log("validate restore failed title=%s err=%s", op->title_id,
           op->error);
    return -1;
  }
  snprintf(op->result, sizeof(op->result), "%s", final_result);
  gc_log("validate complete title=%s result=%s repaired=%llu",
         op->title_id, op->result,
         (unsigned long long)op->repaired_blocks);
  return 0;
}

static int
run_validate_only_op(gc_operation_t *op) {
  gc_game_t game = {0};
  gc_hidden_instance_t hidden[GC_MAX_GAMES];
  char err[256] = {0};
  char restore_err[256] = {0};
  pfs_repair_info_t repair = {0};
  const char *repair_path = NULL;
  char final_result[32] = {0};
  uint64_t source_size = 0;
  uint64_t free_bytes = 0;
  size_t hidden_count = 0;
  int mount_missed = 0;
  int scan_rc = -1;

  memset(hidden, 0, sizeof(hidden));
  gc_checkpoint("validate-only find game");
  gc_log("validate-only start op=%s title=%s", op->id, op->title_id);
  append_operation_phase(op, "resolving");
  job_set_phase("resolving", 0, 0, "Resolving selected source");
  if(find_game_for_operation_source_path(op, &game, 0) != 0 ||
     game.source_kind != GC_SOURCE_COMPRESSED) {
    snprintf(op->error, sizeof(op->error), "%s",
             "compressed game is unavailable");
    gc_log("validate-only failed title=%s err=%s",
           op->title_id, op->error);
    return -1;
  }
  snprintf(op->source_path, sizeof(op->source_path), "%s", game.source_path);
  snprintf(op->output_path, sizeof(op->output_path), "%s", game.source_path);
  snprintf(op->source_kind, sizeof(op->source_kind), "%s", "compressed");
  repair_path = game.source_path;
  source_size = game.source_size;
  free_bytes = game.free_bytes;
  if(close_title_if_running(op->title_id, err, sizeof(err)) != 0) {
    snprintf(op->error, sizeof(op->error), "%s",
             err[0] ? err : "could not close game");
    gc_log("validate-only close game failed title=%s err=%s",
           op->title_id, op->error);
    return -1;
  }
  append_operation_phase(op, "validating");
  job_set_phase("validating", 0, 0, "Preparing validation");
  gc_log("validate-only source title=%s path=%s size=%llu free=%llu validation=%s",
         op->title_id, repair_path ? repair_path : "",
         (unsigned long long)source_size,
         (unsigned long long)free_bytes,
         game.validation_status);
  if(mount_selected_instance_hidden_exclusive(
         op, &game, hidden, GC_MAX_GAMES, &hidden_count, &mount_missed,
         err, sizeof(err)) != 0) {
    snprintf(op->error, sizeof(op->error), "%s",
             err[0] ? err : "could not mount selected source");
    gc_log("validate-only mount failed title=%s err=%s",
           op->title_id, op->error);
    if(mount_switch_restore_after_operation(op, hidden, hidden_count,
                                            restore_err,
                                            sizeof(restore_err)) != 0) {
      gc_log("validate-only mount restore failed title=%s err=%s",
             op->title_id, restore_err[0] ? restore_err : "unknown");
      operation_append_error_detail(op, restore_err);
    }
    return -1;
  }
  if(mount_missed) {
    snprintf(op->error, sizeof(op->error), "%s",
             err[0] ? err : "selected source was not mounted");
    gc_log("validate-only mount missed title=%s path=%s detail=%s",
           op->title_id, repair_path ? repair_path : "", op->error);
    if(mount_switch_restore_after_operation(op, hidden, hidden_count,
                                            restore_err,
                                            sizeof(restore_err)) != 0) {
      gc_log("validate-only mount-missed restore failed title=%s err=%s",
             op->title_id, restore_err[0] ? restore_err : "unknown");
      operation_append_error_detail(op, restore_err);
    }
    return -1;
  }
  gc_checkpoint("validate-only scan");
  if(repair_scan_only_with_wait(op->title_id, repair_path, &repair, &scan_rc,
                                err, sizeof(err)) != 0) {
    op->bad_blocks_found = repair.repaired_blocks;
    op->repaired_blocks = 0;
    operation_store_repair_counters(op, &repair);
    if(repair.outdir[0]) {
      snprintf(op->repair_summary, sizeof(op->repair_summary), "%s",
               repair.outdir);
    }
    snprintf(op->error, sizeof(op->error), "%s",
             err[0] ? err : "validate-only scan failed");
    gc_log("validate-only failed title=%s err=%s", op->title_id, op->error);
    if(mount_switch_restore_after_operation(op, hidden, hidden_count,
                                            restore_err,
                                            sizeof(restore_err)) != 0) {
      gc_log("validate-only scan restore failed title=%s err=%s",
             op->title_id, restore_err[0] ? restore_err : "unknown");
      operation_append_error_detail(op, restore_err);
    }
    return -1;
  }

  op->bad_blocks_found = repair.repaired_blocks;
  op->repaired_blocks = 0;
  operation_store_repair_counters(op, &repair);
  snprintf(op->repair_summary, sizeof(op->repair_summary), "%s",
           repair.outdir);
	  snprintf(final_result, sizeof(final_result), "%s",
	           scan_rc == PFS_REPAIR_SCAN_REPAIR_NEEDED ||
	               repair.repaired_blocks > 0 ? "bad-blocks-found" : "clean");
	  snprintf(op->result, sizeof(op->result), "%s", final_result);
	  if(repair.repaired_blocks > 0) {
	    delete_validation_marker_for_path(op->title_id, repair_path);
	  }
  gc_checkpoint("validate-only force remount");
  if(force_compressed_path_bounce_remount(op->title_id, repair_path,
                                          repair.nested_name,
                                          repair.nested_type,
                                          err, sizeof(err)) != 0) {
    if(shadowmount_mount_missed(err)) {
      if(repair.repaired_blocks == 0) {
        operation_mark_verified_not_mounted(op, err);
        (void)write_validation_marker_ex(op->title_id, repair_path, &repair,
                                         op->result, 0, 0, 0);
      } else {
	        snprintf(op->result, sizeof(op->result), "%s",
	                 "bad-blocks-found-not-mounted");
	        snprintf(final_result, sizeof(final_result), "%s", op->result);
	        op->error[0] = 0;
	        delete_validation_marker_for_path(op->title_id, repair_path);
	        gc_log("validate-only complete but not mounted title=%s bad=%llu detail=%s",
               op->title_id, (unsigned long long)op->bad_blocks_found,
               err[0] ? err : "");
      }
      if(mount_switch_restore_after_operation_ex(op, hidden, hidden_count, 0,
                                                 restore_err,
                                                 sizeof(restore_err)) != 0) {
        snprintf(op->error, sizeof(op->error), "%s",
                 restore_err[0] ? restore_err :
                 "could not restore duplicate instances");
        gc_log("validate-only not-mounted restore failed title=%s err=%s",
               op->title_id, op->error);
        return -1;
      }
      snprintf(op->result, sizeof(op->result), "%s", final_result);
      return 0;
    }
    snprintf(op->error, sizeof(op->error), "%s",
             err[0] ? err : "ShadowMountPlus remount failed");
    gc_log("validate-only remount failed title=%s err=%s",
           op->title_id, op->error);
    if(mount_switch_restore_after_operation(op, hidden, hidden_count,
                                            restore_err,
                                            sizeof(restore_err)) != 0) {
      gc_log("validate-only remount restore failed title=%s err=%s",
             op->title_id, restore_err[0] ? restore_err : "unknown");
      operation_append_error_detail(op, restore_err);
    }
    return -1;
  }
  if(repair.repaired_blocks == 0) {
    (void)write_validation_marker_ex(op->title_id, repair_path, &repair,
                                     op->result, 0, 0, 0);
	  } else {
	    delete_validation_marker_for_path(op->title_id, repair_path);
	  }
  if(mount_switch_restore_after_operation_ex(op, hidden, hidden_count, 0,
                                             restore_err,
                                             sizeof(restore_err)) != 0) {
    snprintf(op->error, sizeof(op->error), "%s",
             restore_err[0] ? restore_err :
             "could not restore duplicate instances");
    gc_log("validate-only restore failed title=%s err=%s", op->title_id,
           op->error);
    return -1;
  }
  snprintf(op->result, sizeof(op->result), "%s", final_result);
  gc_log("validate-only complete title=%s result=%s bad=%llu",
         op->title_id, op->result,
         (unsigned long long)op->bad_blocks_found);
  return 0;
}

static void
operation_append_error_detail(gc_operation_t *op, const char *detail) {
  if(!op || !detail || !detail[0]) return;
  if(op->error[0]) {
    size_t used = strlen(op->error);
    if(used < sizeof(op->error)) {
      snprintf(op->error + used, sizeof(op->error) - used, "; %s", detail);
    }
  } else {
    snprintf(op->error, sizeof(op->error), "%s", detail);
  }
}

static void
refresh_mount_restore_cleared_links_after_failure(gc_operation_t *op,
                                                  gc_mount_link_backup_t *backup,
                                                  char *restore_err,
                                                  size_t restore_err_size) {
  restore_err[0] = 0;
  if(mount_switch_restore_cleared_links(backup, restore_err,
                                        restore_err_size) != 0) {
    gc_log("refresh-mount link restore after failure failed title=%s err=%s",
           op->title_id, restore_err[0] ? restore_err : "unknown");
    operation_append_error_detail(op, restore_err);
  }
}

static void
refresh_mount_request_restore_scan(gc_operation_t *op,
                                   char *scan_err,
                                   size_t scan_err_size) {
  scan_err[0] = 0;
  if(job_cancelled()) {
    gc_log("refresh-mount restore scan skipped after cancel title=%s",
           op->title_id);
  } else if(gc_shadowmount_request_scan_cancelable(scan_err,
                                                   scan_err_size) != 0 &&
            !job_cancelled()) {
    gc_log("refresh-mount restore scan failed title=%s err=%s",
           op->title_id, scan_err[0] ? scan_err : "unknown");
  }
}

static int
run_refresh_mount_op(gc_operation_t *op) {
  gc_game_t game = {0};
  char err[256] = {0};
  char restore_err[256] = {0};
  char scan_err[256] = {0};
  char expected_mount[1024] = {0};
  char expected_image[1024] = {0};
  gc_hidden_instance_t hidden[GC_MAX_GAMES];
  gc_mount_link_backup_t link_backup;
  size_t hidden_count = 0;
  int was_validated = 0;
  int mount_ready = 0;
  int mount_missed = 0;

  memset(&link_backup, 0, sizeof(link_backup));
  gc_checkpoint("refresh-mount find game");
  gc_log("refresh-mount start op=%s title=%s", op->id, op->title_id);
  append_operation_phase(op, "resolving");
  job_set_phase("resolving", 0, 0, "Resolving selected source");
  if(find_game_for_operation_source_path(op, &game, 0) != 0 ||
     game.source_kind == GC_SOURCE_UNKNOWN) {
    snprintf(op->error, sizeof(op->error), "%s",
             job_cancelled() ? "cancelled" : "game instance is unavailable");
    gc_log("refresh-mount failed title=%s err=%s", op->title_id, op->error);
    return -1;
  }
  if(gc_cancel_requested(op->error, sizeof(op->error))) return -1;

  snprintf(op->source_path, sizeof(op->source_path), "%s", game.source_path);
  snprintf(op->output_path, sizeof(op->output_path), "%s", game.source_path);
  snprintf(op->source_kind, sizeof(op->source_kind), "%s",
           source_kind_name(game.source_kind));
  was_validated = game.validation == GC_VALIDATION_VALIDATED;
  job_set_target(game.source_path);

  if(game.is_mounted) {
    snprintf(op->result, sizeof(op->result), "%s", "success");
    gc_log("refresh-mount already mounted title=%s path=%s", op->title_id,
           game.source_path);
    return 0;
  }
  if(gc_cancel_requested(op->error, sizeof(op->error))) return -1;

  if(close_title_if_running(op->title_id, err, sizeof(err)) != 0) {
    snprintf(op->error, sizeof(op->error), "%s",
             err[0] ? err : "could not close game");
    gc_log("refresh-mount failed title=%s err=%s", op->title_id, op->error);
    return -1;
  }

  if(move_remount_expectations(&game, game.source_path,
                               expected_mount, sizeof(expected_mount),
                               expected_image, sizeof(expected_image),
                               err, sizeof(err)) != 0) {
    snprintf(op->error, sizeof(op->error), "%s",
             err[0] ? err : "could not derive mount path");
    gc_log("refresh-mount expectation failed title=%s err=%s",
           op->title_id, op->error);
    return -1;
  }

  if(prepare_shadowmount_for_selected_source(&game, err, sizeof(err)) != 0) {
    snprintf(op->error, sizeof(op->error), "%s",
             err[0] ? err : "could not inspect selected source");
    gc_log("refresh-mount inspect failed title=%s err=%s", op->title_id,
           op->error);
    return -1;
  }
  if(gc_cancel_requested(op->error, sizeof(op->error))) return -1;

  gc_checkpoint("refresh-mount hide competitors");
  append_operation_phase(op, "hiding");
  job_set_phase("hiding", 0, 0, "Hiding other instances");
  if(mount_switch_hide_competitors(op, &game, hidden, GC_MAX_GAMES,
                                   &hidden_count, err, sizeof(err)) != 0) {
    snprintf(op->error, sizeof(op->error), "%s",
             err[0] ? err : "could not hide duplicate instances");
    gc_log("refresh-mount hide failed title=%s err=%s", op->title_id,
           op->error);
    goto restore_and_fail;
  }
  artifact_cache_invalidate();
  if(gc_cancel_requested(op->error, sizeof(op->error))) goto restore_and_fail;
  if(mount_switch_clear_stale_links(op->title_id, expected_mount,
                                    expected_image, &link_backup,
                                    err, sizeof(err)) != 0) {
    snprintf(op->error, sizeof(op->error), "%s",
             err[0] ? err : "could not clear stale mount state");
    gc_log("refresh-mount link clear failed title=%s err=%s", op->title_id,
           op->error);
    goto restore_and_fail;
  }
  if(mount_switch_clear_live_title_mount(op->title_id, err, sizeof(err)) != 0) {
    snprintf(op->error, sizeof(op->error), "%s",
             err[0] ? err : "could not clear live mount state");
    gc_log("refresh-mount live mount clear failed title=%s err=%s",
           op->title_id, op->error);
    goto restore_and_fail;
  }

  gc_checkpoint("refresh-mount mount selected");
  append_operation_phase(op, "mounting");
  job_set_phase("mounting", 0, 0, "Mounting");
  if(gc_shadowmount_request_title_source_scan_cancelable(
         game.title_id, game.source_path, scan_err, sizeof(scan_err)) != 0) {
    snprintf(op->error, sizeof(op->error), "%s",
             scan_err[0] ? scan_err : "could not request ShadowMount scan");
    gc_log("refresh-mount scan failed title=%s err=%s", op->title_id,
           op->error);
    goto restore_and_fail;
  }
  if(gc_cancel_requested(op->error, sizeof(op->error))) goto restore_and_fail;
  err[0] = 0;
  if(wait_for_shadowmount_links(op->title_id, expected_mount, expected_image,
                                err, sizeof(err)) == 0) {
    mount_ready = 1;
  } else if(shadowmount_mount_missed(err)) {
    mount_missed = 1;
    gc_log("refresh-mount selected instance not mounted title=%s path=%s detail=%s",
           op->title_id, game.source_path, err[0] ? err : "");
  } else {
    snprintf(op->error, sizeof(op->error), "%s",
             err[0] ? err : "ShadowMountPlus mount failed");
    gc_log("refresh-mount failed title=%s err=%s", op->title_id, op->error);
    goto restore_and_fail;
  }

  gc_checkpoint("refresh-mount restore competitors");
  append_operation_phase(op, "restoring");
  job_set_phase("restoring", 0, 0, "Restoring other instances");
  if(mount_switch_restore_hidden(op, hidden, hidden_count, restore_err,
                                 sizeof(restore_err)) != 0) {
    snprintf(op->error, sizeof(op->error), "%s",
             restore_err[0] ? restore_err : "could not restore duplicate instances");
    gc_log("refresh-mount restore failed title=%s err=%s", op->title_id,
           op->error);
    artifact_cache_invalidate();
    return -1;
  }
  artifact_cache_invalidate();
  if(!mount_ready &&
     mount_switch_restore_cleared_links(&link_backup, restore_err,
                                        sizeof(restore_err)) != 0) {
    snprintf(op->error, sizeof(op->error), "%s",
             restore_err[0] ? restore_err : "could not restore previous mount links");
    gc_log("refresh-mount link restore failed title=%s err=%s", op->title_id,
           op->error);
    return -1;
  }
  scan_err[0] = 0;
  if(mount_ready) {
    gc_log("refresh-mount post-restore scan skipped after selected mount title=%s",
           op->title_id);
  } else if(job_cancelled()) {
    gc_log("refresh-mount post-restore scan skipped after cancel title=%s",
           op->title_id);
  } else if(gc_shadowmount_request_scan_cancelable(scan_err,
                                                   sizeof(scan_err)) != 0 &&
            !job_cancelled()) {
    gc_log("refresh-mount post-restore scan failed title=%s err=%s",
           op->title_id, scan_err[0] ? scan_err : "unknown");
  }

  if(mount_ready) {
    snprintf(op->result, sizeof(op->result), "%s", "success");
    if(was_validated) {
      (void)write_validation_marker_ex(op->title_id, game.source_path, NULL,
                                       "validated", 0, 0,
                                       game.ampr_hot_swap_optimized);
    }
    gc_log("refresh-mount complete title=%s path=%s", op->title_id,
           game.source_path);
    return 0;
  }

  if(mount_missed) {
    snprintf(op->error, sizeof(op->error), "%s",
             err[0] ? err : "selected source was not mounted");
    gc_log("refresh-mount failed not mounted title=%s path=%s detail=%s",
           op->title_id, game.source_path, op->error);
    return -1;
  }

  snprintf(op->error, sizeof(op->error), "%s", "mount did not complete");
  return -1;

restore_and_fail:
  if(hidden_count > 0) {
    append_operation_phase(op, "restoring");
    job_set_phase("restoring", 0, 0, "Restoring other instances");
    restore_err[0] = 0;
    if(mount_switch_restore_hidden(op, hidden, hidden_count,
                                   restore_err, sizeof(restore_err)) != 0) {
      gc_log("refresh-mount restore after failure failed title=%s err=%s",
             op->title_id, restore_err[0] ? restore_err : "unknown");
      operation_append_error_detail(op, restore_err);
    }
    artifact_cache_invalidate();
    refresh_mount_restore_cleared_links_after_failure(op, &link_backup,
                                                      restore_err,
                                                      sizeof(restore_err));
    refresh_mount_request_restore_scan(op, scan_err, sizeof(scan_err));
  } else if(link_backup.cleared) {
    refresh_mount_restore_cleared_links_after_failure(op, &link_backup,
                                                      restore_err,
                                                      sizeof(restore_err));
    refresh_mount_request_restore_scan(op, scan_err, sizeof(scan_err));
  }
  return -1;
}

static int
read_speed_mount_root(const gc_game_t *game, char *out, size_t out_size,
                      char *err, size_t err_size) {
  char system_root[1024];
  struct stat st;
  if(!game || !valid_title_id(game->title_id) || !out || out_size == 0) {
    snprintf(err, err_size, "%s", "game is unavailable");
    return -1;
  }
  if(!game->is_mounted || !game->mount_path[0]) {
    snprintf(err, err_size, "%s", "game is not mounted");
    return -1;
  }
  int n = snprintf(system_root, sizeof(system_root), "%s/%s",
                   GC_SYSTEM_APP_BASE, game->title_id);
  if(n >= 0 && (size_t)n < sizeof(system_root) &&
     system_ex_title_bound_to(game->title_id, game->mount_path,
                              NULL, 0, NULL, 0, NULL, 0) &&
     stat(system_root, &st) == 0 && S_ISDIR(st.st_mode)) {
    snprintf(out, out_size, "%s", system_root);
    return 0;
  }
  if(stat(game->mount_path, &st) == 0 && S_ISDIR(st.st_mode)) {
    snprintf(out, out_size, "%s", game->mount_path);
    return 0;
  }
  snprintf(err, err_size, "%s", "mounted game folder is unavailable");
  return -1;
}

static int
run_set_read_only_op(gc_operation_t *op) {
  gc_game_t game = {0};
  char err[256] = {0};
  char scan_err[256] = {0};
  int already_present = 0;

  gc_checkpoint("set-read-only find game");
  gc_log("set-read-only start op=%s title=%s", op->id, op->title_id);
  append_operation_phase(op, "resolving");
  job_set_phase("resolving", 0, 0, "Resolving selected image");
  if(find_game_for_operation_source_path(op, &game, 0) != 0 ||
     game.source_kind != GC_SOURCE_IMAGE) {
    snprintf(op->error, sizeof(op->error), "%s",
             job_cancelled() ? "cancelled" : "game is not an image");
    gc_log("set-read-only failed title=%s err=%s", op->title_id, op->error);
    return -1;
  }
  if(gc_cancel_requested(op->error, sizeof(op->error))) return -1;

  snprintf(op->source_path, sizeof(op->source_path), "%s", game.source_path);
  snprintf(op->output_path, sizeof(op->output_path), "%s", game.source_path);
  snprintf(op->source_kind, sizeof(op->source_kind), "%s", "image");
  job_set_target(game.source_path);

  gc_checkpoint("set-read-only config");
  append_operation_phase(op, "configuring");
  job_set_phase("configuring", 0, 0, "Updating ShadowMount config");
  if(gc_shadowmount_ensure_image_read_only(game.source_path,
                                           &already_present,
                                           err, sizeof(err)) != 0) {
    snprintf(op->error, sizeof(op->error), "%s",
             err[0] ? err : "could not update ShadowMount config");
    gc_log("set-read-only failed title=%s err=%s", op->title_id, op->error);
    return -1;
  }

  if(already_present) {
    snprintf(op->result, sizeof(op->result), "%s", "already-read-only");
    gc_log("set-read-only skipped existing rule title=%s path=%s",
           op->title_id, game.source_path);
    return 0;
  }

  gc_checkpoint("set-read-only scan");
  append_operation_phase(op, "mounting");
  job_set_phase("mounting", 0, 0, "Requesting ShadowMount scan");
  if(gc_shadowmount_request_title_source_scan_cancelable(
         game.title_id, game.source_path, scan_err, sizeof(scan_err)) != 0) {
    snprintf(op->error, sizeof(op->error), "%s",
             scan_err[0] ? scan_err : "could not request ShadowMount scan");
    gc_log("set-read-only scan failed title=%s err=%s",
           op->title_id, op->error);
    return -1;
  }
  snprintf(op->result, sizeof(op->result), "%s", "read-only");
  gc_log("set-read-only complete title=%s path=%s",
         op->title_id, game.source_path);
  return 0;
}

static int
run_read_speed_test_op(gc_operation_t *op) {
  gc_game_t game = {0};
  gc_read_speed_ctx_t ctx;
  struct stat st;
  char read_root[1024] = {0};
  char err[256] = {0};
  uint64_t last_files_opened = 0;
  uint64_t last_bytes_read = 0;

  gc_checkpoint("read-speed find game");
  gc_log("read-speed start op=%s title=%s", op->id, op->title_id);
  append_operation_phase(op, "resolving");
  job_set_phase("resolving", 0, 0, "Resolving selected source");
  if(find_game_for_operation_source_path(op, &game, 0) != 0 ||
     game.source_kind == GC_SOURCE_UNKNOWN) {
    snprintf(op->error, sizeof(op->error), "%s", "game data is unavailable");
    gc_log("read-speed failed title=%s err=%s", op->title_id, op->error);
    return -1;
  }
  if(gc_cancel_requested(err, sizeof(err))) {
    snprintf(op->error, sizeof(op->error), "%s", err);
    return -1;
  }
  if(read_speed_mount_root(&game, read_root, sizeof(read_root),
                           err, sizeof(err)) != 0) {
    snprintf(op->error, sizeof(op->error), "%s",
             err[0] ? err : "game is not mounted");
    gc_log("read-speed mount unavailable title=%s err=%s",
           op->title_id, op->error);
    return -1;
  }
  if(stat(read_root, &st) != 0 || !S_ISDIR(st.st_mode)) {
    snprintf(op->error, sizeof(op->error), "%s",
             "mounted game folder is unavailable");
    gc_log("read-speed stat failed title=%s root=%s err=%s",
           op->title_id, read_root, strerror(errno));
    return -1;
  }

  snprintf(op->source_path, sizeof(op->source_path), "%s", game.source_path);
  snprintf(op->output_path, sizeof(op->output_path), "%s", read_root);
  snprintf(op->source_kind, sizeof(op->source_kind), "%s",
           source_kind_name(game.source_kind));
  snprintf(op->format, sizeof(op->format), "%s", "read");
  snprintf(op->delete_policy, sizeof(op->delete_policy), "%s", "none");
  job_set_target(read_root);

  memset(&ctx, 0, sizeof(ctx));
  ctx.buf = malloc(GC_COPY_CHUNK_SIZE);
  if(!ctx.buf) {
    snprintf(op->error, sizeof(op->error), "%s", "out of memory");
    return -1;
  }
  ctx.started_ms = monotonic_millis_gc();
  ctx.deadline_ms = ctx.started_ms + GC_READ_SPEED_TEST_SECONDS * 1000ULL;

  gc_checkpoint("read-speed scanning");
  append_operation_phase(op, "read-test");
  job_set_phase("read-test", 0, GC_READ_SPEED_TEST_SECONDS,
                "Testing read speed");
  atomic_store(&g_job.total_bytes, 0);
  atomic_store(&g_job.copied_bytes, 0);
  gc_log("read-speed reading title=%s root=%s seconds=%d",
         op->title_id, read_root, GC_READ_SPEED_TEST_SECONDS);

  while(!read_speed_time_expired(&ctx)) {
    if(gc_cancel_requested(err, sizeof(err))) {
      snprintf(op->error, sizeof(op->error), "%s", err);
      free(ctx.buf);
      return -1;
    }
    last_files_opened = ctx.files_opened;
    last_bytes_read = ctx.bytes_read;
    err[0] = 0;
    if(read_speed_walk(read_root, &ctx, err, sizeof(err)) != 0) {
      snprintf(op->error, sizeof(op->error), "%s",
               err[0] ? err : "read speed test failed");
      free(ctx.buf);
      return -1;
    }
    read_speed_update_progress(&ctx);
    if(ctx.files_opened == last_files_opened) {
      snprintf(op->error, sizeof(op->error), "%s",
               "no readable files found");
      free(ctx.buf);
      return -1;
    }
    if(ctx.bytes_read == last_bytes_read) {
      snprintf(op->error, sizeof(op->error), "%s",
               "no readable file data found");
      free(ctx.buf);
      return -1;
    }
  }
  read_speed_update_progress(&ctx);
  if(gc_cancel_requested(err, sizeof(err))) {
    snprintf(op->error, sizeof(op->error), "%s", err);
    free(ctx.buf);
    return -1;
  }
  job_set_phase("read-test", GC_READ_SPEED_TEST_SECONDS,
                GC_READ_SPEED_TEST_SECONDS, "Read speed test complete");

  op->compression_source_size = ctx.bytes_read;
  op->compressed_size = ctx.bytes_read;
  op->saved_bytes = 0;
  snprintf(op->result, sizeof(op->result), "%s", "tested");
  gc_log("read-speed complete title=%s root=%s bytes=%llu files=%llu",
         op->title_id, read_root,
         (unsigned long long)ctx.bytes_read,
         (unsigned long long)ctx.files_opened);
  free(ctx.buf);
  return 0;
}

static int
run_delete_game_data_op(gc_operation_t *op) {
  gc_game_t game = {0};
  char err[256] = {0};
  char scan_err[256] = {0};
  char delete_path[1024] = {0};
  const char *physical_delete_path = NULL;

  gc_checkpoint("delete find game");
  gc_log("delete start op=%s title=%s", op->id, op->title_id);
  append_operation_phase(op, "resolving");
  job_set_phase("resolving", 0, 0, "Resolving selected source");
  if(find_game_for_operation_source_path(op, &game, 0) != 0 ||
     game.source_kind == GC_SOURCE_UNKNOWN) {
    snprintf(op->error, sizeof(op->error), "%s", "game data is unavailable");
    gc_log("delete failed title=%s err=%s", op->title_id, op->error);
    return -1;
  }
  if(gc_cancel_requested(err, sizeof(err))) {
    snprintf(op->error, sizeof(op->error), "%s", err);
    return -1;
  }
  if(!game_source_delete_allowed(&game)) {
    snprintf(op->error, sizeof(op->error), "%s",
             "game source cannot be deleted from this location");
    gc_log("delete denied title=%s path=%s", op->title_id,
           game.source_path);
    return -1;
  }

  snprintf(op->source_path, sizeof(op->source_path), "%s", game.source_path);
  snprintf(op->output_path, sizeof(op->output_path), "%s", game.source_path);
  snprintf(op->source_kind, sizeof(op->source_kind), "%s",
           source_kind_name(game.source_kind));
  job_set_target(game.source_path);

  if(close_title_if_running(op->title_id, err, sizeof(err)) != 0) {
    snprintf(op->error, sizeof(op->error), "%s",
             err[0] ? err : "could not close game");
    gc_log("delete close game failed title=%s err=%s",
           op->title_id, op->error);
    return -1;
  }
  if(gc_cancel_requested(err, sizeof(err))) {
    snprintf(op->error, sizeof(op->error), "%s", err);
    return -1;
  }

  gc_checkpoint("delete source");
  gc_job_set_cancel_disabled("delete is removing game data");
  append_operation_phase(op, "deleting");
  job_set_phase("deleting", 0, 0, "Deleting game data");
  cleanup_shadowmount_hints_for_deleted_source(op, &game);
  if(game.source_kind == GC_SOURCE_COMPRESSED) {
    delete_validation_marker_for_path(op->title_id, game.source_path);
  }
  if(game.source_kind == GC_SOURCE_FOLDER) {
    if(hidden_sibling_temp_path(game.source_path, "gc-delete", delete_path,
                                sizeof(delete_path)) != 0) {
      snprintf(op->error, sizeof(op->error), "delete temp path: %s",
               strerror(errno));
      gc_log("delete source failed title=%s err=%s", op->title_id, op->error);
      return -1;
    }
    job_set_current("Preparing delete path");
    remove_tree_gc(delete_path);
    if(rename(game.source_path, delete_path) != 0) {
      snprintf(op->error, sizeof(op->error), "hide source for delete: %s",
               strerror(errno));
      gc_log("delete source failed title=%s err=%s", op->title_id, op->error);
      return -1;
    }
    physical_delete_path = delete_path;
    gc_size_cache_forget(game.source_path);
    gc_size_cache_forget(delete_path);
    artifact_cache_invalidate();
    job_set_phase("mounting", 0, 0, "Requesting ShadowMount scan");
    if(gc_shadowmount_request_scan_cancelable(scan_err, sizeof(scan_err)) != 0) {
      gc_log("delete scan request failed title=%s err=%s", op->title_id,
             scan_err[0] ? scan_err : "unknown");
    }
    append_operation_phase(op, "deleting");
    job_set_phase("deleting", 0, 0, "Removing hidden game data");
  } else {
    physical_delete_path = game.source_path;
  }
  if(delete_source_after_success_with_title(physical_delete_path,
                                            game.source_kind, op->title_id,
                                            err, sizeof(err)) != 0) {
    if(delete_path[0] && physical_delete_path == delete_path) {
      char detail[256];
      snprintf(detail, sizeof(detail), "%s; remaining path: %s",
               err[0] ? err : "delete game data failed", delete_path);
      snprintf(err, sizeof(err), "%s", detail);
    }
    snprintf(op->error, sizeof(op->error), "%s",
             err[0] ? err : "delete game data failed");
    gc_log("delete source failed title=%s err=%s", op->title_id,
           op->error);
    return -1;
  }
  gc_size_cache_forget(game.source_path);
  if(delete_path[0]) gc_size_cache_forget(delete_path);
  artifact_cache_invalidate();

  gc_checkpoint("delete request shadowmount scan");
  job_set_phase("mounting", 0, 0, "Requesting ShadowMount scan");
  if(job_cancelled()) {
    gc_log("delete scan skipped after cancel title=%s", op->title_id);
  } else if(gc_shadowmount_request_scan_cancelable(scan_err,
                                                   sizeof(scan_err)) != 0 &&
            !job_cancelled()) {
    gc_log("delete scan request failed title=%s err=%s", op->title_id,
           scan_err[0] ? scan_err : "unknown");
  }

  snprintf(op->result, sizeof(op->result), "%s", "deleted");
  gc_log("delete complete title=%s path=%s", op->title_id,
         game.source_path);
  return 0;
}

static const char *
ui_theme_normalize(const char *theme) {
  return theme && !strcasecmp(theme, "dark") ? "dark" : "light";
}

static int
ui_settings_theme_read(char *out, size_t out_size) {
  char *json = NULL;
  size_t json_size = 0;
  char theme[16] = {0};
  const char *normalized;

  if(!out || out_size == 0) return -1;
  snprintf(out, out_size, "%s", "light");
  if(read_file_limited(GC_UI_SETTINGS_FILE, &json, &json_size,
                       64 * 1024) != 0) {
    return 0;
  }
  (void)json_size;
  json_find_string_value(json, "theme", theme, sizeof(theme));
  normalized = ui_theme_normalize(theme);
  snprintf(out, out_size, "%s", normalized);
  free(json);
  return 0;
}

static int
ui_settings_theme_write(const char *theme) {
  char tmp[1024];
  const char *normalized = ui_theme_normalize(theme);
  json_buf_t b = {0};
  int fd;
  int n;

  if(mkdirs(GC_BASE) != 0) return -1;
  n = snprintf(tmp, sizeof(tmp), "%s/.ui-settings.json.tmp", GC_BASE);
  if(n < 0 || (size_t)n >= sizeof(tmp)) {
    errno = ENAMETOOLONG;
    return -1;
  }
  if(json_append(&b, "{\"theme\":") != 0 ||
     json_string(&b, normalized) != 0 ||
     json_appendf(&b, ",\"updatedAt\":%ld}\n", (long)time(NULL)) != 0) {
    free(b.data);
    errno = ENOMEM;
    return -1;
  }
  fd = open(tmp, O_WRONLY | O_CREAT | O_TRUNC, 0666);
  if(fd < 0) {
    free(b.data);
    return -1;
  }
  if(write_all_fd(fd, b.data, b.len) != 0 || fsync(fd) != 0) {
    int saved_errno = errno;
    close(fd);
    unlink(tmp);
    free(b.data);
    errno = saved_errno;
    return -1;
  }
  close(fd);
  free(b.data);
  if(rename(tmp, GC_UI_SETTINGS_FILE) != 0) {
    int saved_errno = errno;
    unlink(tmp);
    errno = saved_errno;
    return -1;
  }
  fsync_parent_dir_best_effort(GC_BASE);
  return 0;
}

static int
ui_settings_request(const http_request_t *req) {
  char theme[16] = {0};
  json_buf_t b = {0};

  if(!strcmp(req->method, "POST")) {
    if(!websrv_get_query_arg(req, "theme", theme, sizeof(theme))) {
      return serve_error(req, 400, "theme required");
    }
    if(strcasecmp(theme, "light") && strcasecmp(theme, "dark")) {
      return serve_error(req, 400, "bad theme");
    }
    if(ui_settings_theme_write(theme) != 0) {
      return serve_error(req, 500, "save UI settings failed");
    }
  } else if(strcmp(req->method, "GET")) {
    return serve_error(req, 405, "method not allowed");
  }

  if(ui_settings_theme_read(theme, sizeof(theme)) != 0) {
    snprintf(theme, sizeof(theme), "%s", "light");
  }
  if(json_append(&b, "{\"ok\":true,\"theme\":") != 0 ||
     json_string(&b, ui_theme_normalize(theme)) != 0 ||
     json_append(&b, "}") != 0) {
    free(b.data);
    return serve_error(req, 500, "out of memory");
  }
  return serve_owned(req, 200, b.data, b.len);
}

static int
ampr_versions_request(const http_request_t *req) {
  DIR *d = opendir(GC_AMPR_DIR);
  json_buf_t b = {0};
  int first = 1;
  if(json_append(&b, "{\"ok\":true,\"versions\":[") != 0) {
    free(b.data);
    return serve_error(req, 500, "out of memory");
  }
  if(d) {
    struct dirent *ent;
    while((ent = readdir(d)) != NULL) {
      if(!ampr_version_safe(ent->d_name)) continue;
      char bin[1024];
      char hash[65] = {0};
      char err[128] = {0};
      struct stat st;
      if(ampr_cache_binary_path(ent->d_name, bin, sizeof(bin)) != 0) continue;
      if(stat(bin, &st) != 0 || !S_ISREG(st.st_mode)) continue;
      if(hash_file_sha256_hex(bin, hash, err, sizeof(err)) != 0) continue;
      if(!first && json_append(&b, ",") != 0) {
        closedir(d);
        free(b.data);
        return serve_error(req, 500, "out of memory");
      }
      first = 0;
      if(json_append(&b, "{\"version\":") != 0 ||
         json_string(&b, ent->d_name) != 0 ||
         json_append(&b, ",\"path\":") != 0 ||
         json_string(&b, bin) != 0 ||
         json_append(&b, ",\"sha256\":") != 0 ||
         json_string(&b, hash) != 0 ||
         json_appendf(&b, ",\"size\":%llu,\"mtime\":%ld}",
                      (unsigned long long)st.st_size,
                      (long)st.st_mtime) != 0) {
        closedir(d);
        free(b.data);
        return serve_error(req, 500, "out of memory");
      }
    }
    closedir(d);
  }
  if(json_append(&b, "]}") != 0) {
    free(b.data);
    return serve_error(req, 500, "out of memory");
  }
  return serve_owned(req, 200, b.data, b.len);
}

static int
ampr_upload_request(const http_request_t *req) {
  char version[64];
  char expected_sha[65];
  char set_latest_arg[16] = "";
  char source_url[1024] = "";
  char dir[1024];
  char bin[1024];
  char tmp[1024];
  char meta[1024];
  char latest_tmp[1024];
  char actual_sha[65] = {0};
  unsigned char body_hash[PFS_VHASH_HASH_SIZE];
  int fd = -1;
  if(strcmp(req->method, "POST")) return serve_error(req, 405, "method not allowed");
  if(!websrv_get_query_arg(req, "version", version, sizeof(version)) ||
     !ampr_version_safe(version)) {
    return serve_error(req, 400, "bad AMPR version");
  }
  expected_sha[0] = 0;
  (void)websrv_get_query_arg(req, "sha256", expected_sha,
                              sizeof(expected_sha));
  int set_latest = 1;
  if(websrv_get_query_arg(req, "setLatest", set_latest_arg,
                          sizeof(set_latest_arg))) {
    set_latest =
        strcmp(set_latest_arg, "0") &&
        strcasecmp(set_latest_arg, "false") &&
        strcasecmp(set_latest_arg, "no");
  }
  if(expected_sha[0] && !sha256_hex_valid(expected_sha)) {
    return serve_error(req, 400, "bad AMPR SHA-256");
  }
  (void)websrv_get_query_arg(req, "sourceUrl", source_url, sizeof(source_url));
  if(!req->body || req->body_size == 0) {
    return serve_error(req, 400, "AMPR binary upload is empty");
  }
  pfs_sha256(req->body, req->body_size, body_hash);
  sha256_to_hex(body_hash, actual_sha);
  if(expected_sha[0] && strcasecmp(actual_sha, expected_sha) != 0) {
    return serve_error(req, 400, "AMPR binary hash mismatch");
  }
  if(ampr_cache_dir_for_version(version, dir, sizeof(dir)) != 0 ||
     ampr_cache_binary_path(version, bin, sizeof(bin)) != 0) {
    return serve_error(req, 400, "bad AMPR cache path");
  }
  if(mkdirs(dir) != 0) {
    return serve_error(req, 500, "create AMPR cache failed");
  }
  int n = snprintf(tmp, sizeof(tmp), "%s/.%s.tmp", dir, GC_AMPR_BINARY_NAME);
  if(n < 0 || (size_t)n >= sizeof(tmp)) {
    return serve_error(req, 500, "AMPR temp path too long");
  }
  fd = open(tmp, O_WRONLY | O_CREAT | O_TRUNC, 0666);
  if(fd < 0) return serve_error(req, 500, "open AMPR cache failed");
  if(write_all_fd(fd, req->body, req->body_size) != 0 ||
     fsync(fd) != 0) {
    close(fd);
    unlink(tmp);
    return serve_error(req, 500, "write AMPR cache failed");
  }
  close(fd);
  fd = -1;
  if(rename(tmp, bin) != 0) {
    unlink(tmp);
    return serve_error(req, 500, "publish AMPR cache failed");
  }
  fsync_parent_dir_best_effort(dir);
  n = snprintf(meta, sizeof(meta), "%s/metadata.json", dir);
  if(n >= 0 && (size_t)n < sizeof(meta)) {
    json_buf_t mb = {0};
    if(json_append(&mb, "{\"version\":") == 0 &&
       json_string(&mb, version) == 0 &&
       json_append(&mb, ",\"sourceUrl\":") == 0 &&
       json_string(&mb, source_url) == 0 &&
       json_append(&mb, ",\"sha256\":") == 0 &&
       json_string(&mb, actual_sha) == 0 &&
       json_appendf(&mb, ",\"size\":%llu,\"cachedAt\":%ld}\n",
                    (unsigned long long)req->body_size, (long)time(NULL)) == 0) {
      int mfd = open(meta, O_WRONLY | O_CREAT | O_TRUNC, 0666);
      if(mfd >= 0) {
        (void)write_all_fd(mfd, mb.data, mb.len);
        fsync(mfd);
        close(mfd);
      }
    }
    free(mb.data);
  }
  n = snprintf(latest_tmp, sizeof(latest_tmp), "%s/.latest.json.tmp",
               GC_AMPR_DIR);
  if(set_latest && n >= 0 && (size_t)n < sizeof(latest_tmp)) {
    json_buf_t lb = {0};
    if(json_append(&lb, "{\"version\":") == 0 &&
       json_string(&lb, version) == 0 &&
       json_append(&lb, ",\"sourceUrl\":") == 0 &&
       json_string(&lb, source_url) == 0 &&
       json_append(&lb, ",\"sha256\":") == 0 &&
       json_string(&lb, actual_sha) == 0 &&
       json_append(&lb, ",\"path\":") == 0 &&
       json_string(&lb, bin) == 0 &&
       json_appendf(&lb, ",\"size\":%llu,\"cachedAt\":%ld}\n",
                    (unsigned long long)req->body_size, (long)time(NULL)) == 0) {
      int lfd = open(latest_tmp, O_WRONLY | O_CREAT | O_TRUNC, 0666);
      if(lfd >= 0) {
        if(write_all_fd(lfd, lb.data, lb.len) == 0 && fsync(lfd) == 0) {
          close(lfd);
          lfd = -1;
          (void)rename(latest_tmp, GC_AMPR_LATEST_FILE);
          fsync_parent_dir_best_effort(GC_AMPR_DIR);
        }
        if(lfd >= 0) close(lfd);
      }
    }
    free(lb.data);
  }
  artifact_cache_invalidate();
  json_buf_t b = {0};
  if(json_append(&b, "{\"ok\":true,\"version\":") != 0 ||
     json_string(&b, version) != 0 ||
     json_append(&b, ",\"path\":") != 0 ||
     json_string(&b, bin) != 0 ||
     json_append(&b, ",\"sha256\":") != 0 ||
     json_string(&b, actual_sha) != 0 ||
     json_appendf(&b, ",\"size\":%llu}", (unsigned long long)req->body_size) != 0) {
    free(b.data);
    return serve_error(req, 500, "out of memory");
  }
  return serve_owned(req, 200, b.data, b.len);
}

static int
copy_file_contents(const char *src, const char *dst, char *err, size_t err_size) {
  int in = open(src, O_RDONLY);
  char *buf = NULL;
  if(in < 0) {
    snprintf(err, err_size, "open source: %s", strerror(errno));
    return -1;
  }
  int out = open(dst, O_WRONLY | O_CREAT | O_TRUNC, 0666);
  if(out < 0) {
    snprintf(err, err_size, "open target: %s", strerror(errno));
    close(in);
    return -1;
  }
  buf = malloc(128 * 1024);
  if(!buf) {
    snprintf(err, err_size, "%s", "copy buffer allocation failed");
    close(in);
    close(out);
    return -1;
  }
  for(;;) {
    ssize_t n = read(in, buf, 128 * 1024);
    if(n < 0) {
      if(errno == EINTR) continue;
      snprintf(err, err_size, "read source: %s", strerror(errno));
      free(buf);
      close(in);
      close(out);
      return -1;
    }
    if(n == 0) break;
    if(write_all_fd(out, buf, (size_t)n) != 0) {
      snprintf(err, err_size, "write target: %s", strerror(errno));
      free(buf);
      close(in);
      close(out);
      return -1;
    }
  }
  free(buf);
  close(in);
  if(fsync(out) != 0) {
    snprintf(err, err_size, "sync target: %s", strerror(errno));
    close(out);
    return -1;
  }
  close(out);
  return 0;
}

static int
ampr_find_folder_target(const char *root, char *out, size_t out_size) {
  static const char *rels[] = {
    "fakelib/" GC_AMPR_BINARY_NAME,
    "fakelib/libSceAmpr.prx",
    "sce_module/" GC_AMPR_BINARY_NAME,
    "sce_module/libSceAmpr.prx",
  };
  struct stat st;
  for(size_t i = 0; i < sizeof(rels) / sizeof(rels[0]); i++) {
    char path[1024];
    int n = snprintf(path, sizeof(path), "%s/%s", root, rels[i]);
    if(n < 0 || (size_t)n >= sizeof(path)) continue;
    if(stat(path, &st) == 0 && S_ISREG(st.st_mode)) {
      snprintf(out, out_size, "%s", path);
      return 0;
    }
  }
  errno = ENOENT;
  return -1;
}

static int
run_update_ampr_op(gc_operation_t *op) {
  gc_game_t game = {0};
  char err[256] = {0};
  char actual_sha[65] = {0};
  char target[1024] = {0};
  char parent[1024] = {0};
  char tmp[1024] = {0};
  char expected_mount[1024] = {0};

  gc_checkpoint("update-ampr find game");
  gc_log("update-ampr start op=%s title=%s version=%s sha=%s",
         op->id, op->title_id, op->ampr_version, op->ampr_sha256);
  append_operation_phase(op, "resolving");
  job_set_phase("resolving", 0, 0, "Resolving selected source");
  if(find_game_for_operation_source_path(op, &game, 0) != 0 ||
     game.source_kind == GC_SOURCE_UNKNOWN) {
    snprintf(op->error, sizeof(op->error), "%s", "game source is unavailable");
    return -1;
  }
  snprintf(op->source_path, sizeof(op->source_path), "%s", game.source_path);
  snprintf(op->source_kind, sizeof(op->source_kind), "%s",
           source_kind_name(game.source_kind));
  job_set_target(game.source_path);
  if(hash_file_sha256_hex(op->ampr_cache_path, actual_sha,
                          err, sizeof(err)) != 0) {
    snprintf(op->error, sizeof(op->error), "%s",
             err[0] ? err : "AMPR cache hash failed");
    return -1;
  }
  if(strcasecmp(actual_sha, op->ampr_sha256) != 0) {
    snprintf(op->error, sizeof(op->error), "%s", "cached AMPR hash mismatch");
    return -1;
  }
  if(close_title_if_running(op->title_id, err, sizeof(err)) != 0) {
    snprintf(op->error, sizeof(op->error), "%s",
             err[0] ? err : "could not close game");
    return -1;
  }
  if(ampr_original_backup_prepare(&game, err, sizeof(err)) != 0) {
    snprintf(op->error, sizeof(op->error), "%s",
             err[0] ? err : "original AMPR backup failed");
    return -1;
  }
  append_operation_phase(op, "patching");
  job_set_phase("patching", 0, 0, "Replacing APR-EMU binary");
  if(game.source_kind == GC_SOURCE_FOLDER) {
    if(ampr_find_folder_target(game.source_path, target, sizeof(target)) != 0) {
      snprintf(op->error, sizeof(op->error), "%s",
               "AMPR binary was not found in fakelib or sce_module");
      return -1;
    }
    snprintf(op->ampr_result_mode, sizeof(op->ampr_result_mode), "%s",
             "folder");
    if(path_parent(target, parent, sizeof(parent)) != 0) {
      snprintf(op->error, sizeof(op->error), "%s", "AMPR target path too long");
      return -1;
    }
    int n = snprintf(tmp, sizeof(tmp), "%s/.%s.gc-ampr.tmp",
                     parent, GC_AMPR_BINARY_NAME);
    if(n < 0 || (size_t)n >= sizeof(tmp)) {
      snprintf(op->error, sizeof(op->error), "%s", "AMPR temp path too long");
      return -1;
    }
    unlink(tmp);
    if(copy_file_contents(op->ampr_cache_path, tmp, err, sizeof(err)) != 0) {
      unlink(tmp);
      snprintf(op->error, sizeof(op->error), "%s",
               err[0] ? err : "copy AMPR binary failed");
      return -1;
    }
    if(rename(tmp, target) != 0) {
      unlink(tmp);
      snprintf(op->error, sizeof(op->error), "publish AMPR binary: %s",
               strerror(errno));
      return -1;
    }
    fsync_parent_dir_best_effort(parent);
    if(hash_file_sha256_hex(target, actual_sha, err, sizeof(err)) != 0 ||
       strcasecmp(actual_sha, op->ampr_sha256) != 0) {
      snprintf(op->error, sizeof(op->error), "%s",
               "AMPR target verification failed");
      return -1;
    }
    snprintf(op->output_path, sizeof(op->output_path), "%s", target);
  } else if(game.source_kind == GC_SOURCE_IMAGE) {
    pfs_ampr_hotswap_info_t hs = {0};
    if(game.nested_type != PFS_NESTED_EXFAT) {
      snprintf(op->ampr_result_mode, sizeof(op->ampr_result_mode), "%s",
               "failed");
      snprintf(op->error, sizeof(op->error), "%s",
               "AMPR hot-swap supports direct exFAT images only in this build");
      return -1;
    }
    job_set_phase("patching", 0, 0, "Patching exFAT image");
    if(pfs_ampr_hotswap_exfat_image(game.source_path,
                                    op->ampr_cache_path,
                                    &hs, err, sizeof(err)) != 0) {
      snprintf(op->ampr_result_mode, sizeof(op->ampr_result_mode), "%s",
               "failed");
      snprintf(op->error, sizeof(op->error), "%s",
               err[0] ? err : "exFAT AMPR hot-swap failed");
      return -1;
    }
    snprintf(target, sizeof(target), "%s", game.source_path);
    snprintf(op->output_path, sizeof(op->output_path), "%s", game.source_path);
    snprintf(op->ampr_result_mode, sizeof(op->ampr_result_mode), "%s",
             hs.mode[0] ? hs.mode : "exfat-in-place");
    gc_log("update-ampr exfat patched title=%s path=%s logical=%s mode=%s "
           "oldSize=%llu newSize=%llu oldCluster=%u newCluster=%u alloc=%u",
           op->title_id, game.source_path, hs.logical_path,
           op->ampr_result_mode,
           (unsigned long long)hs.old_size,
           (unsigned long long)hs.new_size,
           hs.old_first_cluster, hs.new_first_cluster,
           hs.allocated_clusters);
  } else if(game.source_kind == GC_SOURCE_COMPRESSED) {
    pfs_ampr_hotswap_info_t hs = {0};
    if(game.nested_type != PFS_NESTED_EXFAT &&
       game.nested_type != PFS_NESTED_PFS) {
      snprintf(op->ampr_result_mode, sizeof(op->ampr_result_mode), "%s",
               "failed");
      snprintf(op->error, sizeof(op->error), "%s",
               "AMPR hot-swap supports compressed nested exFAT or PFS images only");
      return -1;
    }
    job_set_phase("patching", 0, 0,
                  game.nested_type == PFS_NESTED_EXFAT
                      ? "Patching compressed exFAT image"
                      : "Patching compressed PFS image");
    int hs_rc = game.nested_type == PFS_NESTED_EXFAT
        ? pfs_ampr_hotswap_ffpfsc_exfat(game.source_path,
                                        op->ampr_cache_path,
                                        &hs, err, sizeof(err))
        : pfs_ampr_hotswap_ffpfsc_pfs(game.source_path,
                                      op->ampr_cache_path,
                                      &hs, err, sizeof(err));
    if(hs_rc != 0) {
      snprintf(op->ampr_result_mode, sizeof(op->ampr_result_mode), "%s",
               "failed");
      snprintf(op->error, sizeof(op->error), "%s",
               err[0] ? err : "compressed AMPR hot-swap failed");
      return -1;
    }
    delete_vhash_sidecar_if_present(game.source_path, "update-ampr",
                                    op->title_id);
    delete_validation_marker_for_path(op->title_id, game.source_path);
    snprintf(target, sizeof(target), "%s", game.source_path);
    snprintf(op->output_path, sizeof(op->output_path), "%s", game.source_path);
    snprintf(op->ampr_result_mode, sizeof(op->ampr_result_mode), "%s",
             hs.mode[0] ? hs.mode : "ffpfsc-patch");
    gc_log("update-ampr ffpfsc patched title=%s path=%s logical=%s mode=%s "
           "oldSize=%llu newSize=%llu changedBlocks=%llu oldCluster=%u "
           "newCluster=%u alloc=%u",
           op->title_id, game.source_path, hs.logical_path,
           op->ampr_result_mode,
           (unsigned long long)hs.old_size,
           (unsigned long long)hs.new_size,
           (unsigned long long)hs.changed_blocks,
           hs.old_first_cluster, hs.new_first_cluster,
           hs.allocated_clusters);
  } else {
    snprintf(op->ampr_result_mode, sizeof(op->ampr_result_mode), "%s",
             "failed");
    snprintf(op->error, sizeof(op->error), "%s",
             "AMPR hot-swap source is unavailable");
    return -1;
  }
  snprintf(op->format, sizeof(op->format), "%s", "ampr");
  snprintf(op->delete_policy, sizeof(op->delete_policy), "%s", "none");
  append_operation_phase(op, "mounting");
  job_set_phase("mounting", 0, 0, "Remounting APR-EMU update");
  if(update_ampr_remount_source(op, &game, expected_mount,
                                sizeof(expected_mount),
                                err, sizeof(err)) != 0) {
    snprintf(op->error, sizeof(op->error), "%s",
             err[0] ? err : "AMPR hot-swap remount failed");
    gc_log("update-ampr remount failed title=%s err=%s",
           op->title_id, op->error);
    return -1;
  }
  append_operation_phase(op, "validating");
  job_set_phase("validating", 0, 0, "Verifying mounted APR-EMU");
  if(update_ampr_verify_mounted_hash(op->title_id, expected_mount,
                                     op->ampr_sha256,
                                     err, sizeof(err)) != 0) {
    snprintf(op->error, sizeof(op->error), "%s",
             err[0] ? err : "AMPR mounted verification failed");
    gc_log("update-ampr mounted verify failed title=%s err=%s",
           op->title_id, op->error);
    return -1;
  }
  snprintf(op->result, sizeof(op->result), "%s", "updated");
  if(ampr_selection_write(op->title_id, game.source_path,
                          op->ampr_intent[0] ? op->ampr_intent : "manual",
                          op->ampr_version, op->ampr_sha256,
                          op->ampr_result_mode) != 0) {
    gc_log("update-ampr selection marker failed title=%s path=%s intent=%s",
           op->title_id, game.source_path,
           op->ampr_intent[0] ? op->ampr_intent : "manual");
  }
  artifact_cache_invalidate();
  gc_log("update-ampr complete title=%s target=%s version=%s sha=%s",
         op->title_id, target, op->ampr_version, op->ampr_sha256);
  return 0;
}

static int
run_build_ampr_index_op(gc_operation_t *op) {
  gc_game_t game = {0};
  pfs_app_info_t info;
  pfs_ampr_hotswap_info_t hs;
  char err[256] = {0};
  char expected_mount[1024] = {0};

  gc_checkpoint("build-ampr-index find game");
  gc_log("build-ampr-index start op=%s title=%s", op->id, op->title_id);
  append_operation_phase(op, "resolving");
  job_set_phase("resolving", 0, 0, "Resolving selected source");
  if(find_game_for_operation_source_path(op, &game, 0) != 0 ||
     game.source_kind == GC_SOURCE_UNKNOWN) {
    snprintf(op->error, sizeof(op->error), "%s", "game source is unavailable");
    gc_log("build-ampr-index failed title=%s err=%s",
           op->title_id, op->error);
    return -1;
  }
  if(gc_cancel_requested(err, sizeof(err))) {
    snprintf(op->error, sizeof(op->error), "%s", err);
    return -1;
  }
  if(!game.ampr_present) {
    snprintf(op->error, sizeof(op->error), "%s",
             "libSceAmpr.sprx or libSceAmpr.prx was not found for this game");
    gc_log("build-ampr-index skipped title=%s path=%s err=%s",
           op->title_id, game.source_path, op->error);
    return -1;
  }

  snprintf(op->source_path, sizeof(op->source_path), "%s", game.source_path);
  if(game.source_kind == GC_SOURCE_FOLDER) {
    snprintf(op->output_path, sizeof(op->output_path), "%s/ampr_emu.index",
             game.source_path);
  } else {
    snprintf(op->output_path, sizeof(op->output_path), "%s",
             game.source_path);
  }
  snprintf(op->source_kind, sizeof(op->source_kind), "%s",
           source_kind_name(game.source_kind));
  snprintf(op->format, sizeof(op->format), "%s", "ampr");
  snprintf(op->delete_policy, sizeof(op->delete_policy), "%s", "none");
  job_set_target(op->output_path);

  memset(&info, 0, sizeof(info));
  memset(&hs, 0, sizeof(hs));
  if(game.source_kind == GC_SOURCE_FOLDER) {
    gc_checkpoint("build-ampr-index scanning");
    append_operation_phase(op, "scanning");
    job_set_phase("scanning", 0, 0, "Scanning app folder");
    if(pfs_build_ampr_index_for_folder(game.source_path, &info,
                                       err, sizeof(err)) != 0) {
      snprintf(op->error, sizeof(op->error), "%s",
               err[0] ? err : "AMPR index build failed");
      gc_log("build-ampr-index failed title=%s err=%s",
             op->title_id, op->error);
      return -1;
    }
    operation_store_scan_stats(op, &info);
    op->compression_source_size = info.scan_bytes;
    struct stat st;
    if(stat(op->output_path, &st) == 0 && S_ISREG(st.st_mode) &&
       st.st_size > 0) {
      op->compressed_size = (uint64_t)st.st_size;
    }
  } else if(game.source_kind == GC_SOURCE_IMAGE) {
    if(game.nested_type != PFS_NESTED_EXFAT) {
      snprintf(op->error, sizeof(op->error), "%s",
               "AMPR index image patch supports exFAT images only");
      return -1;
    }
    gc_checkpoint("build-ampr-index patch image");
    append_operation_phase(op, "patching");
    job_set_phase("patching", 0, 0, "Building AMPR index into exFAT image");
    if(pfs_ampr_index_exfat_image(game.source_path, &hs,
                                  err, sizeof(err)) != 0) {
      snprintf(op->error, sizeof(op->error), "%s",
               err[0] ? err : "AMPR index image patch failed");
      gc_log("build-ampr-index image failed title=%s err=%s",
             op->title_id, op->error);
      return -1;
    }
    snprintf(op->ampr_result_mode, sizeof(op->ampr_result_mode), "%s",
             hs.mode[0] ? hs.mode : "exfat-index-tail");
    op->compressed_size = hs.new_size;
    if(update_ampr_remount_source(op, &game, expected_mount,
                                  sizeof(expected_mount), err,
                                  sizeof(err)) != 0) {
      snprintf(op->error, sizeof(op->error), "%s",
               err[0] ? err : "AMPR index remount failed");
      gc_log("build-ampr-index image remount failed title=%s err=%s",
             op->title_id, op->error);
      return -1;
    }
  } else if(game.source_kind == GC_SOURCE_COMPRESSED) {
    if(game.nested_type != PFS_NESTED_EXFAT) {
      snprintf(op->error, sizeof(op->error), "%s",
               "AMPR index compressed patch supports nested exFAT only");
      return -1;
    }
    gc_checkpoint("build-ampr-index patch compressed");
    append_operation_phase(op, "patching");
    job_set_phase("patching", 0, 0,
                  "Building AMPR index into compressed exFAT image");
    if(pfs_ampr_index_ffpfsc_exfat(game.source_path, &hs,
                                   err, sizeof(err)) != 0) {
      snprintf(op->error, sizeof(op->error), "%s",
               err[0] ? err : "AMPR index compressed patch failed");
      gc_log("build-ampr-index compressed failed title=%s err=%s",
             op->title_id, op->error);
      return -1;
    }
    delete_vhash_sidecar_if_present(game.source_path, "build-ampr-index",
                                    op->title_id);
    snprintf(op->ampr_result_mode, sizeof(op->ampr_result_mode), "%s",
             hs.mode[0] ? hs.mode : "ffpfsc-index-tail");
    op->compressed_size = hs.new_size;
    op->repaired_blocks = hs.changed_blocks;
    if(update_ampr_remount_source(op, &game, expected_mount,
                                  sizeof(expected_mount), err,
                                  sizeof(err)) != 0) {
      snprintf(op->error, sizeof(op->error), "%s",
               err[0] ? err : "AMPR index remount failed");
      gc_log("build-ampr-index compressed remount failed title=%s err=%s",
             op->title_id, op->error);
      return -1;
    }
  } else {
    snprintf(op->error, sizeof(op->error), "%s",
             "AMPR index can only be built for a folder, exFAT image, or compressed image");
    gc_log("build-ampr-index denied title=%s sourceKind=%s path=%s",
           op->title_id, source_kind_name(game.source_kind), game.source_path);
    return -1;
  }
  op->saved_bytes = 0;
  op->apr_indexed = 1;
  snprintf(op->result, sizeof(op->result), "%s", "indexed");
  artifact_cache_invalidate();
  gc_log("build-ampr-index complete title=%s output=%s bytes=%llu files=%llu mode=%s",
         op->title_id, op->output_path,
         (unsigned long long)op->compressed_size,
         (unsigned long long)info.scan_files,
         op->ampr_result_mode);
  return 0;
}

static void *
operation_thread(void *arg) {
  gc_operation_t *op = arg;
  int rc = -1;
  const char *verb = action_name(op->action);

  gc_checkpoint("operation job begin");
  gc_log("operation begin id=%s action=%s title=%s game=%s", op->id, verb,
         op->title_id, op->display_name);
  if(!job_begin(verb)) {
    char notify_action[32] = {0};
    char notify_game[256] = {0};
    char notify_error[256] = {0};
    pthread_mutex_lock(&g_gc_lock);
    snprintf(op->error, sizeof(op->error), "%s", "job already running");
    op->status = GC_OP_FAILED;
    op->ended_at = time(NULL);
    snprintf(op->phase, sizeof(op->phase), "%s", "complete");
    snprintf(op->result, sizeof(op->result), "%s", "failed");
    snprintf(notify_action, sizeof(notify_action), "%s",
             action_name(op->action));
    snprintf(notify_game, sizeof(notify_game), "%s", op->display_name);
    snprintf(notify_error, sizeof(notify_error), "%s", op->error);
    append_operation_logs(op);
    g_worker_running = 0;
    start_next_locked();
    pthread_mutex_unlock(&g_gc_lock);
    artifact_cache_invalidate();
    gc_log("operation job_begin failed id=%s err=%s", op->id, notify_error);
    gc_notify_operation_done(notify_action, notify_game, "failed",
                             notify_error);
    return NULL;
  }

  gc_checkpoint("operation running");
  if(op->action == GC_ACTION_COMPRESS ||
     op->action == GC_ACTION_MAKE_IMAGE) rc = run_compress_op(op);
  else if(op->action == GC_ACTION_UNCOMPRESS) rc = run_uncompress_op(op);
  else if(op->action == GC_ACTION_EXTRACT_IMAGE) rc = run_extract_image_op(op);
  else if(op->action == GC_ACTION_VALIDATE_REPAIR) rc = run_validate_repair_op(op);
  else if(op->action == GC_ACTION_VALIDATE_ONLY) rc = run_validate_only_op(op);
  else if(op->action == GC_ACTION_MOVE_TO_USB ||
          op->action == GC_ACTION_MOVE_TO_INTERNAL ||
          op->action == GC_ACTION_COPY_TO_USB ||
          op->action == GC_ACTION_COPY_TO_INTERNAL) rc = run_move_op(op);
  else if(op->action == GC_ACTION_REFRESH_MOUNT) rc = run_refresh_mount_op(op);
  else if(op->action == GC_ACTION_DELETE_GAME_DATA) {
    rc = run_delete_game_data_op(op);
	  } else if(op->action == GC_ACTION_READ_SPEED_TEST) {
	    rc = run_read_speed_test_op(op);
	  } else if(op->action == GC_ACTION_BUILD_AMPR_INDEX) {
	    rc = run_build_ampr_index_op(op);
	  } else if(op->action == GC_ACTION_SET_READ_ONLY) {
	    rc = run_set_read_only_op(op);
	  } else if(op->action == GC_ACTION_UPDATE_AMPR) {
	    rc = run_update_ampr_op(op);
	  }

  int cancelled = job_cancelled();
  gc_checkpoint("operation job end");
  job_end(rc, rc == 0 ? NULL : (op->error[0] ? op->error : "operation failed"));

  char notify_action[32] = {0};
  char notify_game[256] = {0};
  char notify_status[16] = {0};
  char notify_error[256] = {0};

  pthread_mutex_lock(&g_gc_lock);
  op->ended_at = time(NULL);
  snprintf(op->phase, sizeof(op->phase), "%s", "complete");
  if(cancelled || operation_error_is_cancelled(op->error)) {
    op->status = GC_OP_CANCELLED;
    snprintf(op->result, sizeof(op->result), "%s", "cancelled");
  } else if(rc == 0) {
    op->status = GC_OP_SUCCESS;
    if(!op->result[0] || operation_result_is_intermediate_phase(op->result)) {
      snprintf(op->result, sizeof(op->result), "%s", "success");
    }
  } else {
    op->status = GC_OP_FAILED;
    snprintf(op->result, sizeof(op->result), "%s", "failed");
  }
  snprintf(notify_action, sizeof(notify_action), "%s", action_name(op->action));
  snprintf(notify_game, sizeof(notify_game), "%s", op->display_name);
  snprintf(notify_status, sizeof(notify_status), "%s",
           operation_notification_status(op->status));
  snprintf(notify_error, sizeof(notify_error), "%s", op->error);
  append_operation_logs(op);
  g_worker_running = 0;
  start_next_locked();
  pthread_mutex_unlock(&g_gc_lock);
  artifact_cache_invalidate();

  gc_checkpoint("operation notification");
  gc_log("operation finished id=%s action=%s title=%s status=%s result=%s err=%s",
         op->id, notify_action, op->title_id, notify_status, op->result,
         notify_error);
  gc_notify_operation_done(notify_action, notify_game, notify_status,
                           notify_error);
  gc_checkpoint("operation idle");
  return NULL;
}

static void
start_next_locked(void) {
  if(g_worker_running || active_op_locked()) return;
  gc_operation_t *next = NULL;
  for(size_t i = 0; i < GC_MAX_OPS; i++) {
    if(g_ops[i].used && g_ops[i].status == GC_OP_QUEUED &&
       (!next || g_ops[i].seq < next->seq)) {
      next = &g_ops[i];
    }
  }
  if(!next) return;
  next->status = GC_OP_RUNNING;
  next->started_at = time(NULL);
  append_operation_logs(next);
  g_worker_running = 1;
  pthread_t t;
  pthread_attr_t at;
  pthread_attr_init(&at);
  pthread_attr_setdetachstate(&at, PTHREAD_CREATE_DETACHED);
  pthread_attr_setstacksize(&at, GC_WORKER_THREAD_STACK_SIZE);
  gc_log("starting worker id=%s action=%s title=%s stack=%u", next->id,
         action_name(next->action), next->title_id,
         (unsigned)GC_WORKER_THREAD_STACK_SIZE);
  int rc = pthread_create(&t, &at, operation_thread, next);
  pthread_attr_destroy(&at);
  if(rc != 0) {
    snprintf(next->error, sizeof(next->error), "could not start worker");
    next->status = GC_OP_FAILED;
    next->ended_at = time(NULL);
    snprintf(next->phase, sizeof(next->phase), "%s", "complete");
    snprintf(next->result, sizeof(next->result), "%s", "failed");
    append_operation_logs(next);
    g_worker_running = 0;
  }
}

/* Called with g_gc_lock held; releases it before building the response. */
static int
enqueue_start_response_locked(const http_request_t *req, gc_operation_t *op) {
  append_operation_logs(op);
  start_next_locked();
  gc_op_status_t status = op->status;
  char id[32];
  snprintf(id, sizeof(id), "%s", op->id);
  pthread_mutex_unlock(&g_gc_lock);

  json_buf_t b = {0};
  if(json_append(&b, "{\"ok\":true,\"id\":") != 0 ||
     json_string(&b, id) != 0 ||
     json_append(&b, ",\"status\":") != 0 ||
     json_string(&b, status_name(status)) != 0 ||
     json_append(&b, "}") != 0) {
    free(b.data);
    return -1;
  }
  return serve_owned(req, 200, b.data, b.len);
}

static const char *
read_title_source_args(const http_request_t *req,
                       char *title_id,
                       size_t title_id_size,
                       char *source_path,
                       size_t source_path_size) {
  if(!websrv_get_query_arg(req, "titleId", title_id, title_id_size) ||
     !valid_title_id(title_id)) {
    return "bad titleId";
  }
  int source_rc = get_required_source_path_arg(req, source_path,
                                               source_path_size);
  if(source_rc == -2) return "sourcePath required";
  if(source_rc != 0) return "bad source path";
  return NULL;
}

static int
title_operation_busy(const char *title_id) {
  pthread_mutex_lock(&g_gc_lock);
  int busy = active_op_for_title_locked(title_id) ||
             pending_op_for_title_locked(title_id);
  pthread_mutex_unlock(&g_gc_lock);
  return busy;
}

static gc_operation_t *
alloc_queued_operation_locked(const http_request_t *req,
                              const char *title_id,
                              gc_action_t action,
                              const char *display_name,
                              int *response_out) {
  if(response_out) *response_out = 0;
  if(active_op_for_title_locked(title_id) ||
     pending_op_for_title_locked(title_id)) {
    pthread_mutex_unlock(&g_gc_lock);
    if(response_out) {
      *response_out = serve_error(req, 409,
                                  "action already running for this game");
    }
    return NULL;
  }
  gc_operation_t *op = alloc_op_locked();
  if(!op) {
    pthread_mutex_unlock(&g_gc_lock);
    if(response_out) *response_out = serve_error(req, 409, "history is full");
    return NULL;
  }
  memset(op, 0, sizeof(*op));
  op->used = 1;
  op->seq = g_next_seq++;
  snprintf(op->id, sizeof(op->id), "op-%llu",
           (unsigned long long)op->seq);
  op->action = action;
  op->status = GC_OP_QUEUED;
  op->created_at = time(NULL);
  snprintf(op->title_id, sizeof(op->title_id), "%s", title_id);
  snprintf(op->display_name, sizeof(op->display_name), "%s",
           display_name ? display_name : title_id);
  return op;
}

static int
enqueue_action(const http_request_t *req, gc_action_t action) {
  char title_id[64];
  char source_path_arg[1024] = "";
  char format[16];
  char format_arg[16];
  char mode_arg[16];
  char destination_arg[24] = "";
  char delete_policy_arg[24];
  char preserve_arg[24];
  char budget_arg[32];
  char order_arg[24];
  char skip_space_arg[16];
  char usb_id[16] = "";
  char requested_delete_policy[16] = "";
  char preserve_original[16] = "";
  char stream_order[24] = "budgeted-gain";
  uint64_t stream_budget_bytes = PFS_STREAM_DEFAULT_BUDGET_BYTES;
  gc_game_t game;
  char delete_policy[16] = "after";
  char target_root[1024] = "";
  int skip_space_check = 0;
  int uncompress_to_internal = 0;
  int compress_to_internal = 0;
  int makes_image = action == GC_ACTION_COMPRESS ||
                    action == GC_ACTION_MAKE_IMAGE;

  const char *arg_err = read_title_source_args(req, title_id,
                                               sizeof(title_id),
                                               source_path_arg,
                                               sizeof(source_path_arg));
  if(arg_err) return serve_error(req, 400, arg_err);
  memset(&game, 0, sizeof(game));
  snprintf(game.title_id, sizeof(game.title_id), "%s", title_id);
  snprintf(game.name, sizeof(game.name), "%s", title_id);
  snprintf(game.source_path, sizeof(game.source_path), "%s", source_path_arg);
  game.source_kind = source_kind_from_name(source_kind_name_from_path(source_path_arg));
  (void)lookup_game_by_source_path(source_path_arg, &game);
  pthread_mutex_lock(&g_gc_lock);
  if(active_op_for_title_locked(title_id) || pending_op_for_title_locked(title_id)) {
    pthread_mutex_unlock(&g_gc_lock);
    return serve_error(req, 409, "action already running for this game");
  }
  pthread_mutex_unlock(&g_gc_lock);
  snprintf(format, sizeof(format), "%s", "pfs");
  if(makes_image &&
     websrv_get_query_arg(req, "format", format_arg, sizeof(format_arg))) {
    if(!strcasecmp(format_arg, "pfs")) {
      snprintf(format, sizeof(format), "%s", "pfs");
    } else if(!strcasecmp(format_arg, "exfat")) {
      snprintf(format, sizeof(format), "%s", "exfat");
    } else {
      return serve_error(req, 400, action == GC_ACTION_MAKE_IMAGE ?
                         "bad image format" : "bad compression format");
    }
  }
  if(action == GC_ACTION_UNCOMPRESS &&
     websrv_get_query_arg(req, "mode", mode_arg, sizeof(mode_arg))) {
    if(!strcasecmp(mode_arg, "image")) {
      snprintf(format, sizeof(format), "%s", "image");
    } else if(!strcasecmp(mode_arg, "app") ||
              !strcasecmp(mode_arg, "folder")) {
      snprintf(format, sizeof(format), "%s", "pfs");
    } else {
      return serve_error(req, 400, "bad uncompress mode");
    }
  }
  if(makes_image &&
     websrv_get_query_arg(req, "destination", destination_arg,
                          sizeof(destination_arg))) {
    if(!strcasecmp(destination_arg, "keep") ||
       !strcasecmp(destination_arg, "inplace") ||
       !strcasecmp(destination_arg, "in-place")) {
      target_root[0] = 0;
    } else if(!strcasecmp(destination_arg, "internal")) {
      compress_to_internal = 1;
    } else if(!strcasecmp(destination_arg, "usb") ||
              !strcasecmp(destination_arg, "external")) {
      /* usbId is parsed after storage discovery. */
    } else {
      return serve_error(req, 400, action == GC_ACTION_MAKE_IMAGE ?
                         "bad image destination" : "bad compression destination");
    }
  }
  if(action == GC_ACTION_UNCOMPRESS &&
     websrv_get_query_arg(req, "destination", destination_arg,
                          sizeof(destination_arg))) {
    if(!strcasecmp(destination_arg, "inplace") ||
       !strcasecmp(destination_arg, "in-place")) {
      target_root[0] = 0;
    } else if(!strcasecmp(destination_arg, "internal")) {
      uncompress_to_internal = 1;
    } else if(!strcasecmp(destination_arg, "usb") ||
              !strcasecmp(destination_arg, "external")) {
      /* usbId is parsed after storage discovery. */
    } else {
      return serve_error(req, 400, "bad uncompress destination");
    }
  }
  if(websrv_get_query_arg(req, "deletePolicy", delete_policy_arg,
                          sizeof(delete_policy_arg))) {
    if(!strcasecmp(delete_policy_arg, "after") ||
       !strcasecmp(delete_policy_arg, "safe")) {
      snprintf(requested_delete_policy, sizeof(requested_delete_policy),
               "%s", "after");
    } else if(!strcasecmp(delete_policy_arg, "stream") ||
              !strcasecmp(delete_policy_arg, "destructive")) {
      if(action == GC_ACTION_UNCOMPRESS) {
        return serve_error(req, 400, "stream unpack is not supported");
      }
      if(action == GC_ACTION_MAKE_IMAGE) {
        return serve_error(req, 400, "destructive image creation is not supported");
      }
      snprintf(requested_delete_policy, sizeof(requested_delete_policy),
               "%s", "stream");
    } else if((action == GC_ACTION_UNCOMPRESS || makes_image) &&
              (!strcasecmp(delete_policy_arg, "keep") ||
               !strcasecmp(delete_policy_arg, "none"))) {
      snprintf(requested_delete_policy, sizeof(requested_delete_policy),
               "%s", "keep");
    } else {
      return serve_error(req, 400, "bad delete policy");
    }
  }
  if(makes_image &&
     websrv_get_query_arg(req, "preserveOriginal", preserve_arg,
                          sizeof(preserve_arg))) {
    if(!strcasecmp(preserve_arg, "hide")) {
      snprintf(preserve_original, sizeof(preserve_original), "%s", "hide");
      if(!requested_delete_policy[0]) {
        snprintf(requested_delete_policy, sizeof(requested_delete_policy),
                 "%s", "keep");
      }
    } else if(!strcasecmp(preserve_arg, "0") ||
              !strcasecmp(preserve_arg, "false") ||
              !strcasecmp(preserve_arg, "none")) {
      preserve_original[0] = 0;
    } else {
      return serve_error(req, 400, "bad preserveOriginal");
    }
  }
  if(preserve_original[0] && !strcmp(requested_delete_policy, "stream")) {
    return serve_error(req, 400,
                       "preserveOriginal=hide is not compatible with destructive compression");
  }
  if(action == GC_ACTION_COMPRESS &&
     websrv_get_query_arg(req, "budgetBytes", budget_arg,
                          sizeof(budget_arg))) {
    if(parse_u64_arg(budget_arg, &stream_budget_bytes) != 0 ||
       stream_budget_bytes == 0) {
      return serve_error(req, 400, "bad stream budget");
    }
  }
  if(action == GC_ACTION_COMPRESS &&
     websrv_get_query_arg(req, "order", order_arg, sizeof(order_arg))) {
    if(!strcasecmp(order_arg, "budgeted-gain")) {
      snprintf(stream_order, sizeof(stream_order), "%s", "budgeted-gain");
    } else if(!strcasecmp(order_arg, "path")) {
      snprintf(stream_order, sizeof(stream_order), "%s", "path");
    } else {
      return serve_error(req, 400, "bad stream order");
    }
  }
  if(makes_image &&
     websrv_get_query_arg(req, "skipSpaceCheck", skip_space_arg,
                          sizeof(skip_space_arg))) {
    if(!strcasecmp(skip_space_arg, "1") ||
       !strcasecmp(skip_space_arg, "true") ||
       !strcasecmp(skip_space_arg, "yes")) {
      skip_space_check = 1;
    } else if(!strcasecmp(skip_space_arg, "0") ||
              !strcasecmp(skip_space_arg, "false") ||
              !strcasecmp(skip_space_arg, "no")) {
      skip_space_check = 0;
    } else {
      return serve_error(req, 400, "bad skipSpaceCheck");
    }
  }
  if(makes_image &&
     websrv_get_query_arg(req, "usbId", usb_id, sizeof(usb_id))) {
    const gc_storage_target_def_t *def = storage_target_def_for_id(usb_id);
    if(storage_target_root_for_id(usb_id, target_root,
                                  sizeof(target_root), NULL) != 0 || !def) {
      return serve_error(req, 400, "bad storage target");
    }
    if(path_under_root(source_path_arg, def->root)) {
      return serve_error(req, 409,
                         "game is already on selected external storage");
    }
    if(requested_delete_policy[0] && !strcmp(requested_delete_policy, "stream")) {
      return serve_error(req, 400,
                         action == GC_ACTION_MAKE_IMAGE ?
                         "destructive image creation is not available while moving to external storage" :
                         "destructive compression is not available while moving to external storage");
    }
    if(!requested_delete_policy[0]) {
      snprintf(requested_delete_policy, sizeof(requested_delete_policy), "%s",
               "after");
    }
  } else if(makes_image && compress_to_internal) {
    if(path_under_root(source_path_arg, "/data")) {
      return serve_error(req, 409, "game is already on internal SSD");
    }
    if(requested_delete_policy[0] && !strcmp(requested_delete_policy, "stream")) {
      return serve_error(req, 400,
                         action == GC_ACTION_MAKE_IMAGE ?
                         "destructive image creation is not available while moving to internal SSD" :
                         "destructive compression is not available while moving to internal SSD");
    }
    snprintf(target_root, sizeof(target_root), "%s", GC_INTERNAL_GAME_ROOT);
    if(!requested_delete_policy[0]) {
      snprintf(requested_delete_policy, sizeof(requested_delete_policy), "%s",
               "after");
    }
  } else if(makes_image && destination_arg[0] &&
            (!strcasecmp(destination_arg, "usb") ||
             !strcasecmp(destination_arg, "external"))) {
    return serve_error(req, 400, "bad storage target");
  }
  if(action == GC_ACTION_UNCOMPRESS) {
    if(uncompress_to_internal) {
      if(path_under_root(source_path_arg, "/data")) {
        return serve_error(req, 409, "compressed game is already on internal SSD");
      }
      snprintf(target_root, sizeof(target_root), "%s", GC_INTERNAL_GAME_ROOT);
    } else if(websrv_get_query_arg(req, "usbId", usb_id, sizeof(usb_id))) {
      const gc_storage_target_def_t *def = storage_target_def_for_id(usb_id);
      if(storage_target_root_for_id(usb_id, target_root,
                                    sizeof(target_root), NULL) != 0 || !def) {
        return serve_error(req, 400, "bad storage target");
      }
      if(path_under_root(source_path_arg, def->root)) {
        return serve_error(req, 409,
                           "compressed game is already on selected external storage");
      }
    } else if(destination_arg[0] &&
              (!strcasecmp(destination_arg, "usb") ||
               !strcasecmp(destination_arg, "external"))) {
      return serve_error(req, 400, "bad storage target");
    }
  }
  if(action == GC_ACTION_COMPRESS && skip_space_check &&
     requested_delete_policy[0] && !strcmp(requested_delete_policy, "stream")) {
    return serve_error(req, 400,
                       "skipSpaceCheck is not available for destructive compression");
  }
  if(action == GC_ACTION_VALIDATE_REPAIR ||
     action == GC_ACTION_VALIDATE_ONLY ||
     action == GC_ACTION_REFRESH_MOUNT) {
    snprintf(delete_policy, sizeof(delete_policy), "%s", "none");
  } else if(makes_image && target_root[0]) {
    snprintf(delete_policy, sizeof(delete_policy), "%s",
             !strcmp(requested_delete_policy, "keep") ? "keep" : "after");
  } else {
    if(action == GC_ACTION_COMPRESS &&
       requested_delete_policy[0] &&
       !strcmp(requested_delete_policy, "stream")) {
      snprintf(delete_policy, sizeof(delete_policy), "%s", "stream");
    } else if(requested_delete_policy[0] &&
              !strcmp(requested_delete_policy, "keep")) {
      snprintf(delete_policy, sizeof(delete_policy), "%s", "keep");
    } else if(requested_delete_policy[0] &&
              !strcmp(requested_delete_policy, "after")) {
      snprintf(delete_policy, sizeof(delete_policy), "%s", "after");
    }
  }
  pthread_mutex_lock(&g_gc_lock);
  int response = 0;
  gc_operation_t *op = alloc_queued_operation_locked(req, title_id, action,
                                                     game.name, &response);
  if(!op) return response;
  snprintf(op->source_path, sizeof(op->source_path), "%s", game.source_path);
  op->output_path[0] = 0;
  snprintf(op->source_kind, sizeof(op->source_kind), "%s",
           source_kind_name(game.source_kind));
  snprintf(op->format, sizeof(op->format), "%s", format);
  snprintf(op->delete_policy, sizeof(op->delete_policy), "%s",
           delete_policy);
  snprintf(op->compression_mode, sizeof(op->compression_mode), "%s",
           compression_mode_or_default(NULL));
  snprintf(op->stream_order, sizeof(op->stream_order), "%s", stream_order);
  op->stream_budget_bytes = stream_budget_bytes;
  op->skip_space_check = skip_space_check;
  snprintf(op->target_root, sizeof(op->target_root), "%s", target_root);
  snprintf(op->preserve_original, sizeof(op->preserve_original), "%s",
           preserve_original);
  return enqueue_start_response_locked(req, op);
}

static const char *
nested_type_name(int nested_type) {
  if(nested_type == PFS_NESTED_PFS) return "pfs";
  if(nested_type == PFS_NESTED_EXFAT) return "exfat";
  return "unknown";
}

static void
infer_uncompress_nested_from_shadow(const char *source_path,
                                    char *nested_name,
                                    size_t nested_name_size,
                                    int *nested_type) {
  char mount_dir[1024];
  DIR *d = NULL;
  if(nested_name && nested_name_size) {
    snprintf(nested_name, nested_name_size, "%s", "pfs_image.dat");
  }
  if(nested_type) *nested_type = PFS_NESTED_PFS;
  if(!source_path ||
     shadow_pfsc_mount_dir_for_outer(source_path, mount_dir,
                                     sizeof(mount_dir)) != 0) {
    return;
  }
  d = opendir(mount_dir);
  if(!d) return;
  struct dirent *ent;
  while((ent = readdir(d))) {
    if(!strcmp(ent->d_name, ".") || !strcmp(ent->d_name, "..")) continue;
    if(!upload_segment_safe(ent->d_name)) continue;
    if(ends_with_ci(ent->d_name, ".exfat")) {
      if(nested_name && nested_name_size) {
        snprintf(nested_name, nested_name_size, "%s", ent->d_name);
      }
      if(nested_type) *nested_type = PFS_NESTED_EXFAT;
      break;
    }
    if(!strcmp(ent->d_name, "pfs_image.dat") ||
       ends_with_ci(ent->d_name, ".ffpfs") ||
       ends_with_ci(ent->d_name, ".pfs")) {
      if(nested_name && nested_name_size) {
        snprintf(nested_name, nested_name_size, "%s", ent->d_name);
      }
      if(nested_type) *nested_type = PFS_NESTED_PFS;
      break;
    }
  }
  closedir(d);
}

static int
build_default_uncompress_image_path(const char *source_path, int nested_type,
                                    char *out, size_t out_size,
                                    char *err, size_t err_size) {
  char parent[1024];
  const char *name = base_name(source_path);
  char stem[256];
  const char *ext = nested_type == PFS_NESTED_EXFAT ? ".exfat" : ".ffpfs";
  if(!source_path || !name || !ends_with_ci(name, ".ffpfsc") ||
     path_parent(source_path, parent, sizeof(parent)) != 0) {
    snprintf(err, err_size, "%s", "bad compressed image path");
    errno = EINVAL;
    return -1;
  }
  size_t name_len = strlen(name);
  size_t ext_len = strlen(".ffpfsc");
  if(name_len <= ext_len || name_len - ext_len >= sizeof(stem)) {
    snprintf(err, err_size, "%s", "bad compressed image name");
    errno = EINVAL;
    return -1;
  }
  memcpy(stem, name, name_len - ext_len);
  stem[name_len - ext_len] = 0;
  int n = snprintf(out, out_size, "%s%s%s%s", parent, parent[1] ? "/" : "",
                   stem, ext);
  if(n < 0 || (size_t)n >= out_size) {
    snprintf(err, err_size, "%s", "image output path too long");
    errno = ENAMETOOLONG;
    return -1;
  }
  return 0;
}

static int
uncompress_plan_request(const http_request_t *req) {
  char title_id[64];
  char source_path_arg[1024] = "";
  char err[256] = {0};
  gc_game_t game;
  gc_operation_t op;
  char nested_name[256];
  char image_output_path[1024];
  int nested_type = PFS_NESTED_PFS;
  if(!websrv_get_query_arg(req, "titleId", title_id, sizeof(title_id)) ||
     !valid_title_id(title_id)) {
    return serve_error(req, 400, "bad titleId");
  }
  int source_rc = get_required_source_path_arg(req, source_path_arg,
                                               sizeof(source_path_arg));
  if(source_rc == -2) {
    return serve_error(req, 400, "sourcePath required");
  }
  if(source_rc != 0) {
    return serve_error(req, 400, "bad source path");
  }
  memset(&op, 0, sizeof(op));
  snprintf(op.title_id, sizeof(op.title_id), "%s", title_id);
  snprintf(op.display_name, sizeof(op.display_name), "%s", title_id);
  snprintf(op.source_path, sizeof(op.source_path), "%s", source_path_arg);
  if(find_game_for_operation_source_path(&op, &game, 0) != 0 ||
     game.source_kind != GC_SOURCE_COMPRESSED) {
    return serve_error(req, 400, "game is not compressed");
  }
  infer_uncompress_nested_from_shadow(game.source_path, nested_name,
                                      sizeof(nested_name), &nested_type);
  if(build_default_uncompress_image_path(game.source_path, nested_type,
                                         image_output_path,
                                         sizeof(image_output_path),
                                         err, sizeof(err)) != 0) {
    return serve_error(req, 400,
                       err[0] ? err : "could not inspect compressed image");
  }
  json_buf_t b = {0};
  const char *type = nested_type_name(nested_type);
  const char *ext = nested_type == PFS_NESTED_EXFAT ? ".exfat" : ".ffpfs";
  if(json_append(&b, "{\"ok\":true,\"titleId\":") != 0 ||
     json_string(&b, title_id) != 0 ||
     json_append(&b, ",\"sourcePath\":") != 0 ||
     json_string(&b, game.source_path) != 0 ||
     json_append(&b, ",\"sourceStorage\":") != 0 ||
     json_string(&b, storage_name_for_path(game.source_path)) != 0 ||
     json_append(&b, ",\"nestedName\":") != 0 ||
     json_string(&b, nested_name) != 0 ||
     json_append(&b, ",\"nestedType\":") != 0 ||
     json_string(&b, type) != 0 ||
     json_append(&b, ",\"imageExtension\":") != 0 ||
     json_string(&b, ext) != 0 ||
     json_append(&b, ",\"filesOutputPath\":") != 0 ||
     json_string(&b, game.output_path) != 0 ||
     json_append(&b, ",\"imageOutputPath\":") != 0 ||
     json_string(&b, image_output_path) != 0 ||
     json_appendf(&b,
                  ",\"requiredBytes\":%llu,\"freeBytes\":%llu,"
                  "\"extraBytes\":%llu}",
                  (unsigned long long)game.required_bytes,
                  (unsigned long long)game.free_bytes,
                  (unsigned long long)game.extra_needed) != 0) {
    free(b.data);
    return -1;
  }
  return serve_owned(req, 200, b.data, b.len);
}

static int
enqueue_move_action(const http_request_t *req, gc_action_t action) {
  char title_id[64];
  char source_path_arg[1024] = "";
  char usb_id[16];
  char target_root[1024] = "";
  int copy_only = transfer_action_copy_only(action);

  const char *arg_err = read_title_source_args(req, title_id,
                                               sizeof(title_id),
                                               source_path_arg,
                                               sizeof(source_path_arg));
  if(arg_err) return serve_error(req, 400, arg_err);
  if(title_operation_busy(title_id)) {
    return serve_error(req, 409, "action already running for this game");
  }
  if(transfer_action_to_external(action)) {
    const gc_storage_target_def_t *def;
    if(!websrv_get_query_arg(req, "usbId", usb_id, sizeof(usb_id)) ||
       !(def = storage_target_def_for_id(usb_id))) {
      return serve_error(req, 400, "bad storage target");
    }
    if(path_under_root(source_path_arg, def->root)) {
      return serve_error(req, 409,
                         "game is already on selected external storage");
    }
    if(resolve_transfer_target_root(def->root, def->target_root,
                                    target_root,
                                    sizeof(target_root)) != 0) {
      return serve_error(req, 400, "bad storage target");
    }
  } else if(transfer_action_to_internal(action)) {
    if(path_under_root(source_path_arg, "/data")) {
      return serve_error(req, 409, "game is already on internal SSD");
    }
    if(resolve_transfer_target_root("/data", GC_INTERNAL_GAME_ROOT,
                                    target_root,
                                    sizeof(target_root)) != 0) {
      return serve_error(req, 400, "bad internal target");
    }
  } else {
    return serve_error(req, 400, "bad transfer action");
  }

  pthread_mutex_lock(&g_gc_lock);
  int response = 0;
  gc_operation_t *op = alloc_queued_operation_locked(req, title_id, action,
                                                     title_id, &response);
  if(!op) return response;
  snprintf(op->source_path, sizeof(op->source_path), "%s", source_path_arg);
  snprintf(op->source_kind, sizeof(op->source_kind), "%s",
           source_kind_name_from_path(source_path_arg));
  snprintf(op->format, sizeof(op->format), "%s", "pfs");
  snprintf(op->delete_policy, sizeof(op->delete_policy), "%s", "after");
  if(copy_only) {
    snprintf(op->delete_policy, sizeof(op->delete_policy), "%s", "keep");
  }
  snprintf(op->target_root, sizeof(op->target_root), "%s", target_root);
  return enqueue_start_response_locked(req, op);
}

static int
enqueue_delete_game_data_action(const http_request_t *req) {
  char title_id[64];
  char source_path_arg[1024] = "";
  char confirm_arg[16] = "";

  if(strcmp(req->method, "POST")) {
    return serve_error(req, 405, "method not allowed");
  }
  const char *arg_err = read_title_source_args(req, title_id,
                                               sizeof(title_id),
                                               source_path_arg,
                                               sizeof(source_path_arg));
  if(arg_err) return serve_error(req, 400, arg_err);
  if(!websrv_get_query_arg(req, "confirm", confirm_arg,
                           sizeof(confirm_arg)) ||
     strcmp(confirm_arg, "delete")) {
    return serve_error(req, 400, "delete confirmation is required");
  }

  if(title_operation_busy(title_id)) {
    return serve_error(req, 409, "action already running for this game");
  }

  pthread_mutex_lock(&g_gc_lock);
  int response = 0;
  gc_operation_t *op = alloc_queued_operation_locked(
      req, title_id, GC_ACTION_DELETE_GAME_DATA, title_id, &response);
  if(!op) return response;
  snprintf(op->source_path, sizeof(op->source_path), "%s", source_path_arg);
  snprintf(op->output_path, sizeof(op->output_path), "%s", source_path_arg);
  snprintf(op->source_kind, sizeof(op->source_kind), "%s",
           source_kind_name_from_path(source_path_arg));
  snprintf(op->format, sizeof(op->format), "%s", "none");
  snprintf(op->delete_policy, sizeof(op->delete_policy), "%s", "delete");
  return enqueue_start_response_locked(req, op);
}

static int
enqueue_read_speed_test_action(const http_request_t *req) {
  char title_id[64];
  char source_path_arg[1024] = "";

  if(strcmp(req->method, "POST")) {
    return serve_error(req, 405, "method not allowed");
  }
  const char *arg_err = read_title_source_args(req, title_id,
                                               sizeof(title_id),
                                               source_path_arg,
                                               sizeof(source_path_arg));
  if(arg_err) return serve_error(req, 400, arg_err);

  if(title_operation_busy(title_id)) {
    return serve_error(req, 409, "action already running for this game");
  }

  pthread_mutex_lock(&g_gc_lock);
  int response = 0;
  gc_operation_t *op = alloc_queued_operation_locked(
      req, title_id, GC_ACTION_READ_SPEED_TEST, title_id, &response);
  if(!op) return response;
  snprintf(op->source_path, sizeof(op->source_path), "%s", source_path_arg);
  snprintf(op->source_kind, sizeof(op->source_kind), "%s",
           source_kind_name_from_path(source_path_arg));
  snprintf(op->format, sizeof(op->format), "%s", "read");
  snprintf(op->delete_policy, sizeof(op->delete_policy), "%s", "none");
  return enqueue_start_response_locked(req, op);
}

static int
enqueue_build_ampr_index_action(const http_request_t *req) {
  char title_id[64];
  char source_path_arg[1024] = "";
  struct stat st;
  gc_source_kind_t source_kind;
  gc_game_t game;
  int ampr_present = 0;

  if(strcmp(req->method, "POST")) {
    return serve_error(req, 405, "method not allowed");
  }
  const char *arg_err = read_title_source_args(req, title_id,
                                               sizeof(title_id),
                                               source_path_arg,
                                               sizeof(source_path_arg));
  if(arg_err) return serve_error(req, 400, arg_err);
  if(stat(source_path_arg, &st) != 0) {
    return serve_error(req, 400, "AMPR index source is unavailable");
  }
  source_kind =
      source_kind_from_name(source_kind_name_from_path(source_path_arg));
  if(source_kind != GC_SOURCE_FOLDER &&
     source_kind != GC_SOURCE_IMAGE &&
     source_kind != GC_SOURCE_COMPRESSED) {
    return serve_error(req, 400,
                       "AMPR index can only be built for a folder, exFAT image, or compressed image");
  }
  memset(&game, 0, sizeof(game));
  if(lookup_game_by_source_path(source_path_arg, &game) == 0) {
    ampr_present = game.ampr_present;
  } else if(source_kind == GC_SOURCE_FOLDER) {
    ampr_present =
        ampr_folder_target_probe(source_path_arg, NULL, 0, NULL);
  }
  if(!ampr_present) {
    return serve_error(req, 400,
                       "libSceAmpr.sprx or libSceAmpr.prx was not found for this game");
  }

  if(title_operation_busy(title_id)) {
    return serve_error(req, 409, "action already running for this game");
  }

  pthread_mutex_lock(&g_gc_lock);
  int response = 0;
  gc_operation_t *op = alloc_queued_operation_locked(
      req, title_id, GC_ACTION_BUILD_AMPR_INDEX, title_id, &response);
  if(!op) return response;
  snprintf(op->source_path, sizeof(op->source_path), "%s", source_path_arg);
  if(source_kind == GC_SOURCE_FOLDER) {
    snprintf(op->output_path, sizeof(op->output_path), "%s/ampr_emu.index",
             source_path_arg);
  } else {
    snprintf(op->output_path, sizeof(op->output_path), "%s", source_path_arg);
  }
  snprintf(op->source_kind, sizeof(op->source_kind), "%s",
           source_kind_name(source_kind));
  snprintf(op->format, sizeof(op->format), "%s", "ampr");
  snprintf(op->delete_policy, sizeof(op->delete_policy), "%s", "none");
  return enqueue_start_response_locked(req, op);
}

static int
enqueue_update_ampr_action(const http_request_t *req) {
  char title_id[64];
  char source_path_arg[1024] = "";
  char version[64] = "";
  char sha[65] = "";
  char intent_arg[16] = "";
  const char *intent = "manual";
  char cache_path[1024] = "";
  char actual_sha[65] = {0};
  char err[256] = {0};
  gc_game_t game;
  if(strcmp(req->method, "POST")) {
    return serve_error(req, 405, "method not allowed");
  }
  const char *arg_err = read_title_source_args(req, title_id,
                                               sizeof(title_id),
                                               source_path_arg,
                                               sizeof(source_path_arg));
  if(arg_err) return serve_error(req, 400, arg_err);
  if(!websrv_get_query_arg(req, "version", version, sizeof(version)) ||
     !ampr_version_safe(version)) {
    return serve_error(req, 400, "bad AMPR version");
  }
  if(!websrv_get_query_arg(req, "sha256", sha, sizeof(sha)) ||
     !sha256_hex_valid(sha)) {
    return serve_error(req, 400, "bad AMPR SHA-256");
  }
  if(ampr_cache_binary_path(version, cache_path, sizeof(cache_path)) != 0) {
    return serve_error(req, 400, "bad AMPR cache path");
  }
  if(hash_file_sha256_hex(cache_path, actual_sha, err, sizeof(err)) != 0) {
    return serve_error(req, 404, err[0] ? err : "cached AMPR binary not found");
  }
  if(strcasecmp(actual_sha, sha) != 0) {
    return serve_error(req, 409, "cached AMPR hash mismatch");
  }
  if(websrv_get_query_arg(req, "intent", intent_arg, sizeof(intent_arg))) {
    intent = ampr_normalized_intent(intent_arg, version);
  } else {
    intent = ampr_normalized_intent(NULL, version);
  }
  if(!strcmp(intent, "latest")) {
    char latest_version[64] = {0};
    char latest_sha[65] = {0};
    if(!ampr_latest_cached(latest_version, sizeof(latest_version),
                           latest_sha) ||
       strcmp(latest_version, version) ||
       strcasecmp(latest_sha, sha) != 0) {
      intent = "manual";
    }
  }
  memset(&game, 0, sizeof(game));
  snprintf(game.title_id, sizeof(game.title_id), "%s", title_id);
  snprintf(game.name, sizeof(game.name), "%s", title_id);
  snprintf(game.source_path, sizeof(game.source_path), "%s", source_path_arg);
  game.source_kind = source_kind_from_name(source_kind_name_from_path(source_path_arg));
  (void)lookup_game_by_source_path(source_path_arg, &game);
  pthread_mutex_lock(&g_gc_lock);
  int response = 0;
  gc_operation_t *op = alloc_queued_operation_locked(
      req, title_id, GC_ACTION_UPDATE_AMPR, game.name, &response);
  if(!op) return response;
  snprintf(op->source_path, sizeof(op->source_path), "%s", game.source_path);
  snprintf(op->source_kind, sizeof(op->source_kind), "%s",
           source_kind_name(game.source_kind));
  snprintf(op->format, sizeof(op->format), "%s", "ampr");
  snprintf(op->delete_policy, sizeof(op->delete_policy), "%s", "none");
  snprintf(op->ampr_version, sizeof(op->ampr_version), "%s", version);
  snprintf(op->ampr_sha256, sizeof(op->ampr_sha256), "%s", sha);
  snprintf(op->ampr_cache_path, sizeof(op->ampr_cache_path), "%s", cache_path);
  snprintf(op->ampr_intent, sizeof(op->ampr_intent), "%s", intent);
  return enqueue_start_response_locked(req, op);
}

static int
enqueue_restore_ampr_original_action(const http_request_t *req) {
  char title_id[64];
  char source_path_arg[1024] = "";
  char original_path[1024] = "";
  char original_sha[65] = {0};
  char actual_sha[65] = {0};
  char err[256] = {0};
  uint64_t original_size = 0;
  gc_game_t game;
  if(strcmp(req->method, "POST")) {
    return serve_error(req, 405, "method not allowed");
  }
  const char *arg_err = read_title_source_args(req, title_id,
                                               sizeof(title_id),
                                               source_path_arg,
                                               sizeof(source_path_arg));
  if(arg_err) return serve_error(req, 400, arg_err);
  if(title_operation_busy(title_id)) {
    return serve_error(req, 409, "action already running for this game");
  }
  if(!ampr_original_lookup(title_id, source_path_arg, original_path,
                           original_sha, &original_size)) {
    return serve_error(req, 404, "original APR-EMU backup was not found");
  }
  (void)original_size;
  if(hash_file_sha256_hex(original_path, actual_sha, err, sizeof(err)) != 0) {
    return serve_error(req, 404,
                       err[0] ? err : "original APR-EMU backup is unreadable");
  }
  if(strcasecmp(actual_sha, original_sha) != 0) {
    return serve_error(req, 409, "original APR-EMU backup hash mismatch");
  }
  memset(&game, 0, sizeof(game));
  snprintf(game.title_id, sizeof(game.title_id), "%s", title_id);
  snprintf(game.name, sizeof(game.name), "%s", title_id);
  snprintf(game.source_path, sizeof(game.source_path), "%s", source_path_arg);
  game.source_kind =
      source_kind_from_name(source_kind_name_from_path(source_path_arg));
  (void)lookup_game_by_source_path(source_path_arg, &game);

  pthread_mutex_lock(&g_gc_lock);
  int response = 0;
  gc_operation_t *op = alloc_queued_operation_locked(
      req, title_id, GC_ACTION_UPDATE_AMPR, game.name, &response);
  if(!op) return response;
  snprintf(op->source_path, sizeof(op->source_path), "%s", game.source_path);
  snprintf(op->source_kind, sizeof(op->source_kind), "%s",
           source_kind_name(game.source_kind));
  snprintf(op->format, sizeof(op->format), "%s", "ampr");
  snprintf(op->delete_policy, sizeof(op->delete_policy), "%s", "none");
  snprintf(op->ampr_version, sizeof(op->ampr_version), "%s", "original");
  snprintf(op->ampr_sha256, sizeof(op->ampr_sha256), "%s", original_sha);
  snprintf(op->ampr_cache_path, sizeof(op->ampr_cache_path), "%s",
           original_path);
  snprintf(op->ampr_intent, sizeof(op->ampr_intent), "%s", "custom");
  return enqueue_start_response_locked(req, op);
}

static int
enqueue_extract_image_action(const http_request_t *req) {
  char title_id[64];
  char source_path_arg[1024] = "";
  gc_game_t game;
  gc_operation_t probe_op = {0};
  char err[256] = {0};
  char target_path[1024] = {0};

  if(strcmp(req->method, "POST")) {
    return serve_error(req, 405, "method not allowed");
  }
  const char *arg_err = read_title_source_args(req, title_id,
                                               sizeof(title_id),
                                               source_path_arg,
                                               sizeof(source_path_arg));
  if(arg_err) return serve_error(req, 400, arg_err);
  if(title_operation_busy(title_id)) {
    return serve_error(req, 409, "action already running for this game");
  }

  memset(&game, 0, sizeof(game));
  snprintf(probe_op.title_id, sizeof(probe_op.title_id), "%s", title_id);
  snprintf(probe_op.display_name, sizeof(probe_op.display_name), "%s",
           title_id);
  snprintf(probe_op.source_path, sizeof(probe_op.source_path), "%s",
           source_path_arg);
  if(find_game_for_operation_source_path(&probe_op, &game, 0) != 0 ||
     game.source_kind != GC_SOURCE_IMAGE) {
    return serve_error(req, 400, "game is not an image");
  }
  if(!game.is_mounted || !game.mount_path[0]) {
    return serve_error(req, 409, "image must be mounted before extract");
  }
  if(build_extract_image_target_path(&game, target_path, sizeof(target_path),
                                     err, sizeof(err)) != 0) {
    return serve_error(req, 400, err[0] ? err : "bad extract target");
  }

  pthread_mutex_lock(&g_gc_lock);
  int response = 0;
  gc_operation_t *op = alloc_queued_operation_locked(
      req, title_id, GC_ACTION_EXTRACT_IMAGE,
      game.name[0] ? game.name : title_id, &response);
  if(!op) return response;
  snprintf(op->source_path, sizeof(op->source_path), "%s", source_path_arg);
  snprintf(op->output_path, sizeof(op->output_path), "%s", target_path);
  snprintf(op->source_kind, sizeof(op->source_kind), "%s", "image");
  snprintf(op->format, sizeof(op->format), "%s", "folder");
  snprintf(op->delete_policy, sizeof(op->delete_policy), "%s", "keep");
  return enqueue_start_response_locked(req, op);
}

static int
enqueue_set_read_only_action(const http_request_t *req) {
  char title_id[64];
  char source_path_arg[1024] = "";
  gc_game_t game;
  gc_operation_t probe_op = {0};

  if(strcmp(req->method, "POST")) {
    return serve_error(req, 405, "method not allowed");
  }
  const char *arg_err = read_title_source_args(req, title_id,
                                               sizeof(title_id),
                                               source_path_arg,
                                               sizeof(source_path_arg));
  if(arg_err) return serve_error(req, 400, arg_err);
  if(title_operation_busy(title_id)) {
    return serve_error(req, 409, "action already running for this game");
  }

  memset(&game, 0, sizeof(game));
  snprintf(probe_op.title_id, sizeof(probe_op.title_id), "%s", title_id);
  snprintf(probe_op.display_name, sizeof(probe_op.display_name), "%s",
           title_id);
  snprintf(probe_op.source_path, sizeof(probe_op.source_path), "%s",
           source_path_arg);
  if(find_game_for_operation_source_path(&probe_op, &game, 0) != 0 ||
     game.source_kind != GC_SOURCE_IMAGE) {
    return serve_error(req, 400, "game is not an image");
  }

  pthread_mutex_lock(&g_gc_lock);
  int response = 0;
  gc_operation_t *op = alloc_queued_operation_locked(
      req, title_id, GC_ACTION_SET_READ_ONLY,
      game.name[0] ? game.name : title_id, &response);
  if(!op) return response;
  snprintf(op->source_path, sizeof(op->source_path), "%s", source_path_arg);
  snprintf(op->output_path, sizeof(op->output_path), "%s", source_path_arg);
  snprintf(op->source_kind, sizeof(op->source_kind), "%s", "image");
  snprintf(op->format, sizeof(op->format), "%s", "image");
  snprintf(op->delete_policy, sizeof(op->delete_policy), "%s", "none");
  return enqueue_start_response_locked(req, op);
}

static int
refresh_mount_request(const http_request_t *req) {
  return enqueue_action(req, GC_ACTION_REFRESH_MOUNT);
}

static int
bad_blocks_request(const http_request_t *req) {
  char dir[1024];
  char path[1024];
  char *data = NULL;
  size_t size = 0;

  if(!websrv_get_query_arg(req, "dir", dir, sizeof(dir)) ||
     !path_is_safe(dir) ||
     !path_under_root(dir, GC_REPAIR_DIR)) {
    return serve_error(req, 400, "bad repair log path");
  }
  join_path(path, sizeof(path), dir, "bad_blocks.tsv");
  if(!path_is_safe(path) || !path_under_root(path, GC_REPAIR_DIR)) {
    return serve_error(req, 400, "bad repair log path");
  }
  if(read_file_limited(path, &data, &size, 8 * 1024 * 1024) != 0) {
    return serve_error(req, 404, "bad block log is unavailable");
  }
  int rc = websrv_send(req->fd, 200, "text/plain; charset=utf-8", data, size);
  free(data);
  return rc;
}

static int
append_op_summary(json_buf_t *b, const gc_operation_t *op) {
  return append_operation_json(b, op, op->id, 0);
}

static int
operation_matches_game_instance(const gc_operation_t *op,
                                const gc_game_t *game) {
  if(!op || !game || strcmp(op->title_id, game->title_id)) return 0;
  if(op->source_path[0]) {
    return game_source_matches(game, op->source_path);
  }
  return 1;
}

static int
operations_same_instance(const gc_operation_t *a, const gc_operation_t *b) {
  if(!a || !b || strcmp(a->title_id, b->title_id)) return 0;
  if(a->source_path[0] && b->source_path[0]) {
    return paths_equal_ignoring_trailing_slash(a->source_path, b->source_path);
  }
  return 1;
}

static gc_operation_t *
active_op_for_game_locked(const gc_game_t *game) {
  for(size_t i = 0; i < GC_MAX_OPS; i++) {
    if(g_ops[i].used && g_ops[i].status == GC_OP_RUNNING &&
       operation_matches_game_instance(&g_ops[i], game)) {
      return &g_ops[i];
    }
  }
  return NULL;
}

static gc_operation_t *
pending_op_for_game_locked(const gc_game_t *game) {
  for(size_t i = 0; i < GC_MAX_OPS; i++) {
    if(g_ops[i].used && g_ops[i].status == GC_OP_QUEUED &&
       operation_matches_game_instance(&g_ops[i], game)) {
      return &g_ops[i];
    }
  }
  return NULL;
}

static int
append_game_summary(json_buf_t *b, int *first, const gc_game_t *game,
                    const gc_operation_t *active,
                    const gc_operation_t *pending) {
  if(!b || !first || !game) return -1;
  if(!*first && json_append(b, ",") != 0) return -1;
  char instance_id[128];
  game_instance_id(game, instance_id, sizeof(instance_id));
  uint64_t stream_min = game->size_estimated
      ? 0
      : stream_min_free_bytes(game->source_size);
  uint64_t stream_extra = game->size_estimated ? 0 : stream_extra_needed(game);
  int can_move_to_external = game->source_kind != GC_SOURCE_UNKNOWN &&
      can_move_to_external_storage(game->source_path);
  int apr_indexed = game->apr_indexed ||
      (active && active->apr_indexed) ||
      (pending && pending->apr_indexed);
  int ampr_hot_swap_optimized = game->ampr_hot_swap_optimized ||
      (active && active->ampr_hot_swap_optimized) ||
      (pending && pending->ampr_hot_swap_optimized);
  if(json_append(b, "{\"titleId\":") != 0 ||
     json_string(b, game->title_id) != 0 ||
     json_append(b, ",\"instanceId\":") != 0 ||
     json_string(b, instance_id) != 0 ||
     json_append(b, ",\"name\":") != 0 ||
     json_string(b, game->name) != 0 ||
     json_append(b, ",\"sourceKind\":") != 0 ||
     json_string(b, source_kind_name(game->source_kind)) != 0 ||
     json_append(b, ",\"sourcePath\":") != 0 ||
     json_string(b, game->source_path) != 0 ||
     json_append(b, ",\"outputPath\":") != 0 ||
     json_string(b, game->output_path) != 0 ||
     json_append(b, ",\"storage\":") != 0 ||
     json_string(b, storage_name_for_path(game->source_path)) != 0 ||
     json_append(b, ",\"mountStatus\":") != 0 ||
     json_string(b, game->mount_status[0] ? game->mount_status :
                 (game->is_mounted ? "mounted" : "not-mounted")) != 0 ||
     json_append(b, ",\"validation\":") != 0 ||
     json_string(b, game->validation_status) != 0 ||
     json_append(b, ",\"primaryAction\":") != 0 ||
     json_string(b, game->primary_action) != 0 ||
     json_append(b, ",\"amprPath\":") != 0 ||
     json_string(b, game->ampr_path) != 0 ||
     json_append(b, ",\"amprVersion\":") != 0 ||
     json_string(b, game->ampr_version) != 0 ||
     json_append(b, ",\"amprSha256\":") != 0 ||
     json_string(b, game->ampr_sha256) != 0 ||
     json_append(b, ",\"amprLatestVersion\":") != 0 ||
     json_string(b, game->ampr_latest_version) != 0 ||
     json_append(b, ",\"amprLatestSha256\":") != 0 ||
     json_string(b, game->ampr_latest_sha256) != 0 ||
     json_append(b, ",\"amprOriginalSha256\":") != 0 ||
     json_string(b, game->ampr_original_sha256) != 0 ||
     json_appendf(b,
                  ",\"sourceSize\":%llu,\"freeBytes\":%llu,"
                  "\"requiredBytes\":%llu,\"extraBytes\":%llu,"
                  "\"streamMinFreeBytes\":%llu,\"streamExtraBytes\":%llu,"
                  "\"compressionSourceSize\":%llu,"
                  "\"compressedSize\":%llu,"
                  "\"savedBytes\":%llu,"
                  "\"aprIndexed\":%s,"
                  "\"amprHotSwapOptimized\":%s,"
                  "\"amprPresent\":%s,"
                  "\"amprUpdateNeeded\":%s,"
                  "\"amprUpdateSupported\":%s,"
                  "\"amprOriginalAvailable\":%s,"
                  "\"amprOriginalSize\":%llu,"
                  "\"sizePending\":%s,"
                  "\"sizeStatus\":\"%s\","
                  "\"sizeEstimated\":%s,"
                  "\"sizeMeasuredAt\":%llu,"
                  "\"sizeRefreshing\":%s,"
                  "\"canStreamDelete\":%s,\"isMounted\":%s,"
                  "\"hasIcon\":%s,"
                  "\"iconSize\":%llu,\"iconMtime\":%llu,"
                  "\"outputExists\":%s,\"canMoveToUsb\":%s,"
                  "\"canMoveToInternal\":%s",
                  (unsigned long long)game->source_size,
                  (unsigned long long)game->free_bytes,
                  (unsigned long long)game->required_bytes,
                  (unsigned long long)game->extra_needed,
                  (unsigned long long)stream_min,
                  (unsigned long long)stream_extra,
                  (unsigned long long)game->compression_source_size,
                  (unsigned long long)game->compressed_size,
                  (unsigned long long)game->saved_bytes,
                  apr_indexed ? "true" : "false",
                  ampr_hot_swap_optimized ? "true" : "false",
                  game->ampr_present ? "true" : "false",
                  game->ampr_update_needed ? "true" : "false",
                  game->ampr_update_supported ? "true" : "false",
                  game->ampr_original_available ? "true" : "false",
                  (unsigned long long)game->ampr_original_size,
                  game->size_pending ? "true" : "false",
                  gc_size_status_name(game->size_status),
                  game->size_estimated ? "true" : "false",
                  (unsigned long long)game->size_measured_at,
                  game->size_refreshing ? "true" : "false",
                  game->can_stream_delete ? "true" : "false",
                  game->is_mounted ? "true" : "false",
                  game->has_icon ? "true" : "false",
                  (unsigned long long)game->icon_size,
                  (unsigned long long)game->icon_mtime,
                  game->output_exists ? "true" : "false",
                  can_move_to_external ? "true" : "false",
                  game->source_kind != GC_SOURCE_UNKNOWN &&
                      !path_under_root(game->source_path, "/data")
                      ? "true" : "false") != 0) {
    return -1;
  }
  if(active) {
    if(json_append(b, ",\"activeOp\":") != 0 ||
       append_op_summary(b, active) != 0) return -1;
  }
  if(pending) {
    if(json_append(b, ",\"pendingOp\":") != 0 ||
       append_op_summary(b, pending) != 0) return -1;
  }
  if(json_append(b, "}") != 0) return -1;
  *first = 0;
  return 0;
}

static int
game_list_has_operation_instance(const gc_game_t *games, size_t count,
                                 const gc_operation_t *op) {
  if(!games || !op || !valid_title_id(op->title_id)) return 0;
  for(size_t i = 0; i < count; i++) {
    if(operation_matches_game_instance(op, &games[i])) return 1;
  }
  return 0;
}

static void
operation_fallback_game(const gc_operation_t *op, gc_game_t *game) {
  if(!op || !game) return;
  memset(game, 0, sizeof(*game));
  snprintf(game->title_id, sizeof(game->title_id), "%s", op->title_id);
  snprintf(game->name, sizeof(game->name), "%s",
           op->display_name[0] ? op->display_name : op->title_id);
  game->source_kind = source_kind_from_name(op->source_kind);
	  if(game->source_kind == GC_SOURCE_UNKNOWN &&
	     (op->action == GC_ACTION_UNCOMPRESS ||
	      op->action == GC_ACTION_EXTRACT_IMAGE ||
	      op->action == GC_ACTION_VALIDATE_REPAIR ||
	      op->action == GC_ACTION_VALIDATE_ONLY)) {
    game->source_kind = op->action == GC_ACTION_EXTRACT_IMAGE
        ? GC_SOURCE_IMAGE
        : GC_SOURCE_COMPRESSED;
  }
  snprintf(game->source_path, sizeof(game->source_path), "%s",
           op->source_path[0] ? op->source_path : op->output_path);
  snprintf(game->output_path, sizeof(game->output_path), "%s",
           op->output_path);
  set_game_mount_status(game, 0, "working");
  /*
   * This function runs while rendering /api/gc/games. Even fallback rows for
   * active operations must not recursively measure folders.
   */
  populate_game_size_ex(game, 0, 0);
  game->can_stream_delete =
      game->source_kind == GC_SOURCE_FOLDER ||
      game->source_kind == GC_SOURCE_COMPRESSED;
  load_game_name(game);
  load_game_icon(game);
  load_validation_state(game);
  snprintf(game->primary_action, sizeof(game->primary_action), "%s",
           op->status == GC_OP_QUEUED ? "Pending" : "Working");
}

static int
json_append_raw_len(json_buf_t *b, const char *s, size_t len) {
  if(json_grow(b, len) != 0) return -1;
  if(len) memcpy(b->data + b->len, s, len);
  b->len += len;
  b->data[b->len] = 0;
  return 0;
}

static int
history_operation_key(const char *id, uint64_t created_at,
                      char *out, size_t out_size) {
  char base[64];
  const char *source = id ? id : "";
  const char *colon = strchr(source, ':');
  size_t len = colon ? (size_t)(colon - source) : strlen(source);
  int n;
  if(!out || out_size == 0) return -1;
  if(len == 0) {
    snprintf(base, sizeof(base), "%s", "recovery");
  } else {
    if(len >= sizeof(base)) len = sizeof(base) - 1;
    memcpy(base, source, len);
    base[len] = 0;
  }
  if(created_at != 0) {
    n = snprintf(out, out_size, "%s:%llu", base,
                 (unsigned long long)created_at);
  } else {
    n = snprintf(out, out_size, "%s", base);
  }
  return n < 0 || (size_t)n >= out_size ? -1 : 0;
}

static int
key_in_list(const char *key, char keys[][GC_HISTORY_KEY_SIZE], size_t count) {
  if(!key || !key[0]) return 0;
  for(size_t i = 0; i < count; i++) {
    if(!strcmp(key, keys[i])) return 1;
  }
  return 0;
}

static int
append_history_json_line(json_buf_t *b, int *first, const char *line) {
  const char *p = line;
  size_t len;
  if(!p) return 0;
  while(*p && isspace((unsigned char)*p)) p++;
  len = strlen(p);
  while(len > 0 && isspace((unsigned char)p[len - 1])) len--;
  if(len < 2 || p[0] != '{' || p[len - 1] != '}') return 0;
  if(!*first && json_append(b, ",") != 0) return -1;
  if(json_append_raw_len(b, p, len) != 0) return -1;
  *first = 0;
  return 0;
}

static int
append_persisted_history(json_buf_t *b, int *first,
                         char memory_keys[][GC_HISTORY_KEY_SIZE],
                         size_t memory_key_count) {
  typedef struct history_slot {
    char key[GC_HISTORY_KEY_SIZE];
    char *line;
    uint64_t order;
  } history_slot_t;

  history_slot_t slots[GC_MAX_OPS];
  size_t count = 0;
  uint64_t order = 1;
  FILE *f = fopen(GC_HISTORY_LOG, "r");
  if(!f) return 0;
  memset(slots, 0, sizeof(slots));

  char line[16384];
  while(fgets(line, sizeof(line), f)) {
    char id[64] = {0};
    char key[GC_HISTORY_KEY_SIZE] = {0};
    uint64_t created_at;
    char *copy;
    int slot = -1;
    line[strcspn(line, "\r\n")] = 0;
    if(!json_find_string_value(line, "id", id, sizeof(id))) continue;
    created_at = json_find_u64_value(line, "createdAt", 0);
    if(history_operation_key(id, created_at, key, sizeof(key)) != 0) {
      continue;
    }
    if(key_in_list(key, memory_keys, memory_key_count)) continue;

    copy = strdup(line);
    if(!copy) {
      fclose(f);
      for(size_t i = 0; i < GC_MAX_OPS; i++) free(slots[i].line);
      return -1;
    }

    for(size_t i = 0; i < count; i++) {
      if(!strcmp(slots[i].key, key)) {
        slot = (int)i;
        break;
      }
    }
    if(slot < 0) {
      if(count < GC_MAX_OPS) {
        slot = (int)count++;
      } else {
        uint64_t oldest_order = UINT64_MAX;
        for(size_t i = 0; i < GC_MAX_OPS; i++) {
          if(slots[i].order < oldest_order) {
            oldest_order = slots[i].order;
            slot = (int)i;
          }
        }
      }
      snprintf(slots[slot].key, sizeof(slots[slot].key), "%s", key);
    }
    free(slots[slot].line);
    slots[slot].line = copy;
    slots[slot].order = order++;
  }
  fclose(f);

  for(size_t emitted = 0; emitted < count; emitted++) {
    uint64_t best_order = 0;
    int best = -1;
    for(size_t i = 0; i < count; i++) {
      if(slots[i].line && slots[i].order > best_order) {
        best_order = slots[i].order;
        best = (int)i;
      }
    }
    if(best < 0) break;
    if(append_history_json_line(b, first, slots[best].line) != 0) {
      for(size_t i = 0; i < GC_MAX_OPS; i++) free(slots[i].line);
      return -1;
    }
    slots[best].order = 0;
  }
  for(size_t i = 0; i < GC_MAX_OPS; i++) free(slots[i].line);
  return 0;
}

static int
games_request(const http_request_t *req) {
  gc_game_t *games = calloc(GC_MAX_GAMES, sizeof(*games));
  size_t count = 0;
  int first = 1;
  int failed = 0;
  if(!games) return serve_error(req, 500, "out of memory");
  discover_games(games, GC_MAX_GAMES, &count, 0);
  queue_folder_game_size_rechecks(games, count);
  for(size_t i = 0; i < count; i++) {
    size_cache_apply_display_to_game(&games[i]);
  }

  json_buf_t b = {0};
  if(json_append(&b, "{\"ok\":true,\"games\":[") != 0) {
    free(b.data);
    free(games);
    return -1;
  }
  pthread_mutex_lock(&g_gc_lock);
  for(size_t i = 0; i < count; i++) {
    gc_operation_t *active = active_op_for_game_locked(&games[i]);
    gc_operation_t *pending = pending_op_for_game_locked(&games[i]);
    if(append_game_summary(&b, &first, &games[i], active, pending) != 0) {
      failed = 1;
      break;
    }
  }
  for(size_t i = 0; !failed && i < GC_MAX_OPS; i++) {
    gc_operation_t *op = &g_ops[i];
    gc_operation_t *active;
    gc_operation_t *pending;
    gc_game_t fallback;
    int already_emitted = 0;
    if(!op->used ||
       (op->status != GC_OP_RUNNING && op->status != GC_OP_QUEUED) ||
       !valid_title_id(op->title_id) ||
       game_list_has_operation_instance(games, count, op)) {
      continue;
    }
    for(size_t j = 0; j < i; j++) {
      if(g_ops[j].used &&
         (g_ops[j].status == GC_OP_RUNNING ||
          g_ops[j].status == GC_OP_QUEUED) &&
         operations_same_instance(&g_ops[j], op)) {
        already_emitted = 1;
        break;
      }
    }
    if(already_emitted) continue;
    active = active_op_for_title_locked(op->title_id);
    pending = pending_op_for_title_locked(op->title_id);
    operation_fallback_game(active ? active : pending, &fallback);
    if(append_game_summary(&b, &first, &fallback, active, pending) != 0) {
      failed = 1;
      break;
    }
  }
  pthread_mutex_unlock(&g_gc_lock);
  if(failed) {
    free(b.data);
    free(games);
    return -1;
  }
  if(json_append(&b, "]}") != 0) {
    free(b.data);
    free(games);
    return -1;
  }
  int rc = serve_owned(req, 200, b.data, b.len);
  free(games);
  return rc;
}

static int
size_priority_request(const http_request_t *req) {
  if(strcmp(req->method, "POST")) {
    return serve_error(req, 405, "method not allowed");
  }
  char path[1024] = {0};
  if(!websrv_get_query_arg(req, "path", path, sizeof(path)) &&
     !websrv_get_query_arg(req, "sourcePath", path, sizeof(path))) {
    return serve_error(req, 400, "path required");
  }
  if(!path_is_safe(path)) {
    return serve_error(req, 400, "bad path");
  }

  gc_game_t *games = calloc(GC_MAX_GAMES, sizeof(*games));
  size_t count = 0;
  int known = 0;
  if(!games) return serve_error(req, 500, "out of memory");
  discover_games(games, GC_MAX_GAMES, &count, 0);
  for(size_t i = 0; i < count; i++) {
    if(games[i].source_kind == GC_SOURCE_FOLDER &&
       game_source_matches(&games[i], path)) {
      snprintf(path, sizeof(path), "%s", games[i].source_path);
      known = 1;
      break;
    }
  }
  free(games);
  if(!known) {
    return serve_error(req, 404, "folder game not found");
  }

  gc_size_status_t status = gc_size_cache_priority(path);
  uint64_t size = 0;
  time_t measured_at = 0;
  gc_size_status_t refreshed_status = status;
  int has_size = gc_size_cache_lookup(path, &size, &measured_at,
                                   &refreshed_status) == 0;
  if(refreshed_status != GC_SIZE_STATUS_UNKNOWN) status = refreshed_status;

  json_buf_t b = {0};
  if(json_append(&b, "{\"ok\":true,\"path\":") != 0 ||
     json_string(&b, path) != 0 ||
     json_appendf(&b,
                  ",\"sourceSize\":%llu,"
                  "\"sizeStatus\":\"%s\","
                  "\"sizeEstimated\":%s,"
                  "\"sizeMeasuredAt\":%llu,"
                  "\"sizeRefreshing\":%s}",
                  has_size ? (unsigned long long)size : 0ULL,
                  gc_size_status_name(status),
                  has_size ? "true" : "false",
                  (unsigned long long)(measured_at > 0 ? measured_at : 0),
                  status == GC_SIZE_STATUS_REFRESHING ? "true" : "false") != 0) {
    free(b.data);
    return -1;
  }
  return serve_owned(req, 200, b.data, b.len);
}

static int
usb_request(const http_request_t *req) {
  (void)req;
  gc_usb_target_t targets[GC_STORAGE_TARGET_COUNT];
  size_t count = 0;
  discover_usb_targets(targets, GC_STORAGE_TARGET_COUNT, &count);

  json_buf_t b = {0};
  if(json_append(&b, "{\"ok\":true,\"usb\":[") != 0) {
    free(b.data);
    return -1;
  }
  for(size_t i = 0; i < count; i++) {
    char target_root[1024];
    if(resolve_transfer_target_root(targets[i].root, targets[i].target_root,
                                    target_root, sizeof(target_root)) != 0) {
      snprintf(target_root, sizeof(target_root), "%s", targets[i].target_root);
    }
    if(i && json_append(&b, ",") != 0) {
      free(b.data);
      return -1;
    }
    if(json_append(&b, "{\"id\":") != 0 ||
       json_string(&b, targets[i].id) != 0 ||
       json_append(&b, ",\"name\":") != 0 ||
       json_string(&b, targets[i].name) != 0 ||
       json_append(&b, ",\"path\":") != 0 ||
       json_string(&b, targets[i].root) != 0 ||
       json_append(&b, ",\"targetRoot\":") != 0 ||
       json_string(&b, target_root) != 0 ||
       json_appendf(&b, ",\"totalBytes\":%llu,\"freeBytes\":%llu}",
                    (unsigned long long)targets[i].total_bytes,
                    (unsigned long long)targets[i].free_bytes) != 0) {
      free(b.data);
      return -1;
    }
  }
  if(json_append(&b, "]}") != 0) {
    free(b.data);
    return -1;
  }
  return serve_owned(req, 200, b.data, b.len);
}

static int
history_request(const http_request_t *req) {
  (void)req;
  json_buf_t b = {0};
  char memory_keys[GC_MAX_OPS][GC_HISTORY_KEY_SIZE];
  size_t memory_key_count = 0;
  int first = 1;
  if(json_append(&b, "{\"ok\":true,\"history\":[") != 0) {
    free(b.data);
    return -1;
  }
  pthread_mutex_lock(&g_gc_lock);
  uint64_t ceiling = UINT64_MAX;
  for(size_t i = 0; i < GC_MAX_OPS; i++) {
    if(g_ops[i].used && memory_key_count < GC_MAX_OPS &&
       history_operation_key(g_ops[i].id, (uint64_t)g_ops[i].created_at,
                             memory_keys[memory_key_count],
                             sizeof(memory_keys[0])) == 0) {
      memory_key_count++;
    }
  }
  for(size_t emitted = 0; emitted < GC_MAX_OPS; emitted++) {
    uint64_t best_seq = 0;
    int best = -1;
    for(size_t i = 0; i < GC_MAX_OPS; i++) {
      if(!g_ops[i].used) continue;
      if(g_ops[i].seq < ceiling && g_ops[i].seq > best_seq) {
        best_seq = g_ops[i].seq;
        best = (int)i;
      }
    }
    if(best < 0) break;
    if(!first && json_append(&b, ",") != 0) break;
    first = 0;
    if(append_op_summary(&b, &g_ops[best]) != 0) break;
    ceiling = best_seq;
  }
  pthread_mutex_unlock(&g_gc_lock);
  if(append_persisted_history(&b, &first, memory_keys,
                              memory_key_count) != 0) {
    free(b.data);
    return -1;
  }
  if(json_append(&b, "]}") != 0) {
    free(b.data);
    return -1;
  }
  return serve_owned(req, 200, b.data, b.len);
}

static long
job_positive_max(long a, long b) {
  if(a < 0) a = 0;
  if(b < 0) b = 0;
  return a > b ? a : b;
}

static long
job_speed_metric_bytes(const char *verb, const char *phase, long copied,
                       long compressed_output, long repair_read,
                       long repair_written, long repair_copy,
                       const char **source_out) {
  const char *source = "copied";
  long metric = copied > 0 ? copied : 0;
  const char *v = verb ? verb : "";
  const char *p = phase ? phase : "";

  if(!strcmp(v, "compress")) {
    if(copied > 0) {
      metric = copied;
      source = "compressed-input";
    } else if(compressed_output > 0) {
      metric = compressed_output;
      source = "compressed-output";
    } else {
      source = "compressed-input";
    }
  } else if(!strcmp(v, "validate-repair") ||
            !strcmp(v, "validate-only")) {
    if(!strcmp(p, "repairing") || !strcmp(p, "copying") ||
       !strcmp(p, "metadata")) {
      metric = job_positive_max(repair_copy, repair_written);
      source = repair_copy > 0 ? "repair-copy" : "repair-write";
      if(metric <= 0) {
        metric = copied > 0 ? copied : 0;
        source = "repair-progress";
      }
    } else {
      metric = copied > 0 ? copied : repair_read;
      source = "validated";
    }
  } else if(!strcmp(v, "uncompress")) {
    source = "unpacked-output";
  } else if(!strcmp(v, "extract-image")) {
    source = "extracted-output";
	  } else if(!strcmp(v, "read-speed-test")) {
	    source = "read-test";
	  } else if(!strcmp(v, "move-to-usb") ||
            !strcmp(v, "move-to-internal") ||
            !strcmp(v, "copy-to-usb") ||
            !strcmp(v, "copy-to-internal")) {
    source = "transferred";
  }

  if(metric <= 0) {
    long repair_metric = job_positive_max(repair_copy,
                         job_positive_max(repair_written, repair_read));
    if(repair_metric > 0) {
      metric = repair_metric;
      source = "repair-io";
    }
  }
  if(source_out) *source_out = source;
  return metric > 0 ? metric : 0;
}

static int
job_request(const http_request_t *req) {
  json_buf_t b = {0};
  int busy = atomic_load(&g_job.busy);
  long total = atomic_load(&g_job.total_bytes);
  long copied = atomic_load(&g_job.copied_bytes);
  long compressed_output = atomic_load(&g_job.compressed_output_bytes);
  long raw_blocks = atomic_load(&g_job.raw_blocks);
  long compressed_blocks = atomic_load(&g_job.compressed_blocks);
  long skipped_zlib_blocks = atomic_load(&g_job.skipped_zlib_blocks);
  long total_blocks = atomic_load(&g_job.total_blocks);
  long done_blocks = (long)atomic_load(&g_job.done_files);
  long phase_step = atomic_load(&g_job.phase_step);
  long phase_count = atomic_load(&g_job.phase_count);
  long bad_blocks_found = atomic_load(&g_job.bad_blocks_found);
  long repaired_blocks = atomic_load(&g_job.repaired_blocks);
  long hash_checked = atomic_load(&g_job.hash_checked_blocks);
  long hash_matched = atomic_load(&g_job.hash_matched_blocks);
  long hash_mismatched = atomic_load(&g_job.hash_mismatched_blocks);
	  long software_compared = atomic_load(&g_job.software_compared_blocks);
	  long writer_wait_us = atomic_load(&g_job.writer_wait_us);
	  long worker_wait_us = atomic_load(&g_job.worker_wait_us);
  long scan_bytes = atomic_load(&g_job.scan_bytes);
  long scan_files = atomic_load(&g_job.scan_files);
  long scan_dirs = atomic_load(&g_job.scan_dirs);
  long scan_entries = atomic_load(&g_job.scan_entries);
  long scan_elapsed_ms = atomic_load(&g_job.scan_elapsed_ms);
  long scan_workers = atomic_load(&g_job.scan_workers);
	  long repair_read_bytes = atomic_load(&g_job.repair_read_bytes);
  long repair_written_bytes = atomic_load(&g_job.repair_written_bytes);
  long repair_copy_bytes = atomic_load(&g_job.repair_copy_bytes);
  long stream_min_free = atomic_load(&g_job.stream_min_free_bytes);
  long stream_budget = atomic_load(&g_job.stream_budget_bytes);
  long stream_credit = atomic_load(&g_job.stream_current_credit_bytes);
  long stream_deleted = atomic_load(&g_job.stream_deleted_bytes);
  long stream_reverse_temp = atomic_load(&g_job.stream_reverse_temp_bytes);
  long stream_forward_files = atomic_load(&g_job.stream_forward_files);
  long stream_reverse_files = atomic_load(&g_job.stream_reverse_files);
  int destructive_stream_active =
      atomic_load(&g_job.destructive_stream_active) != 0;
  int cancel_disabled = atomic_load(&g_job.cancel_disabled) != 0;
  int cancel_requested = atomic_load(&g_job.cancel) != 0;
  int rollback_requested = atomic_load(&g_job.rollback_requested) != 0;
  char current[512], phase[32], verb[16], err[256], active_id[32] = {0};
  char cancel_disabled_reason[128] = {0};
  time_t started_at = 0;
  time_t phase_started_at = 0;
  long elapsed_seconds = 0;
  long phase_elapsed_seconds = 0;
  long speed_metric_bytes = 0;
  long speed_bytes_per_second = 0;
  const char *speed_source = "none";

  pthread_mutex_lock(&g_job.lock);
  snprintf(current, sizeof(current), "%s", g_job.current);
  snprintf(phase, sizeof(phase), "%s", g_job.phase);
  snprintf(verb, sizeof(verb), "%s", g_job.verb);
  snprintf(err, sizeof(err), "%s", g_job.error);
  snprintf(cancel_disabled_reason, sizeof(cancel_disabled_reason), "%s",
           g_job.cancel_disabled_reason);
  started_at = g_job.started_at;
  phase_started_at = g_job.phase_started_at;
  pthread_mutex_unlock(&g_job.lock);

  if(busy && started_at > 0) {
    time_t now = time(NULL);
    if(now > started_at) elapsed_seconds = (long)(now - started_at);
    if(phase_started_at > 0 && now > phase_started_at) {
      phase_elapsed_seconds = (long)(now - phase_started_at);
    }
  }
  pthread_mutex_lock(&g_gc_lock);
  gc_operation_t *active = active_op_locked();
  if(active) {
    snprintf(active_id, sizeof(active_id), "%s", active->id);
    if(active->action == GC_ACTION_COMPRESS &&
       !strcmp(active->delete_policy, "stream") &&
       destructive_stream_active) {
      cancel_disabled = 1;
      if(!cancel_disabled_reason[0]) {
        snprintf(cancel_disabled_reason, sizeof(cancel_disabled_reason), "%s",
                 "unsafe compression cannot be cancelled after it starts");
      }
    }
  }
  pthread_mutex_unlock(&g_gc_lock);

  speed_metric_bytes = job_speed_metric_bytes(
      verb, phase, copied, compressed_output, repair_read_bytes,
      repair_written_bytes, repair_copy_bytes, &speed_source);
  if(elapsed_seconds > 0 && speed_metric_bytes > 0) {
    speed_bytes_per_second = speed_metric_bytes / elapsed_seconds;
  }
  if(!busy) {
    total = 0;
    copied = 0;
    compressed_output = 0;
    raw_blocks = 0;
    compressed_blocks = 0;
    skipped_zlib_blocks = 0;
    total_blocks = 0;
    done_blocks = 0;
    phase_step = 0;
    phase_count = 0;
    bad_blocks_found = 0;
    repaired_blocks = 0;
    hash_checked = 0;
    hash_matched = 0;
    hash_mismatched = 0;
	    software_compared = 0;
	    writer_wait_us = 0;
	    worker_wait_us = 0;
    scan_bytes = 0;
    scan_files = 0;
    scan_dirs = 0;
    scan_entries = 0;
    scan_elapsed_ms = 0;
    scan_workers = 0;
	    repair_read_bytes = 0;
    repair_written_bytes = 0;
    repair_copy_bytes = 0;
    stream_min_free = 0;
    stream_budget = 0;
    stream_credit = 0;
    stream_deleted = 0;
    stream_reverse_temp = 0;
    stream_forward_files = 0;
    stream_reverse_files = 0;
    destructive_stream_active = 0;
    cancel_disabled = 0;
    cancel_requested = 0;
    rollback_requested = 0;
    current[0] = 0;
    phase[0] = 0;
    verb[0] = 0;
    err[0] = 0;
    cancel_disabled_reason[0] = 0;
    active_id[0] = 0;
    started_at = 0;
    phase_started_at = 0;
    elapsed_seconds = 0;
    phase_elapsed_seconds = 0;
    speed_metric_bytes = 0;
    speed_bytes_per_second = 0;
    speed_source = "none";
  }

  if(json_append(&b, "{\"ok\":true,\"busy\":") != 0 ||
     json_append(&b, busy ? "true" : "false") != 0 ||
     json_append(&b, ",\"activeId\":") != 0 ||
     json_string(&b, active_id) != 0 ||
     json_append(&b, ",\"verb\":") != 0 ||
     json_string(&b, verb) != 0 ||
     json_append(&b, ",\"phase\":") != 0 ||
     json_string(&b, phase) != 0 ||
     json_append(&b, ",\"current\":") != 0 ||
     json_string(&b, current) != 0 ||
     json_append(&b, ",\"error\":") != 0 ||
     json_string(&b, err) != 0 ||
     json_appendf(&b,
	                  ",\"totalBytes\":%ld,\"copiedBytes\":%ld,"
	                  "\"compressedOutputBytes\":%ld,"
	                  "\"totalBlocks\":%ld,\"doneBlocks\":%ld,"
	                  "\"rawBlocks\":%ld,\"compressedBlocks\":%ld,"
	                  "\"skippedZlibBlocks\":%ld,"
	                  "\"phaseStep\":%ld,\"phaseCount\":%ld,"
                  "\"repairBlocks\":%ld,"
                  "\"badBlocksFound\":%ld,"
                  "\"repairedBlocks\":%ld,"
                  "\"hashCheckedBlocks\":%ld,"
                  "\"hashMatchedBlocks\":%ld,"
                  "\"hashMismatchedBlocks\":%ld,"
	                  "\"softwareComparedBlocks\":%ld,"
	                  "\"writerWaitUs\":%ld,"
	                  "\"workerWaitUs\":%ld,"
                    "\"scanBytes\":%ld,"
                    "\"scanFiles\":%ld,"
                    "\"scanDirs\":%ld,"
                    "\"scanEntries\":%ld,"
                    "\"scanElapsedMs\":%ld,"
                    "\"scanWorkers\":%ld,"
		                  "\"repairReadBytes\":%ld,"
	                  "\"repairWrittenBytes\":%ld,"
	                  "\"repairCopyBytes\":%ld,"
	                  "\"streamMinFreeBytes\":%ld,"
	                  "\"streamBudgetBytes\":%ld,"
	                  "\"streamCurrentCreditBytes\":%ld,"
	                  "\"streamDeletedBytes\":%ld,"
	                  "\"streamReverseTempBytes\":%ld,"
	                  "\"streamForwardFiles\":%ld,"
	                  "\"streamReverseFiles\":%ld,"
		                  "\"destructiveStreamActive\":%s,"
		                  "\"cancelDisabled\":%s,"
		                  "\"cancelDisabledReason\":",
		                  total, copied, compressed_output,
		                  total_blocks, done_blocks, raw_blocks,
                  compressed_blocks, skipped_zlib_blocks,
                  phase_step, phase_count,
                  bad_blocks_found, bad_blocks_found, repaired_blocks,
                  hash_checked, hash_matched, hash_mismatched,
                  software_compared,
		                  writer_wait_us, worker_wait_us,
                  scan_bytes, scan_files, scan_dirs, scan_entries,
                  scan_elapsed_ms, scan_workers,
                  repair_read_bytes,
	                  repair_written_bytes, repair_copy_bytes,
	                  stream_min_free, stream_budget, stream_credit,
	                  stream_deleted, stream_reverse_temp,
	                  stream_forward_files, stream_reverse_files,
	                  destructive_stream_active ? "true" : "false",
	                  cancel_disabled ? "true" : "false") != 0 ||
     json_string(&b, cancel_disabled_reason) != 0 ||
     json_appendf(&b,
		                  ",\"cancelRequested\":%s,"
			                  "\"rollbackRequested\":%s,"
			                  "\"startedAt\":%ld,"
			                  "\"elapsedSeconds\":%ld,"
			                  "\"phaseElapsedSeconds\":%ld,"
			                  "\"speedMetricBytes\":%ld,"
			                  "\"speedBytesPerSec\":%ld,"
		                  "\"compressBytesPerSecond\":%ld,"
		                  "\"speedSource\":",
		                  cancel_requested ? "true" : "false",
			                  rollback_requested ? "true" : "false",
			                  (long)started_at, elapsed_seconds,
			                  phase_elapsed_seconds,
			                  speed_metric_bytes, speed_bytes_per_second,
			                  speed_bytes_per_second) != 0 ||
     json_string(&b, speed_source) != 0 ||
     json_append(&b, "}") != 0) {
    free(b.data);
    return -1;
  }
  return serve_owned(req, 200, b.data, b.len);
}

int
gc_api_handoff_state_request(const http_request_t *req) {
  (void)req;
  json_buf_t b = {0};
  int job_busy = atomic_load(&g_job.busy);
  int pending_count = 0;
  int busy = 0;
  int resumable = 0;
  const char *reason = "idle";
  char active_id[32] = {0};
  char action[32] = {0};
  char phase[32] = {0};
  char job_phase[32] = {0};
  char current[256] = {0};

  pthread_mutex_lock(&g_job.lock);
  snprintf(job_phase, sizeof(job_phase), "%s", g_job.phase);
  snprintf(current, sizeof(current), "%s", g_job.current);
  pthread_mutex_unlock(&g_job.lock);

  pthread_mutex_lock(&g_gc_lock);
  gc_operation_t *active = active_op_locked();
  for(size_t i = 0; i < GC_MAX_OPS; i++) {
    if(g_ops[i].used && g_ops[i].status == GC_OP_QUEUED) pending_count++;
  }
  if(active) {
    snprintf(active_id, sizeof(active_id), "%s", active->id);
    snprintf(action, sizeof(action), "%s", action_name(active->action));
    snprintf(phase, sizeof(phase), "%s",
             active->phase[0] ? active->phase : job_phase);
    busy = 1;
    reason = "active operation recovery is disabled";
  } else if(job_busy) {
    snprintf(action, sizeof(action), "%s", "unknown");
    snprintf(phase, sizeof(phase), "%s", job_phase);
    busy = 1;
    reason = "active job is not attached to a resumable operation";
  } else if(pending_count > 0) {
    busy = 1;
    reason = "pending operation recovery is disabled";
  }
  pthread_mutex_unlock(&g_gc_lock);

  if(json_append(&b, "{\"ok\":true,\"name\":\"Game Compressor\",") != 0 ||
     json_appendf(&b,
                  "\"pid\":%ld,\"busy\":%s,\"jobBusy\":%s,"
                  "\"pendingCount\":%d,\"resumable\":%s,"
                  "\"activeId\":",
                  (long)getpid(),
                  busy ? "true" : "false",
                  job_busy ? "true" : "false",
                  pending_count,
                  resumable ? "true" : "false") != 0 ||
     json_string(&b, active_id) != 0 ||
     json_append(&b, ",\"action\":") != 0 ||
     json_string(&b, action) != 0 ||
     json_append(&b, ",\"phase\":") != 0 ||
     json_string(&b, phase) != 0 ||
     json_append(&b, ",\"jobPhase\":") != 0 ||
     json_string(&b, job_phase) != 0 ||
     json_append(&b, ",\"current\":") != 0 ||
     json_string(&b, current) != 0 ||
     json_append(&b, ",\"reason\":") != 0 ||
     json_string(&b, reason) != 0 ||
     json_append(&b, "}") != 0) {
    free(b.data);
    return -1;
  }
  return serve_owned(req, 200, b.data, b.len);
}

static int
cancel_active_request(const http_request_t *req) {
  int active_stream_compress = 0;
  int cancel_disabled = atomic_load(&g_job.cancel_disabled) != 0;
  int destructive_stream_active =
      atomic_load(&g_job.destructive_stream_active) != 0;
  char cancel_disabled_reason[128] = {0};
  if(cancel_disabled) {
    pthread_mutex_lock(&g_job.lock);
    snprintf(cancel_disabled_reason, sizeof(cancel_disabled_reason), "%s",
             g_job.cancel_disabled_reason[0] ? g_job.cancel_disabled_reason :
             "operation cannot be cancelled");
    pthread_mutex_unlock(&g_job.lock);
    return serve_error(req, 409, cancel_disabled_reason);
  }
  pthread_mutex_lock(&g_gc_lock);
  gc_operation_t *active = active_op_locked();
  if(active && active->action == GC_ACTION_COMPRESS &&
     !strcmp(active->delete_policy, "stream")) {
    active_stream_compress = 1;
  }
  pthread_mutex_unlock(&g_gc_lock);
  if(active_stream_compress && destructive_stream_active) {
    return serve_error(req, 409,
                       "unsafe compression cannot be cancelled after it starts");
  }
  atomic_store(&g_job.cancel, 1);
  return job_request(req);
}

static int
cancel_queued_request(const http_request_t *req) {
  char id[32];
  if(!websrv_get_query_arg(req, "id", id, sizeof(id))) {
    return serve_error(req, 400, "missing id");
  }
  pthread_mutex_lock(&g_gc_lock);
  gc_operation_t *op = find_op_locked(id);
  if(!op || op->status != GC_OP_QUEUED) {
    pthread_mutex_unlock(&g_gc_lock);
    return serve_error(req, 404, "pending action not found");
  }
  op->status = GC_OP_CANCELLED;
  op->ended_at = time(NULL);
  snprintf(op->phase, sizeof(op->phase), "%s", "complete");
  snprintf(op->result, sizeof(op->result), "%s", "cancelled");
  append_operation_logs(op);
  pthread_mutex_unlock(&g_gc_lock);
  const char body[] = "{\"ok\":true,\"status\":\"cancelled\"}";
  return websrv_send(req->fd, 200, "application/json", body, sizeof(body) - 1);
}

int
gc_api_icon_request(const http_request_t *req) {
  char title_id[64];
  if(!websrv_get_query_arg(req, "titleId", title_id, sizeof(title_id)) ||
     !valid_title_id(title_id)) {
    return websrv_send_error_json(req->fd, 400, "bad titleId");
  }
  char path[1024];
  snprintf(path, sizeof(path), "%s/%s/icon0.png", GC_APP_BASE, title_id);
  int fd = open(path, O_RDONLY);
  if(fd < 0) {
    snprintf(path, sizeof(path), "%s/%s/icon0.png", GC_APPMETA_BASE, title_id);
    fd = open(path, O_RDONLY);
  }
  if(fd < 0) return websrv_send_error_json(req->fd, 404, "icon not found");
  struct stat st;
  if(fstat(fd, &st) != 0 || st.st_size <= 0 || st.st_size > 2 * 1024 * 1024) {
    close(fd);
    return websrv_send_error_json(req->fd, 404, "icon not found");
  }

  char size_arg[32];
  int want_thumb =
      websrv_get_query_arg(req, "size", size_arg, sizeof(size_arg)) &&
      !strcasecmp(size_arg, "thumb");
  if(want_thumb) {
    char thumb_path[1024];
    if(gc_icon_thumb_path(title_id, path, &st, thumb_path,
                          sizeof(thumb_path)) == 0) {
      int thumb_fd = open(thumb_path, O_RDONLY);
      if(thumb_fd >= 0) {
        struct stat thumb_st;
        if(fstat(thumb_fd, &thumb_st) == 0 && thumb_st.st_size > 0 &&
           thumb_st.st_size <= 2 * 1024 * 1024) {
          close(fd);
          fd = thumb_fd;
          st = thumb_st;
        } else {
          close(thumb_fd);
        }
      }
    }
  }

  char *data = malloc((size_t)st.st_size);
  if(!data) {
    close(fd);
    return websrv_send_error_json(req->fd, 500, "out of memory");
  }
  size_t got = 0;
  while(got < (size_t)st.st_size) {
    ssize_t n = read(fd, data + got, (size_t)st.st_size - got);
    if(n < 0) {
      if(errno == EINTR) continue;
      free(data);
      close(fd);
      return websrv_send_error_json(req->fd, 500, "read icon failed");
    }
    if(n == 0) break;
    got += (size_t)n;
  }
  close(fd);
  int rc = websrv_send(req->fd, 200, "image/png", data, got);
  free(data);
  return rc;
}

void
gc_api_recover_on_startup(void) {
  cleanup_force_remount_temps_on_startup();
  cleanup_delete_pending_temps_on_startup();
  mount_switch_restore_recovery_log();
}

int
gc_api_request(const http_request_t *req, const char *url) {
  (void)url;
  if(!strcmp(req->path, "/api/gc/games")) return games_request(req);
  if(!strcmp(req->path, "/api/gc/size-priority")) {
    return size_priority_request(req);
  }
  if(!strcmp(req->path, "/api/gc/usb")) return usb_request(req);
  if(!strcmp(req->path, "/api/gc/uncompress-plan")) {
    return uncompress_plan_request(req);
  }
  if(!strcmp(req->path, "/api/gc/history")) return history_request(req);
  if(!strcmp(req->path, "/api/gc/ui-settings")) return ui_settings_request(req);
  if(!strcmp(req->path, "/api/gc/ampr/versions")) return ampr_versions_request(req);
  if(!strcmp(req->path, "/api/gc/ampr/upload")) return ampr_upload_request(req);
  if(!strcmp(req->path, "/api/gc/job")) return job_request(req);
  if(!strcmp(req->path, "/api/gc/bad-blocks")) return bad_blocks_request(req);
  if(!strcmp(req->path, "/api/gc/job/cancel")) return cancel_active_request(req);
  if(!strcmp(req->path, "/api/gc/queue/cancel")) return cancel_queued_request(req);
  if(!strcmp(req->path, "/api/gc/compress")) {
    return enqueue_action(req, GC_ACTION_COMPRESS);
  }
  if(!strcmp(req->path, "/api/gc/make-image")) {
    return enqueue_action(req, GC_ACTION_MAKE_IMAGE);
  }
  if(!strcmp(req->path, "/api/gc/uncompress")) {
    return enqueue_action(req, GC_ACTION_UNCOMPRESS);
  }
  if(!strcmp(req->path, "/api/gc/extract-image")) {
    return enqueue_extract_image_action(req);
  }
  if(!strcmp(req->path, "/api/gc/set-read-only")) {
    return enqueue_set_read_only_action(req);
  }
  if(!strcmp(req->path, "/api/gc/validate-repair")) {
    return enqueue_action(req, GC_ACTION_VALIDATE_REPAIR);
  }
  if(!strcmp(req->path, "/api/gc/validate-only")) {
    return enqueue_action(req, GC_ACTION_VALIDATE_ONLY);
  }
  if(!strcmp(req->path, "/api/gc/refresh-mount")) {
    return refresh_mount_request(req);
  }
  if(!strcmp(req->path, "/api/gc/move-to-usb")) {
    return enqueue_move_action(req, GC_ACTION_MOVE_TO_USB);
  }
  if(!strcmp(req->path, "/api/gc/move-to-internal")) {
    return enqueue_move_action(req, GC_ACTION_MOVE_TO_INTERNAL);
  }
  if(!strcmp(req->path, "/api/gc/copy-to-usb")) {
    return enqueue_move_action(req, GC_ACTION_COPY_TO_USB);
  }
  if(!strcmp(req->path, "/api/gc/copy-to-internal")) {
    return enqueue_move_action(req, GC_ACTION_COPY_TO_INTERNAL);
  }
  if(!strcmp(req->path, "/api/gc/delete-game-data")) {
    return enqueue_delete_game_data_action(req);
  }
	  if(!strcmp(req->path, "/api/gc/read-speed-test")) {
	    return enqueue_read_speed_test_action(req);
	  }
	  if(!strcmp(req->path, "/api/gc/build-ampr-index")) {
	    return enqueue_build_ampr_index_action(req);
	  }
	  if(!strcmp(req->path, "/api/gc/update-ampr")) {
	    return enqueue_update_ampr_action(req);
	  }
	  if(!strcmp(req->path, "/api/gc/restore-ampr-original")) {
	    return enqueue_restore_ampr_original_action(req);
	  }
	  return serve_error(req, 404, "not found");
	}
