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
#include "gc_notify.h"
#include "gc_shadowmount.h"
#include "pfs_compress.h"
#include "pfs_repair.h"
#include "transfer_internal.h"

#define GC_APP_BASE "/user/app"
#define GC_APPMETA_BASE "/user/appmeta"
#define GC_BASE "/data/GameCompressor"
#define GC_LOG_DIR GC_BASE "/logs"
#define GC_VALIDATION_DIR GC_BASE "/validations"
#define GC_HISTORY_DIR GC_BASE "/history"
#define GC_HISTORY_LOG GC_HISTORY_DIR "/history.jsonl"
#define GC_REPAIR_DIR GC_LOG_DIR "/repair"
#define GC_SIZE_CACHE_LOG GC_BASE "/size-cache.jsonl"
#define GC_HISTORY_KEY_SIZE 96
#define GC_MAX_GAMES 128
#define GC_MAX_OPS 64
#define GC_SIZE_CACHE_MAX 256
#define GC_SIZE_QUEUE_MAX 128
#define GC_CLOSE_WAIT_SECONDS 20
#define GC_REPAIR_WAIT_SECONDS 180
#define GC_REPAIR_WAIT_STEP_SECONDS 2
#define GC_REMOUNT_WAIT_SECONDS 30
#define GC_REMOUNT_WAIT_STEP_SECONDS 1
#define GC_MOUNT_SCAN_REQUEST_SECONDS 2
#define GC_SHADOWMOUNT_RESTART_WAIT_SECONDS 4
#define GC_FORCE_REMOUNT_PREFIX ".__gc_remount_"
#define GC_WORKER_THREAD_STACK_SIZE (1024 * 1024)
#define GC_SHADOW_PFSC_BASE "/mnt/shadowmnt/pfsc"
#define GC_SHADOW_IMAGE_BASE "/mnt/shadowmnt"
#define GC_INTERNAL_GAME_ROOT "/data/homebrew"
#define GC_USB_COUNT 8
#define GC_COPY_CHUNK_SIZE (1024 * 1024)
#define GC_GIB (1024ULL * 1024ULL * 1024ULL)
#define GC_STREAM_MIN_FREE_BYTES (1ULL * GC_GIB)

typedef enum gc_source_kind {
  GC_SOURCE_UNKNOWN = 0,
  GC_SOURCE_FOLDER,
  GC_SOURCE_IMAGE,
  GC_SOURCE_COMPRESSED,
} gc_source_kind_t;

typedef enum gc_action {
  GC_ACTION_COMPRESS = 1,
  GC_ACTION_UNCOMPRESS,
  GC_ACTION_VALIDATE_REPAIR,
  GC_ACTION_VALIDATE_ONLY,
  GC_ACTION_MOVE_TO_USB,
  GC_ACTION_MOVE_TO_INTERNAL,
  GC_ACTION_REFRESH_MOUNT,
} gc_action_t;

typedef enum gc_op_status {
  GC_OP_UNUSED = 0,
  GC_OP_QUEUED,
  GC_OP_RUNNING,
  GC_OP_SUCCESS,
  GC_OP_FAILED,
  GC_OP_CANCELLED,
} gc_op_status_t;

typedef enum gc_validation_state {
  GC_VALIDATION_NONE = 0,
  GC_VALIDATION_VALIDATED,
  GC_VALIDATION_CHANGED,
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
  int can_stream_delete;
  int has_icon;
  uint64_t icon_size;
  uint64_t icon_mtime;
  int output_exists;
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
  char compression_profile[16];
  char stream_order[24];
  uint64_t stream_budget_bytes;
  char target_root[1024];
  char phase[32];
  char result[32];
  char error[256];
  char repair_summary[1024];
  uint64_t repaired_blocks;
  uint64_t bad_blocks_found;
  uint64_t hash_checked_blocks;
  uint64_t hash_mismatched_blocks;
  uint64_t software_compared_blocks;
  time_t created_at;
  time_t started_at;
  time_t ended_at;
  int cancel_requested;
  int recovery_direct;
} gc_operation_t;

typedef struct gc_usb_target {
  char id[16];
  char name[64];
  char root[64];
  char target_root[96];
  uint64_t total_bytes;
  uint64_t free_bytes;
} gc_usb_target_t;

typedef struct gc_size_cache_entry {
  int used;
  int measuring;
  char path[1024];
  uint64_t size;
  time_t measured_at;
} gc_size_cache_entry_t;

typedef struct gc_history_recovery_event {
  char id[64];
  char status[32];
  char action[32];
  char phase[32];
  char result[32];
  char state[32];
  char title_id[64];
  char display_name[256];
  char source_path[1024];
  char output_path[1024];
  char source_kind[32];
  char format[16];
  char delete_policy[16];
  char compression_profile[16];
  char stream_order[24];
  uint64_t stream_budget_bytes;
  char target_root[1024];
  char repair_summary[1024];
  time_t created_at;
  time_t started_at;
  time_t ended_at;
} gc_history_recovery_event_t;

static pthread_mutex_t g_gc_lock = PTHREAD_MUTEX_INITIALIZER;
static gc_operation_t g_ops[GC_MAX_OPS];
static uint64_t g_next_seq = 1;
static int g_worker_running = 0;
static pthread_mutex_t g_size_cache_lock = PTHREAD_MUTEX_INITIALIZER;
static gc_size_cache_entry_t g_size_cache[GC_SIZE_CACHE_MAX];
static char g_size_queue[GC_SIZE_QUEUE_MAX][1024];
static size_t g_size_queue_count = 0;
static int g_size_cache_loaded = 0;
static int g_size_worker_running = 0;

extern int sceSystemServiceGetAppIdOfRunningBigApp(void);
extern int sceSystemServiceGetAppTitleId(int app_id, char *title_id);
extern uint32_t sceLncUtilKillApp(uint32_t app_id);

static int size_cache_get(const char *path, uint64_t *size_out);
static void size_cache_store(const char *path, uint64_t size);
static void size_cache_forget(const char *path);
static void size_cache_queue_measure(const char *path);

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

static const char *
action_name(gc_action_t action) {
  if(action == GC_ACTION_COMPRESS) return "compress";
  if(action == GC_ACTION_UNCOMPRESS) return "uncompress";
  if(action == GC_ACTION_VALIDATE_REPAIR) return "validate-repair";
  if(action == GC_ACTION_VALIDATE_ONLY) return "validate-only";
  if(action == GC_ACTION_MOVE_TO_USB) return "move-to-usb";
  if(action == GC_ACTION_MOVE_TO_INTERNAL) return "move-to-internal";
  if(action == GC_ACTION_REFRESH_MOUNT) return "refresh-mount";
  return "unknown";
}

static gc_action_t
action_from_name(const char *name) {
  if(!strcmp(name ? name : "", "compress")) return GC_ACTION_COMPRESS;
  if(!strcmp(name ? name : "", "uncompress")) return GC_ACTION_UNCOMPRESS;
  if(!strcmp(name ? name : "", "validate-repair")) {
    return GC_ACTION_VALIDATE_REPAIR;
  }
  if(!strcmp(name ? name : "", "validate-only")) return GC_ACTION_VALIDATE_ONLY;
  if(!strcmp(name ? name : "", "move-to-usb")) return GC_ACTION_MOVE_TO_USB;
  if(!strcmp(name ? name : "", "move-to-internal")) {
    return GC_ACTION_MOVE_TO_INTERNAL;
  }
  if(!strcmp(name ? name : "", "refresh-mount")) {
    return GC_ACTION_REFRESH_MOUNT;
  }
  return (gc_action_t)0;
}

static const char *
compression_profile_or_default(const char *profile) {
  if(!strcasecmp(profile ? profile : "", "fast")) return "fast";
  return "space";
}

static int
compression_profile_value(const char *profile) {
  if(!strcmp(compression_profile_or_default(profile), "fast")) {
    return PFS_COMPRESS_PROFILE_FAST;
  }
  return PFS_COMPRESS_PROFILE_SPACE;
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
operation_notification_status(gc_op_status_t status) {
  if(status == GC_OP_SUCCESS) return "success";
  if(status == GC_OP_CANCELLED) return "cancelled";
  return "failed";
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
free_bytes_for_output(const char *output_path, uint64_t *out) {
  char parent[1024];
  struct statvfs sv;
  if(path_parent(output_path, parent, sizeof(parent)) != 0) return -1;
  if(statvfs(parent, &sv) != 0) return -1;
  uint64_t block = sv.f_frsize ? (uint64_t)sv.f_frsize :
      (sv.f_bsize ? (uint64_t)sv.f_bsize : 1ULL);
  if(out) *out = saturating_mul((uint64_t)sv.f_bavail, block);
  return 0;
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

static const char *
storage_name_for_path(const char *path) {
  if(path_under_root(path, "/data")) return "internal";
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

static int
valid_usb_id(const char *id, int *index_out) {
  if(!id || strlen(id) != 4 || strncmp(id, "usb", 3)) return 0;
  if(id[3] < '0' || id[3] > '7') return 0;
  if(index_out) *index_out = id[3] - '0';
  return 1;
}

static int
usb_target_for_id(const char *id, gc_usb_target_t *out) {
  int idx = 0;
  char root[64];
  struct stat st;
  if(!valid_usb_id(id, &idx)) return -1;
  snprintf(root, sizeof(root), "/mnt/usb%d", idx);
  if(stat(root, &st) != 0 || !S_ISDIR(st.st_mode)) return -1;
  memset(out, 0, sizeof(*out));
  snprintf(out->id, sizeof(out->id), "usb%d", idx);
  snprintf(out->name, sizeof(out->name), "USB %d", idx);
  snprintf(out->root, sizeof(out->root), "%s", root);
  snprintf(out->target_root, sizeof(out->target_root), "%s/homebrew", root);
  if(space_for_path(root, &out->free_bytes, &out->total_bytes) != 0) {
    return -1;
  }
  return 0;
}

static int
discover_usb_targets(gc_usb_target_t *targets, size_t max, size_t *count_out) {
  size_t count = 0;
  for(int i = 0; i < GC_USB_COUNT && count < max; i++) {
    char id[16];
    snprintf(id, sizeof(id), "usb%d", i);
    if(usb_target_for_id(id, &targets[count]) == 0) count++;
  }
  if(count_out) *count_out = count;
  return 0;
}

static const char *const GC_SHADOW_SOURCE_ROOTS[] = {
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

static uint64_t
source_size_bytes_exact(const char *path, gc_source_kind_t kind) {
  struct stat st;
  if(kind == GC_SOURCE_FOLDER) {
    du_state_t du;
    du_walk(path, &du);
    return du.bytes;
  }
  if(stat(path, &st) == 0 && st.st_size > 0) return (uint64_t)st.st_size;
  return 0;
}

static void
populate_game_size(gc_game_t *g, int exact_folder_size) {
  if(!g) return;
  g->source_size = 0;
  g->required_bytes = 0;
  g->extra_needed = 0;
  g->size_pending = 0;

  if(g->source_kind == GC_SOURCE_FOLDER) {
    uint64_t cached_size = 0;
    if(exact_folder_size) {
      g->source_size = source_size_bytes_exact(g->source_path, g->source_kind);
      g->required_bytes = g->source_size;
      size_cache_store(g->source_path, g->source_size);
    } else if(size_cache_get(g->source_path, &cached_size) == 0) {
      g->source_size = cached_size;
      g->required_bytes = cached_size;
    } else {
      g->size_pending = 1;
      size_cache_queue_measure(g->source_path);
    }
  } else {
    g->source_size = source_size_bytes_exact(g->source_path, g->source_kind);
    g->required_bytes = g->source_size;
  }

  if(g->output_path[0] && free_bytes_for_output(g->output_path,
                                                &g->free_bytes) == 0 &&
     !g->size_pending) {
    g->extra_needed = g->free_bytes >= g->required_bytes
        ? 0
        : g->required_bytes - g->free_bytes;
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

static int
stream_delete_allowed_by_space(const gc_game_t *game) {
  return stream_delete_allowed_by_space_budget(game, 0);
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
  for(int i = 0; GC_SHADOW_SOURCE_ROOTS[i]; i++) {
    if(find_outer_pfsc_candidate(GC_SHADOW_SOURCE_ROOTS[i], title_id,
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
  if(!valid_title_id(title_id) || !out || out_size == 0) return -1;
  for(int i = 0; GC_SHADOW_SOURCE_ROOTS[i]; i++) {
    int n = snprintf(candidate, sizeof(candidate), "%s/%s.ffpfsc",
                     GC_SHADOW_SOURCE_ROOTS[i], title_id);
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

static int
force_remount_original_name_for_temp(const char *name,
                                     char *out, size_t out_size) {
  const char *marker;
  char title_id[64];
  size_t title_len;
  size_t prefix_len = strlen(GC_FORCE_REMOUNT_PREFIX);
  size_t name_len = strlen(name ? name : "");
  size_t suffix_len = strlen(".ffpfsc");
  int is_ffpfsc = ends_with_ci(name, ".ffpfsc");
  int is_app_dir = name_len > strlen("-app") &&
                   ends_with_ci(name, "-app");

  if(!name || !out || out_size == 0) {
    return -1;
  }
  marker = strstr(name, GC_FORCE_REMOUNT_PREFIX);
  if(!marker) return -1;
  title_len = (size_t)(marker - name);
  if(title_len != 9 || title_len >= sizeof(title_id)) return -1;
  if(name_len <= title_len + prefix_len +
                 (is_ffpfsc ? suffix_len : (is_app_dir ? strlen("-app") : 0))) {
    return -1;
  }
  memcpy(title_id, name, title_len);
  title_id[title_len] = 0;
  if(!valid_title_id(title_id)) return -1;
  if(snprintf(out, out_size, "%s%s", title_id,
              is_ffpfsc ? ".ffpfsc" : (is_app_dir ? "-app" : "")) >=
     (int)out_size) {
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

  for(int i = 0; GC_SHADOW_SOURCE_ROOTS[i]; i++) {
    restored += cleanup_force_remount_temps_in_root(GC_SHADOW_SOURCE_ROOTS[i]);
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
size_cache_find_locked(const char *path) {
  if(!path || !path[0]) return -1;
  for(size_t i = 0; i < GC_SIZE_CACHE_MAX; i++) {
    if(g_size_cache[i].used && !strcmp(g_size_cache[i].path, path)) {
      return (int)i;
    }
  }
  return -1;
}

static int
size_cache_slot_locked(void) {
  int oldest = -1;
  time_t oldest_time = 0;
  for(size_t i = 0; i < GC_SIZE_CACHE_MAX; i++) {
    if(!g_size_cache[i].used) return (int)i;
    if(g_size_cache[i].measuring) continue;
    if(oldest < 0 || g_size_cache[i].measured_at < oldest_time) {
      oldest = (int)i;
      oldest_time = g_size_cache[i].measured_at;
    }
  }
  return oldest >= 0 ? oldest : 0;
}

static void
size_cache_store_memory_locked(const char *path, uint64_t size,
                               time_t measured_at) {
  if(!path || !path[0]) return;
  int idx = size_cache_find_locked(path);
  if(idx < 0) idx = size_cache_slot_locked();
  memset(&g_size_cache[idx], 0, sizeof(g_size_cache[idx]));
  g_size_cache[idx].used = 1;
  snprintf(g_size_cache[idx].path, sizeof(g_size_cache[idx].path), "%s", path);
  g_size_cache[idx].size = size;
  g_size_cache[idx].measured_at = measured_at > 0 ? measured_at : time(NULL);
}

static void
size_cache_forget_memory_locked(const char *path) {
  int idx = size_cache_find_locked(path);
  if(idx >= 0) memset(&g_size_cache[idx], 0, sizeof(g_size_cache[idx]));
}

static void
size_cache_load_locked(void) {
  if(g_size_cache_loaded) return;
  g_size_cache_loaded = 1;

  FILE *f = fopen(GC_SIZE_CACHE_LOG, "r");
  if(!f) return;

  char line[2048];
  while(fgets(line, sizeof(line), f)) {
    char path[1024] = {0};
    struct stat st;
    line[strcspn(line, "\r\n")] = 0;
    if(!json_find_string_value(line, "path", path, sizeof(path)) ||
       !path_is_safe(path)) {
      continue;
    }
    uint64_t valid = json_find_u64_value(line, "valid", 1);
    if(!valid) {
      size_cache_forget_memory_locked(path);
      continue;
    }
    if(lstat(path, &st) != 0 || !S_ISDIR(st.st_mode)) {
      size_cache_forget_memory_locked(path);
      continue;
    }
    uint64_t size = json_find_u64_value(line, "size", 0);
    uint64_t measured_at = json_find_u64_value(line, "measuredAt", 0);
    size_cache_store_memory_locked(path, size, (time_t)measured_at);
  }
  fclose(f);
}

static void
size_cache_append_row(const char *path, uint64_t size, int valid,
                      time_t measured_at) {
  if(mkdirs(GC_BASE) != 0) return;
  int fd = open(GC_SIZE_CACHE_LOG, O_WRONLY | O_CREAT | O_APPEND, 0666);
  if(fd < 0) return;
  json_buf_t b = {0};
  if(json_append(&b, "{\"path\":") == 0 &&
     json_string(&b, path) == 0 &&
     json_appendf(&b, ",\"size\":%llu,\"valid\":%d,"
                  "\"measuredAt\":%ld}\n",
                  (unsigned long long)size,
                  valid ? 1 : 0,
                  (long)measured_at) == 0) {
    write_all_fd(fd, b.data, b.len);
    fsync(fd);
  }
  free(b.data);
  close(fd);
}

static int
size_cache_get(const char *path, uint64_t *size_out) {
  int ok = 0;
  pthread_mutex_lock(&g_size_cache_lock);
  size_cache_load_locked();
  int idx = size_cache_find_locked(path);
  if(idx >= 0 && !g_size_cache[idx].measuring &&
     g_size_cache[idx].measured_at > 0 &&
     g_size_cache[idx].size > 0) {
    if(size_out) *size_out = g_size_cache[idx].size;
    ok = 1;
  }
  pthread_mutex_unlock(&g_size_cache_lock);
  return ok ? 0 : -1;
}

static void
size_cache_store(const char *path, uint64_t size) {
  if(!path || !path_is_safe(path)) return;
  time_t now = time(NULL);
  pthread_mutex_lock(&g_size_cache_lock);
  size_cache_load_locked();
  size_cache_store_memory_locked(path, size, now);
  pthread_mutex_unlock(&g_size_cache_lock);
  size_cache_append_row(path, size, 1, now);
}

static void
size_cache_forget(const char *path) {
  if(!path || !path_is_safe(path)) return;
  time_t now = time(NULL);
  pthread_mutex_lock(&g_size_cache_lock);
  size_cache_load_locked();
  size_cache_forget_memory_locked(path);
  pthread_mutex_unlock(&g_size_cache_lock);
  size_cache_append_row(path, 0, 0, now);
}

static void *
size_cache_worker(void *arg) {
  (void)arg;
  while(1) {
    char path[1024];
    pthread_mutex_lock(&g_size_cache_lock);
    if(g_size_queue_count == 0) {
      g_size_worker_running = 0;
      pthread_mutex_unlock(&g_size_cache_lock);
      return NULL;
    }
    snprintf(path, sizeof(path), "%s", g_size_queue[0]);
    if(g_size_queue_count > 1) {
      memmove(&g_size_queue[0], &g_size_queue[1],
              (g_size_queue_count - 1) * sizeof(g_size_queue[0]));
    }
    g_size_queue_count--;
    pthread_mutex_unlock(&g_size_cache_lock);

    struct stat st;
    if(lstat(path, &st) != 0 || !S_ISDIR(st.st_mode)) {
      gc_log("size cache skipped missing folder path=%s", path);
      size_cache_forget(path);
      continue;
    }

    gc_log("size cache measuring path=%s", path);
    du_state_t du;
    du_walk(path, &du);
    size_cache_store(path, du.bytes);
    gc_log("size cache measured path=%s bytes=%llu entries=%llu",
           path, (unsigned long long)du.bytes,
           (unsigned long long)du.entries);
  }
}

static void
size_cache_queue_measure(const char *path) {
  if(!path || !path_is_safe(path)) return;
  int start_worker = 0;

  pthread_mutex_lock(&g_size_cache_lock);
  size_cache_load_locked();
  int idx = size_cache_find_locked(path);
  if(idx >= 0 && g_size_cache[idx].measured_at > 0 &&
     !g_size_cache[idx].measuring) {
    pthread_mutex_unlock(&g_size_cache_lock);
    return;
  }
  if(idx >= 0 && g_size_cache[idx].measuring) {
    pthread_mutex_unlock(&g_size_cache_lock);
    return;
  }
  for(size_t i = 0; i < g_size_queue_count; i++) {
    if(!strcmp(g_size_queue[i], path)) {
      pthread_mutex_unlock(&g_size_cache_lock);
      return;
    }
  }
  if(g_size_queue_count >= GC_SIZE_QUEUE_MAX) {
    gc_log("size cache queue full path=%s", path);
    pthread_mutex_unlock(&g_size_cache_lock);
    return;
  }
  if(idx < 0) idx = size_cache_slot_locked();
  memset(&g_size_cache[idx], 0, sizeof(g_size_cache[idx]));
  g_size_cache[idx].used = 1;
  g_size_cache[idx].measuring = 1;
  snprintf(g_size_cache[idx].path, sizeof(g_size_cache[idx].path), "%s", path);
  snprintf(g_size_queue[g_size_queue_count++], sizeof(g_size_queue[0]), "%s",
           path);
  if(!g_size_worker_running) {
    g_size_worker_running = 1;
    start_worker = 1;
  }
  pthread_mutex_unlock(&g_size_cache_lock);

  if(start_worker) {
    pthread_t t;
    pthread_attr_t at;
    pthread_attr_init(&at);
    pthread_attr_setdetachstate(&at, PTHREAD_CREATE_DETACHED);
    pthread_attr_setstacksize(&at, GC_WORKER_THREAD_STACK_SIZE);
    int rc = pthread_create(&t, &at, size_cache_worker, NULL);
    pthread_attr_destroy(&at);
    if(rc != 0) {
      pthread_mutex_lock(&g_size_cache_lock);
      g_size_worker_running = 0;
      pthread_mutex_unlock(&g_size_cache_lock);
      gc_log("size cache worker start failed rc=%d", rc);
    }
  }
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
  if(!json_find_string_value(json, "titleName", g->name, sizeof(g->name)) &&
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

static int
load_validation_state(gc_game_t *g) {
  if(g->source_kind != GC_SOURCE_COMPRESSED) {
    snprintf(g->validation_status, sizeof(g->validation_status), "%s", "n/a");
    g->validation = GC_VALIDATION_NONE;
    return 0;
  }

  char path[1024];
  char *json = NULL;
  size_t json_size = 0;
  struct stat st;
  marker_path_for_title(g->title_id, path, sizeof(path));
  if(read_file_limited(path, &json, &json_size, 64 * 1024) != 0) {
    snprintf(g->validation_status, sizeof(g->validation_status), "%s",
             "not-validated");
    g->validation = GC_VALIDATION_NONE;
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
  if(marker_matches_path) {
    g->compression_source_size =
        json_find_u64_value(json, "compressionSourceSize", 0);
    g->compressed_size =
        json_find_u64_value(json, "compressionCompressedSize", 0);
    g->saved_bytes =
        json_find_u64_value(json, "compressionSavedBytes", 0);
  }
  if(!marker_matches_path ||
     !strcmp(marker_status, "bad-blocks-found") ||
     !strcmp(marker_status, "bad-blocks-found-not-mounted")) {
    snprintf(g->validation_status, sizeof(g->validation_status), "%s",
             "not-validated");
    g->validation = GC_VALIDATION_NONE;
    (void)unlink(path);
  } else {
    snprintf(g->validation_status, sizeof(g->validation_status), "%s",
             "validated");
    g->validation = GC_VALIDATION_VALIDATED;
  }
  free(json);
  return 0;
}

static int
write_validation_marker_ex(const char *title_id, const char *image_path,
                           const pfs_repair_info_t *info, const char *result,
                           uint64_t compression_source_size) {
  if(mkdirs(GC_VALIDATION_DIR) != 0) return -1;
  char marker[1024];
  marker_path_for_title(title_id, marker, sizeof(marker));
  struct stat st;
  if(stat(image_path, &st) != 0) return -1;
  uint64_t compressed_size = st.st_size > 0 ? (uint64_t)st.st_size : 0;
  uint64_t saved_bytes = 0;

  if(compression_source_size > 0) {
    saved_bytes = compression_source_size > compressed_size
        ? compression_source_size - compressed_size
        : 0;
  } else {
    char *old_json = NULL;
    size_t old_json_size = 0;
    char old_path[1024] = {0};
    if(read_file_limited(marker, &old_json, &old_json_size, 64 * 1024) == 0) {
      (void)old_json_size;
      json_find_string_value(old_json, "path", old_path, sizeof(old_path));
      if(!strcmp(old_path, image_path)) {
        compression_source_size =
            json_find_u64_value(old_json, "compressionSourceSize", 0);
        if(compression_source_size > compressed_size) {
          saved_bytes = compression_source_size - compressed_size;
        }
      }
      free(old_json);
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
                   "\n  \"status\":",
                   (unsigned long long)(st.st_size > 0 ? st.st_size : 0),
                   (unsigned long long)st.st_mtime,
                   (unsigned long long)compression_source_size,
                   (unsigned long long)compressed_size,
                   (unsigned long long)saved_bytes) == 0 &&
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
  int fd = open(marker, O_WRONLY | O_CREAT | O_TRUNC, 0666);
  if(fd < 0) {
    free(b.data);
    return -1;
  }
  int rc = write_all_fd(fd, b.data, b.len);
  fsync(fd);
  close(fd);
  free(b.data);
  return rc;
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

  marker_path_for_title(title_id, marker, sizeof(marker));
  if(read_file_limited(marker, &json, &json_size, 64 * 1024) != 0) {
    return 0;
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
  int fd = open(marker, O_WRONLY | O_CREAT | O_TRUNC, 0666);
  if(fd < 0) {
    free(b.data);
    return -1;
  }
  int rc = write_all_fd(fd, b.data, b.len);
  fsync(fd);
  close(fd);
  free(b.data);
  return rc;
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

  snprintf(mountpoint, sizeof(mountpoint), "/system_ex/app/%s",
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
    if(now >= next_scan_at && now + GC_REMOUNT_WAIT_STEP_SECONDS < deadline) {
      scan_err[0] = 0;
      if(gc_shadowmount_request_scan(scan_err, sizeof(scan_err)) == 0) {
        next_scan_at = now + GC_MOUNT_SCAN_REQUEST_SECONDS;
      } else {
        gc_log("shadowmount scan request failed title=%s err=%s",
               title_id ? title_id : "", scan_err[0] ? scan_err : "unknown");
        next_scan_at = now + GC_MOUNT_SCAN_REQUEST_SECONDS;
      }
    }

    if(now >= deadline || job_cancelled()) {
      if(!job_cancelled() && !restart_recovery_attempted) {
        char restart_detail[512] = {0};
        restart_recovery_attempted = 1;
        job_set_phase("mounting", 0, 0, "Restarting ShadowMountPlus");
        if(gc_shadowmount_restart_running(restart_detail,
                                          sizeof(restart_detail)) == 0) {
          gc_log("shadowmount restart recovery title=%s detail=%s",
                 title_id ? title_id : "",
                 restart_detail[0] ? restart_detail : "started");
          sleep(GC_SHADOWMOUNT_RESTART_WAIT_SECONDS);
          scan_err[0] = 0;
          if(gc_shadowmount_request_scan(scan_err, sizeof(scan_err)) != 0) {
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
    sleep(GC_REMOUNT_WAIT_STEP_SECONDS);
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
    sleep(1);
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
detect_game_source(gc_game_t *g, int exact_folder_size) {
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
    g->output_exists = dec_info.output_exists;
  } else if(ends_with_ci(g->source_path, ".ffpfsc") &&
     pfs_decompress_probe(g->source_path, &dec_info, err,
                          sizeof(err)) == 0) {
    g->source_kind = GC_SOURCE_COMPRESSED;
    snprintf(g->output_path, sizeof(g->output_path), "%s",
             dec_info.output_path);
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

  populate_game_size(g, exact_folder_size);
  g->can_stream_delete =
      g->source_kind == GC_SOURCE_FOLDER ||
      g->source_kind == GC_SOURCE_COMPRESSED;
  load_validation_state(g);

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

static int
discover_games(gc_game_t *games, size_t max_games, size_t *count_out,
               int exact_folder_sizes) {
  DIR *d = opendir(GC_APP_BASE);
  size_t count = 0;
  struct dirent *ent;

  if(d) {
    while((ent = readdir(d)) && count < max_games) {
      if(!valid_title_id(ent->d_name)) continue;
      gc_game_t *g = &games[count];
      memset(g, 0, sizeof(*g));
      snprintf(g->title_id, sizeof(g->title_id), "%s", ent->d_name);

      char link_path[1024];
      snprintf(link_path, sizeof(link_path), "%s/%s/mount.lnk", GC_APP_BASE,
               g->title_id);
      if(read_link_file(link_path, g->mount_path, sizeof(g->mount_path)) != 0) {
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
      load_game_name(g);
      load_game_icon(g);
      detect_game_source(g, exact_folder_sizes);
      count++;
    }
    closedir(d);
  }

  for(int i = 0; GC_SHADOW_SOURCE_ROOTS[i] && count < max_games; i++) {
    DIR *root = opendir(GC_SHADOW_SOURCE_ROOTS[i]);
    if(!root) continue;
    while((ent = readdir(root)) && count < max_games) {
      char title_id[64];
      char source_path[1024];
      gc_game_t *g;
      struct stat st;
      int duplicate = 0;
      int expect_kind = GC_SOURCE_UNKNOWN;

      title_id[0] = 0;
      if(ends_with_ci(ent->d_name, ".ffpfsc")) {
        if(strip_extension_base(ent->d_name, title_id, sizeof(title_id)) != 0 ||
           !valid_title_id(title_id)) {
          continue;
        }
        expect_kind = GC_SOURCE_COMPRESSED;
      } else if(valid_title_id(ent->d_name)) {
        snprintf(title_id, sizeof(title_id), "%s", ent->d_name);
        expect_kind = GC_SOURCE_FOLDER;
      } else if(strlen(ent->d_name) == 13 &&
                !strcasecmp(ent->d_name + 9, "-app")) {
        memcpy(title_id, ent->d_name, 9);
        title_id[9] = 0;
        if(!valid_title_id(title_id)) continue;
        expect_kind = GC_SOURCE_FOLDER;
      } else {
        continue;
      }

      for(size_t j = 0; j < count; j++) {
        if(!strcmp(games[j].title_id, title_id)) {
          duplicate = 1;
          break;
        }
      }
      if(duplicate) continue;
      if(snprintf(source_path, sizeof(source_path), "%s/%s",
                  GC_SHADOW_SOURCE_ROOTS[i], ent->d_name) >=
         (int)sizeof(source_path)) {
        continue;
      }
      if(stat(source_path, &st) != 0) continue;
      if(expect_kind == GC_SOURCE_COMPRESSED && !S_ISREG(st.st_mode)) continue;
      if(expect_kind == GC_SOURCE_FOLDER && !S_ISDIR(st.st_mode)) continue;

      g = &games[count];
      memset(g, 0, sizeof(*g));
      snprintf(g->title_id, sizeof(g->title_id), "%s", title_id);
      if(expect_kind == GC_SOURCE_COMPRESSED) {
        snprintf(g->image_path, sizeof(g->image_path), "%s", source_path);
      } else {
        snprintf(g->mount_path, sizeof(g->mount_path), "%s", source_path);
      }
      load_game_name(g);
      load_game_icon(g);
      detect_game_source(g, exact_folder_sizes);
      if(g->source_kind != expect_kind) continue;
      count++;
    }
    closedir(root);
  }

  if(count_out) *count_out = count;
  return 0;
}

static int
find_game_by_title_ex(const char *title_id, gc_game_t *out,
                      int exact_folder_sizes) {
  gc_game_t games[GC_MAX_GAMES];
  size_t count = 0;
  discover_games(games, GC_MAX_GAMES, &count, exact_folder_sizes);
  for(size_t i = 0; i < count; i++) {
    if(!strcmp(games[i].title_id, title_id)) {
      if(out) *out = games[i];
      return 0;
    }
  }
  return -1;
}

static int
find_game_by_title(const char *title_id, gc_game_t *out) {
  return find_game_by_title_ex(title_id, out, 1);
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
  op->hash_mismatched_blocks = 0;
  op->software_compared_blocks = repair->software_compared_blocks;
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
append_history_log(const gc_operation_t *op) {
  if(mkdirs(GC_HISTORY_DIR) != 0) return;
  int fd = open(GC_HISTORY_LOG, O_WRONLY | O_CREAT | O_APPEND, 0666);
  if(fd < 0) return;
  json_buf_t b = {0};
  char row_id[64];
  uint64_t bad_blocks_found = op->bad_blocks_found;
  uint64_t hash_mismatched_blocks = 0;
  uint64_t software_compared_blocks = op->software_compared_blocks;
  if(op->status == GC_OP_SUCCESS && op->repaired_blocks > 0) {
    bad_blocks_found = 0;
    software_compared_blocks = 0;
  }
  if(op->status == GC_OP_RUNNING && op->phase[0]) {
    snprintf(row_id, sizeof(row_id), "%s:%s", op->id, op->phase);
  } else {
    snprintf(row_id, sizeof(row_id), "%s", op->id);
  }
  if(json_append(&b, "{\"id\":") == 0 &&
     json_string(&b, row_id) == 0 &&
     json_append(&b, ",\"titleId\":") == 0 &&
     json_string(&b, op->title_id) == 0 &&
     json_append(&b, ",\"displayName\":") == 0 &&
     json_string(&b, op->display_name) == 0 &&
     json_append(&b, ",\"action\":") == 0 &&
     json_string(&b, action_name(op->action)) == 0 &&
     json_append(&b, ",\"status\":") == 0 &&
     json_string(&b, status_name(op->status)) == 0 &&
     json_append(&b, ",\"phase\":") == 0 &&
     json_string(&b, op->phase) == 0 &&
     json_append(&b, ",\"result\":") == 0 &&
     json_string(&b, op->result) == 0 &&
     json_append(&b, ",\"error\":") == 0 &&
     json_string(&b, op->error) == 0 &&
     json_append(&b, ",\"sourcePath\":") == 0 &&
     json_string(&b, op->source_path) == 0 &&
     json_append(&b, ",\"outputPath\":") == 0 &&
     json_string(&b, op->output_path) == 0 &&
     json_append(&b, ",\"sourceKind\":") == 0 &&
     json_string(&b, op->source_kind) == 0 &&
     json_append(&b, ",\"format\":") == 0 &&
     json_string(&b, op->format) == 0 &&
     json_append(&b, ",\"deletePolicy\":") == 0 &&
     json_string(&b, op->delete_policy) == 0 &&
     json_append(&b, ",\"compressionProfile\":") == 0 &&
     json_string(&b, compression_profile_or_default(op->compression_profile)) == 0 &&
     json_append(&b, ",\"streamOrder\":") == 0 &&
     json_string(&b, op->stream_order[0] ? op->stream_order : "budgeted-gain") == 0 &&
     json_append(&b, ",\"targetRoot\":") == 0 &&
     json_string(&b, op->target_root) == 0 &&
     json_append(&b, ",\"repairSummary\":") == 0 &&
     json_string(&b, op->repair_summary) == 0 &&
	     json_appendf(&b,
	                  ",\"streamBudgetBytes\":%llu,"
	                  "\"createdAt\":%ld,\"startedAt\":%ld,\"endedAt\":%ld,"
	                  "\"repairedBlocks\":%llu,\"badBlocksFound\":%llu,"
	                  "\"hashCheckedBlocks\":%llu,"
	                  "\"hashMismatchedBlocks\":%llu,"
	                  "\"softwareComparedBlocks\":%llu}\n",
	                  (unsigned long long)op->stream_budget_bytes,
	                  (long)op->created_at, (long)op->started_at,
	                  (long)op->ended_at,
	                  (unsigned long long)op->repaired_blocks,
	                  (unsigned long long)bad_blocks_found,
	                  (unsigned long long)op->hash_checked_blocks,
	                  (unsigned long long)hash_mismatched_blocks,
	                  (unsigned long long)software_compared_blocks) == 0) {
    write_all_fd(fd, b.data, b.len);
    fsync(fd);
  }
  free(b.data);
  close(fd);
}

static int
history_phase_is_recoverable(const char *phase) {
  return !strcmp(phase ? phase : "", "compressing") ||
         !strcmp(phase ? phase : "", "compressed") ||
         !strcmp(phase ? phase : "", "source-deleted") ||
         !strcmp(phase ? phase : "", "validating") ||
         !strcmp(phase ? phase : "", "repairing");
}

static int
history_status_is_terminal(const char *status) {
  return !strcmp(status ? status : "", "success") ||
         !strcmp(status ? status : "", "failed") ||
         !strcmp(status ? status : "", "cancelled");
}

static int
history_status_is_pending(const char *status) {
  return !strcmp(status ? status : "", "pending") ||
         !strcmp(status ? status : "", "queued");
}

static uint64_t
operation_seq_from_id(const char *id) {
  const char *p = id ? id : "";
  if(strncmp(p, "op-", 3)) return 0;
  p += 3;
  if(!isdigit((unsigned char)*p)) return 0;
  uint64_t value = 0;
  while(isdigit((unsigned char)*p)) {
    unsigned digit = (unsigned)(*p - '0');
    if(value > (UINT64_MAX - digit) / 10ULL) return 0;
    value = value * 10ULL + digit;
    p++;
  }
  return value;
}

static int
history_action_phase_is_resumable(const char *action, const char *phase) {
  if(!strcmp(action ? action : "", "validate-repair") ||
     !strcmp(action ? action : "", "validate-only")) {
    return 1;
  }
  if(!strcmp(action ? action : "", "compress")) {
    return !strcmp(phase ? phase : "", "compressed") ||
           !strcmp(phase ? phase : "", "source-deleted") ||
           !strcmp(phase ? phase : "", "repairing");
  }
  return 0;
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
  append_history_log(op);
  pthread_mutex_unlock(&g_gc_lock);
}

static int
remove_tree_gc(const char *path) {
  struct stat st;
  if(lstat(path, &st) != 0) return errno == ENOENT ? 0 : -1;
  if(!S_ISDIR(st.st_mode)) return unlink(path);
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
  return rmdir(path);
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

static int
copy_regular_file_gc(const char *src, const char *dst,
                     const struct stat *st, uint64_t *copied,
                     char *err, size_t err_size) {
  int in = -1;
  int out = -1;
  char *buf = NULL;
  int rc = -1;

  in = open(src, O_RDONLY);
  if(in < 0) {
    snprintf(err, err_size, "open source: %s", strerror(errno));
    goto done;
  }
  out = open(dst, O_WRONLY | O_CREAT | O_EXCL, st->st_mode & 0777);
  if(out < 0) {
    snprintf(err, err_size, "create target: %s", strerror(errno));
    goto done;
  }
  buf = malloc(GC_COPY_CHUNK_SIZE);
  if(!buf) {
    snprintf(err, err_size, "%s", "out of memory");
    goto done;
  }

  while(1) {
    if(job_cancelled()) {
      snprintf(err, err_size, "%s", "cancelled");
      errno = EINTR;
      goto done;
    }
    ssize_t n = read(in, buf, GC_COPY_CHUNK_SIZE);
    if(n < 0) {
      if(errno == EINTR) continue;
      snprintf(err, err_size, "read source: %s", strerror(errno));
      goto done;
    }
    if(n == 0) break;
    if(write_all_fd(out, buf, (size_t)n) != 0) {
      snprintf(err, err_size, "write target: %s", strerror(errno));
      goto done;
    }
    if(copied) {
      *copied += (uint64_t)n;
      job_store_u64(&g_job.copied_bytes, *copied);
    }
  }
  fsync(out);
  if(close(out) != 0) {
    out = -1;
    snprintf(err, err_size, "close target: %s", strerror(errno));
    goto done;
  }
  out = -1;
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
  if(lstat(src, &st) != 0) {
    snprintf(err, err_size, "stat source: %s", strerror(errno));
    return -1;
  }
  job_set_current(path_basename(src));

  if(S_ISDIR(st.st_mode)) {
    if(mkdir(dst, st.st_mode & 0777) != 0) {
      snprintf(err, err_size, "create target folder: %s", strerror(errno));
      return -1;
    }
    DIR *d = opendir(src);
    if(!d) {
      snprintf(err, err_size, "open source folder: %s", strerror(errno));
      return -1;
    }
    struct dirent *ent;
    while((ent = readdir(d))) {
      if(!strcmp(ent->d_name, ".") || !strcmp(ent->d_name, "..")) continue;
      if(job_cancelled()) {
        closedir(d);
        snprintf(err, err_size, "%s", "cancelled");
        errno = EINTR;
        return -1;
      }
      char src_child[1024];
      char dst_child[1024];
      join_path(src_child, sizeof(src_child), src, ent->d_name);
      join_path(dst_child, sizeof(dst_child), dst, ent->d_name);
      if(copy_tree_gc(src_child, dst_child, copied, err, err_size) != 0) {
        closedir(d);
        return -1;
      }
    }
    closedir(d);
    preserve_mode_time(dst, &st);
    return 0;
  }

  if(S_ISREG(st.st_mode)) {
    return copy_regular_file_gc(src, dst, &st, copied, err, err_size);
  }

  if(S_ISLNK(st.st_mode)) {
    char target[1024];
    ssize_t n = readlink(src, target, sizeof(target) - 1);
    if(n < 0) {
      snprintf(err, err_size, "read symlink: %s", strerror(errno));
      return -1;
    }
    target[n] = 0;
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
  if(kind == GC_SOURCE_FOLDER) {
    if(remove_tree_gc(path) != 0) {
      snprintf(err, err_size, "remove source folder: %s", strerror(errno));
      return -1;
    }
    size_cache_forget(path);
    return 0;
  }
  if(unlink(path) != 0 && errno != ENOENT) {
    snprintf(err, err_size, "remove source image: %s", strerror(errno));
    return -1;
  }
  return 0;
}

static void
cleanup_failed_safe_compress_output(const char *path, const char *title_id) {
  if(!path || !path[0]) return;
  if(unlink(path) == 0 || errno == ENOENT) {
    gc_log("compress cleanup removed output title=%s path=%s",
           title_id ? title_id : "", path);
  } else {
    gc_log("compress cleanup could not remove output title=%s path=%s err=%s",
           title_id ? title_id : "", path, strerror(errno));
  }
}

static int
recovery_tmp_path_for_output(const char *output_path, char *out,
                             size_t out_size) {
  if(!output_path || !output_path[0]) return -1;
  int n = snprintf(out, out_size, "%s.tmp", output_path);
  return n < 0 || (size_t)n >= out_size ? -1 : 0;
}

static int
recovery_path_exists_for_kind(const char *path, const char *source_kind) {
  struct stat st;
  if(!path || !path[0] || lstat(path, &st) != 0) return 0;
  gc_source_kind_t kind = source_kind_from_name(source_kind);
  if(kind == GC_SOURCE_FOLDER) return S_ISDIR(st.st_mode);
  if(kind == GC_SOURCE_IMAGE || kind == GC_SOURCE_COMPRESSED) {
    return S_ISREG(st.st_mode);
  }
  return S_ISDIR(st.st_mode) || S_ISREG(st.st_mode);
}

static int
recovery_queued_event_valid(const gc_history_recovery_event_t *ev,
                            char *err, size_t err_size) {
  const char *source;
  struct stat st;
  if(!ev || !valid_title_id(ev->title_id)) {
    snprintf(err, err_size, "%s", "queued operation has invalid title");
    return 0;
  }
  if(!action_from_name(ev->action)) {
    snprintf(err, err_size, "%s", "queued operation has invalid action");
    return 0;
  }
  if(!strcmp(ev->action, "compress")) {
    if(!recovery_path_exists_for_kind(ev->source_path, ev->source_kind)) {
      snprintf(err, err_size, "%s", "queued source is missing");
      return 0;
    }
    if(ev->output_path[0] && lstat(ev->output_path, &st) == 0) {
      snprintf(err, err_size, "%s", "queued output already exists");
      return 0;
    }
    return 1;
  }
  if(!strcmp(ev->action, "uncompress") ||
     !strcmp(ev->action, "validate-repair") ||
     !strcmp(ev->action, "validate-only")) {
    source = ev->source_path[0] ? ev->source_path : ev->output_path;
    if(!source || !source[0] || stat(source, &st) != 0 ||
       !S_ISREG(st.st_mode) || !ends_with_ci(source, ".ffpfsc")) {
      snprintf(err, err_size, "%s", "queued compressed image is missing");
      return 0;
    }
    return 1;
  }
  if(!strcmp(ev->action, "move-to-usb") ||
     !strcmp(ev->action, "move-to-internal")) {
    if(!recovery_path_exists_for_kind(ev->source_path, ev->source_kind)) {
      snprintf(err, err_size, "%s", "queued move source is missing");
      return 0;
    }
    if(!ev->output_path[0]) {
      snprintf(err, err_size, "%s", "queued move target is missing");
      return 0;
    }
    if(lstat(ev->output_path, &st) == 0) {
      snprintf(err, err_size, "%s", "queued move target already exists");
      return 0;
    }
    return 1;
  }
  snprintf(err, err_size, "%s", "queued operation is unsupported");
  return 0;
}

static void
history_base_id(const char *id, char *out, size_t out_size) {
  const char *colon = strchr(id ? id : "", ':');
  size_t len = colon ? (size_t)(colon - id) : strlen(id ? id : "");
  if(!out || out_size == 0) return;
  if(len == 0) {
    snprintf(out, out_size, "%s", "recovery");
    return;
  }
  if(len >= out_size) len = out_size - 1;
  memmove(out, id, len);
  out[len] = 0;
}

static int
recovery_event_from_history_line(const char *line,
                                 gc_history_recovery_event_t *out) {
  if(!line || !out) return -1;
  memset(out, 0, sizeof(*out));
  json_find_string_value(line, "id", out->id, sizeof(out->id));
  history_base_id(out->id, out->id, sizeof(out->id));
  json_find_string_value(line, "titleId", out->title_id,
                         sizeof(out->title_id));
  json_find_string_value(line, "displayName", out->display_name,
                         sizeof(out->display_name));
  json_find_string_value(line, "action", out->action, sizeof(out->action));
  json_find_string_value(line, "status", out->status, sizeof(out->status));
  json_find_string_value(line, "phase", out->phase, sizeof(out->phase));
  json_find_string_value(line, "result", out->result, sizeof(out->result));
  if(!out->phase[0] && history_phase_is_recoverable(out->result)) {
    snprintf(out->phase, sizeof(out->phase), "%s", out->result);
  }
  json_find_string_value(line, "sourcePath", out->source_path,
                         sizeof(out->source_path));
  json_find_string_value(line, "outputPath", out->output_path,
                         sizeof(out->output_path));
  json_find_string_value(line, "sourceKind", out->source_kind,
                         sizeof(out->source_kind));
  json_find_string_value(line, "format", out->format, sizeof(out->format));
  json_find_string_value(line, "deletePolicy", out->delete_policy,
                         sizeof(out->delete_policy));
  json_find_string_value(line, "compressionProfile", out->compression_profile,
                         sizeof(out->compression_profile));
  json_find_string_value(line, "streamOrder", out->stream_order,
                         sizeof(out->stream_order));
  out->stream_budget_bytes =
      json_find_u64_value(line, "streamBudgetBytes", 0);
  json_find_string_value(line, "targetRoot", out->target_root,
                         sizeof(out->target_root));
  json_find_string_value(line, "repairSummary", out->repair_summary,
                         sizeof(out->repair_summary));
  out->created_at = (time_t)json_find_u64_value(line, "createdAt", 0);
  out->started_at = (time_t)json_find_u64_value(line, "startedAt", 0);
  out->ended_at = (time_t)json_find_u64_value(line, "endedAt", 0);
  if(!valid_title_id(out->title_id) || !out->action[0] ||
     !out->status[0]) {
    return -1;
  }
  if(!out->display_name[0]) {
    snprintf(out->display_name, sizeof(out->display_name), "%s",
             out->title_id);
  }
  return 0;
}

static void
recovery_append_terminal_history(const gc_history_recovery_event_t *ev,
                                 gc_op_status_t status,
                                 const char *result,
                                 const char *error) {
  gc_operation_t op;
  memset(&op, 0, sizeof(op));
  op.used = 1;
  pthread_mutex_lock(&g_gc_lock);
  op.seq = g_next_seq++;
  if(ev && ev->id[0]) {
    snprintf(op.id, sizeof(op.id), "%s", ev->id);
  } else {
    snprintf(op.id, sizeof(op.id), "recovery-%llu",
             (unsigned long long)op.seq);
  }
  op.action = ev ? action_from_name(ev->action) : GC_ACTION_VALIDATE_REPAIR;
  if(!op.action) op.action = GC_ACTION_VALIDATE_REPAIR;
  op.status = status;
  snprintf(op.phase, sizeof(op.phase), "%s", "complete");
  op.created_at = ev && ev->created_at ? ev->created_at : time(NULL);
  op.started_at = ev && ev->started_at ? ev->started_at : op.created_at;
  op.ended_at = time(NULL);
  if(ev) {
    snprintf(op.title_id, sizeof(op.title_id), "%s", ev->title_id);
    snprintf(op.display_name, sizeof(op.display_name), "%s",
             ev->display_name[0] ? ev->display_name : ev->title_id);
    snprintf(op.source_path, sizeof(op.source_path), "%s", ev->source_path);
    snprintf(op.output_path, sizeof(op.output_path), "%s", ev->output_path);
    snprintf(op.source_kind, sizeof(op.source_kind), "%s", ev->source_kind);
    snprintf(op.format, sizeof(op.format), "%s",
             ev->format[0] ? ev->format : "pfs");
    snprintf(op.delete_policy, sizeof(op.delete_policy), "%s",
             ev->delete_policy);
    snprintf(op.compression_profile, sizeof(op.compression_profile), "%s",
             compression_profile_or_default(ev->compression_profile));
    snprintf(op.stream_order, sizeof(op.stream_order), "%s",
             ev->stream_order[0] ? ev->stream_order : "budgeted-gain");
    op.stream_budget_bytes = ev->stream_budget_bytes;
    snprintf(op.target_root, sizeof(op.target_root), "%s", ev->target_root);
    snprintf(op.repair_summary, sizeof(op.repair_summary), "%s",
             ev->repair_summary);
  }
  snprintf(op.result, sizeof(op.result), "%s", result ? result : "");
  snprintf(op.error, sizeof(op.error), "%s", error ? error : "");
  append_history_log(&op);
  pthread_mutex_unlock(&g_gc_lock);
  gc_notify_operation_done(action_name(op.action), op.display_name,
                           operation_notification_status(status), op.error);
}

static void
recovery_append_phase_history(const gc_history_recovery_event_t *ev,
                              const char *phase) {
  gc_operation_t op;
  if(!ev || !phase || !phase[0]) return;
  memset(&op, 0, sizeof(op));
  op.used = 1;
  op.action = action_from_name(ev->action);
  if(!op.action) op.action = GC_ACTION_VALIDATE_REPAIR;
  op.status = GC_OP_RUNNING;
  snprintf(op.id, sizeof(op.id), "%s", ev->id[0] ? ev->id : "recovery");
  snprintf(op.title_id, sizeof(op.title_id), "%s", ev->title_id);
  snprintf(op.display_name, sizeof(op.display_name), "%s",
           ev->display_name[0] ? ev->display_name : ev->title_id);
  snprintf(op.source_path, sizeof(op.source_path), "%s", ev->source_path);
  snprintf(op.output_path, sizeof(op.output_path), "%s", ev->output_path);
  snprintf(op.source_kind, sizeof(op.source_kind), "%s", ev->source_kind);
  snprintf(op.format, sizeof(op.format), "%s",
           ev->format[0] ? ev->format : "pfs");
  snprintf(op.delete_policy, sizeof(op.delete_policy), "%s",
           ev->delete_policy);
  snprintf(op.compression_profile, sizeof(op.compression_profile), "%s",
           compression_profile_or_default(ev->compression_profile));
  snprintf(op.stream_order, sizeof(op.stream_order), "%s",
           ev->stream_order[0] ? ev->stream_order : "budgeted-gain");
  op.stream_budget_bytes = ev->stream_budget_bytes;
  snprintf(op.target_root, sizeof(op.target_root), "%s", ev->target_root);
  snprintf(op.repair_summary, sizeof(op.repair_summary), "%s",
           ev->repair_summary);
  snprintf(op.phase, sizeof(op.phase), "%s", phase);
  snprintf(op.result, sizeof(op.result), "%s", phase);
  op.created_at = ev->created_at ? ev->created_at : time(NULL);
  op.started_at = ev->started_at ? ev->started_at : op.created_at;
  append_history_log(&op);
}

static void
recovery_remove_repair_temps(const char *title_id, const char *output_path) {
  char parent[1024];
  char prefix[96];
  DIR *d;

  if(!valid_title_id(title_id) || !output_path || !output_path[0] ||
     path_parent(output_path, parent, sizeof(parent)) != 0) {
    return;
  }
  int n = snprintf(prefix, sizeof(prefix), ".%s.repair.", title_id);
  if(n < 0 || (size_t)n >= sizeof(prefix)) return;
  d = opendir(parent);
  if(!d) return;
  struct dirent *ent;
  while((ent = readdir(d))) {
    if(strncmp(ent->d_name, prefix, strlen(prefix)) ||
       !ends_with_ci(ent->d_name, ".tmp")) {
      continue;
    }
    char path[1024];
    join_path(path, sizeof(path), parent, ent->d_name);
    if(unlink(path) == 0 || errno == ENOENT) {
      gc_log("recovery removed repair temp title=%s path=%s",
             title_id, path);
    } else {
      gc_log("recovery repair temp remove failed title=%s path=%s err=%s",
             title_id, path, strerror(errno));
    }
  }
  closedir(d);
}

static int
recovery_enqueue_validate_repair(const gc_history_recovery_event_t *ev) {
  if(!ev || !valid_title_id(ev->title_id) || !ev->output_path[0]) return -1;
  gc_action_t action = action_from_name(ev->action);
  if(action != GC_ACTION_VALIDATE_ONLY) action = GC_ACTION_VALIDATE_REPAIR;
  pthread_mutex_lock(&g_gc_lock);
  if(pending_op_for_title_locked(ev->title_id) ||
     active_op_for_title_locked(ev->title_id)) {
    pthread_mutex_unlock(&g_gc_lock);
    gc_log("recovery skipped duplicate title=%s", ev->title_id);
    return 0;
  }
  gc_operation_t *op = alloc_op_locked();
  if(!op) {
    pthread_mutex_unlock(&g_gc_lock);
    gc_log("recovery could not allocate operation title=%s", ev->title_id);
    return -1;
  }
  memset(op, 0, sizeof(*op));
  op->used = 1;
  op->seq = operation_seq_from_id(ev->id);
  if(op->seq == 0) op->seq = g_next_seq++;
  if(op->seq >= g_next_seq) g_next_seq = op->seq + 1;
  snprintf(op->id, sizeof(op->id), "%s", ev->id[0] ? ev->id : "recovery");
  op->action = action;
  op->status = GC_OP_QUEUED;
  op->created_at = ev->created_at ? ev->created_at : time(NULL);
  snprintf(op->title_id, sizeof(op->title_id), "%s", ev->title_id);
  snprintf(op->display_name, sizeof(op->display_name), "%s",
           ev->display_name[0] ? ev->display_name : ev->title_id);
  snprintf(op->source_path, sizeof(op->source_path), "%s", ev->output_path);
  snprintf(op->output_path, sizeof(op->output_path), "%s", ev->output_path);
  snprintf(op->source_kind, sizeof(op->source_kind), "%s", "compressed");
  snprintf(op->format, sizeof(op->format), "%s",
           ev->format[0] ? ev->format : "pfs");
  snprintf(op->delete_policy, sizeof(op->delete_policy), "%s", "none");
  op->recovery_direct = 1;
  append_history_log(op);
  start_next_locked();
  pthread_mutex_unlock(&g_gc_lock);
  gc_log("recovery enqueued %s title=%s path=%s",
         action_name(action), ev->title_id, ev->output_path);
  return 0;
}

static int
recovery_enqueue_stream_resume(const gc_history_recovery_event_t *ev) {
  char journal_path[1024];
  struct stat st;
  if(!ev || !valid_title_id(ev->title_id) || !ev->output_path[0]) return -1;
  if(pfs_compress_stream_journal_path(ev->output_path, journal_path,
                                      sizeof(journal_path)) != 0 ||
     stat(journal_path, &st) != 0 || !S_ISREG(st.st_mode)) {
    return -1;
  }
  pthread_mutex_lock(&g_gc_lock);
  if(pending_op_for_title_locked(ev->title_id) ||
     active_op_for_title_locked(ev->title_id)) {
    pthread_mutex_unlock(&g_gc_lock);
    gc_log("recovery skipped duplicate stream title=%s", ev->title_id);
    return 0;
  }
  gc_operation_t *op = alloc_op_locked();
  if(!op) {
    pthread_mutex_unlock(&g_gc_lock);
    gc_log("recovery could not allocate stream operation title=%s",
           ev->title_id);
    return -1;
  }
  memset(op, 0, sizeof(*op));
  op->used = 1;
  op->seq = operation_seq_from_id(ev->id);
  if(op->seq == 0) op->seq = g_next_seq++;
  if(op->seq >= g_next_seq) g_next_seq = op->seq + 1;
  snprintf(op->id, sizeof(op->id), "%s", ev->id[0] ? ev->id : "recovery");
  op->action = GC_ACTION_COMPRESS;
  op->status = GC_OP_QUEUED;
  op->created_at = ev->created_at ? ev->created_at : time(NULL);
  snprintf(op->title_id, sizeof(op->title_id), "%s", ev->title_id);
  snprintf(op->display_name, sizeof(op->display_name), "%s",
           ev->display_name[0] ? ev->display_name : ev->title_id);
  snprintf(op->source_path, sizeof(op->source_path), "%s", ev->source_path);
  snprintf(op->output_path, sizeof(op->output_path), "%s", ev->output_path);
  snprintf(op->source_kind, sizeof(op->source_kind), "%s",
           ev->source_kind[0] ? ev->source_kind : "folder");
  snprintf(op->format, sizeof(op->format), "%s",
           ev->format[0] ? ev->format : "pfs");
  snprintf(op->delete_policy, sizeof(op->delete_policy), "%s", "stream");
  snprintf(op->compression_profile, sizeof(op->compression_profile), "%s",
           compression_profile_or_default(ev->compression_profile));
  snprintf(op->stream_order, sizeof(op->stream_order), "%s",
           ev->stream_order[0] ? ev->stream_order : "budgeted-gain");
  op->stream_budget_bytes = ev->stream_budget_bytes;
  op->recovery_direct = 1;
  append_history_log(op);
  start_next_locked();
  pthread_mutex_unlock(&g_gc_lock);
  gc_log("recovery enqueued stream resume title=%s journal=%s",
         ev->title_id, journal_path);
  return 0;
}

static int
recovery_mark_repairing_and_enqueue(const gc_history_recovery_event_t *ev,
                                    int remove_temps) {
  char scan_err[256] = {0};
  struct stat st;

  if(!ev || !ev->output_path[0] ||
     stat(ev->output_path, &st) != 0 || !S_ISREG(st.st_mode) ||
     !ends_with_ci(ev->output_path, ".ffpfsc")) {
    recovery_append_terminal_history(ev, GC_OP_FAILED, "failed",
                                     "recovery output is missing");
    return -1;
  }

  if(remove_temps) {
    recovery_remove_repair_temps(ev->title_id, ev->output_path);
  }
  if(gc_shadowmount_request_scan(scan_err, sizeof(scan_err)) != 0) {
    gc_log("recovery scan request failed title=%s err=%s", ev->title_id,
           scan_err[0] ? scan_err : "unknown");
  }
  return recovery_enqueue_validate_repair(ev);
}

static int
recovery_process_history_event(gc_history_recovery_event_t *ev) {
  if(!ev || history_status_is_terminal(ev->status) ||
     strcmp(ev->status, "running")) {
    return 0;
  }
  if(!history_phase_is_recoverable(ev->phase) &&
     strcmp(ev->action, "validate-repair") &&
     strcmp(ev->action, "validate-only")) {
    return 0;
  }

  if(!strcmp(ev->action, "compress") && !strcmp(ev->phase, "compressing")) {
    if(!strcmp(ev->delete_policy, "stream") &&
       recovery_enqueue_stream_resume(ev) == 0) {
      recovery_append_phase_history(ev, "compressing");
      return 0;
    }
    char tmp_path[1024] = {0};
    if(ev->output_path[0]) {
      (void)recovery_tmp_path_for_output(ev->output_path, tmp_path,
                                         sizeof(tmp_path));
    }
    if(tmp_path[0]) {
      if(remove_tree_gc(tmp_path) == 0) {
        gc_log("recovery removed incomplete compression temp title=%s path=%s",
               ev->title_id, tmp_path);
      } else {
        gc_log("recovery temp cleanup failed title=%s path=%s err=%s",
               ev->title_id, tmp_path, strerror(errno));
      }
    }
    recovery_append_terminal_history(ev, GC_OP_CANCELLED, "cancelled",
                                     "recovered incomplete compression");
    return 0;
  }

  if(!strcmp(ev->action, "compress") && !strcmp(ev->phase, "compressed")) {
    gc_source_kind_t kind = source_kind_from_name(ev->source_kind);
    char err[256] = {0};
    struct stat st;
    if(ev->source_path[0] && lstat(ev->source_path, &st) == 0) {
      if(kind == GC_SOURCE_UNKNOWN) {
        recovery_append_terminal_history(ev, GC_OP_FAILED, "failed",
                                         "recovery source kind is unknown");
        return -1;
      }
      if(delete_source_after_success(ev->source_path, kind, err,
                                     sizeof(err)) != 0) {
        recovery_append_terminal_history(ev, GC_OP_FAILED, "failed",
            err[0] ? err : "recovery source delete failed");
        return -1;
      }
    }
    recovery_append_phase_history(ev, "source-deleted");
    snprintf(ev->phase, sizeof(ev->phase), "%s", "source-deleted");
    return recovery_mark_repairing_and_enqueue(ev, 0);
  }

  if(!strcmp(ev->action, "validate-repair") ||
     !strcmp(ev->action, "validate-only")) {
    return recovery_mark_repairing_and_enqueue(ev, 1);
  }

  if(!strcmp(ev->action, "compress") &&
     (!strcmp(ev->phase, "source-deleted") ||
      !strcmp(ev->phase, "repairing"))) {
    return recovery_mark_repairing_and_enqueue(
        ev, !strcmp(ev->phase, "repairing"));
  }

  gc_log("recovery ignoring history event title=%s action=%s phase=%s",
         ev->title_id, ev->action, ev->phase);
  return 0;
}

static int
recovery_restore_queued_op(const gc_history_recovery_event_t *ev) {
  char err[256] = {0};
  gc_action_t action;
  uint64_t seq;

  if(!ev || !history_status_is_pending(ev->status)) return 0;
  if(!recovery_queued_event_valid(ev, err, sizeof(err))) {
    gc_log("recovery dropping queued id=%s title=%s reason=%s",
           ev->id, ev->title_id, err);
    recovery_append_terminal_history(ev, GC_OP_CANCELLED, "cancelled", err);
    return 0;
  }
  action = action_from_name(ev->action);
  if(!action) {
    recovery_append_terminal_history(ev, GC_OP_CANCELLED, "cancelled",
                                     "queued operation has invalid action");
    return 0;
  }

  pthread_mutex_lock(&g_gc_lock);
  if(pending_op_for_title_locked(ev->title_id) ||
     active_op_for_title_locked(ev->title_id)) {
    pthread_mutex_unlock(&g_gc_lock);
    gc_log("recovery skipped duplicate queued title=%s id=%s",
           ev->title_id, ev->id);
    return 0;
  }
  gc_operation_t *op = alloc_op_locked();
  if(!op) {
    pthread_mutex_unlock(&g_gc_lock);
    recovery_append_terminal_history(ev, GC_OP_FAILED, "failed",
                                     "could not restore queued operation");
    return -1;
  }
  memset(op, 0, sizeof(*op));
  op->used = 1;
  seq = operation_seq_from_id(ev->id);
  op->seq = seq ? seq : g_next_seq++;
  if(op->seq >= g_next_seq) g_next_seq = op->seq + 1;
  snprintf(op->id, sizeof(op->id), "%s", ev->id[0] ? ev->id : "recovery");
  op->action = action;
  op->status = GC_OP_QUEUED;
  op->created_at = ev->created_at ? ev->created_at : time(NULL);
  op->started_at = ev->started_at;
  snprintf(op->title_id, sizeof(op->title_id), "%s", ev->title_id);
  snprintf(op->display_name, sizeof(op->display_name), "%s",
           ev->display_name[0] ? ev->display_name : ev->title_id);
  snprintf(op->source_path, sizeof(op->source_path), "%s", ev->source_path);
  snprintf(op->output_path, sizeof(op->output_path), "%s", ev->output_path);
  snprintf(op->source_kind, sizeof(op->source_kind), "%s", ev->source_kind);
  snprintf(op->format, sizeof(op->format), "%s",
           ev->format[0] ? ev->format : "pfs");
  snprintf(op->delete_policy, sizeof(op->delete_policy), "%s",
           ev->delete_policy[0] ? ev->delete_policy : "after");
  snprintf(op->compression_profile, sizeof(op->compression_profile), "%s",
           compression_profile_or_default(ev->compression_profile));
  snprintf(op->target_root, sizeof(op->target_root), "%s", ev->target_root);
  snprintf(op->repair_summary, sizeof(op->repair_summary), "%s",
           ev->repair_summary);
  if(action == GC_ACTION_VALIDATE_REPAIR ||
     action == GC_ACTION_VALIDATE_ONLY) {
    op->recovery_direct = 1;
  }
  append_history_log(op);
  start_next_locked();
  pthread_mutex_unlock(&g_gc_lock);
  gc_log("recovery restored queued id=%s action=%s title=%s",
         op->id, action_name(op->action), op->title_id);
  return 0;
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
repair_with_wait(const char *title_id, const char *path,
                 pfs_repair_info_t *info, char *err, size_t err_size) {
  time_t deadline = time(NULL) + GC_REPAIR_WAIT_SECONDS;
  gc_checkpoint("repair waiting for mount");
  gc_log("repair wait title=%s path=%s", title_id ? title_id : "",
         path ? path : "");
  while(1) {
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
      if(gc_shadowmount_request_scan(scan_err, sizeof(scan_err)) != 0) {
        gc_log("repair wait scan request failed title=%s err=%s",
               title_id ? title_id : "",
               scan_err[0] ? scan_err : "unknown");
      }
    }
    job_set_phase("mounting", 0, 0, "Waiting for ShadowMountPlus");
    sleep(GC_REPAIR_WAIT_STEP_SECONDS);
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
      if(gc_shadowmount_request_scan(scan_err, sizeof(scan_err)) != 0) {
        gc_log("validate-only wait scan request failed title=%s err=%s",
               title_id ? title_id : "",
               scan_err[0] ? scan_err : "unknown");
      }
    }
    job_set_phase("mounting", 0, 0, "Waiting for ShadowMountPlus");
    sleep(GC_REPAIR_WAIT_STEP_SECONDS);
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
  int is_ffpfsc = ends_with_ci(name, ".ffpfsc");
  if(!path || !title_id || !valid_title_id(title_id) || !name ||
     path_parent(path, parent, sizeof(parent)) != 0) {
    return -1;
  }
  size_t name_len = strlen(name);
  size_t suffix_len = strlen(".ffpfsc");
  size_t app_suffix_len = strlen("-app");
  int is_app_dir = !is_ffpfsc && name_len > app_suffix_len &&
                   ends_with_ci(name, "-app");
  size_t stem_len = is_ffpfsc ? name_len - suffix_len :
                    (is_app_dir ? name_len - app_suffix_len : name_len);
  if(stem_len != 9 || strncmp(name, title_id, stem_len) != 0) return -1;
  int n = snprintf(out, out_size, "%s/%.*s%s%ld_%u%s",
                   parent, (int)stem_len, name, GC_FORCE_REMOUNT_PREFIX,
                   (long)time(NULL), (unsigned)getpid(),
                   is_ffpfsc ? ".ffpfsc" : (is_app_dir ? "-app" : ""));
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
  job_set_phase("mounting", 0, 0, current ? current : "Waiting for remount");
  if(gc_shadowmount_request_scan(scan_err, sizeof(scan_err)) != 0) {
    gc_log("compressed remount scan request failed title=%s err=%s",
           title_id ? title_id : "", scan_err[0] ? scan_err : "unknown");
  }
  return wait_for_shadowmount_links(title_id, expected_mount, expected_image,
                                    err, err_size);
}

static int
wait_for_folder_shadowmount(const char *title_id, const char *path,
                            const char *current,
                            char *err, size_t err_size) {
  char scan_err[256] = {0};
  job_set_phase("mounting", 0, 0, current ? current : "Waiting for remount");
  if(gc_shadowmount_request_scan(scan_err, sizeof(scan_err)) != 0) {
    gc_log("folder remount scan request failed title=%s err=%s",
           title_id ? title_id : "", scan_err[0] ? scan_err : "unknown");
  }
  return wait_for_shadowmount_links(title_id, path, "", err, err_size);
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
  if(gc_shadowmount_request_scan(scan_err, sizeof(scan_err)) != 0) {
    gc_log("compressed temp remount scan failed title=%s err=%s",
           title_id ? title_id : "", scan_err[0] ? scan_err : "unknown");
  }
  sleep(GC_MOUNT_SCAN_REQUEST_SECONDS);
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
  return wait_for_compressed_shadowmount(title_id, original_path, nested_name,
                                         nested_type,
                                         "Waiting for final remount",
                                         err, err_size);
}

static int
force_folder_path_bounce_remount(const char *title_id,
                                 const char *original_path,
                                 char *err, size_t err_size) {
  char temp_path[1024];
  char scan_err[256] = {0};
  struct stat st;

  if(stat(original_path, &st) != 0 || !S_ISDIR(st.st_mode)) {
    snprintf(err, err_size, "folder remount path unavailable: %s",
             strerror(errno));
    return -1;
  }
  if(build_force_remount_temp_path(original_path, title_id, temp_path,
                                   sizeof(temp_path)) != 0) {
    snprintf(err, err_size, "%s", "could not build folder remount temp path");
    return -1;
  }
  if(rename(original_path, temp_path) != 0) {
    snprintf(err, err_size, "rename folder for remount: %s", strerror(errno));
    return -1;
  }
  gc_log("folder remount bounce title=%s original=%s temp=%s",
         title_id ? title_id : "", original_path, temp_path);

  job_set_phase("mounting", 0, 0, "Refreshing temporary mount state");
  if(gc_shadowmount_request_scan(scan_err, sizeof(scan_err)) != 0) {
    gc_log("folder temp remount scan failed title=%s err=%s",
           title_id ? title_id : "", scan_err[0] ? scan_err : "unknown");
  }
  sleep(GC_MOUNT_SCAN_REQUEST_SECONDS);
  if(rename(temp_path, original_path) != 0) {
    snprintf(err, err_size, "restore folder name: %s", strerror(errno));
    gc_log("folder remount restore failed title=%s temp=%s original=%s err=%s",
           title_id ? title_id : "", temp_path, original_path,
           err && err[0] ? err : "");
    return -1;
  }
  gc_log("folder remount restore title=%s temp=%s original=%s",
         title_id ? title_id : "", temp_path, original_path);
  return wait_for_folder_shadowmount(title_id, original_path,
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
build_move_target_path(const char *target_root, const char *source_path,
                       char *out, size_t out_size,
                       char *err, size_t err_size) {
  const char *name = path_basename(source_path);
  if(!target_root || !target_root[0] || !name || !upload_segment_safe(name)) {
    snprintf(err, err_size, "%s", "bad move target");
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
run_move_op(gc_operation_t *op) {
  gc_game_t game = {0};
  char err[256] = {0};
  char target_path[1024] = {0};
  char temp_path[1024] = {0};
  char expected_mount[1024] = {0};
  char expected_image[1024] = {0};
  struct stat st;
  uint64_t copied = 0;
  uint64_t free_bytes = 0;

  gc_checkpoint("move find game");
  gc_log("move start op=%s title=%s action=%s targetRoot=%s", op->id,
         op->title_id, action_name(op->action), op->target_root);
  if(find_game_by_title(op->title_id, &game) != 0 ||
     game.source_kind == GC_SOURCE_UNKNOWN) {
    snprintf(op->error, sizeof(op->error), "%s", "game is unavailable");
    gc_log("move failed title=%s err=%s", op->title_id, op->error);
    return -1;
  }
  snprintf(op->source_path, sizeof(op->source_path), "%s", game.source_path);
  if(close_title_if_running(op->title_id, err, sizeof(err)) != 0) {
    snprintf(op->error, sizeof(op->error), "%s",
             err[0] ? err : "could not close game");
    gc_log("move close game failed title=%s err=%s", op->title_id,
           op->error);
    return -1;
  }
  if(build_move_target_path(op->target_root, game.source_path, target_path,
                            sizeof(target_path), err, sizeof(err)) != 0) {
    snprintf(op->error, sizeof(op->error), "%s", err);
    gc_log("move failed title=%s err=%s", op->title_id, op->error);
    return -1;
  }
  snprintf(op->output_path, sizeof(op->output_path), "%s", target_path);
  if(!strcmp(game.source_path, target_path)) {
    snprintf(op->error, sizeof(op->error), "%s", "game is already in that location");
    gc_log("move failed title=%s err=%s", op->title_id, op->error);
    return -1;
  }
  if(stat(target_path, &st) == 0) {
    snprintf(op->error, sizeof(op->error), "%s", "target already exists");
    gc_log("move failed title=%s err=%s", op->title_id, op->error);
    return -1;
  }
  if(op->action == GC_ACTION_MOVE_TO_USB) {
    char usb_root[1024];
    if(path_parent(op->target_root, usb_root, sizeof(usb_root)) != 0 ||
       stat(usb_root, &st) != 0 || !S_ISDIR(st.st_mode)) {
      snprintf(op->error, sizeof(op->error), "%s", "selected USB is unavailable");
      gc_log("move failed title=%s err=%s", op->title_id, op->error);
      return -1;
    }
  }
  if(mkdirs(op->target_root) != 0) {
    snprintf(op->error, sizeof(op->error), "create target root: %s",
             strerror(errno));
    gc_log("move failed title=%s err=%s", op->title_id, op->error);
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
    gc_log("move failed title=%s err=%s", op->title_id, op->error);
    return -1;
  }
  if(snprintf(temp_path, sizeof(temp_path), "%s.moving", target_path) >=
     (int)sizeof(temp_path)) {
    snprintf(op->error, sizeof(op->error), "%s", "temporary move path too long");
    return -1;
  }
  remove_tree_gc(temp_path);

  job_set_target(target_path);
  job_set_phase("copying", 0, 0, "Transferring game");
  job_store_u64(&g_job.total_bytes, game.source_size);
  atomic_store(&g_job.copied_bytes, 0);
  gc_checkpoint("move copy");
  if(copy_tree_gc(game.source_path, temp_path, &copied, err, sizeof(err)) != 0) {
    snprintf(op->error, sizeof(op->error), "%s", err[0] ? err : "move failed");
    remove_tree_gc(temp_path);
    gc_log("move copy failed title=%s err=%s", op->title_id, op->error);
    return -1;
  }

  job_set_current("Finalizing move");
  if(rename(temp_path, target_path) != 0) {
    snprintf(op->error, sizeof(op->error), "rename target: %s",
             strerror(errno));
    remove_tree_gc(temp_path);
    gc_log("move finalize failed title=%s err=%s", op->title_id, op->error);
    return -1;
  }
  if(game.source_kind == GC_SOURCE_FOLDER) {
    size_cache_store(target_path, game.source_size);
  }

  gc_checkpoint("move delete source");
  if(delete_source_after_success(game.source_path, game.source_kind,
                                 err, sizeof(err)) != 0) {
    snprintf(op->error, sizeof(op->error), "%s", err);
    gc_log("move delete source failed title=%s err=%s", op->title_id,
           op->error);
    return -1;
  }
  if(game.source_kind == GC_SOURCE_COMPRESSED) {
    (void)rewrite_moved_validation_marker(op->title_id, game.source_path,
                                          target_path);
  }

  err[0] = 0;
  job_set_phase("mounting", 0, 0, "Requesting ShadowMount scan");
  if(gc_shadowmount_request_scan(err, sizeof(err)) != 0) {
    gc_log("move scan request failed title=%s err=%s", op->title_id,
           err[0] ? err : "unknown");
  }
  if(move_remount_expectations(&game, target_path, expected_mount,
                               sizeof(expected_mount), expected_image,
                               sizeof(expected_image), err,
                               sizeof(err)) != 0) {
    snprintf(op->error, sizeof(op->error), "%s",
             err[0] ? err : "could not derive moved remount path");
    gc_log("move remount failed title=%s err=%s", op->title_id, op->error);
    return -1;
  }
  gc_checkpoint("move wait remount");
  job_set_phase("mounting", 0, 0, "Waiting for remount");
  if(wait_for_shadowmount_links(op->title_id, expected_mount, expected_image,
                                err, sizeof(err)) != 0) {
    snprintf(op->error, sizeof(op->error), "%s",
             err[0] ? err : "ShadowMountPlus remount failed");
    gc_log("move remount failed title=%s err=%s", op->title_id, op->error);
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
  pfs_repair_info_t repair = {0};
  int format = !strcmp(op->format, "exfat") ?
      PFS_COMPRESS_FORMAT_EXFAT : PFS_COMPRESS_FORMAT_PFS;
  int delete_policy = !strcmp(op->delete_policy, "stream") ?
      PFS_DELETE_STREAM : PFS_DELETE_KEEP;
  int compression_profile = compression_profile_value(op->compression_profile);
  uint64_t stream_budget_bytes = op->stream_budget_bytes ?
      op->stream_budget_bytes : PFS_STREAM_DEFAULT_BUDGET_BYTES;
  pfs_stream_options_t stream_opts = {
    .budget_bytes = stream_budget_bytes,
    .reserve_bytes = 0,
    .order = (!strcmp(op->stream_order, "path")) ?
        PFS_STREAM_ORDER_PATH : PFS_STREAM_ORDER_BUDGETED_GAIN,
  };
  if(delete_policy == PFS_DELETE_STREAM) {
    compression_profile = PFS_COMPRESS_PROFILE_FAST;
    snprintf(op->compression_profile, sizeof(op->compression_profile), "%s",
             "fast");
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
  uint64_t target_free_bytes = 0;
  int moving_to_target = 0;
  struct stat st;

  gc_checkpoint("compress find game");
  snprintf(op->format, sizeof(op->format), "%s",
           format == PFS_COMPRESS_FORMAT_EXFAT ? "exfat" : "pfs");
  snprintf(op->compression_profile, sizeof(op->compression_profile), "%s",
           compression_profile_or_default(op->compression_profile));
  gc_log("compress start op=%s title=%s format=%s policy=%s profile=%s", op->id,
         op->title_id, op->format, op->delete_policy,
         op->compression_profile);
  if(op->recovery_direct && delete_policy == PFS_DELETE_STREAM &&
     op->output_path[0]) {
    char journal_path[1024];
    if(pfs_compress_stream_journal_path(op->output_path, journal_path,
                                        sizeof(journal_path)) != 0) {
      snprintf(op->error, sizeof(op->error), "%s",
               "could not derive stream journal path");
      return -1;
    }
    append_operation_phase(op, "compressing");
    atomic_store(&g_job.stream_min_free_bytes, 0);
    atomic_store(&g_job.stream_budget_bytes,
                 stream_budget_bytes > (uint64_t)LONG_MAX ? LONG_MAX :
                 (long)stream_budget_bytes);
    job_set_phase("compressing", 0, 0, "Resuming stream compression");
    int resume_rc = pfs_compress_resume_stream_journal_profile(
        journal_path, PFS_COMPRESS_DEFAULT_WORKERS, compression_profile,
        &info, err, sizeof(err));
    if(resume_rc != 0) {
      snprintf(op->error, sizeof(op->error), "%s",
               err[0] ? err : "stream compression resume failed");
      gc_log("compress resume failed title=%s err=%s", op->title_id,
             op->error);
      return -1;
    }
    snprintf(op->source_path, sizeof(op->source_path), "%s", info.source_path);
    snprintf(op->output_path, sizeof(op->output_path), "%s", info.output_path);
    snprintf(op->source_kind, sizeof(op->source_kind), "%s", "folder");
    compressed_output_committed = 1;
    append_operation_phase(op, "source-deleted");
    goto post_compress;
  }
  if(find_game_by_title(op->title_id, &game) != 0) {
    snprintf(op->error, sizeof(op->error), "%s", "game is no longer mounted");
    gc_log("compress failed title=%s err=%s", op->title_id, op->error);
    return -1;
  }
  snprintf(op->source_path, sizeof(op->source_path), "%s", game.source_path);
  if(op->target_root[0]) {
    moving_to_target = 1;
    if(delete_policy == PFS_DELETE_STREAM) {
      snprintf(op->error, sizeof(op->error), "%s",
               "destructive compression is not available while moving to USB");
      gc_log("compress failed title=%s err=%s", op->title_id, op->error);
      return -1;
    }
    if(build_compress_target_path(op->target_root, &game,
                                  compress_output_path,
                                  sizeof(compress_output_path),
                                  err, sizeof(err)) != 0) {
      snprintf(op->error, sizeof(op->error), "%s", err);
      gc_log("compress failed title=%s err=%s", op->title_id, op->error);
      return -1;
    }
    snprintf(op->output_path, sizeof(op->output_path), "%s",
             compress_output_path);
  } else {
    snprintf(compress_output_path, sizeof(compress_output_path), "%s",
             game.output_path);
    snprintf(op->output_path, sizeof(op->output_path), "%s", game.output_path);
  }
  snprintf(op->source_kind, sizeof(op->source_kind), "%s",
           source_kind_name(game.source_kind));
  if(close_title_if_running(op->title_id, err, sizeof(err)) != 0) {
    snprintf(op->error, sizeof(op->error), "%s",
             err[0] ? err : "could not close game");
    gc_log("compress close game failed title=%s err=%s", op->title_id,
           op->error);
    return -1;
  }
  append_operation_phase(op, "compressing");
  job_set_phase("compressing", 0, 0, "Compressing game");
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
    return -1;
  }
  if(moving_to_target) {
    if(path_parent(op->target_root, err, sizeof(err)) != 0 ||
       stat(err, &st) != 0 || !S_ISDIR(st.st_mode)) {
      snprintf(op->error, sizeof(op->error), "%s", "selected USB is unavailable");
      gc_log("compress failed title=%s err=%s", op->title_id, op->error);
      return -1;
    }
    if(mkdirs(op->target_root) != 0) {
      snprintf(op->error, sizeof(op->error), "create target root: %s",
               strerror(errno));
      gc_log("compress failed title=%s err=%s", op->title_id, op->error);
      return -1;
    }
    if(stat(compress_output_path, &st) == 0) {
      snprintf(op->error, sizeof(op->error), "%s", "target already exists");
      gc_log("compress failed title=%s err=%s", op->title_id, op->error);
      return -1;
    }
    if(space_for_path(op->target_root, &target_free_bytes, NULL) != 0 ||
       target_free_bytes < game.required_bytes) {
      uint64_t need = target_free_bytes < game.required_bytes
          ? game.required_bytes - target_free_bytes
          : game.required_bytes;
      snprintf(op->error, sizeof(op->error),
               "not enough free storage on selected USB; free %llu more bytes",
               (unsigned long long)need);
      gc_log("compress failed title=%s err=%s", op->title_id, op->error);
      return -1;
    }
  }
  if(!moving_to_target && delete_policy != PFS_DELETE_STREAM &&
     game.free_bytes < game.required_bytes) {
    snprintf(op->error, sizeof(op->error),
             "not enough free storage; free %llu more bytes",
             (unsigned long long)(game.required_bytes - game.free_bytes));
    gc_log("compress failed title=%s err=%s", op->title_id, op->error);
    return -1;
  }
  if(delete_policy == PFS_DELETE_STREAM &&
     !stream_delete_allowed_by_space_budget(&game, stream_budget_bytes)) {
    uint64_t min_free =
        stream_min_free_bytes_for_budget(game.source_size,
                                         stream_budget_bytes);
    uint64_t need = game.free_bytes < min_free ? min_free - game.free_bytes : 0;
    snprintf(op->error, sizeof(op->error),
             "not enough free storage for delete-while-processing; free %llu more bytes",
             (unsigned long long)need);
    gc_log("compress failed title=%s err=%s", op->title_id, op->error);
    return -1;
  }
  if(delete_policy == PFS_DELETE_STREAM) {
    atomic_store(&g_job.stream_min_free_bytes,
                 (long)stream_min_free_bytes_for_budget(game.source_size,
                                                        stream_budget_bytes));
    atomic_store(&g_job.stream_budget_bytes,
                 stream_budget_bytes > (uint64_t)LONG_MAX ? LONG_MAX :
                 (long)stream_budget_bytes);
  }
  gc_checkpoint("compress shadowmount hints");
  if(planned_compress_nested(&game, format, planned_nested_name,
                             sizeof(planned_nested_name),
                             &planned_nested_type) != 0) {
    snprintf(op->error, sizeof(op->error), "%s",
             "could not derive ShadowMount hints");
    gc_log("compress failed title=%s err=%s", op->title_id, op->error);
    return -1;
  }
  if(gc_shadowmount_prepare_pfsc_hints_for_title(op->title_id,
                                                compress_output_path,
                                                planned_nested_name,
                                                planned_nested_type,
                                                err, sizeof(err)) != 0) {
    snprintf(op->error, sizeof(op->error), "ShadowMount hint failed: %s",
             err[0] ? err : "unknown");
    gc_log("compress failed title=%s err=%s", op->title_id, op->error);
    return -1;
  }
  gc_log("compress shadowmount hints title=%s outer=%s nested=%s type=%d",
         op->title_id, compress_output_path, planned_nested_name,
         planned_nested_type);

  gc_checkpoint("compress writing ffpfsc");
  int compress_rc = pfs_compress_source_to_ffpfsc_opts_profile_output_ex(
      game.source_path, 0, PFS_COMPRESS_DEFAULT_WORKERS,
      format, delete_policy, compression_profile,
      moving_to_target ? compress_output_path : NULL,
      delete_policy == PFS_DELETE_STREAM ? &stream_opts : NULL,
      &info, err, sizeof(err));
  if(compress_rc != 0) {
    snprintf(op->error, sizeof(op->error), "%s",
             err[0] ? err : "compression failed");
    gc_log("compress failed title=%s err=%s", op->title_id, op->error);
    return -1;
  }
  snprintf(op->output_path, sizeof(op->output_path), "%s", info.output_path);
  gc_log("compress wrote title=%s output=%s nested=%s nestedSize=%llu storedSize=%llu",
         op->title_id, info.output_path, info.nested_name,
         (unsigned long long)info.nested_size,
         (unsigned long long)info.stored_size);
  compressed_output_committed = 1;
  if(delete_policy == PFS_DELETE_STREAM) {
    append_operation_phase(op, "source-deleted");
  } else {
    append_operation_phase(op, "compressed");
  }

post_compress:
  gc_checkpoint("compress request shadowmount scan");
  err[0] = 0;
  job_set_phase("mounting", 0, 0, "Requesting ShadowMount scan");
  if(gc_shadowmount_request_scan(err, sizeof(err)) != 0) {
    snprintf(op->error, sizeof(op->error),
             "ShadowMount scan request failed: %s",
             err[0] ? err : "unknown");
    gc_log("compress scan request failed title=%s err=%s", op->title_id,
           op->error);
    if(!compressed_output_committed) {
      cleanup_failed_safe_compress_output(info.output_path, op->title_id);
    }
    return -1;
  }

  gc_checkpoint("compress wait repair");
  job_set_phase("mounting", 0, 0, "Waiting for ShadowMountPlus");
  append_operation_phase(op, "repairing");
  if(repair_with_wait(op->title_id, info.output_path, &repair, err,
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
    gc_log("compress repair failed title=%s err=%s", op->title_id, op->error);
    if(!compressed_output_committed) {
      cleanup_failed_safe_compress_output(info.output_path, op->title_id);
    }
    return -1;
  }

  gc_checkpoint("compress force remount");
  operation_store_repair_success(op, &repair);
  if(repair_force_path_bounce_remount(op->title_id, info.output_path, &repair,
                                      err, sizeof(err)) != 0) {
    if(shadowmount_mount_missed(err)) {
      operation_mark_verified_not_mounted(op, err);
      (void)write_validation_marker_ex(op->title_id, info.output_path, &repair,
                                       op->result, game.source_size);
      if(delete_policy != PFS_DELETE_STREAM) {
        gc_checkpoint("compress delete source");
        job_set_current("Deleting original source");
        err[0] = 0;
        if(delete_source_after_success(game.source_path, game.source_kind,
                                       err, sizeof(err)) != 0) {
          snprintf(op->error, sizeof(op->error), "%s", err);
          gc_log("compress delete source failed title=%s err=%s", op->title_id,
                 op->error);
          return -1;
        }
        gc_log("compress source deleted title=%s path=%s", op->title_id,
               game.source_path);
        append_operation_phase(op, "source-deleted");
        snprintf(op->result, sizeof(op->result), "%s",
                 "verified-not-mounted");
      }
      return 0;
    }
    snprintf(op->error, sizeof(op->error), "%s",
             err[0] ? err : "repair smoke verification failed");
    gc_log("compress smoke verify failed title=%s err=%s", op->title_id,
           op->error);
    return -1;
  }

  (void)write_validation_marker_ex(op->title_id, info.output_path, &repair,
                                   op->result, game.source_size);
  if(delete_policy != PFS_DELETE_STREAM) {
    gc_checkpoint("compress delete source");
    job_set_current("Deleting original source");
    err[0] = 0;
    if(delete_source_after_success(game.source_path, game.source_kind,
                                   err, sizeof(err)) != 0) {
      snprintf(op->error, sizeof(op->error), "%s", err);
      gc_log("compress delete source failed title=%s err=%s", op->title_id,
             op->error);
      return -1;
    }
    gc_log("compress source deleted title=%s path=%s", op->title_id,
           game.source_path);
    append_operation_phase(op, "source-deleted");
    snprintf(op->result, sizeof(op->result), "%s",
             repair.repaired_blocks > 0 ? "repaired" : "success");
  }
  uint64_t final_compressed_size =
      source_size_bytes_exact(info.output_path, GC_SOURCE_COMPRESSED);
  uint64_t saved_bytes = game.source_size > final_compressed_size
      ? game.source_size - final_compressed_size
      : 0;
  gc_log("compress complete title=%s result=%s repaired=%llu saved=%llu",
         op->title_id, op->result,
         (unsigned long long)op->repaired_blocks,
         (unsigned long long)saved_bytes);
  return 0;
}

static int
run_uncompress_op(gc_operation_t *op) {
  gc_game_t game = {0};
  char err[256] = {0};
  pfs_decompress_info_t info = {0};
  int delete_policy = !strcmp(op->delete_policy, "stream") ?
      PFS_DELETE_STREAM : PFS_DELETE_KEEP;

  gc_checkpoint("uncompress find game");
  gc_log("uncompress start op=%s title=%s policy=%s", op->id, op->title_id,
         op->delete_policy);
  if(find_game_by_title(op->title_id, &game) != 0 ||
     game.source_kind != GC_SOURCE_COMPRESSED) {
    snprintf(op->error, sizeof(op->error), "%s", "compressed game is unavailable");
    gc_log("uncompress failed title=%s err=%s", op->title_id, op->error);
    return -1;
  }
  snprintf(op->source_path, sizeof(op->source_path), "%s", game.source_path);
  snprintf(op->output_path, sizeof(op->output_path), "%s", game.output_path);
  snprintf(op->source_kind, sizeof(op->source_kind), "%s", "compressed");
  if(close_title_if_running(op->title_id, err, sizeof(err)) != 0) {
    snprintf(op->error, sizeof(op->error), "%s",
             err[0] ? err : "could not close game");
    gc_log("uncompress close game failed title=%s err=%s", op->title_id,
           op->error);
    return -1;
  }
  job_set_phase("unpacking", 0, 0, "Unpacking game");
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
  if(delete_policy != PFS_DELETE_STREAM &&
     game.free_bytes < game.required_bytes) {
    snprintf(op->error, sizeof(op->error),
             "not enough free storage; free %llu more bytes",
             (unsigned long long)(game.required_bytes - game.free_bytes));
    gc_log("uncompress failed title=%s err=%s", op->title_id, op->error);
    return -1;
  }
  if(delete_policy == PFS_DELETE_STREAM &&
     !stream_delete_allowed_by_space(&game)) {
    uint64_t min_free = stream_min_free_bytes(game.source_size);
    uint64_t need = game.free_bytes < min_free ? min_free - game.free_bytes : 0;
    snprintf(op->error, sizeof(op->error),
             "not enough free storage for delete-while-processing; free %llu more bytes",
             (unsigned long long)need);
    gc_log("uncompress failed title=%s err=%s", op->title_id, op->error);
    return -1;
  }
  if(delete_policy == PFS_DELETE_STREAM) {
    atomic_store(&g_job.stream_min_free_bytes,
                 (long)stream_min_free_bytes(game.source_size));
  }
  gc_checkpoint("uncompress writing source");
  if(pfs_decompress_ffpfsc_to_app_opts(game.source_path, 0,
                                       PFS_COMPRESS_DEFAULT_WORKERS,
                                       delete_policy, &info, err,
                                       sizeof(err)) != 0) {
    snprintf(op->error, sizeof(op->error), "%s",
             err[0] ? err : "uncompress failed");
    gc_log("uncompress failed title=%s err=%s", op->title_id, op->error);
    return -1;
  }
  snprintf(op->output_path, sizeof(op->output_path), "%s", info.output_path);
  gc_log("uncompress wrote title=%s output=%s nested=%s", op->title_id,
         info.output_path, info.nested_name);
  if(delete_policy != PFS_DELETE_STREAM) {
    gc_checkpoint("uncompress delete source");
    if(delete_source_after_success(game.source_path, GC_SOURCE_COMPRESSED,
                                   err, sizeof(err)) != 0) {
      snprintf(op->error, sizeof(op->error), "%s", err);
      gc_log("uncompress delete source failed title=%s err=%s", op->title_id,
             op->error);
      return -1;
    }
    gc_log("uncompress source deleted title=%s path=%s", op->title_id,
           game.source_path);
  }

  gc_checkpoint("uncompress shadowmount hint cleanup");
  char hint_err[256] = {0};
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

  struct stat output_st;
  pfs_app_info_t output_probe = {0};
  err[0] = 0;
  if(stat(info.output_path, &output_st) != 0 ||
     !S_ISDIR(output_st.st_mode) ||
     pfs_app_probe(info.output_path, &output_probe, err, sizeof(err)) != 0) {
    snprintf(op->error, sizeof(op->error), "%s",
             err[0] ? err : "uncompressed app folder is not valid");
    gc_log("uncompress output verify failed title=%s path=%s err=%s",
           op->title_id, info.output_path,
           op->error);
    return -1;
  }
  gc_checkpoint("uncompress request shadowmount scan");
  job_set_phase("mounting", 0, 0, "Requesting ShadowMount scan");
  err[0] = 0;
  if(gc_shadowmount_request_scan(err, sizeof(err)) != 0) {
    gc_log("uncompress scan request failed title=%s err=%s", op->title_id,
           err[0] ? err : "unknown");
  }
  gc_checkpoint("uncompress force remount");
  if(force_folder_path_bounce_remount(op->title_id, info.output_path,
                                      err, sizeof(err)) != 0) {
    if(shadowmount_mount_missed(err)) {
      snprintf(op->result, sizeof(op->result), "%s", "not-mounted");
      op->error[0] = 0;
      size_cache_queue_measure(info.output_path);
      gc_log("uncompress complete but not mounted title=%s output=%s detail=%s",
             op->title_id, op->output_path, err[0] ? err : "");
      return 0;
    }
    snprintf(op->error, sizeof(op->error), "%s",
             err[0] ? err : "ShadowMountPlus remount failed");
    gc_log("uncompress remount failed title=%s err=%s", op->title_id,
           op->error);
    return -1;
  }
  size_cache_queue_measure(info.output_path);
  snprintf(op->result, sizeof(op->result), "%s", "success");
  gc_log("uncompress complete title=%s output=%s", op->title_id,
         op->output_path);
  return 0;
}

static int
run_validate_repair_op(gc_operation_t *op) {
  gc_game_t game = {0};
  char err[256] = {0};
  pfs_repair_info_t repair = {0};
  const char *repair_path = NULL;
  uint64_t source_size = 0;
  uint64_t free_bytes = 0;
  int recovery_mode = op->recovery_direct && op->source_path[0];
  gc_checkpoint("validate find game");
  gc_log("validate start op=%s title=%s", op->id, op->title_id);
  if(recovery_mode) {
    struct stat st;
    if(stat(op->source_path, &st) != 0 || !S_ISREG(st.st_mode) ||
       !ends_with_ci(op->source_path, ".ffpfsc")) {
      snprintf(op->error, sizeof(op->error), "%s",
               "compressed game is unavailable");
      gc_log("validate failed title=%s err=%s", op->title_id, op->error);
      return -1;
    }
    repair_path = op->source_path;
    snprintf(op->output_path, sizeof(op->output_path), "%s", op->source_path);
    snprintf(op->source_kind, sizeof(op->source_kind), "%s", "compressed");
    source_size = st.st_size > 0 ? (uint64_t)st.st_size : 0;
    (void)free_bytes_for_output(repair_path, &free_bytes);
  } else {
    if(find_game_by_title(op->title_id, &game) != 0 ||
       game.source_kind != GC_SOURCE_COMPRESSED) {
      snprintf(op->error, sizeof(op->error), "%s", "compressed game is unavailable");
      gc_log("validate failed title=%s err=%s", op->title_id, op->error);
      return -1;
    }
    snprintf(op->source_path, sizeof(op->source_path), "%s", game.source_path);
    snprintf(op->output_path, sizeof(op->output_path), "%s", game.source_path);
    snprintf(op->source_kind, sizeof(op->source_kind), "%s", "compressed");
    repair_path = game.source_path;
    source_size = game.source_size;
    free_bytes = game.free_bytes;
  }
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
         recovery_mode ? "recovery" : game.validation_status);
  err[0] = 0;
  job_set_phase("mounting", 0, 0, "Requesting ShadowMount scan");
  if(gc_shadowmount_request_scan(err, sizeof(err)) != 0) {
    gc_log("validate scan request failed title=%s err=%s", op->title_id,
           err[0] ? err : "unknown");
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
    return -1;
  }
  operation_store_repair_success(op, &repair);
  if(repair_force_path_bounce_remount(op->title_id, repair_path, &repair,
                                      err, sizeof(err)) != 0) {
    if(shadowmount_mount_missed(err)) {
      operation_mark_verified_not_mounted(op, err);
      (void)write_validation_marker_ex(op->title_id, repair_path, &repair,
                                       op->result, 0);
      return 0;
    }
    snprintf(op->error, sizeof(op->error), "%s",
             err[0] ? err : "repair smoke verification failed");
    gc_log("validate smoke verify failed title=%s err=%s", op->title_id,
           op->error);
    return -1;
  }
  (void)write_validation_marker_ex(op->title_id, repair_path, &repair,
                                   op->result, 0);
  gc_log("validate complete title=%s result=%s repaired=%llu",
         op->title_id, op->result,
         (unsigned long long)op->repaired_blocks);
  return 0;
}

static int
run_validate_only_op(gc_operation_t *op) {
  gc_game_t game = {0};
  char err[256] = {0};
  pfs_repair_info_t repair = {0};
  const char *repair_path = NULL;
  uint64_t source_size = 0;
  uint64_t free_bytes = 0;
  int scan_rc = -1;
  int recovery_mode = op->recovery_direct && op->source_path[0];

  gc_checkpoint("validate-only find game");
  gc_log("validate-only start op=%s title=%s", op->id, op->title_id);
  if(recovery_mode) {
    struct stat st;
    if(stat(op->source_path, &st) != 0 || !S_ISREG(st.st_mode) ||
       !ends_with_ci(op->source_path, ".ffpfsc")) {
      snprintf(op->error, sizeof(op->error), "%s",
               "compressed game is unavailable");
      gc_log("validate-only failed title=%s err=%s",
             op->title_id, op->error);
      return -1;
    }
    repair_path = op->source_path;
    snprintf(op->output_path, sizeof(op->output_path), "%s", op->source_path);
    snprintf(op->source_kind, sizeof(op->source_kind), "%s", "compressed");
    source_size = st.st_size > 0 ? (uint64_t)st.st_size : 0;
    (void)free_bytes_for_output(repair_path, &free_bytes);
  } else {
    if(find_game_by_title(op->title_id, &game) != 0 ||
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
  }
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
         recovery_mode ? "recovery" : game.validation_status);
  err[0] = 0;
  job_set_phase("mounting", 0, 0, "Requesting ShadowMount scan");
  if(gc_shadowmount_request_scan(err, sizeof(err)) != 0) {
    gc_log("validate-only scan request failed title=%s err=%s",
           op->title_id, err[0] ? err : "unknown");
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
    return -1;
  }

  op->bad_blocks_found = repair.repaired_blocks;
  op->repaired_blocks = 0;
  operation_store_repair_counters(op, &repair);
  snprintf(op->repair_summary, sizeof(op->repair_summary), "%s",
           repair.outdir);
  snprintf(op->result, sizeof(op->result), "%s",
           scan_rc == PFS_REPAIR_SCAN_REPAIR_NEEDED ||
               repair.repaired_blocks > 0 ? "bad-blocks-found" : "clean");
  if(repair.repaired_blocks > 0) {
    delete_validation_marker(op->title_id);
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
                                         op->result, 0);
      } else {
        snprintf(op->result, sizeof(op->result), "%s",
                 "bad-blocks-found-not-mounted");
        op->error[0] = 0;
        delete_validation_marker(op->title_id);
        gc_log("validate-only complete but not mounted title=%s bad=%llu detail=%s",
               op->title_id, (unsigned long long)op->bad_blocks_found,
               err[0] ? err : "");
      }
      return 0;
    }
    snprintf(op->error, sizeof(op->error), "%s",
             err[0] ? err : "ShadowMountPlus remount failed");
    gc_log("validate-only remount failed title=%s err=%s",
           op->title_id, op->error);
    return -1;
  }
  if(repair.repaired_blocks == 0) {
    (void)write_validation_marker_ex(op->title_id, repair_path, &repair,
                                     op->result, 0);
  } else {
    delete_validation_marker(op->title_id);
  }
  gc_log("validate-only complete title=%s result=%s bad=%llu",
         op->title_id, op->result,
         (unsigned long long)op->bad_blocks_found);
  return 0;
}

static int
run_refresh_mount_op(gc_operation_t *op) {
  gc_game_t game = {0};
  pfs_decompress_info_t dec = {0};
  char err[256] = {0};
  int was_validated = 0;

  gc_checkpoint("refresh-mount find game");
  gc_log("refresh-mount start op=%s title=%s", op->id, op->title_id);
  if(find_game_by_title(op->title_id, &game) != 0 ||
     game.source_kind != GC_SOURCE_COMPRESSED) {
    snprintf(op->error, sizeof(op->error), "%s",
             "compressed game is unavailable");
    gc_log("refresh-mount failed title=%s err=%s", op->title_id, op->error);
    return -1;
  }

  snprintf(op->source_path, sizeof(op->source_path), "%s", game.source_path);
  snprintf(op->output_path, sizeof(op->output_path), "%s", game.source_path);
  snprintf(op->source_kind, sizeof(op->source_kind), "%s", "compressed");
  was_validated = game.validation == GC_VALIDATION_VALIDATED;
  job_set_target(game.source_path);
  job_set_phase("mounting", 0, 0, "Requesting ShadowMount scan");

  if(pfs_decompress_detect_nested(game.source_path, &dec, err,
                                  sizeof(err)) != 0) {
    snprintf(op->error, sizeof(op->error), "%s",
             err[0] ? err : "could not inspect compressed image");
    gc_log("refresh-mount inspect failed title=%s err=%s", op->title_id,
           op->error);
    return -1;
  }

  gc_checkpoint("refresh-mount force remount");
  if(force_compressed_path_bounce_remount(op->title_id, game.source_path,
                                          dec.nested_name, dec.nested_type,
                                          err, sizeof(err)) != 0) {
    if(shadowmount_mount_missed(err)) {
      snprintf(op->result, sizeof(op->result), "%s", "not-mounted");
      op->error[0] = 0;
      if(was_validated) {
        (void)write_validation_marker_ex(op->title_id, game.source_path, NULL,
                                         "verified-not-mounted", 0);
      }
      gc_log("refresh-mount complete but not mounted title=%s path=%s detail=%s",
             op->title_id, game.source_path, err[0] ? err : "");
      return 0;
    }
    snprintf(op->error, sizeof(op->error), "%s",
             err[0] ? err : "ShadowMountPlus remount failed");
    gc_log("refresh-mount failed title=%s err=%s", op->title_id, op->error);
    return -1;
  }

  snprintf(op->result, sizeof(op->result), "%s", "success");
  if(was_validated) {
    (void)write_validation_marker_ex(op->title_id, game.source_path, NULL,
                                     "validated", 0);
  }
  gc_log("refresh-mount complete title=%s path=%s", op->title_id,
         game.source_path);
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
    append_history_log(op);
    g_worker_running = 0;
    start_next_locked();
    pthread_mutex_unlock(&g_gc_lock);
    gc_log("operation job_begin failed id=%s err=%s", op->id, notify_error);
    gc_notify_operation_done(notify_action, notify_game, "failed",
                             notify_error);
    return NULL;
  }

  gc_checkpoint("operation running");
  if(op->action == GC_ACTION_COMPRESS) rc = run_compress_op(op);
  else if(op->action == GC_ACTION_UNCOMPRESS) rc = run_uncompress_op(op);
  else if(op->action == GC_ACTION_VALIDATE_REPAIR) rc = run_validate_repair_op(op);
  else if(op->action == GC_ACTION_VALIDATE_ONLY) rc = run_validate_only_op(op);
  else if(op->action == GC_ACTION_MOVE_TO_USB ||
          op->action == GC_ACTION_MOVE_TO_INTERNAL) rc = run_move_op(op);
  else if(op->action == GC_ACTION_REFRESH_MOUNT) rc = run_refresh_mount_op(op);

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
    if(!op->result[0] || history_phase_is_recoverable(op->result)) {
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
  append_history_log(op);
  g_worker_running = 0;
  start_next_locked();
  pthread_mutex_unlock(&g_gc_lock);

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
  append_history_log(next);
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
    append_history_log(next);
    g_worker_running = 0;
  }
}

static int
append_low_space_problem(json_buf_t *problem, const gc_game_t *game,
                         int stream_ok,
                         const char *requested_delete_policy,
                         uint64_t stream_budget_bytes) {
  uint64_t stream_min =
      stream_min_free_bytes_for_budget(game->source_size, stream_budget_bytes);
  uint64_t stream_extra = game->free_bytes >= stream_min ?
      0 : stream_min - game->free_bytes;
  if(json_append(problem, "{\"ok\":false,\"error\":\"not enough free storage\","
                 "\"lowSpace\":true,\"canStreamDelete\":") != 0 ||
     json_append(problem, stream_ok ? "true" : "false") != 0 ||
     json_append(problem, ",\"streamDeleteAvailable\":") != 0 ||
     json_append(problem, game->can_stream_delete ? "true" : "false") != 0 ||
     json_appendf(problem,
                  ",\"requiredBytes\":%llu,\"freeBytes\":%llu,"
                  "\"extraBytes\":%llu,\"streamMinFreeBytes\":%llu,"
                 "\"streamExtraBytes\":%llu",
                  (unsigned long long)game->required_bytes,
                  (unsigned long long)game->free_bytes,
                  (unsigned long long)game->extra_needed,
                  (unsigned long long)stream_min,
                  (unsigned long long)stream_extra) != 0) {
    return -1;
  }
  if(requested_delete_policy && requested_delete_policy[0]) {
    if(json_append(problem, ",\"requestedDeletePolicy\":") != 0 ||
       json_string(problem, requested_delete_policy) != 0) {
      return -1;
    }
  }
  return json_append(problem, "}") == 0 ? -2 : -1;
}

static int
append_stream_unsupported_problem(json_buf_t *problem) {
  return json_append(problem,
      "{\"ok\":false,\"error\":\"destructive compression is not available "
      "for this source\",\"streamUnsupported\":true}") == 0 ? -2 : -1;
}

static int
preflight_action(const gc_game_t *game, gc_action_t action, int confirm_stream,
                 const char *requested_delete_policy,
                 uint64_t stream_budget_bytes,
                 char *delete_policy, size_t delete_policy_size,
                 json_buf_t *problem) {
  if(action == GC_ACTION_VALIDATE_REPAIR ||
     action == GC_ACTION_VALIDATE_ONLY ||
     action == GC_ACTION_REFRESH_MOUNT) {
    snprintf(delete_policy, delete_policy_size, "%s", "none");
    return 0;
  }
  if(game->output_exists &&
     (action == GC_ACTION_COMPRESS || action == GC_ACTION_UNCOMPRESS)) {
    return json_append(problem,
        "{\"ok\":false,\"error\":\"output exists\",\"outputExists\":true}") == 0
        ? -2 : -1;
  }
  if(game->size_pending || game->source_size == 0 || game->required_bytes == 0) {
    if(action == GC_ACTION_COMPRESS &&
       game->source_kind == GC_SOURCE_FOLDER) {
      snprintf(delete_policy, delete_policy_size, "%s",
               requested_delete_policy && !strcmp(requested_delete_policy, "stream")
                   ? "stream"
                   : "after");
      return 0;
    }
    return json_append(problem,
        "{\"ok\":false,\"error\":\"could not measure game size\"}") == 0
        ? -2 : -1;
  }
  int stream_ok = stream_delete_allowed_by_space_budget(game,
                                                       stream_budget_bytes);
  if(requested_delete_policy && !strcmp(requested_delete_policy, "stream")) {
    if(!game->can_stream_delete) return append_stream_unsupported_problem(problem);
    if(stream_ok) {
      snprintf(delete_policy, delete_policy_size, "%s", "stream");
      return 0;
    }
    return append_low_space_problem(problem, game, stream_ok, "stream",
                                    stream_budget_bytes);
  }
  if(requested_delete_policy && !strcmp(requested_delete_policy, "after")) {
    if(game->free_bytes >= game->required_bytes) {
      snprintf(delete_policy, delete_policy_size, "%s", "after");
      return 0;
    }
    return append_low_space_problem(problem, game, stream_ok, "after",
                                    stream_budget_bytes);
  }
  if(game->free_bytes >= game->required_bytes) {
    snprintf(delete_policy, delete_policy_size, "%s", "after");
    return 0;
  }
  if(stream_ok && confirm_stream) {
    snprintf(delete_policy, delete_policy_size, "%s", "stream");
    return 0;
  }
  return append_low_space_problem(problem, game, stream_ok, "",
                                  stream_budget_bytes);
}

static int
enqueue_action(const http_request_t *req, gc_action_t action) {
  char title_id[64];
  char format[16];
  char format_arg[16];
  char profile_arg[24];
  char confirm_arg[16];
  char delete_policy_arg[24];
  char budget_arg[32];
  char order_arg[24];
  char usb_id[16];
  char requested_delete_policy[16] = "";
  char compression_profile[16] = "space";
  char stream_order[24] = "budgeted-gain";
  uint64_t stream_budget_bytes = PFS_STREAM_DEFAULT_BUDGET_BYTES;
  int confirm_stream = 0;
  gc_game_t game;
  json_buf_t problem = {0};
  char delete_policy[16] = "after";
  char target_root[1024] = "";
  char target_path[1024] = "";
  char err[256] = {0};
  uint64_t target_free = 0;

  if(!websrv_get_query_arg(req, "titleId", title_id, sizeof(title_id)) ||
     !valid_title_id(title_id)) {
    return serve_error(req, 400, "bad titleId");
  }
  pthread_mutex_lock(&g_gc_lock);
  if(active_op_for_title_locked(title_id) || pending_op_for_title_locked(title_id)) {
    pthread_mutex_unlock(&g_gc_lock);
    return serve_error(req, 409, "action already running for this game");
  }
  pthread_mutex_unlock(&g_gc_lock);
  snprintf(format, sizeof(format), "%s", "pfs");
  if(action == GC_ACTION_COMPRESS &&
     websrv_get_query_arg(req, "format", format_arg, sizeof(format_arg))) {
    if(!strcasecmp(format_arg, "pfs")) {
      snprintf(format, sizeof(format), "%s", "pfs");
    } else if(!strcasecmp(format_arg, "exfat")) {
      snprintf(format, sizeof(format), "%s", "exfat");
    } else {
      return serve_error(req, 400, "bad compression format");
    }
  }
  if(action == GC_ACTION_COMPRESS &&
     (websrv_get_query_arg(req, "profile", profile_arg, sizeof(profile_arg)) ||
      websrv_get_query_arg(req, "compressionProfile", profile_arg,
                           sizeof(profile_arg)))) {
    if(!strcasecmp(profile_arg, "fast")) {
      snprintf(compression_profile, sizeof(compression_profile), "%s", "fast");
    } else if(!strcasecmp(profile_arg, "space")) {
      snprintf(compression_profile, sizeof(compression_profile), "%s", "space");
    } else {
      return serve_error(req, 400, "bad compression profile");
    }
  }
  if(websrv_get_query_arg(req, "confirmStream", confirm_arg,
                          sizeof(confirm_arg)) &&
     strcmp(confirm_arg, "0")) {
    confirm_stream = 1;
  }
  if(websrv_get_query_arg(req, "deletePolicy", delete_policy_arg,
                          sizeof(delete_policy_arg))) {
    if(!strcasecmp(delete_policy_arg, "after") ||
       !strcasecmp(delete_policy_arg, "safe")) {
      snprintf(requested_delete_policy, sizeof(requested_delete_policy),
               "%s", "after");
    } else if(!strcasecmp(delete_policy_arg, "stream") ||
              !strcasecmp(delete_policy_arg, "destructive")) {
      snprintf(requested_delete_policy, sizeof(requested_delete_policy),
               "%s", "stream");
    } else {
      return serve_error(req, 400, "bad delete policy");
    }
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
  if(find_game_by_title_ex(title_id, &game, 0) != 0) {
    return serve_error(req, 404, "game is not mounted");
  }
  if(action == GC_ACTION_COMPRESS &&
     game.source_kind != GC_SOURCE_FOLDER &&
     game.source_kind != GC_SOURCE_IMAGE) {
    return serve_error(req, 400, "game is not compressible");
  }
  if(action == GC_ACTION_COMPRESS &&
     websrv_get_query_arg(req, "usbId", usb_id, sizeof(usb_id))) {
    gc_usb_target_t usb;
    struct stat st;
    if(usb_target_for_id(usb_id, &usb) != 0) {
      return serve_error(req, 400, "bad usbId");
    }
    if(requested_delete_policy[0] && !strcmp(requested_delete_policy, "stream")) {
      return serve_error(req, 400,
                         "destructive compression is not available while moving to USB");
    }
    snprintf(target_root, sizeof(target_root), "%s", usb.target_root);
    target_free = usb.free_bytes;
    snprintf(requested_delete_policy, sizeof(requested_delete_policy), "%s",
             "after");
    if(build_compress_target_path(target_root, &game, target_path,
                                  sizeof(target_path), err, sizeof(err)) != 0) {
      return serve_error(req, 400, err[0] ? err : "bad compression target");
    }
    if(stat(target_path, &st) == 0) {
      return serve_error(req, 409, "target already exists");
    }
    if(!game.size_pending && game.required_bytes > 0 &&
       target_free < game.required_bytes) {
      json_buf_t target_problem = {0};
      uint64_t extra = game.required_bytes - target_free;
      if(json_append(&target_problem,
                     "{\"ok\":false,\"error\":\"not enough free storage\","
                     "\"lowSpace\":true,\"canStreamDelete\":false,"
                     "\"requestedDeletePolicy\":\"after\"") != 0 ||
         json_appendf(&target_problem,
                      ",\"requiredBytes\":%llu,\"freeBytes\":%llu,"
                      "\"extraBytes\":%llu}",
                      (unsigned long long)game.required_bytes,
                      (unsigned long long)target_free,
                      (unsigned long long)extra) != 0) {
        free(target_problem.data);
        return -1;
      }
      return serve_owned(req, 409, target_problem.data, target_problem.len);
    }
  }
  if((action == GC_ACTION_UNCOMPRESS ||
      action == GC_ACTION_VALIDATE_REPAIR ||
      action == GC_ACTION_VALIDATE_ONLY ||
      action == GC_ACTION_REFRESH_MOUNT) &&
     game.source_kind != GC_SOURCE_COMPRESSED) {
    return serve_error(req, 400, "game is not compressed");
  }
  if(action == GC_ACTION_COMPRESS && target_root[0]) {
    snprintf(delete_policy, sizeof(delete_policy), "%s", "after");
  } else {
    int p = preflight_action(&game, action, confirm_stream,
                             requested_delete_policy, stream_budget_bytes,
                             delete_policy,
                             sizeof(delete_policy), &problem);
    if(p == -2) return serve_owned(req, 409, problem.data, problem.len);
    if(p != 0) {
      free(problem.data);
      return serve_error(req, 500, "preflight failed");
    }
  }
  free(problem.data);

  pthread_mutex_lock(&g_gc_lock);
  if(active_op_for_title_locked(title_id) || pending_op_for_title_locked(title_id)) {
    pthread_mutex_unlock(&g_gc_lock);
    return serve_error(req, 409, "action already running for this game");
  }
  gc_operation_t *op = alloc_op_locked();
  if(!op) {
    pthread_mutex_unlock(&g_gc_lock);
    return serve_error(req, 409, "history is full");
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
  snprintf(op->display_name, sizeof(op->display_name), "%s", game.name);
  snprintf(op->source_path, sizeof(op->source_path), "%s", game.source_path);
  snprintf(op->output_path, sizeof(op->output_path), "%s",
           target_path[0] ? target_path : game.output_path);
  snprintf(op->source_kind, sizeof(op->source_kind), "%s",
           source_kind_name(game.source_kind));
  snprintf(op->format, sizeof(op->format), "%s", format);
  snprintf(op->delete_policy, sizeof(op->delete_policy), "%s",
           delete_policy);
  snprintf(op->compression_profile, sizeof(op->compression_profile), "%s",
           compression_profile_or_default(compression_profile));
  snprintf(op->stream_order, sizeof(op->stream_order), "%s", stream_order);
  op->stream_budget_bytes = stream_budget_bytes;
  snprintf(op->target_root, sizeof(op->target_root), "%s", target_root);
  append_history_log(op);
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

static int
enqueue_move_action(const http_request_t *req, gc_action_t action) {
  char title_id[64];
  char usb_id[16];
  char target_root[1024];
  char target_path[1024];
  char err[256] = {0};
  uint64_t target_free = 0;
  gc_game_t game;

  if(!websrv_get_query_arg(req, "titleId", title_id, sizeof(title_id)) ||
     !valid_title_id(title_id)) {
    return serve_error(req, 400, "bad titleId");
  }
  pthread_mutex_lock(&g_gc_lock);
  if(active_op_for_title_locked(title_id) || pending_op_for_title_locked(title_id)) {
    pthread_mutex_unlock(&g_gc_lock);
    return serve_error(req, 409, "action already running for this game");
  }
  pthread_mutex_unlock(&g_gc_lock);
  if(find_game_by_title_ex(title_id, &game, 0) != 0 ||
     game.source_kind == GC_SOURCE_UNKNOWN) {
    return serve_error(req, 404, "game is not mounted");
  }

  if(action == GC_ACTION_MOVE_TO_USB) {
    gc_usb_target_t usb;
    if(!websrv_get_query_arg(req, "usbId", usb_id, sizeof(usb_id)) ||
       usb_target_for_id(usb_id, &usb) != 0) {
      return serve_error(req, 400, "bad usbId");
    }
    snprintf(target_root, sizeof(target_root), "%s", usb.target_root);
    target_free = usb.free_bytes;
  } else if(action == GC_ACTION_MOVE_TO_INTERNAL) {
    snprintf(target_root, sizeof(target_root), "%s", GC_INTERNAL_GAME_ROOT);
    if(path_under_root(game.source_path, "/data")) {
      return serve_error(req, 409, "game is already on internal SSD");
    }
    if(space_for_path(target_root, &target_free, NULL) != 0) {
      return serve_error(req, 500, "internal SSD is unavailable");
    }
  } else {
    return serve_error(req, 400, "bad move action");
  }

  if(build_move_target_path(target_root, game.source_path, target_path,
                            sizeof(target_path), err, sizeof(err)) != 0) {
    return serve_error(req, 400, err[0] ? err : "bad move target");
  }
  if(!strcmp(game.source_path, target_path)) {
    return serve_error(req, 409, "game is already in that location");
  }
  struct stat st;
  if(stat(target_path, &st) == 0) {
    return serve_error(req, 409, "target already exists");
  }
  if(target_free < game.source_size) {
    json_buf_t problem = {0};
    uint64_t extra = game.source_size - target_free;
    if(json_append(&problem,
                   "{\"ok\":false,\"error\":\"not enough free storage\","
                   "\"lowSpace\":true,\"canStreamDelete\":false") != 0 ||
       json_appendf(&problem,
                    ",\"requiredBytes\":%llu,\"freeBytes\":%llu,"
                    "\"extraBytes\":%llu}",
                    (unsigned long long)game.source_size,
                    (unsigned long long)target_free,
                    (unsigned long long)extra) != 0) {
      free(problem.data);
      return -1;
    }
    return serve_owned(req, 409, problem.data, problem.len);
  }

  pthread_mutex_lock(&g_gc_lock);
  if(active_op_for_title_locked(title_id) || pending_op_for_title_locked(title_id)) {
    pthread_mutex_unlock(&g_gc_lock);
    return serve_error(req, 409, "action already running for this game");
  }
  gc_operation_t *op = alloc_op_locked();
  if(!op) {
    pthread_mutex_unlock(&g_gc_lock);
    return serve_error(req, 409, "history is full");
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
  snprintf(op->display_name, sizeof(op->display_name), "%s", game.name);
  snprintf(op->source_path, sizeof(op->source_path), "%s", game.source_path);
  snprintf(op->output_path, sizeof(op->output_path), "%s", target_path);
  snprintf(op->source_kind, sizeof(op->source_kind), "%s",
           source_kind_name(game.source_kind));
  snprintf(op->format, sizeof(op->format), "%s", "pfs");
  snprintf(op->delete_policy, sizeof(op->delete_policy), "%s", "after");
  snprintf(op->target_root, sizeof(op->target_root), "%s", target_root);
  append_history_log(op);
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
  uint64_t bad_blocks_found = op->bad_blocks_found;
  uint64_t hash_mismatched_blocks = 0;
  uint64_t software_compared_blocks = op->software_compared_blocks;
  if(op->status == GC_OP_SUCCESS && op->repaired_blocks > 0) {
    bad_blocks_found = 0;
    software_compared_blocks = 0;
  }
  int ok = json_append(b, "{\"id\":") == 0 &&
      json_string(b, op->id) == 0 &&
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
      json_append(b, ",\"compressionProfile\":") == 0 &&
      json_string(b, compression_profile_or_default(op->compression_profile)) == 0 &&
      json_append(b, ",\"streamOrder\":") == 0 &&
      json_string(b, op->stream_order[0] ? op->stream_order : "budgeted-gain") == 0 &&
      json_append(b, ",\"targetRoot\":") == 0 &&
      json_string(b, op->target_root) == 0 &&
      json_append(b, ",\"repairSummary\":") == 0 &&
      json_string(b, op->repair_summary) == 0 &&
      json_appendf(b,
                   ",\"streamBudgetBytes\":%llu,"
                   "\"createdAt\":%ld,\"startedAt\":%ld,\"endedAt\":%ld,"
                   "\"repairedBlocks\":%llu,\"badBlocksFound\":%llu,"
                   "\"hashCheckedBlocks\":%llu,"
                   "\"hashMismatchedBlocks\":%llu,"
                   "\"softwareComparedBlocks\":%llu}",
                   (unsigned long long)op->stream_budget_bytes,
                   (long)op->created_at, (long)op->started_at,
                   (long)op->ended_at,
                   (unsigned long long)op->repaired_blocks,
                   (unsigned long long)bad_blocks_found,
                   (unsigned long long)op->hash_checked_blocks,
                   (unsigned long long)hash_mismatched_blocks,
                   (unsigned long long)software_compared_blocks) == 0;
  return ok ? 0 : -1;
}

static int
append_game_summary(json_buf_t *b, int *first, const gc_game_t *game,
                    const gc_operation_t *active,
                    const gc_operation_t *pending) {
  if(!b || !first || !game) return -1;
  if(!*first && json_append(b, ",") != 0) return -1;
  uint64_t stream_min = stream_min_free_bytes(game->source_size);
  uint64_t stream_extra = stream_extra_needed(game);
  if(json_append(b, "{\"titleId\":") != 0 ||
     json_string(b, game->title_id) != 0 ||
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
     json_append(b, ",\"validation\":") != 0 ||
     json_string(b, game->validation_status) != 0 ||
     json_append(b, ",\"primaryAction\":") != 0 ||
     json_string(b, game->primary_action) != 0 ||
     json_appendf(b,
                  ",\"sourceSize\":%llu,\"freeBytes\":%llu,"
                  "\"requiredBytes\":%llu,\"extraBytes\":%llu,"
                  "\"streamMinFreeBytes\":%llu,\"streamExtraBytes\":%llu,"
                  "\"compressionSourceSize\":%llu,"
                  "\"compressedSize\":%llu,"
                  "\"savedBytes\":%llu,"
                  "\"sizePending\":%s,"
                  "\"canStreamDelete\":%s,\"hasIcon\":%s,"
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
                  game->size_pending ? "true" : "false",
                  game->can_stream_delete ? "true" : "false",
                  game->has_icon ? "true" : "false",
                  (unsigned long long)game->icon_size,
                  (unsigned long long)game->icon_mtime,
                  game->output_exists ? "true" : "false",
                  game->source_kind != GC_SOURCE_UNKNOWN ? "true" : "false",
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
game_list_has_title(const gc_game_t *games, size_t count,
                    const char *title_id) {
  if(!games || !title_id || !title_id[0]) return 0;
  for(size_t i = 0; i < count; i++) {
    if(!strcmp(games[i].title_id, title_id)) return 1;
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
	      op->action == GC_ACTION_VALIDATE_REPAIR ||
	      op->action == GC_ACTION_VALIDATE_ONLY)) {
    game->source_kind = GC_SOURCE_COMPRESSED;
  }
  snprintf(game->source_path, sizeof(game->source_path), "%s",
           op->source_path[0] ? op->source_path : op->output_path);
  snprintf(game->output_path, sizeof(game->output_path), "%s",
           op->output_path);
  populate_game_size(game, 1);
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
  int n;
  if(!out || out_size == 0) return -1;
  history_base_id(id, base, sizeof(base));
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

  char line[4096];
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
  gc_game_t games[GC_MAX_GAMES];
  size_t count = 0;
  int first = 1;
  int failed = 0;
  discover_games(games, GC_MAX_GAMES, &count, 0);

  json_buf_t b = {0};
  if(json_append(&b, "{\"ok\":true,\"games\":[") != 0) {
    free(b.data);
    return -1;
  }
  pthread_mutex_lock(&g_gc_lock);
  for(size_t i = 0; i < count; i++) {
    gc_operation_t *active = active_op_for_title_locked(games[i].title_id);
    gc_operation_t *pending = pending_op_for_title_locked(games[i].title_id);
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
       game_list_has_title(games, count, op->title_id)) {
      continue;
    }
    for(size_t j = 0; j < i; j++) {
      if(g_ops[j].used &&
         (g_ops[j].status == GC_OP_RUNNING ||
          g_ops[j].status == GC_OP_QUEUED) &&
         !strcmp(g_ops[j].title_id, op->title_id)) {
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
    return -1;
  }
  if(json_append(&b, "]}") != 0) {
    free(b.data);
    return -1;
  }
  return serve_owned(req, 200, b.data, b.len);
}

static int
usb_request(const http_request_t *req) {
  (void)req;
  gc_usb_target_t targets[GC_USB_COUNT];
  size_t count = 0;
  discover_usb_targets(targets, GC_USB_COUNT, &count);

  json_buf_t b = {0};
  if(json_append(&b, "{\"ok\":true,\"usb\":[") != 0) {
    free(b.data);
    return -1;
  }
  for(size_t i = 0; i < count; i++) {
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
       json_string(&b, targets[i].target_root) != 0 ||
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
    if(compressed_output > 0) {
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
  } else if(!strcmp(v, "move-to-usb") ||
            !strcmp(v, "move-to-internal")) {
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
  int rollback_requested = atomic_load(&g_job.rollback_requested) != 0;
  char current[512], phase[32], verb[16], err[256], active_id[32] = {0};
  time_t started_at = 0;
  long elapsed_seconds = 0;
  long speed_metric_bytes = 0;
  long speed_bytes_per_second = 0;
  const char *speed_source = "none";

  pthread_mutex_lock(&g_job.lock);
  snprintf(current, sizeof(current), "%s", g_job.current);
  snprintf(phase, sizeof(phase), "%s", g_job.phase);
  snprintf(verb, sizeof(verb), "%s", g_job.verb);
  snprintf(err, sizeof(err), "%s", g_job.error);
  started_at = g_job.started_at;
  pthread_mutex_unlock(&g_job.lock);

  if(busy && started_at > 0) {
    time_t now = time(NULL);
    if(now > started_at) elapsed_seconds = (long)(now - started_at);
  }
  pthread_mutex_lock(&g_gc_lock);
  gc_operation_t *active = active_op_locked();
  if(active) snprintf(active_id, sizeof(active_id), "%s", active->id);
  pthread_mutex_unlock(&g_gc_lock);

  speed_metric_bytes = job_speed_metric_bytes(
      verb, phase, copied, compressed_output, repair_read_bytes,
      repair_written_bytes, repair_copy_bytes, &speed_source);
  if(elapsed_seconds > 0 && speed_metric_bytes > 0) {
    speed_bytes_per_second = speed_metric_bytes / elapsed_seconds;
  }
  hash_mismatched = 0;

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
    rollback_requested = 0;
    current[0] = 0;
    phase[0] = 0;
    verb[0] = 0;
    err[0] = 0;
    active_id[0] = 0;
    started_at = 0;
    elapsed_seconds = 0;
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
	                  "\"rollbackRequested\":%s,"
		                  "\"startedAt\":%ld,"
		                  "\"elapsedSeconds\":%ld,"
		                  "\"speedMetricBytes\":%ld,"
		                  "\"speedBytesPerSec\":%ld,"
		                  "\"compressBytesPerSecond\":%ld,"
		                  "\"speedSource\":",
		                  total, copied, compressed_output,
		                  total_blocks, done_blocks, raw_blocks,
	                  compressed_blocks, skipped_zlib_blocks,
	                  phase_step, phase_count,
                  bad_blocks_found, bad_blocks_found, repaired_blocks,
                  hash_checked, hash_matched, hash_mismatched,
                  software_compared,
	                  writer_wait_us, worker_wait_us, repair_read_bytes,
	                  repair_written_bytes, repair_copy_bytes,
	                  stream_min_free, stream_budget, stream_credit,
	                  stream_deleted, stream_reverse_temp,
	                  stream_forward_files, stream_reverse_files,
		                  destructive_stream_active ? "true" : "false",
		                  rollback_requested ? "true" : "false",
		                  (long)started_at, elapsed_seconds,
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
    resumable = history_action_phase_is_resumable(action, phase);
    if(active->action == GC_ACTION_COMPRESS &&
       !strcmp(active->delete_policy, "stream") &&
       !strcmp(phase, "compressing") &&
       active->output_path[0]) {
      char journal_path[1024];
      struct stat st;
      if(pfs_compress_stream_journal_path(active->output_path, journal_path,
                                          sizeof(journal_path)) == 0 &&
         stat(journal_path, &st) == 0 && S_ISREG(st.st_mode)) {
        resumable = 1;
      }
    }
    if(active->action == GC_ACTION_VALIDATE_REPAIR ||
       active->action == GC_ACTION_VALIDATE_ONLY) {
      resumable = 1;
    }
    busy = 1;
    reason = resumable ? "active work can resume automatically" :
        "active work is not resumable";
  } else if(job_busy) {
    snprintf(action, sizeof(action), "%s", "unknown");
    snprintf(phase, sizeof(phase), "%s", job_phase);
    busy = 1;
    reason = "active job is not attached to a resumable operation";
  } else if(pending_count > 0) {
    busy = 1;
    resumable = 1;
    reason = "pending work can resume automatically";
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
  int destructive_stream_active =
      atomic_load(&g_job.destructive_stream_active) != 0;
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
  append_history_log(op);
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

  FILE *f = fopen(GC_HISTORY_LOG, "r");
  if(!f) return;

  typedef struct recovery_slot {
    char key[GC_HISTORY_KEY_SIZE];
    gc_history_recovery_event_t ev;
    uint64_t order;
    uint64_t seq;
  } recovery_slot_t;

  recovery_slot_t latest[GC_MAX_OPS];
  size_t count = 0;
  uint64_t order = 1;
  char line[4096];
  memset(latest, 0, sizeof(latest));
  while(fgets(line, sizeof(line), f)) {
    gc_history_recovery_event_t ev;
    char key[GC_HISTORY_KEY_SIZE];
    uint64_t seq;
    int slot = -1;
    line[strcspn(line, "\r\n")] = 0;
    if(recovery_event_from_history_line(line, &ev) != 0) continue;
    if(history_operation_key(ev.id, (uint64_t)ev.created_at,
                             key, sizeof(key)) != 0) {
      continue;
    }
    seq = operation_seq_from_id(ev.id);
    if(seq >= g_next_seq) g_next_seq = seq + 1;
    for(size_t i = 0; i < count; i++) {
      if(!strcmp(latest[i].key, key)) {
        slot = (int)i;
        break;
      }
    }
    if(slot < 0) {
      if(count >= GC_MAX_OPS) continue;
      slot = (int)count++;
      snprintf(latest[slot].key, sizeof(latest[slot].key), "%s", key);
    }
    latest[slot].ev = ev;
    latest[slot].order = order++;
    latest[slot].seq = seq;
  }
  fclose(f);

  if(!count) return;
  gc_checkpoint("history recovery scan");
  for(size_t emitted = 0; emitted < count; emitted++) {
    int best = -1;
    uint64_t best_seq = UINT64_MAX;
    uint64_t best_order = UINT64_MAX;
    for(size_t i = 0; i < count; i++) {
      if(latest[i].order == 0) continue;
      uint64_t seq = latest[i].seq ? latest[i].seq : UINT64_MAX;
      if(seq < best_seq || (seq == best_seq && latest[i].order < best_order)) {
        best = (int)i;
        best_seq = seq;
        best_order = latest[i].order;
      }
    }
    if(best < 0) break;
    latest[best].order = 0;
    gc_history_recovery_event_t *ev = &latest[best].ev;
    if(history_status_is_pending(ev->status)) {
      (void)recovery_restore_queued_op(ev);
      continue;
    }
    if(strcmp(ev->status, "running")) {
      continue;
    }
    if(history_action_phase_is_resumable(ev->action, ev->phase) ||
       history_phase_is_recoverable(ev->phase)) {
      gc_log("recovery processing id=%s title=%s action=%s phase=%s output=%s",
             ev->id, ev->title_id, ev->action, ev->phase, ev->output_path);
      (void)recovery_process_history_event(ev);
    } else {
      gc_log("recovery cancelling non-resumable id=%s title=%s action=%s phase=%s",
             ev->id, ev->title_id, ev->action, ev->phase);
      recovery_append_terminal_history(
          ev, GC_OP_CANCELLED, "cancelled",
          "recovered non-resumable operation after restart");
    }
  }
}

int
gc_api_request(const http_request_t *req, const char *url) {
  (void)url;
  if(!strcmp(req->path, "/api/gc/games")) return games_request(req);
  if(!strcmp(req->path, "/api/gc/usb")) return usb_request(req);
  if(!strcmp(req->path, "/api/gc/history")) return history_request(req);
  if(!strcmp(req->path, "/api/gc/job")) return job_request(req);
  if(!strcmp(req->path, "/api/gc/bad-blocks")) return bad_blocks_request(req);
  if(!strcmp(req->path, "/api/gc/job/cancel")) return cancel_active_request(req);
  if(!strcmp(req->path, "/api/gc/queue/cancel")) return cancel_queued_request(req);
  if(!strcmp(req->path, "/api/gc/compress")) {
    return enqueue_action(req, GC_ACTION_COMPRESS);
  }
  if(!strcmp(req->path, "/api/gc/uncompress")) {
    return enqueue_action(req, GC_ACTION_UNCOMPRESS);
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
  return serve_error(req, 404, "not found");
}
