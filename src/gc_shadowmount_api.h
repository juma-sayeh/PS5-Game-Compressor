/*
 * Game Compressor - ShadowMountPlus HTTP API client (OpenAPI v1).
 *
 * Talks to the ShadowMountPlus payload REST API documented in the
 * OpenAPI specification at:
 *
 *   https://github.com/drakmor/ShadowMountPlus/blob/1.7/docs/openapi.yaml
 *
 * ShadowMount / ShadowMountPlus author: Drakmor
 *   https://github.com/drakmor/ShadowMountPlus
 *
 * Server: http://127.0.0.1:10101 (default loopback endpoint on the PS5).
 *
 * On startup gc_shadowmount_api_probe() checks whether the new
 * API-capable version is reachable and, if so, flips an internal flag
 * that the rest of the codebase can query with
 * gc_shadowmount_api_available(). All endpoint helpers below are no-ops
 * (return -1) while that flag is cleared, so callers can unconditionally
 * prefer the new API and transparently fall back to the legacy
 * file-based hint interface (gc_shadowmount.*).
 */
#ifndef GC_SHADOWMOUNT_API_H
#define GC_SHADOWMOUNT_API_H

#include <stddef.h>

/*
 * Field capacity constants for the structs below. Sized to comfortably
 * hold the maximum lengths emitted by the ShadowMountPlus OpenAPI
 * schema so that over-long server values truncate safely instead of
 * overflowing the fixed-size char buffers.
 */
#define GC_SM_PATH_LEN 1024          /* absolute filesystem path */
#define GC_SM_MOUNT_POINT_LEN 512    /* mount-point path (e.g. /mnt/...) */
#define GC_SM_TITLE_NAME_LEN 256     /* human-readable game title */
#define GC_SM_TITLE_ID_LEN 16        /* title id, e.g. "CUSA12345" + NUL */
#define GC_SM_BACKEND_LEN 16         /* image backend id */
#define GC_SM_VERSION_LEN 64         /* ShadowMountPlus version string */
#define GC_SM_CAP_LEN 32             /* single capability token */
#define GC_SM_MAX_CAPS 18            /* max capabilities reported by /version */
#define GC_SM_RUNTIME_PATH_LEN 1024 /* app/runtime root path */
#define GC_SM_SOURCE_TYPE_LEN 16    /* source type token */
#define GC_SM_IMAGE_TYPE_LEN 16     /* nested image type token */
#define GC_SM_PLATFORM_LEN 16       /* platform token (e.g. "ps5") */
#define GC_SM_CONTENT_ID_LEN 64     /* PS5 content id */
#define GC_SM_TIME_LEN 48           /* ISO-style timestamp string */
#define GC_SM_ICON_URL_LEN 256      /* icon URL (may be empty) */
#define GC_SM_MODE_LEN 8            /* mount mode: "ro"/"rw"/"r/o"/"r/w" */
#define GC_SM_FILESYSTEM_LEN 32     /* filesystem name (e.g. exfat/ufs) */
#define GC_SM_MAX_MOUNTS 32          /* max storage mounts returned by /storage */
#define GC_SM_OPERATION_LEN 16      /* storage job operation token */
#define GC_SM_STATE_LEN 24          /* storage job state token */
#define GC_SM_ERROR_LEN 256         /* storage job result_error string */
#define GC_SM_MAX_SCAN_PATHS 64     /* max scan roots in settings */
#define GC_SM_MAX_MANUAL_PATHS 256  /* max manual.lst entries */
#define GC_SM_LOG_CONTENT_MAX (256 * 1024) /* max debug/kernel log tail */

/*
 * Image record returned by POST /api/v1/images (ImagesResponse.images).
 * Mirrors the OpenAPI Image schema. One entry per source image SM knows
 * about (regardless of whether it is currently mounted).
 */
typedef struct {
  char path[GC_SM_PATH_LEN];           /* absolute path of the image file */
  char mount_point[GC_SM_MOUNT_POINT_LEN]; /* path the image is mapped to */
  long long size;                      /* image size in bytes */
  long long mtime_sec;                 /* image mtime, seconds since epoch */
  int mtime_nsec;                      /* image mtime, nanoseconds component */
  int unit_id;                         /* backend unit id, -1 if unknown */
  char backend[GC_SM_BACKEND_LEN];     /* backend name (e.g. pfsc/exfat) */
  int complete;                        /* 1 if the image is fully built */
  int source_available;                /* 1 if the source file still exists */
  int mapped;                          /* 1 if currently mapped to a unit */
  int mounted;                         /* 1 if currently mounted for use */
} gc_sm_image_t;

/*
 * Game record returned by POST /api/v1/games (GamesResponse.games).
 * Mirrors the OpenAPI Game schema: one entry per title SM manages.
 * size_bytes / size_status are only populated when the request passes
 * include_size=true (directory traversal is opt-in and can be slow).
 */
typedef struct {
  char path[GC_SM_PATH_LEN];           /* registered source image path */
  char runtime_path[GC_SM_RUNTIME_PATH_LEN]; /* app/runtime root when mounted */
  char source_type[GC_SM_SOURCE_TYPE_LEN]; /* "folder"/"image"/"compressed" */
  char image_type[GC_SM_IMAGE_TYPE_LEN]; /* nested image type */
  char platform[GC_SM_PLATFORM_LEN];   /* platform token (e.g. "ps5") */
  char title_id[GC_SM_TITLE_ID_LEN];   /* title id (CUSA/PPSA/...) */
  char content_id[GC_SM_CONTENT_ID_LEN]; /* PS5 content id */
  char title_name[GC_SM_TITLE_NAME_LEN]; /* human-readable title */
  char last_access_time[GC_SM_TIME_LEN]; /* last-access timestamp */
  char install_time[GC_SM_TIME_LEN];   /* install timestamp */
  char icon_url[GC_SM_ICON_URL_LEN];   /* icon URL (may be empty) */
  long long app_db_size_bytes;         /* app.db size in bytes */
  int installed;                       /* 1 if recorded in app.db */
  int managed;                         /* 1 if SM manages this title */
  int mounted;                         /* 1 if currently mounted */
  int image_backed;                    /* 1 if backed by an image file */
  int source_available;                /* 1 if the source file still exists */
  long long size_bytes;                /* on-disk size (include_size only) */
  int size_status;                     /* SM size status (include_size only) */
  int status;                          /* SM game status code */
} gc_sm_game_t;

/*
 * Version info returned by POST /api/v1/version (VersionResponse).
 * Also filled by the startup probe gc_shadowmount_api_probe().
 */
typedef struct {
  char shadowmount_version[GC_SM_VERSION_LEN]; /* SM payload version string */
  int api_version;                      /* OpenAPI protocol version number */
  char capabilities[GC_SM_MAX_CAPS][GC_SM_CAP_LEN]; /* advertised capability tokens */
  int capability_count;                 /* number of capabilities populated */
} gc_sm_version_t;

/*
 * Storage mount entry (StorageMount schema). One per mounted filesystem
 * returned by POST /api/v1/storage.
 */
typedef struct {
  char source[GC_SM_PATH_LEN];            /* device source path */
  char mount_point[GC_SM_MOUNT_POINT_LEN]; /* mount point path */
  char filesystem[GC_SM_FILESYSTEM_LEN];  /* filesystem type */
  long long total_bytes;                  /* total capacity */
  long long free_bytes;                   /* free blocks */
  long long available_bytes;              /* available to unprivileged user */
  long long used_bytes;                   /* used bytes */
  int read_only;                          /* 1 if mounted read-only */
} gc_sm_storage_mount_t;

/*
 * Storage space response (StorageSpaceResponse). Returned by
 * POST /api/v1/storage. mounts[0..count-1] are populated.
 */
typedef struct {
  int status;                             /* 0 on success */
  int count;                              /* number of mounts populated */
  gc_sm_storage_mount_t mounts[GC_SM_MAX_MOUNTS];
} gc_sm_storage_space_t;

/*
 * Storage job (StorageJobResponse). Returned by the move/copy/delete/
 * unpack/status/cancel endpoints. Represents the current or last
 * asynchronous storage operation.
 */
typedef struct {
  int status;                             /* 0 on success */
  long long job_id;                       /* server-assigned job id */
  char operation[GC_SM_OPERATION_LEN];    /* ""/"move"/"copy"/"delete"/"unpack" */
  char state[GC_SM_STATE_LEN];           /* idle/preparing/.../completed/failed */
  int active;                             /* 1 if job currently running */
  int cancellable;                        /* 1 if job can still be cancelled */
  int cancel_requested;                   /* 1 if cancellation was requested */
  char title_id[GC_SM_TITLE_ID_LEN];     /* affected title id */
  char source_type[GC_SM_SOURCE_TYPE_LEN]; /* ""/"folder"/"image" */
  char source[GC_SM_PATH_LEN];           /* original source path */
  char runtime_source[GC_SM_RUNTIME_PATH_LEN]; /* runtime source path */
  char destination[GC_SM_PATH_LEN];      /* destination dir (empty for delete) */
  int delete_source;                      /* 1 for unpack jobs that delete image */
  long long total_bytes;                  /* total bytes to transfer */
  long long processed_bytes;             /* bytes processed so far */
  long long total_files;                  /* total files to transfer */
  long long processed_files;             /* files processed so far */
  double progress_percent;               /* 0.0 - 100.0 */
  long long speed_bytes_per_second;      /* average byte rate */
  long long elapsed_ms;                  /* elapsed milliseconds */
  int affected_titles;                    /* number of affected titles */
  int result_status;                      /* errno-style result (0=ok) */
  char result_error[GC_SM_ERROR_LEN];    /* error description */
  int scan_queued;                        /* 1 if scan was queued after job */
} gc_sm_storage_job_t;

/*
 * Manual update response (ManualUpdateResponse). Returned by
 * POST /api/v1/manual/add and /api/v1/manual/remove.
 */
typedef struct {
  int status;                             /* 0 on success */
  char path[GC_SM_PATH_LEN];             /* the path that was added/removed */
  int present;                            /* 1 if path is now in manual.lst */
  int changed;                            /* 1 if manual.lst was modified */
} gc_sm_manual_update_t;

/*
 * Settings snapshot (SettingsResponse minus scan_paths, which are
 * returned separately via output parameters). Returned by
 * POST /api/v1/settings.
 */
typedef struct {
  int status;                             /* 0 on success */
  int debug;                              /* debug logging enabled */
  int quiet_mode;                         /* quiet mode enabled */
  int update_emulators;                   /* auto-update emulators enabled */
  int auto_update_ampr;                   /* auto-update libSceAmpr enabled */
  int auto_remove_missing_games;         /* remove apps with missing source */
  int auto_remove_missing_delay_seconds; /* delay before removal */
  int allow_lan_access;                   /* bind to 0.0.0.0 when true */
  int fan_target_temperature;             /* 0=system, 50-91 degrees C */
  int api_enabled;                        /* HTTP API enabled in config.ini */
  int scan_path_count;                    /* number of scan roots */
} gc_sm_settings_t;

/*
 * Log tail response (DebugLogResponse / KernelLogResponse). The
 * content field is malloc'd and must be freed by the caller.
 */
typedef struct {
  int status;                             /* 0 on success */
  long long file_size;                    /* total file size (debug-log) */
  long long total_bytes;                  /* total bytes (kernel-log) */
  int returned_bytes;                     /* bytes returned in content */
  int truncated;                          /* 1 if content was truncated */
  char *content;                          /* malloc'd log text, NUL-terminated */
} gc_sm_log_response_t;

/*
 * Probe http://127.0.0.1:10101/api/v1/version at startup. On a
 * successful 2xx answer with a zero JSON status the internal "new API
 * available" flag is set and the version info is optionally returned in
 * `out`. Returns 0 when the new ShadowMount API answered, -1 otherwise.
 */
int gc_shadowmount_api_probe(gc_sm_version_t *out,
                             char *err, size_t err_size);

/*
 * Returns 1 if the ShadowMountPlus HTTP API (introduced in
 * ShadowMount 1.7+) is available, 0 otherwise. The flag is set by
 * gc_shadowmount_api_probe() on a successful startup probe and stays
 * set until process exit. While 0, all endpoint helpers below are
 * no-ops (return -1).
 */
int gc_shadowmount_api_available(void);

/* POST /api/v1/version -> VersionResponse */
int gc_shadowmount_api_get_version(gc_sm_version_t *out,
                                   char *err, size_t err_size);

/* POST /api/v1/images -> ImagesResponse. Fills `out[0..*count-1]`. */
int gc_shadowmount_api_list_images(gc_sm_image_t *out, int max_count,
                                   int *count,
                                   char *err, size_t err_size);

/*
 * POST /api/v1/images and return the record of the image registered at
 * `path`.  Returns 1 when found (out populated), 0 when no image at that
 * path is registered, -1 on error.
 */
int gc_shadowmount_api_find_image(const char *path,
                                  gc_sm_image_t *out,
                                  char *err, size_t err_size);

/*
 * POST /api/v1/games -> GamesResponse. Fills `out[0..*count-1]`.
 * Pass include_size != 0 to send GamesListRequest.include_size so the
 * server populates size_bytes/size_status (directory traversal can be
 * slow, so it is opt-in per the OpenAPI schema).
 */
int gc_shadowmount_api_list_games(gc_sm_game_t *out, int max_count,
                                  int *count, int include_size,
                                  char *err, size_t err_size);

/*
 * POST /api/v1/games/mount with MountRequest {title_id, mode?}.
 * mode may be NULL (omit), "ro", "rw", "r/o" or "r/w" per the
 * OpenAPI MountRequest.mode enum.
 *
 * The mount is resolved BY TITLE ONLY: the request carries no
 * source_path, so SM mounts whatever image it has registered for the
 * title. A title is only re-discovered through SM's own scan; if SM has
 * not registered the requested source the mount fails with HTTP 404
 * "No such file or directory" (handled/registered via the legacy
 * manual.lst fallback in gc_shadowmount_request_title_source_scan()).
 * SM supports a single active mount at a time, so mounting a second
 * title returns 409 ("Device busy") until the first is unmounted.
 * Returns 0 on success, -1 on error (err filled).
 */
int gc_shadowmount_api_mount_game(const char *title_id,
                                  char *err, size_t err_size);
/*
 * Same as gc_shadowmount_api_mount_game() but sends an explicit
 * MountRequest.mode. Pass NULL to omit the field (server default),
 * or one of "ro"/"rw"/"r/o"/"r/w" per the OpenAPI MountRequest.mode
 * enum. gc_shadowmount_api_mount_game() is a thin wrapper that calls
 * this with mode = NULL.
 */
int gc_shadowmount_api_mount_game_mode(const char *title_id,
                                        const char *mode,
                                        char *err, size_t err_size);

/* POST /api/v1/games/unmount with TitleRequest {title_id}.
 * Returns 0 on success, -1 on error (err filled). */
int gc_shadowmount_api_unmount_game(const char *title_id,
                                       char *err, size_t err_size);

/*
 * Query /api/v1/games and return the title_id of the first currently
 * mounted game. Returns 1 when a mounted game was found (title_id_out
 * populated), 0 when none is mounted, -1 on error.
 */
int gc_shadowmount_api_find_mounted_game(char *title_id_out,
                                          size_t title_id_size,
                                          char *err, size_t err_size);

/*
 * POST /api/v1/scan -> forces SM to re-scan all registered sources and
 * clear its internal cached metadata.  Call this after modifying an
 * image file (.ffpfsc/.exfat) so SM drops stale block-offset/size
 * caches before the next mount attempt.  Returns 0 on success, -1 on
 * error (err filled).
 */
int gc_shadowmount_api_scan(char *err, size_t err_size);

/*
 * POST /api/v1/scan with ScanRequest {reset_attempts}. Same as
 * gc_shadowmount_api_scan() but optionally resets all title
 * install/mount and image-mount retry counters immediately before
 * the requested scan starts. Returns 0 on success, -1 on error.
 */
int gc_shadowmount_api_scan_reset(int reset_attempts,
                                   char *err, size_t err_size);

/* POST /api/v1/storage -> StorageSpaceResponse. */
int gc_shadowmount_api_get_storage_space(gc_sm_storage_space_t *out,
                                          char *err, size_t err_size);

/*
 * POST /api/v1/manual/list -> ManualListResponse. Fills
 * paths_out[0..*count-1]. Returns 0 on success, -1 on error.
 */
int gc_shadowmount_api_list_manual_sources(
    char (*paths_out)[GC_SM_PATH_LEN], int max_paths, int *count,
    char *err, size_t err_size);

/* POST /api/v1/manual/add with ManualPathRequest {path}. */
int gc_shadowmount_api_add_manual_source(const char *path,
                                          gc_sm_manual_update_t *out,
                                          char *err, size_t err_size);

/* POST /api/v1/manual/remove with ManualPathRequest {path}. */
int gc_shadowmount_api_remove_manual_source(const char *path,
                                             gc_sm_manual_update_t *out,
                                             char *err, size_t err_size);

/*
 * POST /api/v1/games/info with TitleRequest {title_id} ->
 * GameInfoResponse. Fills *out with detailed metadata and physical
 * source size. Returns 0 on success, -1 on error.
 */
int gc_shadowmount_api_get_game_info(const char *title_id,
                                      gc_sm_game_t *out,
                                      char *err, size_t err_size);

/*
 * GET /api/v1/games/icon?title_id=<id>[&size=thumb] -> PNG binary.
 * Fetches the game icon (or 128x128 thumbnail when want_thumb != 0)
 * from the ShadowMount API. Returns the PNG bytes as a malloc'd
 * buffer in *data_out (caller frees) with *size_out bytes.
 * Returns 0 on success, -1 on error.
 */
int gc_shadowmount_api_get_game_icon(const char *title_id, int want_thumb,
                                      unsigned char **data_out,
                                      size_t *size_out,
                                      char *err, size_t err_size);

/*
 * POST /api/v1/settings -> SettingsResponse. Fills *out with current
 * runtime settings. If scan_paths_out is non-NULL, up to max_scan_paths
 * scan roots are written to scan_paths_out[0..*scan_path_count-1].
 * Returns 0 on success, -1 on error.
 */
int gc_shadowmount_api_get_settings(gc_sm_settings_t *out,
                                     char (*scan_paths_out)[GC_SM_PATH_LEN],
                                     int max_scan_paths, int *scan_path_count,
                                     char *err, size_t err_size);

/*
 * POST /api/v1/settings/update with SettingsUpdateRequest.
 * Atomically updates web-managed config.ini keys. scan_paths is an
 * array of scan_path_count path strings (may be NULL/0 to restore
 * defaults). Returns 0 on success, -1 on error.
 */
int gc_shadowmount_api_update_settings(const gc_sm_settings_t *settings,
                                        const char *const *scan_paths,
                                        int scan_path_count,
                                        char *err, size_t err_size);

/*
 * POST /api/v1/debug-log -> DebugLogResponse. Reads a bounded tail of
 * debug.log. max_bytes clamped to [4096, 262144], 0 uses default 131072.
 * out->content is malloc'd (caller frees). Returns 0 on success.
 */
int gc_shadowmount_api_get_debug_log(int max_bytes,
                                      gc_sm_log_response_t *out,
                                      char *err, size_t err_size);

/*
 * POST /api/v1/kernel-log -> KernelLogResponse. Reads recent SDK log
 * events. max_bytes clamped to [4096, 262144], 0 uses default 131072.
 * out->content is malloc'd (caller frees). Returns 0 on success.
 */
int gc_shadowmount_api_get_kernel_log(int max_bytes,
                                       gc_sm_log_response_t *out,
                                       char *err, size_t err_size);

/*
 * POST /api/v1/games/move with StorageDestinationRequest
 * {title_id, destination_dir}. Accepts an async move job (202).
 * Optional *out receives the job snapshot. Returns 0 on success.
 */
int gc_shadowmount_api_move_game_source(const char *title_id,
                                         const char *destination_dir,
                                         gc_sm_storage_job_t *out,
                                         char *err, size_t err_size);

/* POST /api/v1/games/copy with StorageDestinationRequest. See move. */
int gc_shadowmount_api_copy_game_source(const char *title_id,
                                         const char *destination_dir,
                                         gc_sm_storage_job_t *out,
                                         char *err, size_t err_size);

/*
 * POST /api/v1/games/unpack with StorageUnpackRequest
 * {title_id, destination_dir, delete_source?}. Mounts an image
 * read-only and copies its game tree into a folder. Returns 0 on
 * success.
 */
int gc_shadowmount_api_unpack_game_image(const char *title_id,
                                          const char *destination_dir,
                                          int delete_source,
                                          gc_sm_storage_job_t *out,
                                          char *err, size_t err_size);

/*
 * POST /api/v1/games/storage/status with StorageJobStatusRequest
 * {job_id?}. When has_job_id is 0, returns the current or most
 * recently completed job. Returns 0 on success.
 */
int gc_shadowmount_api_get_storage_job_status(long long job_id,
                                               int has_job_id,
                                               gc_sm_storage_job_t *out,
                                               char *err, size_t err_size);

/* POST /api/v1/games/storage/cancel with StorageJobCancelRequest
 * {job_id}. Cancels an active storage job. Returns 0 on success. */
int gc_shadowmount_api_cancel_storage_job(long long job_id,
                                           gc_sm_storage_job_t *out,
                                           char *err, size_t err_size);

/*
 * POST /api/v1/games/delete with StorageDeleteRequest
 * {title_id, confirm:true}. Accepts an async delete job (202).
 * Optional *out receives the job snapshot. Returns 0 on success.
 */
int gc_shadowmount_api_delete_game_source(const char *title_id,
                                           gc_sm_storage_job_t *out,
                                           char *err, size_t err_size);

/*
 * POST /api/v1/games/uninstall with TitleRequest {title_id}.
 * Requests game uninstallation via AppInstUtil. Returns 0 on
 * success, -1 on error.
 */
int gc_shadowmount_api_uninstall_game(const char *title_id,
                                       char *err, size_t err_size);

#endif
