/*
 * File Explorer - streamed PFS/PFSC app compression.
 */

#pragma once

#include <stddef.h>
#include <stdint.h>

typedef struct pfs_app_info {
  char title_id[64];
  char source_path[1024];
  char output_path[1024];
  char nested_name[256];
  uint64_t nested_size;
  uint64_t stored_size;
  int  format;
  int  nested_type;
  int  source_type;
  int  delete_policy;
  int  compression_profile;
  int  output_exists;
  uint64_t stream_budget_bytes;
  uint64_t stream_reserve_bytes;
  uint64_t stream_forward_files;
  uint64_t stream_reverse_files;
  uint64_t scan_bytes;
  uint64_t scan_files;
  uint64_t scan_dirs;
  uint64_t scan_entries;
  uint64_t scan_elapsed_ms;
  uint64_t scan_workers;
} pfs_app_info_t;

typedef struct pfs_compress_plan pfs_compress_plan_t;

typedef struct pfs_decompress_info {
  char source_path[1024];
  char output_path[1024];
  char nested_name[256];
  uint64_t nested_size;
  int  nested_type;
  int  delete_policy;
  int  output_exists;
} pfs_decompress_info_t;

#define PFS_COMPRESS_FORMAT_PFS   0
#define PFS_COMPRESS_FORMAT_EXFAT 1

#define PFS_COMPRESS_SOURCE_UNKNOWN 0
#define PFS_COMPRESS_SOURCE_APP     1
#define PFS_COMPRESS_SOURCE_IMAGE   2

#define PFS_DELETE_KEEP   0
#define PFS_DELETE_AFTER  1
#define PFS_DELETE_STREAM 2

#define PFS_COMPRESS_PROFILE_SPACE 1
#define PFS_COMPRESS_PROFILE_FAST  2

#define PFS_STREAM_DEFAULT_BUDGET_BYTES 1073741824ULL
#define PFS_STREAM_ORDER_PATH          0
#define PFS_STREAM_ORDER_BUDGETED_GAIN 1

typedef struct pfs_stream_options {
  uint64_t budget_bytes;
  uint64_t reserve_bytes;
  int order;
} pfs_stream_options_t;

#define PFS_NESTED_UNKNOWN 0
#define PFS_NESTED_PFS     1
#define PFS_NESTED_EXFAT   2

#define PFS_COMPRESS_DEFAULT_WORKERS 4
#define PFS_COMPRESS_MAX_WORKERS 64
#define PFS_DECOMPRESS_DEFAULT_WORKERS 12

int pfs_app_probe(const char *path, pfs_app_info_t *info,
                  char *err, size_t err_size);

int pfs_image_probe(const char *path, pfs_app_info_t *info,
                    char *err, size_t err_size);

int pfs_compress_source_to_ffpfsc_opts_profile_output_ex(
                                      const char *path, int overwrite,
                                      int workers, int format,
                                      int delete_policy,
                                      int compression_profile,
                                      const char *output_path,
                                      const pfs_stream_options_t *stream_opts,
                                      pfs_app_info_t *info,
                                      char *err, size_t err_size);
int pfs_compress_prepare_source_to_ffpfsc_opts_profile_output_ex(
                                      const char *path, int overwrite,
                                      int format,
                                      int delete_policy,
                                      int compression_profile,
                                      const char *output_path,
                                      const pfs_stream_options_t *stream_opts,
                                      pfs_compress_plan_t **plan_out,
                                      pfs_app_info_t *info,
                                      char *err, size_t err_size);
int pfs_compress_execute_prepared_to_ffpfsc(pfs_compress_plan_t *plan,
                                            int workers,
                                            pfs_app_info_t *info,
                                            char *err, size_t err_size);
void pfs_compress_plan_free(pfs_compress_plan_t *plan);

int pfs_decompress_probe(const char *path, pfs_decompress_info_t *info,
                         char *err, size_t err_size);

int pfs_decompress_detect_nested(const char *path, pfs_decompress_info_t *info,
                                 char *err, size_t err_size);

int pfs_decompress_probe_image(const char *path, pfs_decompress_info_t *info,
                               char *err, size_t err_size);

int pfs_decompress_ffpfsc_to_app_opts_output(const char *path, int overwrite,
                                      int workers, int delete_policy,
                                      const char *output_path,
                                      pfs_decompress_info_t *info,
                                      char *err, size_t err_size);

int pfs_decompress_ffpfsc_to_image_opts_output(const char *path, int overwrite,
                                        int workers, int delete_policy,
                                        const char *output_path,
                                        pfs_decompress_info_t *info,
                                        char *err, size_t err_size);
