/*
 * File Explorer - PFSC post-mount repair.
 */

#pragma once

#include <stddef.h>
#include <stdint.h>

typedef struct pfs_repair_info {
  char title_id[64];
  char source_path[1024];
  char mounted_path[1024];
  char nested_name[256];
  char output_path[1024];
  char temp_path[1024];
  char outdir[1024];
  char journal_path[1024];
  char storage_error[256];
  char hash_mode[16];
  uint64_t source_size;
  uint64_t required_free_bytes;
  uint64_t available_free_bytes;
  uint64_t logical_size;
  uint64_t nested_size;
  uint64_t block_count;
  uint64_t scanned_blocks;
  uint64_t matched_blocks;
  uint64_t repaired_blocks;
  uint64_t hash_checked_blocks;
  uint64_t hash_matched_blocks;
  uint64_t hash_mismatched_blocks;
  uint64_t software_compared_blocks;
  uint64_t post_verify_blocks;
  uint64_t post_verify_mount_blocks;
  uint64_t old_stored_size;
  uint64_t new_stored_size;
  uint64_t bytes_moved;
  int nested_type;
  int repair_mode;
  int storage_check_ok;
  int noop;
  int resumed;
} pfs_repair_info_t;

#define PFS_REPAIR_MODE_INPLACE 1
#define PFS_REPAIR_MODE_COPY_REPLACE 2
#define PFS_REPAIR_SCAN_REPAIR_NEEDED 2

int pfs_repair_ffpfsc_auto(const char *path, pfs_repair_info_t *info,
                           char *err, size_t err_size);
int pfs_repair_ffpfsc_inplace(const char *path, pfs_repair_info_t *info,
                              char *err, size_t err_size);
int pfs_repair_ffpfsc_scan_only(const char *path, pfs_repair_info_t *info,
                                char *err, size_t err_size);
int pfs_repair_ffpfsc_copy_replace_temp_only(const char *path,
                                             pfs_repair_info_t *info,
                                             char *err, size_t err_size);
int pfs_repair_ffpfsc_smoke_verify(const char *path, pfs_repair_info_t *info,
                                   char *err, size_t err_size);
void pfs_repair_set_benchmark_tuning(int workers, int read_ahead_blocks);
void pfs_repair_set_benchmark_output_base(const char *path);
void pfs_repair_set_benchmark_ignore_sidecar(int ignore_sidecar);
void pfs_repair_set_benchmark_block_limit(uint64_t block_limit);
