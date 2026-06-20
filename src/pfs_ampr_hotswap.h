/*
 * Game Compressor - APR-EMU hot-swap helpers for exFAT/PFSC images.
 */

#pragma once

#include <stddef.h>
#include <stdint.h>

typedef struct pfs_ampr_hotswap_info {
  char mode[32];
  char logical_path[128];
  uint64_t old_size;
  uint64_t new_size;
  uint64_t changed_blocks;
  uint32_t old_first_cluster;
  uint32_t new_first_cluster;
  uint32_t allocated_clusters;
  int tail_migrated;
} pfs_ampr_hotswap_info_t;

int pfs_ampr_hotswap_exfat_image(const char *image_path,
                                 const char *replacement_path,
                                 pfs_ampr_hotswap_info_t *info,
                                 char *err, size_t err_size);

int pfs_ampr_hotswap_ffpfsc_exfat(const char *ffpfsc_path,
                                  const char *replacement_path,
                                  pfs_ampr_hotswap_info_t *info,
                                  char *err, size_t err_size);

int pfs_ampr_hotswap_ffpfsc_pfs(const char *ffpfsc_path,
                                const char *replacement_path,
                                pfs_ampr_hotswap_info_t *info,
                                char *err, size_t err_size);

int pfs_ampr_index_exfat_image(const char *image_path,
                               pfs_ampr_hotswap_info_t *info,
                               char *err, size_t err_size);

int pfs_ampr_index_ffpfsc_exfat(const char *ffpfsc_path,
                                pfs_ampr_hotswap_info_t *info,
                                char *err, size_t err_size);
