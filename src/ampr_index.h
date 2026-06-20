/*
 * Game Compressor - AMPR APR resolver index builder.
 */

#pragma once

#include <stddef.h>
#include <stdint.h>

typedef struct ampr_index_entry {
  const char *rel;
  uint64_t size;
  int64_t mtime;
} ampr_index_entry_t;

typedef struct ampr_index_stats {
  uint64_t indexed_files;
  uint64_t output_size;
  uint64_t hash_slots;
  uint64_t probed_entries;
  uint64_t probe_steps;
  uint64_t max_probe;
  uint64_t duplicate_hash_groups;
  uint64_t duplicate_hash_entries;
} ampr_index_stats_t;

int ampr_index_entries_need_index(const ampr_index_entry_t *entries,
                                  size_t count);

int ampr_index_build_from_entries(const char *root,
                                  const ampr_index_entry_t *entries,
                                  size_t count,
                                  int allow_case_collisions,
                                  ampr_index_stats_t *stats,
                                  char *err,
                                  size_t err_size);

int ampr_index_build_to_memory(const ampr_index_entry_t *entries,
                               size_t count,
                               int allow_case_collisions,
                               unsigned char **out,
                               size_t *out_size,
                               ampr_index_stats_t *stats,
                               char *err,
                               size_t err_size);
