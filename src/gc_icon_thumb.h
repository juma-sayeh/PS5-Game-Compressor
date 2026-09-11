#ifndef GC_ICON_THUMB_H
#define GC_ICON_THUMB_H

#include <stddef.h>
#include <sys/stat.h>

#define GC_ICON_THUMB_SIZE 96

int gc_icon_thumb_path(const char *title_id,
                       const char *source_path,
                       const struct stat *source_st,
                       char *out_path,
                       size_t out_path_size);

/*
 * Generate a 96x96 thumbnail PNG from raw PNG source bytes in memory.
 * On success, *out is a malloc'd buffer (caller frees) and *out_size
 * is the byte length. Returns 0 on success, -1 on failure.
 */
int gc_icon_thumb_from_memory(const unsigned char *source, int source_size,
                               unsigned char **out, int *out_size);

#endif
