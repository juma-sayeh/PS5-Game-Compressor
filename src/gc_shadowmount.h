#ifndef GC_SHADOWMOUNT_H
#define GC_SHADOWMOUNT_H

#include <stddef.h>

#include "pfs_compress.h"

int gc_shadowmount_write_pfsc_hints(const char *outer_path,
                                    const char *nested_name,
                                    int nested_type,
                                    char *err,
                                    size_t err_size);
int gc_shadowmount_prepare_pfsc_hints_for_title(const char *title_id,
                                                const char *outer_path,
                                                const char *nested_name,
                                                int nested_type,
                                                char *err,
                                                size_t err_size);
int gc_shadowmount_remove_pfsc_hints(const char *outer_path,
                                     const char *nested_name,
                                     int nested_type,
                                     char *err,
                                     size_t err_size);
int gc_shadowmount_remove_title_pfsc_hints(const char *title_id,
                                           const char *outer_path,
                                           char *err,
                                           size_t err_size);
int gc_shadowmount_remove_outer_sector_hint(const char *outer_path,
                                            char *err,
                                            size_t err_size);

int gc_shadowmount_request_scan(char *err, size_t err_size);
int gc_shadowmount_restart_running(char *detail, size_t detail_size);

#endif
