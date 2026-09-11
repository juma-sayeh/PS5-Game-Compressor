/*
 * Game Compressor - ShadowMount interface.
 *
 * This header is the single entry point for talking to ShadowMountPlus.
 * All routines use the legacy file-based hint interface (config.ini /
 * autotune.ini / manual.lst) by default. The only place where the new
 * ShadowMountPlus HTTP API (OpenAPI v1, http://127.0.0.1:10101) is
 * used is gc_shadowmount_request_title_source_scan(): if the API is
 * available (SM >= 1.7), the source path is upserted into manual.lst
 * and a scan is triggered so SM discovers it, then the game is mounted
 * via POST /api/v1/games/mount. Similarly,
 * gc_shadowmount_restart_running() probes the API before and after
 * launching the payload to detect whether the new API is active.
 */
#ifndef GC_SHADOWMOUNT_H
#define GC_SHADOWMOUNT_H

#include <stddef.h>

#include "pfs_compress.h"

/*
 * Write PFSC sector hint (65536) and image mode hint for a compressed
 * outer image. Legacy: writes autotune.ini + config.ini. New API: no-op
 * (the API manages mount state; hint files are not consumed).
 *
 * Returns 0 on success, -1 on error (err filled).
 */
int gc_shadowmount_write_pfsc_hints(const char *outer_path,
                                     const char *nested_name,
                                     int nested_type,
                                     char *err,
                                     size_t err_size);

/*
 * Remove any existing hints for the given title, then write fresh PFSC
 * hints for the outer image. Legacy: remove_title + write. New API: no-op.
 *
 * Returns 0 on success, -1 on error (err filled).
 */
int gc_shadowmount_prepare_pfsc_hints_for_title(const char *title_id,
                                                 const char *outer_path,
                                                 const char *nested_name,
                                                 int nested_type,
                                                 char *err,
                                                 size_t err_size);

/*
 * Set a read-only image hint for a direct (non-compressed) image, removing
 * any existing title hints first. Legacy: remove_title + image_ro in
 * config.ini. New API: no-op.
 *
 * Returns 0 on success, -1 on error (err filled).
 */
int gc_shadowmount_prepare_image_hints_for_title(const char *title_id,
                                                  const char *image_path,
                                                  int nested_type,
                                                  char *err,
                                                  size_t err_size);

/*
 * Ensure that an image_ro hint for the given image exists in config.ini.
 * If the hint is already present, *already_present is set to 1 and the
 * function returns 0 without writing. Legacy: appends to config.ini.
 * New API: no-op, *already_present = 1.
 *
 * Returns 0 on success, -1 on error (err filled).
 */
int gc_shadowmount_ensure_image_read_only(const char *image_path,
                                           int *already_present,
                                           char *err,
                                           size_t err_size);

/*
 * Remove the PFSC sector hint and image mode hint for the given outer
 * image and (for exFAT nested types) the nested image's hints as well.
 * Legacy: removes from autotune.ini + config.ini. New API: no-op.
 *
 * Returns 0 on success, -1 on error (err filled).
 */
int gc_shadowmount_remove_pfsc_hints(const char *outer_path,
                                      const char *nested_name,
                                      int nested_type,
                                      char *err,
                                      size_t err_size);

/*
 * Remove all hint entries related to a title: the outer_path hints plus
 * any .exfat, .pfs, and .ffpfsc image-name hints derived from title_id.
 * Legacy: removes from config.ini + autotune.ini. New API: no-op.
 *
 * Returns 0 on success, -1 on error (err filled).
 */
int gc_shadowmount_remove_title_pfsc_hints(const char *title_id,
                                            const char *outer_path,
                                            char *err,
                                            size_t err_size);

/*
 * Remove only the outer sector hint (image_sector) for the given path.
 * Legacy: removes from autotune.ini. New API: no-op.
 *
 * Returns 0 on success, -1 on error (err filled).
 */
int gc_shadowmount_remove_outer_sector_hint(const char *outer_path,
                                             char *err,
                                             size_t err_size);

/*
 * Upsert a source path into manual.lst (no title association) and trigger
 * a ShadowMountPlus scan. Legacy: write manual.lst + touch. New API: no-op
 * (the API auto-discovers games; no generic rescan endpoint exists).
 *
 * Returns 0 on success, -1 on error (err filled).
 */
int gc_shadowmount_request_source_scan(const char *source_path,
                                        char *err,
                                        size_t err_size);

/*
 * Upsert a title-specific source path into manual.lst and trigger a scan,
 * or mount the managed game directly via the new API. Legacy: manual.lst
 * upsert + touch. New API: upsert manual.lst + touch so SM discovers the
 * source, then POST /api/v1/games/mount (the API resolves the source by
 * title_id from its internal registry which is fed by the manual.lst scan).
 *
 * Returns 0 on success, -1 on error (err filled).
 */
int gc_shadowmount_request_title_source_scan(const char *title_id,
                                              const char *source_path,
                                              char *err,
                                              size_t err_size);

/*
 * Touch /data/shadowmount/manual.lst to trigger a ShadowMountPlus rescan
 * of all registered sources. Legacy: utimes() on manual.lst. New API:
 * no-op (no generic rescan endpoint; per-title mount is used instead).
 *
 * Returns 0 on success, -1 on error (err filled).
 */
int gc_shadowmount_request_scan(char *err, size_t err_size);

/*
 * Unmount a managed game by title after an operation completes. For SM 1.7+
 * (API available): calls POST /api/v1/games/unmount. For legacy: touches
 * manual.lst to trigger a rescan (legacy SM manages its own mount lifecycle).
 *
 * Call this after gc_shadowmount_request_title_source_scan() and the
 * operation that accessed /mnt/shadowmnt has finished.
 *
 * Returns 0 on success, -1 on error (err filled).
 */
int gc_shadowmount_unmount_title(const char *title_id,
                                  char *err, size_t err_size);

/*
 * (Re)launch the ShadowMountPlus payload process. Probes the new HTTP API
 * first; if already reachable (SM 1.7+), returns 0 immediately. If not,
 * finds and launches the ShadowMountPlus ELF via Payload Manager or
 * sceKernelLoadStartModule. After a successful launch, re-probes the API
 * to detect whether SM 1.7+ came up (logs "ready" once or "legacy mode"
 * once). Always uses the legacy ELF launcher regardless of API mode.
 *
 * Returns 0 on success, -1 on failure (detail filled).
 */
int gc_shadowmount_restart_running(char *detail, size_t detail_size);

#endif
