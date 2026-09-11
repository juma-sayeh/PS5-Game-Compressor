/*
 * Game Compressor - narrow API surface.
 *
 * These four entry points are the only functions the web server (gc_websrv.c)
 * and the startup path (gc_main.c) call into gc_api.c. All game-discovery,
 * queue, history, and operation logic lives behind them.
 */

#pragma once

#include "websrv.h"

/*
 * Main HTTP request dispatcher. Routes /api/gc/ paths to the appropriate
 * handler: games listing, job control, compress/uncompress, validate,
 * move/copy, delete, read-speed-test, build-ampr-index, set-read-only,
 * update-ampr, restore-ampr-original, USB targets, history, UI settings,
 * bad-blocks, and ampr version/upload sub-routes.
 *
 * Returns the HTTP response code (200, 400, 404, 405, 409, 500, etc.).
 */
int gc_api_request(const http_request_t *req, const char *url);

/*
 * Serve a resized PNG icon for a title, cached on disk. Accepts a title_id
 * and optional size query parameter. Generates the thumbnail on first
 * request from the title's icon0.png and serves it on subsequent calls.
 *
 * Returns the HTTP response code (200 on success, 404 if no icon).
 */
int gc_api_icon_request(const http_request_t *req);

/*
 * Return the runtime handoff/state as JSON. Used by a new Game Compressor
 * instance to determine whether the previous instance has an active job
 * (busy/resumable/pending) before deciding whether to take over or exit.
 *
 * Returns the HTTP response code (200 on success).
 */
int gc_api_handoff_state_request(const http_request_t *req);

/*
 * Startup recovery: cleans up interrupted force-remount temp files, delete
 * pending temps, and restores mount-switch recovery state. Called once from
 * gc_main.c after power-guard start and before the web server begins
 * listening. The slow ShadowMount restart and games-list cache warmup
 * (icon + AMPR prewarm) are kicked off in a background thread so they do
 * not block the web server from starting; /api/gc/games blocks on a
 * condition variable until the warmup completes, then returns the full
 * list.
 */
void gc_api_recover_on_startup(void);
