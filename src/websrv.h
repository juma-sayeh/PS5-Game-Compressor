/*
 * Game Compressor - tiny HTTP server helpers.
 *
 * A minimal HTTP/1.1 server used to serve the web UI assets and the
 * /api/gc/ JSON endpoints. Every response is sent with
 * "Connection: close" (one request per TCP connection) and
 * "Access-Control-Allow-Origin: *" so the UI can be opened from any
 * host. Cache-Control defaults to no-store for dynamic JSON; durable
 * browser caching is opt-in via websrv_send_cached() for stable
 * assets such as game icons.
 */
#pragma once

#include <stddef.h>

/*
 * A parsed HTTP request. Populated by the server loop before the
 * request is dispatched to a handler.
 *
 *   fd         - the client socket fd; handlers write the response here
 *   method     - "GET" / "POST" / etc. (upper-case, NUL-terminated)
 *   path       - request-target path without the query string
 *                (e.g. "/api/gc/games"), NUL-terminated
 *   query      - the raw query string (without the leading '?'),
 *                URL-encoded, NUL-terminated; may be empty
 *   body       - request body bytes (malloc'd, freed by the server)
 *   body_size  - size of body in bytes (0 if none)
 */
typedef struct http_request {
  int  fd;
  char method[8];
  char path[1024];
  char query[2048];
  char *body;
  size_t body_size;
} http_request_t;

/*
 * Callback invoked once the server socket is bound and listening, so
 * the caller can log the URL or signal readiness. `arg` is the
 * opaque pointer passed to websrv_listen().
 */
typedef void (*websrv_ready_cb_t)(unsigned short port, void *arg);

/*
 * Send a complete HTTP response: status line, default headers
 * (Connection: close, Access-Control-Allow-Origin: *,
 * Cache-Control: no-store), Content-Type: <mime>, Content-Length,
 * then the body bytes. `data` may be NULL when size == 0. Use this
 * for all dynamic JSON/text responses that must not be cached.
 *
 * Returns 0 on success, -1 on write error.
 */
int websrv_send(int fd, int status, const char *mime,
                 const void *data, size_t size);

/*
 * Like websrv_send() but marks the response as durably cacheable by the
 * browser (Cache-Control: public, max-age=<max_age_seconds>, immutable).
 * Use for responses whose URL already carries a cache-busting version
 * query argument (e.g. /api/gc/icon?...&v=...) so a stale entry can never
 * be served: when the underlying content changes the version argument
 * changes, producing a new URL and a fresh fetch.
 */
int websrv_send_cached(int fd, int status, const char *mime,
                        const void *data, size_t size, int max_age_seconds);

/*
 * Send a JSON error response: status code <status> with body
 * {"ok":false,"error":"<message>"} and Cache-Control: no-store.
 * `message` is JSON-escaped. Returns 0 on success, -1 on write error.
 */
int websrv_send_error_json(int fd, int status, const char *message);

/*
 * Look up a single query-parameter value by name in req->query,
 * URL-decoding it into `out`. `name` is matched case-sensitively.
 * Returns 1 if the parameter was present (out is NUL-terminated),
 * 0 if it was absent (out[0] is set to 0). `out` is truncated to
 * out_size - 1.
 */
int websrv_get_query_arg(const http_request_t *req, const char *name,
                         char *out, size_t out_size);

/*
 * Bind a listening socket on `port` and run the single-threaded
 * accept/handle loop. `ready_cb` (if non-NULL) is invoked once the
 * socket is listening, with the bound port and `ready_arg`. The loop
 * runs until the process exits or an internal exit is requested.
 *
 * Returns 0 on clean shutdown, -1 if the socket could not be bound.
 */
int websrv_listen(unsigned short port, websrv_ready_cb_t ready_cb,
                  void *ready_arg);
