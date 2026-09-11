/*
 * Game Compressor - ShadowMountPlus HTTP API client (OpenAPI v1).
 *
 * Minimal self-contained client for the API exposed at
 * http://127.0.0.1:10101 by ShadowMountPlus >= 1.7. The project keeps
 * no third-party JSON dependency, so the schemas defined in
 * docs/openapi.yaml are decoded with a tiny field extractor plus a
 * brace/quote aware object scanner, mirroring the helpers already used
 * in gc_main.c. HTTP is a raw socket POST with Content-Length and
 * Connection: close, matching the server contract (JSON body <= 4096
 * bytes, Content-Length required).
 */

#include "gc_shadowmount_api.h"
#include "gc_diag.h"

#include <arpa/inet.h>
#include <errno.h>
#include <netinet/in.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <unistd.h>

#define SM_API_HOST "127.0.0.1"
#define SM_API_PORT 10101
#define SM_API_TIMEOUT_SEC 5
#define SM_API_RESPONSE_MAX (256 * 1024)
#define SM_API_BINARY_MAX (2 * 1024 * 1024)
#define SM_API_BODY_MAX 4096

static int g_shadowmount_api_ready = 0;

#define SM_LOG_SNIPPET_LEN 256

static void
sm_log_snippet(const char* label, const char* path, const char* snippet,
               size_t total_len) {
    char buf[SM_LOG_SNIPPET_LEN + 1];
    size_t copy = total_len < SM_LOG_SNIPPET_LEN ? total_len : SM_LOG_SNIPPET_LEN;
    if (!snippet || total_len == 0) {
        gc_trace("shadowmount api %s: POST %s (empty)", label ? label : "log",
               path ? path : "/");
        return;
    }
    memcpy(buf, snippet, copy);
    buf[copy] = 0;
    for (size_t i = 0; i < copy; i++) {
        if (buf[i] == '\r' || buf[i] == '\n') buf[i] = ' ';
    }
    gc_trace("shadowmount api %s: POST %s %s%s (%zu bytes)",
           label ? label : "log", path ? path : "/", buf,
           copy < total_len ? "..." : "", total_len);
}

static void
set_err(char* err, size_t err_size, const char* message) {
    if (err && err_size && !err[0]) {
        snprintf(err, err_size, "%s", message ? message : "shadowmount api error");
    }
}

static void
set_errno_err(char* err, size_t err_size, const char* context) {
    if (err && err_size && !err[0]) {
        snprintf(err, err_size, "%s: %s", context ? context : "shadowmount api",
                 strerror(errno));
    }
}

static int
sm_send_all(int fd, const char* data, size_t size) {
    size_t off = 0;
    while (off < size) {
        ssize_t n = send(fd, data + off, size - off, 0);
        if (n < 0) {
            if (errno == EINTR) continue;
            return -1;
        }
        if (n == 0) return -1;
        off += (size_t)n;
    }
    return 0;
}

static const char*
sm_json_find(const char* json, const char* name) {
    char pattern[96];
    int n = snprintf(pattern, sizeof(pattern), "\"%s\"", name ? name : "");
    if (n < 0 || (size_t)n >= sizeof(pattern)) return NULL;
    const char* p = strstr(json ? json : "", pattern);
    if (!p) return NULL;
    p += (size_t)n;
    while (*p == ' ' || *p == '\t' || *p == '\r' || *p == '\n') p++;
    if (*p != ':') return NULL;
    p++;
    while (*p == ' ' || *p == '\t' || *p == '\r' || *p == '\n') p++;
    return p;
}

static int
sm_json_bool(const char* json, const char* name, int default_value) {
    const char* p = sm_json_find(json, name);
    if (!p) return default_value;
    if (!strncmp(p, "true", 4)) return 1;
    if (!strncmp(p, "false", 5)) return 0;
    return default_value;
}

static long long
sm_json_long(const char* json, const char* name, long long default_value) {
    const char* p = sm_json_find(json, name);
    char* end = NULL;
    if (!p) return default_value;
    errno = 0;
    long long value = strtoll(p, &end, 10);
    if (errno != 0 || end == p) return default_value;
    return value;
}

static double
sm_json_double(const char* json, const char* name, double default_value) {
    const char* p = sm_json_find(json, name);
    char* end = NULL;
    if (!p) return default_value;
    errno = 0;
    double value = strtod(p, &end);
    if (errno != 0 || end == p) return default_value;
    return value;
}

/*
 * Extract a JSON string value into a dynamically grown malloc'd
 * buffer. Handles escape sequences. Returns the buffer (NUL-
 * terminated) or NULL if the field is absent/not a string. If
 * out_len is non-NULL, receives the length excluding the NUL.
 */
static char*
sm_json_string_alloc(const char* json, const char* name,
                     size_t* out_len) {
    const char* p = sm_json_find(json, name);
    size_t cap = 4096;
    size_t pos = 0;
    char* out;
    if (!p || *p != '"') return NULL;
    p++;
    out = malloc(cap);
    if (!out) return NULL;
    while (*p && *p != '"') {
        char c;
        if (*p == '\\' && p[1]) {
            p++;
            if (*p == 'n') c = '\n';
            else if (*p == 'r') c = '\r';
            else if (*p == 't') c = '\t';
            else c = *p;
        } else {
            c = *p;
        }
        if (pos + 1 >= cap) {
            char* tmp = realloc(out, cap * 2);
            if (!tmp) {
                free(out);
                return NULL;
            }
            out = tmp;
            cap *= 2;
        }
        out[pos++] = c;
        p++;
    }
    out[pos] = 0;
    if (out_len) *out_len = pos;
    return out;
}

/* Forward declaration: used by sm_json_string_array before its definition. */
static const char* sm_json_array_start(const char* json, const char* name);

/*
 * Parse a JSON string array field into paths_out[0..*count-1].
 * Each element is truncated to GC_SM_PATH_LEN-1. Returns count.
 */
static int
sm_json_string_array(const char* json, const char* name,
                     char (*paths_out)[GC_SM_PATH_LEN], int max_paths) {
    const char* p = sm_json_array_start(json, name);
    int n = 0;
    if (!p || !paths_out || max_paths <= 0) return 0;
    while (*p && *p != ']' && n < max_paths) {
        char tmp[GC_SM_PATH_LEN];
        size_t i = 0;
        while (*p == ' ' || *p == '\t' || *p == ',' || *p == '\r' || *p == '\n') p++;
        if (*p != '"') {
            if (*p == ']') break;
            p++;
            continue;
        }
        p++;
        while (*p && *p != '"' && i + 1 < sizeof(tmp)) {
            if (*p == '\\' && p[1]) {
                p++;
                tmp[i++] = *p++;
                continue;
            }
            tmp[i++] = *p++;
        }
        tmp[i] = 0;
        if (*p == '"') p++;
        snprintf(paths_out[n], GC_SM_PATH_LEN, "%s", tmp);
        n++;
        while (*p && *p != ',' && *p != ']') p++;
    }
    return n;
}

static int
sm_json_string(const char* json, const char* name,
               char* out, size_t out_size) {
    const char* p = sm_json_find(json, name);
    size_t pos = 0;
    if (!out || out_size == 0) return 0;
    out[0] = 0;
    if (!p || *p != '"') return 0;
    p++;
    while (*p && *p != '"' && pos + 1 < out_size) {
        if (*p == '\\' && p[1]) {
            p++;
            if (*p == 'n') out[pos++] = '\n';
            else if (*p == 'r') out[pos++] = '\r';
            else if (*p == 't') out[pos++] = '\t';
            else out[pos++] = *p;
            p++;
            continue;
        }
        out[pos++] = *p++;
    }
    out[pos] = 0;
    return 1;
}

static const char*
sm_json_array_start(const char* json, const char* name) {
    const char* p = sm_json_find(json, name);
    if (!p) return NULL;
    while (*p == ' ' || *p == '\t' || *p == '\r' || *p == '\n') p++;
    if (*p != '[') return NULL;
    return p + 1;
}

/*
 * Extract the content (without outer braces) of the next JSON object
 * starting at *cursor into `out`. Advances *cursor past the object.
 * Returns 1 when an object was copied, 0 at end of array. Braces and
 * quotes inside string literals are handled so nested object fields do
 * not confuse the depth counter.
 */
static int
sm_json_next_object(const char** cursor, char* out, size_t out_size) {
    const char* p = *cursor;
    int depth = 0;
    int in_str = 0;
    int esc = 0;
    size_t pos = 0;
    if (!out || out_size == 0) return 0;
    out[0] = 0;
    while (*p && *p != '{') {
        if (*p == ']') {
            *cursor = p;
            return 0;
        }
        p++;
    }
    if (*p != '{') {
        *cursor = p;
        return 0;
    }
    p++;
    depth = 1;
    while (*p && depth > 0 && pos + 1 < out_size) {
        char c = *p;
        if (in_str) {
            if (esc) {
                esc = 0;
            } else if (c == '\\') {
                esc = 1;
            } else if (c == '"') {
                in_str = 0;
            }
            out[pos++] = c;
            p++;
            continue;
        }
        if (c == '"') {
            in_str = 1;
            out[pos++] = c;
            p++;
            continue;
        }
        if (c == '{') {
            depth++;
        } else if (c == '}') {
            depth--;
            if (depth == 0) {
                p++;
                break;
            }
        }
        out[pos++] = c;
        p++;
    }
    out[pos] = 0;
    while (*p == ' ' || *p == '\t' || *p == '\r' || *p == '\n' || *p == ',') p++;
    *cursor = p;
    return 1;
}

static int
sm_parse_capabilities(const char* json,
                      char caps[][GC_SM_CAP_LEN], int max_caps) {
    const char* p = sm_json_find(json, "capabilities");
    int n = 0;
    if (!p) return 0;
    while (*p == ' ' || *p == '\t' || *p == '\r' || *p == '\n') p++;
    if (*p != '[') return 0;
    p++;
    while (*p && *p != ']' && n < max_caps) {
        char tmp[GC_SM_CAP_LEN];
        size_t i = 0;
        while (*p == ' ' || *p == '\t' || *p == ',' || *p == '\r' || *p == '\n') p++;
        if (*p != '"') {
            if (*p == ']') break;
            p++;
            continue;
        }
        p++;
        while (*p && *p != '"' && i + 1 < sizeof(tmp)) {
            if (*p == '\\' && p[1]) {
                p++;
                tmp[i++] = *p++;
                continue;
            }
            tmp[i++] = *p++;
        }
        tmp[i] = 0;
        if (*p == '"') p++;
        snprintf(caps[n], GC_SM_CAP_LEN, "%s", tmp);
        n++;
        while (*p && *p != ',' && *p != ']') p++;
    }
    return n;
}

static void
sm_parse_image(const char* obj, gc_sm_image_t* img) {
    memset(img, 0, sizeof(*img));
    sm_json_string(obj, "path", img->path, sizeof(img->path));
    sm_json_string(obj, "mount_point", img->mount_point,
                   sizeof(img->mount_point));
    img->size = sm_json_long(obj, "size", 0);
    img->mtime_sec = sm_json_long(obj, "mtime_sec", 0);
    img->mtime_nsec = (int)sm_json_long(obj, "mtime_nsec", 0);
    img->unit_id = (int)sm_json_long(obj, "unit_id", -1);
    sm_json_string(obj, "backend", img->backend, sizeof(img->backend));
    img->complete = sm_json_bool(obj, "complete", 0);
    img->source_available = sm_json_bool(obj, "source_available", 0);
    img->mapped = sm_json_bool(obj, "mapped", 0);
    img->mounted = sm_json_bool(obj, "mounted", 0);
}

static void
sm_parse_game(const char* obj, gc_sm_game_t* g) {
    memset(g, 0, sizeof(*g));
    sm_json_string(obj, "path", g->path, sizeof(g->path));
    sm_json_string(obj, "runtime_path", g->runtime_path,
                   sizeof(g->runtime_path));
    sm_json_string(obj, "source_type", g->source_type,
                   sizeof(g->source_type));
    sm_json_string(obj, "image_type", g->image_type,
                   sizeof(g->image_type));
    sm_json_string(obj, "platform", g->platform, sizeof(g->platform));
    sm_json_string(obj, "title_id", g->title_id, sizeof(g->title_id));
    sm_json_string(obj, "content_id", g->content_id, sizeof(g->content_id));
    sm_json_string(obj, "title_name", g->title_name, sizeof(g->title_name));
    sm_json_string(obj, "last_access_time", g->last_access_time,
                   sizeof(g->last_access_time));
    sm_json_string(obj, "install_time", g->install_time,
                   sizeof(g->install_time));
    sm_json_string(obj, "icon_url", g->icon_url, sizeof(g->icon_url));
    g->app_db_size_bytes = sm_json_long(obj, "app_db_size_bytes", 0);
    g->installed = sm_json_bool(obj, "installed", 0);
    g->managed = sm_json_bool(obj, "managed", 0);
    g->mounted = sm_json_bool(obj, "mounted", 0);
    g->image_backed = sm_json_bool(obj, "image_backed", 0);
    g->source_available = sm_json_bool(obj, "source_available", 0);
    g->size_bytes = sm_json_long(obj, "size_bytes", 0);
    g->size_status = (int)sm_json_long(obj, "size_status", 0);
    g->status = (int)sm_json_long(obj, "status", 0);
}

static int
sm_title_id_valid(const char* title_id) {
    size_t len;
    if (!title_id) return 0;
    len = strlen(title_id);
    if (len != 9) return 0;
    if (strncmp(title_id, "CUSA", 4) != 0 &&
        strncmp(title_id, "PPSA", 4) != 0 &&
        strncmp(title_id, "FAKE", 4) != 0)
        return 0;
    for (size_t i = 4; i < 9; i++) {
        if (title_id[i] < '0' || title_id[i] > '9') return 0;
    }
    return 1;
}

/*
 * Perform a single POST against the ShadowMount API. On a 2xx reply the
 * response body is returned as a malloc'd NUL-terminated string in
 * *response_body (caller frees). On failure a message is written to err
 * and -1 is returned. http_status (if requested) receives the numeric
 * status even on non-2xx responses.
 */
static int
sm_post(const char* path, const char* request_body,
        char** response_body, int* http_status,
        char* err, size_t err_size) {
    int fd = -1;
    struct timeval timeout;
    struct sockaddr_in addr;
    char request[4608];
    const char* body_ptr;
    size_t body_len;
    int header_len;
    char* buf = NULL;
    size_t cap = SM_API_RESPONSE_MAX;
    size_t used = 0;
    int status = 0;
    char* body_start;
    size_t body_len_out;
    int rc = -1;

    if (response_body) *response_body = NULL;
    if (http_status) *http_status = 0;

    body_ptr = request_body ? request_body : "";
    body_len = strlen(body_ptr);
    if (body_len > SM_API_BODY_MAX) {
        set_err(err, err_size, "ShadowMount API request body too large");
        gc_log("shadowmount api request body too large: POST %s len=%zu",
               path ? path : "/", body_len);
        return -1;
    }

    fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) {
        set_errno_err(err, err_size, "ShadowMount API socket");
        gc_log("shadowmount api socket failed: %s", strerror(errno));
        return -1;
    }
    timeout.tv_sec = SM_API_TIMEOUT_SEC;
    timeout.tv_usec = 0;
    setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &timeout, sizeof(timeout));
    setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout));

    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_port = htons(SM_API_PORT);
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    if (connect(fd, (struct sockaddr*)&addr, sizeof(addr)) != 0) {
        set_errno_err(err, err_size, "ShadowMount API connect");
        gc_log("shadowmount api connect failed: %s", strerror(errno));
        goto done;
    }

    header_len = snprintf(request, sizeof(request),
                          "POST %s HTTP/1.1\r\n"
                          "Host: %s:%d\r\n"
                          "Content-Type: application/json\r\n"
                          "Content-Length: %zu\r\n"
                          "Connection: close\r\n"
                          "\r\n",
                          path ? path : "/", SM_API_HOST, SM_API_PORT, body_len);
    if (header_len < 0 || (size_t)header_len + body_len >= sizeof(request)) {
        set_err(err, err_size, "ShadowMount API request too large");
        gc_trace("shadowmount api request too large: POST %s", path ? path : "/");
        goto done;
    }
    memcpy(request + header_len, body_ptr, body_len);
    sm_log_snippet("request", path ? path : "/", body_ptr, body_len);
    if (sm_send_all(fd, request, (size_t)header_len + body_len) != 0) {
        set_errno_err(err, err_size, "ShadowMount API send");
        gc_log("shadowmount api send failed: POST %s err=%s",
               path ? path : "/", strerror(errno));
        goto done;
    }

    buf = malloc(cap);
    if (!buf) {
        set_err(err, err_size, "ShadowMount API response allocation failed");
        gc_log("shadowmount api response alloc failed: POST %s", path ? path : "/");
        goto done;
    }
    while (used + 1 < cap) {
        ssize_t got = recv(fd, buf + used, cap - 1 - used, 0);
        if (got < 0) {
            if (errno == EINTR) continue;
            break;
        }
        if (got == 0) break;
        used += (size_t)got;
    }
    buf[used] = 0;

    if (strncmp(buf, "HTTP/1.1 ", 9) != 0 &&
        strncmp(buf, "HTTP/1.0 ", 9) != 0) {
        set_err(err, err_size, "ShadowMount API bad HTTP response");
        gc_trace("shadowmount api bad http response: POST %s", path ? path : "/");
        goto done;
    }
    status = atoi(buf + 9);
    if (http_status) *http_status = status;
    body_start = strstr(buf, "\r\n\r\n");
    if (!body_start) {
        set_err(err, err_size, "ShadowMount API response has no body");
        gc_trace("shadowmount api no body: POST %s http=%d", path ? path : "/",
               status);
        goto done;
    }
    body_start += 4;
    if (status < 200 || status >= 300) {
        snprintf(err, err_size, "ShadowMount API HTTP %d: %.200s",
                 status, body_start);
        gc_log("shadowmount api non-2xx: POST %s http=%d body=%.200s",
               path ? path : "/", status, body_start);
        goto done;
    }
    if (response_body) {
        body_len_out = strlen(body_start) + 1;
        *response_body = malloc(body_len_out);
        if (!*response_body) {
            set_err(err, err_size, "ShadowMount API body copy failed");
            gc_log("shadowmount api body copy failed: POST %s", path ? path : "/");
            goto done;
        }
        memcpy(*response_body, body_start, body_len_out);
    }
    sm_log_snippet("response", path ? path : "/", body_start,
                   strlen(body_start));
    rc = 0;

done:
    if (fd >= 0) close(fd);
    free(buf);
    return rc;
}

/*
 * Perform a single GET against the ShadowMount API. The response body
 * is returned as a malloc'd buffer in *data_out with *size_out bytes.
 * Unlike sm_post(), this handles binary (non-JSON) responses correctly
 * by using the actual byte count rather than strlen. Used for the icon
 * endpoint GET /api/v1/games/icon which returns raw PNG data.
 */
static int
sm_get_binary(const char* path_with_query,
              unsigned char** data_out, size_t* size_out,
              int* http_status, char* err, size_t err_size) {
    int fd = -1;
    struct timeval timeout;
    struct sockaddr_in addr;
    char request[2048];
    int header_len;
    char* buf = NULL;
    size_t cap = SM_API_RESPONSE_MAX;
    size_t used = 0;
    int status = 0;
    char* header_end;
    size_t header_bytes;
    size_t body_len;
    int rc = -1;

    if (data_out) *data_out = NULL;
    if (size_out) *size_out = 0;
    if (http_status) *http_status = 0;

    fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) {
        set_errno_err(err, err_size, "ShadowMount API socket");
        gc_log("shadowmount api socket failed: %s", strerror(errno));
        return -1;
    }
    timeout.tv_sec = SM_API_TIMEOUT_SEC;
    timeout.tv_usec = 0;
    setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &timeout, sizeof(timeout));
    setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout));

    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_port = htons(SM_API_PORT);
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    if (connect(fd, (struct sockaddr*)&addr, sizeof(addr)) != 0) {
        set_errno_err(err, err_size, "ShadowMount API connect");
        gc_log("shadowmount api connect failed: %s", strerror(errno));
        goto done;
    }

    header_len = snprintf(request, sizeof(request),
                          "GET %s HTTP/1.1\r\n"
                          "Host: %s:%d\r\n"
                          "Connection: close\r\n"
                          "\r\n",
                          path_with_query, SM_API_HOST, SM_API_PORT);
    if (header_len < 0 || (size_t)header_len >= sizeof(request)) {
        set_err(err, err_size, "ShadowMount API request too large");
        goto done;
    }
    if (sm_send_all(fd, request, (size_t)header_len) != 0) {
        set_errno_err(err, err_size, "ShadowMount API send");
        gc_log("shadowmount api send failed: GET %s", path_with_query);
        goto done;
    }

    buf = malloc(cap);
    if (!buf) {
        set_err(err, err_size, "ShadowMount API response allocation failed");
        goto done;
    }
    while (1) {
        if (used + 1 >= cap) {
            size_t new_cap = cap;
            if (cap >= SM_API_BINARY_MAX) break;
            new_cap = cap * 2;
            if (new_cap > SM_API_BINARY_MAX) new_cap = SM_API_BINARY_MAX;
            char* new_buf = realloc(buf, new_cap);
            if (!new_buf) break;
            buf = new_buf;
            cap = new_cap;
        }
        ssize_t got = recv(fd, buf + used, cap - 1 - used, 0);
        if (got < 0) {
            if (errno == EINTR) continue;
            break;
        }
        if (got == 0) break;
        used += (size_t)got;
    }
    buf[used] = 0;

    if (used + 1 >= cap && cap >= SM_API_BINARY_MAX) {
        gc_trace("shadowmount api binary response truncated at %zu bytes: GET %s",
               used, path_with_query);
    }

    if (used < 9 ||
        (strncmp(buf, "HTTP/1.1 ", 9) != 0 &&
            strncmp(buf, "HTTP/1.0 ", 9) != 0)) {
        set_err(err, err_size, "ShadowMount API bad HTTP response");
        gc_log("shadowmount api bad http response: GET %s", path_with_query);
        goto done;
    }
    status = atoi(buf + 9);
    if (http_status) *http_status = status;

    header_end = strstr(buf, "\r\n\r\n");
    if (!header_end) {
        set_err(err, err_size, "ShadowMount API response has no body");
        goto done;
    }
    header_end += 4;
    header_bytes = (size_t)(header_end - buf);
    body_len = used - header_bytes;

    if (status < 200 || status >= 300) {
        snprintf(err, err_size, "ShadowMount API HTTP %d: %.200s",
                 status, header_end);
        gc_log("shadowmount api non-2xx: GET %s http=%d",
               path_with_query, status);
        goto done;
    }

    if (data_out) {
        if (body_len > 0) {
            *data_out = malloc(body_len);
            if (!*data_out) {
                set_err(err, err_size, "ShadowMount API body copy failed");
                goto done;
            }
            memcpy(*data_out, header_end, body_len);
            if (size_out) *size_out = body_len;
        } else {
            *data_out = malloc(1);
            if (*data_out) (*data_out)[0] = 0;
            if (size_out) *size_out = 0;
        }
    }
    gc_trace("shadowmount api GET %s http=%d body=%zu",
           path_with_query, status, body_len);
    rc = 0;

done:
    if (fd >= 0) close(fd);
    free(buf);
    return rc;
}

int
gc_shadowmount_api_probe(gc_sm_version_t* out,
                         char* err, size_t err_size) {
    char* body = NULL;
    int status = 0;
    if (err && err_size) err[0] = 0;
    g_shadowmount_api_ready = 0;
    if (sm_post("/api/v1/version", "{}", &body, &status,
                err, err_size) != 0) {
        return -1;
    }
    if (sm_json_long(body, "status", -1) != 0) {
        set_err(err, err_size, "ShadowMount API probe status non-zero");
        free(body);
        return -1;
    }
    g_shadowmount_api_ready = 1;
    if (out) {
        memset(out, 0, sizeof(*out));
        out->api_version = (int)sm_json_long(body, "api_version", 0);
        sm_json_string(body, "shadowmount_version", out->shadowmount_version,
                       sizeof(out->shadowmount_version));
        out->capability_count =
            sm_parse_capabilities(body, out->capabilities, GC_SM_MAX_CAPS);
    }
    free(body);
    return 0;
}

int
gc_shadowmount_api_available(void) {
    return g_shadowmount_api_ready;
}

int
gc_shadowmount_api_scan(char* err, size_t err_size) {
    char* body = NULL;
    int status = 0;
    if (err && err_size) err[0] = 0;
    if (!g_shadowmount_api_ready) {
        set_err(err, err_size, "ShadowMount API not available");
        return -1;
    }
    if (sm_post("/api/v1/scan", "{}", &body, &status,
                err, err_size) != 0) {
        return -1;
    }
    if (sm_json_long(body, "status", -1) != 0) {
        set_err(err, err_size, "ShadowMount API scan failed");
        free(body);
        return -1;
    }
    free(body);
    return 0;
}

int
gc_shadowmount_api_get_version(gc_sm_version_t* out,
                               char* err, size_t err_size) {
    char* body = NULL;
    int status = 0;
    if (err && err_size) err[0] = 0;
    if (!g_shadowmount_api_ready) {
        set_err(err, err_size, "ShadowMount API not available");
        return -1;
    }
    if (sm_post("/api/v1/version", "{}", &body, &status,
                err, err_size) != 0) {
        return -1;
    }
    if (sm_json_long(body, "status", -1) != 0) {
        set_err(err, err_size, "ShadowMount API version status non-zero");
        free(body);
        return -1;
    }
    if (out) {
        memset(out, 0, sizeof(*out));
        out->api_version = (int)sm_json_long(body, "api_version", 0);
        sm_json_string(body, "shadowmount_version", out->shadowmount_version,
                       sizeof(out->shadowmount_version));
        out->capability_count =
            sm_parse_capabilities(body, out->capabilities, GC_SM_MAX_CAPS);
    }
    free(body);
    return 0;
}

int
gc_shadowmount_api_list_images(gc_sm_image_t* out, int max_count,
                               int* count,
                               char* err, size_t err_size) {
    char* body = NULL;
    int status = 0;
    const char* cursor;
    char obj[1024];
    int n = 0;
    if (err && err_size) err[0] = 0;
    if (count) *count = 0;
    if (!g_shadowmount_api_ready) {
        set_err(err, err_size, "ShadowMount API not available");
        return -1;
    }
    if (max_count < 0) max_count = 0;
    if (sm_post("/api/v1/images", "{}", &body, &status,
                err, err_size) != 0) {
        return -1;
    }
    if (sm_json_long(body, "status", -1) != 0) {
        set_err(err, err_size, "ShadowMount API list images failed");
        free(body);
        return -1;
    }
    cursor = sm_json_array_start(body, "images");
    if (!cursor) {
        free(body);
        return 0;
    }
    while (n < max_count && sm_json_next_object(&cursor, obj, sizeof(obj))) {
        if (out) sm_parse_image(obj, &out[n]);
        n++;
    }
    if (count) *count = n;
    free(body);
    return 0;
}

/*
 * Query /api/v1/images and return the record of the image registered at
 * `path`.  Returns 1 when the image was found (out populated if non-NULL),
 * 0 when no image at that path is registered, -1 on error.
 */
int
gc_shadowmount_api_find_image(const char* path,
                              gc_sm_image_t* out,
                              char* err, size_t err_size) {
    char* body = NULL;
    int status = 0;
    const char* cursor;
    char obj[1024];
    int found = 0;
    if (err && err_size) err[0] = 0;
    if (out) memset(out, 0, sizeof(*out));
    if (!path || !path[0]) {
        set_err(err, err_size, "bad ShadowMount image path");
        return -1;
    }
    if (!g_shadowmount_api_ready) {
        set_err(err, err_size, "ShadowMount API not available");
        return -1;
    }
    if (sm_post("/api/v1/images", "{}", &body, &status,
                err, err_size) != 0) {
        return -1;
    }
    if (sm_json_long(body, "status", -1) != 0) {
        set_err(err, err_size, "ShadowMount API list images failed");
        free(body);
        return -1;
    }
    cursor = sm_json_array_start(body, "images");
    if (!cursor) {
        free(body);
        return 0;
    }
    while (sm_json_next_object(&cursor, obj, sizeof(obj))) {
        gc_sm_image_t img;
        sm_parse_image(obj, &img);
        if (img.path[0] && strcmp(img.path, path) == 0) {
            if (out) *out = img;
            found = 1;
            break;
        }
    }
    free(body);
    return found;
}

int
gc_shadowmount_api_list_games(gc_sm_game_t* out, int max_count,
                              int* count, int include_size,
                              char* err, size_t err_size) {
    const char* request_body = include_size ? "{\"include_size\":true}" : "{}";
    char* body = NULL;
    int status = 0;
    const char* cursor;
    char obj[1024];
    int n = 0;
    if (err && err_size) err[0] = 0;
    if (count) *count = 0;
    if (!g_shadowmount_api_ready) {
        set_err(err, err_size, "ShadowMount API not available");
        return -1;
    }
    if (max_count < 0) max_count = 0;
    if (sm_post("/api/v1/games", request_body, &body, &status,
                err, err_size) != 0) {
        return -1;
    }
    if (sm_json_long(body, "status", -1) != 0) {
        set_err(err, err_size, "ShadowMount API list games failed");
        free(body);
        return -1;
    }
    cursor = sm_json_array_start(body, "games");
    if (!cursor) {
        free(body);
        return 0;
    }
    while (n < max_count && sm_json_next_object(&cursor, obj, sizeof(obj))) {
        if (out) sm_parse_game(obj, &out[n]);
        n++;
    }
    if (count) *count = n;
    free(body);
    return 0;
}

static int
sm_mode_valid(const char* mode) {
    if (!mode || !mode[0]) return 0;
    return !strcmp(mode, "ro") || !strcmp(mode, "rw") ||
        !strcmp(mode, "r/o") || !strcmp(mode, "r/w");
}

/*
 * Base single-shot mount: one POST /api/v1/games/mount with no retry.
 * The response body is returned as a malloc'd string in
 * *response_body (caller frees); *http_status receives the HTTP
 * status even on non-2xx responses so the caller can detect 409.
 */
static int
sm_mount_once(const char* title_id, const char* mode,
              char** response_body, int* http_status,
              char* err, size_t err_size) {
    char body_buf[160];
    if (err && err_size) err[0] = 0;
    if (response_body) *response_body = NULL;
    if (http_status) *http_status = 0;
    if (!sm_title_id_valid(title_id)) {
        set_err(err, err_size, "bad ShadowMount title id");
        return -1;
    }
    if (mode && *mode && !sm_mode_valid(mode)) {
        set_err(err, err_size, "bad ShadowMount mount mode");
        return -1;
    }
    if (!g_shadowmount_api_ready) {
        set_err(err, err_size, "ShadowMount API not available");
        return -1;
    }
    if (mode && *mode) {
        snprintf(body_buf, sizeof(body_buf),
                 "{\"title_id\":\"%s\",\"mode\":\"%s\"}", title_id, mode);
    } else {
        snprintf(body_buf, sizeof(body_buf), "{\"title_id\":\"%s\"}", title_id);
    }
    if (sm_post("/api/v1/games/mount", body_buf, response_body, http_status,
                err, err_size) != 0) {
        return -1;
    }
    if (sm_json_long(*response_body, "status", -1) != 0) {
        set_err(err, err_size, "ShadowMount API mount failed");
        return -1;
    }
    return 0;
}

/*
 * Base single-shot unmount: one POST /api/v1/games/unmount with no
 * retry. See sm_mount_once for parameter semantics.
 */
static int
sm_unmount_once(const char* title_id,
                char** response_body, int* http_status,
                char* err, size_t err_size) {
    char body_buf[128];
    if (err && err_size) err[0] = 0;
    if (response_body) *response_body = NULL;
    if (http_status) *http_status = 0;
    if (!sm_title_id_valid(title_id)) {
        set_err(err, err_size, "bad ShadowMount title id");
        return -1;
    }
    if (!g_shadowmount_api_ready) {
        set_err(err, err_size, "ShadowMount API not available");
        return -1;
    }
    snprintf(body_buf, sizeof(body_buf), "{\"title_id\":\"%s\"}", title_id);
    if (sm_post("/api/v1/games/unmount", body_buf, response_body, http_status,
                err, err_size) != 0) {
        return -1;
    }
    if (sm_json_long(*response_body, "status", -1) != 0) {
        set_err(err, err_size, "ShadowMount API unmount failed");
        return -1;
    }
    return 0;
}

/*
 * Mount wrapper (single-shot). Issues one POST /api/v1/games/mount and
 * trusts the "mounted" boolean in the response. No retry loop, no
 * separate verification request. ShadowMountPlus supports a single
 * active mount, so a 409 ("Device busy") means another game is mounted
 * and the caller must resolve the conflict before retrying.
 */
int
gc_shadowmount_api_mount_game_mode(const char* title_id, const char* mode,
                                   char* err, size_t err_size) {
    char* resp = NULL;
    int http = 0;
    if (err && err_size) err[0] = 0;
    if (!sm_title_id_valid(title_id)) {
        set_err(err, err_size, "bad ShadowMount title id");
        return -1;
    }
    if (mode && *mode && !sm_mode_valid(mode)) {
        set_err(err, err_size, "bad ShadowMount mount mode");
        return -1;
    }
    if (!g_shadowmount_api_ready) {
        set_err(err, err_size, "ShadowMount API not available");
        return -1;
    }
    gc_trace("shadowmount api mount: request title=%s mode=%s",
           title_id, mode && *mode ? mode : "(default)");
    if (sm_mount_once(title_id, mode, &resp, &http, err, err_size) != 0) {
        gc_log("shadowmount api mount: failed title=%s http=%d err=%s",
               title_id, http, (err && err[0]) ? err : "unknown");
        free(resp);
        return -1;
    }
    int mounted = sm_json_bool(resp, "mounted", 0);
    free(resp);
    if (!mounted) {
        set_err(err, err_size, "ShadowMount API mount: not mounted");
        gc_log("shadowmount api mount: not mounted title=%s", title_id);
        return -1;
    }
    gc_trace("shadowmount api mount: ok title=%s", title_id);
    return 0;
}

int
gc_shadowmount_api_mount_game(const char* title_id,
                              char* err, size_t err_size) {
    return gc_shadowmount_api_mount_game_mode(title_id, NULL, err, err_size);
}

/*
 * Unmount wrapper (single-shot). Issues one POST /api/v1/games/unmount
 * and trusts the response status. No retry loop, no verification.
 */
int
gc_shadowmount_api_unmount_game(const char* title_id,
                                char* err, size_t err_size) {
    char* resp = NULL;
    int http = 0;
    if (err && err_size) err[0] = 0;
    if (!sm_title_id_valid(title_id)) {
        set_err(err, err_size, "bad ShadowMount title id");
        return -1;
    }
    if (!g_shadowmount_api_ready) {
        set_err(err, err_size, "ShadowMount API not available");
        return -1;
    }
    gc_trace("shadowmount api unmount: request title=%s", title_id);
    if (sm_unmount_once(title_id, &resp, &http, err, err_size) != 0) {
        gc_log("shadowmount api unmount: failed title=%s http=%d err=%s",
               title_id, http, (err && err[0]) ? err : "unknown");
        free(resp);
        return -1;
    }
    free(resp);
    gc_trace("shadowmount api unmount: ok title=%s", title_id);
    return 0;
}

/*
 * Query /api/v1/games and return the title_id of the first currently
 * mounted game. Returns 1 when a mounted game was found (title_id_out
 * populated), 0 when none is mounted, -1 on error.
 */
int
gc_shadowmount_api_find_mounted_game(char* title_id_out,
                                     size_t title_id_size,
                                     char* err, size_t err_size) {
    char* body = NULL;
    int status = 0;
    const char* cursor;
    char obj[1024];
    int found = 0;
    if (err && err_size) err[0] = 0;
    if (title_id_out && title_id_size) title_id_out[0] = 0;
    if (!g_shadowmount_api_ready) {
        set_err(err, err_size, "ShadowMount API not available");
        return -1;
    }
    if (sm_post("/api/v1/games", "{}", &body, &status, err, err_size) != 0) {
        return -1;
    }
    if (sm_json_long(body, "status", -1) != 0) {
        set_err(err, err_size, "ShadowMount API list games failed");
        free(body);
        return -1;
    }
    cursor = sm_json_array_start(body, "games");
    if (!cursor) {
        free(body);
        return 0;
    }
    while (sm_json_next_object(&cursor, obj, sizeof(obj))) {
        gc_sm_game_t g;
        sm_parse_game(obj, &g);
        if (g.mounted && g.title_id[0]) {
            if (title_id_out && title_id_size) {
                snprintf(title_id_out, title_id_size, "%s", g.title_id);
            }
            found = 1;
            break;
        }
    }
    free(body);
    return found;
}

/* ---- Helper parsers for response schemas not yet decoded above ---- */

static void
sm_parse_storage_mount(const char* obj, gc_sm_storage_mount_t* m) {
    memset(m, 0, sizeof(*m));
    sm_json_string(obj, "source", m->source, sizeof(m->source));
    sm_json_string(obj, "mount_point", m->mount_point,
                   sizeof(m->mount_point));
    sm_json_string(obj, "filesystem", m->filesystem, sizeof(m->filesystem));
    m->total_bytes = sm_json_long(obj, "total_bytes", 0);
    m->free_bytes = sm_json_long(obj, "free_bytes", 0);
    m->available_bytes = sm_json_long(obj, "available_bytes", 0);
    m->used_bytes = sm_json_long(obj, "used_bytes", 0);
    m->read_only = sm_json_bool(obj, "read_only", 0);
}

static void
sm_parse_storage_job(const char* json, gc_sm_storage_job_t* job) {
    memset(job, 0, sizeof(*job));
    job->status = (int)sm_json_long(json, "status", 0);
    job->job_id = sm_json_long(json, "job_id", 0);
    sm_json_string(json, "operation", job->operation, sizeof(job->operation));
    sm_json_string(json, "state", job->state, sizeof(job->state));
    job->active = sm_json_bool(json, "active", 0);
    job->cancellable = sm_json_bool(json, "cancellable", 0);
    job->cancel_requested = sm_json_bool(json, "cancel_requested", 0);
    sm_json_string(json, "title_id", job->title_id, sizeof(job->title_id));
    sm_json_string(json, "source_type", job->source_type,
                   sizeof(job->source_type));
    sm_json_string(json, "source", job->source, sizeof(job->source));
    sm_json_string(json, "runtime_source", job->runtime_source,
                   sizeof(job->runtime_source));
    sm_json_string(json, "destination", job->destination,
                   sizeof(job->destination));
    job->delete_source = sm_json_bool(json, "delete_source", 0);
    job->total_bytes = sm_json_long(json, "total_bytes", 0);
    job->processed_bytes = sm_json_long(json, "processed_bytes", 0);
    job->total_files = sm_json_long(json, "total_files", 0);
    job->processed_files = sm_json_long(json, "processed_files", 0);
    job->progress_percent = sm_json_double(json, "progress_percent", 0.0);
    job->speed_bytes_per_second =
        sm_json_long(json, "speed_bytes_per_second", 0);
    job->elapsed_ms = sm_json_long(json, "elapsed_ms", 0);
    job->affected_titles = (int)sm_json_long(json, "affected_titles", 0);
    job->result_status = (int)sm_json_long(json, "result_status", 0);
    sm_json_string(json, "result_error", job->result_error,
                   sizeof(job->result_error));
    job->scan_queued = sm_json_bool(json, "scan_queued", 0);
}

static void
sm_parse_settings(const char* json, gc_sm_settings_t* s) {
    memset(s, 0, sizeof(*s));
    s->status = (int)sm_json_long(json, "status", 0);
    s->debug = sm_json_bool(json, "debug", 0);
    s->quiet_mode = sm_json_bool(json, "quiet_mode", 0);
    s->update_emulators = sm_json_bool(json, "update_emulators", 0);
    s->auto_update_ampr = sm_json_bool(json, "auto_update_ampr", 0);
    s->auto_remove_missing_games =
        sm_json_bool(json, "auto_remove_missing_games", 0);
    s->auto_remove_missing_delay_seconds =
        (int)sm_json_long(json, "auto_remove_missing_delay_seconds", 0);
    s->allow_lan_access = sm_json_bool(json, "allow_lan_access", 0);
    s->fan_target_temperature =
        (int)sm_json_long(json, "fan_target_temperature", 0);
    s->api_enabled = sm_json_bool(json, "api_enabled", 0);
    s->scan_path_count = (int)sm_json_long(json, "scan_path_count", 0);
}

static void
sm_parse_log_response(const char* json, gc_sm_log_response_t* out) {
    size_t content_len = 0;
    memset(out, 0, sizeof(*out));
    out->status = (int)sm_json_long(json, "status", 0);
    out->file_size = sm_json_long(json, "file_size", 0);
    out->total_bytes = sm_json_long(json, "total_bytes", 0);
    out->returned_bytes = (int)sm_json_long(json, "returned_bytes", 0);
    out->truncated = sm_json_bool(json, "truncated", 0);
    out->content = sm_json_string_alloc(json, "content", &content_len);
    if (!out->content) {
        out->content = malloc(1);
        if (out->content) out->content[0] = 0;
    }
    if (out->returned_bytes == 0 && content_len > 0)
        out->returned_bytes = (int)content_len;
}

static void
sm_parse_manual_update(const char* json, gc_sm_manual_update_t* out) {
    memset(out, 0, sizeof(*out));
    out->status = (int)sm_json_long(json, "status", 0);
    sm_json_string(json, "path", out->path, sizeof(out->path));
    out->present = sm_json_bool(json, "present", 0);
    out->changed = sm_json_bool(json, "changed", 0);
}

/* ---- Missing OpenAPI v1 endpoint implementations ---- */

int
gc_shadowmount_api_scan_reset(int reset_attempts,
                              char* err, size_t err_size) {
    char* body = NULL;
    int status = 0;
    const char* req = reset_attempts
                          ? "{\"reset_attempts\":true}"
                          : "{\"reset_attempts\":false}";
    if (err && err_size) err[0] = 0;
    if (!g_shadowmount_api_ready) {
        set_err(err, err_size, "ShadowMount API not available");
        return -1;
    }
    if (sm_post("/api/v1/scan", req, &body, &status,
                err, err_size) != 0) {
        return -1;
    }
    if (sm_json_long(body, "status", -1) != 0) {
        set_err(err, err_size, "ShadowMount API scan failed");
        free(body);
        return -1;
    }
    free(body);
    return 0;
}

int
gc_shadowmount_api_get_storage_space(gc_sm_storage_space_t* out,
                                     char* err, size_t err_size) {
    char* body = NULL;
    int status = 0;
    const char* cursor;
    char obj[1024];
    int n = 0;
    if (err && err_size) err[0] = 0;
    if (!g_shadowmount_api_ready) {
        set_err(err, err_size, "ShadowMount API not available");
        return -1;
    }
    if (sm_post("/api/v1/storage", "{}", &body, &status,
                err, err_size) != 0) {
        return -1;
    }
    if (sm_json_long(body, "status", -1) != 0) {
        set_err(err, err_size, "ShadowMount API storage failed");
        free(body);
        return -1;
    }
    if (!out) {
        free(body);
        return 0;
    }
    memset(out, 0, sizeof(*out));
    out->status = (int)sm_json_long(body, "status", 0);
    cursor = sm_json_array_start(body, "mounts");
    if (!cursor) {
        free(body);
        return 0;
    }
    while (n < GC_SM_MAX_MOUNTS &&
        sm_json_next_object(&cursor, obj, sizeof(obj))) {
        sm_parse_storage_mount(obj, &out->mounts[n]);
        n++;
    }
    out->count = n;
    free(body);
    return 0;
}

int
gc_shadowmount_api_list_manual_sources(
    char (*paths_out)[GC_SM_PATH_LEN], int max_paths, int* count,
    char* err, size_t err_size) {
    char* body = NULL;
    int status = 0;
    int n = 0;
    if (err && err_size) err[0] = 0;
    if (count) *count = 0;
    if (!g_shadowmount_api_ready) {
        set_err(err, err_size, "ShadowMount API not available");
        return -1;
    }
    if (sm_post("/api/v1/manual/list", "{}", &body, &status,
                err, err_size) != 0) {
        return -1;
    }
    if (sm_json_long(body, "status", -1) != 0) {
        set_err(err, err_size, "ShadowMount API manual list failed");
        free(body);
        return -1;
    }
    if (paths_out && max_paths > 0) {
        n = sm_json_string_array(body, "paths", paths_out, max_paths);
    }
    if (count) *count = n;
    free(body);
    return 0;
}

int
gc_shadowmount_api_add_manual_source(const char* path,
                                     gc_sm_manual_update_t* out,
                                     char* err, size_t err_size) {
    char body_buf[1100];
    char* resp = NULL;
    int http = 0;
    if (err && err_size) err[0] = 0;
    if (!path || path[0] != '/') {
        set_err(err, err_size, "bad manual source path");
        return -1;
    }
    if (!g_shadowmount_api_ready) {
        set_err(err, err_size, "ShadowMount API not available");
        return -1;
    }
    snprintf(body_buf, sizeof(body_buf), "{\"path\":\"%s\"}", path);
    if (sm_post("/api/v1/manual/add", body_buf, &resp, &http,
                err, err_size) != 0) {
        gc_log("shadowmount api manual add: failed path=%s http=%d err=%s",
               path, http, (err && err[0]) ? err : "unknown");
        free(resp);
        return -1;
    }
    if (out && resp) sm_parse_manual_update(resp, out);
    gc_trace("shadowmount api manual add: ok path=%s changed=%d",
           path, out ? out->changed : -1);
    free(resp);
    return 0;
}

int
gc_shadowmount_api_remove_manual_source(const char* path,
                                        gc_sm_manual_update_t* out,
                                        char* err, size_t err_size) {
    char body_buf[1100];
    char* resp = NULL;
    int http = 0;
    if (err && err_size) err[0] = 0;
    if (!path || path[0] != '/') {
        set_err(err, err_size, "bad manual source path");
        return -1;
    }
    if (!g_shadowmount_api_ready) {
        set_err(err, err_size, "ShadowMount API not available");
        return -1;
    }
    snprintf(body_buf, sizeof(body_buf), "{\"path\":\"%s\"}", path);
    if (sm_post("/api/v1/manual/remove", body_buf, &resp, &http,
                err, err_size) != 0) {
        gc_log("shadowmount api manual remove: failed path=%s http=%d err=%s",
               path, http, (err && err[0]) ? err : "unknown");
        free(resp);
        return -1;
    }
    if (out && resp) sm_parse_manual_update(resp, out);
    gc_trace("shadowmount api manual remove: ok path=%s changed=%d",
           path, out ? out->changed : -1);
    free(resp);
    return 0;
}

int
gc_shadowmount_api_get_game_info(const char* title_id,
                                 gc_sm_game_t* out,
                                 char* err, size_t err_size) {
    char body_buf[64];
    char* resp = NULL;
    int http = 0;
    if (err && err_size) err[0] = 0;
    if (!sm_title_id_valid(title_id)) {
        set_err(err, err_size, "bad ShadowMount title id");
        return -1;
    }
    if (!g_shadowmount_api_ready) {
        set_err(err, err_size, "ShadowMount API not available");
        return -1;
    }
    snprintf(body_buf, sizeof(body_buf), "{\"title_id\":\"%s\"}", title_id);
    if (sm_post("/api/v1/games/info", body_buf, &resp, &http,
                err, err_size) != 0) {
        gc_log("shadowmount api game info: failed title=%s http=%d err=%s",
               title_id, http, (err && err[0]) ? err : "unknown");
        free(resp);
        return -1;
    }
    if (out && resp) sm_parse_game(resp, out);
    gc_trace("shadowmount api game info: ok title=%s", title_id);
    free(resp);
    return 0;
}

int
gc_shadowmount_api_get_game_icon(const char* title_id, int want_thumb,
                                 unsigned char** data_out,
                                 size_t* size_out,
                                 char* err, size_t err_size) {
    char path_with_query[256];
    int http = 0;
    if (err && err_size) err[0] = 0;
    if (data_out) *data_out = NULL;
    if (size_out) *size_out = 0;
    if (!sm_title_id_valid(title_id)) {
        set_err(err, err_size, "bad ShadowMount title id");
        return -1;
    }
    if (!g_shadowmount_api_ready) {
        set_err(err, err_size, "ShadowMount API not available");
        return -1;
    }
    if (want_thumb) {
        snprintf(path_with_query, sizeof(path_with_query),
                 "/api/v1/games/icon?title_id=%s&size=thumb", title_id);
    } else {
        snprintf(path_with_query, sizeof(path_with_query),
                 "/api/v1/games/icon?title_id=%s", title_id);
    }
    if (sm_get_binary(path_with_query, data_out, size_out, &http,
                      err, err_size) != 0) {
        gc_log("shadowmount api icon: failed title=%s http=%d err=%s",
               title_id, http, (err && err[0]) ? err : "unknown");
        return -1;
    }
    gc_trace("shadowmount api icon: ok title=%s thumb=%d size=%zu",
           title_id, want_thumb, size_out ? *size_out : 0);
    return 0;
}

int
gc_shadowmount_api_get_settings(gc_sm_settings_t* out,
                                char (*scan_paths_out)[GC_SM_PATH_LEN],
                                int max_scan_paths, int* scan_path_count,
                                char* err, size_t err_size) {
    char* body = NULL;
    int status = 0;
    if (err && err_size) err[0] = 0;
    if (scan_path_count) *scan_path_count = 0;
    if (!g_shadowmount_api_ready) {
        set_err(err, err_size, "ShadowMount API not available");
        return -1;
    }
    if (sm_post("/api/v1/settings", "{}", &body, &status,
                err, err_size) != 0) {
        return -1;
    }
    if (!out) {
        free(body);
        return 0;
    }
    sm_parse_settings(body, out);
    if (scan_paths_out && max_scan_paths > 0) {
        int n = sm_json_string_array(body, "scan_paths",
                                     scan_paths_out, max_scan_paths);
        if (scan_path_count) *scan_path_count = n;
    }
    free(body);
    return 0;
}

int
gc_shadowmount_api_update_settings(const gc_sm_settings_t* settings,
                                   const char* const * scan_paths,
                                   int scan_path_count,
                                   char* err, size_t err_size) {
    char body[SM_API_BODY_MAX + 1];
    int pos = 0;
    int i;
    char* resp = NULL;
    int http = 0;
    if (err && err_size) err[0] = 0;
    if (!settings) {
        set_err(err, err_size, "null settings");
        return -1;
    }
    if (!g_shadowmount_api_ready) {
        set_err(err, err_size, "ShadowMount API not available");
        return -1;
    }
    pos = snprintf(body, sizeof(body),
                   "{\"debug\":%s,\"quiet_mode\":%s,\"update_emulators\":%s,"
                   "\"auto_update_ampr\":%s,\"auto_remove_missing_games\":%s,"
                   "\"auto_remove_missing_delay_seconds\":%d,"
                   "\"allow_lan_access\":%s,\"fan_target_temperature\":%d,"
                   "\"scan_paths\":[",
                   settings->debug ? "true" : "false",
                   settings->quiet_mode ? "true" : "false",
                   settings->update_emulators ? "true" : "false",
                   settings->auto_update_ampr ? "true" : "false",
                   settings->auto_remove_missing_games ? "true" : "false",
                   settings->auto_remove_missing_delay_seconds,
                   settings->allow_lan_access ? "true" : "false",
                   settings->fan_target_temperature);
    if (pos < 0 || (size_t)pos >= sizeof(body)) {
        set_err(err, err_size, "settings update body too large");
        return -1;
    }
    for (i = 0; i < scan_path_count && scan_paths && scan_paths[i]; i++) {
        int n;
        if (i > 0) {
            if ((size_t)pos >= sizeof(body) - 1) {
                set_err(err, err_size, "settings update body too large");
                return -1;
            }
            body[pos++] = ',';
        }
        n = snprintf(body + pos, sizeof(body) - pos, "\"%s\"",
                     scan_paths[i]);
        if (n < 0 || (size_t)n >= sizeof(body) - pos) {
            set_err(err, err_size, "settings update body too large");
            return -1;
        }
        pos += n;
    }
    if ((size_t)pos >= sizeof(body) - 2) {
        set_err(err, err_size, "settings update body too large");
        return -1;
    }
    body[pos++] = ']';
    body[pos++] = '}';
    body[pos] = 0;
    if (sm_post("/api/v1/settings/update", body, &resp, &http,
                err, err_size) != 0) {
        gc_log("shadowmount api settings update: failed http=%d err=%s",
               http, (err && err[0]) ? err : "unknown");
        free(resp);
        return -1;
    }
    gc_trace("shadowmount api settings update: ok");
    free(resp);
    return 0;
}

int
gc_shadowmount_api_get_debug_log(int max_bytes,
                                 gc_sm_log_response_t* out,
                                 char* err, size_t err_size) {
    char body_buf[64];
    char* resp = NULL;
    int http = 0;
    if (err && err_size) err[0] = 0;
    if (!g_shadowmount_api_ready) {
        set_err(err, err_size, "ShadowMount API not available");
        return -1;
    }
    if (max_bytes < 4096) max_bytes = 4096;
    if (max_bytes > 262144) max_bytes = 262144;
    snprintf(body_buf, sizeof(body_buf), "{\"max_bytes\":%d}", max_bytes);
    if (sm_post("/api/v1/debug-log", body_buf, &resp, &http,
                err, err_size) != 0) {
        gc_log("shadowmount api debug-log: failed http=%d err=%s",
               http, (err && err[0]) ? err : "unknown");
        free(resp);
        return -1;
    }
    if (out && resp) sm_parse_log_response(resp, out);
    free(resp);
    return 0;
}

int
gc_shadowmount_api_get_kernel_log(int max_bytes,
                                  gc_sm_log_response_t* out,
                                  char* err, size_t err_size) {
    char body_buf[64];
    char* resp = NULL;
    int http = 0;
    if (err && err_size) err[0] = 0;
    if (!g_shadowmount_api_ready) {
        set_err(err, err_size, "ShadowMount API not available");
        return -1;
    }
    if (max_bytes < 4096) max_bytes = 4096;
    if (max_bytes > 262144) max_bytes = 262144;
    snprintf(body_buf, sizeof(body_buf), "{\"max_bytes\":%d}", max_bytes);
    if (sm_post("/api/v1/kernel-log", body_buf, &resp, &http,
                err, err_size) != 0) {
        gc_trace("shadowmount api kernel-log: failed http=%d err=%s",
               http, (err && err[0]) ? err : "unknown");
        free(resp);
        return -1;
    }
    if (out && resp) sm_parse_log_response(resp, out);
    free(resp);
    return 0;
}

int
gc_shadowmount_api_move_game_source(const char* title_id,
                                    const char* destination_dir,
                                    gc_sm_storage_job_t* out,
                                    char* err, size_t err_size) {
    char body_buf[1600];
    char* resp = NULL;
    int http = 0;
    if (err && err_size) err[0] = 0;
    if (!sm_title_id_valid(title_id)) {
        set_err(err, err_size, "bad ShadowMount title id");
        return -1;
    }
    if (!destination_dir || destination_dir[0] != '/') {
        set_err(err, err_size, "bad destination directory");
        return -1;
    }
    if (!g_shadowmount_api_ready) {
        set_err(err, err_size, "ShadowMount API not available");
        return -1;
    }
    snprintf(body_buf, sizeof(body_buf),
             "{\"title_id\":\"%s\",\"destination_dir\":\"%s\"}",
             title_id, destination_dir);
    if (sm_post("/api/v1/games/move", body_buf, &resp, &http,
                err, err_size) != 0) {
        gc_log("shadowmount api move: failed title=%s http=%d err=%s",
               title_id, http, (err && err[0]) ? err : "unknown");
        free(resp);
        return -1;
    }
    if (out && resp) sm_parse_storage_job(resp, out);
    gc_trace("shadowmount api move: accepted title=%s job_id=%lld",
           title_id, out ? out->job_id : -1);
    free(resp);
    return 0;
}

int
gc_shadowmount_api_copy_game_source(const char* title_id,
                                    const char* destination_dir,
                                    gc_sm_storage_job_t* out,
                                    char* err, size_t err_size) {
    char body_buf[1600];
    char* resp = NULL;
    int http = 0;
    if (err && err_size) err[0] = 0;
    if (!sm_title_id_valid(title_id)) {
        set_err(err, err_size, "bad ShadowMount title id");
        return -1;
    }
    if (!destination_dir || destination_dir[0] != '/') {
        set_err(err, err_size, "bad destination directory");
        return -1;
    }
    if (!g_shadowmount_api_ready) {
        set_err(err, err_size, "ShadowMount API not available");
        return -1;
    }
    snprintf(body_buf, sizeof(body_buf),
             "{\"title_id\":\"%s\",\"destination_dir\":\"%s\"}",
             title_id, destination_dir);
    if (sm_post("/api/v1/games/copy", body_buf, &resp, &http,
                err, err_size) != 0) {
        gc_log("shadowmount api copy: failed title=%s http=%d err=%s",
               title_id, http, (err && err[0]) ? err : "unknown");
        free(resp);
        return -1;
    }
    if (out && resp) sm_parse_storage_job(resp, out);
    gc_trace("shadowmount api copy: accepted title=%s job_id=%lld",
           title_id, out ? out->job_id : -1);
    free(resp);
    return 0;
}

int
gc_shadowmount_api_unpack_game_image(const char* title_id,
                                     const char* destination_dir,
                                     int delete_source,
                                     gc_sm_storage_job_t* out,
                                     char* err, size_t err_size) {
    char body_buf[1700];
    char* resp = NULL;
    int http = 0;
    if (err && err_size) err[0] = 0;
    if (!sm_title_id_valid(title_id)) {
        set_err(err, err_size, "bad ShadowMount title id");
        return -1;
    }
    if (!destination_dir || destination_dir[0] != '/') {
        set_err(err, err_size, "bad destination directory");
        return -1;
    }
    if (!g_shadowmount_api_ready) {
        set_err(err, err_size, "ShadowMount API not available");
        return -1;
    }
    snprintf(body_buf, sizeof(body_buf),
             "{\"title_id\":\"%s\",\"destination_dir\":\"%s\","
             "\"delete_source\":%s}",
             title_id, destination_dir,
             delete_source ? "true" : "false");
    if (sm_post("/api/v1/games/unpack", body_buf, &resp, &http,
                err, err_size) != 0) {
        gc_log("shadowmount api unpack: failed title=%s http=%d err=%s",
               title_id, http, (err && err[0]) ? err : "unknown");
        free(resp);
        return -1;
    }
    if (out && resp) sm_parse_storage_job(resp, out);
    gc_trace("shadowmount api unpack: accepted title=%s job_id=%lld",
           title_id, out ? out->job_id : -1);
    free(resp);
    return 0;
}

int
gc_shadowmount_api_get_storage_job_status(long long job_id,
                                          int has_job_id,
                                          gc_sm_storage_job_t* out,
                                          char* err, size_t err_size) {
    char body_buf[64];
    char* resp = NULL;
    int http = 0;
    if (err && err_size) err[0] = 0;
    if (!g_shadowmount_api_ready) {
        set_err(err, err_size, "ShadowMount API not available");
        return -1;
    }
    if (has_job_id) {
        snprintf(body_buf, sizeof(body_buf),
                 "{\"job_id\":%lld}", job_id);
    } else {
        snprintf(body_buf, sizeof(body_buf), "{}");
    }
    if (sm_post("/api/v1/games/storage/status", body_buf, &resp, &http,
                err, err_size) != 0) {
        gc_log("shadowmount api storage status: failed http=%d err=%s",
               http, (err && err[0]) ? err : "unknown");
        free(resp);
        return -1;
    }
    if (out && resp) sm_parse_storage_job(resp, out);
    free(resp);
    return 0;
}

int
gc_shadowmount_api_cancel_storage_job(long long job_id,
                                      gc_sm_storage_job_t* out,
                                      char* err, size_t err_size) {
    char body_buf[64];
    char* resp = NULL;
    int http = 0;
    if (err && err_size) err[0] = 0;
    if (job_id < 1) {
        set_err(err, err_size, "bad job id");
        return -1;
    }
    if (!g_shadowmount_api_ready) {
        set_err(err, err_size, "ShadowMount API not available");
        return -1;
    }
    snprintf(body_buf, sizeof(body_buf),
             "{\"job_id\":%lld}", job_id);
    if (sm_post("/api/v1/games/storage/cancel", body_buf, &resp, &http,
                err, err_size) != 0) {
        gc_log("shadowmount api storage cancel: failed job=%lld http=%d err=%s",
               job_id, http, (err && err[0]) ? err : "unknown");
        free(resp);
        return -1;
    }
    if (out && resp) sm_parse_storage_job(resp, out);
    gc_trace("shadowmount api storage cancel: ok job=%lld", job_id);
    free(resp);
    return 0;
}

int
gc_shadowmount_api_delete_game_source(const char* title_id,
                                      gc_sm_storage_job_t* out,
                                      char* err, size_t err_size) {
    char body_buf[64];
    char* resp = NULL;
    int http = 0;
    if (err && err_size) err[0] = 0;
    if (!sm_title_id_valid(title_id)) {
        set_err(err, err_size, "bad ShadowMount title id");
        return -1;
    }
    if (!g_shadowmount_api_ready) {
        set_err(err, err_size, "ShadowMount API not available");
        return -1;
    }
    snprintf(body_buf, sizeof(body_buf),
             "{\"title_id\":\"%s\",\"confirm\":true}", title_id);
    if (sm_post("/api/v1/games/delete", body_buf, &resp, &http,
                err, err_size) != 0) {
        gc_log("shadowmount api delete: failed title=%s http=%d err=%s",
               title_id, http, (err && err[0]) ? err : "unknown");
        free(resp);
        return -1;
    }
    if (out && resp) sm_parse_storage_job(resp, out);
    gc_trace("shadowmount api delete: accepted title=%s job_id=%lld",
           title_id, out ? out->job_id : -1);
    free(resp);
    return 0;
}

int
gc_shadowmount_api_uninstall_game(const char* title_id,
                                  char* err, size_t err_size) {
    char body_buf[64];
    char* resp = NULL;
    int http = 0;
    if (err && err_size) err[0] = 0;
    if (!sm_title_id_valid(title_id)) {
        set_err(err, err_size, "bad ShadowMount title id");
        return -1;
    }
    if (!g_shadowmount_api_ready) {
        set_err(err, err_size, "ShadowMount API not available");
        return -1;
    }
    snprintf(body_buf, sizeof(body_buf),
             "{\"title_id\":\"%s\"}", title_id);
    if (sm_post("/api/v1/games/uninstall", body_buf, &resp, &http,
                err, err_size) != 0) {
        gc_log("shadowmount api uninstall: failed title=%s http=%d err=%s",
               title_id, http, (err && err[0]) ? err : "unknown");
        free(resp);
        return -1;
    }
    if (sm_json_long(resp, "status", -1) != 0) {
        set_err(err, err_size, "ShadowMount API uninstall failed");
        free(resp);
        return -1;
    }
    gc_trace("shadowmount api uninstall: ok title=%s", title_id);
    free(resp);
    return 0;
}
