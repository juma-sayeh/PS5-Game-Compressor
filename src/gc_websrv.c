/*
 * Game Compressor - tiny standalone HTTP server.
 */

#include <ctype.h>
#include <errno.h>
#include <pthread.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <time.h>
#include <unistd.h>

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>

#include "asset.h"
#include "gc_diag.h"
#include "gc_api.h"
#include "websrv.h"

#define HEADER_MAX 65536
#define CLIENT_RCVBUF_SIZE (1024 * 1024)
#define CLIENT_THREAD_STACK_SIZE (1024 * 1024)
#define GC_HANDOFF_TOKEN "gc-local-reload"

static int             g_websrv_srvfd = -1;
static pthread_mutex_t g_websrv_lock = PTHREAD_MUTEX_INITIALIZER;
static volatile sig_atomic_t g_websrv_exit_requested = 0;
static time_t          g_started_at = 0;
static int             g_launcher_runtime_disabled = 0;
static gc_launcher_diag_t g_launcher_diag = {
  .launcher_enabled = 1,
  .launcher_attempted = 0,
  .appinst_init_rc = GC_DIAG_SKIPPED,
  .title_dir_resolved = 0,
  .install_title_dir_resolved = 0,
  .install_title_rc = GC_DIAG_SKIPPED,
  .uninstall_resolved = 0,
  .uninstall_rc = GC_DIAG_SKIPPED,
  .install_all_resolved = 0,
  .install_all_rc = GC_DIAG_SKIPPED,
  .user_app_writable = -1,
  .launcher_install_rc = -1,
  .launcher_final_state = "deferred",
};

static const char *
status_text(int status) {
  switch(status) {
  case 200: return "OK";
  case 201: return "Created";
  case 204: return "No Content";
  case 400: return "Bad Request";
  case 403: return "Forbidden";
  case 404: return "Not Found";
  case 405: return "Method Not Allowed";
  case 409: return "Conflict";
  case 500: return "Internal Server Error";
  default:  return "OK";
  }
}

static const char *
json_bool(int value) {
  return value ? "true" : "false";
}

static const char *
json_tristate(int value) {
  if(value < 0) return "null";
  return value ? "true" : "false";
}

int
websrv_write_all(int fd, const void *data, size_t size) {
  const char *p = data;
  while(size > 0) {
    ssize_t n = send(fd, p, size, 0);
    if(n < 0) {
      if(errno == EINTR) continue;
      return -1;
    }
    if(n == 0) return -1;
    p += n;
    size -= (size_t)n;
  }
  return 0;
}

int
websrv_send_headers(int fd, int status, const char *mime, size_t size,
                    const char *extra) {
  char header[1024];
  int n = snprintf(header, sizeof(header),
                   "HTTP/1.1 %d %s\r\n"
                   "Connection: close\r\n"
                   "Access-Control-Allow-Origin: *\r\n"
                   "Cache-Control: no-store, no-cache, must-revalidate, max-age=0\r\n"
                   "Pragma: no-cache\r\n"
                   "Expires: 0\r\n"
                   "Content-Type: %s\r\n"
                   "Content-Length: %lu\r\n"
                   "%s"
                   "\r\n",
                   status, status_text(status),
                   mime ? mime : "application/octet-stream",
                   (unsigned long)size,
                   extra ? extra : "");
  if(n < 0 || (size_t)n >= sizeof(header)) return -1;
  return websrv_write_all(fd, header, (size_t)n);
}

int
websrv_send(int fd, int status, const char *mime, const void *data,
            size_t size) {
  if(websrv_send_headers(fd, status, mime, size, NULL) != 0) return -1;
  if(size == 0) return 0;
  return websrv_write_all(fd, data, size);
}

int
websrv_send_text(int fd, int status, const char *mime, const char *body) {
  return websrv_send(fd, status, mime, body ? body : "",
                     body ? strlen(body) : 0);
}

static void
json_escape_small(char *out, size_t out_size, const char *message) {
  size_t pos = 0;
  if(out_size == 0) return;
  out[0] = 0;
  for(const unsigned char *p = (const unsigned char *)(message ? message : "");
      *p && pos + 7 < out_size; p++) {
    if(*p == '"' || *p == '\\') {
      out[pos++] = '\\';
      out[pos++] = (char)*p;
    } else if(*p == '\n') {
      out[pos++] = '\\';
      out[pos++] = 'n';
    } else if(*p == '\r') {
      out[pos++] = '\\';
      out[pos++] = 'r';
    } else if(*p == '\t') {
      out[pos++] = '\\';
      out[pos++] = 't';
    } else if(*p < 0x20) {
      int n = snprintf(out + pos, out_size - pos, "\\u%04x", *p);
      if(n < 0 || (size_t)n >= out_size - pos) break;
      pos += (size_t)n;
    } else {
      out[pos++] = (char)*p;
    }
  }
  out[pos] = 0;
}

int
websrv_send_error_json(int fd, int status, const char *message) {
  char escaped[512];
  char body[640];
  json_escape_small(escaped, sizeof(escaped), message);
  snprintf(body, sizeof(body), "{\"ok\":false,\"error\":\"%s\"}", escaped);
  return websrv_send_text(fd, status, "application/json", body);
}

static int
hex_value(int ch) {
  if(ch >= '0' && ch <= '9') return ch - '0';
  if(ch >= 'a' && ch <= 'f') return ch - 'a' + 10;
  if(ch >= 'A' && ch <= 'F') return ch - 'A' + 10;
  return -1;
}

void
websrv_url_decode(char *out, size_t out_size, const char *in) {
  size_t pos = 0;
  if(out_size == 0) return;
  for(size_t i = 0; in && in[i] && pos + 1 < out_size; i++) {
    if(in[i] == '%' && isxdigit((unsigned char)in[i + 1]) &&
       isxdigit((unsigned char)in[i + 2])) {
      int hi = hex_value(in[i + 1]);
      int lo = hex_value(in[i + 2]);
      out[pos++] = (char)((hi << 4) | lo);
      i += 2;
    } else if(in[i] == '+') {
      out[pos++] = ' ';
    } else {
      out[pos++] = in[i];
    }
  }
  out[pos] = 0;
}

int
websrv_get_query_arg(const http_request_t *req, const char *name, char *out,
                     size_t out_size) {
  char query[2048];
  char *saveptr = NULL;
  if(!req || !name || !out || out_size == 0) return 0;
  out[0] = 0;
  snprintf(query, sizeof(query), "%s", req->query);
  for(char *part = strtok_r(query, "&", &saveptr); part;
      part = strtok_r(NULL, "&", &saveptr)) {
    char *eq = strchr(part, '=');
    char key[256];
    if(!eq) continue;
    *eq = 0;
    websrv_url_decode(key, sizeof(key), part);
    if(strcmp(key, name)) continue;
    websrv_url_decode(out, out_size, eq + 1);
    return 1;
  }
  return 0;
}

void
websrv_request_exit(void) {
  pthread_mutex_lock(&g_websrv_lock);
  g_websrv_exit_requested = 1;
  int srvfd = g_websrv_srvfd;
  g_websrv_srvfd = -1;
  pthread_mutex_unlock(&g_websrv_lock);
  if(srvfd >= 0) close(srvfd);
}

int
websrv_exit_requested(void) {
  return g_websrv_exit_requested ? 1 : 0;
}

void
websrv_set_runtime_diag(int launcher_disabled) {
  pthread_mutex_lock(&g_websrv_lock);
  g_launcher_runtime_disabled = launcher_disabled ? 1 : 0;
  g_launcher_diag.launcher_enabled = !g_launcher_runtime_disabled;
  pthread_mutex_unlock(&g_websrv_lock);
}

void
websrv_set_launcher_diag(const gc_launcher_diag_t *diag) {
  if(!diag) return;

  pthread_mutex_lock(&g_websrv_lock);
  g_launcher_diag = *diag;
  g_launcher_diag.launcher_enabled =
      !g_launcher_runtime_disabled && diag->launcher_enabled;
  pthread_mutex_unlock(&g_websrv_lock);
}

static int
status_request(const http_request_t *req) {
  char body[512];
  time_t now = time(NULL);
  long uptime = g_started_at ? (long)(now - g_started_at) : 0;
  int n = snprintf(body, sizeof(body),
                   "{\"ok\":true,\"name\":\"Game Compressor\","
                   "\"version\":\"%s\",\"pid\":%ld,\"uptime\":%ld,"
                   "\"port\":%d}",
#ifdef GAME_COMPRESSOR_VERSION
                   GAME_COMPRESSOR_VERSION,
#else
                   "dev",
#endif
                   (long)getpid(), uptime,
#ifdef GAME_COMPRESSOR_PORT
                   GAME_COMPRESSOR_PORT
#else
                   5910
#endif
                   );
  if(n < 0 || (size_t)n >= sizeof(body)) {
    return websrv_send_error_json(req->fd, 500, "status too large");
  }
  return websrv_send(req->fd, 200, "application/json", body, (size_t)n);
}

static int
diag_request(const http_request_t *req) {
  char checkpoint[192];
  char launcher_final[64];
  char body[2048];
  gc_launcher_diag_t launcher;

  pthread_mutex_lock(&g_websrv_lock);
  launcher = g_launcher_diag;
  pthread_mutex_unlock(&g_websrv_lock);

  json_escape_small(checkpoint, sizeof(checkpoint), gc_diag_checkpoint());
  json_escape_small(launcher_final, sizeof(launcher_final),
                    launcher.launcher_final_state);
  int n = snprintf(body, sizeof(body),
                   "{\"ok\":true,\"app\":\"Game Compressor\","
                   "\"checkpoint\":\"%s\","
                   "\"launcher\":{"
                   "\"launcher_enabled\":%s,"
                   "\"launcher_attempted\":%s,"
                   "\"appinst_init_rc\":%d,"
                   "\"title_dir_resolved\":%s,"
                   "\"install_title_dir_resolved\":%s,"
                   "\"install_title_rc\":%d,"
                   "\"uninstall_resolved\":%s,"
                   "\"uninstall_rc\":%d,"
                   "\"install_all_resolved\":%s,"
                   "\"install_all_rc\":%d,"
                   "\"user_app_writable\":%s,"
                   "\"launcher_install_rc\":%d,"
                   "\"launcher_final_state\":\"%s\"},"
                   "\"routes\":[\"/\",\"/api/status\",\"/api/diag\","
                   "\"/api/control/shutdown\","
                   "\"/api/control/handoff-state\","
                   "\"/api/control/handoff-shutdown\","
                   "\"/api/gc/games\",\"/api/gc/usb\","
		                   "\"/api/gc/history\",\"/api/gc/job\","
		                   "\"/api/gc/job/cancel\",\"/api/gc/queue/cancel\","
		                   "\"/api/gc/bad-blocks\","
		                   "\"/api/gc/uncompress-plan\","
		                   "\"/api/gc/compress\",\"/api/gc/uncompress\","
	                   "\"/api/gc/validate-repair\",\"/api/gc/validate-only\","
	                   "\"/api/gc/refresh-mount\","
                   "\"/api/gc/move-to-usb\","
                   "\"/api/gc/move-to-internal\","
	                   "\"/api/gc/delete-game-data\","
	                   "\"/api/gc/read-speed-test\","
	                   "\"/api/gc/read-eof-test\","
	                   "\"/api/gc/original-restore\"]}",
                   checkpoint,
                   json_bool(launcher.launcher_enabled),
                   json_bool(launcher.launcher_attempted),
                   launcher.appinst_init_rc,
                   json_bool(launcher.title_dir_resolved),
                   json_bool(launcher.install_title_dir_resolved),
                   launcher.install_title_rc,
                   json_bool(launcher.uninstall_resolved),
                   launcher.uninstall_rc,
                   json_bool(launcher.install_all_resolved),
                   launcher.install_all_rc,
                   json_tristate(launcher.user_app_writable),
                   launcher.launcher_install_rc,
                   launcher_final);
  if(n < 0 || (size_t)n >= sizeof(body)) {
    return websrv_send_error_json(req->fd, 500, "diag too large");
  }
  return websrv_send(req->fd, 200, "application/json", body, (size_t)n);
}

static int
shutdown_request(const http_request_t *req) {
  const char body[] = "{\"ok\":true,\"shutdown\":true}";
  int rc = websrv_send(req->fd, 200, "application/json", body,
                       sizeof(body) - 1);
  websrv_request_exit();
  return rc;
}

static int
handoff_shutdown_request(const http_request_t *req) {
  char token[64];
  char mode[32];
  if(!websrv_get_query_arg(req, "token", token, sizeof(token)) ||
     strcmp(token, GC_HANDOFF_TOKEN)) {
    return websrv_send_error_json(req->fd, 403, "bad handoff token");
  }
  if(!websrv_get_query_arg(req, "mode", mode, sizeof(mode))) {
    snprintf(mode, sizeof(mode), "%s", "reload");
  }
  if(strcmp(mode, "reload") && strcmp(mode, "takeover")) {
    snprintf(mode, sizeof(mode), "%s", "reload");
  }
  char body[128];
  int n = snprintf(body, sizeof(body),
                   "{\"ok\":true,\"shutdown\":true,\"handoff\":true,"
                   "\"mode\":\"%s\"}", mode);
  if(n < 0 || (size_t)n >= sizeof(body)) {
    return websrv_send_error_json(req->fd, 500, "handoff response too large");
  }
  int rc = websrv_send(req->fd, 200, "application/json", body, (size_t)n);
  websrv_request_exit();
  return rc;
}

static int
dispatch_request(const http_request_t *req) {
  if(!strcmp(req->method, "GET") || !strcmp(req->method, "HEAD")) {
    if(!strcmp(req->path, "/") || !strcmp(req->path, "/index.html")) {
      return asset_request(req, "/game-compressor.html");
    }
    if(!strcmp(req->path, "/api/status") || !strcmp(req->path, "/api/version")) {
      return status_request(req);
    }
    if(!strcmp(req->path, "/api/diag")) {
      return diag_request(req);
    }
    if(!strcmp(req->path, "/api/control/shutdown")) {
      return shutdown_request(req);
    }
    if(!strcmp(req->path, "/api/control/handoff-state")) {
      return gc_api_handoff_state_request(req);
    }
    if(!strcmp(req->path, "/api/control/handoff-shutdown")) {
      return handoff_shutdown_request(req);
    }
    if(!strcmp(req->path, "/api/gc/icon")) {
      return gc_api_icon_request(req);
    }
    if(!strncmp(req->path, "/api/gc/", 8)) {
      return gc_api_request(req, req->path);
    }
    return asset_request(req, req->path);
  }

  if(!strcmp(req->method, "POST")) {
    if(!strcmp(req->path, "/api/gc/job/cancel") ||
       !strcmp(req->path, "/api/gc/queue/cancel") ||
	       !strcmp(req->path, "/api/gc/compress") ||
	       !strcmp(req->path, "/api/gc/uncompress") ||
	       !strcmp(req->path, "/api/gc/validate-repair") ||
	       !strcmp(req->path, "/api/gc/validate-only") ||
       !strcmp(req->path, "/api/gc/refresh-mount") ||
       !strcmp(req->path, "/api/gc/move-to-usb") ||
       !strcmp(req->path, "/api/gc/move-to-internal") ||
       !strcmp(req->path, "/api/gc/copy-to-usb") ||
	       !strcmp(req->path, "/api/gc/copy-to-internal") ||
	       !strcmp(req->path, "/api/gc/delete-game-data") ||
	       !strcmp(req->path, "/api/gc/read-speed-test") ||
	       !strcmp(req->path, "/api/gc/read-eof-test") ||
	       !strcmp(req->path, "/api/gc/original-restore")) {
	      return gc_api_request(req, req->path);
	    }
    return websrv_send_error_json(req->fd, 404, "not found");
  }

  return websrv_send_error_json(req->fd, 405, "method not allowed");
}

static char *
find_header_end(char *buf, size_t len) {
  for(size_t i = 3; i < len; i++) {
    if(buf[i - 3] == '\r' && buf[i - 2] == '\n' &&
       buf[i - 1] == '\r' && buf[i] == '\n') {
      return buf + i + 1;
    }
  }
  return NULL;
}

static size_t
parse_content_length(const char *headers) {
  const char *p = headers;
  while(p && *p) {
    const char *line_end = strstr(p, "\r\n");
    size_t len = line_end ? (size_t)(line_end - p) : strlen(p);
    if(len > 15 && !strncasecmp(p, "Content-Length:", 15)) {
      const char *v = p + 15;
      while(*v == ' ' || *v == '\t') v++;
      return (size_t)strtoul(v, NULL, 10);
    }
    if(!line_end) break;
    p = line_end + 2;
  }
  return 0;
}

static void
drain_request_body(int fd, size_t already, size_t total) {
  char buf[4096];
  size_t remaining = total > already ? total - already : 0;
  while(remaining > 0) {
    size_t want = remaining < sizeof(buf) ? remaining : sizeof(buf);
    ssize_t n = recv(fd, buf, want, 0);
    if(n < 0) {
      if(errno == EINTR) continue;
      break;
    }
    if(n == 0) break;
    remaining -= (size_t)n;
  }
}

static void *
client_thread(void *arg) {
  int fd = (int)(intptr_t)arg;
  char *buf = malloc(HEADER_MAX + 1);
  if(!buf) {
    close(fd);
    return NULL;
  }

  size_t len = 0;
  char *header_end = NULL;
  while(len < HEADER_MAX) {
    ssize_t n = recv(fd, buf + len, HEADER_MAX - len, 0);
    if(n < 0) {
      if(errno == EINTR) continue;
      free(buf);
      close(fd);
      return NULL;
    }
    if(n == 0) break;
    len += (size_t)n;
    header_end = find_header_end(buf, len);
    if(header_end) break;
  }
  if(!header_end) {
    websrv_send_error_json(fd, 400, "bad request");
    free(buf);
    close(fd);
    return NULL;
  }
  buf[len] = 0;

  char *line_end = strstr(buf, "\r\n");
  if(!line_end) {
    websrv_send_error_json(fd, 400, "bad request");
    free(buf);
    close(fd);
    return NULL;
  }
  *line_end = 0;

  http_request_t req;
  memset(&req, 0, sizeof(req));
  req.fd = fd;
  char url[2048];
  if(sscanf(buf, "%7s %2047s", req.method, url) != 2) {
    websrv_send_error_json(fd, 400, "bad request");
    free(buf);
    close(fd);
    return NULL;
  }
  char *q = strchr(url, '?');
  if(q) {
    *q = 0;
    snprintf(req.query, sizeof(req.query), "%s", q + 1);
  }
  websrv_url_decode(req.path, sizeof(req.path), url);

  size_t content_len = parse_content_length(line_end + 2);
  size_t header_bytes = (size_t)(header_end - buf);
  size_t initial_body = len > header_bytes ? len - header_bytes : 0;
  if(content_len > initial_body) {
    drain_request_body(fd, initial_body, content_len);
  }

  dispatch_request(&req);
  free(buf);
  close(fd);
  return NULL;
}

int
websrv_listen(unsigned short port, websrv_ready_cb_t ready_cb, void *ready_arg) {
  int srvfd = socket(AF_INET, SOCK_STREAM, 0);
  if(srvfd < 0) return -1;

  int yes = 1;
  setsockopt(srvfd, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(yes));

  struct sockaddr_in addr;
  memset(&addr, 0, sizeof(addr));
  addr.sin_family = AF_INET;
  addr.sin_port = htons(port);
  addr.sin_addr.s_addr = htonl(INADDR_ANY);
  if(bind(srvfd, (struct sockaddr *)&addr, sizeof(addr)) != 0) {
    close(srvfd);
    return -1;
  }
  if(listen(srvfd, 64) != 0) {
    close(srvfd);
    return -1;
  }

  pthread_mutex_lock(&g_websrv_lock);
  g_websrv_srvfd = srvfd;
  g_started_at = time(NULL);
  pthread_mutex_unlock(&g_websrv_lock);

  if(ready_cb) ready_cb(port, ready_arg);

  while(!g_websrv_exit_requested) {
    struct sockaddr_in cli;
    socklen_t cli_len = sizeof(cli);
    int fd = accept(srvfd, (struct sockaddr *)&cli, &cli_len);
    if(fd < 0) {
      if(errno == EINTR) continue;
      if(g_websrv_exit_requested) break;
      continue;
    }

    int rcvbuf = CLIENT_RCVBUF_SIZE;
    setsockopt(fd, SOL_SOCKET, SO_RCVBUF, &rcvbuf, sizeof(rcvbuf));

    pthread_t t;
    pthread_attr_t at;
    pthread_attr_init(&at);
    pthread_attr_setdetachstate(&at, PTHREAD_CREATE_DETACHED);
    pthread_attr_setstacksize(&at, CLIENT_THREAD_STACK_SIZE);
    int rc = pthread_create(&t, &at, client_thread, (void *)(intptr_t)fd);
    pthread_attr_destroy(&at);
    if(rc != 0) {
      close(fd);
    }
  }

  pthread_mutex_lock(&g_websrv_lock);
  if(g_websrv_srvfd == srvfd) g_websrv_srvfd = -1;
  pthread_mutex_unlock(&g_websrv_lock);
  close(srvfd);
  return 0;
}
