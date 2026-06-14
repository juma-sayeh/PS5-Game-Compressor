/*
 * Game Compressor - standalone payload entrypoint.
 */

#include <arpa/inet.h>
#include <errno.h>
#include <ifaddrs.h>
#include <net/if.h>
#include <netinet/in.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <sys/types.h>
#include <unistd.h>

#include "gc_app_installer.h"
#include "gc_api.h"
#include "gc_diag.h"
#include "gc_notify.h"
#include "gc_power_guard.h"
#include "websrv.h"

#ifndef GAME_COMPRESSOR_PORT
#define GAME_COMPRESSOR_PORT 5910
#endif

#define LISTEN_RETRY_COUNT 5
#define LISTEN_RETRY_SECONDS 1
#define LOCAL_HTTP_TIMEOUT_SECONDS 1
#define HANDOFF_WAIT_SECONDS 8

typedef enum handoff_result {
  HANDOFF_CONTINUE = 0,
  HANDOFF_EXIT = 1,
} handoff_result_t;

static void
detect_lan_ip(char *out, size_t out_size) {
  struct ifaddrs *ifaddr = NULL;

  snprintf(out, out_size, "<PS5_IP>");
  if(getifaddrs(&ifaddr) != 0) return;

  for(struct ifaddrs *ifa = ifaddr; ifa; ifa = ifa->ifa_next) {
    if(!ifa->ifa_addr || ifa->ifa_addr->sa_family != AF_INET) continue;
    if(ifa->ifa_flags & IFF_LOOPBACK) continue;

    struct sockaddr_in *sa = (struct sockaddr_in *)ifa->ifa_addr;
    const char *ip = inet_ntop(AF_INET, &sa->sin_addr, out, out_size);
    if(ip && strncmp(out, "169.254.", 8) != 0) {
      freeifaddrs(ifaddr);
      return;
    }
  }

  freeifaddrs(ifaddr);
}

static void
on_ready(unsigned short port, void *arg) {
  (void)arg;
  char ip[64];
  detect_lan_ip(ip, sizeof(ip));
  gc_checkpoint("web server ready");
  gc_log("web ui ready http://%s:%u/", ip, (unsigned)port);
  gc_launcher_start();
}

static int
send_all_local(int fd, const char *data, size_t size) {
  size_t off = 0;
  while(off < size) {
    ssize_t n = send(fd, data + off, size - off, 0);
    if(n < 0) {
      if(errno == EINTR) continue;
      return -1;
    }
    if(n == 0) return -1;
    off += (size_t)n;
  }
  return 0;
}

static int
connect_loopback(unsigned short port) {
  int fd = socket(AF_INET, SOCK_STREAM, 0);
  if(fd < 0) return -1;

  struct timeval timeout;
  timeout.tv_sec = LOCAL_HTTP_TIMEOUT_SECONDS;
  timeout.tv_usec = 0;
  setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &timeout, sizeof(timeout));
  setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout));

  struct sockaddr_in addr;
  memset(&addr, 0, sizeof(addr));
  addr.sin_family = AF_INET;
  addr.sin_port = htons(port);
  addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
  if(connect(fd, (struct sockaddr *)&addr, sizeof(addr)) != 0) {
    close(fd);
    return -1;
  }
  return fd;
}

static int
local_http_get(unsigned short port, const char *path,
               char *body, size_t body_size) {
  int fd = connect_loopback(port);
  if(fd < 0) return -1;

  char request[1536];
  int n = snprintf(request, sizeof(request),
                   "GET %s HTTP/1.1\r\n"
                   "Host: 127.0.0.1\r\n"
                   "Connection: close\r\n"
                   "\r\n",
                   path ? path : "/");
  if(n < 0 || (size_t)n >= sizeof(request)) {
    close(fd);
    return -1;
  }

  int rc = send_all_local(fd, request, (size_t)n);
  char response[8192];
  size_t used = 0;
  while(rc == 0) {
    ssize_t got = recv(fd, response + used, sizeof(response) - 1 - used, 0);
    if(got < 0 && errno == EINTR) continue;
    if(got <= 0) break;
    used += (size_t)got;
    if(used >= sizeof(response) - 1) break;
  }
  close(fd);
  if(rc != 0) return -1;
  response[used] = 0;
  char *p = strstr(response, "\r\n\r\n");
  p = p ? p + 4 : response;
  if(body && body_size) {
    snprintf(body, body_size, "%s", p);
  }
  return 0;
}

static int
local_port_open(unsigned short port) {
  int fd = connect_loopback(port);
  if(fd < 0) return 0;
  close(fd);
  return 1;
}

static int
wait_for_old_server_down(unsigned short port) {
  for(int i = 0; i < HANDOFF_WAIT_SECONDS; i++) {
    if(!local_port_open(port)) return 0;
    sleep(1);
  }
  return local_port_open(port) ? -1 : 0;
}

static const char *
json_find_field(const char *json, const char *name) {
  char pattern[96];
  int n = snprintf(pattern, sizeof(pattern), "\"%s\"", name ? name : "");
  if(n < 0 || (size_t)n >= sizeof(pattern)) return NULL;
  const char *p = strstr(json ? json : "", pattern);
  if(!p) return NULL;
  p += (size_t)n;
  while(*p == ' ' || *p == '\t' || *p == '\r' || *p == '\n') p++;
  if(*p != ':') return NULL;
  p++;
  while(*p == ' ' || *p == '\t' || *p == '\r' || *p == '\n') p++;
  return p;
}

static int
json_bool_field(const char *json, const char *name, int default_value) {
  const char *p = json_find_field(json, name);
  if(!p) return default_value;
  if(!strncmp(p, "true", 4)) return 1;
  if(!strncmp(p, "false", 5)) return 0;
  return default_value;
}

static long
json_long_field(const char *json, const char *name, long default_value) {
  const char *p = json_find_field(json, name);
  char *end = NULL;
  if(!p) return default_value;
  errno = 0;
  long value = strtol(p, &end, 10);
  if(errno != 0 || end == p) return default_value;
  return value;
}

static int
json_string_field(const char *json, const char *name,
                  char *out, size_t out_size) {
  const char *p = json_find_field(json, name);
  size_t pos = 0;
  if(!out || out_size == 0) return 0;
  out[0] = 0;
  if(!p || *p != '"') return 0;
  p++;
  while(*p && *p != '"' && pos + 1 < out_size) {
    if(*p == '\\' && p[1]) {
      p++;
      if(*p == 'n') out[pos++] = '\n';
      else if(*p == 'r') out[pos++] = '\r';
      else if(*p == 't') out[pos++] = '\t';
      else out[pos++] = *p;
      p++;
      continue;
    }
    out[pos++] = *p++;
  }
  out[pos] = 0;
  return 1;
}

static int
handoff_phase_resumable(const char *action, const char *phase) {
  if(!strcmp(action ? action : "", "validate-repair") ||
     !strcmp(action ? action : "", "validate-only")) {
    return 1;
  }
  if(!strcmp(action ? action : "", "compress")) {
    return !strcmp(phase ? phase : "", "compressed") ||
           !strcmp(phase ? phase : "", "source-deleted") ||
           !strcmp(phase ? phase : "", "repairing");
  }
  return 0;
}

static int
request_handoff_shutdown(unsigned short port, int takeover,
                         int supports_handoff) {
  char body[1024];
  char path[256];
  if(supports_handoff) {
    snprintf(path, sizeof(path),
             "/api/control/handoff-shutdown?token=gc-local-reload&mode=%s",
             takeover ? "takeover" : "reload");
    if(local_http_get(port, path, body, sizeof(body)) == 0 &&
       json_bool_field(body, "ok", 0)) {
      return 0;
    }
  }
  return local_http_get(port, "/api/control/shutdown", body, sizeof(body));
}

static handoff_result_t
handoff_existing_instance(unsigned short port) {
  char body[4096];
  char action[64] = {0};
  char phase[64] = {0};
  char reason[256] = {0};
  int supports_handoff = 0;
  int busy = 0;
  int resumable = 0;
  int pending_count = 0;

  if(local_http_get(port, "/api/control/handoff-state",
                    body, sizeof(body)) == 0 &&
     json_bool_field(body, "ok", 0)) {
    supports_handoff = 1;
    busy = json_bool_field(body, "busy", 0);
    resumable = json_bool_field(body, "resumable", 0);
    pending_count = (int)json_long_field(body, "pendingCount", 0);
    json_string_field(body, "action", action, sizeof(action));
    json_string_field(body, "phase", phase, sizeof(phase));
    json_string_field(body, "reason", reason, sizeof(reason));
  } else if(local_http_get(port, "/api/gc/job", body, sizeof(body)) == 0 &&
            json_bool_field(body, "ok", 0)) {
    busy = json_bool_field(body, "busy", 0);
    json_string_field(body, "verb", action, sizeof(action));
    json_string_field(body, "phase", phase, sizeof(phase));
    resumable = handoff_phase_resumable(action, phase);
    snprintf(reason, sizeof(reason), "%s",
             resumable ? "active work can resume automatically" :
             "active work is not resumable");
  } else if(local_http_get(port, "/api/status", body, sizeof(body)) != 0) {
    return HANDOFF_CONTINUE;
  }

  if(busy && !resumable) {
    gc_log("handoff skipped busy non-resumable action=%s phase=%s pending=%d reason=%s",
           action, phase, pending_count, reason);
    gc_notify_message("Reload skipped", "Current job is still running");
    return HANDOFF_EXIT;
  }

  if(request_handoff_shutdown(port, busy && resumable, supports_handoff) != 0 ||
     wait_for_old_server_down(port) != 0) {
    gc_log("handoff failed to stop previous instance busy=%d resumable=%d",
           busy, resumable);
    gc_notify_message("Reload skipped", "Previous app stayed open");
    return HANDOFF_EXIT;
  }

  if(busy && resumable) {
    gc_log("handoff takeover accepted action=%s phase=%s pending=%d",
           action, phase, pending_count);
    gc_notify_message("Reloading", "Work will resume");
  } else {
    gc_log("handoff stopped idle previous instance");
    gc_notify_message("Ready", NULL);
  }
  return HANDOFF_CONTINUE;
}

int
main(int argc, char **argv) {
  gc_diag_init();
  gc_diag_install_signal_handlers();
  gc_checkpoint("process start");
  gc_log("PID=%ld argc=%d", (long)getpid(), argc);
  for(int i = 0; i < argc; i++) {
    gc_log("argv[%d]=%s", i, argv && argv[i] ? argv[i] : "(null)");
  }

  if(handoff_existing_instance((unsigned short)GAME_COMPRESSOR_PORT) ==
     HANDOFF_EXIT) {
    gc_log("handoff requested this instance to exit");
    return 0;
  }

  gc_power_guard_start();
  gc_api_recover_on_startup();

  int rc = -1;
  for(int attempt = 1; attempt <= LISTEN_RETRY_COUNT; attempt++) {
    gc_checkpoint("web listen starting");
    rc = websrv_listen((unsigned short)GAME_COMPRESSOR_PORT, on_ready, NULL);
    gc_log("websrv_listen returned rc=%d attempt=%d", rc, attempt);
    if(rc == 0) return 0;
    sleep(LISTEN_RETRY_SECONDS);
  }
  return rc;
}
