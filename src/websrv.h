/*
 * Game Compressor - tiny HTTP server helpers.
 */

#pragma once

#include <stddef.h>


typedef struct http_request {
  int  fd;
  char method[8];
  char path[1024];
  char query[2048];
} http_request_t;

typedef void (*websrv_ready_cb_t)(unsigned short port, void *arg);

typedef struct gc_launcher_diag {
  int launcher_enabled;
  int launcher_attempted;
  int appinst_init_rc;
  int title_dir_resolved;
  int install_title_dir_resolved;
  int install_title_rc;
  int uninstall_resolved;
  int uninstall_rc;
  int install_all_resolved;
  int install_all_rc;
  int user_app_writable;
  int launcher_install_rc;
  char launcher_final_state[32];
} gc_launcher_diag_t;


int websrv_send(int fd, int status, const char *mime,
                const void *data, size_t size);

int websrv_send_error_json(int fd, int status, const char *message);

int websrv_get_query_arg(const http_request_t *req, const char *name,
                         char *out, size_t out_size);

int websrv_listen(unsigned short port, websrv_ready_cb_t ready_cb,
                  void *ready_arg);

void websrv_set_launcher_diag(const gc_launcher_diag_t *diag);
