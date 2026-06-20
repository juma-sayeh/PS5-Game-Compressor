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
  char *body;
  size_t body_size;
} http_request_t;

typedef void (*websrv_ready_cb_t)(unsigned short port, void *arg);

int websrv_send(int fd, int status, const char *mime,
                const void *data, size_t size);

int websrv_send_error_json(int fd, int status, const char *message);

int websrv_get_query_arg(const http_request_t *req, const char *name,
                         char *out, size_t out_size);

int websrv_listen(unsigned short port, websrv_ready_cb_t ready_cb,
                  void *ready_arg);
