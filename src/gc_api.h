/*
 * Game Compressor - narrow API surface.
 */

#pragma once

#include "websrv.h"

int gc_api_request(const http_request_t *req, const char *url);
int gc_api_icon_request(const http_request_t *req);
int gc_api_handoff_state_request(const http_request_t *req);
void gc_api_recover_on_startup(void);
