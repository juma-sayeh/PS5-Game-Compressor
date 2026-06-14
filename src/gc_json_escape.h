/*
 * Game Compressor - small JSON string escaping helper.
 */

#pragma once

#include <stdio.h>
#include <stddef.h>

static inline void
gc_json_escape_small(char *out, size_t out_size, const char *in) {
  size_t pos = 0;
  if(out_size == 0) return;
  out[0] = 0;
  for(const unsigned char *p = (const unsigned char *)(in ? in : "");
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
