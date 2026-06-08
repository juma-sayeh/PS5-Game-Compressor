/*
 * Game Compressor - PS5 system notifications.
 */

#include <stdbool.h>
#include <stdio.h>
#include <string.h>
#include <strings.h>

#define SCE_NOTIFICATION_LOCAL_USER_ID_SYSTEM 0xFE

#include "gc_notify.h"

#define GC_NOTIFY_APP_TITLE "Game Compressor"

typedef struct notify_request {
  char reserved[45];
  char message[3075];
} notify_request_t;

int sceNotificationSend(int user_id, bool is_logged, const char *payload);
int sceKernelSendNotificationRequest(int, notify_request_t *, size_t,
                                     int) __attribute__((weak));

static const char *
action_label(const char *action) {
  if(!strcmp(action ? action : "", "compress")) return "Compress";
  if(!strcmp(action ? action : "", "uncompress")) return "Unpack";
  if(!strcmp(action ? action : "", "validate-repair")) return "Repair";
  if(!strcmp(action ? action : "", "validate-only")) return "Validate";
  if(!strcmp(action ? action : "", "move-to-usb")) return "Move";
  if(!strcmp(action ? action : "", "move-to-internal")) return "Move";
  return "Operation";
}

static const char *
action_past_label(const char *action) {
  if(!strcmp(action ? action : "", "compress")) return "Compressed";
  if(!strcmp(action ? action : "", "uncompress")) return "Unpacked";
  if(!strcmp(action ? action : "", "validate-repair")) {
    return "Validated and repaired";
  }
  if(!strcmp(action ? action : "", "validate-only")) return "Validated";
  if(!strcmp(action ? action : "", "move-to-usb")) return "Moved to USB";
  if(!strcmp(action ? action : "", "move-to-internal")) {
    return "Moved to internal storage";
  }
  return "Completed";
}

static const char *
action_cancel_label(const char *action) {
  if(!strcmp(action ? action : "", "compress")) return "Cancelled compression";
  if(!strcmp(action ? action : "", "uncompress")) return "Cancelled unpack";
  if(!strcmp(action ? action : "", "validate-repair")) {
    return "Cancelled validation and repair";
  }
  if(!strcmp(action ? action : "", "validate-only")) {
    return "Cancelled validation";
  }
  if(!strcmp(action ? action : "", "move-to-usb")) return "Cancelled USB move";
  if(!strcmp(action ? action : "", "move-to-internal")) {
    return "Cancelled internal move";
  }
  return "Cancelled operation";
}

static int
contains_ci(const char *haystack, const char *needle) {
  size_t needle_len = strlen(needle ? needle : "");
  if(!haystack || !needle_len) return 0;
  for(const char *p = haystack; *p; p++) {
    if(!strncasecmp(p, needle, needle_len)) return 1;
  }
  return 0;
}

static int
message_is_failure(const char *message) {
  if(!message || !message[0]) return 0;
  return contains_ci(message, "failed") ||
         contains_ci(message, "could not") ||
         contains_ci(message, "error");
}

static size_t
utf8_len(unsigned char c) {
  if(c < 0x80) return 1;
  if((c & 0xe0) == 0xc0) return 2;
  if((c & 0xf0) == 0xe0) return 3;
  if((c & 0xf8) == 0xf0) return 4;
  return 1;
}

static void
compact_text(char *out, size_t out_size, const char *in, size_t max_chars) {
  const unsigned char *p = (const unsigned char *)(in ? in : "");
  size_t pos = 0;
  size_t chars = 0;
  int pending_space = 0;
  int truncated = 0;

  if(out_size == 0) return;
  out[0] = 0;

  while(*p) {
    if(*p == ' ' || *p == '\t' || *p == '\n' || *p == '\r') {
      if(pos > 0) pending_space = 1;
      p++;
      continue;
    }
    if(*p < 0x20) {
      p++;
      continue;
    }

    if(chars >= max_chars) {
      truncated = 1;
      break;
    }
    if(pending_space) {
      if(pos + 1 >= out_size) {
        truncated = 1;
        break;
      }
      out[pos++] = ' ';
      chars++;
      pending_space = 0;
      if(chars >= max_chars) {
        truncated = 1;
        break;
      }
    }

    size_t len = utf8_len(*p);
    for(size_t i = 1; i < len; i++) {
      if(!p[i] || (p[i] & 0xc0) != 0x80) {
        len = 1;
        break;
      }
    }
    if(pos + len >= out_size) {
      truncated = 1;
      break;
    }
    memcpy(out + pos, p, len);
    pos += len;
    chars++;
    p += len;
  }

  while(pos > 0 && out[pos - 1] == ' ') pos--;
  if(truncated && out_size >= 4) {
    while(pos > 0 && pos + 4 > out_size) pos--;
    if(pos > 0 && pos + 4 <= out_size) {
      memcpy(out + pos, "...", 4);
      return;
    }
  }
  out[pos] = 0;
}

static void
toast_escape(char *out, size_t out_size, const char *in) {
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

static int
gc_notify_debug(const char *message, const char *submessage) {
  notify_request_t req;

  if(!sceKernelSendNotificationRequest) return -1;
  memset(&req, 0, sizeof(req));
  if(submessage && submessage[0]) {
    snprintf(req.message, sizeof(req.message), "%s\n%s",
             message ? message : GC_NOTIFY_APP_TITLE, submessage);
  } else {
    snprintf(req.message, sizeof(req.message), "%s",
             message ? message : GC_NOTIFY_APP_TITLE);
  }
  if(req.message[0]) {
    return sceKernelSendNotificationRequest(0, &req, sizeof(req), 0);
  }
  return 0;
}

static int
gc_notify_toast(const char *message, const char *submessage) {
  char payload[4096];
  char escaped_message[1024];
  char escaped_submessage[1024];
  int n;

  toast_escape(escaped_message, sizeof(escaped_message),
               message ? message : GC_NOTIFY_APP_TITLE);
  toast_escape(escaped_submessage, sizeof(escaped_submessage),
               submessage ? submessage : "");
  n = snprintf(payload, sizeof(payload),
               "{"
               "\"rawData\":{"
               "\"viewTemplateType\":\"InteractiveToastTemplateB\","
               "\"channelType\":\"Downloads\","
               "\"useCaseId\":\"IDC\","
               "\"toastOverwriteType\":\"No\","
               "\"isImmediate\":true,"
               "\"priority\":100,"
               "\"viewData\":{"
               "\"icon\":{\"type\":\"Predefined\",\"parameters\":{\"icon\":\"download\"}},"
               "\"message\":{\"body\":\"%s\"},"
               "\"subMessage\":{\"body\":\"%s\"}"
               "},"
               "\"platformViews\":{"
               "\"previewDisabled\":{"
               "\"viewData\":{"
               "\"icon\":{\"type\":\"Predefined\",\"parameters\":{\"icon\":\"download\"}},"
               "\"message\":{\"body\":\"%s\"}"
               "}"
               "}"
               "}"
               "},"
               "\"localNotificationId\":\"5910\""
               "}",
               escaped_message, escaped_submessage, escaped_message);
  if(n < 0 || (size_t)n >= sizeof(payload)) return -1;
  return sceNotificationSend(SCE_NOTIFICATION_LOCAL_USER_ID_SYSTEM, true,
                             payload);
}

static void
gc_notify_send(const char *message, const char *submessage) {
  char compact_message[96];
  char compact_submessage[192];

  compact_text(compact_message, sizeof(compact_message),
               message ? message : GC_NOTIFY_APP_TITLE, 36);
  compact_text(compact_submessage, sizeof(compact_submessage),
               submessage ? submessage : "", 72);
  if(gc_notify_toast(compact_message, compact_submessage) == 0) return;
  (void)gc_notify_debug(compact_message, compact_submessage);
}

void
gc_notify_operation_done(const char *action, const char *game,
                         const char *status, const char *detail) {
  const char *name = (game && game[0]) ? game : "Game";
  const char *label = action_label(action);
  const char *past = action_past_label(action);
  const char *cancel = action_cancel_label(action);
  char message[512];
  char submessage[1024];

  if(!strcmp(status ? status : "", "success")) {
    snprintf(message, sizeof(message), "%s", GC_NOTIFY_APP_TITLE);
    if(!strcmp(action ? action : "", "move-to-usb")) {
      snprintf(submessage, sizeof(submessage), "Moved %s to USB", name);
    } else if(!strcmp(action ? action : "", "move-to-internal")) {
      snprintf(submessage, sizeof(submessage),
               "Moved %s to internal storage", name);
    } else {
      snprintf(submessage, sizeof(submessage), "%s %s", past, name);
    }
  } else if(!strcmp(status ? status : "", "cancelled")) {
    snprintf(message, sizeof(message), "%s - Cancelled", GC_NOTIFY_APP_TITLE);
    snprintf(submessage, sizeof(submessage), "%s for %s", cancel, name);
  } else {
    snprintf(message, sizeof(message), "%s - Failed", GC_NOTIFY_APP_TITLE);
    if(detail && detail[0]) {
      snprintf(submessage, sizeof(submessage), "Failed to %s %s: %s",
               label, name, detail);
    } else {
      snprintf(submessage, sizeof(submessage), "Failed to %s %s",
               label, name);
    }
  }
  gc_notify_send(message, submessage);
}

void
gc_notify_message(const char *title, const char *detail) {
  const char *label = (title && title[0]) ? title : "Notice";
  char message[128];
  char submessage[1024];

  snprintf(message, sizeof(message), "%s%s", GC_NOTIFY_APP_TITLE,
           message_is_failure(label) || message_is_failure(detail)
               ? " - Failed" : "");
  if(detail && detail[0]) {
    snprintf(submessage, sizeof(submessage), "%s: %s", label, detail);
  } else {
    snprintf(submessage, sizeof(submessage), "%s", label);
  }
  gc_notify_send(message, submessage);
}
