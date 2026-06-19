/*
 * Game Compressor - browser-friendly title icon thumbnails.
 */

#include "gc_icon_thumb.h"

#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#if defined(__clang__)
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wcast-qual"
#pragma clang diagnostic ignored "-Wcomma"
#pragma clang diagnostic ignored "-Wconditional-uninitialized"
#pragma clang diagnostic ignored "-Wdouble-promotion"
#pragma clang diagnostic ignored "-Wimplicit-fallthrough"
#pragma clang diagnostic ignored "-Wmissing-field-initializers"
#pragma clang diagnostic ignored "-Wshorten-64-to-32"
#pragma clang diagnostic ignored "-Wsign-conversion"
#pragma clang diagnostic ignored "-Wunused-function"
#pragma clang diagnostic ignored "-Wunused-parameter"
#pragma clang diagnostic ignored "-Wunused-variable"
#endif

#define STBI_ONLY_PNG
#define STBI_NO_THREAD_LOCALS
#define STB_IMAGE_IMPLEMENTATION
#include "stb_image.h"

#define STB_IMAGE_RESIZE_IMPLEMENTATION
#include "stb_image_resize2.h"

#define STB_IMAGE_WRITE_IMPLEMENTATION
#include "stb_image_write.h"

#if defined(__clang__)
#pragma clang diagnostic pop
#endif

#define GC_ICON_CACHE_DIR "/data/GameCompressor/icon-cache"
#define GC_ICON_SOURCE_MAX (2 * 1024 * 1024)

static int
mkdir_if_missing(const char *path) {
  if(mkdir(path, 0777) == 0) return 0;
  if(errno == EEXIST) return 0;
  return -1;
}

static int
ensure_icon_cache_dir(void) {
  if(mkdir_if_missing("/data/GameCompressor") != 0) return -1;
  return mkdir_if_missing(GC_ICON_CACHE_DIR);
}

static int
read_file_exact(const char *path, size_t size, unsigned char **out) {
  *out = NULL;
  if(size == 0 || size > GC_ICON_SOURCE_MAX) return -1;
  int fd = open(path, O_RDONLY);
  if(fd < 0) return -1;
  unsigned char *data = (unsigned char *)malloc(size);
  if(!data) {
    close(fd);
    return -1;
  }
  size_t got = 0;
  while(got < size) {
    ssize_t n = read(fd, data + got, size - got);
    if(n < 0) {
      if(errno == EINTR) continue;
      free(data);
      close(fd);
      return -1;
    }
    if(n == 0) break;
    got += (size_t)n;
  }
  close(fd);
  if(got != size) {
    free(data);
    return -1;
  }
  *out = data;
  return 0;
}

static void
cleanup_old_title_thumbs(const char *title_id, const char *keep_path) {
  DIR *dir = opendir(GC_ICON_CACHE_DIR);
  if(!dir) return;
  char prefix[96];
  int n = snprintf(prefix, sizeof(prefix), "%s-%d-", title_id,
                   GC_ICON_THUMB_SIZE);
  if(n < 0 || (size_t)n >= sizeof(prefix)) {
    closedir(dir);
    return;
  }
  struct dirent *ent;
  while((ent = readdir(dir)) != NULL) {
    if(strncmp(ent->d_name, prefix, strlen(prefix)) != 0) continue;
    char path[1024];
    n = snprintf(path, sizeof(path), "%s/%s", GC_ICON_CACHE_DIR, ent->d_name);
    if(n < 0 || (size_t)n >= sizeof(path)) continue;
    if(!strcmp(path, keep_path)) continue;
    unlink(path);
  }
  closedir(dir);
}

static int
write_thumbnail_png(const char *source_path, size_t source_size,
                    const char *cache_path) {
  unsigned char *source = NULL;
  if(read_file_exact(source_path, source_size, &source) != 0) return -1;

  int width = 0;
  int height = 0;
  int channels = 0;
  unsigned char *decoded = stbi_load_from_memory(source, (int)source_size,
                                                  &width, &height, &channels,
                                                  4);
  free(source);
  if(!decoded || width <= 0 || height <= 0 || width > 4096 || height > 4096) {
    if(decoded) stbi_image_free(decoded);
    return -1;
  }

  const int thumb = GC_ICON_THUMB_SIZE;
  unsigned char *resized = (unsigned char *)malloc((size_t)thumb *
                                                   (size_t)thumb * 4U);
  if(!resized) {
    stbi_image_free(decoded);
    return -1;
  }

  unsigned char *resize_result =
      stbir_resize_uint8_srgb(decoded, width, height, width * 4,
                              resized, thumb, thumb, thumb * 4,
                              STBIR_RGBA);
  stbi_image_free(decoded);
  if(!resize_result) {
    free(resized);
    return -1;
  }

  char tmp_path[1100];
  int n = snprintf(tmp_path, sizeof(tmp_path), "%s.tmp.%ld", cache_path,
                   (long)getpid());
  if(n < 0 || (size_t)n >= sizeof(tmp_path)) {
    free(resized);
    return -1;
  }

  unlink(tmp_path);
  int ok = stbi_write_png(tmp_path, thumb, thumb, 4, resized, thumb * 4);
  free(resized);
  if(!ok) {
    unlink(tmp_path);
    return -1;
  }

  int fd = open(tmp_path, O_RDONLY);
  if(fd >= 0) {
    fsync(fd);
    close(fd);
  }
  if(rename(tmp_path, cache_path) != 0) {
    unlink(tmp_path);
    return -1;
  }
  return 0;
}

int
gc_icon_thumb_path(const char *title_id,
                   const char *source_path,
                   const struct stat *source_st,
                   char *out_path,
                   size_t out_path_size) {
  if(!title_id || !*title_id || !source_path || !source_st ||
     !out_path || out_path_size == 0) {
    return -1;
  }
  if(source_st->st_size <= 0 || source_st->st_size > GC_ICON_SOURCE_MAX) {
    return -1;
  }
  if(ensure_icon_cache_dir() != 0) return -1;

  long long mtime = (long long)source_st->st_mtime;
  long long size = (long long)source_st->st_size;
  int n = snprintf(out_path, out_path_size, "%s/%s-%d-%lld-%lld.png",
                   GC_ICON_CACHE_DIR, title_id, GC_ICON_THUMB_SIZE,
                   mtime, size);
  if(n < 0 || (size_t)n >= out_path_size) return -1;

  if(access(out_path, R_OK) == 0) return 0;
  if(write_thumbnail_png(source_path, (size_t)source_st->st_size,
                         out_path) != 0) {
    return -1;
  }
  cleanup_old_title_thumbs(title_id, out_path);
  return 0;
}
