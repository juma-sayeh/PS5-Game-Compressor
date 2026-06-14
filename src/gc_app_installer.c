/*
 * Game Compressor - install the PS5 home-screen web launcher tile.
 */

#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <pthread.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

#include <ps5/kernel.h>

#include "gc_app_installer.h"
#include "gc_diag.h"
#include "gc_notify.h"
#include "websrv.h"

#define GC_APP_ROOT "/user/app"
#define GC_APP_PARENT GC_APP_ROOT "/"
#define GC_DATA_ROOT "/data"
#define GC_DATA_DIR "/data/GameCompressor"
#define GC_MARKER_PATH GC_DATA_DIR "/launcher.ok"
#define GC_INSTALL_MARKER "game-compressor-launcher-v1\n"
#define GC_APPINST_MODULE "libSceAppInstUtil.sprx"
#define GC_APPINST_PATH "/system/common/lib/libSceAppInstUtil.sprx"
#define GC_USER_SERVICE_MODULE "libSceUserService.sprx"
#define GC_USER_SERVICE_PATH "/system/common/lib/libSceUserService.sprx"
#define GC_NETCTL_MODULE "libSceNetCtl.sprx"
#define GC_NETCTL_PATH "/system/common/lib/libSceNetCtl.sprx"
#define GC_NID_INSTALL_TITLE_DIR "Wudg3Xe3heE"

#define INCASSET(name, file)                                                   \
  __asm__(".section .rodata\n"                                                 \
          ".global " #name "\n"                                                \
          ".global " #name "_end\n"                                            \
          ".global " #name "_size\n"                                           \
          ".align 16\n" #name ":\n"                                            \
          ".incbin \"" file "\"\n" #name "_end:\n" #name "_size:\n"            \
          ".quad " #name "_end - " #name "\n"                                  \
          ".previous\n");                                                      \
  extern const uint8_t name[];                                                 \
  extern const size_t name##_size

INCASSET(gc_launcher_param_json, "assets-app/param.json");
INCASSET(gc_launcher_icon0_png, "assets-app/icon0.png");

typedef int (*app_install_title_dir_fn)(const char *, const char *, void *);
typedef int (*appinst_initialize_fn)(void);
typedef int (*appinst_install_all_fn)(void *);
typedef int (*appinst_uninstall_fn)(const char *);
typedef int (*netctl_init_fn)(void);
typedef int (*user_service_initialize_fn)(void *);

int sceKernelLoadStartModule(const char *, size_t, const void *, uint32_t,
                             void *, int *);

typedef struct appinst_api {
  appinst_initialize_fn      initialize;
  app_install_title_dir_fn install_title_dir;
  appinst_install_all_fn     install_all;
  appinst_uninstall_fn       uninstall;
} appinst_api_t;

typedef struct launcher_thread_arg {
  char ip[64];
} launcher_thread_arg_t;

static pthread_mutex_t g_launcher_start_lock = PTHREAD_MUTEX_INITIALIZER;
static int g_launcher_started = 0;

static gc_launcher_diag_t g_launcher_diag = {
  .launcher_enabled = 1,
  .launcher_attempted = 0,
  .appinst_init_rc = -1,
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

static const uint8_t g_install_marker[] = GC_INSTALL_MARKER;

static void
publish_launcher_diag(void) {
  websrv_set_launcher_diag(&g_launcher_diag);
}

static void
reset_launcher_diag(void) {
  gc_launcher_diag_t diag = {
    .launcher_enabled = 1,
    .launcher_attempted = 1,
    .appinst_init_rc = -1,
    .title_dir_resolved = 0,
    .install_title_dir_resolved = 0,
    .install_title_rc = GC_DIAG_SKIPPED,
    .uninstall_resolved = 0,
    .uninstall_rc = GC_DIAG_SKIPPED,
    .install_all_resolved = 0,
    .install_all_rc = GC_DIAG_SKIPPED,
    .user_app_writable = -1,
    .launcher_install_rc = -1,
    .launcher_final_state = "started",
  };
  g_launcher_diag = diag;
  publish_launcher_diag();
}

static void
set_launcher_final(const char *state, int rc) {
  snprintf(g_launcher_diag.launcher_final_state,
           sizeof(g_launcher_diag.launcher_final_state), "%s",
           state ? state : "failed_nonfatal");
  g_launcher_diag.launcher_install_rc = rc;
  publish_launcher_diag();
}

static int
resolve_module_handle(const char *module, const char *path, uint32_t *handle) {
  int rc = kernel_dynlib_handle(-1, module, handle);
  if(rc == 0) return 0;

  if(path && path[0]) {
    int load_rc = sceKernelLoadStartModule(path, 0, NULL, 0, NULL, NULL);
    gc_log("launcher load module %s rc=0x%08x", path, (unsigned)load_rc);
    if(load_rc > 0) {
      *handle = (uint32_t)load_rc;
      return 0;
    }

    rc = kernel_dynlib_handle(-1, module, handle);
    if(rc == 0) return 0;
  }

  return rc;
}

static int
resolve_module_nid(const char *module, const char *path, const char *nid,
                   void **out) {
  uint32_t handle = 0;
  void *addr = NULL;

  if(out) *out = NULL;
  if(!module || !nid || !out) return -1;

  int rc = resolve_module_handle(module, path, &handle);
  if(rc != 0) return rc;

  addr = (void *)kernel_dynlib_resolve(-1, handle, nid);
  if(!addr) return -2;

  *out = addr;
  return 0;
}

static int
resolve_module_symbol(const char *module, const char *path, const char *symbol,
                      void **out) {
  uint32_t handle = 0;
  void *addr = NULL;

  if(out) *out = NULL;
  if(!module || !symbol || !out) return -1;

  int rc = resolve_module_handle(module, path, &handle);
  if(rc != 0) return rc;

  addr = (void *)kernel_dynlib_dlsym(-1, handle, symbol);
  if(!addr) return -2;

  *out = addr;
  return 0;
}

static void
resolve_appinst(appinst_api_t *api) {
  memset(api, 0, sizeof(*api));

  int init_rc = resolve_module_symbol(GC_APPINST_MODULE, GC_APPINST_PATH,
                                      "sceAppInstUtilInitialize",
                                      (void **)&api->initialize);
  int title_rc = resolve_module_nid(GC_APPINST_MODULE, GC_APPINST_PATH,
                                    GC_NID_INSTALL_TITLE_DIR,
                                    (void **)&api->install_title_dir);
  int all_rc = resolve_module_symbol(GC_APPINST_MODULE, GC_APPINST_PATH,
                                     "sceAppInstUtilAppInstallAll",
                                     (void **)&api->install_all);
  int uninstall_rc = resolve_module_symbol(GC_APPINST_MODULE, GC_APPINST_PATH,
                                           "sceAppInstUtilAppUnInstall",
                                           (void **)&api->uninstall);

  g_launcher_diag.title_dir_resolved = api->install_title_dir != NULL;
  g_launcher_diag.install_title_dir_resolved = api->install_title_dir != NULL;
  gc_log("launcher resolve AppInst init=%s rc=0x%08x titleDir=%s rc=0x%08x "
         "installAll=%s rc=0x%08x uninstall=%s rc=0x%08x",
         api->initialize ? "ok" : "missing", (unsigned)init_rc,
         api->install_title_dir ? "ok" : "missing", (unsigned)title_rc,
         api->install_all ? "ok" : "missing", (unsigned)all_rc,
         api->uninstall ? "ok" : "missing", (unsigned)uninstall_rc);

  g_launcher_diag.uninstall_resolved = api->uninstall != NULL;
  g_launcher_diag.install_all_resolved = api->install_all != NULL;
  publish_launcher_diag();
}

static int
probe_dir_writable(const char *dir) {
  char probe[256];
  int n = snprintf(probe, sizeof(probe), "%s/.gc_launcher_write_probe", dir);
  if(n < 0 || (size_t)n >= sizeof(probe)) return 0;

  int fd = open(probe, O_WRONLY | O_CREAT | O_TRUNC, 0600);
  if(fd < 0) return 0;

  close(fd);
  unlink(probe);
  return 1;
}

static int
mkdir_if_needed(const char *path) {
  if(mkdir(path, 0755) == 0) return 0;
  return errno == EEXIST ? 0 : -1;
}

static int
write_file(const char *path, const uint8_t *data, size_t size) {
  FILE *file = fopen(path, "wb");
  if(!file) return -1;

  size_t written = fwrite(data, 1, size, file);
  int close_rc = fclose(file);

  return (written == size && close_rc == 0) ? 0 : -1;
}

static int
file_differs(const char *path, const uint8_t *expected, size_t expected_size) {
  struct stat st;
  if(stat(path, &st) != 0) return 1;
  if(st.st_size < 0 || (size_t)st.st_size != expected_size) return 1;

  FILE *file = fopen(path, "rb");
  if(!file) return 1;

  uint8_t *actual = malloc(expected_size ? expected_size : 1);
  if(!actual) {
    fclose(file);
    return 1;
  }

  size_t read = fread(actual, 1, expected_size, file);
  fclose(file);

  int differs = read != expected_size || memcmp(actual, expected, expected_size);
  free(actual);
  return differs;
}

static int
ensure_data_dir(void) {
  if(mkdir_if_needed(GC_DATA_ROOT) != 0) return -1;
  return mkdir_if_needed(GC_DATA_DIR);
}

static int
install_app(const appinst_api_t *api, const char *title_id, const char *dir) {
  if(api->install_title_dir) {
    int err = api->install_title_dir(title_id, dir, NULL);
    g_launcher_diag.install_title_rc = err;
    g_launcher_diag.launcher_install_rc = err;
    publish_launcher_diag();
    if(err == 0) return 0;
    gc_log("launcher install AppInstallTitleDir failed rc=0x%08x",
           (unsigned)err);
  } else {
    g_launcher_diag.install_title_rc = GC_DIAG_SKIPPED;
    publish_launcher_diag();
    gc_log("launcher install AppInstallTitleDir not resolved");
  }

  if(!api->install_all) {
    g_launcher_diag.install_all_rc = GC_DIAG_SKIPPED;
    publish_launcher_diag();
    return -ENOSYS;
  }

  int err = api->install_all(NULL);
  g_launcher_diag.install_all_rc = err;
  g_launcher_diag.launcher_install_rc = err;
  publish_launcher_diag();
  gc_log("launcher install AppInstallAll rc=0x%08x", (unsigned)err);
  return err;
}

static int
uninstall_launcher_title(const appinst_api_t *api, const char *title_id,
                         const char *label) {
  if(!api->uninstall) {
    g_launcher_diag.uninstall_rc = GC_DIAG_SKIPPED;
    publish_launcher_diag();
    gc_log("launcher install remove %s skipped: AppUnInstall missing", label);
    return -ENOSYS;
  }

  int err = api->uninstall(title_id);
  g_launcher_diag.uninstall_rc = err;
  publish_launcher_diag();
  gc_log("launcher install remove %s rc=0x%08x", label, (unsigned)err);
  return err;
}

static void
init_ps5_services(void) {
  int user_prio = 256;
  netctl_init_fn netctl_init = NULL;
  user_service_initialize_fn user_service_initialize = NULL;

  int netctl_resolve_rc =
      resolve_module_symbol(GC_NETCTL_MODULE, GC_NETCTL_PATH, "sceNetCtlInit",
                            (void **)&netctl_init);
  if(netctl_init) {
    int netctl_rc = netctl_init();
    gc_log("launcher sceNetCtlInit rc=0x%08x", (unsigned)netctl_rc);
  } else {
    gc_log("launcher sceNetCtlInit skipped resolve rc=0x%08x",
           (unsigned)netctl_resolve_rc);
  }

  int user_resolve_rc =
      resolve_module_symbol(GC_USER_SERVICE_MODULE, GC_USER_SERVICE_PATH,
                            "sceUserServiceInitialize",
                            (void **)&user_service_initialize);
  if(user_service_initialize) {
    int user_service_rc = user_service_initialize(&user_prio);
    gc_log("launcher sceUserServiceInitialize rc=0x%08x",
           (unsigned)user_service_rc);
  } else {
    gc_log("launcher sceUserServiceInitialize skipped resolve rc=0x%08x",
           (unsigned)user_resolve_rc);
  }
}

static int
gc_install_app_if_needed(void) {
  appinst_api_t api;
  char app_dir[256];
  char sce_sys_dir[256];
  char param_path[256];
  char icon_path[256];
  struct stat st;

  reset_launcher_diag();
  resolve_appinst(&api);

  g_launcher_diag.user_app_writable = probe_dir_writable(GC_APP_ROOT);
  publish_launcher_diag();
  gc_log("launcher install %s writable=%d", GC_APP_ROOT,
         g_launcher_diag.user_app_writable);

  snprintf(app_dir, sizeof(app_dir), GC_APP_ROOT "/%s",
           GAME_COMPRESSOR_LAUNCHER_TITLE_ID);
  snprintf(sce_sys_dir, sizeof(sce_sys_dir), "%s/sce_sys", app_dir);
  snprintf(param_path, sizeof(param_path), "%s/param.json", sce_sys_dir);
  snprintf(icon_path, sizeof(icon_path), "%s/icon0.png", sce_sys_dir);

  int app_exists = stat(app_dir, &st) == 0;
  int assets_changed = !app_exists ||
                       file_differs(param_path, gc_launcher_param_json,
                                    gc_launcher_param_json_size) ||
                       file_differs(icon_path, gc_launcher_icon0_png,
                                    gc_launcher_icon0_png_size) ||
                       file_differs(GC_MARKER_PATH, g_install_marker,
                                    sizeof(g_install_marker) - 1);

  if(app_exists && assets_changed) {
    gc_notify_message("Updating home tile", NULL);
  } else if(app_exists) {
    gc_notify_message("Refreshing home tile", NULL);
  } else {
    gc_notify_message("Installing home tile", NULL);
  }

  if(!api.initialize) {
    g_launcher_diag.appinst_init_rc = -ENOSYS;
    publish_launcher_diag();
    gc_log("launcher install AppInst initialize missing");
    set_launcher_final("failed_nonfatal", -ENOSYS);
    gc_notify_message("Home tile setup failed", "Use web UI");
    return -1;
  }

  int err = api.initialize();
  g_launcher_diag.appinst_init_rc = err;
  publish_launcher_diag();
  if(err) {
    gc_log("launcher install sceAppInstUtilInitialize failed rc=0x%08x",
           (unsigned)err);
    set_launcher_final("failed_nonfatal", err);
    gc_notify_message("Home tile setup failed", "Use web UI");
    return -1;
  }

  uninstall_launcher_title(&api, GAME_COMPRESSOR_LAUNCHER_TITLE_ID,
                           "Game Compressor tile");

  if(mkdir_if_needed(app_dir) != 0 || mkdir_if_needed(sce_sys_dir) != 0) {
    gc_log("launcher install mkdir failed errno=%d", errno);
    set_launcher_final("failed_nonfatal", -errno);
    gc_notify_message("Home tile setup failed", "Use web UI");
    return -1;
  }

  if(write_file(param_path, gc_launcher_param_json,
                gc_launcher_param_json_size) != 0) {
    gc_log("launcher install failed writing %s errno=%d", param_path, errno);
    set_launcher_final("failed_nonfatal", -errno);
    gc_notify_message("Home tile setup failed", "Use web UI");
    return -1;
  }

  if(write_file(icon_path, gc_launcher_icon0_png,
                gc_launcher_icon0_png_size) != 0) {
    gc_log("launcher install failed writing %s errno=%d", icon_path, errno);
    set_launcher_final("failed_nonfatal", -errno);
    gc_notify_message("Home tile setup failed", "Use web UI");
    return -1;
  }

  err = install_app(&api, GAME_COMPRESSOR_LAUNCHER_TITLE_ID, GC_APP_PARENT);
  if(err) {
    gc_log("launcher install install_app failed rc=0x%08x", (unsigned)err);
    set_launcher_final("failed_nonfatal", err);
    gc_notify_message("Home tile setup failed", "Use web UI");
    return -1;
  }

  if(ensure_data_dir() != 0) {
    gc_log("launcher install warning failed creating %s errno=%d",
           GC_DATA_DIR, errno);
  } else if(write_file(GC_MARKER_PATH, g_install_marker,
                       sizeof(g_install_marker) - 1) != 0) {
    gc_log("launcher install warning failed writing %s errno=%d",
           GC_MARKER_PATH, errno);
  }

  set_launcher_final("installed", 0);
  gc_notify_message("Ready", "Home tile installed");
  return 1;
}

static void *
launcher_thread(void *arg) {
  launcher_thread_arg_t *state = arg;

  gc_log("launcher started after web server");
  init_ps5_services();

  int app_install_status = gc_install_app_if_needed();
  if(app_install_status >= 0) {
    gc_log("launcher ready rc=%d", app_install_status);
  } else {
    gc_log("launcher skipped rc=%d, web server remains available",
           app_install_status);
    gc_notify_message("Home tile skipped", "Use web UI");
  }

  free(state);
  return NULL;
}

int
gc_launcher_start(const char *ip) {
  pthread_mutex_lock(&g_launcher_start_lock);
  if(g_launcher_started) {
    pthread_mutex_unlock(&g_launcher_start_lock);
    return 0;
  }
  g_launcher_started = 1;
  pthread_mutex_unlock(&g_launcher_start_lock);

  launcher_thread_arg_t *state = calloc(1, sizeof(*state));
  if(!state) {
    set_launcher_final("failed_nonfatal", -ENOMEM);
    gc_log("launcher skipped: failed allocating thread state");
    gc_notify_message("Home tile skipped", "Use web UI");
    return -1;
  }
  snprintf(state->ip, sizeof(state->ip), "%s",
           ip && ip[0] ? ip : "<PS5_IP>");

  pthread_t thread;
  pthread_attr_t attr;
  pthread_attr_init(&attr);
  pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_DETACHED);
  int rc = pthread_create(&thread, &attr, launcher_thread, state);
  pthread_attr_destroy(&attr);
  if(rc != 0) {
    set_launcher_final("failed_nonfatal", -rc);
    gc_log("launcher skipped: pthread_create rc=%d", rc);
    gc_notify_message("Home tile skipped", "Use web UI");
    free(state);
    return -1;
  }
  return 0;
}
