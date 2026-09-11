/*
 * gc_api_sm.c — ShadowMountPlus API-specific implementations.
 *
 * This file is NOT a standalone translation unit. It is #include'd at the
 * end of gc_api.c so every static helper, type, and global defined there is
 * visible here. Each _sm function contains the body that runs only when
 * gc_shadowmount_api_available() is true; gc_api.c keeps the availability
 * check at the top of the original function and delegates here.
 */

/* --- forward declarations for _sm helpers that call each other --- */
static int gc_shadowmount_source_enable_sm(gc_game_t *g);
static int gc_shadowmount_ampr_mount_game_sm(gc_game_t *g);
static int wait_for_shadowmount_links_sm(const char *title_id,
                                         const char *expected_mount_link,
                                         const char *expected_image_link,
                                         char *err, size_t err_size);
static void gc_games_cache_refresh_sm(void);
static int force_compressed_path_bounce_remount_sm(
    const char *title_id, const char *original_path,
    const char *nested_name, int nested_type, int keep_mounted,
    char *err, size_t err_size);
static int force_image_path_bounce_remount_sm(
    const char *title_id, const char *original_path,
    int nested_type, int keep_mounted, char *err, size_t err_size);
static int mount_switch_clear_stale_links_sm(
    const char *title_id, const char *expected_mount,
    const char *expected_image, gc_mount_link_backup_t *backup,
    char *err, size_t err_size);
static int repair_force_path_bounce_remount_sm(const char *title_id,
    const char *original_path, pfs_repair_info_t *info,
    char *err, size_t err_size);

/*
 * gc_shadowmount_source_enable — SM body
 * Original guard:  if(!gc_shadowmount_api_available()) return 0;
 */
static int
gc_shadowmount_source_enable_sm(gc_game_t *g) {
  char err[256] = { 0 };
  struct stat st;
  if(!g || !g->is_mounted || !valid_title_id(g->title_id) ||
     !g->source_path[0] || !g->mount_path[0] ||
     !path_under_root(g->mount_path, GC_SHADOW_IMAGE_BASE)) {
    return 0;
  }
  if(stat(g->mount_path, &st) == 0 && S_ISDIR(st.st_mode)) return 0;
  if(gc_shadowmount_request_title_source_scan(g->title_id, g->source_path, err,
                                               sizeof(err)) != 0) {
    gc_log("ensure shadow mount: mount request failed title=%s err=%s",
           g->title_id, err[0] ? err : "unknown");
    return 0;
  }
  return 1;
}

/*
 * gc_shadowmount_ampr_mount_game — SM body
 * Original guard (line 1116):  if(!gc_shadowmount_api_available()) return 0;
 */
static int
gc_shadowmount_ampr_mount_game_sm(gc_game_t *g) {
  char err[256] = { 0 };
  char mount_link[1024] = { 0 };
  struct stat st;
  const char *probe_root = NULL;
  int scan_rc;

  if(!g) return 0;
  if(g->source_kind != GC_SOURCE_COMPRESSED &&
     g->source_kind != GC_SOURCE_IMAGE) {
    return 0;
  }
  if(!valid_title_id(g->title_id) || !g->source_path[0]) return 0;
  if(atomic_load(&g_job.busy)) return 0;
  if(gc_any_operation_pending()) return 0;
  /* Don't disrupt mounts while a game is running. */
  if(sceSystemServiceGetAppIdOfRunningBigApp() >= 0) return 0;
  /*
   * Skip the temporary mount when this title's icon is already cached.
   * The mount exists only to cache the icon0.png and probe the embedded
   * APR-EMU binary; re-mounting every game on every /api/gc/games poll
   * is expensive (ShadowMountPlus is a single-mount device) and causes a
   * flood of mount/unmount requests. detect_game_source_ex() falls back
   * to gc_ampr_cache_lookup() for the AMPR fields, and /api/gc/icon
   * serves the cached icon, so skipping is safe. The cache is dropped
   * explicitly when a title's data actually changes (e.g. after an AMPR
   * update) so the next refresh re-probes that one title.
   */
  if(gc_icon_cache_has(g->title_id)) {
    return 0;
  }

  /* If the mount is already active for this game, probe it directly. */
  if(g->mount_path[0] && path_under_root(g->mount_path, GC_SHADOW_IMAGE_BASE) &&
     stat(g->mount_path, &st) == 0 && S_ISDIR(st.st_mode)) {
    probe_root = g->mount_path;
  } else {
    /*
     * ShadowMountPlus supports a single active mount. If another game
     * currently holds the device, unmount it before mounting the one
     * we need.
     */
    char blocking_tid[64] = { 0 };
    char find_err[128] = { 0 };
    if(gc_shadowmount_api_find_mounted_game(blocking_tid, sizeof(blocking_tid),
                                            find_err, sizeof(find_err)) == 1 &&
       strcasecmp(blocking_tid, g->title_id) != 0) {
      char unmount_err[256] = { 0 };
      if(gc_shadowmount_unmount_title(blocking_tid, unmount_err,
                                      sizeof(unmount_err)) != 0) {
        gc_log("ampr mount game: failed to unmount blocking title=%s err=%s",
               blocking_tid, unmount_err[0] ? unmount_err : "unknown");
      }
    }
    /* Mount this title, probe, then unmount to free the device. */
    scan_rc = gc_shadowmount_request_title_source_scan(
      g->title_id, g->source_path, err, sizeof(err));
    if(scan_rc != 0) {
      gc_log("ampr mount game: request failed title=%s err=%s", g->title_id,
             err[0] ? err : "unknown");
      return scan_rc;
    }
    /*
     * The mount API is synchronous: the mount is live once the request
     * returns, so read mount.lnk once instead of polling for it.
     */
    if(read_title_link(g->title_id, "mount.lnk", mount_link,
                       sizeof(mount_link)) != 0 ||
       !mount_link[0] || !path_under_root(mount_link, GC_SHADOW_IMAGE_BASE) ||
       stat(mount_link, &st) != 0 || !S_ISDIR(st.st_mode)) {
      gc_log("ampr mount game: mount link not available title=%s", g->title_id);
      err[0] = 0;
      gc_shadowmount_unmount_title(g->title_id, err, sizeof(err));
      return 0;
    }
    probe_root = mount_link;
  }

  /*
   * The mount is now live (probe_root is set in both branches above).
   * Cache the mounted flag for this title so /api/gc/games keeps
   * reporting the game as mounted/available even after the temporary
   * mount is released below. In sm 1.7 mounting is dynamic, so the
   * cached flag (not the transient on-disk hint) is the availability
   * signal returned to the frontend.
   */
  gc_games_cache_set_mounted(g->title_id, 1, "mounted");

  /* Probe AMPR while the mount is active. */
  int found = ampr_folder_target_probe(probe_root, g->ampr_path,
                                       sizeof(g->ampr_path), g->ampr_sha256);

  /*
   * Cache the game icon while the mount is active. The icon0.png at
   * /user/app/<title_id>/ may be a symlink into the mount and is only
   * resolvable while the game is mounted. Both the full-size PNG and
   * a 96x96 thumbnail are generated and cached in memory so that
   * /api/gc/icon can serve either version later without re-mounting.
   * If the app/appmeta symlinks are missing, fall back to reading the
   * icon directly from the active mount root (probe_root).
   */
  {
    char icon_path[1024];
    struct stat icon_st;
    int icon_fd = -1;
    snprintf(icon_path, sizeof(icon_path), "%s/%s/icon0.png", GC_APP_BASE,
             g->title_id);
    icon_fd = open(icon_path, O_RDONLY);
    if(icon_fd < 0) {
      snprintf(icon_path, sizeof(icon_path), "%s/%s/icon0.png", GC_APPMETA_BASE,
               g->title_id);
      icon_fd = open(icon_path, O_RDONLY);
    }
    if(icon_fd < 0 && probe_root && probe_root[0]) {
      snprintf(icon_path, sizeof(icon_path), "%s/sce_sys/icon0.png",
               probe_root);
      icon_fd = open(icon_path, O_RDONLY);
    }
    if(icon_fd < 0 && probe_root && probe_root[0]) {
      snprintf(icon_path, sizeof(icon_path), "%s/icon0.png", probe_root);
      icon_fd = open(icon_path, O_RDONLY);
    }
    if(icon_fd < 0) {
      gc_log("ampr mount game: icon not found title=%s", g->title_id);
    } else if(fstat(icon_fd, &icon_st) != 0) {
      gc_log("ampr mount game: icon fstat failed title=%s path=%s err=%s",
             g->title_id, icon_path, strerror(errno));
    } else if(icon_st.st_size <= 0 ||
              (uint64_t)icon_st.st_size > 2 * 1024 * 1024) {
      gc_log("ampr mount game: icon bad size title=%s path=%s size=%lld",
             g->title_id, icon_path, (long long)icon_st.st_size);
    } else {
      size_t icon_size = (size_t)icon_st.st_size;
      unsigned char *icon_buf = malloc(icon_size);
      if(icon_buf) {
        size_t got = 0;
        while(got < icon_size) {
          ssize_t n = read(icon_fd, icon_buf + got, icon_size - got);
          if(n < 0) {
            if(errno == EINTR) continue;
            break;
          }
          if(n == 0) break;
          got += (size_t)n;
        }
        if(got > 0) {
          /* Generate thumbnail from the full-size PNG bytes. */
          unsigned char *thumb_buf = NULL;
          int thumb_len = 0;
          if(got <= INT_MAX) {
            gc_icon_thumb_from_memory(icon_buf, (int)got, &thumb_buf,
                                      &thumb_len);
          }
          gc_icon_cache_store(g->title_id, icon_buf, got, thumb_buf,
                              (thumb_buf && thumb_len > 0) ? (size_t)thumb_len
                                                           : 0);
          free(thumb_buf);
        } else {
          gc_log("ampr mount game: icon read empty title=%s path=%s",
                 g->title_id, icon_path);
        }
        free(icon_buf);
      } else {
        gc_log("ampr mount game: icon alloc failed title=%s size=%zu",
               g->title_id, icon_size);
      }
    }
    if(icon_fd >= 0) close(icon_fd);
  }

  /* Unmount to free the single-mount device for the next game. */
  err[0] = 0;
  if(gc_shadowmount_unmount_title(g->title_id, err, sizeof(err)) != 0) {
    gc_log("ampr mount game: unmount failed title=%s err=%s", g->title_id,
           err[0] ? err : "unknown");
  }

  return found ? 1 : 0;
}

/*
 * wait_for_shadowmount_links — SM body
 * Original guard (line 3323):  if(gc_shadowmount_api_available()) { return 0; }
 * In sm 1.7 the mount API is synchronous, so the mount is already live
 * when the caller returns; no link-wait polling is needed.
 */
static int
wait_for_shadowmount_links_sm(const char *title_id,
                               const char *expected_mount_link,
                               const char *expected_image_link,
                               char *err, size_t err_size) {
  if(err && err_size) err[0] = 0;
  gc_log("shadowmount wait title=%s mount=%s image=%s",
         title_id ? title_id : "",
         expected_mount_link ? expected_mount_link : "",
         expected_image_link ? expected_image_link : "");
  gc_log("shadowmount ready (api synchronous) title=%s",
         title_id ? title_id : "");
  return 0;
}

/*
 * gc_games_cache_refresh — SM body
 * Original flag (line 4448):  dynamic_mount = gc_shadowmount_api_available();
 * When the SM API is available, mounting is dynamic so every discovered
 * title is cached as mounted regardless of the transient mount.lnk hint.
 */
static void
gc_games_cache_refresh_sm(void) {
  gc_game_t *games;
  size_t count = 0;
  if(gc_any_operation_pending()) {
    gc_games_cache_invalidate();
    return;
  }
  games = calloc(GC_MAX_GAMES, sizeof(*games));
  if(!games) {
    gc_games_cache_invalidate();
    return;
  }
  discover_games(games, GC_MAX_GAMES, &count, 0);
  for(size_t i = 0; i < count; i++) {
    set_game_mount_status(&games[i], 1, "mounted");
  }
  pthread_mutex_lock(&g_games_cache_lock);
  size_t copy_count = count < GC_MAX_GAMES ? count : GC_MAX_GAMES;
  memcpy(g_games_cache, games, copy_count * sizeof(g_games_cache[0]));
  g_games_cache_count = copy_count;
  g_games_cache_ready = 1;
  g_games_cache_dirty = 0;
  pthread_mutex_unlock(&g_games_cache_lock);
  free(games);
}

/*
 * force_compressed_path_bounce_remount — SM body
 * Original guard (line 6028):  if(gc_shadowmount_api_available()) { ... }
 * On the API path the bounce-rename is skipped.  The full
 * unmount → verify-absent → mount → verify-present → scan sequence
 * is handled by request_title_source_scan_via_api (called via
 * wait_for_compressed_shadowmount_sm), which polls the SM games
 * list to confirm each state transition before proceeding.
 */
static int
force_compressed_path_bounce_remount_sm(const char *title_id,
                                        const char *original_path,
                                        const char *nested_name,
                                        int nested_type, int keep_mounted,
                                        char *err, size_t err_size) {
  char hint_err2[256] = { 0 };
  if(gc_shadowmount_write_pfsc_hints(original_path, nested_name, nested_type,
                                     hint_err2, sizeof(hint_err2)) != 0) {
    gc_log("compressed remount api hint failed title=%s err=%s",
           title_id ? title_id : "", hint_err2[0] ? hint_err2 : "unknown");
  }
  return wait_for_compressed_shadowmount_sm(
    title_id, original_path, nested_name, nested_type,
    "Waiting for final remount", keep_mounted, err, err_size);
}

/*
 * force_image_path_bounce_remount — SM body
 * Original guard (line 6227):  if(gc_shadowmount_api_available()) { return ...; }
 * On the API path the bounce-rename is unnecessary; the SM mount API
 * performs the remount directly.
 */
static int
force_image_path_bounce_remount_sm(const char *title_id,
                                   const char *original_path,
                                   int nested_type, int keep_mounted,
                                   char *err, size_t err_size) {
  return wait_for_image_shadowmount_sm(title_id, original_path, nested_type,
                                    "Waiting for final remount", keep_mounted,
                                    err, err_size);
}

/*
 * mount_switch_clear_stale_links — SM body
 * Original guard (line 7062):  if(gc_shadowmount_api_available()) { return 0; }
 * In sm 1.7 the SM API manages mount state. Deleting the legacy
 * mount.lnk / mount_img.lnk links would make the API mount fail, and the
 * API unmount+mount already performs the state transition, so link
 * clearing is obsolete and must be skipped.
 */
static int
mount_switch_clear_stale_links_sm(const char *title_id,
                                   const char *expected_mount,
                                   const char *expected_image,
                                   gc_mount_link_backup_t *backup,
                                   char *err, size_t err_size) {
  (void)title_id;
  (void)expected_mount;
  (void)expected_image;
  (void)backup;
  (void)err;
  (void)err_size;
  return 0;
}

static int
run_refresh_mount_op_sm(gc_operation_t *op) {
  gc_game_t game = { 0 };
  char err[256] = { 0 };
  char restore_err[256] = { 0 };
  char scan_err[256] = { 0 };
  char expected_mount[1024] = { 0 };
  char expected_image[1024] = { 0 };
  gc_hidden_instance_t hidden[GC_MAX_GAMES];
  gc_mount_link_backup_t link_backup;
  size_t hidden_count = 0;
  int was_validated = 0;
  int mount_ready = 0;
  int mount_missed = 0;

  memset(&link_backup, 0, sizeof(link_backup));
  gc_checkpoint("refresh-mount find game");
  gc_log("refresh-mount start op=%s title=%s", op->id, op->title_id);
  append_operation_phase(op, "resolving");
  job_set_phase("resolving", 0, 0, "Resolving selected source");
  if(find_game_for_operation_source_path(op, &game, 0) != 0 ||
     game.source_kind == GC_SOURCE_UNKNOWN) {
    snprintf(op->error, sizeof(op->error), "%s",
             job_cancelled() ? "cancelled" : "game instance is unavailable");
    gc_log("refresh-mount failed title=%s err=%s", op->title_id, op->error);
    return -1;
  }
  if(gc_cancel_requested(op->error, sizeof(op->error))) return -1;

  snprintf(op->source_path, sizeof(op->source_path), "%s", game.source_path);
  snprintf(op->output_path, sizeof(op->output_path), "%s", game.source_path);
  snprintf(op->source_kind, sizeof(op->source_kind), "%s",
           source_kind_name(game.source_kind));
  was_validated = game.validation == GC_VALIDATION_VALIDATED;
  job_set_target(game.source_path);

  if(game.is_mounted) {
    snprintf(op->result, sizeof(op->result), "%s", "success");
    gc_log("refresh-mount already mounted title=%s path=%s", op->title_id,
           game.source_path);
    return 0;
  }
  if(gc_cancel_requested(op->error, sizeof(op->error))) return -1;

  if(close_title_if_running(op->title_id, err, sizeof(err)) != 0) {
    snprintf(op->error, sizeof(op->error), "%s",
             err[0] ? err : "could not close game");
    gc_log("refresh-mount failed title=%s err=%s", op->title_id, op->error);
    return -1;
  }

  if(move_remount_expectations(&game, game.source_path,
                               expected_mount, sizeof(expected_mount),
                               expected_image, sizeof(expected_image),
                               err, sizeof(err)) != 0) {
    snprintf(op->error, sizeof(op->error), "%s",
             err[0] ? err : "could not derive mount path");
    gc_log("refresh-mount expectation failed title=%s err=%s",
           op->title_id, op->error);
    return -1;
  }

  if(prepare_shadowmount_for_selected_source(&game, err, sizeof(err)) != 0) {
    snprintf(op->error, sizeof(op->error), "%s",
             err[0] ? err : "could not inspect selected source");
    gc_log("refresh-mount inspect failed title=%s err=%s", op->title_id,
           op->error);
    return -1;
  }
  if(gc_cancel_requested(op->error, sizeof(op->error))) return -1;

  gc_checkpoint("refresh-mount hide competitors");
  append_operation_phase(op, "hiding");
  job_set_phase("hiding", 0, 0, "Hiding other instances");
  if(mount_switch_hide_competitors(op, &game, hidden, GC_MAX_GAMES,
                                   &hidden_count, err, sizeof(err)) != 0) {
    snprintf(op->error, sizeof(op->error), "%s",
             err[0] ? err : "could not hide duplicate instances");
    gc_log("refresh-mount hide failed title=%s err=%s", op->title_id,
           op->error);
    goto restore_and_fail;
  }
  artifact_cache_invalidate();
  if(gc_cancel_requested(op->error, sizeof(op->error))) goto restore_and_fail;
  if(mount_switch_clear_stale_links(op->title_id, expected_mount,
                                    expected_image, &link_backup,
                                    err, sizeof(err)) != 0) {
    snprintf(op->error, sizeof(op->error), "%s",
             err[0] ? err : "could not clear stale mount state");
    gc_log("refresh-mount link clear failed title=%s err=%s", op->title_id,
           op->error);
    goto restore_and_fail;
  }
  {
    char unmount_err[256] = { 0 };
    if(gc_shadowmount_unmount_title(op->title_id, unmount_err,
                                    sizeof(unmount_err)) != 0) {
      gc_log("refresh-mount api unmount failed title=%s err=%s", op->title_id,
             unmount_err[0] ? unmount_err : "unknown");
    }
  }

  gc_checkpoint("refresh-mount mount selected");
  append_operation_phase(op, "mounting");
  job_set_phase("mounting", 0, 0, "Mounting");
  if(gc_shadowmount_request_title_source_scan_cancelable(
       game.title_id, game.source_path, scan_err, sizeof(scan_err)) != 0) {
    snprintf(op->error, sizeof(op->error), "%s",
             scan_err[0] ? scan_err : "could not request ShadowMount scan");
    gc_log("refresh-mount scan failed title=%s err=%s", op->title_id,
           op->error);
    goto restore_and_fail;
  }
  if(gc_cancel_requested(op->error, sizeof(op->error))) goto restore_and_fail;
  err[0] = 0;
  if(wait_for_shadowmount_links_sm(op->title_id, expected_mount, expected_image,
                                err, sizeof(err)) == 0) {
    mount_ready = 1;
  } else if(shadowmount_mount_missed(err)) {
    mount_missed = 1;
    gc_log("refresh-mount selected instance not mounted title=%s path=%s detail=%s",
      op->title_id, game.source_path, err[0] ? err : "");
  } else {
    snprintf(op->error, sizeof(op->error), "%s",
             err[0] ? err : "ShadowMountPlus mount failed");
    gc_log("refresh-mount failed title=%s err=%s", op->title_id, op->error);
    goto restore_and_fail;
  }

  gc_checkpoint("refresh-mount restore competitors");
  append_operation_phase(op, "restoring");
  job_set_phase("restoring", 0, 0, "Restoring other instances");
  if(mount_switch_restore_hidden(op, hidden, hidden_count, restore_err,
                                 sizeof(restore_err)) != 0) {
    snprintf(op->error, sizeof(op->error), "%s",
             restore_err[0] ? restore_err : "could not restore duplicate instances");
    gc_log("refresh-mount restore failed title=%s err=%s", op->title_id,
           op->error);
    artifact_cache_invalidate();
    return -1;
  }
  artifact_cache_invalidate();
  if(!mount_ready &&
     mount_switch_restore_cleared_links(&link_backup, restore_err,
                                        sizeof(restore_err)) != 0) {
    snprintf(op->error, sizeof(op->error), "%s",
             restore_err[0] ? restore_err : "could not restore previous mount links");
    gc_log("refresh-mount link restore failed title=%s err=%s", op->title_id,
           op->error);
    return -1;
  }
  scan_err[0] = 0;
  if(mount_ready) {
    gc_log("refresh-mount post-restore scan skipped after selected mount title=%s",
      op->title_id);
  } else if(job_cancelled()) {
    gc_log("refresh-mount post-restore scan skipped after cancel title=%s",
           op->title_id);
  } else if(gc_shadowmount_request_scan_cancelable(scan_err,
                                                   sizeof(scan_err)) != 0 &&
            !job_cancelled()) {
    gc_log("refresh-mount post-restore scan failed title=%s err=%s",
           op->title_id, scan_err[0] ? scan_err : "unknown");
  }

  if(mount_ready) {
    snprintf(op->result, sizeof(op->result), "%s", "success");
    if(was_validated) {
      (void)write_validation_marker_ex(op->title_id, game.source_path, NULL,
                                       "validated", 0, 0,
                                       game.ampr_hot_swap_optimized);
    }
    gc_games_cache_set_mounted(op->title_id, 1, "mounted");
    gc_log("refresh-mount complete title=%s path=%s", op->title_id,
           game.source_path);
    gc_shadowmount_unmount_title(op->title_id, NULL, 0);
    return 0;
  }

  if(mount_missed) {
    snprintf(op->error, sizeof(op->error), "%s",
             err[0] ? err : "selected source was not mounted");
    gc_log("refresh-mount failed not mounted title=%s path=%s detail=%s",
           op->title_id, game.source_path, op->error);
    return -1;
  }

  snprintf(op->error, sizeof(op->error), "%s", "mount did not complete");
  return -1;

restore_and_fail:
  if(hidden_count > 0) {
    append_operation_phase(op, "restoring");
    job_set_phase("restoring", 0, 0, "Restoring other instances");
    restore_err[0] = 0;
    if(mount_switch_restore_hidden(op, hidden, hidden_count,
                                   restore_err, sizeof(restore_err)) != 0) {
      gc_log("refresh-mount restore after failure failed title=%s err=%s",
             op->title_id, restore_err[0] ? restore_err : "unknown");
      operation_append_error_detail(op, restore_err);
    }
    artifact_cache_invalidate();
    refresh_mount_restore_cleared_links_after_failure(op, &link_backup,
                                                      restore_err,
                                                      sizeof(restore_err));
    refresh_mount_request_restore_scan(op, scan_err, sizeof(scan_err));
  } else if(link_backup.cleared) {
    refresh_mount_restore_cleared_links_after_failure(op, &link_backup,
                                                      restore_err,
                                                      sizeof(restore_err));
    refresh_mount_request_restore_scan(op, scan_err, sizeof(scan_err));
  }
  return -1;
}

static int
run_build_ampr_index_op_sm(gc_operation_t *op) {
  gc_game_t game = { 0 };
  pfs_app_info_t info;
  pfs_ampr_hotswap_info_t hs;
  char err[256] = { 0 };
  char expected_mount[1024] = { 0 };

  gc_checkpoint("build-ampr-index find game");
  gc_log("build-ampr-index start op=%s title=%s", op->id, op->title_id);
  append_operation_phase(op, "resolving");
  job_set_phase("resolving", 0, 0, "Resolving selected source");
  if(find_game_for_operation_source_path(op, &game, 0) != 0 ||
     game.source_kind == GC_SOURCE_UNKNOWN) {
    snprintf(op->error, sizeof(op->error), "%s", "game source is unavailable");
    gc_log("build-ampr-index failed title=%s err=%s",
           op->title_id, op->error);
    return -1;
  }
  if(gc_cancel_requested(err, sizeof(err))) {
    snprintf(op->error, sizeof(op->error), "%s", err);
    return -1;
  }
  if(!game.ampr_present) {
    snprintf(op->error, sizeof(op->error), "%s",
             "libSceAmpr.sprx or libSceAmpr.prx was not found for this game");
    gc_log("build-ampr-index skipped title=%s path=%s err=%s",
           op->title_id, game.source_path, op->error);
    return -1;
  }

  snprintf(op->source_path, sizeof(op->source_path), "%s", game.source_path);
  if(game.source_kind == GC_SOURCE_FOLDER) {
    snprintf(op->output_path, sizeof(op->output_path), "%s/ampr_emu.index",
             game.source_path);
  } else {
    snprintf(op->output_path, sizeof(op->output_path), "%s",
             game.source_path);
  }
  snprintf(op->source_kind, sizeof(op->source_kind), "%s",
           source_kind_name(game.source_kind));
  snprintf(op->format, sizeof(op->format), "%s", "ampr");
  snprintf(op->delete_policy, sizeof(op->delete_policy), "%s", "none");
  job_set_target(op->output_path);

  memset(&info, 0, sizeof(info));
  memset(&hs, 0, sizeof(hs));
  if(game.source_kind == GC_SOURCE_IMAGE ||
     game.source_kind == GC_SOURCE_COMPRESSED) {
    /*
     * Unmount the image before patching it.  Rebuilding the PFSC/exFAT
     * structure of a .ffpfsc/.exfat while the PFS driver still has the
     * image mounted can leave stale vnode state; the subsequent remount
     * then fails with EIO.  .ffpfsc files are also mountable read-only
     * only, so gc_shadowmount_request_title_source_scan() requests
     * mode "ro" for them.  Dropping the mount first lets the driver
     * release the old vnode so the rebuilt file is opened fresh.
     */
    {
      char pre_unmount_err[256] = { 0 };
      if(gc_shadowmount_unmount_title(op->title_id, pre_unmount_err,
                                      sizeof(pre_unmount_err)) != 0) {
        gc_log("build-ampr-index pre-patch unmount failed title=%s err=%s",
               op->title_id, pre_unmount_err[0] ? pre_unmount_err : "unknown");
      }
    }
    sync();
    usleep(1000000);
  }
  if(game.source_kind == GC_SOURCE_FOLDER) {
    gc_checkpoint("build-ampr-index scanning");
    append_operation_phase(op, "scanning");
    job_set_phase("scanning", 0, 0, "Scanning app folder");
    if(pfs_build_ampr_index_for_folder(game.source_path, &info,
                                       err, sizeof(err)) != 0) {
      snprintf(op->error, sizeof(op->error), "%s",
               err[0] ? err : "AMPR index build failed");
      gc_log("build-ampr-index failed title=%s err=%s",
             op->title_id, op->error);
      return -1;
    }
    operation_store_scan_stats(op, &info);
    op->compression_source_size = info.scan_bytes;
    struct stat st;
    if(stat(op->output_path, &st) == 0 && S_ISREG(st.st_mode) &&
       st.st_size > 0) {
      op->compressed_size = (uint64_t)st.st_size;
    }
  } else if(game.source_kind == GC_SOURCE_IMAGE) {
    if(game.nested_type != PFS_NESTED_EXFAT) {
      snprintf(op->error, sizeof(op->error), "%s",
               "AMPR index image patch supports exFAT images only");
      return -1;
    }
    gc_checkpoint("build-ampr-index patch image");
    append_operation_phase(op, "patching");
    job_set_phase("patching", 0, 0, "Building AMPR index into exFAT image");
    if(pfs_ampr_index_exfat_image(game.source_path, &hs,
                                  err, sizeof(err)) != 0) {
      snprintf(op->error, sizeof(op->error), "%s",
               err[0] ? err : "AMPR index image patch failed");
      gc_log("build-ampr-index image failed title=%s err=%s",
             op->title_id, op->error);
      return -1;
    }
    delete_validation_marker_for_path(op->title_id, game.source_path);
    snprintf(op->ampr_result_mode, sizeof(op->ampr_result_mode), "%s",
             hs.mode[0] ? hs.mode : "exfat-index-tail");
    op->compressed_size = hs.new_size;
    if(update_ampr_remount_source(op, &game, expected_mount,
                                  sizeof(expected_mount), err,
                                  sizeof(err)) != 0) {
      snprintf(op->error, sizeof(op->error), "%s",
               err[0] ? err : "AMPR index remount failed");
      gc_log("build-ampr-index image remount failed title=%s err=%s",
             op->title_id, op->error);
      return -1;
    }
  } else if(game.source_kind == GC_SOURCE_COMPRESSED) {
    if(game.nested_type != PFS_NESTED_EXFAT) {
      snprintf(op->error, sizeof(op->error), "%s",
               "AMPR index compressed patch supports nested exFAT only");
      return -1;
    }
    gc_checkpoint("build-ampr-index patch compressed");
    append_operation_phase(op, "patching");
    job_set_phase("patching", 0, 0,
                  "Building AMPR index into compressed exFAT image");
    if(pfs_ampr_index_ffpfsc_exfat(game.source_path, &hs,
                                   err, sizeof(err)) != 0) {
      snprintf(op->error, sizeof(op->error), "%s",
               err[0] ? err : "AMPR index compressed patch failed");
      gc_log("build-ampr-index compressed failed title=%s err=%s",
             op->title_id, op->error);
      return -1;
    }
    delete_vhash_sidecar_if_present(game.source_path, "build-ampr-index",
                                    op->title_id);
    delete_validation_marker_for_path(op->title_id, game.source_path);
    snprintf(op->ampr_result_mode, sizeof(op->ampr_result_mode), "%s",
             hs.mode[0] ? hs.mode : "ffpfsc-index-tail");
    op->compressed_size = hs.new_size;
    op->repaired_blocks = hs.changed_blocks;
    if(update_ampr_remount_source(op, &game, expected_mount,
                                  sizeof(expected_mount), err,
                                  sizeof(err)) != 0) {
      snprintf(op->error, sizeof(op->error), "%s",
               err[0] ? err : "AMPR index remount failed");
      gc_log("build-ampr-index compressed remount failed title=%s err=%s",
             op->title_id, op->error);
      return -1;
    }
  } else {
    snprintf(op->error, sizeof(op->error), "%s",
             "AMPR index can only be built for a folder, exFAT image, or compressed image");
    gc_log("build-ampr-index denied title=%s sourceKind=%s path=%s",
           op->title_id, source_kind_name(game.source_kind), game.source_path);
    return -1;
  }
  op->saved_bytes = 0;
  op->apr_indexed = 1;
  snprintf(op->result, sizeof(op->result), "%s", "indexed");
  artifact_cache_invalidate();
  gc_log("build-ampr-index complete title=%s output=%s bytes=%llu files=%llu mode=%s",
         op->title_id, op->output_path,
         (unsigned long long)op->compressed_size,
         (unsigned long long)info.scan_files,
         op->ampr_result_mode);
  return 0;
}

static int
gc_api_icon_request_sm(const http_request_t *req) {
  char title_id[64];
  if(!websrv_get_query_arg(req, "titleId", title_id, sizeof(title_id)) ||
     !valid_title_id(title_id)) {
    return websrv_send_error_json(req->fd, 400, "bad titleId");
  }

  char size_arg[32];
  int want_thumb =
    websrv_get_query_arg(req, "size", size_arg, sizeof(size_arg)) &&
    !strcasecmp(size_arg, "thumb");

  /*
   * Try the in-memory icon cache first. Both the full-size PNG and a
   * 96x96 thumbnail are cached during the AMPR mount probe or on the
   * first successful disk read below. icon0.png at
   * /user/app/<title_id>/ may be a symlink into the shadow mount and
   * is only resolvable while the game is mounted, so serving from
   * the cache avoids a re-mount.
   */
  {
    unsigned char *cached = NULL;
    size_t cached_size = 0;
    if(gc_icon_cache_lookup(title_id, want_thumb, &cached, &cached_size) &&
       cached && cached_size > 0) {
      int rc = websrv_send_cached(req->fd, 200, "image/png", cached,
                                  cached_size, GC_ICON_CACHE_MAX_AGE);
      free(cached);
      return rc;
    }
  }

  /*
   * When the ShadowMountPlus HTTP API is available, fetch the icon
   * through the SM API (GET /api/v1/games/icon?title_id=...) before
   * falling back to local disk. This covers titles managed by SM
   * whose icon0.png is not directly reachable at the standard
   * /user/app or /user/appmeta paths.
   */
  unsigned char *sm_icon = NULL;
  size_t sm_icon_size = 0;
  char sm_err[256] = { 0 };
  if(gc_shadowmount_api_get_game_icon(title_id, 0, &sm_icon, &sm_icon_size,
                                      sm_err, sizeof(sm_err)) == 0 &&
     sm_icon && sm_icon_size > 0) {
    unsigned char *thumb_buf = NULL;
    int thumb_len = 0;
    if(sm_icon_size <= (size_t)INT_MAX) {
      gc_icon_thumb_from_memory(sm_icon, (int)sm_icon_size, &thumb_buf,
                                &thumb_len);
    }
    gc_icon_cache_store(title_id, sm_icon, sm_icon_size, thumb_buf,
                        (thumb_buf && thumb_len > 0) ? (size_t)thumb_len : 0);
    int rc;
    if(want_thumb && thumb_buf && thumb_len > 0) {
      rc = websrv_send_cached(req->fd, 200, "image/png", thumb_buf,
                              (size_t)thumb_len, GC_ICON_CACHE_MAX_AGE);
    } else {
      rc = websrv_send_cached(req->fd, 200, "image/png", sm_icon,
                              sm_icon_size, GC_ICON_CACHE_MAX_AGE);
    }
    free(thumb_buf);
    free(sm_icon);
    return rc;
  }
  gc_log("icon endpoint: SM API icon fetch failed title=%s err=%s", title_id,
         sm_err[0] ? sm_err : "unknown");
  free(sm_icon);

  char path[1024];
  snprintf(path, sizeof(path), "%s/%s/icon0.png", GC_APP_BASE, title_id);
  int fd = open(path, O_RDONLY);
  if(fd < 0) {
    snprintf(path, sizeof(path), "%s/%s/icon0.png", GC_APPMETA_BASE, title_id);
    fd = open(path, O_RDONLY);
  }
  if(fd < 0) return websrv_send_error_json(req->fd, 404, "icon not found");
  struct stat st;
  if(fstat(fd, &st) != 0 || st.st_size <= 0 || st.st_size > 2 * 1024 * 1024) {
    close(fd);
    return websrv_send_error_json(req->fd, 404, "icon not found");
  }

  /*
   * Read the full-size icon into memory so we can cache both the
   * full-size and the thumbnail version, regardless of which was
   * originally requested.
   */
  char *data = malloc((size_t)st.st_size);
  if(!data) {
    close(fd);
    return websrv_send_error_json(req->fd, 500, "out of memory");
  }
  size_t got = 0;
  while(got < (size_t)st.st_size) {
    ssize_t n = read(fd, data + got, (size_t)st.st_size - got);
    if(n < 0) {
      if(errno == EINTR) continue;
      free(data);
      close(fd);
      return websrv_send_error_json(req->fd, 500, "read icon failed");
    }
    if(n == 0) break;
    got += (size_t)n;
  }
  close(fd);

  /* Generate thumbnail from the full-size PNG bytes. */
  unsigned char *thumb_buf = NULL;
  int thumb_len = 0;
  if(got > 0 && got <= INT_MAX) {
    gc_icon_thumb_from_memory((const unsigned char *)data, (int)got, &thumb_buf,
                              &thumb_len);
  }

  /* Cache both full-size and thumbnail in memory. */
  if(got > 0) {
    gc_icon_cache_store(title_id, (const unsigned char *)data, got, thumb_buf,
                        (thumb_buf && thumb_len > 0) ? (size_t)thumb_len : 0);
  }

  /* Serve the requested version: thumbnail if requested and available,
   * otherwise the full-size icon. */
  int rc;
  if(want_thumb && thumb_buf && thumb_len > 0) {
    rc = websrv_send_cached(req->fd, 200, "image/png", thumb_buf,
                            (size_t)thumb_len, GC_ICON_CACHE_MAX_AGE);
  } else {
    rc = websrv_send_cached(req->fd, 200, "image/png", data, got,
                            GC_ICON_CACHE_MAX_AGE);
  }
  free(thumb_buf);
  free(data);
  return rc;
}

static int
run_validate_repair_op_sm(gc_operation_t *op) {
  gc_game_t game = { 0 };
  gc_hidden_instance_t hidden[GC_MAX_GAMES];
  char err[256] = { 0 };
  char restore_err[256] = { 0 };
  pfs_repair_info_t repair = { 0 };
  const char *repair_path = NULL;
  char final_result[32] = { 0 };
  uint64_t source_size = 0;
  uint64_t free_bytes = 0;
  size_t hidden_count = 0;
  int mount_missed = 0;

  memset(hidden, 0, sizeof(hidden));
  gc_checkpoint("validate find game");
  gc_log("validate start op=%s title=%s", op->id, op->title_id);
  append_operation_phase(op, "resolving");
  job_set_phase("resolving", 0, 0, "Resolving selected source");
  if(find_game_for_operation_source_path(op, &game, 0) != 0 ||
     game.source_kind != GC_SOURCE_COMPRESSED) {
    snprintf(op->error, sizeof(op->error), "%s",
             "compressed game is unavailable");
    gc_log("validate failed title=%s err=%s", op->title_id, op->error);
    return -1;
  }
  snprintf(op->source_path, sizeof(op->source_path), "%s", game.source_path);
  snprintf(op->output_path, sizeof(op->output_path), "%s", game.source_path);
  snprintf(op->source_kind, sizeof(op->source_kind), "%s", "compressed");
  repair_path = game.source_path;
  source_size = game.source_size;
  free_bytes = game.free_bytes;
  if(close_title_if_running(op->title_id, err, sizeof(err)) != 0) {
    snprintf(op->error, sizeof(op->error), "%s",
             err[0] ? err : "could not close game");
    gc_log("validate close game failed title=%s err=%s", op->title_id,
           op->error);
    return -1;
  }
  append_operation_phase(op, "repairing");
  job_set_phase("validating", 0, 0, "Preparing validation");
  gc_log("validate source title=%s path=%s size=%llu free=%llu validation=%s",
         op->title_id, repair_path ? repair_path : "",
         (unsigned long long)source_size,
         (unsigned long long)free_bytes,
         game.validation_status);
  gc_checkpoint("validate outer slack cleanup");
  err[0] = 0;
  if(mount_selected_instance_hidden_exclusive(
         op, &game, hidden, GC_MAX_GAMES, &hidden_count, &mount_missed,
         err, sizeof(err)) != 0) {
    snprintf(op->error, sizeof(op->error), "%s",
             err[0] ? err : "could not mount selected source");
    gc_log("validate mount failed title=%s err=%s", op->title_id, op->error);
    if(mount_switch_restore_after_operation(op, hidden, hidden_count,
                                            restore_err,
                                            sizeof(restore_err)) != 0) {
      gc_log("validate mount restore failed title=%s err=%s",
             op->title_id, restore_err[0] ? restore_err : "unknown");
      operation_append_error_detail(op, restore_err);
    }
    return -1;
  }
  if(mount_missed) {
    snprintf(op->error, sizeof(op->error), "%s",
             err[0] ? err : "selected source was not mounted");
    gc_log("validate mount missed title=%s path=%s detail=%s",
           op->title_id, repair_path ? repair_path : "", op->error);
    if(mount_switch_restore_after_operation(op, hidden, hidden_count,
                                            restore_err,
                                            sizeof(restore_err)) != 0) {
      gc_log("validate mount-missed restore failed title=%s err=%s",
             op->title_id, restore_err[0] ? restore_err : "unknown");
      operation_append_error_detail(op, restore_err);
    }
    return -1;
  }
  gc_checkpoint("validate repair");
  if(repair_with_wait(op->title_id, repair_path, &repair, err,
                      sizeof(err)) != 0) {
    op->bad_blocks_found = repair.repaired_blocks;
    op->repaired_blocks = 0;
    operation_store_repair_counters(op, &repair);
    if(repair.outdir[0]) {
      snprintf(op->repair_summary, sizeof(op->repair_summary), "%s",
               repair.outdir);
    }
    snprintf(op->error, sizeof(op->error), "%s",
             err[0] ? err : "validate and repair failed");
    gc_log("validate repair failed title=%s err=%s", op->title_id, op->error);
    if(mount_switch_restore_after_operation(op, hidden, hidden_count,
                                            restore_err,
                                            sizeof(restore_err)) != 0) {
      gc_log("validate repair restore failed title=%s err=%s",
             op->title_id, restore_err[0] ? restore_err : "unknown");
      operation_append_error_detail(op, restore_err);
    }
    return -1;
  }
  operation_store_repair_success(op, &repair);
  snprintf(final_result, sizeof(final_result), "%s", op->result);
  if(repair_force_path_bounce_remount_sm(op->title_id, repair_path, &repair,
                                      err, sizeof(err)) != 0) {
    if(shadowmount_mount_missed(err)) {
      operation_mark_verified_not_mounted(op, err);
      snprintf(final_result, sizeof(final_result), "%s", op->result);
      (void)write_validation_marker_ex(op->title_id, repair_path, &repair,
                                       op->result, 0, 0, 0);
      if(mount_switch_restore_after_operation_ex(op, hidden, hidden_count, 0,
                                                 restore_err,
                                                 sizeof(restore_err)) != 0) {
        snprintf(op->error, sizeof(op->error), "%s",
                 restore_err[0] ? restore_err :
                 "could not restore duplicate instances");
        gc_log("validate verified-not-mounted restore failed title=%s err=%s",
               op->title_id, op->error);
        return -1;
      }
      snprintf(op->result, sizeof(op->result), "%s", final_result);
      return 0;
    }
    snprintf(op->error, sizeof(op->error), "%s",
             err[0] ? err : "repair smoke verification failed");
    gc_log("validate smoke verify failed title=%s err=%s", op->title_id,
           op->error);
    if(mount_switch_restore_after_operation(op, hidden, hidden_count,
                                            restore_err,
                                            sizeof(restore_err)) != 0) {
      gc_log("validate smoke restore failed title=%s err=%s",
             op->title_id, restore_err[0] ? restore_err : "unknown");
      operation_append_error_detail(op, restore_err);
    }
    return -1;
  }
  (void)write_validation_marker_ex(op->title_id, repair_path, &repair,
                                   op->result, 0, 0, 0);
  if(mount_switch_restore_after_operation_ex(op, hidden, hidden_count, 0,
                                             restore_err,
                                             sizeof(restore_err)) != 0) {
    snprintf(op->error, sizeof(op->error), "%s",
             restore_err[0] ? restore_err :
             "could not restore duplicate instances");
    gc_log("validate restore failed title=%s err=%s", op->title_id,
           op->error);
    return -1;
  }
  snprintf(op->result, sizeof(op->result), "%s", final_result);
  gc_log("validate complete title=%s result=%s repaired=%llu",
         op->title_id, op->result,
         (unsigned long long)op->repaired_blocks);
  return 0;
}

/*
 * wait_for_compressed_shadowmount — SM body
 * In SM mode the scan failure is a hard error (API is synchronous).
 */
static int
wait_for_compressed_shadowmount_sm(const char *title_id, const char *path,
                                   const char *nested_name, int nested_type,
                                   const char *current, int keep_mounted,
                                   char *err, size_t err_size) {
  char expected_image[1024];
  char expected_mount[1024];
  char scan_err[256] = { 0 };
  if(expected_compressed_shadow_paths(
       path, nested_name, nested_type, expected_image, sizeof(expected_image),
       expected_mount, sizeof(expected_mount)) != 0) {
    snprintf(err, err_size, "%s", "could not derive compressed remount path");
    return -1;
  }
  if(gc_cancel_requested(err, err_size)) return -1;
  job_set_phase("mounting", 0, 0, current ? current : "Waiting for remount");
  if(gc_shadowmount_request_title_source_scan_cancelable(
       title_id, path, scan_err, sizeof(scan_err)) != 0) {
    if(job_cancelled()) return -1;
    gc_log("compressed remount scan request failed title=%s err=%s",
           title_id ? title_id : "", scan_err[0] ? scan_err : "unknown");
    snprintf(err, err_size, "%s",
             scan_err[0] ? scan_err : "ShadowMount remount failed");
    return -1;
  }
  int rc_wait = wait_for_shadowmount_links_sm(title_id, expected_mount,
                                           expected_image, err, err_size);
  if(rc_wait != 0 && scan_err[0]) {
    char combined[512];
    snprintf(combined, sizeof(combined), "%s; %s", scan_err,
             err[0] ? err : "unknown");
    snprintf(err, err_size, "%s", combined);
    gc_log("compressed remount failed title=%s scan_err=%s wait_err=%s",
           title_id ? title_id : "", scan_err, err[0] ? err : "unknown");
  }
  if(!keep_mounted) gc_shadowmount_unmount_title(title_id, NULL, 0);
  return rc_wait;
}

/*
 * wait_for_image_shadowmount — SM body
 */
static int
wait_for_image_shadowmount_sm(const char *title_id, const char *path,
                              int nested_type, const char *current,
                              int keep_mounted, char *err, size_t err_size) {
  char expected_mount[1024];
  char scan_err[256] = { 0 };
  if(!path || !path[0] ||
     shadow_image_mount_point(path, nested_type, expected_mount,
                              sizeof(expected_mount)) != 0) {
    snprintf(err, err_size, "%s", "could not derive image remount path");
    return -1;
  }
  if(gc_cancel_requested(err, err_size)) return -1;
  job_set_phase("mounting", 0, 0, current ? current : "Waiting for remount");
  if(gc_shadowmount_request_title_source_scan_cancelable(
       title_id, path, scan_err, sizeof(scan_err)) != 0) {
    if(job_cancelled()) return -1;
    gc_log("image remount scan request failed title=%s err=%s",
           title_id ? title_id : "", scan_err[0] ? scan_err : "unknown");
    snprintf(err, err_size, "%s",
             scan_err[0] ? scan_err : "ShadowMount remount failed");
    return -1;
  }
  int rc_wait =
    wait_for_shadowmount_links_sm(title_id, expected_mount, path, err, err_size);
  if(rc_wait != 0 && scan_err[0]) {
    char combined[512];
    snprintf(combined, sizeof(combined), "%s; %s", scan_err,
             err[0] ? err : "unknown");
    snprintf(err, err_size, "%s", combined);
    gc_log("image remount failed title=%s scan_err=%s wait_err=%s",
           title_id ? title_id : "", scan_err, err[0] ? err : "unknown");
  }
  if(!keep_mounted) gc_shadowmount_unmount_title(title_id, NULL, 0);
  return rc_wait;
}

static int
repair_force_path_bounce_remount_sm(const char *title_id,
                                    const char *original_path,
                                    pfs_repair_info_t *info,
                                    char *err, size_t err_size) {
  if(!info) return 0;
  if(force_compressed_path_bounce_remount_sm(title_id, original_path,
                                             info->nested_name,
                                             info->nested_type, 0,
                                             err, err_size) != 0) {
    return -1;
  }
  return post_repair_smoke_verify(title_id, original_path, info, 1,
                                  err, err_size);
}

/*
 * run_read_speed_test_op — SM body
 * Original guard:  if(gc_shadowmount_api_available()) return run_read_speed_test_op_sm(op);
 * In SM mode the game is mounted on demand via gc_shadowmount_source_enable
 * (which delegates to gc_shadowmount_source_enable_sm), the read-speed test
 * runs against the live shadow mount, and the mount is released at the end so
 * the single-mount ShadowMountPlus device is free for the next title.
 */
static int
run_read_speed_test_op_sm(gc_operation_t *op) {
  gc_game_t game = { 0 };
  gc_read_speed_ctx_t ctx = { 0 };
  struct stat st;
  char read_root[1024] = { 0 };
  char err[256] = { 0 };
  uint64_t last_files_opened = 0;
  uint64_t last_bytes_read = 0;
  int rc = -1;

  gc_checkpoint("read-speed find game");
  gc_log("read-speed start op=%s title=%s", op->id, op->title_id);
  append_operation_phase(op, "resolving");
  job_set_phase("resolving", 0, 0, "Resolving selected source");
  if(find_game_for_operation_source_path(op, &game, 0) != 0 ||
     game.source_kind == GC_SOURCE_UNKNOWN) {
    snprintf(op->error, sizeof(op->error), "%s", "game data is unavailable");
    gc_log("read-speed failed title=%s err=%s", op->title_id, op->error);
    goto done;
  }
  if(gc_cancel_requested(err, sizeof(err))) {
    snprintf(op->error, sizeof(op->error), "%s", err);
    goto done;
  }
  (void)gc_shadowmount_source_enable(&game);
  if(read_speed_mount_root(&game, read_root, sizeof(read_root),
                           err, sizeof(err)) != 0) {
    snprintf(op->error, sizeof(op->error), "%s",
             err[0] ? err : "game is not mounted");
    gc_log("read-speed mount unavailable title=%s err=%s",
           op->title_id, op->error);
    goto done;
  }
  if(stat(read_root, &st) != 0 || !S_ISDIR(st.st_mode)) {
    snprintf(op->error, sizeof(op->error), "%s",
             "mounted game folder is unavailable");
    gc_log("read-speed stat failed title=%s root=%s err=%s",
           op->title_id, read_root, strerror(errno));
    goto done;
  }

  snprintf(op->source_path, sizeof(op->source_path), "%s", game.source_path);
  snprintf(op->output_path, sizeof(op->output_path), "%s", read_root);
  snprintf(op->source_kind, sizeof(op->source_kind), "%s",
           source_kind_name(game.source_kind));
  snprintf(op->format, sizeof(op->format), "%s", "read");
  snprintf(op->delete_policy, sizeof(op->delete_policy), "%s", "none");
  job_set_target(read_root);

  ctx.buf = malloc(GC_COPY_CHUNK_SIZE);
  if(!ctx.buf) {
    snprintf(op->error, sizeof(op->error), "%s", "out of memory");
    goto done;
  }
  ctx.started_ms = monotonic_millis_gc();
  ctx.deadline_ms = ctx.started_ms + GC_READ_SPEED_TEST_SECONDS * 1000ULL;

  gc_checkpoint("read-speed scanning");
  append_operation_phase(op, "read-test");
  job_set_phase("read-test", 0, GC_READ_SPEED_TEST_SECONDS,
                "Testing read speed");
  atomic_store(&g_job.total_bytes, 0);
  atomic_store(&g_job.copied_bytes, 0);
  gc_log("read-speed reading title=%s root=%s seconds=%d",
         op->title_id, read_root, GC_READ_SPEED_TEST_SECONDS);

  while(!read_speed_time_expired(&ctx)) {
    if(gc_cancel_requested(err, sizeof(err))) {
      snprintf(op->error, sizeof(op->error), "%s", err);
      goto done;
    }
    last_files_opened = ctx.files_opened;
    last_bytes_read = ctx.bytes_read;
    err[0] = 0;
    if(read_speed_walk(read_root, &ctx, err, sizeof(err)) != 0) {
      snprintf(op->error, sizeof(op->error), "%s",
               err[0] ? err : "read speed test failed");
      goto done;
    }
    read_speed_update_progress(&ctx);
    if(ctx.files_opened == last_files_opened) {
      snprintf(op->error, sizeof(op->error), "%s",
               "no readable files found");
      goto done;
    }
    if(ctx.bytes_read == last_bytes_read) {
      snprintf(op->error, sizeof(op->error), "%s",
               "no readable file data found");
      goto done;
    }
  }
  read_speed_update_progress(&ctx);
  if(gc_cancel_requested(err, sizeof(err))) {
    snprintf(op->error, sizeof(op->error), "%s", err);
    goto done;
  }
  job_set_phase("read-test", GC_READ_SPEED_TEST_SECONDS,
                GC_READ_SPEED_TEST_SECONDS, "Read speed test complete");

  op->compression_source_size = ctx.bytes_read;
  op->compressed_size = ctx.bytes_read;
  op->saved_bytes = 0;
  snprintf(op->result, sizeof(op->result), "%s", "tested");
  gc_log("read-speed complete title=%s root=%s bytes=%llu files=%llu",
         op->title_id, read_root,
         (unsigned long long)ctx.bytes_read,
         (unsigned long long)ctx.files_opened);
  rc = 0;
done:
  if(game.is_mounted && game.mount_path[0] &&
     path_under_root(game.mount_path, GC_SHADOW_IMAGE_BASE)) {
    (void)gc_shadowmount_unmount_title(game.title_id, NULL, 0);
  }
  free(ctx.buf);
  return rc;
}

/*
 * run_set_read_only_op — SM body
 * Original guard:  if(gc_shadowmount_api_available()) return run_set_read_only_op_sm(op);
 * The scan mounts the image read-only via the SM API; unmount at the end so
 * the single-mount ShadowMountPlus device is free for the next title. The base
 * (non-SM) path in gc_api.c does not unmount — see gc_api_old.c.
 */
static int
run_set_read_only_op_sm(gc_operation_t *op) {
  gc_game_t game = { 0 };
  char err[256] = { 0 };
  char scan_err[256] = { 0 };
  int already_present = 0;

  gc_checkpoint("set-read-only find game");
  gc_log("set-read-only start op=%s title=%s", op->id, op->title_id);
  append_operation_phase(op, "resolving");
  job_set_phase("resolving", 0, 0, "Resolving selected image");
  if(find_game_for_operation_source_path(op, &game, 0) != 0 ||
     game.source_kind != GC_SOURCE_IMAGE) {
    snprintf(op->error, sizeof(op->error), "%s",
             job_cancelled() ? "cancelled" : "game is not an image");
    gc_log("set-read-only failed title=%s err=%s", op->title_id, op->error);
    return -1;
  }
  if(gc_cancel_requested(op->error, sizeof(op->error))) return -1;

  snprintf(op->source_path, sizeof(op->source_path), "%s", game.source_path);
  snprintf(op->output_path, sizeof(op->output_path), "%s", game.source_path);
  snprintf(op->source_kind, sizeof(op->source_kind), "%s", "image");
  job_set_target(game.source_path);

  gc_checkpoint("set-read-only config");
  append_operation_phase(op, "configuring");
  job_set_phase("configuring", 0, 0, "Updating ShadowMount config");
  if(gc_shadowmount_ensure_image_read_only(game.source_path,
                                           &already_present,
                                           err, sizeof(err)) != 0) {
    snprintf(op->error, sizeof(op->error), "%s",
             err[0] ? err : "could not update ShadowMount config");
    gc_log("set-read-only failed title=%s err=%s", op->title_id, op->error);
    return -1;
  }

  if(already_present) {
    snprintf(op->result, sizeof(op->result), "%s", "already-read-only");
    gc_log("set-read-only skipped existing rule title=%s path=%s",
           op->title_id, game.source_path);
    return 0;
  }

  gc_checkpoint("set-read-only scan");
  append_operation_phase(op, "mounting");
  job_set_phase("mounting", 0, 0, "Requesting ShadowMount scan");
  if(gc_shadowmount_request_title_source_scan_cancelable(
       game.title_id, game.source_path, scan_err, sizeof(scan_err)) != 0) {
    snprintf(op->error, sizeof(op->error), "%s",
             scan_err[0] ? scan_err : "could not request ShadowMount scan");
    gc_log("set-read-only scan failed title=%s err=%s",
           op->title_id, op->error);
    return -1;
  }
  snprintf(op->result, sizeof(op->result), "%s", "read-only");
  gc_log("set-read-only complete title=%s path=%s",
         op->title_id, game.source_path);
  gc_shadowmount_unmount_title(game.title_id, NULL, 0);
  return 0;
}
