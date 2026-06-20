# Game Compressor 1.0.2

Game Compressor 1.0.2 focuses on APR-EMU update handling, safer image patching,
and UI polish.

Compared against `v1.0.1`.

## Key Changes

- Added APR-EMU version management for titles that include `libSceAmpr.sprx`.
  You can apply the latest cached version, choose a specific version, upload a
  custom `.sprx`/`.prx`, or restore the original title-provided binary when a
  backup is available.
- Added APR-EMU hot-swap support for direct `.exfat` images and compatible
  compressed `.ffpfsc` images, including mounted hash verification after update.
- Added direct `ampr_emu.index` insertion/refresh support for `.exfat` and
  compressed exFAT-backed `.ffpfsc` images.
- Improved compression layout for future APR-EMU updates by keeping AMPR files,
  indexes, exFAT metadata, and executable payload blocks in update-friendly raw
  regions.
- Added title icon thumbnails in the web UI.
- Added dark mode with saved settings and cookie-backed fast initial loading.
- Improved validation ETA/progress reporting and APR-EMU operation status
  labels.

See `CHANGELOG.md` for the full detailed change list.
