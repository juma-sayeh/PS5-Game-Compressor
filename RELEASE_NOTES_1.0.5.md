# Game Compressor 1.0.5

Game Compressor 1.0.5 adds auto-detects the new API (v1.17+) and transparently falls back to the legacy file-based
backend, and extends APR-EMU version management.

Compared against `v1.0.4`.

## Key Changes

- Added ShadowMountPlus API support (v1.17+ version).
- Added per-title APR-EMU version pinning from the *APR-EMU Version* picker.
  When the pinned version matches the installed one, the *Apr Update* primary
  action and *APR update needed* chip are suppressed. The picker renders a
  *Pinned* badge on the pinned version and a status note naming it.
- Added a *Manage Custom APR* modal that manages `libSceAmpr.sprx` files. It
  replaces the old *Upload Custom File* button in the version picker.

## Fixes

- Fixed some UI issues.

See `CHANGELOG.md` for the full detailed change list.
