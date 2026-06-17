# Game Compressor 0.9.9

Game Compressor 0.9.9 focuses on mounted image recovery/extraction, safer
storage handling, and a few operator-facing quality-of-life fixes.

Compared against `v0.9.7`.

## Highlights

- Added `Extract to Folder` for mounted image entries. It copies the currently
  mounted image contents into a normal title folder, verifies the result, and
  switches ShadowMountPlus toward the extracted folder when possible.
- Fixed the `/api/gc/extract-image` route so the UI extract action reaches the
  backend.
- Added a terminate button and a reminder to stop Game Compressor after jobs
  finish before launching games.
- Added a guarded `Continue anyway` retry for safe compression when the only
  failure is inability to probe free space.
- Improved free-space probing by falling back to mounted storage roots when an
  output folder does not exist yet.
- Improved move/copy destination selection so existing storage layouts such as
  `/homebrew` and `/etaHEN/games` are preferred.
- Hardened PFSC repair journal resume behavior by discarding journals with bad
  or mismatched counters.
- Improved game title detection for localized `param.json` metadata.
- Skipped automatic background size rechecks for USB-hosted folders during
  normal library refresh.

## Upgrade Notes

- Runtime data remains under:

  ```text
  /data/GameCompressor
  ```

- No rebuilt ShadowMountPlus binary is required. This release only uses the
  supported ShadowMountPlus control/config flow.

See `CHANGELOG.md` for the full detailed change list.
