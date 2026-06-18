# Game Compressor 1.0.0

Game Compressor 1.0.0 is the APR Emu compatibility release.

Compared against `v0.9.9`.

## Key Changes

- Built APR Emu indexing directly into Game Compressor. Before compressing a
  title that uses APR Emu, the app performs the same `build_ampr_index.py` work
  on the PS5 and includes the generated `ampr_emu.index` in the compressed
  output.
- Added a secondary `Build AMPR Index` action for folder titles. This lets users
  build or refresh the index without starting a compression job.
- Rebuilds APR indexes cleanly. Existing `ampr_emu.index` files are replaced for
  APR titles during pre-compress indexing so stale indexes are not carried into
  read-only compressed images.
- Shows `APR indexed` only on the selected game screen and operation history,
  based on the app's latest recorded state for that exact folder, not on a
  sidebar scan or a loose file-existence check.
- Cleans common macOS metadata from game folders before compression, including
  `.DS_Store`, AppleDouble `._*` files, `.Spotlight-V100`, and `__MACOSX`.
- Improves same-device compression stability by keeping default compression
  worker counts conservative instead of overloading PS5 storage and UI
  responsiveness.
- Removes the compression optimization selector and the old Fast compression
  profile path.
- Improves delete/cleanup behavior by removing validation sidecars such as
  `.vhash` when compressed outputs are deleted or cleaned up after failure.
- Updates the terminate flow. The terminate button removes the Game Compressor
  home tile, stops the payload, and replaces the whole UI with a final
  terminated screen telling the user to exit the window and enjoy playing.

## Compatibility Notes

- APR handling is done by Game Compressor on the PS5. Users do not need to run
  the manual `build_ampr_index.py` script before compressing APR Emu titles.
- APR index generation only runs when APR Emu markers are present or when the
  user explicitly runs `Build AMPR Index` on a folder.
- No rebuilt ShadowMountPlus or kstuff binary is required.

See `CHANGELOG.md` for the full detailed change list.
