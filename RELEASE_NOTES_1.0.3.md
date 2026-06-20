# Game Compressor 1.0.3

Game Compressor 1.0.3 is a small stability release for compressed `.ffpfsc`
images and the compression dialog.

Compared against `v1.0.2`.

## Fixes

- Improved USB-to-USB compression reliability by zeroing PFSC/PFS padding that
  could otherwise prevent the final `.ffpfsc` from mounting.
- Fixed compressed APR-EMU update and AMPR index rebuild finalization so their
  rewritten `.ffpfsc` images also have clean outer padding.
- Let validate and repair clean older Game Compressor `.ffpfsc` wrapper padding
  before mount, while skipping that cleanup for third-party layouts and still
  continuing validation.
- Clarified that Compress always outputs `.ffpfsc`; the format choice is for
  the nested image inside the compressed archive.

See `CHANGELOG.md` for the concise change list.
