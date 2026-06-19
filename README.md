# PS5 Game Compressor

Standalone PS5 payload for compressing, unpacking, validating, repairing, and
moving ShadowMountPlus-mounted games from a simple web UI.

Game Compressor is made for the day-to-day workflow after your games are already
mounted through ShadowMountPlus: pick a title, choose an action, and let the PS5
do the work. The app keeps long operations running on the console even if the
browser window is closed.

## Main Features

- Compress mounted game folders or images into FF-PFSC output.
- Choose PFS or exFAT output when compressing.
- Automatically build APR Emu `ampr_emu.index` before compressing APR titles.
- Manually run `Build AMPR Index` on a folder when you only want to refresh the
  APR index.
- Validate compressed games and repair detected PFSC block issues when possible.
- Uncompress compressed games back to folder/app form.
- Move supported titles between internal storage and USB storage.
- Track progress, speed, estimated remaining time, and operation history.
- Keep operations running on the PS5 even if the browser tab closes.
- Install or refresh a PS5 home-screen launcher tile:

```text
Game Compressor / PSGC50001 -> http://127.0.0.1:5910/
```

The PS5 home-screen tile opens the Game Compressor web UI in the browser. It is
not the compression worker itself. Compression, validation, repair, move, and
uncompress jobs continue on the PS5 if you close the browser window or reopen
the tile later.

## APR Emu Support

APR Emu titles need an `ampr_emu.index` file and the correct ShadowMountPlus
read-only image settings when they are run from internal SSD. Game Compressor
handles those details for the common workflows:

- To compress a folder-format APR Emu game from USB and run it from internal
  SSD, plug in the USB drive, select the title, and choose `Compress`. Game
  Compressor builds `ampr_emu.index` using the same workflow as
  `build_ampr_index.py`, writes the ShadowMountPlus read-only and sector-size
  settings, creates the `.ffpfsc` image, mounts it, and validates the mounted
  image byte-for-byte against the original.
- If the compressed game stutters and you still want it on internal SSD, open
  the secondary action menu, choose `Uncompress`, then select `exFAT`. The
  ShadowMountPlus settings and `ampr_emu.index` are already in place, so Game
  Compressor leaves them unchanged and creates an uncompressed exFAT image.
- If you already know the game should stay uncompressed, open the secondary
  action menu, choose `Make Image`, then select `exFAT` and `Internal SSD`.
  Game Compressor detects the APR Emu title, builds or refreshes
  `ampr_emu.index`, applies the read-only ShadowMountPlus settings, and creates
  the uncompressed image.
- If you already have an exFAT image and `ampr_emu.index` exists, use `Set Read
  Only` from the secondary action menu to apply the ShadowMountPlus read-only
  settings for that image.

The only unsupported automatic case is an existing exFAT image with no
`ampr_emu.index`. For that case, run the game once from external USB without
read-only settings so APR Emu can create its index, confirm the game starts,
then copy it to internal SSD and use `Set Read Only`.

Non-APR titles keep the normal compression path. When APR indexing is performed,
the selected game screen and operation history show `APR indexed`.

## Requirements

- A PS5 homebrew environment capable of running payload ELFs.
- [ShadowMountPlus](https://github.com/drakmor/ShadowMountPlus) latest version,
  installed and managing mounted titles.
- [KStuff Lite](https://github.com/EchoStretch/kstuff-lite/releases/tag/v1.07)
  1.07 Beta or later.
- [Payload Manager](https://github.com/itsPLK/ps5-payload-manager) or another
  method to launch `game-compressor.elf`.

This project assumes you already understand the risks of running PS5 homebrew
payloads. Keep backups of important data and test with non-critical titles
first.

## Build

Set `PS5_PAYLOAD_SDK` to your local SDK path, then run `make`:

```sh
export PS5_PAYLOAD_SDK=/path/to/ps5-payload-sdk
make
```

The build output is:

```text
game-compressor.elf
```

Generated build outputs are intentionally ignored by git:

```text
build/
gen/
game-compressor.elf
```

## Deploy

Copy the built ELF to your PS5 payload folder. A typical Payload Manager path is:

```text
/data/pldmgr/payloads/game-compressor/game-compressor.elf
```

This repo also includes Payload Manager metadata in:

```text
payload-manager/game-compressor.elf.json
```

If you rebuild the ELF, update that JSON checksum before publishing or
redistributing it.

## How To Use

1. Make sure ShadowMountPlus has mounted one or more games.
2. Launch `game-compressor.elf` from Payload Manager or your payload loader.
3. Open the web UI:

```text
http://<PS5_IP>:5910/
```

4. Pick a game from the left sidebar.
5. Use the primary action:
   - Folder/image titles show `Compress`.
   - Compressed titles show `Validate and Repair`.
6. For compression, choose either `PFS` or `exFAT` when prompted.
7. Use the secondary action menu for supported actions such as:
   - `Build AMPR Index`
   - `Uncompress`
   - `Move to USB`
   - `Move to Internal SSD`
8. Use the History button to review previous operations.

The app remembers the last game you viewed using a browser cookie, so reopening
the UI returns to that title when it is still available.

If you close the browser window during an operation, open the Game Compressor
tile again or go back to `http://<PS5_IP>:5910/` to see the current job.

## Game Discovery

Mounted games are shown automatically. Game Compressor also scans known game
storage folders, including:

```text
/data/homebrew
/data/etaHEN/games
/mnt/ext0
/mnt/ext0/homebrew
/mnt/ext0/etaHEN/games
/mnt/ext1
/mnt/ext1/homebrew
/mnt/ext1/etaHEN/games
/mnt/usb0 through /mnt/usb7
/mnt/usb0/homebrew through /mnt/usb7/homebrew
/mnt/usb0/etaHEN/games through /mnt/usb7/etaHEN/games
```

## When You Are Done

Game Compressor should only be launched when you need to compress, validate,
repair, move, or unpack games. After your games are compressed and you no longer
need the web UI, it is preferred that Game Compressor is no longer running.

Use the terminate button in the top bar when you are done. Game Compressor
removes its home-screen tile, stops the payload, and leaves a final screen
telling you to exit the browser window.

## Notes

- Launcher installation is nonfatal. If it fails, the web UI can still be used
  directly from `http://<PS5_IP>:5910/`.
- Payload operations are owned by the PS5-side worker, not the browser tab.
- Compression and repair workflows can take a long time on large titles.

## Credits

Created by Juma Sayeh.

Tested by Osama Abualia.

Built on and inspired by work from:

- [PSBrew/MkPFS](https://github.com/PSBrew/MkPFS)
- Drakmor's [ShadowMountPlus](https://github.com/drakmor/ShadowMountPlus),
  [APR Emu](https://github.com/drakmor/ampr_emu), and `build_ampr_index.py`
  work, which Game Compressor builds on for mounted-title support and PS5-side
  `ampr_emu.index` generation.

Made with love in Palestine.

## Disclaimer

This is homebrew software for experimental PS5 workflows. Use it at your own
risk. The project is not affiliated with Sony, PlayStation, or any game
publisher.
