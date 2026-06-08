# PS5 Game Compressor

Standalone PS5 payload for compressing, unpacking, validating, and repairing
ShadowMountPlus-mounted games from a simple web UI.

Game Compressor is built for homebrew/debug workflows where games are already
mounted through ShadowMountPlus. It does not include a file browser. Instead, it
discovers titles from ShadowMountPlus tracker links under:

```text
/user/app/<TITLE_ID>/mount.lnk
/user/app/<TITLE_ID>/mount_img.lnk
```

## What It Does

- Compresses mounted game folders or images into FF-PFSC output.
- Supports PFS and exFAT compression output choices.
- Validates compressed PFSC games and records persistent validation markers.
- Repairs detected PFSC block issues when possible.
- Unpacks compressed games back to app form.
- Moves supported titles between internal storage and USB storage.
- Shows operation progress, speed, estimated remaining time, and history.
- Keeps operations running on the payload even if the browser tab closes.
- Installs or refreshes a PS5 home-screen web launcher tile:

```text
Game Compressor / PSGC50001 -> http://127.0.0.1:5910/
```

## Requirements

- A PS5 homebrew environment capable of running payload ELFs.
- ShadowMountPlus already installed and managing mounted titles.
- Payload Manager or another way to launch `game-compressor.elf`.
- FTP access to deploy the payload if you are not copying it manually.
- PS5 Payload SDK for building from source.

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
   - `Uncompress`
   - `Move to USB`
   - `Move to Internal SSD`
8. Use the History button to review previous operations.

The app remembers the last game you viewed using a browser cookie, so reopening
the UI returns to that title when it is still available.

## Runtime Paths

Persistent validation markers:

```text
/data/GameCompressor/validations
```

Repair workspace:

```text
/data/GameCompressor/logs/repair
```

Launcher marker:

```text
/data/GameCompressor/launcher.ok
```

Compatibility shutdown endpoint:

```text
GET /api/control/shutdown
```

## Notes

- Launcher installation is nonfatal. If it fails, the web UI can still be used
  directly from `http://<PS5_IP>:5910/`.
- Payload operations are owned by the PS5-side worker, not the browser tab.
- Server-side notifications are sent on completion when supported by the
  runtime environment.
- Compression and repair workflows can take a long time on large titles.

## Credits

Created by Juma Sayeh.

Tested by Osama Abualia.

Built on and inspired by work from:

- [PSBrew/MkPFS](https://github.com/PSBrew/MkPFS)
- [drakmor/ShadowMountPlus](https://github.com/drakmor/ShadowMountPlus)

Made with love in Palestine.

## Disclaimer

This is homebrew software for experimental PS5 workflows. Use it at your own
risk. The project is not affiliated with Sony, PlayStation, or any game
publisher.
