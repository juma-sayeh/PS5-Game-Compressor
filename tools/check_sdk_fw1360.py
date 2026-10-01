#!/usr/bin/env python3
"""Reject SDK CRTs that cannot initialize PS5 firmware 13.60."""

import pathlib
import sys


def main() -> int:
    if len(sys.argv) != 2:
        print("usage: check_sdk_fw1360.py PS5_PAYLOAD_SDK", file=sys.stderr)
        return 2

    crt = pathlib.Path(sys.argv[1]) / "target" / "lib" / "crt1.o"
    try:
        data = crt.read_bytes()
    except OSError as exc:
        print(f"Cannot read SDK CRT {crt}: {exc}", file=sys.stderr)
        return 1

    # Firmware is compared as the little-endian 32-bit value 0x13600000 in
    # the SDK's kernel initialization switch. SDK v0.43 contains this case.
    if (0x13600000).to_bytes(4, "little") not in data:
        print(
            "This SDK CRT has no firmware 13.60 case. "
            "Install ps5-payload-sdk v0.43 or newer.",
            file=sys.stderr,
        )
        return 1

    print("SDK CRT contains firmware 13.60 support")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
