#!/usr/bin/env python3
"""Check shape agreement and a bounded native-Go/Rust timing ratio."""

from __future__ import annotations

import argparse
import json
from pathlib import Path


def load(path: Path) -> dict[str, object]:
    with path.open(encoding="utf-8") as stream:
        value = json.load(stream)
    if not isinstance(value, dict):
        raise ValueError(f"{path} does not contain a JSON object")
    return value


def integer(record: dict[str, object], key: str, path: Path) -> int:
    value = record.get(key)
    if not isinstance(value, int):
        raise ValueError(f"{path}: {key} is not an integer")
    return value


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--go", required=True, type=Path)
    parser.add_argument("--rust", required=True, type=Path)
    parser.add_argument("--max-ratio", required=True, type=float)
    args = parser.parse_args()

    go = load(args.go)
    rust = load(args.rust)
    query = go.get("query", args.go.stem)
    if query != rust.get("query"):
        raise SystemExit(f"{query}: Go/Rust query names differ")
    for key in ("rows", "width"):
        go_value = integer(go, key, args.go)
        rust_value = integer(rust, key, args.rust)
        if go_value != rust_value:
            raise SystemExit(f"{query}: {key} differs (Go={go_value}, Rust={rust_value})")

    go_ns = integer(go, "avg_ns", args.go)
    rust_ns = integer(rust, "avg_ns", args.rust)
    if go_ns <= 0 or rust_ns <= 0:
        raise SystemExit(f"{query}: non-positive benchmark timing")
    ratio = go_ns / rust_ns
    print(
        f"{query}: Go {go_ns / 1e6:.3f} ms, Rust {rust_ns / 1e6:.3f} ms, "
        f"ratio {ratio:.2f}x, shape ({go['rows']}, {go['width']})"
    )
    if ratio > args.max_ratio:
        raise SystemExit(f"{query}: ratio {ratio:.2f}x exceeds {args.max_ratio:.2f}x")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
