#!/usr/bin/env bash

set -euo pipefail

usage() {
  echo "usage: $0 --id ID --goos GOOS --goarch GOARCH --extension EXT --binary PATH --output-root DIR" >&2
  exit 2
}

asset_id=
goos=
goarch=
extension=
binary=
output_root=
while (($# > 0)); do
  case "$1" in
    --id) asset_id=${2-}; shift 2 ;;
    --goos) goos=${2-}; shift 2 ;;
    --goarch) goarch=${2-}; shift 2 ;;
    --extension) extension=${2-}; shift 2 ;;
    --binary) binary=${2-}; shift 2 ;;
    --output-root) output_root=${2-}; shift 2 ;;
    *) usage ;;
  esac
done

if [[ -z "$asset_id" || -z "$goos" || -z "$goarch" || -z "$extension" || -z "$binary" || -z "$output_root" ]]; then
  usage
fi
if [[ ! -f "$binary" ]]; then
  echo "native asset does not exist: $binary" >&2
  exit 1
fi

asset_dir="$output_root/assets/$asset_id"
mkdir -p "$asset_dir"
staged_binary=$(mktemp)
trap 'rm -f "$staged_binary"' EXIT
cp "$binary" "$staged_binary"

strip_tool=
if command -v llvm-strip >/dev/null 2>&1; then
  strip_tool=$(command -v llvm-strip)
elif command -v strip >/dev/null 2>&1; then
  strip_tool=$(command -v strip)
fi
if [[ -n "$strip_tool" ]]; then
  "$strip_tool" --strip-debug "$staged_binary" 2>/dev/null || true
fi

library_name="libgolars_native.$extension"
gzip -n -9 -c "$staged_binary" > "$asset_dir/$library_name.gz"

cat > "$output_root/embedded_${goos}_${goarch}.go" <<EOF
//go:build $goos && $goarch

package native

import _ "embed"

//go:embed assets/$asset_id/$library_name.gz
var generatedNativeLibraryCompressed []byte

func init() {
	embeddedLibraryCompressed = generatedNativeLibraryCompressed
	embeddedLibraryExtension = ".$extension"
}
EOF

gofmt -w "$output_root/embedded_${goos}_${goarch}.go"
