# Native backend

Supported targets embed one compressed platform library directly in this Go
module. The backend loads without CGO, a Rust toolchain, a separately installed
shared object, or runtime network access. Relational execution is always
delegated to this backend; unsupported targets return an availability error.

Checked-in targets are Linux amd64/arm64 and macOS arm64. Windows support is
intentionally deferred.

The checked-in libraries are refreshed from `rust/` by release maintainers. The
build-tagged embed files are intentionally small and stable; refreshing an
asset changes only its compressed binary.
