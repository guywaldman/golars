# Native backend

Supported targets embed one compressed platform library directly in this Go
module. The backend loads without CGO, a Rust toolchain, a separately installed
shared object, or runtime network access. The portable Go executor remains
available as the fallback on unsupported targets and plans.

The checked-in libraries are refreshed from `rust/` by release maintainers. The
build-tagged embed files are intentionally small and stable; refreshing an
asset changes only its compressed binary.
