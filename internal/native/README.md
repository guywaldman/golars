# Native backend

The native backend is distributed as part of the Go module. Supported targets
embed their platform library and load it at runtime without CGO, a Rust
toolchain, or a separately installed shared object. The Go implementation
remains available as the portable execution path.

Native libraries are built from `rust/` by maintainers and copied into the
target-specific `assets/` directory before publishing a release. Consumers do
not run Cargo or any setup command.

The repository currently embeds assets for macOS arm64 and Linux amd64/arm64.
Other targets retain the portable Go executor while CI verifies their Go and
Rust cross-builds.
