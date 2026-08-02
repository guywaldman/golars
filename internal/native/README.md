# Native backend

The native backend is distributed as part of the Go module. Supported targets
embed their platform library and load it at runtime without CGO, a Rust
toolchain, or a separately installed shared object. The Go implementation
remains available as the portable execution path.

Native libraries are built from `rust/` by CI and copied into the target-specific
`assets/` directory through a reviewable update PR before publishing a release.
Consumers do not run Cargo or any setup command. A checkout without generated
assets uses the portable Go executor.
