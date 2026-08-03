# Contributing

Thanks for helping improve Golars. The repository is a Go module with a
native Rust/Polars execution backend, so changes should be validated both at
the API boundary and across the supported native targets.  
It's experimental, so don't be scared of breaking changes but please make sure to write clearly if there are any within the scope of your changes.

## Development setup

Install Go and Rust/Cargo, then run the standard unit suite from the repository
root:

```sh
go test ./...
```

Run static analysis before opening a change:

```sh
go vet ./...
```

## End-to-end tests

The emulator suite uses Testcontainers and requires Docker. It intentionally
fails when Docker is unavailable rather than silently skipping coverage:

```sh
go test -tags=e2e ./... -count=1 -v
```

## Benchmarks

Run the Go-vs-native-Polars sweep with:

```sh
./benchmarks/run.sh
```

See [`benchmarks/README.md`](benchmarks/README.md) for workload definitions,
scale controls, and reproducible comparison settings.

## Before opening a pull request

- Keep public API changes documented in `README.md` and covered by tests.
- Keep generated benchmark outputs and local datasets out of the repository.
