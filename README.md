# Golars

An Arrow-native, Polars-shaped DataFrame API for Go.

The package deliberately has a zero-session default API:

```go
result, err := golars.ScanParquet("s3://analytics/events/*.parquet").
	Filter(golars.Col("tenant").Eq(golars.Lit("acme"))).
	GroupBy(golars.Col("user_id")).
	Agg(golars.Col("amount").Sum().Alias("total")).
	Collect(ctx)
```

`Engine` is optional. Use it when an application needs an isolated memory
quota, batch size, parallelism, or object-store registration; ordinary code
uses the immutable package default engine and does not manage a session.

Arrow buffers are reference-counted. `DataFrame`, `Series`, and `LazyFrame`
therefore expose `Close` for deterministic release, with `Release` as an Arrow
style alias. A finalizer is installed as a safety net, but it is not a prompt
memory-management mechanism; use `defer value.Close()` for large or
long-lived values. Lazy branches retain their own table references, so closing
one branch does not invalidate another.

## Current surface

- Arrow-backed `DataFrame` and `Series`, with retained Arrow interop.
- Eager and lazy `Select`, `Filter`, `WithColumns`, `Limit`/`Head`, `Sort`,
  `GroupBy(...).Agg(...)`, joins, window expressions, `Unique`, `Explode`,
  `Unnest`, `Drop`, `Rename`, and `DropNulls`.
- Expression trees with arithmetic, comparisons, boolean logic, null handling,
  casts, aggregates, conditional expressions, partitioned windows, descending
  sort, membership/range predicates, and common string/date kernels.
- Arrow type descriptors cover primitive, temporal, decimal, dictionary
  (categorical/enum), nested list/struct/map, fixed-size, view, interval, and
  run-end encoded types. `DataFrame`/`Series` constructors accept Arrow arrays,
  chunked arrays, tables, and record batches without a copy at the boundary.
- Lazy scans and eager reads for Parquet, CSV, NDJSON, JSON arrays, and Arrow
  IPC file/stream formats.
- Lazy and eager sinks for the same formats, with non-overwriting local-file
  defaults. `LazyFrame.Sink` selects the writer from the destination extension.
- Row-oriented helpers: `Rows`/`ToDicts` materialize detached row maps, while
  `ForEachRow` streams them without retaining the complete result.
- Local globbing and Hive-style Parquet partition discovery.
- Hive dataset writes with `Create`/`Append`, partition columns,
  `_SUCCESS`, and `_golars_schema.json`.
- Built-in S3 and GCS URI support using standard credential discovery. Custom
  `ObjectStore` implementations can override them with `RegisterObjectStore`
  or `WithObjectStore`.
- Engine-wide memory accounting with a 1 GiB default quota and contextual
  cancellation through `context.Context`.

## Native execution

On supported targets, eligible local Parquet plans run through the Rust/Polars
backend. The target-specific native library is embedded in the Go module and
loaded with CGO-free FFI; consumers only need the normal `go get` and Go build
workflow. No Rust toolchain, C compiler, shared-library installation,
environment variables, network access at runtime, or companion executable is
required. Native assets currently cover macOS arm64 and Linux amd64/arm64.
Windows native support is tracked in issue #3. Other targets and plans use the
portable Go executor automatically. Applications can force the Go executor with
`WithNativeBackend(false)`.

To consume an unpublished local checkout from another module, use a local
replacement together with an explicit synthetic version:

```sh
go mod edit -replace=github.com/guywaldman/golars=/absolute/path/to/golars
go get github.com/guywaldman/golars@v0.0.0
```

The explicit version prevents `go get` from querying the module proxy for a
remote version before applying the replacement.

## Query planning and storage

- Lazy scans derive the columns required by a terminal projection and its
  filters. Parquet reads only those Arrow columns; CSV and JSON readers apply
  the same projection at ingestion.
- The optimized logical plan performs expression simplification, projection
  collapse, predicate pushdown through safe projections, adjacent-filter
  fusion, limit fusion/pushdown, type coercion, and storage predicate/projection
  pushdown. `ExplainUnoptimized` and `ExplainOptimized` expose both forms.
- Build-tagged end-to-end tests exercise Parquet write, object listing, remote
  reads, and lazy execution against LocalStack S3 and fake-gcs-server through
  Testcontainers.

Run the unit suite with:

```sh
go test ./...
```

Run the emulator suite on a machine with Docker:

```sh
go test -tags=e2e ./... -count=1 -v
```

The E2E suite requires Docker and fails fast when Docker is unavailable. Remote
Parquet/IPC objects that do not expose the optional range interface are
materialized before decoding.
The harness uses the Docker client configuration supplied by the environment;
it does not select or auto-detect a container runtime.

## Streaming and predicate pruning

- `RangeObjectStore` enables bounded random-access reads. The S3 and GCS
  adapters implement object stats and range reads; older stores keep the
  materializing fallback.
- Safe simple predicates (`=`, `!=`, `<`, `<=`, `>`, `>=`, `is_null`, and
  `is_not_null`) prune Parquet row groups when valid min/max statistics exist.
- Local Parquet scans use memory-mapped reads when available; row groups proven
  to satisfy a predicate skip the per-row filter kernel and, when the terminal
  projection permits, avoid decoding filter-only columns. Partial groups still
  receive exact evaluation.
- `LazyFrame.ForEachBatch` executes incrementally, and `LazyFrame.Sink` writes
  Parquet batches without first collecting the complete frame.
- `LazyFrame.ForEachRow` is the row-oriented streaming counterpart; use
  `LazyFrame.Height(ctx)` to count rows without retaining them.
- Regression tests cover row-group selection, range-only reads, batch
  iteration, and streaming sinks.

## Examples

```go
df, err := golars.ReadCSV(ctx, "events.csv")
if err != nil { return err }
defer df.Close()

summary, err := df.GroupBy(golars.Col("country")).Agg(
	golars.Col("amount").Sum().Alias("amount_total"),
	golars.Col("user_id").Count().Alias("users"),
)
if err != nil { return err }
defer summary.Close()

err = summary.WriteParquet(ctx, "out.parquet")
```

For an explicitly configured engine:

```go
engine, err := golars.NewEngine(
	golars.WithMemoryLimit(512<<20),
	golars.WithBatchSize(32<<10),
	golars.WithObjectStore("s3", s3Store),
)
if err != nil { return err }
defer engine.Close()

df, err := engine.ScanParquet("s3://bucket/events/*.parquet").Collect(ctx)
```

The implementation is intentionally a fast-breaking v0 API. The execution
kernel and format contracts are stable enough for experimentation, while the
public type names and richer Polars parity surface will continue to grow.

## Repository layout

- `golars.go` is the small public compatibility facade. It aliases the public
  types and forwards constructors/functions to the implementation package.
- `internal/core` owns the tightly coupled public API implementation,
  logical-plan/execution kernels, and white-box tests. Keeping that package
  together avoids artificial import cycles while keeping the module root free
  of implementation files.
- `internal/arrowutil` contains shared Arrow ownership and scalar helpers;
  `internal/jsonio` handles JSON inference and encoding; `internal/ipcio`
  handles Arrow IPC file readers.
- `internal/parquetio` owns Parquet readers, row-group pruning, projection,
  and ranged object reads; `internal/storage` owns S3/GCS adapters; and
  `internal/hive` owns Hive partition enrichment.
- `internal/storageuri` contains object-URI validation. These packages are
  implementation boundaries and are not part of the public API.
- `benchmarks` contains the Go/Rust comparison harness; `cmd` contains its
  runnable utilities.

## Benchmarking

The [`benchmarks`](benchmarks) directory contains equivalent Go and Rust
Polars workloads over the same generated Parquet input. The runners report
timing, allocation, result-cardinality, and memory metrics; see
[`benchmarks/README.md`](benchmarks/README.md) for reproducible commands.
