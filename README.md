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

## Current surface

- Arrow-backed `DataFrame` and `Series`, with retained Arrow interop.
- Eager and lazy `Select`, `Filter`, `WithColumns`, `Limit`/`Head`, `Sort`,
  `GroupBy(...).Agg(...)`, `Drop`, `Rename`, and `DropNulls`.
- Expression trees with arithmetic, comparisons, boolean logic, null handling,
  casts, aggregates, descending sort, and common string kernels.
- Lazy scans and eager reads for Parquet, CSV, NDJSON, JSON arrays, and Arrow
  IPC file/stream formats.
- Sinks for the same formats, with non-overwriting local-file defaults.
- Local globbing and Hive-style Parquet partition discovery.
- Hive dataset writes with `Create`/`Append`, partition columns,
  `_SUCCESS`, and `_golars_schema.json`.
- Pluggable `ObjectStore` implementations. Official S3 and GCS adapters are
  included; register them once with `RegisterObjectStore` or `WithObjectStore`.
- Engine-wide memory accounting with a 1 GiB default quota and contextual
  cancellation through `context.Context`.

## Milestone 2

- Lazy scans derive the columns required by a terminal projection and its
  filters. Parquet reads only those Arrow columns; CSV and JSON readers apply
  the same projection at ingestion.
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

The E2E tests skip when Docker is unavailable. Remote Parquet/IPC objects are
currently materialized before decoding; row-group predicate pruning and
streaming remote range reads remain later milestones.

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
