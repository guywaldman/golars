# Golars

A high-performance Arrow-native, Polars-like DataFrame API for Go.

If you know [polars](https://pola.rs/), you'll feel right at home. In fact, golars uses native polars under the hood.

```shell
go get github.com/guywaldman/golars
```

The package bundles native dependencies such that you can install it easily (as you would `polars`).  
It exposes high-level DataFrame APIs, with "zero configuration" support (e.g., for reading from S3/GCS):

```go
import gl "github.com/guywaldman/polars"

// ...
df, _ := golars.ReadParquet(ctx, "events.parquet")
summary, _ := df.GroupBy(golars.Col("country")).Agg(
	golars.Col("amount").Sum().Alias("amount_total"),
	golars.Col("user_id").Count().Alias("users"),
)
_ = summary.WriteCSV(ctx, "out.csv")
```

Note that you can read from local files as well as cloud storage (S3 with `s3://` or Google Cloud Storage with `gs://`) using their default credential discovery, as Polars one.
If you need to configure it (such as for auth), you can use:

```go
store, err := gl.NewGCSStore(context.Background(), option.WithoutAuthentication())
if err != nil {
  panic(err)
}
defer store.Close()

if err := gl.RegisterObjectStore("gs", store); err != nil {
  panic(err)
}

// ...
```

## Featureset

- [X] Arrow-backed `DataFrame` and `Series`, with retained Arrow interop.
- [X] Eager and lazy `Select`, `Filter`, `WithColumns`, `Limit`/`Head`, `Sort`,
  `GroupBy(...).Agg(...)`, joins, window expressions, `Unique`, `Explode`,
  `Unnest`, `Drop`, `Rename`, and `DropNulls`.
- [X] Expression trees with arithmetic (including modulo), comparisons, boolean logic, null handling,
  casts, aggregates, conditional expressions, partitioned windows, descending
  sort, membership/range predicates, and common string/date kernels.
- [X] Arrow type descriptors - primitive, temporal, decimal, dictionary
  (categorical/enum), nested list/struct/map, fixed-size, view, interval, and
  run-end encoded types. `DataFrame`/`Series` constructors accept Arrow arrays,
  chunked arrays, tables, and record batches without a copy at the boundary.
- [X] Lazy scans and eager reads for Parquet, CSV, NDJSON, JSON arrays, and Arrow IPC file/stream formats.
- [X] Lazy and eager sinks for the same formats, with non-overwriting local-file defaults. `LazyFrame.Sink` selects the writer from the destination extension.
- [X] Row-oriented helpers: `Rows`/`ToDicts` materialize detached row maps, while
  `ForEachRow` streams them without retaining the complete result.
- [X] Local globbing and Hive-style Parquet partition discovery.
- [X] Hive dataset writes with `Create`/`Append`, partition columns,
  `_SUCCESS`, and `_golars_schema.json`.
- [X] Built-in S3 and GCS URI support using standard credential discovery. Custom
  `ObjectStore` implementations can override them with `RegisterObjectStore`
  or `WithObjectStore`.
- [X] Engine-wide memory accounting with a 1 GiB default quota and contextual
  cancellation through `context.Context`.

## Native execution

On supported targets, eligible local Parquet plans run through the Rust/Polars
backend. The target-specific native library is embedded in the Go module and
loaded with CGO-free FFI, such that consumers only need the normal `go get` and Go build
workflow.
Other targets and plans use the portable Go executor automatically. Applications can force the Go executor with
`WithNativeBackend(false)`.

## Query planning and storage

Similar to Polars:

- [X] Lazy scans derive the columns required by a terminal projection and its
  filters. Parquet reads only those Arrow columns; CSV and JSON readers apply
  the same projection at ingestion.
- [X] The optimized logical plan performs expression simplification, projection
  collapse, predicate pushdown through safe projections, adjacent-filter
  fusion, limit fusion/pushdown, type coercion, and storage predicate/projection
  pushdown. `ExplainUnoptimized` and `ExplainOptimized` expose both forms.

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
- `benchmarks` contains the Go/Rust comparison harness and its runnable tools;
  there is no benchmark-only code under `cmd/`.

See [`CONTRIBUTING.md`](CONTRIBUTING.md) for the development setup, test
commands, E2E requirements, and benchmark workflow.

## Performance and Benchmarks

The [`benchmarks`](benchmarks) directory contains equivalent Go and Rust

These are some results on an M3 Max MacBook Pro with 128 GiB RAM and 16 cores at varying scales (format indicates golars/polars):

| Input Rows | Projection | Filter + Select | Grouped Aggregation | Window Aggregation | Self Join |
|---:|---:|---:|---:|---:|---:|
| 5K | 0.558 / 0.353 (1.58x) | 0.546 / 0.439 (1.24x) | 1.578 / 1.313 (1.20x) | 1.048 / 0.876 (1.20x) | 7.301 / 2.136 (3.42x) |
| 10K | 0.500 / 0.422 (1.19x) | 0.411 / 0.391 (1.05x) | 0.831 / 0.873 (0.95x) | 1.177 / 1.072 (1.10x) | 7.561 / 1.723 (4.39x) |
| 200K | 0.962 / 1.008 (0.95x) | 0.562 / 0.515 (1.09x) | 1.275 / 1.356 (0.94x) | 9.604 / 1.667 (5.76x) | 11.638 / 1.868 (6.23x) |
| 500K | 1.402 / 1.301 (1.08x) | 0.681 / 0.573 (1.19x) | 1.863 / 1.882 (0.99x) | 17.097 / 2.388 (7.16x) | 10.906 / 2.405 (4.54x) |
| 1M | 1.610 / 1.688 (0.95x) | 1.007 / 0.932 (1.08x) | 2.718 / 2.527 (1.08x) | 32.209 / 3.381 (9.53x) | 11.554 / 3.151 (3.67x) |
| 5M | 5.769 / 5.833 (0.99x) | 2.271 / 2.271 (1.00x) | 4.994 / 5.070 (0.98x) | 229.646 / 15.214 (15.09x) | 13.001 / 7.967 (1.63x) |

For a 1M row dataset:

| Workload | golars ms/op | polars ms/op | ratio | golars B/op | polars RSS delta | rows |
|---|---:|---:|---:|---:|---:|---:|
| `projection_only` | 1.610 | 1.688 | 0.95x | 3,306 | 33.2 MiB | 1,000,000 |
| `filter_select` | 1.007 | 0.932 | 1.08x | 4,826 | 6.6 MiB | 500,000 |
| `compound_filter` | 1.138 | 0.974 | 1.17x | 6,762 | 2.5 MiB | 15,625 |
| `derived_projection` | 2.273 | 2.251 | 1.01x | 9,162 | 28.4 MiB | 500,000 |
| `grouped_aggregation` | 2.718 | 2.527 | 1.08x | 5,642 | 20.4 MiB | 128 |
| `multi_aggregation` | 3.495 | 3.772 | 0.93x | 7,914 | 18.1 MiB | 32 |
| `string_filter` | 1.819 | 2.050 | 0.89x | 5,242 | 17.1 MiB | 10,000 |
| `limit_pushdown` | 0.855 | 0.922 | 0.93x | 7,746 | 4.1 MiB | 1,000 |
| `sorted_topk` | 5.309 | 5.287 | 1.00x | 7,194 | 12.4 MiB | 100 |
| `window_aggregation` | 32.209 | 3.381 | 9.53x | 408,630,405 | 15.6 MiB | 500,000 |
| `self_join` | 11.554 | 3.151 | 3.67x | 59,484,768 | 36.9 MiB | 125,008 |
| `unique_tenant` | 18.793 | 4.828 | 3.89x | 229,897,232 | 21.8 MiB | 32 |
| `full_aggregation` | 6.274 | 5.608 | 1.12x | 6,042 | 25.7 MiB | 32 |

To reproduce this exact scale sweep with explicit thread parity:

```sh
GOMAXPROCS=16 POLARS_MAX_THREADS=16 \
GOLARS_BENCH_TIME=3x \
GOLARS_BENCH_POLARS_ITERATIONS=3 \
GOLARS_BENCH_WARMUP=1 \
./benchmarks/run.sh
```
