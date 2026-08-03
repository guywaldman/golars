# Golars

A high-performance Arrow-native, Polars-like DataFrame API for Go.

If you know [polars](https://pola.rs/), you'll feel right at home. In fact, golars uses native polars under the hood.

```shell
go get github.com/guywaldman/golars
```

The package bundles native dependencies such that you can install it easily (as you would `polars`).  
It exposes high-level DataFrame APIs, with "zero configuration" support (e.g., for reading from S3/GCS):

```go
import gl "github.com/guywaldman/golars"

// ...
df, _ := gl.ReadParquet(ctx, "events.parquet")
summary, _ := df.GroupBy(gl.Col("country")).Agg(
	gl.Col("amount").Sum().Alias("amount_total"),
	gl.Col("user_id").Count().Alias("users"),
)
_ = summary.WriteCSV(ctx, "out.csv")
```

Note that you can read from local files as well as cloud storage (S3 with `s3://` or Google Cloud Storage with `gs://`) using their default credential discovery, as Polars does.
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
- [X] Row-oriented constructors: `DataFrameFromMaps` and generic
  `DataFrameFromStructs`, with nullable missing fields and standard `json` tags.
- [X] Eager and lazy `Select`/`SelectSeq`, `Filter`, `WithColumns`/`WithColumnsSeq`,
  `Limit`/`Head`/`Tail`, `Slice`, `Reverse`, `Clear`, `Sort`,
  `GroupBy(...).Agg(...)`, joins, window expressions, `Unique`, `Explode`,
  `Unnest`, `Unpivot`/`Melt`, fixed rolling expressions, as-of joins, `Drop`,
  `Rename`, `DropNulls`, and `DropNaNs`.
- [X] Expression trees with arithmetic (including modulo, powers, rounding, and
  trigonometric kernels), comparisons, boolean logic, null/NaN handling, casts,
  aggregates, conditional expressions, partitioned windows, descending sort,
  membership/range predicates, list expressions, and common string/date kernels.
- [X] Native JSON-column decoding into typed structs and lists, with SIMD parsing,
  row-level nulling for malformed JSON, field-level nulling for bad leaves,
  coercion controls, and strict required-field validation.
- [X] Struct field expressions such as `Col("payload").Struct().Element("user_id")`,
  including nested struct access.
- [X] Arrow type descriptors - primitive, temporal, decimal, dictionary
  (categorical/enum), nested list/struct/map, fixed-size, view, interval, and
  run-end encoded types. `DataFrame`/`Series` constructors accept Arrow arrays,
  chunked arrays, tables, and record batches without a copy at the boundary.
- [X] Lazy scans and eager reads for Parquet, CSV, NDJSON, JSON arrays, and Arrow IPC file/stream formats (with `ScanArrow`/`ReadArrow` aliases).
- [X] Scan projection and bounded reads, CSV delimiters, headers, comments,
  null markers, quote handling, and schema-inference controls.
- [X] Lazy and eager sinks for the same formats, with non-overwriting local-file defaults. `LazyFrame.Sink` selects the writer from the destination extension.
- [X] Row-oriented helpers: `Rows` materializes detached row maps, while
  `ForEachRow` streams them without retaining the complete result. `ToDicts`
  remains as a Polars-compatible alias.
- [X] Series value access with `Values`/`ToSlice`, zero-copy `Slice`, `Clone`,
  `IsEmpty`, and Arrow chunk metadata; DataFrames expose `Columns`, `IsEmpty`,
  `Clone`, and the corresponding row operations.
- [X] Local globbing and Hive-style Parquet partition discovery.
- [X] Hive dataset writes with `Create`/`Append`, partition columns,
  `_SUCCESS`, and `_golars_schema.json`.
- [X] Built-in S3 and GCS URI support using standard credential discovery. Custom
  `ObjectStore` implementations can override them with `RegisterObjectStore`
  or `WithObjectStore`.
- [X] Engine-wide memory accounting with a 1 GiB default quota and contextual
  cancellation through `context.Context`.

## Native execution

All relational execution runs through the embedded Rust/Polars backend. The
Go layer owns the public API, Arrow conversion, and storage adapters (including
custom object stores and formats Polars does not open directly). The
target-specific native library is embedded in the Go module and loaded with
CGO-free FFI, so consumers only need the normal `go get` and Go build workflow.
For object-store scans, the Go adapter preserves projected columns, simple
Parquet predicates, row-group pruning, and bounded reads before handing Arrow
batches to Polars.

The checked-in assets currently support Linux amd64/arm64 and macOS arm64.
A target without an embedded asset returns an explicit native-backend
availability error; there is no portable execution fallback.

## Supported platforms

| Operating system | Architectures | Native execution |
| --- | --- | --- |
| Linux | amd64, arm64 | Supported and validated in CI |
| macOS | arm64 | Supported and validated in CI |
| Windows | — | Not supported yet ([#3](https://github.com/guywaldman/golars/issues/3)) |

The package can still be compiled on other Go targets, but collection returns
an explicit native-backend availability error because no matching Polars
library is embedded.

## Fast JSON parsing

`DecodeJSON` turns a string expression into a typed Arrow value. You can provide
an Arrow descriptor or derive the schema directly from a Go struct. Exported
fields use standard `json` tags; pointer fields are nullable, and nested
structs and lists are supported.

```go
schema := gl.StructOf(
	gl.Field{Name: "id", Type: gl.Int64, Nullable: false},
	gl.Field{Name: "tenant", Type: gl.String, Nullable: true},
	gl.Field{Name: "tags", Type: gl.ListOf(gl.String), Nullable: true},
)

decoded := gl.DecodeJSON(gl.Col("payload_json"), schema)
result, err := lf.Select(decoded.Alias("payload")).Collect(ctx)
```

The struct form avoids a separate schema declaration:

```go
type Payload struct {
	ID      int64    `json:"id"`
	Tenant  string   `json:"tenant"`
	Tags    []string `json:"tags"`
	Profile *struct {
		Country string `json:"country"`
	} `json:"profile"`
}

decoded := gl.DecodeJSON(gl.Col("payload_json"), Payload{})
// Or use gl.DecodeJSONAs[Payload](gl.Col("payload_json")).
```

Rows with malformed JSON or incompatible leaves become null by default, while
valid sibling fields are retained. Use `WithJSONCoerce(false)` for strict JSON
types, `WithJSONErrorMode(JSONErrorOnError)` to fail the query, or
`WithJSONStrictRequiredFields(true)` to enforce non-nullable struct fields.

## Examples

```go
df, err := gl.ReadCSV(ctx, "events.csv")
if err != nil { return err }
defer df.Close()

summary, err := df.GroupBy(gl.Col("country")).Agg(
	gl.Col("amount").Sum().Alias("amount_total"),
	gl.Col("user_id").Count().Alias("users"),
)
if err != nil { return err }
defer summary.Close()

err = summary.WriteParquet(ctx, "out.parquet")
```

For an explicitly configured engine:

```go
engine, err := gl.NewEngine(
	gl.WithMemoryLimit(512<<20),
	gl.WithBatchSize(32<<10),
	gl.WithObjectStore("s3", s3Store),
)
if err != nil { return err }
defer engine.Close()

df, err := engine.ScanParquet("s3://bucket/events/*.parquet").Collect(ctx)
```

The implementation is intentionally a fast-breaking v0 API. The execution
kernel and format contracts are stable enough for experimentation, while the
public type names and richer Polars parity surface will continue to grow.

## Performance and Benchmarks

The [`benchmarks`](benchmarks) directory contains equivalent Go and Rust
Polars workloads. The following run was captured on 2026-08-03 on a MacBook
Pro with an Apple M3 Max (16 cores, 128 GB RAM), using Go 1.26.4, Rust 1.96.0,
Polars 0.52.0, and 16 threads for both implementations. Each cell is
`golars / polars` in milliseconds per execution, followed by the golars-to-
polars ratio.

Each implementation had one unmeasured warmup and three measured executions.
The Go side used `-benchtime=3x`; the Rust side used `--warmup 1
--iterations 3`.

| Input Rows | Projection | Filter + Select | Grouped Aggregation | Window Aggregation | Self Join |
|---:|---:|---:|---:|---:|---:|
| 5K | 0.592 / 0.349 (1.69x) | 0.523 / 0.377 (1.39x) | 0.925 / 0.867 (1.07x) | 0.900 / 1.136 (0.79x) | 2.157 / 1.913 (1.13x) |
| 10K | 0.731 / 0.406 (1.80x) | 0.802 / 0.362 (2.22x) | 0.902 / 0.812 (1.11x) | 1.039 / 0.998 (1.04x) | 2.330 / 1.541 (1.51x) |
| 200K | 1.061 / 0.771 (1.38x) | 0.694 / 0.484 (1.43x) | 1.365 / 1.296 (1.05x) | 1.579 / 1.575 (1.00x) | 2.400 / 1.749 (1.37x) |
| 500K | 1.658 / 1.176 (1.41x) | 0.900 / 0.639 (1.41x) | 1.745 / 1.655 (1.05x) | 2.013 / 2.587 (0.78x) | 3.164 / 2.794 (1.13x) |
| 1M | 2.093 / 1.729 (1.21x) | 1.054 / 0.872 (1.21x) | 2.417 / 2.299 (1.05x) | 3.330 / 3.423 (0.97x) | 3.457 / 3.102 (1.11x) |
| 5M | 5.248 / 5.349 (0.98x) | 2.136 / 2.163 (0.99x) | 5.463 / 4.763 (1.15x) | 13.530 / 13.562 (1.00x) | 9.485 / 8.038 (1.18x) |

For the 1M-row dataset, the complete workload set was:

| Workload | golars ms/op | polars ms/op | ratio | golars B/op | polars RSS delta | rows |
|---|---:|---:|---:|---:|---:|---:|
| `projection_only` | 2.093 | 1.729 | 1.21x | 2,322 | 36.2 MiB | 1,000,000 |
| `filter_select` | 1.054 | 0.872 | 1.21x | 3,586 | 6.2 MiB | 500,000 |
| `compound_filter` | 1.056 | 0.843 | 1.25x | 5,034 | 3.0 MiB | 15,625 |
| `derived_projection` | 2.349 | 1.956 | 1.20x | 7,194 | 30.5 MiB | 500,000 |
| `grouped_aggregation` | 2.417 | 2.299 | 1.05x | 4,178 | 26.0 MiB | 128 |
| `multi_aggregation` | 3.191 | 3.169 | 1.01x | 5,842 | 17.6 MiB | 32 |
| `string_filter` | 1.807 | 1.830 | 0.99x | 3,898 | 19.1 MiB | 10,000 |
| `limit_pushdown` | 0.786 | 0.806 | 0.98x | 4,642 | 2.8 MiB | 1,000 |
| `sorted_topk` | 4.656 | 5.051 | 0.92x | 5,682 | 13.0 MiB | 100 |
| `window_aggregation` | 3.330 | 3.423 | 0.97x | 6,186 | 13.0 MiB | 500,000 |
| `self_join` | 3.457 | 3.102 | 1.11x | 10,482 | 31.5 MiB | 125,008 |
| `unique_tenant` | 4.404 | 4.757 | 0.93x | 3,421 | 20.8 MiB | 32 |
| `full_aggregation` | 5.106 | 5.341 | 0.96x | 4,330 | 19.6 MiB | 32 |
| `json_decode` | 41.711 | 52.246 | 0.80x | 5,728 | 157.9 MiB | 1,000,000 |

Go's `B/op` is the allocation metric reported by `go test`; the Polars column
is the process RSS increase reported by the Rust runner, so those two memory
figures are directional rather than directly interchangeable.

To reproduce this exact scale sweep with explicit thread parity:

```sh
GOMAXPROCS=16 POLARS_MAX_THREADS=16 \
GOLARS_BENCH_TIME=3x \
GOLARS_BENCH_POLARS_ITERATIONS=3 \
GOLARS_BENCH_WARMUP=1 \
./benchmarks/run.sh
```

## Contributing

Feel very welcome to do so! See [`CONTRIBUTING.md`](CONTRIBUTING.md).
