# Cross-language benchmarks

The benchmark pair runs the same lazy Parquet queries against the same
generated input. The workload names are:

- `projection_only` reads three columns without a filter, exercising pure
  projection pushdown.
- `filter_select` filters on the sorted `id` column and selects `group_id` and
  `value`, exercising predicate pruning plus projection pushdown.
- `compound_filter` combines the sorted `id` predicate with a categorical
  tenant predicate and projects three columns.
- `derived_projection` filters, computes a numeric derived column and a string
  length, then projects the derived result.
- `grouped_aggregation` applies the `id` filter and runs the fast numeric
  `group_id`/`value` sum path.
- `multi_aggregation` groups by a string tenant key and computes sum, mean and
  count together, exercising the general aggregation path.
- `string_filter` applies a non-pushdown string prefix predicate and projects
  three columns.
- `limit_pushdown` stops after a small filtered result, exercising limit
  propagation through projection and scan batches.
- `sorted_topk` filters, sorts by a scan column, and keeps the first 100 rows,
  exposing top-k and materialization gaps.
- `window_aggregation` computes a partitioned sum over the filtered rows.
- `self_join` joins two bounded slices of the same input on the tenant key.
- `unique_tenant` deduplicates a projected string key.
- `full_aggregation` groups the complete input without a selective filter.

Generate one input file, then run each implementation in separate processes:

```sh
go run ./cmd/golars-benchmark-data \
  -output /tmp/golars-benchmark.parquet \
  -rows 1000000 \
  -row-group-size 65536

go run ./cmd/golars-benchmark \
  -dataset /tmp/golars-benchmark.parquet \
  -query filter_select \
  -iterations 5

cargo run --release \
  --manifest-path benchmarks/polars/Cargo.toml \
  -- --dataset /tmp/golars-benchmark.parquet \
  --query filter_select \
  --iterations 5
```

Repeat both runner commands with each workload name above. Each emits one JSON
record containing elapsed time, rows, and peak resident set size. The Go runner
also reports runtime heap deltas and allocation counters; standard Go
`-benchmem` reports allocations per operation. The timed Rust path uses its
normal allocator so the performance result is not perturbed by memory
instrumentation. Compare values on the same machine, CPU, runtime versions,
input file, thread count, and warmup/iteration settings.
The `rows` field must agree between implementations before timings are
interpreted; the Go benchmark tests also check exact shapes and aggregation
values.

For standard Go allocation reports, run:

```sh
go test ./benchmarks -run '^$' -bench . -benchmem \
  -args -bench-dataset=/tmp/golars-benchmark.parquet -bench-cutoff=500000
```

The Rust project is pinned to the Polars version in its lockfile. Update it
deliberately when changing the comparison baseline.
