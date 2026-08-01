# Cross-language benchmarks

The benchmark pair runs the same two lazy Parquet queries against the same
generated input:

- `filter_select` filters on the sorted `id` column and selects `group_id` and
  `value`. This exercises projection and row-group predicate pruning.
- `grouped_aggregation` applies the same filter and groups `value` by
  `group_id`.

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

Repeat both runner commands with `-query grouped_aggregation`. Each emits one
JSON record containing elapsed time, rows, and peak resident set size. The Go
runner also reports runtime heap and allocation counters; standard Go
`-benchmem` reports allocations per operation. The timed Rust path uses its
normal allocator so the performance result is not perturbed by memory
instrumentation. Compare values on the same machine, CPU, runtime versions,
input file, thread count, and warmup/iteration settings.

For standard Go allocation reports, run:

```sh
go test ./benchmarks -run '^$' -bench . -benchmem \
  -args -bench-dataset=/tmp/golars-benchmark.parquet -bench-cutoff=500000
```

The Rust project is pinned to the Polars version in its lockfile. Update it
deliberately when changing the comparison baseline.
