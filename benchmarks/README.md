# Cross-language benchmarks

The benchmark harness runs the same lazy Parquet workloads against golars and
native Rust Polars. The Go side is a normal Go benchmark (`go test -bench`),
so its output includes the standard `ns/op`, `B/op`, and `allocs/op` metrics.

## Quick gobench

Generate the input inside the benchmark test process and run one workload at
5,000 rows:

```sh
go test ./benchmarks \
  -run '^$' \
  -bench . \
  -benchmem \
  -benchtime=3x \
  -args -bench-rows=5000
```

The generated input is deterministic. `-bench-cutoff` defaults to half the
generated row count, so the same command works for every scale. To benchmark
an existing Parquet file instead:

```sh
go test ./benchmarks \
  -run '^$' \
  -bench . \
  -benchmem \
  -benchtime=3x \
  -args \
  -bench-dataset=/tmp/golars-benchmark.parquet \
  -bench-cutoff=500000
```

## Go vs native Polars across scales

Run the complete workload set at the default scales (5K, 10K, 200K, 500K,
1M, and 5M rows):

```sh
./benchmarks/run.sh
```

Add 10M when the machine has enough time and memory:

```sh
GOLARS_BENCH_SCALES="5000 10000 200000 500000 1000000 5000000 10000000" \
./benchmarks/run.sh
```

The script builds the pinned Rust Polars runner when needed, creates one
deterministic Parquet file per scale, runs Go's `-benchmem` benchmarks, and
runs each equivalent Polars query with matching warmups and iterations. It
prints the result directory; Go output is stored as `${rows}-go.txt` and the
Polars measurements as `${rows}-polars-${query}.json`.

The default comparison uses one unmeasured warmup and three measured
executions per workload (`-benchtime=3x` for Go and `--warmup 1
--iterations 3` for Polars). `GOLARS_BENCH_TIME`, `GOLARS_BENCH_WARMUP`, and
`GOLARS_BENCH_POLARS_ITERATIONS` override those values.

Use fewer scales while iterating:

```sh
GOLARS_BENCH_SCALES="5000 200000" \
GOLARS_BENCH_TIME=1x \
GOLARS_BENCH_POLARS_ITERATIONS=1 \
./benchmarks/run.sh
```

`GOMAXPROCS` and `POLARS_MAX_THREADS` should be set to the same value when
comparing implementations, for example:

```sh
GOMAXPROCS=2 POLARS_MAX_THREADS=2 ./benchmarks/run.sh
```

Compare runs only on the same machine, CPU, Go/Rust versions, thread count,
input, warmup, and iteration settings. Verify that result shapes and row
counts agree before interpreting timings. The tests in this package validate
the canonical result shapes and representative values for every workload.

The Rust baseline is pinned by `benchmarks/polars/Cargo.lock`; update it
deliberately when changing the comparison version.
