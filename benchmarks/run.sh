#!/usr/bin/env bash
set -euo pipefail

repo_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
dataset_dir="$(mktemp -d "${TMPDIR:-/tmp}/golars-bench-data.XXXXXX")"
result_dir="${GOLARS_BENCH_OUTPUT:-$(mktemp -d "${TMPDIR:-/tmp}/golars-bench-results.XXXXXX")}"
scales="${GOLARS_BENCH_SCALES:-5000 10000 200000 500000 1000000 5000000}"
row_group_size="${GOLARS_BENCH_ROW_GROUP_SIZE:-65536}"
bench_time="${GOLARS_BENCH_TIME:-3x}"
polars_iterations="${GOLARS_BENCH_POLARS_ITERATIONS:-3}"
polars_warmup="${GOLARS_BENCH_WARMUP:-1}"
polars_bin="${GOLARS_POLARS_BENCH_BIN:-$repo_dir/benchmarks/polars/target/release/golars-polars-benchmark}"

mkdir -p "$result_dir"
trap 'rm -rf "$dataset_dir"' EXIT

if [[ ! -x "$polars_bin" ]]; then
	(cd "$repo_dir" && cargo build --manifest-path benchmarks/polars/Cargo.toml --release)
fi

queries=(
	projection_only
	filter_select
	compound_filter
	derived_projection
	grouped_aggregation
	multi_aggregation
	string_filter
	limit_pushdown
	sorted_topk
	window_aggregation
	self_join
	unique_tenant
	full_aggregation
)

echo "benchmark results: $result_dir"
for rows in $scales; do
	dataset="$dataset_dir/input-${rows}.parquet"
	cutoff=$((rows / 2))
	echo "=== ${rows} rows ==="
	(cd "$repo_dir" && go run ./benchmarks/cmd/golars-benchmark-data \
		-output "$dataset" \
		-rows "$rows" \
		-row-group-size "$row_group_size")

	(cd "$repo_dir" && go test ./benchmarks \
		-run '^$' \
		-bench . \
		-benchmem \
		-count=1 \
		-benchtime="$bench_time" \
		-args \
		-bench-dataset="$dataset" \
		-bench-cutoff="$cutoff" \
		-bench-row-group-size="$row_group_size") \
		| tee "$result_dir/${rows}-go.txt"

	for query in "${queries[@]}"; do
		output="$result_dir/${rows}-polars-${query}.json"
		"$polars_bin" \
			--dataset "$dataset" \
			--query "$query" \
			--cutoff "$cutoff" \
			--iterations "$polars_iterations" \
			--warmup "$polars_warmup" \
			| tee "$output"
	done
done
