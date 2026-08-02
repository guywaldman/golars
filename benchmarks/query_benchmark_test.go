package benchmarks

import (
	"context"
	"flag"
	"os"
	"runtime"
	"testing"

	golars "github.com/guywaldman/golars"
)

var benchmarkDataset = flag.String("bench-dataset", "", "Parquet input for the cross-language benchmarks")
var benchmarkCutoff = flag.Int64("bench-cutoff", DefaultCutoff, "inclusive id filter cutoff")

func BenchmarkProjectionOnly(b *testing.B) {
	benchmarkQuery(b, ProjectionOnly)
}

func BenchmarkFilterSelect(b *testing.B) {
	benchmarkQuery(b, FilterSelect)
}

func BenchmarkCompoundFilter(b *testing.B) {
	benchmarkQuery(b, CompoundFilter)
}

func BenchmarkDerivedProjection(b *testing.B) {
	benchmarkQuery(b, DerivedProjection)
}

func BenchmarkGroupedAggregation(b *testing.B) {
	benchmarkQuery(b, GroupedAggregation)
}

func BenchmarkMultiAggregation(b *testing.B) {
	benchmarkQuery(b, MultiAggregation)
}

func BenchmarkStringFilter(b *testing.B) {
	benchmarkQuery(b, StringFilter)
}

func BenchmarkLimitPushdown(b *testing.B) {
	benchmarkQuery(b, LimitPushdown)
}

func BenchmarkSortedTopK(b *testing.B) {
	benchmarkQuery(b, SortedTopK)
}

func BenchmarkWindowAggregation(b *testing.B) {
	benchmarkQuery(b, WindowAggregation)
}

func BenchmarkSelfJoin(b *testing.B) {
	benchmarkQuery(b, SelfJoin)
}

func BenchmarkUniqueTenant(b *testing.B) {
	benchmarkQuery(b, UniqueTenant)
}

func BenchmarkFullAggregation(b *testing.B) {
	benchmarkQuery(b, FullAggregation)
}

func benchmarkQuery(b *testing.B, query Query) {
	path := *benchmarkDataset
	if path == "" {
		path = os.Getenv("GOLARS_BENCH_DATASET")
	}
	if path == "" {
		b.Skip("set -bench-dataset or GOLARS_BENCH_DATASET")
	}
	info, err := os.Stat(path)
	if err != nil {
		b.Fatal(err)
	}
	engine, err := golars.NewEngine(
		golars.WithBatchSize(DefaultRowGroupSize),
		golars.WithParallelism(runtime.GOMAXPROCS(0)),
	)
	if err != nil {
		b.Fatal(err)
	}
	defer engine.Close()
	ctx := context.Background()
	var rows int64
	b.SetBytes(info.Size())
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		result, err := Execute(ctx, engine, path, query, *benchmarkCutoff)
		if err != nil {
			b.Fatal(err)
		}
		rows += result.Height()
		result.Close()
	}
	b.ReportMetric(float64(rows)/float64(b.N), "rows")
}
