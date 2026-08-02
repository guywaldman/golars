package benchmarks

import (
	"context"
	"flag"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	golars "github.com/guywaldman/golars"
)

var benchmarkDataset = flag.String("bench-dataset", "", "Parquet input for the cross-language benchmarks")
var benchmarkRows = flag.Int64("bench-rows", 0, "generate a deterministic Parquet input with this many rows")
var benchmarkRowGroupSize = flag.Int64("bench-row-group-size", DefaultRowGroupSize, "row-group size for a generated benchmark input")
var benchmarkCutoff = flag.Int64("bench-cutoff", -1, "inclusive id filter cutoff; defaults to half the generated input")
var generatedBenchmarkDataset string
var generatedBenchmarkDirectory string

func TestMain(m *testing.M) {
	flag.Parse()
	if *benchmarkDataset == "" && os.Getenv("GOLARS_BENCH_DATASET") == "" && *benchmarkRows > 0 {
		if *benchmarkRowGroupSize <= 0 {
			_, _ = os.Stderr.WriteString("-bench-row-group-size must be positive\n")
			os.Exit(2)
		}
		directory, err := os.MkdirTemp("", "golars-benchmark-")
		if err != nil {
			_, _ = os.Stderr.WriteString(err.Error() + "\n")
			os.Exit(2)
		}
		generatedBenchmarkDirectory = directory
		generatedBenchmarkDataset = filepath.Join(directory, "input.parquet")
		if err := WriteDataset(generatedBenchmarkDataset, *benchmarkRows, *benchmarkRowGroupSize); err != nil {
			_, _ = os.Stderr.WriteString(err.Error() + "\n")
			os.Exit(2)
		}
	}
	exitCode := m.Run()
	if generatedBenchmarkDirectory != "" {
		_ = os.RemoveAll(generatedBenchmarkDirectory)
	}
	os.Exit(exitCode)
}

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
		path = generatedBenchmarkDataset
	}
	if path == "" {
		b.Skip("set -bench-dataset, GOLARS_BENCH_DATASET, or -bench-rows")
	}
	info, err := os.Stat(path)
	if err != nil {
		b.Fatal(err)
	}
	engine, err := golars.NewEngine(
		golars.WithBatchSize(*benchmarkRowGroupSize),
		golars.WithParallelism(runtime.GOMAXPROCS(0)),
	)
	if err != nil {
		b.Fatal(err)
	}
	defer engine.Close()
	ctx := context.Background()
	cutoff := *benchmarkCutoff
	if cutoff < 0 {
		cutoff = DefaultCutoff
		if *benchmarkRows > 0 {
			cutoff = *benchmarkRows / 2
		}
	}
	warmup, err := Execute(ctx, engine, path, query, cutoff)
	if err != nil {
		b.Fatal(err)
	}
	warmup.Close()
	var rows int64
	b.SetBytes(info.Size())
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		result, err := Execute(ctx, engine, path, query, cutoff)
		if err != nil {
			b.Fatal(err)
		}
		rows += result.Height()
		result.Close()
	}
	b.ReportMetric(float64(rows)/float64(b.N), "rows")
}
