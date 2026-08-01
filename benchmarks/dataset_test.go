package benchmarks

import (
	"context"
	"fmt"
	"math"
	"path/filepath"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/array"
	golars "github.com/guywaldman/golars"
)

func TestWriteDatasetAndExecute(t *testing.T) {
	path := filepath.Join(t.TempDir(), "input.parquet")
	const rows int64 = 10_000
	const cutoff int64 = 5_000
	if err := WriteDataset(path, rows, 1_000); err != nil {
		t.Fatal(err)
	}
	engine, err := golars.NewEngine(golars.WithBatchSize(10))
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close()
	tests := []struct {
		query  Query
		width  int
		height int64
	}{
		{ProjectionOnly, 3, rows},
		{FilterSelect, 2, rows - cutoff},
		{CompoundFilter, 3, countTenantRows(rows, cutoff, 7)},
		{DerivedProjection, 3, rows - cutoff},
		{GroupedAggregation, 2, 128},
		{MultiAggregation, 4, 32},
		{StringFilter, 3, countPayloadPrefix(rows)},
	}
	for _, test := range tests {
		result, err := Execute(context.Background(), engine, path, test.query, cutoff)
		if err != nil {
			t.Fatalf("%s: %v", test.query, err)
		}
		height, width := result.Shape()
		result.Close()
		if height != test.height || width != test.width {
			t.Fatalf("%s: unexpected shape=(%d,%d), want (%d,%d)", test.query, height, width, test.height, test.width)
		}
	}

	checkDerivedProjection(t, ExecuteOrFatal(t, engine, path, DerivedProjection, cutoff), cutoff)
	checkGroupedAggregation(t, ExecuteOrFatal(t, engine, path, GroupedAggregation, cutoff), rows, cutoff)
	checkMultiAggregation(t, ExecuteOrFatal(t, engine, path, MultiAggregation, cutoff), rows, cutoff)
}

func ExecuteOrFatal(t *testing.T, engine *golars.Engine, path string, query Query, cutoff int64) *golars.DataFrame {
	t.Helper()
	result, err := Execute(context.Background(), engine, path, query, cutoff)
	if err != nil {
		t.Fatalf("%s: %v", query, err)
	}
	return result
}

func checkDerivedProjection(t *testing.T, result *golars.DataFrame, cutoff int64) {
	t.Helper()
	defer result.Close()
	revenue := float64Values(t, result, "revenue")
	payloadLength := int64Values(t, result, "payload_len")
	if len(revenue) == 0 || len(revenue) != len(payloadLength) {
		t.Fatalf("derived projection lengths=(%d,%d)", len(revenue), len(payloadLength))
	}
	wantRevenue := float64(cutoff%10_000) / 10 * float64(cutoff%20+1)
	if math.Abs(revenue[0]-wantRevenue) > 1e-9 {
		t.Fatalf("first revenue=%v, want %v", revenue[0], wantRevenue)
	}
	if payloadLength[0] != int64(len(fmt.Sprintf("payload-%04d", cutoff%10_000))) {
		t.Fatalf("first payload length=%d", payloadLength[0])
	}
}

func checkGroupedAggregation(t *testing.T, result *golars.DataFrame, rows, cutoff int64) {
	t.Helper()
	defer result.Close()
	keys := int64Values(t, result, "group_id")
	totals := float64Values(t, result, "total")
	if len(keys) != 128 || len(totals) != 128 {
		t.Fatalf("grouped columns lengths=(%d,%d)", len(keys), len(totals))
	}
	for i, key := range keys {
		var want float64
		first := cutoff + (key-cutoff%128+128)%128
		for row := first; row < rows; row += 128 {
			want += float64(row%10_000) / 10
		}
		if math.Abs(totals[i]-want) > 1e-9 {
			t.Fatalf("group %d total=%v, want %v", key, totals[i], want)
		}
	}
}

func checkMultiAggregation(t *testing.T, result *golars.DataFrame, rows, cutoff int64) {
	t.Helper()
	defer result.Close()
	tenants := stringValues(t, result, "tenant")
	sums := float64Values(t, result, "value_sum")
	means := float64Values(t, result, "value_mean")
	counts := int64Values(t, result, "value_count")
	if len(tenants) != 32 || len(sums) != 32 || len(means) != 32 || len(counts) != 32 {
		t.Fatalf("multi-aggregation column lengths=(%d,%d,%d,%d)", len(tenants), len(sums), len(means), len(counts))
	}
	wantSums := make(map[string]float64, 32)
	wantCounts := make(map[string]int64, 32)
	for row := cutoff; row < rows; row++ {
		tenant := fmt.Sprintf("tenant-%02d", row%32)
		wantSums[tenant] += float64(row%10_000) / 10
		wantCounts[tenant]++
	}
	for i, tenant := range tenants {
		if counts[i] != wantCounts[tenant] {
			t.Fatalf("tenant %s count=%d, want %d", tenant, counts[i], wantCounts[tenant])
		}
		if math.Abs(sums[i]-wantSums[tenant]) > 1e-9 {
			t.Fatalf("tenant %s sum=%v, want %v", tenant, sums[i], wantSums[tenant])
		}
		if math.Abs(means[i]-sums[i]/float64(counts[i])) > 1e-9 {
			t.Fatalf("tenant %s mean=%v, sum/count=%v", tenant, means[i], sums[i]/float64(counts[i]))
		}
	}
}

func countTenantRows(rows, cutoff, tenant int64) int64 {
	var count int64
	for row := cutoff; row < rows; row++ {
		if row%32 == tenant {
			count++
		}
	}
	return count
}

func countPayloadPrefix(rows int64) int64 {
	var count int64
	for row := int64(0); row < rows; row++ {
		if row%10_000 < 100 {
			count++
		}
	}
	return count
}

func int64Values(t *testing.T, df *golars.DataFrame, name string) []int64 {
	t.Helper()
	series, err := df.Column(name)
	if err != nil {
		t.Fatal(err)
	}
	defer series.Close()
	chunked := series.ArrowChunked()
	defer chunked.Release()
	values := make([]int64, 0, series.Len())
	for _, chunk := range chunked.Chunks() {
		arrayValues, ok := chunk.(*array.Int64)
		if !ok {
			t.Fatalf("column %q has type %T", name, chunk)
		}
		for i := 0; i < arrayValues.Len(); i++ {
			values = append(values, arrayValues.Value(i))
		}
	}
	return values
}

func float64Values(t *testing.T, df *golars.DataFrame, name string) []float64 {
	t.Helper()
	series, err := df.Column(name)
	if err != nil {
		t.Fatal(err)
	}
	defer series.Close()
	chunked := series.ArrowChunked()
	defer chunked.Release()
	values := make([]float64, 0, series.Len())
	for _, chunk := range chunked.Chunks() {
		arrayValues, ok := chunk.(*array.Float64)
		if !ok {
			t.Fatalf("column %q has type %T", name, chunk)
		}
		for i := 0; i < arrayValues.Len(); i++ {
			values = append(values, arrayValues.Value(i))
		}
	}
	return values
}

func stringValues(t *testing.T, df *golars.DataFrame, name string) []string {
	t.Helper()
	series, err := df.Column(name)
	if err != nil {
		t.Fatal(err)
	}
	defer series.Close()
	chunked := series.ArrowChunked()
	defer chunked.Release()
	values := make([]string, 0, series.Len())
	for _, chunk := range chunked.Chunks() {
		arrayValues, ok := chunk.(*array.String)
		if !ok {
			t.Fatalf("column %q has type %T", name, chunk)
		}
		for i := 0; i < arrayValues.Len(); i++ {
			values = append(values, arrayValues.Value(i))
		}
	}
	return values
}
