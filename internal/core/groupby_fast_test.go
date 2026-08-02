package core

import (
	"context"
	"reflect"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/array"
)

func TestFastGroupByPreservesNumericAggregates(t *testing.T) {
	ids, err := NewSeries("id", []int64{1, 1, 2, 2, 3})
	if err != nil {
		t.Fatal(err)
	}
	defer ids.Close()
	values, err := NewSeries("value", []float64{1, 2, 3, 4, 5})
	if err != nil {
		t.Fatal(err)
	}
	defer values.Close()
	df, err := NewDataFrame(ids, values)
	if err != nil {
		t.Fatal(err)
	}
	defer df.Close()

	grouped, err := df.Lazy().GroupBy(Col("id")).Agg(Col("value").Sum().Alias("total")).Collect(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer grouped.Close()
	if got := numericColumnValues(t, grouped, "id"); !reflect.DeepEqual(got, []int64{1, 2, 3}) {
		t.Fatalf("group keys=%v", got)
	}
	if got := numericColumnValues(t, grouped, "total"); !reflect.DeepEqual(got, []float64{3, 7, 5}) {
		t.Fatalf("sums=%v", got)
	}
	eager, err := df.GroupBy(Col("id")).Agg(Col("value").Sum().Alias("total"))
	if err != nil {
		t.Fatal(err)
	}
	defer eager.Close()
	if got := numericColumnValues(t, eager, "total"); !reflect.DeepEqual(got, []float64{3, 7, 5}) {
		t.Fatalf("eager sums=%v", got)
	}

	counted, err := df.Lazy().GroupBy(Col("id")).Agg(Col("value").Count().Alias("count")).Collect(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer counted.Close()
	if got := numericColumnValues(t, counted, "count"); !reflect.DeepEqual(got, []int64{2, 2, 1}) {
		t.Fatalf("counts=%v", got)
	}

	means, err := df.Lazy().GroupBy(Col("id")).Agg(Col("value").Mean().Alias("mean")).Collect(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer means.Close()
	if got := numericColumnValues(t, means, "mean"); !reflect.DeepEqual(got, []float64{1.5, 3.5, 5}) {
		t.Fatalf("means=%v", got)
	}
}

func TestSimpleGroupByMultiAggregatePreservesNullSemantics(t *testing.T) {
	keys, err := NewSeries("tenant", []string{"a", "a", "b", "a"})
	if err != nil {
		t.Fatal(err)
	}
	defer keys.Close()
	a, b, c := 1.0, 3.0, 5.0
	values, err := NewSeries("value", []*float64{&a, nil, &b, &c})
	if err != nil {
		t.Fatal(err)
	}
	defer values.Close()
	df, err := NewDataFrame(keys, values)
	if err != nil {
		t.Fatal(err)
	}
	defer df.Close()

	grouped, err := df.Lazy().GroupBy(Col("tenant")).Agg(
		Col("value").Count().Alias("count"),
		Col("value").Sum().Alias("sum"),
		Col("value").Mean().Alias("mean"),
	).Collect(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer grouped.Close()
	if grouped.Height() != 2 {
		t.Fatalf("rows=%d, want 2", grouped.Height())
	}
	if got := numericColumnValues(t, grouped, "sum"); !reflect.DeepEqual(got, []float64{6, 3}) {
		t.Fatalf("sums=%v", got)
	}
	if got := numericColumnValues(t, grouped, "mean"); !reflect.DeepEqual(got, []float64{3, 3}) {
		t.Fatalf("means=%v", got)
	}
	if got := numericColumnValues(t, grouped, "count"); !reflect.DeepEqual(got, []int64{2, 1}) {
		t.Fatalf("counts=%v", got)
	}
}

func numericColumnValues(t *testing.T, df *DataFrame, name string) any {
	t.Helper()
	series, err := df.Column(name)
	if err != nil {
		t.Fatal(err)
	}
	defer series.Close()
	chunked := series.ArrowChunked()
	defer chunked.Release()
	switch values := chunked.Chunk(0).(type) {
	case *array.Int64:
		out := make([]int64, values.Len())
		for i := range out {
			out[i] = values.Value(i)
		}
		return out
	case *array.Float64:
		out := make([]float64, values.Len())
		for i := range out {
			out[i] = values.Value(i)
		}
		return out
	default:
		t.Fatalf("column %q has type %T", name, chunked.Chunk(0))
		return nil
	}
}
