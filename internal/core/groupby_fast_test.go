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

func TestGroupByScanRequestIncludesOnlyRequiredColumns(t *testing.T) {
	base := &planNode{kind: planFilter, input: &planNode{kind: planSource}, exprs: []Expr{Col("id").Ge(Lit(int64(10)))}}
	request := deriveGroupByScanRequest(base, []Expr{Col("group_id")}, []Expr{Col("value").Sum()})
	if !reflect.DeepEqual(request.columns, []string{"group_id", "id", "value"}) {
		t.Fatalf("columns=%v", request.columns)
	}
	if len(request.filters) != 1 {
		t.Fatalf("filters=%d", len(request.filters))
	}
}

func TestScanRequestResolvesWithColumnsDependencies(t *testing.T) {
	source := &planNode{kind: planSource}
	filter := &planNode{
		kind:  planFilter,
		input: source,
		exprs: []Expr{Col("id").Ge(Lit(int64(10)))},
	}
	withColumns := &planNode{
		kind:  planWithColumns,
		input: filter,
		exprs: []Expr{
			Col("value").Mul(Col("quantity").Cast(Float64)).Alias("revenue"),
			Col("payload").Str().Lengths().Alias("payload_len"),
		},
	}
	selectNode := &planNode{
		kind:  planSelect,
		input: withColumns,
		exprs: []Expr{Col("tenant"), Col("revenue"), Col("payload_len")},
	}
	request := deriveScanRequest([]*planNode{source, filter, withColumns, selectNode})
	if !reflect.DeepEqual(request.columns, []string{"id", "payload", "quantity", "tenant", "value"}) {
		t.Fatalf("columns=%v", request.columns)
	}
	if !reflect.DeepEqual(request.projectionColumns, []string{"payload", "quantity", "tenant", "value"}) {
		t.Fatalf("projection columns=%v", request.projectionColumns)
	}
	if !reflect.DeepEqual(request.filterOnlyColumns, []string{"id"}) {
		t.Fatalf("filter-only columns=%v", request.filterOnlyColumns)
	}
	if len(request.filters) != 1 || !request.projectionOnlyBatches {
		t.Fatalf("request filters=%d projection-only=%v", len(request.filters), request.projectionOnlyBatches)
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
