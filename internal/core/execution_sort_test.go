package core

import (
	"context"
	"reflect"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/array"
)

func TestSortUsesTypedOrdering(t *testing.T) {
	values, err := NewSeries("value", []int64{10, 2, 1})
	if err != nil {
		t.Fatal(err)
	}
	defer values.Close()
	df, err := NewDataFrame(values)
	if err != nil {
		t.Fatal(err)
	}
	defer df.Close()

	sorted, err := df.Sort(Col("value"))
	if err != nil {
		t.Fatal(err)
	}
	defer sorted.Close()
	if got := sortedInt64Values(t, sorted); !reflect.DeepEqual(got, []int64{1, 2, 10}) {
		t.Fatalf("sorted values=%v", got)
	}

	descending, err := df.Sort(Col("value").Desc())
	if err != nil {
		t.Fatal(err)
	}
	defer descending.Close()
	if got := sortedInt64Values(t, descending); !reflect.DeepEqual(got, []int64{10, 2, 1}) {
		t.Fatalf("descending values=%v", got)
	}
}

func TestSortLimitKeepsStableTopKRows(t *testing.T) {
	ids, err := NewSeries("id", []int64{0, 1, 2, 3})
	if err != nil {
		t.Fatal(err)
	}
	defer ids.Close()
	values, err := NewSeries("value", []float64{3, 5, 5, 1})
	if err != nil {
		t.Fatal(err)
	}
	defer values.Close()
	df, err := NewDataFrame(ids, values)
	if err != nil {
		t.Fatal(err)
	}
	defer df.Close()
	plan := df.Lazy().Sort(Col("value").Desc()).Limit(2)
	defer plan.Close()
	result, err := plan.Collect(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer result.Close()
	if got := int64ColumnValues(t, result, "id"); !reflect.DeepEqual(got, []int64{1, 2}) {
		t.Fatalf("top-k ids=%v", got)
	}
}

func TestTailRunsThroughNativePlan(t *testing.T) {
	values, err := NewSeries("value", []int64{1, 2, 3, 4})
	if err != nil {
		t.Fatal(err)
	}
	defer values.Close()
	df, err := NewDataFrame(values)
	if err != nil {
		t.Fatal(err)
	}
	defer df.Close()

	tail, err := df.Tail(2)
	if err != nil {
		t.Fatal(err)
	}
	defer tail.Close()
	if got := int64ColumnValues(t, tail, "value"); !reflect.DeepEqual(got, []int64{3, 4}) {
		t.Fatalf("tail values=%v", got)
	}
}

func sortedInt64Values(t *testing.T, df *DataFrame) []int64 {
	return int64ColumnValues(t, df, "value")
}

func int64ColumnValues(t *testing.T, df *DataFrame, name string) []int64 {
	t.Helper()
	column, err := df.Column(name)
	if err != nil {
		t.Fatal(err)
	}
	defer column.Close()
	chunked := column.ArrowChunked()
	defer chunked.Release()
	valuesArray, ok := chunked.Chunk(0).(*array.Int64)
	if !ok {
		t.Fatalf("sorted column type=%T", chunked.Chunk(0))
	}
	got := make([]int64, valuesArray.Len())
	for i := range got {
		got[i] = valuesArray.Value(i)
	}
	return got
}
