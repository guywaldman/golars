package golars

import (
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

func sortedInt64Values(t *testing.T, df *DataFrame) []int64 {
	t.Helper()
	column, err := df.Column("value")
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
