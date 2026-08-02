package core

import (
	"context"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/array"
	nativebackend "github.com/guywaldman/golars/internal/native"
)

func TestNativeCollectKeepsFrameUntilArrowAccess(t *testing.T) {
	if _, err := nativebackend.Open(); err != nil {
		t.Skipf("native backend unavailable: %v", err)
	}
	ids, err := NewSeries("id", []int64{1, 2, 3})
	if err != nil {
		t.Fatal(err)
	}
	defer ids.Close()
	values, err := NewSeries("value", []int64{10, 20, 30})
	if err != nil {
		t.Fatal(err)
	}
	defer values.Close()
	input, err := NewDataFrame(ids, values)
	if err != nil {
		t.Fatal(err)
	}
	defer input.Close()
	path := filepath.Join(t.TempDir(), "input.parquet")
	if err := WriteParquet(context.Background(), input, path, WithOverwrite(true)); err != nil {
		t.Fatal(err)
	}

	engine, err := NewEngine()
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close()
	result, err := engine.ScanParquet(path).
		Filter(Col("id").Gt(Lit(int64(1)))).
		Select(Col("value")).
		Collect(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer result.Close()
	if result.native == nil {
		t.Fatal("native collect returned a materialized Go table")
	}
	if result.Height() != 2 || result.Width() != 1 {
		t.Fatalf("shape=(%d,%d), want (2,1)", result.Height(), result.Width())
	}
	if got := result.Schema().Names(); !reflect.DeepEqual(got, []string{"value"}) {
		t.Fatalf("schema names=%v", got)
	}

	column, err := result.Column("value")
	if err != nil {
		t.Fatal(err)
	}
	defer column.Close()
	if result.native != nil {
		t.Fatal("Arrow column access did not materialize the native frame")
	}
	chunked := column.ArrowChunked()
	defer chunked.Release()
	var got []int64
	for _, chunk := range chunked.Chunks() {
		valuesArray, ok := chunk.(*array.Int64)
		if !ok {
			t.Fatalf("column type=%T, want *array.Int64", chunk)
		}
		for row := 0; row < valuesArray.Len(); row++ {
			got = append(got, valuesArray.Value(row))
		}
	}
	if !reflect.DeepEqual(got, []int64{20, 30}) {
		t.Fatalf("values=%v", got)
	}
}
