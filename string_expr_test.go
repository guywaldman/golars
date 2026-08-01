package golars

import (
	"context"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/array"
)

func TestStringExpressionsPreserveNullsAndUnicode(t *testing.T) {
	alpha := "Alpha"
	accented := "éclair"
	name, err := NewSeries("name", []*string{&alpha, nil, &accented, &alpha})
	if err != nil {
		t.Fatal(err)
	}
	defer name.Close()
	df, err := NewDataFrame(name)
	if err != nil {
		t.Fatal(err)
	}
	defer df.Close()

	contains, err := df.Filter(Col("name").Str().Contains(Lit("ha")))
	if err != nil {
		t.Fatal(err)
	}
	defer contains.Close()
	if contains.Height() != 2 {
		t.Fatalf("contains height=%d, want 2", contains.Height())
	}
	starts, err := df.Filter(Col("name").Str().StartsWith(Lit("é")))
	if err != nil {
		t.Fatal(err)
	}
	defer starts.Close()
	if starts.Height() != 1 {
		t.Fatalf("starts height=%d, want 1", starts.Height())
	}
	ends, err := df.Filter(Col("name").Str().EndsWith(Lit("air")))
	if err != nil {
		t.Fatal(err)
	}
	defer ends.Close()
	if ends.Height() != 1 {
		t.Fatalf("ends height=%d, want 1", ends.Height())
	}

	lengths, err := df.Lazy().Select(Col("name").Str().Lengths().Alias("length")).Collect(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer lengths.Close()
	series, err := lengths.Column("length")
	if err != nil {
		t.Fatal(err)
	}
	defer series.Close()
	chunked := series.ArrowChunked()
	defer chunked.Release()
	values, ok := chunked.Chunk(0).(*array.Int64)
	if !ok {
		t.Fatalf("length type=%T", chunked.Chunk(0))
	}
	if values.Value(0) != 5 || values.IsValid(1) || values.Value(2) != 6 || values.Value(3) != 5 {
		t.Fatalf("length values=%v/%v/%v/%v", values.Value(0), values.IsValid(1), values.Value(2), values.Value(3))
	}
}
