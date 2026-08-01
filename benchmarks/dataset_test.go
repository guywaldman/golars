package benchmarks

import (
	"context"
	"path/filepath"
	"testing"

	golars "github.com/guywaldman/golars"
)

func TestWriteDatasetAndExecute(t *testing.T) {
	path := filepath.Join(t.TempDir(), "input.parquet")
	if err := WriteDataset(path, 100, 10); err != nil {
		t.Fatal(err)
	}
	engine, err := golars.NewEngine(golars.WithBatchSize(10))
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close()
	filterResult, err := Execute(context.Background(), engine, path, FilterSelect, 50)
	if err != nil {
		t.Fatal(err)
	}
	filterHeight, filterWidth := filterResult.Shape()
	if filterHeight != 50 || filterWidth != 2 {
		filterResult.Close()
		t.Fatalf("unexpected filter result shape=(%d,%d)", filterHeight, filterWidth)
	}
	filterResult.Close()
	groupResult, err := Execute(context.Background(), engine, path, GroupedAggregation, 50)
	if err != nil {
		t.Fatal(err)
	}
	defer groupResult.Close()
	if groupResult.Height() != 50 || groupResult.Width() != 2 {
		t.Fatalf("unexpected grouped result shape=(%d,%d)", groupResult.Height(), groupResult.Width())
	}
}
