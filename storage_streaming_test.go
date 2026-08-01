package golars

import (
	"bytes"
	"context"
	"io"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	parquetfile "github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
)

func TestRowGroupPruningAndRangeReads(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "data.parquet")
	writerEngine, err := NewEngine(WithBatchSize(2))
	if err != nil {
		t.Fatal(err)
	}
	defer writerEngine.Close()
	id, err := NewSeries("id", []int64{1, 2, 3, 4, 5, 6})
	if err != nil {
		t.Fatal(err)
	}
	defer id.Close()
	value, err := NewSeries("value", []int64{1, 2, 3, 4, 5, 6})
	if err != nil {
		t.Fatal(err)
	}
	defer value.Close()
	df, err := newDataFrame(writerEngine, id, value)
	if err != nil {
		t.Fatal(err)
	}
	defer df.Close()
	if err := WriteParquet(ctx, df, path); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	reader, err := parquetfile.NewParquetReader(bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	props := pqarrow.ArrowReadProperties{BatchSize: 2}
	arrowReader, err := pqarrow.NewFileReader(reader, props, memory.NewGoAllocator())
	if err != nil {
		_ = reader.Close()
		t.Fatal(err)
	}
	schema, err := arrowReader.Schema()
	if err != nil {
		_ = reader.Close()
		t.Fatal(err)
	}
	rowGroups, err := parquetRowGroupIndices(reader, schema, []*exprNode{Col("value").Gt(Lit(int64(4))).node})
	_ = reader.Close()
	if err != nil {
		t.Fatal(err)
	}
	if len(rowGroups) != 1 {
		t.Fatalf("expected one matching row group, got %v", rowGroups)
	}

	store := &recordingRangeStore{uri: "mem://bucket/data.parquet", data: data}
	engine, err := NewEngine(WithObjectStore("mem", store), WithBatchSize(2))
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close()
	result, err := engine.ScanParquet(store.uri).
		Filter(Col("value").Gt(Lit(int64(4)))).
		Select(Col("id")).
		Collect(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer result.Close()
	if result.Height() != 2 || result.Width() != 1 {
		t.Fatalf("unexpected pruned result shape=(%d,%d)", result.Height(), result.Width())
	}
	if store.fullOpens != 0 || len(store.ranges) == 0 {
		t.Fatalf("expected range reads only, full opens=%d ranges=%d", store.fullOpens, len(store.ranges))
	}
}

func TestParquetFilterGuaranteesPreservePartialGroups(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "data.parquet")
	engine, err := NewEngine(WithBatchSize(2))
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close()
	values, err := NewSeries("value", []int64{1, 2, 3, 4, 5, 6})
	if err != nil {
		t.Fatal(err)
	}
	defer values.Close()
	df, err := newDataFrame(engine, values)
	if err != nil {
		t.Fatal(err)
	}
	defer df.Close()
	if err := WriteParquet(ctx, df, path, WithWriteBatchSize(2)); err != nil {
		t.Fatal(err)
	}
	result, err := engine.ScanParquet(path).
		Filter(Col("value").Gt(Lit(int64(3)))).
		Select(Col("value")).
		Collect(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer result.Close()
	if result.Height() != 3 {
		t.Fatalf("expected three matching rows, got %d", result.Height())
	}
	table, err := result.ArrowTable()
	if err != nil {
		t.Fatal(err)
	}
	defer table.Release()
	row := 0
	for _, chunk := range table.Column(0).Data().Chunks() {
		valuesArray := chunk.(*array.Int64)
		for index := 0; index < valuesArray.Len(); index++ {
			expected := int64(4 + row)
			if !valuesArray.IsValid(index) || valuesArray.Value(index) != expected {
				t.Fatalf("row %d=%v, want %d", row, valuesArray.Value(index), expected)
			}
			row++
		}
	}
	if row != 3 {
		t.Fatalf("validated %d rows, want 3", row)
	}
}

func TestParquetFilterGuaranteesKeepNullSemantics(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "nullable.parquet")
	engine, err := NewEngine(WithBatchSize(2))
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close()
	builder := array.NewInt64Builder(memory.NewGoAllocator())
	builder.Append(5)
	builder.AppendNull()
	builder.Append(6)
	builder.Append(7)
	valuesArray := builder.NewInt64Array()
	builder.Release()
	defer valuesArray.Release()
	values, err := NewSeriesFromArrow("value", valuesArray)
	if err != nil {
		t.Fatal(err)
	}
	defer values.Close()
	df, err := newDataFrame(engine, values)
	if err != nil {
		t.Fatal(err)
	}
	defer df.Close()
	if err := WriteParquet(ctx, df, path, WithWriteBatchSize(2)); err != nil {
		t.Fatal(err)
	}
	result, err := engine.ScanParquet(path).Filter(Col("value").Ge(Lit(int64(5)))).Collect(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer result.Close()
	if result.Height() != 3 {
		t.Fatalf("expected null filter to keep three values, got %d", result.Height())
	}
}

func TestForEachBatchAndStreamingSink(t *testing.T) {
	ctx := context.Background()
	engine, err := NewEngine(WithBatchSize(2))
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close()
	id, err := NewSeries("id", []int64{1, 2, 3, 4, 5})
	if err != nil {
		t.Fatal(err)
	}
	defer id.Close()
	df, err := newDataFrame(engine, id)
	if err != nil {
		t.Fatal(err)
	}
	defer df.Close()
	var batches, rows int
	err = df.Lazy().Filter(Col("id").Gt(Lit(int64(1)))).ForEachBatch(ctx, func(batch arrow.RecordBatch) error {
		batches++
		rows += int(batch.NumRows())
		if batch.NumCols() != 1 {
			t.Fatalf("unexpected batch width=%d", batch.NumCols())
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if batches != 3 || rows != 4 {
		t.Fatalf("unexpected batch stream batches=%d rows=%d", batches, rows)
	}
	sinkPath := filepath.Join(t.TempDir(), "sink.parquet")
	if err := df.Lazy().Filter(Col("id").Gt(Lit(int64(1)))).Sink(ctx, sinkPath); err != nil {
		t.Fatal(err)
	}
	sinked, err := ReadParquet(ctx, sinkPath)
	if err != nil {
		t.Fatal(err)
	}
	defer sinked.Close()
	if sinked.Height() != 4 || sinked.Width() != 1 {
		t.Fatalf("unexpected sink shape=(%d,%d)", sinked.Height(), sinked.Width())
	}
}

type recordingRangeStore struct {
	mu        sync.Mutex
	uri       string
	data      []byte
	fullOpens int
	ranges    [][2]int64
}

func (s *recordingRangeStore) Open(_ context.Context, uri string) (io.ReadCloser, error) {
	s.mu.Lock()
	s.fullOpens++
	s.mu.Unlock()
	if uri != s.uri {
		return nil, os.ErrNotExist
	}
	return io.NopCloser(bytes.NewReader(s.data)), nil
}

func (s *recordingRangeStore) Create(_ context.Context, uri string) (io.WriteCloser, error) {
	if uri != s.uri {
		return nil, os.ErrNotExist
	}
	return nopWriteCloser{Writer: io.Discard}, nil
}

func (s *recordingRangeStore) Glob(_ context.Context, pattern string) ([]string, error) {
	if pattern != s.uri {
		return nil, os.ErrNotExist
	}
	return []string{s.uri}, nil
}

func (s *recordingRangeStore) Stat(_ context.Context, uri string) (ObjectInfo, error) {
	if uri != s.uri {
		return ObjectInfo{}, os.ErrNotExist
	}
	return ObjectInfo{Size: int64(len(s.data))}, nil
}

func (s *recordingRangeStore) OpenRange(_ context.Context, uri string, offset, length int64) (io.ReadCloser, error) {
	if uri != s.uri || offset < 0 || length <= 0 || offset >= int64(len(s.data)) {
		return nil, os.ErrNotExist
	}
	end := offset + length
	if end > int64(len(s.data)) {
		end = int64(len(s.data))
	}
	s.mu.Lock()
	s.ranges = append(s.ranges, [2]int64{offset, end - offset})
	s.mu.Unlock()
	return io.NopCloser(bytes.NewReader(s.data[offset:end])), nil
}

type nopWriteCloser struct{ io.Writer }

func (nopWriteCloser) Close() error { return nil }
