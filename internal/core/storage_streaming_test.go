package core

import (
	"bytes"
	"context"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"sync"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	parquetfile "github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
)

func TestBuiltInGCSStoreIsCreatedOnFirstUse(t *testing.T) {
	engine, err := NewEngine()
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close()

	store, err := engine.ensureObjectStore(context.Background(), "gs")
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := store.(*GCSStore); !ok {
		t.Fatalf("default gs store type=%T, want *GCSStore", store)
	}
	registered, ok := engine.ObjectStore("gs")
	if !ok || registered != store {
		t.Fatal("default gs store was not retained by the engine")
	}
}

func TestSingleRemotePathDoesNotList(t *testing.T) {
	store := &recordingRangeStore{uri: "mem://bucket/data.parquet"}
	engine, err := NewEngine(WithObjectStore("mem", store))
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close()

	paths, err := expandPathsForEngine(context.Background(), engine, store.uri, formatParquet)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(paths, []string{store.uri}) {
		t.Fatalf("paths=%v, want [%s]", paths, store.uri)
	}
	if store.globCalls != 0 {
		t.Fatalf("single object path called Glob %d times", store.globCalls)
	}
}

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

func TestParquetProjectionPreservesFullAndPartialGroups(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "projected.parquet")
	engine, err := NewEngine(WithBatchSize(2))
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close()
	id, err := NewSeries("id", []int64{1, 2, 3, 4, 5, 6})
	if err != nil {
		t.Fatal(err)
	}
	defer id.Close()
	value, err := NewSeries("value", []int64{10, 20, 30, 40, 50, 60})
	if err != nil {
		t.Fatal(err)
	}
	defer value.Close()
	df, err := newDataFrame(engine, id, value)
	if err != nil {
		t.Fatal(err)
	}
	defer df.Close()
	if err := WriteParquet(ctx, df, path, WithWriteBatchSize(2)); err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		name      string
		predicate Expr
		expected  []int64
	}{
		{
			name:      "full groups",
			predicate: Col("id").Gt(Lit(int64(2))),
			expected:  []int64{30, 40, 50, 60},
		},
		{
			name:      "partial and full groups",
			predicate: Col("id").Gt(Lit(int64(3))),
			expected:  []int64{40, 50, 60},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			result, err := engine.ScanParquet(path).
				Filter(test.predicate).
				Select(Col("value")).
				Collect(ctx)
			if err != nil {
				t.Fatal(err)
			}
			defer result.Close()
			if got := allInt64ColumnValues(t, result, "value"); len(got) != len(test.expected) {
				t.Fatalf("values=%v, want %v", got, test.expected)
			} else {
				for i := range got {
					if got[i] != test.expected[i] {
						t.Fatalf("values=%v, want %v", got, test.expected)
					}
				}
			}
		})
	}
}

func TestParquetProjectionKeepsPartialFiltersExact(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "compound-projection.parquet")
	engine, err := NewEngine(WithBatchSize(2))
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close()
	id, err := NewSeries("id", []int64{1, 2, 3, 4, 5, 6})
	if err != nil {
		t.Fatal(err)
	}
	defer id.Close()
	tenant, err := NewSeries("tenant", []string{"a", "a", "b", "b", "a", "b"})
	if err != nil {
		t.Fatal(err)
	}
	defer tenant.Close()
	value, err := NewSeries("value", []int64{10, 20, 30, 40, 50, 60})
	if err != nil {
		t.Fatal(err)
	}
	defer value.Close()
	df, err := newDataFrame(engine, id, tenant, value)
	if err != nil {
		t.Fatal(err)
	}
	defer df.Close()
	if err := WriteParquet(ctx, df, path, WithWriteBatchSize(2)); err != nil {
		t.Fatal(err)
	}
	result, err := engine.ScanParquet(path).
		Filter(Col("id").Gt(Lit(int64(2))).And(Col("tenant").Eq(Lit("b")))).
		Select(Col("tenant"), Col("value")).
		Collect(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer result.Close()
	if result.Height() != 3 {
		t.Fatalf("expected three matching rows, got %d", result.Height())
	}
	if got := allInt64ColumnValues(t, result, "value"); !reflect.DeepEqual(got, []int64{30, 40, 60}) {
		t.Fatalf("values=%v, want [30 40 60]", got)
	}
	series, err := result.Column("tenant")
	if err != nil {
		t.Fatal(err)
	}
	defer series.Close()
	chunked := series.ArrowChunked()
	defer chunked.Release()
	values, ok := chunked.Chunk(0).(array.StringLike)
	if !ok {
		t.Fatalf("tenant column has type %T", chunked.Chunk(0))
	}
	for row := 0; row < values.Len(); row++ {
		if !values.IsValid(row) || values.Value(row) != "b" {
			t.Fatalf("tenant row %d=%q, want b", row, values.Value(row))
		}
	}
}

func TestParquetUnsupportedFilterFallsBackToExactEvaluation(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "unsupported-filter.parquet")
	engine, err := NewEngine(WithBatchSize(2))
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close()
	id, err := NewSeries("id", []int64{1, 2, 3, 4, 5, 6})
	if err != nil {
		t.Fatal(err)
	}
	defer id.Close()
	value, err := NewSeries("value", []int64{10, 20, 30, 40, 50, 60})
	if err != nil {
		t.Fatal(err)
	}
	defer value.Close()
	df, err := newDataFrame(engine, id, value)
	if err != nil {
		t.Fatal(err)
	}
	defer df.Close()
	if err := WriteParquet(ctx, df, path, WithWriteBatchSize(2)); err != nil {
		t.Fatal(err)
	}
	result, err := engine.ScanParquet(path).
		Filter(Col("id").Add(Lit(int64(1))).Gt(Lit(int64(3)))).
		Select(Col("value")).
		Collect(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer result.Close()
	got := allInt64ColumnValues(t, result, "value")
	expected := []int64{30, 40, 50, 60}
	if len(got) != len(expected) {
		t.Fatalf("values=%v, want %v", got, expected)
	}
	for i := range got {
		if got[i] != expected[i] {
			t.Fatalf("values=%v, want %v", got, expected)
		}
	}
}

func TestParquetProjectionKeepsGroupedSchemaStable(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "grouped.parquet")
	engine, err := NewEngine(WithBatchSize(2))
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close()
	id, err := NewSeries("id", []int64{1, 2, 3, 4, 5, 6})
	if err != nil {
		t.Fatal(err)
	}
	defer id.Close()
	group, err := NewSeries("group", []int64{1, 1, 2, 2, 3, 3})
	if err != nil {
		t.Fatal(err)
	}
	defer group.Close()
	value, err := NewSeries("value", []int64{10, 20, 30, 40, 50, 60})
	if err != nil {
		t.Fatal(err)
	}
	defer value.Close()
	df, err := newDataFrame(engine, id, group, value)
	if err != nil {
		t.Fatal(err)
	}
	defer df.Close()
	if err := WriteParquet(ctx, df, path, WithWriteBatchSize(2)); err != nil {
		t.Fatal(err)
	}
	result, err := engine.ScanParquet(path).
		Filter(Col("id").Gt(Lit(int64(3)))).
		GroupBy(Col("group")).
		Agg(Col("value").Sum().Alias("total")).
		Collect(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer result.Close()
	if result.Height() != 2 || result.Width() != 2 {
		t.Fatalf("unexpected grouped shape=(%d,%d)", result.Height(), result.Width())
	}
	groups := numericColumnValues(t, result, "group").([]int64)
	totals := numericColumnValues(t, result, "total").([]int64)
	if len(groups) != 2 || groups[0] != 2 || groups[1] != 3 {
		t.Fatalf("groups=%v, want [2 3]", groups)
	}
	if len(totals) != 2 || totals[0] != 40 || totals[1] != 110 {
		t.Fatalf("totals=%v, want [40 110]", totals)
	}
}

func allInt64ColumnValues(t *testing.T, df *DataFrame, name string) []int64 {
	t.Helper()
	series, err := df.Column(name)
	if err != nil {
		t.Fatal(err)
	}
	defer series.Close()
	chunked := series.ArrowChunked()
	defer chunked.Release()
	var values []int64
	for _, chunk := range chunked.Chunks() {
		arrayValues, ok := chunk.(*array.Int64)
		if !ok {
			t.Fatalf("column %q has type %T, want *array.Int64", name, chunk)
		}
		for index := 0; index < arrayValues.Len(); index++ {
			if !arrayValues.IsValid(index) {
				t.Fatalf("column %q has null at row %d", name, len(values))
			}
			values = append(values, arrayValues.Value(index))
		}
	}
	return values
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
	globCalls int
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
	s.mu.Lock()
	s.globCalls++
	s.mu.Unlock()
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
