package parquetio

import (
	"bytes"
	"context"
	"io"
	"os"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/parquet"
	parquetfile "github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"github.com/guywaldman/golars/internal/storage"
)

// OpenFile opens a local Parquet path. Memory mapping is preferred when the
// Arrow reader supports it, with a regular-file fallback.
func OpenFile(ctx context.Context, path string, config Config, request Request) (Source, error) {
	reader, err := openLocalParquetFile(path)
	if err != nil {
		return nil, err
	}
	props := pqarrow.ArrowReadProperties{BatchSize: config.BatchSize, Parallel: config.Parallel}
	arrowReader, err := pqarrow.NewFileReader(reader, props, config.Allocator)
	if err != nil {
		_ = reader.Close()
		return nil, err
	}
	return openParquetReader(ctx, config, reader, arrowReader, request, true)
}

// OpenReader materializes a stream before opening it as Parquet. This is used
// for object stores that expose only sequential reads.
func OpenReader(ctx context.Context, file io.ReadCloser, config Config, request Request) (Source, error) {
	data, err := io.ReadAll(file)
	if err != nil {
		_ = file.Close()
		return nil, err
	}
	reader, err := parquetfile.NewParquetReader(bytes.NewReader(data))
	if err != nil {
		_ = file.Close()
		return nil, err
	}
	props := pqarrow.ArrowReadProperties{BatchSize: config.BatchSize, Parallel: config.Parallel}
	arrowReader, err := pqarrow.NewFileReader(reader, props, config.Allocator)
	if err != nil {
		_ = reader.Close()
		_ = file.Close()
		return nil, err
	}
	return openParquetReader(ctx, config, reader, arrowReader, request, false, file)
}

// OpenRanged uses object metadata and byte ranges instead of materializing the
// complete Parquet object.
func OpenRanged(ctx context.Context, store storage.RangeObjectStore, path string, config Config, request Request) (Source, error) {
	info, err := store.Stat(ctx, path)
	if err != nil {
		return nil, err
	}
	ranged, err := newRangeReaderAtSeeker(ctx, store, path, info.Size)
	if err != nil {
		return nil, err
	}
	reader, err := parquetfile.NewParquetReader(ranged)
	if err != nil {
		_ = ranged.Close()
		return nil, err
	}
	props := pqarrow.ArrowReadProperties{BatchSize: config.BatchSize, Parallel: config.Parallel}
	arrowReader, err := pqarrow.NewFileReader(reader, props, config.Allocator)
	if err != nil {
		_ = reader.Close()
		_ = ranged.Close()
		return nil, err
	}
	return openParquetReader(ctx, config, reader, arrowReader, request, false, ranged)
}

func openLocalParquetFile(path string) (*parquetfile.Reader, error) {
	if reader, err := parquetfile.OpenParquetFile(path, true); err == nil {
		return reader, nil
	}
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	reader, err := parquetfile.NewParquetReader(file)
	if err != nil {
		_ = file.Close()
		return nil, err
	}
	return reader, nil
}

type emptySource struct{ schema *arrow.Schema }

func (s *emptySource) Schema() *arrow.Schema          { return s.schema }
func (s *emptySource) Next() bool                     { return false }
func (s *emptySource) RecordBatch() arrow.RecordBatch { return nil }
func (s *emptySource) Err() error                     { return nil }
func (s *emptySource) Release()                       {}

type fileSource struct {
	reader            Source
	closers           []io.Closer
	filtersFullyMatch bool
}

func (s *fileSource) Schema() *arrow.Schema          { return s.reader.Schema() }
func (s *fileSource) Next() bool                     { return s.reader.Next() }
func (s *fileSource) RecordBatch() arrow.RecordBatch { return s.reader.RecordBatch() }
func (s *fileSource) Err() error                     { return s.reader.Err() }
func (s *fileSource) FiltersFullyMatch() bool {
	if source, ok := s.reader.(FilterGuaranteeSource); ok {
		return source.FiltersFullyMatch()
	}
	return s.filtersFullyMatch
}
func (s *fileSource) Release() {
	if s.reader != nil {
		s.reader.Release()
	}
	for _, closer := range s.closers {
		_ = closer.Close()
	}
	s.reader = nil
	s.closers = nil
}

func openParquetReader(ctx context.Context, config Config, reader *parquetfile.Reader, arrowReader *pqarrow.FileReader, request Request, local bool, closers ...io.Closer) (Source, error) {
	closeAll := func() {
		_ = reader.Close()
		for _, closer := range closers {
			_ = closer.Close()
		}
	}
	schema, err := arrowReader.Schema()
	if err != nil {
		closeAll()
		return nil, err
	}
	columnIndices, err := parquetColumnIndices(arrowReader, request.Columns, config.MissingColumn)
	if err != nil {
		closeAll()
		return nil, err
	}
	rowGroups, fullMatches, selectionApplicable, err := rowGroupSelection(reader, schema, request.Filters, request.SelectionApplicable)
	if err != nil {
		closeAll()
		return nil, err
	}
	fileClosers := make([]io.Closer, 0, len(closers)+1)
	fileClosers = append(fileClosers, reader)
	fileClosers = append(fileClosers, closers...)
	if len(request.Filters) > 0 && selectionApplicable && len(rowGroups) == 0 {
		return &fileSource{reader: &emptySource{schema: schema}, closers: fileClosers}, nil
	}
	if local && selectionApplicable && len(request.FilterOnlyColumns) > 0 && len(request.ProjectionColumns) > 0 && hasFullMatch(fullMatches) {
		projectionIndices, projectionErr := parquetColumnIndices(arrowReader, request.ProjectionColumns, config.MissingColumn)
		if projectionErr != nil {
			closeAll()
			return nil, projectionErr
		}
		requestedSchema := parquetRequestedSchema(schema, columnIndices)
		projected := newProjectionSource(ctx, config.Allocator, arrowReader, requestedSchema, columnIndices, projectionIndices, rowGroups, fullMatches, request.ProjectionOnlyBatches)
		return &fileSource{reader: projected, closers: fileClosers}, nil
	}
	recordReader, err := arrowReader.GetRecordReader(ctx, columnIndices, rowGroups)
	if err != nil {
		closeAll()
		return nil, err
	}
	var source Source = recordReader
	filtersFullyMatch := len(fullMatches) > 0
	for _, full := range fullMatches {
		filtersFullyMatch = filtersFullyMatch && full
	}
	if len(request.Filters) > 0 && selectionApplicable {
		source = newFilterSource(recordReader, reader, rowGroups, fullMatches)
	}
	return &fileSource{reader: source, closers: fileClosers, filtersFullyMatch: filtersFullyMatch}, nil
}

var _ parquet.ReaderAtSeeker = (*rangeReaderAtSeeker)(nil)
