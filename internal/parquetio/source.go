package parquetio

import (
	"bytes"
	"context"
	"io"
	"os"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	parquetfile "github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"github.com/guywaldman/golars/internal/storage"
)

// OpenFile opens a local Parquet path. Memory mapping is preferred when the
// Arrow reader supports it, with a regular-file fallback.
func OpenFile(ctx context.Context, path string, config Config, request Request) (Source, error) {
	reader, err := openLocalParquetFile(path, config.Allocator)
	if err != nil {
		return nil, err
	}
	props := parquetArrowReadProperties(config)
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
	props := parquetArrowReadProperties(config)
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
	props := parquetArrowReadProperties(config)
	arrowReader, err := pqarrow.NewFileReader(reader, props, config.Allocator)
	if err != nil {
		_ = reader.Close()
		_ = ranged.Close()
		return nil, err
	}
	return openParquetReader(ctx, config, reader, arrowReader, request, false, ranged)
}

func parquetArrowReadProperties(config Config) pqarrow.ArrowReadProperties {
	// Metadata-based binary preallocation over-reserves this workload and adds
	// more copying than it saves while the builders grow their actual payloads.
	return pqarrow.ArrowReadProperties{
		BatchSize:          config.BatchSize,
		Parallel:           config.Parallel,
		PreAllocBinaryData: false,
	}
}

func openLocalParquetFile(path string, allocator memory.Allocator) (*parquetfile.Reader, error) {
	props := parquet.NewReaderProperties(allocator)
	props.BufferedStreamEnabled = true
	if reader, err := parquetfile.OpenParquetFile(path, true, parquetfile.WithReadProps(props)); err == nil {
		return reader, nil
	}
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	reader, err := parquetfile.NewParquetReader(file, parquetfile.WithReadProps(props))
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
func (s *fileSource) PrefetchesBatches() bool {
	_, ok := s.reader.(*parallelSource)
	return ok
}
func (s *fileSource) GuaranteedFilterColumns() []string {
	if source, ok := s.reader.(interface{ GuaranteedFilterColumns() []string }); ok {
		return source.GuaranteedFilterColumns()
	}
	return nil
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
	rowGroups, fullMatches, projectionMatches, selectionApplicable, err := rowGroupSelection(reader, schema, request.Filters, request.SelectionApplicable, request.FilterOnlyColumns)
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
	if local && selectionApplicable && len(request.FilterOnlyColumns) > 0 && len(request.ProjectionColumns) > 0 && hasFullMatch(projectionMatches) {
		projectionIndices, projectionErr := parquetColumnIndices(arrowReader, request.ProjectionColumns, config.MissingColumn)
		if projectionErr != nil {
			closeAll()
			return nil, projectionErr
		}
		requestedSchema := parquetRequestedSchema(schema, columnIndices)
		parallelism := config.Parallelism
		if request.Limit > 0 {
			parallelism = 1
		}
		projected := newProjectionSource(ctx, config.Allocator, arrowReader, requestedSchema, columnIndices, projectionIndices, rowGroups, fullMatches, projectionMatches, request.FilterOnlyColumns, request.ProjectionOnlyBatches, parallelism)
		return &fileSource{reader: projected, closers: fileClosers}, nil
	}
	// The Arrow reader parallelizes columns, but row groups remain serial. For
	// local files, independent readers improve scan throughput while keeping the
	// bounded source contract. Exact filter evaluation remains in the plan.
	if local && config.Parallel && config.Parallelism > 1 && shouldParallelizeLocalScan(request) {
		selectedGroups := rowGroups
		if len(selectedGroups) == 0 {
			selectedGroups = make([]int, reader.NumRowGroups())
			for i := range selectedGroups {
				selectedGroups[i] = i
			}
		}
		if len(selectedGroups) > 1 {
			parallel := newParallelSource(ctx, arrowReader, schema, columnIndices, selectedGroups, config.Parallelism)
			return &fileSource{reader: parallel, closers: fileClosers}, nil
		}
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

func shouldParallelizeLocalScan(request Request) bool {
	if request.Limit > 0 {
		return false
	}
	// Keep filtered projections on one reader when the predicate column is also
	// part of the output. A downstream blocking operator may only need a small
	// subset, so fanning out those row groups would eagerly duplicate buffers.
	return len(request.Filters) == 0 || len(request.FilterOnlyColumns) > 0
}

var _ parquet.ReaderAtSeeker = (*rangeReaderAtSeeker)(nil)
