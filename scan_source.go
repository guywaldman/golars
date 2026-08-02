package golars

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/csv"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/guywaldman/golars/internal/hive"
	"github.com/guywaldman/golars/internal/ipcio"
	"github.com/guywaldman/golars/internal/parquetio"
	"github.com/guywaldman/golars/internal/storageuri"
)

type formatKind uint8

const (
	formatParquet formatKind = iota
	formatCSV
	formatNDJSON
	formatJSON
	formatIPCFile
	formatIPCStream
)

func scanFormat(path string, format formatKind, options ...ScanOption) *LazyFrame {
	return getDefaultEngine().scanFormat(path, format, options...)
}

func (e *Engine) scanFormat(path string, format formatKind, options ...ScanOption) *LazyFrame {
	if e == nil {
		return &LazyFrame{err: ErrClosed}
	}
	config := defaultScanConfig(e)
	for _, option := range options {
		option(&config)
	}
	return newScanLazyFrameWithEngine(e, path, format, config)
}

func newScanLazyFrameWithEngine(engine *Engine, path string, format formatKind, config scanConfig) *LazyFrame {
	factory := func(ctx context.Context, engine *Engine) (recordSource, error) {
		paths, err := expandPathsForEngine(ctx, engine, path)
		if err != nil {
			return nil, err
		}
		return newMultiSource(ctx, engine, paths, format, config)
	}
	return newLazyFrame(engine, &planNode{kind: planSource, source: factory}, nil)
}

func scanReader(reader io.Reader, format formatKind, options ...ScanOption) *LazyFrame {
	engine := getDefaultEngine()
	config := defaultScanConfig(engine)
	if format == formatNDJSON {
		config.hasHeader = false
	}
	for _, option := range options {
		option(&config)
	}
	factory := func(ctx context.Context, engine *Engine) (recordSource, error) {
		if reader == nil {
			return nil, fmt.Errorf("golars: nil input reader")
		}
		return openReaderSource(ctx, engine, &nonClosingReader{Reader: reader}, format, config, scanRequestFromContext(ctx))
	}
	return newLazyFrame(engine, &planNode{kind: planSource, source: factory}, nil)
}

type nonClosingReader struct{ io.Reader }

func (nonClosingReader) Close() error { return nil }

func expandPaths(ctx context.Context, path string) ([]string, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if strings.ContainsAny(path, "*?[") {
		paths, err := filepath.Glob(path)
		if err != nil {
			return nil, err
		}
		sort.Strings(paths)
		if len(paths) == 0 {
			return nil, fmt.Errorf("golars: no files matched %q", path)
		}
		return paths, nil
	}
	if info, err := os.Stat(path); err == nil && info.IsDir() {
		var paths []string
		err := filepath.WalkDir(path, func(candidate string, entry os.DirEntry, walkErr error) error {
			if walkErr != nil {
				return walkErr
			}
			if entry.IsDir() || !strings.EqualFold(filepath.Ext(candidate), ".parquet") {
				return nil
			}
			paths = append(paths, candidate)
			return nil
		})
		if err != nil {
			return nil, err
		}
		sort.Strings(paths)
		if len(paths) == 0 {
			return nil, fmt.Errorf("golars: no Parquet files found under %q", path)
		}
		return paths, nil
	}
	return []string{path}, nil
}

func expandPathsForEngine(ctx context.Context, engine *Engine, path string) ([]string, error) {
	if scheme := storageuri.Scheme(path); scheme != "" {
		store, ok := engine.ObjectStore(scheme)
		if !ok {
			return nil, fmt.Errorf("golars: no object store registered for %q", scheme)
		}
		return store.Glob(ctx, path)
	}
	return expandPaths(ctx, path)
}

type fileSource struct {
	reader            recordSource
	closers           []io.Closer
	filtersFullyMatch bool
}

func (s *fileSource) Schema() *arrow.Schema          { return s.reader.Schema() }
func (s *fileSource) Next() bool                     { return s.reader.Next() }
func (s *fileSource) RecordBatch() arrow.RecordBatch { return s.reader.RecordBatch() }
func (s *fileSource) Err() error                     { return s.reader.Err() }
func (s *fileSource) FiltersFullyMatch() bool {
	if source, ok := s.reader.(filterGuaranteeSource); ok {
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

type multiSource struct {
	sources []recordSource
	index   int
	current recordSource
	err     error
}

func newMultiSource(ctx context.Context, engine *Engine, paths []string, format formatKind, config scanConfig) (recordSource, error) {
	if len(paths) == 0 {
		return nil, fmt.Errorf("golars: scan matched no files")
	}
	sources := make([]recordSource, 0, len(paths))
	partitionKeys := hive.PartitionKeys(paths)
	for _, path := range paths {
		source, err := openSource(ctx, engine, path, format, config)
		if err != nil {
			for _, created := range sources {
				created.Release()
			}
			return nil, err
		}
		if len(partitionKeys) > 0 {
			source = hive.NewPartitionSource(source, hive.PartitionValues(path, partitionKeys), engine.allocator)
		}
		sources = append(sources, source)
	}
	return &multiSource{sources: sources}, nil
}

func openSource(ctx context.Context, engine *Engine, path string, format formatKind, config scanConfig) (recordSource, error) {
	request := scanRequestFromContext(ctx)
	if scheme := storageuri.Scheme(path); scheme != "" {
		store, ok := engine.ObjectStore(scheme)
		if !ok {
			return nil, fmt.Errorf("golars: no object store registered for %q", scheme)
		}
		if format == formatParquet {
			if ranged, rangedOK := store.(RangeObjectStore); rangedOK {
				return openRangedParquetSource(ctx, engine, ranged, path, config, request)
			}
		}
		reader, err := store.Open(ctx, path)
		if err != nil {
			return nil, err
		}
		// ObjectStore.Open deliberately has a stream contract. Formats that need
		// random access (Parquet and Arrow file IPC) are materialized into a
		// bytes.Reader by remote stores; stream formats remain incremental.
		return openReaderSource(ctx, engine, reader, format, config, request)
	}
	return openLocalSource(ctx, engine, path, format, config, request)
}

func (s *multiSource) Schema() *arrow.Schema {
	if s.current != nil {
		return s.current.Schema()
	}
	if s.index < len(s.sources) {
		return s.sources[s.index].Schema()
	}
	return nil
}
func (s *multiSource) Next() bool {
	for {
		if s.current == nil {
			if s.index >= len(s.sources) {
				return false
			}
			s.current = s.sources[s.index]
			s.index++
		}
		if s.current.Next() {
			return true
		}
		if err := s.current.Err(); err != nil {
			s.err = err
			return false
		}
		s.current.Release()
		s.current = nil
	}
}
func (s *multiSource) RecordBatch() arrow.RecordBatch { return s.current.RecordBatch() }
func (s *multiSource) Err() error                     { return s.err }
func (s *multiSource) FiltersFullyMatch() bool {
	if source, ok := s.current.(filterGuaranteeSource); ok {
		return source.FiltersFullyMatch()
	}
	return false
}
func (s *multiSource) Release() {
	if s.current != nil {
		s.current.Release()
		s.current = nil
	}
	for _, source := range s.sources[s.index:] {
		source.Release()
	}
	s.sources = nil
}

func openLocalSource(ctx context.Context, engine *Engine, path string, format formatKind, config scanConfig, request scanRequest) (recordSource, error) {
	if strings.Contains(path, "://") {
		return nil, fmt.Errorf("golars: remote storage %q requires an Engine object store; local source cannot open it", path)
	}
	if format == formatParquet {
		return parquetio.OpenFile(ctx, path, parquetConfig(engine, config), parquetRequest(request))
	}
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	if format == formatIPCFile {
		reader, err := ipc.NewFileReader(file)
		if err != nil {
			_ = file.Close()
			return nil, err
		}
		return &fileSource{reader: ipcio.NewFileSource(reader), closers: []io.Closer{reader, file}}, nil
	}
	return openStreamSource(engine, file, format, config, request)
}

func openReaderSource(ctx context.Context, engine *Engine, file io.ReadCloser, format formatKind, config scanConfig, request scanRequest) (recordSource, error) {
	if format == formatParquet || format == formatIPCFile {
		data, err := io.ReadAll(file)
		if err != nil {
			_ = file.Close()
			return nil, err
		}
		if format == formatParquet {
			return parquetio.OpenReader(ctx, file, parquetConfig(engine, config), parquetRequest(request))
		}
		reader, err := ipc.NewFileReader(bytes.NewReader(data))
		if err != nil {
			_ = file.Close()
			return nil, err
		}
		return &fileSource{reader: ipcio.NewFileSource(reader), closers: []io.Closer{reader, file}}, nil
	}
	return openStreamSource(engine, file, format, config, request)
}

func openStreamSource(engine *Engine, file io.ReadCloser, format formatKind, config scanConfig, request scanRequest) (recordSource, error) {
	closeFile := func(reader recordSource) recordSource {
		return &fileSource{reader: reader, closers: []io.Closer{file}}
	}
	switch format {
	case formatCSV:
		chunk := int(config.batchSize)
		if chunk <= 0 {
			chunk = 1
		}
		options := []csv.Option{
			csv.WithHeader(config.hasHeader),
			csv.WithComma(config.delimiter),
			csv.WithChunk(chunk),
			csv.WithAllocator(engine.allocator),
		}
		if config.hasHeader && len(request.columns) > 0 {
			options = append(options, csv.WithIncludeColumns(request.columns))
		}
		return closeFile(csv.NewInferringReader(file, options...)), nil
	case formatIPCStream:
		reader, err := ipc.NewReader(file, ipc.WithAllocator(engine.allocator))
		if err != nil {
			_ = file.Close()
			return nil, err
		}
		return closeFile(reader), nil
	case formatNDJSON, formatJSON:
		table, err := readJSONTable(file, engine.allocator, format == formatJSON, request.columns)
		_ = file.Close()
		if err != nil {
			return nil, err
		}
		return newTableSource(table, config.batchSize), nil
	default:
		_ = file.Close()
		return nil, fmt.Errorf("golars: unsupported format")
	}
}
