package golars

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/csv"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/arrow/scalar"
	"github.com/apache/arrow-go/v18/parquet"
	parquetfile "github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
)

type ScanOption func(*scanConfig)

type scanConfig struct {
	hasHeader bool
	delimiter rune
	batchSize int64
	jsonArray bool
}

func defaultScanConfig(engine *Engine) scanConfig {
	return scanConfig{hasHeader: true, delimiter: ',', batchSize: engine.batchSize}
}
func WithHasHeader(enabled bool) ScanOption { return func(c *scanConfig) { c.hasHeader = enabled } }
func WithDelimiter(delimiter rune) ScanOption {
	return func(c *scanConfig) {
		if delimiter != 0 {
			c.delimiter = delimiter
		}
	}
}
func WithScanBatchSize(rows int64) ScanOption {
	return func(c *scanConfig) {
		if rows > 0 {
			c.batchSize = rows
		}
	}
}

type WriteOption func(*writeConfig)

type writeConfig struct {
	overwrite bool
	hasHeader bool
	delimiter rune
	batchSize int64
}

func defaultWriteConfig(engine *Engine) writeConfig {
	return writeConfig{hasHeader: true, delimiter: ',', batchSize: engine.batchSize}
}
func WithOverwrite(enabled bool) WriteOption { return func(c *writeConfig) { c.overwrite = enabled } }
func WithWriteHeader(enabled bool) WriteOption {
	return func(c *writeConfig) { c.hasHeader = enabled }
}
func WithWriteDelimiter(delimiter rune) WriteOption {
	return func(c *writeConfig) {
		if delimiter != 0 {
			c.delimiter = delimiter
		}
	}
}
func WithWriteBatchSize(rows int64) WriteOption {
	return func(c *writeConfig) {
		if rows > 0 {
			c.batchSize = rows
		}
	}
}

func Scan(path string, options ...ScanOption) *LazyFrame {
	engine := getDefaultEngine()
	lower := strings.ToLower(path)
	switch {
	case strings.HasSuffix(lower, ".parquet"):
		return ScanParquet(path, options...)
	case strings.HasSuffix(lower, ".csv"):
		return ScanCSV(path, options...)
	case strings.HasSuffix(lower, ".ndjson"), strings.HasSuffix(lower, ".jsonl"):
		return ScanNDJSON(path, options...)
	case strings.HasSuffix(lower, ".json"):
		return ScanJSON(path, options...)
	case strings.HasSuffix(lower, ".arrow"), strings.HasSuffix(lower, ".feather"):
		return ScanIPCFile(path, options...)
	default:
		return newLazyFrame(engine, nil, fmt.Errorf("%w: cannot infer format from %q", ErrInvalidPlan, path))
	}
}

func (e *Engine) Scan(path string, options ...ScanOption) *LazyFrame {
	if e == nil {
		return &LazyFrame{err: ErrClosed}
	}
	lower := strings.ToLower(path)
	switch {
	case strings.HasSuffix(lower, ".parquet"):
		return e.ScanParquet(path, options...)
	case strings.HasSuffix(lower, ".csv"):
		return e.ScanCSV(path, options...)
	case strings.HasSuffix(lower, ".ndjson"), strings.HasSuffix(lower, ".jsonl"):
		return e.ScanNDJSON(path, options...)
	case strings.HasSuffix(lower, ".json"):
		return e.ScanJSON(path, options...)
	case strings.HasSuffix(lower, ".arrow"), strings.HasSuffix(lower, ".feather"):
		return e.ScanIPCFile(path, options...)
	default:
		return &LazyFrame{engine: e, err: fmt.Errorf("%w: cannot infer format from %q", ErrInvalidPlan, path)}
	}
}

func ScanParquet(path string, options ...ScanOption) *LazyFrame {
	return scanFormat(path, formatParquet, options...)
}
func ScanCSV(path string, options ...ScanOption) *LazyFrame {
	return scanFormat(path, formatCSV, options...)
}
func ScanNDJSON(path string, options ...ScanOption) *LazyFrame {
	config := scanConfig{hasHeader: false, delimiter: ',', batchSize: getDefaultEngine().batchSize}
	for _, option := range options {
		option(&config)
	}
	config.jsonArray = false
	return newScanLazyFrame(path, formatNDJSON, config)
}
func ScanJSON(path string, options ...ScanOption) *LazyFrame {
	config := defaultScanConfig(getDefaultEngine())
	for _, option := range options {
		option(&config)
	}
	config.jsonArray = true
	return newScanLazyFrame(path, formatJSON, config)
}
func ScanIPCFile(path string, options ...ScanOption) *LazyFrame {
	return scanFormat(path, formatIPCFile, options...)
}
func ScanIPCStream(path string, options ...ScanOption) *LazyFrame {
	return scanFormat(path, formatIPCStream, options...)
}

func (e *Engine) ScanParquet(path string, options ...ScanOption) *LazyFrame {
	return e.scanFormat(path, formatParquet, options...)
}
func (e *Engine) ScanCSV(path string, options ...ScanOption) *LazyFrame {
	return e.scanFormat(path, formatCSV, options...)
}
func (e *Engine) ScanNDJSON(path string, options ...ScanOption) *LazyFrame {
	return e.scanFormat(path, formatNDJSON, options...)
}
func (e *Engine) ScanJSON(path string, options ...ScanOption) *LazyFrame {
	return e.scanFormat(path, formatJSON, options...)
}
func (e *Engine) ScanIPCFile(path string, options ...ScanOption) *LazyFrame {
	return e.scanFormat(path, formatIPCFile, options...)
}
func (e *Engine) ScanIPCStream(path string, options ...ScanOption) *LazyFrame {
	return e.scanFormat(path, formatIPCStream, options...)
}
func (e *Engine) ReadParquet(ctx context.Context, path string, options ...ScanOption) (*DataFrame, error) {
	return e.ScanParquet(path, options...).Collect(ctx)
}
func (e *Engine) ReadCSV(ctx context.Context, path string, options ...ScanOption) (*DataFrame, error) {
	return e.ScanCSV(path, options...).Collect(ctx)
}
func (e *Engine) ReadNDJSON(ctx context.Context, path string, options ...ScanOption) (*DataFrame, error) {
	return e.ScanNDJSON(path, options...).Collect(ctx)
}
func (e *Engine) ReadJSON(ctx context.Context, path string, options ...ScanOption) (*DataFrame, error) {
	return e.ScanJSON(path, options...).Collect(ctx)
}
func (e *Engine) ReadIPCFile(ctx context.Context, path string, options ...ScanOption) (*DataFrame, error) {
	return e.ScanIPCFile(path, options...).Collect(ctx)
}

func ReadParquet(ctx context.Context, path string, options ...ScanOption) (*DataFrame, error) {
	return ScanParquet(path, options...).Collect(ctx)
}
func ReadCSV(ctx context.Context, path string, options ...ScanOption) (*DataFrame, error) {
	return ScanCSV(path, options...).Collect(ctx)
}
func ReadNDJSON(ctx context.Context, path string, options ...ScanOption) (*DataFrame, error) {
	return ScanNDJSON(path, options...).Collect(ctx)
}
func ReadJSON(ctx context.Context, path string, options ...ScanOption) (*DataFrame, error) {
	return ScanJSON(path, options...).Collect(ctx)
}
func ReadIPCFile(ctx context.Context, path string, options ...ScanOption) (*DataFrame, error) {
	return ScanIPCFile(path, options...).Collect(ctx)
}

func ScanCSVReader(reader io.Reader, options ...ScanOption) *LazyFrame {
	return scanReader(reader, formatCSV, options...)
}
func ScanNDJSONReader(reader io.Reader, options ...ScanOption) *LazyFrame {
	return scanReader(reader, formatNDJSON, options...)
}
func ScanJSONReader(reader io.Reader, options ...ScanOption) *LazyFrame {
	return scanReader(reader, formatJSON, options...)
}
func ScanIPCStreamReader(reader io.Reader, options ...ScanOption) *LazyFrame {
	return scanReader(reader, formatIPCStream, options...)
}
func ReadCSVReader(ctx context.Context, reader io.Reader, options ...ScanOption) (*DataFrame, error) {
	return ScanCSVReader(reader, options...).Collect(ctx)
}
func ReadNDJSONReader(ctx context.Context, reader io.Reader, options ...ScanOption) (*DataFrame, error) {
	return ScanNDJSONReader(reader, options...).Collect(ctx)
}
func ReadJSONReader(ctx context.Context, reader io.Reader, options ...ScanOption) (*DataFrame, error) {
	return ScanJSONReader(reader, options...).Collect(ctx)
}
func ReadIPCStreamReader(ctx context.Context, reader io.Reader, options ...ScanOption) (*DataFrame, error) {
	return ScanIPCStreamReader(reader, options...).Collect(ctx)
}

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

func newScanLazyFrame(path string, format formatKind, config scanConfig) *LazyFrame {
	return newScanLazyFrameWithEngine(getDefaultEngine(), path, format, config)
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
	if scheme := uriScheme(path); scheme != "" {
		store, ok := engine.ObjectStore(scheme)
		if !ok {
			return nil, fmt.Errorf("golars: no object store registered for %q", scheme)
		}
		return store.Glob(ctx, path)
	}
	return expandPaths(ctx, path)
}

type fileSource struct {
	reader  recordSource
	closers []io.Closer
}

func (s *fileSource) Schema() *arrow.Schema          { return s.reader.Schema() }
func (s *fileSource) Next() bool                     { return s.reader.Next() }
func (s *fileSource) RecordBatch() arrow.RecordBatch { return s.reader.RecordBatch() }
func (s *fileSource) Err() error                     { return s.reader.Err() }
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
	partitionKeys := hivePartitionKeys(paths)
	for _, path := range paths {
		source, err := openSource(ctx, engine, path, format, config)
		if err != nil {
			for _, created := range sources {
				created.Release()
			}
			return nil, err
		}
		if len(partitionKeys) > 0 {
			source = newPartitionSource(source, hivePartitionValues(path, partitionKeys), engine.allocator)
		}
		sources = append(sources, source)
	}
	return &multiSource{sources: sources}, nil
}

func uriScheme(path string) string {
	if index := strings.Index(path, "://"); index > 0 {
		return strings.ToLower(path[:index])
	}
	return ""
}

func openSource(ctx context.Context, engine *Engine, path string, format formatKind, config scanConfig) (recordSource, error) {
	request := scanRequestFromContext(ctx)
	if scheme := uriScheme(path); scheme != "" {
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
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	closeFile := func(reader recordSource) recordSource { return &fileSource{reader: reader, closers: []io.Closer{file}} }
	switch format {
	case formatParquet:
		parquetReader, err := parquetfile.NewParquetReader(file)
		if err != nil {
			_ = file.Close()
			return nil, err
		}
		props := pqarrow.ArrowReadProperties{BatchSize: config.batchSize, Parallel: engine.parallelism > 1}
		arrowReader, err := pqarrow.NewFileReader(parquetReader, props, engine.allocator)
		if err != nil {
			_ = parquetReader.Close()
			return nil, err
		}
		return newParquetRecordSource(ctx, engine, parquetReader, arrowReader, request)
	case formatCSV:
		chunk := int(config.batchSize)
		if chunk <= 0 {
			chunk = 1
		}
		options := []csv.Option{csv.WithHeader(config.hasHeader), csv.WithComma(config.delimiter), csv.WithChunk(chunk), csv.WithAllocator(engine.allocator)}
		if config.hasHeader && len(request.columns) > 0 {
			options = append(options, csv.WithIncludeColumns(request.columns))
		}
		reader := csv.NewInferringReader(file, options...)
		return closeFile(reader), nil
	case formatIPCFile:
		reader, err := ipc.NewFileReader(file)
		if err != nil {
			_ = file.Close()
			return nil, err
		}
		return &fileSource{reader: newIPCFileSource(reader), closers: []io.Closer{reader, file}}, nil
	case formatIPCStream:
		reader, err := ipc.NewReader(file, ipc.WithAllocator(engine.allocator))
		if err != nil {
			_ = file.Close()
			return nil, err
		}
		return &fileSource{reader: reader, closers: []io.Closer{file}}, nil
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

func openReaderSource(ctx context.Context, engine *Engine, file io.ReadCloser, format formatKind, config scanConfig, request scanRequest) (recordSource, error) {
	closeFile := func(reader recordSource) recordSource { return &fileSource{reader: reader, closers: []io.Closer{file}} }
	switch format {
	case formatParquet, formatIPCFile:
		data, err := io.ReadAll(file)
		if err != nil {
			_ = file.Close()
			return nil, err
		}
		if format == formatParquet {
			parquetReader, err := parquetfile.NewParquetReader(bytes.NewReader(data))
			if err != nil {
				_ = file.Close()
				return nil, err
			}
			props := pqarrow.ArrowReadProperties{BatchSize: config.batchSize, Parallel: engine.parallelism > 1}
			arrowReader, err := pqarrow.NewFileReader(parquetReader, props, engine.allocator)
			if err != nil {
				_ = parquetReader.Close()
				_ = file.Close()
				return nil, err
			}
			return newParquetRecordSource(ctx, engine, parquetReader, arrowReader, request, file)
		}
		reader, err := ipc.NewFileReader(bytes.NewReader(data))
		if err != nil {
			_ = file.Close()
			return nil, err
		}
		return closeFile(newIPCFileSource(reader)), nil
	case formatCSV:
		chunk := int(config.batchSize)
		if chunk <= 0 {
			chunk = 1
		}
		options := []csv.Option{csv.WithHeader(config.hasHeader), csv.WithComma(config.delimiter), csv.WithChunk(chunk), csv.WithAllocator(engine.allocator)}
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

func parquetColumnIndices(reader *pqarrow.FileReader, request scanRequest) ([]int, error) {
	if len(request.columns) == 0 {
		return nil, nil
	}
	schema, err := reader.Schema()
	if err != nil {
		return nil, err
	}
	indices := make([]int, 0, len(request.columns))
	seen := make(map[int]struct{}, len(request.columns))
	for _, name := range request.columns {
		fields := schema.FieldIndices(name)
		if len(fields) == 0 {
			return nil, &ColumnNotFoundError{Name: name, Available: schemaNames(schema)}
		}
		if _, exists := seen[fields[0]]; !exists {
			indices = append(indices, fields[0])
			seen[fields[0]] = struct{}{}
		}
	}
	sort.Ints(indices)
	return indices, nil
}

func WriteParquet(ctx context.Context, df *DataFrame, destination string, options ...WriteOption) error {
	return writeWithFile(ctx, df, destination, options, func(file io.Writer, table arrow.Table, config writeConfig) error {
		props := parquet.NewWriterProperties()
		return pqarrow.WriteTable(table, file, config.batchSize, props, pqarrow.NewArrowWriterProperties())
	})
}

func writeParquetBatches(ctx context.Context, lf *LazyFrame, destination string, options ...WriteOption) error {
	if lf == nil {
		return ErrClosed
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := lf.engine.checkOpen(); err != nil {
		return err
	}
	config := defaultWriteConfig(lf.engine)
	for _, option := range options {
		option(&config)
	}
	if !config.overwrite && uriScheme(destination) == "" {
		if _, err := os.Stat(destination); err == nil {
			return fmt.Errorf("golars: destination exists: %q (use WithOverwrite(true))", destination)
		} else if !os.IsNotExist(err) {
			return err
		}
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	var file io.WriteCloser
	var writer *pqarrow.FileWriter
	open := func(schema *arrow.Schema) error {
		var err error
		if scheme := uriScheme(destination); scheme != "" {
			store, ok := lf.engine.ObjectStore(scheme)
			if !ok {
				return fmt.Errorf("golars: no object store registered for %q", scheme)
			}
			file, err = store.Create(ctx, destination)
		} else {
			file, err = os.Create(destination)
		}
		if err != nil {
			return err
		}
		writer, err = pqarrow.NewFileWriter(schema, file, parquet.NewWriterProperties(), pqarrow.NewArrowWriterProperties())
		if err != nil {
			_ = file.Close()
			file = nil
			return err
		}
		return nil
	}
	closeOutput := func() error {
		if writer != nil {
			return writer.Close()
		}
		if file != nil {
			return file.Close()
		}
		return nil
	}
	streamErr := lf.ForEachBatch(ctx, func(batch arrow.RecordBatch) error {
		if writer == nil {
			if err := open(batch.Schema()); err != nil {
				return err
			}
		}
		return writer.Write(batch)
	})
	closeErr := closeOutput()
	if streamErr != nil {
		return streamErr
	}
	return closeErr
}

func WriteCSV(ctx context.Context, df *DataFrame, destination string, options ...WriteOption) error {
	return writeWithFile(ctx, df, destination, options, func(file io.Writer, table arrow.Table, config writeConfig) error {
		writer := csv.NewWriter(file, table.Schema(), csv.WithHeader(config.hasHeader), csv.WithComma(config.delimiter))
		return writeBatches(ctx, table, config.batchSize, func(batch arrow.RecordBatch) error { return writer.Write(batch) }, writer.Flush)
	})
}
func WriteNDJSON(ctx context.Context, df *DataFrame, destination string, options ...WriteOption) error {
	return writeJSON(ctx, df, destination, false, options...)
}
func WriteJSON(ctx context.Context, df *DataFrame, destination string, options ...WriteOption) error {
	return writeJSON(ctx, df, destination, true, options...)
}
func WriteIPCFile(ctx context.Context, df *DataFrame, destination string, options ...WriteOption) error {
	return writeWithFile(ctx, df, destination, options, func(file io.Writer, table arrow.Table, config writeConfig) error {
		writer, err := ipc.NewFileWriter(file, ipc.WithSchema(table.Schema()), ipc.WithAllocator(df.engine.allocator))
		if err != nil {
			return err
		}
		if err := writeBatches(ctx, table, config.batchSize, writer.Write, nil); err != nil {
			_ = writer.Close()
			return err
		}
		return writer.Close()
	})
}
func WriteIPCStream(ctx context.Context, df *DataFrame, destination string, options ...WriteOption) error {
	return writeWithFile(ctx, df, destination, options, func(file io.Writer, table arrow.Table, config writeConfig) error {
		writer := ipc.NewWriter(file, ipc.WithSchema(table.Schema()), ipc.WithAllocator(df.engine.allocator))
		if err := writeBatches(ctx, table, config.batchSize, writer.Write, nil); err != nil {
			_ = writer.Close()
			return err
		}
		return writer.Close()
	})
}

func writeWithFile(ctx context.Context, df *DataFrame, destination string, options []WriteOption, write func(io.Writer, arrow.Table, writeConfig) error) error {
	if err := df.ensureOpen(); err != nil {
		return err
	}
	config := defaultWriteConfig(df.engine)
	for _, option := range options {
		option(&config)
	}
	if !config.overwrite && uriScheme(destination) == "" {
		if _, err := os.Stat(destination); err == nil {
			return fmt.Errorf("golars: destination exists: %q (use WithOverwrite(true))", destination)
		} else if !os.IsNotExist(err) {
			return err
		}
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	var file io.WriteCloser
	var err error
	if scheme := uriScheme(destination); scheme != "" {
		store, ok := df.engine.ObjectStore(scheme)
		if !ok {
			return fmt.Errorf("golars: no object store registered for %q", scheme)
		}
		file, err = store.Create(ctx, destination)
	} else {
		file, err = os.Create(destination)
	}
	if err != nil {
		return err
	}
	table, err := df.ArrowTable()
	if err != nil {
		_ = file.Close()
		return err
	}
	defer table.Release()
	writeErr := write(file, table, config)
	closeErr := file.Close()
	if writeErr != nil {
		return writeErr
	}
	if errors.Is(closeErr, os.ErrClosed) {
		return nil
	}
	return closeErr
}

func writeBatches(ctx context.Context, table arrow.Table, batchSize int64, write func(arrow.RecordBatch) error, flush func() error) error {
	reader := array.NewTableReader(table, batchSize)
	defer reader.Release()
	for reader.Next() {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := write(reader.RecordBatch()); err != nil {
			return err
		}
	}
	if err := reader.Err(); err != nil {
		return err
	}
	if flush != nil {
		return flush()
	}
	return nil
}

func writeJSON(ctx context.Context, df *DataFrame, destination string, arrayOutput bool, options ...WriteOption) error {
	return writeWithFile(ctx, df, destination, options, func(file io.Writer, table arrow.Table, config writeConfig) error {
		encoder := json.NewEncoder(file)
		if arrayOutput {
			if _, err := io.WriteString(file, "["); err != nil {
				return err
			}
		}
		first := true
		err := writeBatches(ctx, table, config.batchSize, func(batch arrow.RecordBatch) error {
			for row := 0; row < int(batch.NumRows()); row++ {
				if arrayOutput && !first {
					if _, err := io.WriteString(file, ","); err != nil {
						return err
					}
				}
				first = false
				object := make(map[string]any, batch.NumCols())
				for col := 0; col < int(batch.NumCols()); col++ {
					value, err := jsonScalarValue(batch.Column(col), row)
					if err != nil {
						return err
					}
					object[batch.Schema().Field(col).Name] = value
				}
				if err := encoder.Encode(object); err != nil {
					return err
				}
				if arrayOutput {
					// Encoder adds a newline; the next comma remains valid JSON whitespace.
				}
			}
			return nil
		}, nil)
		if err != nil {
			return err
		}
		if arrayOutput {
			_, err = io.WriteString(file, "]\n")
			return err
		}
		return nil
	})
}

func jsonScalarValue(arr arrow.Array, row int) (any, error) {
	value, err := scalar.GetScalar(arr, row)
	if err != nil {
		return nil, err
	}
	if !value.IsValid() {
		return nil, nil
	}
	switch value := value.(type) {
	case *scalar.Boolean:
		return value.Value, nil
	}
	// Arrow scalar concrete values expose their fields; JSON fallback keeps
	// unsupported extension values deterministic instead of silently dropping them.
	if number, ok := scalarNumber(value); ok && (arrow.IsInteger(value.DataType().ID()) || arrow.IsFloating(value.DataType().ID())) {
		return number, nil
	}
	return value.String(), nil
}

func readJSONTable(reader io.Reader, mem memory.Allocator, arrayInput bool, columns []string) (arrow.Table, error) {
	decoder := json.NewDecoder(reader)
	decoder.UseNumber()
	var values []map[string]any
	if arrayInput {
		token, err := decoder.Token()
		if err != nil {
			return nil, err
		}
		if delimiter, ok := token.(json.Delim); !ok || delimiter != '[' {
			return nil, fmt.Errorf("golars: JSON input must be an array of objects")
		}
		for decoder.More() {
			var object map[string]any
			if err := decoder.Decode(&object); err != nil {
				return nil, err
			}
			values = append(values, object)
		}
		if _, err := decoder.Token(); err != nil {
			return nil, err
		}
	} else {
		for {
			var object map[string]any
			err := decoder.Decode(&object)
			if err == io.EOF {
				break
			}
			if err != nil {
				return nil, err
			}
			values = append(values, object)
		}
	}
	return tableFromJSONObjects(values, mem, columns)
}

func tableFromJSONObjects(values []map[string]any, mem memory.Allocator, projectedColumns []string) (arrow.Table, error) {
	namesSet := map[string]struct{}{}
	for _, object := range values {
		for name := range object {
			namesSet[name] = struct{}{}
		}
	}
	names := make([]string, 0, len(namesSet))
	for name := range namesSet {
		names = append(names, name)
	}
	sort.Strings(names)
	if len(projectedColumns) > 0 {
		projected := make(map[string]struct{}, len(projectedColumns))
		for _, name := range projectedColumns {
			if _, exists := namesSet[name]; !exists {
				return nil, &ColumnNotFoundError{Name: name, Available: names}
			}
			projected[name] = struct{}{}
		}
		filtered := names[:0]
		for _, name := range names {
			if _, keep := projected[name]; keep {
				filtered = append(filtered, name)
			}
		}
		names = filtered
	}
	fields := make([]arrow.Field, len(names))
	builders := make([]array.Builder, len(names))
	for i, name := range names {
		dt := inferJSONType(values, name)
		fields[i] = arrow.Field{Name: name, Type: dt, Nullable: true}
		builders[i] = array.NewBuilder(mem, dt)
	}
	defer func() {
		for _, builder := range builders {
			builder.Release()
		}
	}()
	for _, object := range values {
		for i, name := range names {
			value, exists := object[name]
			if !exists || value == nil {
				builders[i].AppendNull()
				continue
			}
			sc, err := jsonToScalar(value, fields[i].Type)
			if err != nil {
				return nil, err
			}
			if err := scalar.Append(builders[i], sc); err != nil {
				return nil, err
			}
		}
	}
	columns := make([]arrow.Array, len(builders))
	for i, builder := range builders {
		columns[i] = builder.NewArray()
	}
	defer releaseArrays(columns)
	return array.NewTableFromSlice(arrow.NewSchema(fields, nil), arraysAsSlices(columns)), nil
}

func inferJSONType(values []map[string]any, name string) arrow.DataType {
	for _, object := range values {
		value := object[name]
		switch value := value.(type) {
		case bool:
			return arrow.FixedWidthTypes.Boolean
		case json.Number:
			if strings.ContainsAny(value.String(), ".eE") {
				return arrow.PrimitiveTypes.Float64
			}
			return arrow.PrimitiveTypes.Int64
		case float64:
			return arrow.PrimitiveTypes.Float64
		case string:
			return arrow.BinaryTypes.String
		case []any, map[string]any:
			return arrow.BinaryTypes.String
		}
	}
	return arrow.BinaryTypes.String
}

func jsonToScalar(value any, dt arrow.DataType) (scalar.Scalar, error) {
	switch value := value.(type) {
	case json.Number:
		return scalar.MakeScalarParam(value.String(), dt)
	case []any, map[string]any:
		encoded, err := json.Marshal(value)
		if err != nil {
			return nil, err
		}
		return scalar.MakeScalarParam(string(encoded), dt)
	default:
		return scalar.MakeScalarParam(value, dt)
	}
}

type ipcFileSource struct {
	reader  *ipc.FileReader
	index   int
	current arrow.RecordBatch
	err     error
}

func newIPCFileSource(reader *ipc.FileReader) recordSource { return &ipcFileSource{reader: reader} }
func (s *ipcFileSource) Schema() *arrow.Schema             { return s.reader.Schema() }
func (s *ipcFileSource) Next() bool {
	// FileReader owns the current batch and releases it when RecordBatch is
	// called for the next index. Do not release it here: callers may have
	// retained the previous batch for a collected table.
	s.current = nil
	if s.index >= s.reader.NumRecords() {
		return false
	}
	var err error
	s.current, err = s.reader.RecordBatch(s.index)
	if err != nil {
		s.err = err
		return false
	}
	s.index++
	return true
}
func (s *ipcFileSource) RecordBatch() arrow.RecordBatch { return s.current }
func (s *ipcFileSource) Err() error                     { return s.err }
func (s *ipcFileSource) Release() {
	if s.reader != nil {
		_ = s.reader.Close()
		s.reader = nil
	}
}
