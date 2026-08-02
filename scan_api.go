package golars

import (
	"context"
	"fmt"
	"io"
	"strings"
)

func Scan(path string, options ...ScanOption) *LazyFrame {
	engine := getDefaultEngine()
	format, ok := formatForPath(path)
	if !ok {
		return newLazyFrame(engine, nil, fmt.Errorf("%w: cannot infer format from %q", ErrInvalidPlan, path))
	}
	return engine.scanFormat(path, format, options...)
}

func (e *Engine) Scan(path string, options ...ScanOption) *LazyFrame {
	if e == nil {
		return &LazyFrame{err: ErrClosed}
	}
	format, ok := formatForPath(path)
	if !ok {
		return &LazyFrame{engine: e, err: fmt.Errorf("%w: cannot infer format from %q", ErrInvalidPlan, path)}
	}
	return e.scanFormat(path, format, options...)
}

func formatForPath(path string) (formatKind, bool) {
	lower := strings.ToLower(path)
	switch {
	case strings.HasSuffix(lower, ".parquet"):
		return formatParquet, true
	case strings.HasSuffix(lower, ".csv"):
		return formatCSV, true
	case strings.HasSuffix(lower, ".ndjson"), strings.HasSuffix(lower, ".jsonl"):
		return formatNDJSON, true
	case strings.HasSuffix(lower, ".json"):
		return formatJSON, true
	case strings.HasSuffix(lower, ".arrow"), strings.HasSuffix(lower, ".feather"):
		return formatIPCFile, true
	default:
		return 0, false
	}
}

func ScanParquet(path string, options ...ScanOption) *LazyFrame {
	return scanFormat(path, formatParquet, options...)
}
func ScanCSV(path string, options ...ScanOption) *LazyFrame {
	return scanFormat(path, formatCSV, options...)
}
func ScanNDJSON(path string, options ...ScanOption) *LazyFrame {
	return scanFormat(path, formatNDJSON, options...)
}
func ScanJSON(path string, options ...ScanOption) *LazyFrame {
	return scanFormat(path, formatJSON, options...)
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
