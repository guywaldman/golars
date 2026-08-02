package core

import (
	"context"
	"fmt"
	"io"
	"strings"
)

// Scan creates a lazy scan whose format is inferred from path.
func Scan(path string, options ...ScanOption) *LazyFrame {
	engine := getDefaultEngine()
	format, ok := formatForPath(path)
	if !ok {
		return newLazyFrame(engine, nil, fmt.Errorf("%w: cannot infer format from %q", ErrInvalidPlan, path))
	}
	return engine.scanFormat(path, format, options...)
}

// Scan creates a lazy scan on an Engine whose format is inferred from path.
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
	case strings.HasSuffix(lower, ".arrows"):
		return formatIPCStream, true
	default:
		return 0, false
	}
}

// ScanParquet creates a lazy Parquet scan.
func ScanParquet(path string, options ...ScanOption) *LazyFrame {
	return scanFormat(path, formatParquet, options...)
}

// ScanCSV creates a lazy CSV scan.
func ScanCSV(path string, options ...ScanOption) *LazyFrame {
	return scanFormat(path, formatCSV, options...)
}

// ScanNDJSON creates a lazy newline-delimited JSON scan.
func ScanNDJSON(path string, options ...ScanOption) *LazyFrame {
	return scanFormat(path, formatNDJSON, options...)
}

// ScanJSON creates a lazy JSON array scan.
func ScanJSON(path string, options ...ScanOption) *LazyFrame {
	return scanFormat(path, formatJSON, options...)
}

// ScanIPCFile creates a lazy Arrow IPC file scan.
func ScanIPCFile(path string, options ...ScanOption) *LazyFrame {
	return scanFormat(path, formatIPCFile, options...)
}

// ScanIPCStream creates a lazy Arrow IPC stream scan.
func ScanIPCStream(path string, options ...ScanOption) *LazyFrame {
	return scanFormat(path, formatIPCStream, options...)
}

// ScanParquet creates a lazy Parquet scan on an Engine.
func (e *Engine) ScanParquet(path string, options ...ScanOption) *LazyFrame {
	return e.scanFormat(path, formatParquet, options...)
}

// ScanCSV creates a lazy CSV scan on an Engine.
func (e *Engine) ScanCSV(path string, options ...ScanOption) *LazyFrame {
	return e.scanFormat(path, formatCSV, options...)
}

// ScanNDJSON creates a lazy newline-delimited JSON scan on an Engine.
func (e *Engine) ScanNDJSON(path string, options ...ScanOption) *LazyFrame {
	return e.scanFormat(path, formatNDJSON, options...)
}

// ScanJSON creates a lazy JSON array scan on an Engine.
func (e *Engine) ScanJSON(path string, options ...ScanOption) *LazyFrame {
	return e.scanFormat(path, formatJSON, options...)
}

// ScanIPCFile creates a lazy Arrow IPC file scan on an Engine.
func (e *Engine) ScanIPCFile(path string, options ...ScanOption) *LazyFrame {
	return e.scanFormat(path, formatIPCFile, options...)
}

// ScanIPCStream creates a lazy Arrow IPC stream scan on an Engine.
func (e *Engine) ScanIPCStream(path string, options ...ScanOption) *LazyFrame {
	return e.scanFormat(path, formatIPCStream, options...)
}

// ReadParquet reads a Parquet path into an eager DataFrame on an Engine.
func (e *Engine) ReadParquet(ctx context.Context, path string, options ...ScanOption) (*DataFrame, error) {
	return e.ScanParquet(path, options...).Collect(ctx)
}

// ReadCSV reads a CSV path into an eager DataFrame on an Engine.
func (e *Engine) ReadCSV(ctx context.Context, path string, options ...ScanOption) (*DataFrame, error) {
	return e.ScanCSV(path, options...).Collect(ctx)
}

// ReadNDJSON reads newline-delimited JSON into an eager DataFrame on an Engine.
func (e *Engine) ReadNDJSON(ctx context.Context, path string, options ...ScanOption) (*DataFrame, error) {
	return e.ScanNDJSON(path, options...).Collect(ctx)
}

// ReadJSON reads a JSON array into an eager DataFrame on an Engine.
func (e *Engine) ReadJSON(ctx context.Context, path string, options ...ScanOption) (*DataFrame, error) {
	return e.ScanJSON(path, options...).Collect(ctx)
}

// ReadIPCFile reads an Arrow IPC file into an eager DataFrame on an Engine.
func (e *Engine) ReadIPCFile(ctx context.Context, path string, options ...ScanOption) (*DataFrame, error) {
	return e.ScanIPCFile(path, options...).Collect(ctx)
}

// ReadParquet reads a Parquet path into an eager DataFrame.
func ReadParquet(ctx context.Context, path string, options ...ScanOption) (*DataFrame, error) {
	return ScanParquet(path, options...).Collect(ctx)
}

// ReadCSV reads a CSV path into an eager DataFrame.
func ReadCSV(ctx context.Context, path string, options ...ScanOption) (*DataFrame, error) {
	return ScanCSV(path, options...).Collect(ctx)
}

// ReadNDJSON reads newline-delimited JSON into an eager DataFrame.
func ReadNDJSON(ctx context.Context, path string, options ...ScanOption) (*DataFrame, error) {
	return ScanNDJSON(path, options...).Collect(ctx)
}

// ReadJSON reads a JSON array into an eager DataFrame.
func ReadJSON(ctx context.Context, path string, options ...ScanOption) (*DataFrame, error) {
	return ScanJSON(path, options...).Collect(ctx)
}

// ReadIPCFile reads an Arrow IPC file into an eager DataFrame.
func ReadIPCFile(ctx context.Context, path string, options ...ScanOption) (*DataFrame, error) {
	return ScanIPCFile(path, options...).Collect(ctx)
}

// ScanCSVReader creates a lazy CSV scan from a reader.
func ScanCSVReader(reader io.Reader, options ...ScanOption) *LazyFrame {
	return scanReader(reader, formatCSV, options...)
}

// ScanNDJSONReader creates a lazy newline-delimited JSON scan from a reader.
func ScanNDJSONReader(reader io.Reader, options ...ScanOption) *LazyFrame {
	return scanReader(reader, formatNDJSON, options...)
}

// ScanJSONReader creates a lazy JSON array scan from a reader.
func ScanJSONReader(reader io.Reader, options ...ScanOption) *LazyFrame {
	return scanReader(reader, formatJSON, options...)
}

// ScanIPCStreamReader creates a lazy Arrow IPC stream scan from a reader.
func ScanIPCStreamReader(reader io.Reader, options ...ScanOption) *LazyFrame {
	return scanReader(reader, formatIPCStream, options...)
}

// ReadCSVReader reads CSV data from a reader into an eager DataFrame.
func ReadCSVReader(ctx context.Context, reader io.Reader, options ...ScanOption) (*DataFrame, error) {
	return ScanCSVReader(reader, options...).Collect(ctx)
}

// ReadNDJSONReader reads newline-delimited JSON from a reader into a DataFrame.
func ReadNDJSONReader(ctx context.Context, reader io.Reader, options ...ScanOption) (*DataFrame, error) {
	return ScanNDJSONReader(reader, options...).Collect(ctx)
}

// ReadJSONReader reads a JSON array from a reader into a DataFrame.
func ReadJSONReader(ctx context.Context, reader io.Reader, options ...ScanOption) (*DataFrame, error) {
	return ScanJSONReader(reader, options...).Collect(ctx)
}

// ReadIPCStreamReader reads an Arrow IPC stream from a reader into a DataFrame.
func ReadIPCStreamReader(ctx context.Context, reader io.Reader, options ...ScanOption) (*DataFrame, error) {
	return ScanIPCStreamReader(reader, options...).Collect(ctx)
}
