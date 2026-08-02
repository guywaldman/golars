package core

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/csv"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/guywaldman/golars/internal/jsonio"
	"github.com/guywaldman/golars/internal/storageuri"
)

func openSinkDestination(ctx context.Context, engine *Engine, destination string, config writeConfig) (io.WriteCloser, error) {
	if err := ensureWritableDestination(destination, config.overwrite); err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if scheme := storageuri.Scheme(destination); scheme != "" {
		store, ok := engine.ObjectStore(scheme)
		if !ok {
			return nil, fmt.Errorf("golars: no object store registered for %q", scheme)
		}
		return store.Create(ctx, destination)
	}
	return os.Create(destination)
}

func writeCSVBatches(ctx context.Context, lf *LazyFrame, destination string, options ...WriteOption) error {
	if lf == nil {
		return ErrClosed
	}
	config := defaultWriteConfig(lf.engine)
	for _, option := range options {
		option(&config)
	}
	var file io.WriteCloser
	var writer *csv.Writer
	streamErr := lf.ForEachBatch(ctx, func(batch arrow.RecordBatch) error {
		if writer == nil {
			var err error
			file, err = openSinkDestination(ctx, lf.engine, destination, config)
			if err != nil {
				return err
			}
			writer = csv.NewWriter(file, batch.Schema(), csv.WithHeader(config.hasHeader), csv.WithComma(config.delimiter))
		}
		return writer.Write(batch)
	})
	if writer != nil {
		if err := writer.Flush(); streamErr == nil && err != nil {
			streamErr = err
		}
		if err := writer.Error(); streamErr == nil && err != nil {
			streamErr = err
		}
	}
	if file != nil {
		if err := file.Close(); streamErr == nil && err != nil {
			streamErr = err
		}
	}
	return streamErr
}

func writeJSONBatches(ctx context.Context, lf *LazyFrame, destination string, arrayOutput bool, options ...WriteOption) error {
	if lf == nil {
		return ErrClosed
	}
	config := defaultWriteConfig(lf.engine)
	for _, option := range options {
		option(&config)
	}
	var file io.WriteCloser
	var encoder *json.Encoder
	first := true
	streamErr := lf.ForEachBatch(ctx, func(batch arrow.RecordBatch) error {
		if encoder == nil {
			var err error
			file, err = openSinkDestination(ctx, lf.engine, destination, config)
			if err != nil {
				return err
			}
			encoder = json.NewEncoder(file)
			if arrayOutput {
				if _, err := io.WriteString(file, "["); err != nil {
					return err
				}
			}
		}
		for row := 0; row < int(batch.NumRows()); row++ {
			if arrayOutput && !first {
				if _, err := io.WriteString(file, ","); err != nil {
					return err
				}
			}
			first = false
			object := make(map[string]any, batch.NumCols())
			for column := 0; column < int(batch.NumCols()); column++ {
				value, err := jsonio.ScalarValue(batch.Column(column), row)
				if err != nil {
					return err
				}
				object[batch.Schema().Field(column).Name] = value
			}
			if err := encoder.Encode(object); err != nil {
				return err
			}
		}
		return nil
	})
	if encoder != nil && arrayOutput && streamErr == nil {
		_, streamErr = io.WriteString(file, "]\n")
	}
	if file != nil {
		if err := file.Close(); streamErr == nil && err != nil {
			streamErr = err
		}
	}
	return streamErr
}

func writeIPCBatches(ctx context.Context, lf *LazyFrame, destination string, fileFormat bool, options ...WriteOption) error {
	if lf == nil {
		return ErrClosed
	}
	config := defaultWriteConfig(lf.engine)
	for _, option := range options {
		option(&config)
	}
	var file io.WriteCloser
	var writer interface {
		Write(arrow.RecordBatch) error
		Close() error
	}
	streamErr := lf.ForEachBatch(ctx, func(batch arrow.RecordBatch) error {
		if writer == nil {
			var err error
			file, err = openSinkDestination(ctx, lf.engine, destination, config)
			if err != nil {
				return err
			}
			if fileFormat {
				writer, err = ipcFileWriter(file, batch.Schema(), lf.engine)
			} else {
				writer = ipc.NewWriter(file, ipc.WithSchema(batch.Schema()), ipc.WithAllocator(lf.engine.allocator))
			}
			if err != nil {
				_ = file.Close()
				file = nil
				return err
			}
		}
		return writer.Write(batch)
	})
	if writer != nil {
		if err := writer.Close(); streamErr == nil && err != nil {
			streamErr = err
		}
	}
	if file != nil {
		if err := file.Close(); streamErr == nil && err != nil {
			streamErr = err
		}
	}
	return streamErr
}

type ipcFileBatchWriter struct {
	writer interface {
		Write(arrow.RecordBatch) error
		Close() error
	}
}

func (w *ipcFileBatchWriter) Write(batch arrow.RecordBatch) error { return w.writer.Write(batch) }
func (w *ipcFileBatchWriter) Close() error                        { return w.writer.Close() }

func ipcFileWriter(file io.Writer, schema *arrow.Schema, engine *Engine) (*ipcFileBatchWriter, error) {
	writer, err := ipc.NewFileWriter(file, ipc.WithSchema(schema), ipc.WithAllocator(engine.allocator))
	if err != nil {
		return nil, err
	}
	return &ipcFileBatchWriter{writer: writer}, nil
}
