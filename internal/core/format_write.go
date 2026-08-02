package core

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/csv"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"github.com/guywaldman/golars/internal/storageuri"
)

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
	if err := ensureWritableDestination(destination, config.overwrite); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	var file io.WriteCloser
	var writer *pqarrow.FileWriter
	open := func(schema *arrow.Schema) error {
		var err error
		if scheme := storageuri.Scheme(destination); scheme != "" {
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
	if err := ensureWritableDestination(destination, config.overwrite); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	var file io.WriteCloser
	var err error
	if scheme := storageuri.Scheme(destination); scheme != "" {
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

func ensureWritableDestination(destination string, overwrite bool) error {
	if overwrite || storageuri.Scheme(destination) != "" {
		return nil
	}
	if _, err := os.Stat(destination); err == nil {
		return fmt.Errorf("golars: destination exists: %q (use WithOverwrite(true))", destination)
	} else if !os.IsNotExist(err) {
		return err
	}
	return nil
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
