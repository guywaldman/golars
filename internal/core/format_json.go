package core

import (
	"context"
	"encoding/json"
	"io"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/guywaldman/golars/internal/jsonio"
)

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
					value, err := jsonio.ScalarValue(batch.Column(col), row)
					if err != nil {
						return err
					}
					object[batch.Schema().Field(col).Name] = value
				}
				if err := encoder.Encode(object); err != nil {
					return err
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

func readJSONTable(reader io.Reader, mem memory.Allocator, arrayInput bool, columns []string) (arrow.Table, error) {
	return jsonio.ReadTable(reader, mem, arrayInput, columns, func(name string, available []string) error {
		return &ColumnNotFoundError{Name: name, Available: available}
	})
}
