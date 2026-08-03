package core

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
)

// Row is one DataFrame row keyed by column name. Values are Go values suitable
// for JSON encoding; nulls are represented by nil.
type Row = map[string]any

// rowFromBatch converts a borrowed Arrow row into values that remain valid
// after the batch is released.
func rowFromBatch(batch arrow.RecordBatch, row int) Row {
	fields := batch.Schema().Fields()
	result := make(Row, len(fields))
	for column, field := range fields {
		result[field.Name] = detachRowValue(batch.Column(column).GetOneForMarshal(row))
	}
	return result
}

func detachRowValue(value any) any {
	switch value := value.(type) {
	case nil:
		return nil
	case json.RawMessage:
		decoder := json.NewDecoder(bytes.NewReader(value))
		decoder.UseNumber()
		var decoded any
		if err := decoder.Decode(&decoded); err == nil {
			return detachRowValue(decoded)
		}
		return append(json.RawMessage(nil), value...)
	case []byte:
		return append([]byte(nil), value...)
	case []any:
		result := make([]any, len(value))
		for i, item := range value {
			result[i] = detachRowValue(item)
		}
		return result
	case map[string]any:
		result := make(map[string]any, len(value))
		for key, item := range value {
			result[key] = detachRowValue(item)
		}
		return result
	default:
		return value
	}
}

// ForEachRow calls callback once per row without materializing the complete
// result. Rows passed to callback are detached from the borrowed Arrow batch.
func (lf *LazyFrame) ForEachRow(ctx context.Context, callback func(Row) error) error {
	if callback == nil {
		return fmt.Errorf("%w: nil row callback", ErrInvalidPlan)
	}
	if ctx == nil {
		ctx = context.Background()
	}
	return lf.ForEachBatch(ctx, func(batch arrow.RecordBatch) error {
		for row := 0; row < int(batch.NumRows()); row++ {
			if err := ctx.Err(); err != nil {
				return err
			}
			if err := callback(rowFromBatch(batch, row)); err != nil {
				return err
			}
		}
		return nil
	})
}

// ForEachRow calls callback once per row in an eager DataFrame.
func (df *DataFrame) ForEachRow(callback func(Row) error) error {
	if callback == nil {
		return fmt.Errorf("%w: nil row callback", ErrInvalidPlan)
	}
	if err := df.ensureOpen(); err != nil {
		return err
	}
	reader := array.NewTableReader(df.table, df.engine.batchSize)
	defer reader.Release()
	for reader.Next() {
		batch := reader.RecordBatch()
		for row := 0; row < int(batch.NumRows()); row++ {
			if err := callback(rowFromBatch(batch, row)); err != nil {
				return err
			}
		}
	}
	return reader.Err()
}

// Rows materializes an eager DataFrame as detached row maps.
func (df *DataFrame) Rows() ([]Row, error) {
	rows := make([]Row, 0)
	err := df.ForEachRow(func(row Row) error {
		rows = append(rows, row)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return rows, nil
}

// ToDicts is a Polars-compatible alias for Rows. Go callers should prefer Rows.
func (df *DataFrame) ToDicts() ([]Row, error) { return df.Rows() }

// Rows executes a lazy plan and materializes its result as detached row maps.
func (lf *LazyFrame) Rows(ctx context.Context) ([]Row, error) {
	rows := make([]Row, 0)
	err := lf.ForEachRow(ctx, func(row Row) error {
		rows = append(rows, row)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return rows, nil
}

// ToDicts is a Polars-compatible alias for Rows. Go callers should prefer Rows.
func (lf *LazyFrame) ToDicts(ctx context.Context) ([]Row, error) { return lf.Rows(ctx) }

// Height executes a lazy plan as a stream and counts its rows without retaining
// the result. For an eager DataFrame, use Height directly.
func (lf *LazyFrame) Height(ctx context.Context) (int64, error) {
	var rows int64
	err := lf.ForEachBatch(ctx, func(batch arrow.RecordBatch) error {
		rows += batch.NumRows()
		return nil
	})
	if err != nil {
		return 0, err
	}
	return rows, nil
}
