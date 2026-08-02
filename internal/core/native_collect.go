package core

import (
	"context"
	"errors"
	"fmt"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	nativebackend "github.com/guywaldman/golars/internal/native"
)

func collectNative(ctx context.Context, engine *Engine, plan *planNode) (*DataFrame, error) {
	if engine == nil {
		return nil, fmt.Errorf("golars: native execution requires an engine")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	request, err := marshalNativePlan(ctx, engine, plan)
	if err != nil {
		return nil, err
	}
	runtime, err := nativebackend.Open()
	if err != nil {
		return nil, fmt.Errorf("golars: open native Polars backend: %w", err)
	}
	frame, err := runtime.ExecuteFrame(request)
	if err != nil {
		return nil, fmt.Errorf("golars: native Polars execution: %w", err)
	}
	if err := ctx.Err(); err != nil {
		frame.Close()
		return nil, err
	}
	return newDataFrameWithNative(frame, engine), nil
}

func schemaFromNativeIPC(data []byte) (*arrow.Schema, error) {
	reader, err := ipc.NewMappedFileReader(data)
	if err != nil {
		return nil, err
	}
	defer reader.Close()
	return normalizeNativeSchema(reader.Schema()), nil
}

func normalizeNativeSchema(schema *arrow.Schema) *arrow.Schema {
	if schema == nil {
		return nil
	}
	fields := append([]arrow.Field(nil), schema.Fields()...)
	changed := false
	for index, field := range fields {
		switch field.Type.ID() {
		case arrow.LARGE_STRING, arrow.STRING_VIEW:
			field.Type = arrow.BinaryTypes.String
			fields[index] = field
			changed = true
		}
	}
	if !changed {
		return schema
	}
	metadata := schema.Metadata()
	return arrow.NewSchema(fields, &metadata)
}

func tableFromNativeIPC(data []byte, allocator *quotaAllocator) (arrow.Table, error) {
	reader, err := ipc.NewMappedFileReader(data, ipc.WithAllocator(allocator))
	if err != nil {
		return nil, err
	}
	defer reader.Close()
	records := make([]arrow.RecordBatch, 0, reader.NumRecords())
	for index := 0; index < reader.NumRecords(); index++ {
		record, err := reader.RecordBatch(index)
		if err != nil {
			for _, retained := range records {
				retained.Release()
			}
			return nil, err
		}
		normalized, err := normalizeNativeRecord(record, allocator)
		if err != nil {
			for _, retained := range records {
				retained.Release()
			}
			return nil, err
		}
		records = append(records, normalized)
	}
	table, err := safeTableFromRecords(normalizeNativeSchema(reader.Schema()), records)
	for _, record := range records {
		record.Release()
	}
	return table, err
}

func normalizeNativeRecord(record arrow.RecordBatch, allocator *quotaAllocator) (arrow.RecordBatch, error) {
	fields := append([]arrow.Field(nil), record.Schema().Fields()...)
	columns := make([]arrow.Array, len(fields))
	changed := false
	for index, values := range record.Columns() {
		if values.DataType().ID() != arrow.LARGE_STRING && values.DataType().ID() != arrow.STRING_VIEW {
			values.Retain()
			columns[index] = values
			continue
		}
		stringValues, ok := values.(array.StringLike)
		if !ok {
			for _, column := range columns {
				if column != nil {
					column.Release()
				}
			}
			return nil, errors.New("golars: native string result does not implement StringLike")
		}
		builder := array.NewStringBuilder(allocator)
		for row := 0; row < stringValues.Len(); row++ {
			if stringValues.IsValid(row) {
				builder.Append(stringValues.Value(row))
			} else {
				builder.AppendNull()
			}
		}
		columns[index] = builder.NewStringArray()
		builder.Release()
		fields[index].Type = arrow.BinaryTypes.String
		changed = true
	}
	if !changed {
		for _, column := range columns {
			column.Release()
		}
		record.Retain()
		return record, nil
	}
	normalized := array.NewRecordBatch(arrow.NewSchema(fields, nil), columns, record.NumRows())
	for _, column := range columns {
		column.Release()
	}
	return normalized, nil
}
