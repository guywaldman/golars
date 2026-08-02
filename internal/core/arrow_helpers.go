package core

import (
	"fmt"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
)

func safeTableFromRecords(schema *arrow.Schema, records []arrow.RecordBatch) (table arrow.Table, err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf("golars: could not assemble record batches: %v", recovered)
		}
	}()
	return array.NewTableFromRecords(schema, records), nil
}

func emptyRecordBatch(schema *arrow.Schema, mem *quotaAllocator) (arrow.RecordBatch, error) {
	columns := make([]arrow.Array, schema.NumFields())
	for i := range columns {
		builder := array.NewBuilder(mem, schema.Field(i).Type)
		columns[i] = builder.NewArray()
		builder.Release()
	}
	return array.NewRecordBatch(schema, columns, 0), nil
}
