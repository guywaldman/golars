package golars

import (
	"context"
	"fmt"
	"math"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/compute"
	"github.com/guywaldman/golars/internal/arrowutil"
)

func sortDataFrame(ctx context.Context, df *DataFrame, exprs []Expr) (*DataFrame, error) {
	reader := array.NewTableReader(df.table, df.table.NumRows())
	defer reader.Release()
	if !reader.Next() {
		if err := reader.Err(); err != nil {
			return nil, err
		}
		return DataFrameFromArrow(df.table)
	}
	batch := reader.RecordBatch()
	result, _, err := applySort(ctx, df.engine, batch, exprs)
	if err != nil {
		return nil, err
	}
	defer result.Release()
	table := array.NewTableFromRecords(result.Schema(), []arrow.RecordBatch{result})
	return &DataFrame{table: table, engine: df.engine}, nil
}

func applySort(ctx context.Context, engine *Engine, batch arrow.RecordBatch, exprs []Expr) (arrow.RecordBatch, int64, error) {
	// Sort evaluates expression keys, delegates ordering to Arrow's stable
	// sort_indices kernel, and applies the resulting permutation to the batch.
	if len(exprs) == 0 || batch.NumRows() < 2 {
		batch.Retain()
		return batch, math.MaxInt64, nil
	}
	keys := make([]arrow.Array, len(exprs))
	fields := make([]arrow.Field, len(exprs))
	for i, expr := range exprs {
		arr, err := evalExpr(ctx, engine, expr.node, batch)
		if err != nil {
			arrowutil.ReleaseArrays(keys)
			return nil, 0, err
		}
		keys[i] = arr
		fields[i] = arrow.Field{Name: fmt.Sprintf("__golars_sort_%d", i), Type: arr.DataType(), Nullable: true}
	}
	sortBatch := array.NewRecordBatch(arrow.NewSchema(fields, nil), keys, batch.NumRows())
	keysForSort := make([]compute.SortKey, len(exprs))
	for i, expr := range exprs {
		order := compute.SortOrderAscending
		nulls := compute.SortNullsAtStart
		if expr.node != nil && expr.node.descending {
			order = compute.SortOrderDescending
			nulls = compute.SortNullsAtEnd
		}
		keysForSort[i] = compute.SortKey{ColumnIndex: i, Order: order, NullPlacement: nulls}
	}
	indices, err := compute.SortIndicesRecordBatch(ctx, sortBatch, keysForSort)
	sortBatch.Release()
	arrowutil.ReleaseArrays(keys)
	if err != nil {
		return nil, 0, err
	}
	defer indices.Release()
	return reorderRecordWithArray(ctx, engine, batch, indices)
}

func reorderRecord(ctx context.Context, engine *Engine, batch arrow.RecordBatch, indices []int) (arrow.RecordBatch, int64, error) {
	indexValues := make([]int64, len(indices))
	for i, index := range indices {
		indexValues[i] = int64(index)
	}
	indexSeries, err := NewSeries("__golars_sort_indices", indexValues)
	if err != nil {
		return nil, 0, err
	}
	defer indexSeries.Close()
	index := indexSeries.data.Chunk(0)
	defer index.Release()
	return reorderRecordWithArray(ctx, engine, batch, index)
}

func reorderRecordWithArray(ctx context.Context, engine *Engine, batch arrow.RecordBatch, index arrow.Array) (arrow.RecordBatch, int64, error) {
	cols := make([]arrow.Array, batch.NumCols())
	var err error
	for i := range cols {
		cols[i], err = compute.TakeArray(compute.WithAllocator(ctx, engine.allocator), batch.Column(i), index)
		if err != nil {
			arrowutil.ReleaseArrays(cols)
			return nil, 0, err
		}
	}
	rec := array.NewRecordBatch(batch.Schema(), cols, int64(index.Len()))
	arrowutil.ReleaseArrays(cols)
	return rec, math.MaxInt64, nil
}
