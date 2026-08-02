package core

import (
	"container/heap"
	"context"
	"fmt"
	"math"
	"sort"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/compute"
	"github.com/guywaldman/golars/internal/arrowutil"
)

func sortDataFrame(ctx context.Context, df *DataFrame, exprs []Expr) (*DataFrame, error) {
	batch, reader, err := wholeTableBatch(df.table, df.engine)
	if err != nil {
		return nil, err
	}
	defer reader.Release()
	defer batch.Release()
	result, _, err := applySort(ctx, df.engine, batch, exprs)
	if err != nil {
		return nil, err
	}
	defer result.Release()
	table := array.NewTableFromRecords(result.Schema(), []arrow.RecordBatch{result})
	return newDataFrameWithTable(table, df.engine), nil
}

type topKFloatEntry struct {
	row   int
	value float64
}

type topKFloatHeap struct {
	entries    []topKFloatEntry
	descending bool
}

func (h topKFloatHeap) Len() int { return len(h.entries) }
func (h topKFloatHeap) Less(i, j int) bool {
	return topKFloatWorse(h.entries[i], h.entries[j], h.descending)
}
func (h topKFloatHeap) Swap(i, j int)   { h.entries[i], h.entries[j] = h.entries[j], h.entries[i] }
func (h *topKFloatHeap) Push(value any) { h.entries = append(h.entries, value.(topKFloatEntry)) }
func (h *topKFloatHeap) Pop() any {
	last := len(h.entries) - 1
	value := h.entries[last]
	h.entries = h.entries[:last]
	return value
}

func sortLimitDataFrame(ctx context.Context, df *DataFrame, exprs []Expr, limit int64) (*DataFrame, bool, error) {
	if len(exprs) != 1 || limit <= 0 {
		return nil, false, nil
	}
	batch, reader, err := wholeTableBatch(df.table, df.engine)
	if err != nil {
		return nil, false, err
	}
	defer reader.Release()
	defer batch.Release()
	if limit >= batch.NumRows() {
		return nil, false, nil
	}
	key, err := evalExpr(ctx, df.engine, exprs[0].node, batch)
	if err != nil {
		return nil, false, err
	}
	defer key.Release()
	values, ok := key.(*array.Float64)
	if !ok || values.NullN() != 0 {
		return nil, false, nil
	}
	descending := exprs[0].node != nil && exprs[0].node.descending
	selected := topKFloatHeap{entries: make([]topKFloatEntry, 0, int(limit)), descending: descending}
	heap.Init(&selected)
	for row := 0; row < values.Len(); row++ {
		value := values.Value(row)
		if math.IsNaN(value) {
			return nil, false, nil
		}
		entry := topKFloatEntry{row: row, value: value}
		if len(selected.entries) < int(limit) {
			heap.Push(&selected, entry)
			continue
		}
		worst := selected.entries[0]
		if topKFloatWorse(worst, entry, descending) {
			selected.entries[0] = entry
			heap.Fix(&selected, 0)
		}
	}
	sort.Slice(selected.entries, func(i, j int) bool {
		return topKFloatBetter(selected.entries[i], selected.entries[j], descending)
	})
	indices := make([]int, len(selected.entries))
	for i, entry := range selected.entries {
		indices[i] = entry.row
	}
	result, _, err := reorderRecord(ctx, df.engine, batch, indices)
	if err != nil {
		return nil, true, err
	}
	defer result.Release()
	table := array.NewTableFromRecords(result.Schema(), []arrow.RecordBatch{result})
	return newDataFrameWithTable(table, df.engine), true, nil
}

func topKFloatWorse(left, right topKFloatEntry, descending bool) bool {
	if left.value == right.value {
		return left.row > right.row
	}
	if descending {
		return left.value < right.value
	}
	return left.value > right.value
}

func topKFloatBetter(left, right topKFloatEntry, descending bool) bool {
	return topKFloatWorse(right, left, descending)
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
