package core

import (
	"context"
	"fmt"
	"math"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/compute"
	"github.com/guywaldman/golars/internal/arrowutil"
	"github.com/guywaldman/golars/internal/prefetch"
)

func (lf *LazyFrame) execute(ctx context.Context) (arrow.Table, error) {
	return lf.collectTable(ctx, nil)
}

func (lf *LazyFrame) collectWithScanRequest(ctx context.Context, request *scanRequest) (*DataFrame, error) {
	table, err := lf.collectTable(ctx, request)
	if err != nil {
		return nil, err
	}
	return newDataFrameWithTable(table, lf.engine), nil
}

func (lf *LazyFrame) collectTable(ctx context.Context, request *scanRequest) (arrow.Table, error) {
	var records []arrow.RecordBatch
	var schema *arrow.Schema
	err := lf.executeBatchesWithScanRequest(ctx, request, func(batch arrow.RecordBatch) error {
		if schema == nil {
			schema = batch.Schema()
		}
		batch.Retain()
		records = append(records, batch)
		return nil
	})
	if err != nil {
		for _, record := range records {
			record.Release()
		}
		return nil, err
	}
	if schema == nil {
		for _, record := range records {
			record.Release()
		}
		return nil, fmt.Errorf("golars: source produced no schema")
	}
	table, tableErr := safeTableFromRecords(schema, records)
	for _, record := range records {
		record.Release()
	}
	return table, tableErr
}

func (lf *LazyFrame) executeBatches(ctx context.Context, callback func(arrow.RecordBatch) error) error {
	return lf.executeBatchesWithScanRequest(ctx, nil, callback)
}

func (lf *LazyFrame) executeBatchesWithScanRequest(ctx context.Context, request *scanRequest, callback func(arrow.RecordBatch) error) error {
	nodes := make([]*planNode, 0, 8)
	plan := optimizePlan(lf.plan)
	for node := plan; node != nil; node = node.input {
		nodes = append(nodes, node)
	}
	if len(nodes) == 0 {
		return ErrInvalidPlan
	}
	reversePlans(nodes)
	root := nodes[0]
	var source recordSource
	var err error
	if request == nil {
		derived := deriveScanRequest(nodes)
		request = &derived
	}
	ctx = withScanRequest(ctx, *request)
	switch root.kind {
	case planTable:
		source = newTableSource(root.table, lf.engine.batchSize)
	case planSource:
		source, err = root.source(ctx, lf.engine)
	default:
		return fmt.Errorf("%w: plan has no source", ErrInvalidPlan)
	}
	if err != nil {
		return err
	}
	defer source.Release()

	sourceSchema := source.Schema()
	emitted := false
	remaining := int64(math.MaxInt64)
	var prefetched *prefetch.Prefetch
	if lf.engine.parallelism > 1 {
		prefetched = prefetch.New(ctx, source)
		defer prefetched.Close()
	}
	nextBatch := func() (arrow.RecordBatch, bool, bool) {
		if prefetched != nil {
			item, ok := prefetched.Next()
			if !ok {
				return nil, false, false
			}
			return item.RecordBatch, true, item.FiltersFullyMatch
		}
		if !source.Next() {
			return nil, false, false
		}
		batch := source.RecordBatch()
		if batch == nil {
			return nil, true, false
		}
		batch.Retain()
		filtersFullyMatch := false
		if guaranteed, ok := source.(filterGuaranteeSource); ok {
			filtersFullyMatch = guaranteed.FiltersFullyMatch()
		}
		return batch, true, filtersFullyMatch
	}
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		batch, ok, filtersFullyMatch := nextBatch()
		if !ok {
			break
		}
		if batch == nil {
			continue
		}
		for i := 1; i < len(nodes); i++ {
			node := nodes[i]
			if filtersFullyMatch && node.kind == planFilter {
				continue
			}
			if node.kind == planFilter && i+1 < len(nodes) && nodes[i+1].kind == planSelect && canFuseFilterSelect(nodes[i+1].exprs) {
				next, fuseErr := applyFilterSelect(ctx, lf.engine, batch, node.exprs[0].node, nodes[i+1].exprs)
				batch.Release()
				if fuseErr != nil {
					return fuseErr
				}
				batch = next
				i++
				continue
			}
			var next arrow.RecordBatch
			next, remaining, err = applyPlanNode(ctx, lf.engine, node, batch, remaining)
			batch.Release()
			if err != nil {
				return err
			}
			if next == nil {
				batch = nil
				break
			}
			batch = next
		}
		if batch != nil {
			emitted = true
			callbackErr := callback(batch)
			batch.Release()
			if callbackErr != nil {
				return callbackErr
			}
		}
		if remaining == 0 {
			break
		}
	}
	if err := source.Err(); err != nil {
		return err
	}
	if emitted {
		return nil
	}
	if sourceSchema == nil {
		return fmt.Errorf("golars: source produced no schema")
	}
	// Run transformations over a zero-row batch so Select/WithColumns still
	// produce their projected schema for empty results.
	empty, emptyErr := emptyRecordBatch(sourceSchema, lf.engine.allocator)
	if emptyErr != nil {
		return emptyErr
	}
	for _, node := range nodes[1:] {
		next, _, nodeErr := applyPlanNode(ctx, lf.engine, node, empty, math.MaxInt64)
		currentSchema := empty.Schema()
		empty.Release()
		if nodeErr != nil {
			return nodeErr
		}
		if next == nil {
			var nextErr error
			empty, nextErr = emptyRecordBatch(currentSchema, lf.engine.allocator)
			if nextErr != nil {
				return nextErr
			}
			continue
		}
		empty = next
	}
	if empty == nil {
		return nil
	}
	callbackErr := callback(empty)
	empty.Release()
	return callbackErr
}

func reversePlans(nodes []*planNode) {
	for i, j := 0, len(nodes)-1; i < j; i, j = i+1, j-1 {
		nodes[i], nodes[j] = nodes[j], nodes[i]
	}
}

func applyPlanNode(ctx context.Context, engine *Engine, node *planNode, batch arrow.RecordBatch, remaining int64) (arrow.RecordBatch, int64, error) {
	switch node.kind {
	case planLimit:
		if node.limit == 0 || remaining == 0 {
			return nil, 0, nil
		}
		if remaining == math.MaxInt64 {
			remaining = node.limit
		}
		if int64(batch.NumRows()) > remaining {
			out := batch.NewSlice(0, remaining)
			return out, 0, nil
		}
		batch.Retain()
		return batch, remaining - batch.NumRows(), nil
	case planFilter:
		filter, err := evalFilter(ctx, engine, node.exprs[0].node, batch)
		if err != nil {
			return nil, remaining, err
		}
		defer filter.Release()
		if filter.DataType().ID() != arrow.BOOL {
			return nil, remaining, fmt.Errorf("%w: filter must be boolean, got %s", ErrTypeMismatch, filter.DataType())
		}
		out, err := compute.FilterRecordBatch(compute.WithAllocator(ctx, engine.allocator), batch, filter, compute.DefaultFilterOptions())
		if err != nil {
			return nil, remaining, err
		}
		return out, remaining, nil
	case planSelect:
		out, _, err := applySelect(ctx, engine, batch, node.exprs)
		return out, remaining, err
	case planWithColumns:
		out, _, err := applyWithColumns(ctx, engine, batch, node.exprs)
		return out, remaining, err
	case planSort:
		out, _, err := applySort(ctx, engine, batch, node.exprs)
		return out, remaining, err
	case planGroupBy:
		return nil, remaining, fmt.Errorf("golars: grouped plans require a blocking execution")
	default:
		batch.Retain()
		return batch, remaining, nil
	}
}

func canFuseFilterSelect(exprs []Expr) bool {
	for _, expr := range exprs {
		if expr.node != nil && expr.node.kind == exprCall && expr.node.name == "all_columns" {
			return false
		}
	}
	return true
}

func applyFilterSelect(ctx context.Context, engine *Engine, batch arrow.RecordBatch, filterNode *exprNode, exprs []Expr) (arrow.RecordBatch, error) {
	filter, err := evalFilter(ctx, engine, filterNode, batch)
	if err != nil {
		return nil, err
	}
	defer filter.Release()
	if filter.DataType().ID() != arrow.BOOL {
		return nil, fmt.Errorf("%w: filter must be boolean, got %s", ErrTypeMismatch, filter.DataType())
	}
	columns := make([]arrow.Array, 0, len(exprs))
	fields := make([]arrow.Field, 0, len(exprs))
	for _, expr := range exprs {
		column, evalErr := evalExpr(ctx, engine, expr.node, batch)
		if evalErr != nil {
			arrowutil.ReleaseArrays(columns)
			return nil, evalErr
		}
		columns = append(columns, column)
		fields = append(fields, arrow.Field{Name: expr.outputName(), Type: column.DataType(), Nullable: true})
	}
	selected := array.NewRecordBatch(arrow.NewSchema(fields, nil), columns, batch.NumRows())
	arrowutil.ReleaseArrays(columns)
	filtered, err := compute.FilterRecordBatch(compute.WithAllocator(ctx, engine.allocator), selected, filter, compute.DefaultFilterOptions())
	selected.Release()
	return filtered, err
}
