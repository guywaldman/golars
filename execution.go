package golars

import (
	"context"
	"fmt"
	"math"
	"sort"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/compute"
	"github.com/apache/arrow-go/v18/arrow/scalar"
)

type tableSource struct {
	reader *array.TableReader
}

type scanRequest struct {
	columns               []string
	projectionColumns     []string
	filterOnlyColumns     []string
	projectionOnlyBatches bool
	filters               []*exprNode
}

type scanRequestContextKey struct{}

func withScanRequest(ctx context.Context, request scanRequest) context.Context {
	return context.WithValue(ctx, scanRequestContextKey{}, request)
}

func scanRequestFromContext(ctx context.Context) scanRequest {
	request, _ := ctx.Value(scanRequestContextKey{}).(scanRequest)
	return request
}

func deriveScanRequest(nodes []*planNode) scanRequest {
	if len(nodes) < 2 || nodes[len(nodes)-1].kind != planSelect {
		return scanRequest{}
	}
	columns := make(map[string]struct{})
	projectionColumns := make(map[string]struct{})
	filterColumns := make(map[string]struct{})
	var requestFilters []*exprNode
	for _, node := range nodes[1 : len(nodes)-1] {
		switch node.kind {
		case planFilter:
			filter := node.exprs[0].node
			collectExprColumns(filter, columns)
			collectExprColumns(filter, filterColumns)
			if filter != nil {
				requestFilters = append(requestFilters, filter)
			}
		case planLimit:
			// Limits do not require additional columns.
		default:
			return scanRequest{}
		}
	}
	for _, expr := range nodes[len(nodes)-1].exprs {
		if expr.node != nil && expr.node.kind == exprCall && expr.node.name == "all_columns" {
			return scanRequest{}
		}
		collectExprColumns(expr.node, columns)
		collectExprColumns(expr.node, projectionColumns)
	}
	if len(columns) == 0 {
		return scanRequest{}
	}
	result := make([]string, 0, len(columns))
	for column := range columns {
		result = append(result, column)
	}
	sort.Strings(result)
	return scanRequest{
		columns:               result,
		projectionColumns:     sortedColumnNames(projectionColumns),
		filterOnlyColumns:     filterOnlyColumnNames(filterColumns, projectionColumns),
		projectionOnlyBatches: true,
		filters:               requestFilters,
	}
}

func deriveGroupByScanRequest(plan *planNode, keys, exprs []Expr) scanRequest {
	if plan == nil {
		return scanRequest{}
	}
	nodes := make([]*planNode, 0, 8)
	for node := plan; node != nil; node = node.input {
		nodes = append(nodes, node)
	}
	reversePlans(nodes)
	columns := make(map[string]struct{})
	projectionColumns := make(map[string]struct{})
	filterColumns := make(map[string]struct{})
	for _, expr := range keys {
		collectExprColumns(expr.node, columns)
		collectExprColumns(expr.node, projectionColumns)
	}
	for _, expr := range exprs {
		collectExprColumns(expr.node, columns)
		collectExprColumns(expr.node, projectionColumns)
	}
	var filters []*exprNode
	for _, node := range nodes[1:] {
		switch node.kind {
		case planFilter:
			filter := node.exprs[0].node
			collectExprColumns(filter, columns)
			collectExprColumns(filter, filterColumns)
			if filter != nil {
				filters = append(filters, filter)
			}
		case planLimit:
		default:
			return scanRequest{}
		}
	}
	if len(columns) == 0 {
		return scanRequest{}
	}
	result := make([]string, 0, len(columns))
	for column := range columns {
		result = append(result, column)
	}
	sort.Strings(result)
	return scanRequest{
		columns:           result,
		projectionColumns: sortedColumnNames(projectionColumns),
		filterOnlyColumns: filterOnlyColumnNames(filterColumns, projectionColumns),
		filters:           filters,
	}
}

func sortedColumnNames(columns map[string]struct{}) []string {
	result := make([]string, 0, len(columns))
	for column := range columns {
		result = append(result, column)
	}
	sort.Strings(result)
	return result
}

func filterOnlyColumnNames(filters, projection map[string]struct{}) []string {
	result := make([]string, 0, len(filters))
	for column := range filters {
		if _, used := projection[column]; !used {
			result = append(result, column)
		}
	}
	sort.Strings(result)
	return result
}

func collectExprColumns(node *exprNode, columns map[string]struct{}) {
	if node == nil {
		return
	}
	if node.kind == exprColumn {
		columns[node.name] = struct{}{}
		return
	}
	for _, arg := range node.args {
		collectExprColumns(arg, columns)
	}
}

func newTableSource(table arrow.Table, batchSize int64) *tableSource {
	return &tableSource{reader: array.NewTableReader(table, batchSize)}
}
func (s *tableSource) Schema() *arrow.Schema          { return s.reader.Schema() }
func (s *tableSource) Next() bool                     { return s.reader.Next() }
func (s *tableSource) RecordBatch() arrow.RecordBatch { return s.reader.RecordBatch() }
func (s *tableSource) Err() error                     { return s.reader.Err() }
func (s *tableSource) Release()                       { s.reader.Release() }

func (lf *LazyFrame) execute(ctx context.Context) (arrow.Table, error) {
	return lf.collectTable(ctx, nil)
}

func (lf *LazyFrame) collectWithScanRequest(ctx context.Context, request *scanRequest) (*DataFrame, error) {
	table, err := lf.collectTable(ctx, request)
	if err != nil {
		return nil, err
	}
	return &DataFrame{table: table, engine: lf.engine}, nil
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
	for node := lf.plan; node != nil; node = node.input {
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
	var prefetch *batchPrefetch
	if lf.engine.parallelism > 1 {
		prefetch = newBatchPrefetch(ctx, source)
		defer prefetch.close()
	}
	nextBatch := func() (arrow.RecordBatch, bool, bool) {
		if prefetch != nil {
			item, ok := <-prefetch.batches
			if !ok {
				return nil, false, false
			}
			return item.batch, true, item.filtersFullyMatch
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
			releaseArrays(columns)
			return nil, evalErr
		}
		columns = append(columns, column)
		fields = append(fields, arrow.Field{Name: expr.outputName(), Type: column.DataType(), Nullable: true})
	}
	selected := array.NewRecordBatch(arrow.NewSchema(fields, nil), columns, batch.NumRows())
	releaseArrays(columns)
	filtered, err := compute.FilterRecordBatch(compute.WithAllocator(ctx, engine.allocator), selected, filter, compute.DefaultFilterOptions())
	selected.Release()
	return filtered, err
}

func evalFilter(ctx context.Context, engine *Engine, node *exprNode, batch arrow.RecordBatch) (arrow.Array, error) {
	if node == nil {
		return nil, fmt.Errorf("%w: nil filter expression", ErrInvalidPlan)
	}
	if node.kind == exprAlias && len(node.args) == 1 {
		return evalFilter(ctx, engine, node.args[0], batch)
	}
	if node.kind == exprCall {
		switch node.name {
		case "equal", "not_equal", "greater", "greater_equal", "less", "less_equal":
			if result, optimized, err := evalScalarComparison(ctx, engine, node, batch); optimized {
				return result, err
			}
		case "and_kleene", "or_kleene":
			if len(node.args) == 2 {
				left, err := evalFilter(ctx, engine, node.args[0], batch)
				if err != nil {
					return nil, err
				}
				right, err := evalFilter(ctx, engine, node.args[1], batch)
				if err != nil {
					left.Release()
					return nil, err
				}
				defer left.Release()
				defer right.Release()
				leftDatum := compute.NewDatum(left)
				rightDatum := compute.NewDatum(right)
				defer leftDatum.Release()
				defer rightDatum.Release()
				out, err := compute.CallFunction(compute.WithAllocator(ctx, engine.allocator), node.name, nil, leftDatum, rightDatum)
				if err != nil {
					return nil, err
				}
				arrayOut, ok := out.(*compute.ArrayDatum)
				if !ok {
					out.Release()
					return nil, fmt.Errorf("golars: filter %s returned %T", node.name, out)
				}
				result := arrayOut.MakeArray()
				out.Release()
				return result, nil
			}
		case "invert", "is_null", "is_valid":
			if len(node.args) == 1 {
				input, err := evalFilter(ctx, engine, node.args[0], batch)
				if err != nil {
					return nil, err
				}
				defer input.Release()
				inputDatum := compute.NewDatum(input)
				defer inputDatum.Release()
				out, err := compute.CallFunction(compute.WithAllocator(ctx, engine.allocator), node.name, nil, inputDatum)
				if err != nil {
					return nil, err
				}
				arrayOut, ok := out.(*compute.ArrayDatum)
				if !ok {
					out.Release()
					return nil, fmt.Errorf("golars: filter %s returned %T", node.name, out)
				}
				result := arrayOut.MakeArray()
				out.Release()
				return result, nil
			}
		}
	}
	return evalExpr(ctx, engine, node, batch)
}

func evalScalarComparison(ctx context.Context, engine *Engine, node *exprNode, batch arrow.RecordBatch) (arrow.Array, bool, error) {
	if len(node.args) != 2 {
		return nil, false, nil
	}
	columnNode, literalNode := node.args[0], node.args[1]
	reversed := false
	if columnNode.kind != exprColumn || literalNode.kind != exprLiteral {
		columnNode, literalNode = literalNode, columnNode
		if columnNode.kind != exprColumn || literalNode.kind != exprLiteral {
			return nil, false, nil
		}
		reversed = true
	}
	indices := batch.Schema().FieldIndices(columnNode.name)
	if len(indices) == 0 {
		return nil, true, &ColumnNotFoundError{Name: columnNode.name, Available: schemaNames(batch.Schema())}
	}
	column := batch.Column(indices[0])
	column.Retain()
	defer column.Release()
	literal, err := comparisonLiteralScalar(literalNode.value, column.DataType())
	if err != nil {
		return nil, true, err
	}
	left := compute.NewDatum(column)
	right := compute.NewDatum(literal)
	defer left.Release()
	defer right.Release()
	name := node.name
	if reversed {
		name = invertParquetComparison(name)
	}
	out, err := compute.CallFunction(compute.WithAllocator(ctx, engine.allocator), name, nil, left, right)
	if err != nil {
		return nil, true, err
	}
	arrayOut, ok := out.(*compute.ArrayDatum)
	if !ok {
		out.Release()
		return nil, true, fmt.Errorf("golars: filter %s returned %T", node.name, out)
	}
	result := arrayOut.MakeArray()
	out.Release()
	return result, true, nil
}

func comparisonLiteralScalar(value any, dtype arrow.DataType) (scalar.Scalar, error) {
	if value == nil {
		return scalar.MakeNullScalar(dtype), nil
	}
	if timestamp, ok := value.(time.Time); ok {
		switch dtype.ID() {
		case arrow.TIMESTAMP:
			return scalar.MakeScalarParam(arrow.Timestamp(timestamp.UnixNano()), dtype)
		case arrow.DATE64:
			return scalar.MakeScalarParam(arrow.Date64(timestamp.UnixMilli()), dtype)
		}
	}
	return scalar.MakeScalarParam(value, dtype)
}

func applySelect(ctx context.Context, engine *Engine, batch arrow.RecordBatch, exprs []Expr) (arrow.RecordBatch, int64, error) {
	columns := make([]arrow.Array, 0, len(exprs))
	fields := make([]arrow.Field, 0, len(exprs))
	for _, expr := range exprs {
		if expr.node != nil && expr.node.kind == exprCall && expr.node.name == "all_columns" {
			for i := 0; i < int(batch.NumCols()); i++ {
				col := batch.Column(i)
				col.Retain()
				columns = append(columns, col)
				fields = append(fields, batch.Schema().Field(i))
			}
			continue
		}
		col, err := evalExpr(ctx, engine, expr.node, batch)
		if err != nil {
			releaseArrays(columns)
			return nil, 0, err
		}
		columns = append(columns, col)
		fields = append(fields, arrow.Field{Name: expr.outputName(), Type: col.DataType(), Nullable: true})
	}
	schema := arrow.NewSchema(fields, nil)
	out := array.NewRecordBatch(schema, columns, batch.NumRows())
	releaseArrays(columns)
	return out, math.MaxInt64, nil
}

func applyWithColumns(ctx context.Context, engine *Engine, batch arrow.RecordBatch, exprs []Expr) (arrow.RecordBatch, int64, error) {
	columns := make([]arrow.Array, int(batch.NumCols()))
	fields := make([]arrow.Field, int(batch.NumCols()))
	for i := range columns {
		columns[i] = batch.Column(i)
		columns[i].Retain()
		fields[i] = batch.Schema().Field(i)
	}
	for _, expr := range exprs {
		col, err := evalExpr(ctx, engine, expr.node, batch)
		if err != nil {
			releaseArrays(columns)
			return nil, 0, err
		}
		name := expr.outputName()
		indices := fieldIndices(fields, name)
		if len(indices) > 0 {
			columns[indices[0]].Release()
			columns[indices[0]] = col
			fields[indices[0]] = arrow.Field{Name: name, Type: col.DataType(), Nullable: true}
		} else {
			columns = append(columns, col)
			fields = append(fields, arrow.Field{Name: name, Type: col.DataType(), Nullable: true})
		}
	}
	out := array.NewRecordBatch(arrow.NewSchema(fields, nil), columns, batch.NumRows())
	releaseArrays(columns)
	return out, math.MaxInt64, nil
}

func applySort(ctx context.Context, engine *Engine, batch arrow.RecordBatch, exprs []Expr) (arrow.RecordBatch, int64, error) {
	// Sort is implemented as a stable row permutation for a single batch. The
	// blocking plan path below sorts a collected table; this fast path preserves
	// streaming for scan pipelines whose batches are already bounded.
	if len(exprs) == 0 || batch.NumRows() < 2 {
		batch.Retain()
		return batch, math.MaxInt64, nil
	}
	keys := make([]arrow.Array, len(exprs))
	for i, expr := range exprs {
		arr, err := evalExpr(ctx, engine, expr.node, batch)
		if err != nil {
			releaseArrays(keys)
			return nil, 0, err
		}
		keys[i] = arr
	}
	indices := make([]int, batch.NumRows())
	for i := range indices {
		indices[i] = i
	}
	sort.SliceStable(indices, func(i, j int) bool {
		for keyIndex, key := range keys {
			cmp := compareArrayValues(key, indices[i], indices[j])
			if cmp != 0 {
				if exprs[keyIndex].node != nil && exprs[keyIndex].node.descending {
					return cmp > 0
				}
				return cmp < 0
			}
		}
		return false
	})
	releaseArrays(keys)
	// Arrow's take kernel is not needed for this compact implementation; build
	// slices one row at a time through a take index array in a later optimizer.
	return reorderRecord(ctx, engine, batch, indices)
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
	cols := make([]arrow.Array, batch.NumCols())
	for i := range cols {
		datum := compute.NewDatum(batch.Column(i))
		idxDatum := compute.NewDatum(index)
		out, err := compute.CallFunction(compute.WithAllocator(ctx, engine.allocator), "take", nil, datum, idxDatum)
		datum.Release()
		idxDatum.Release()
		if err != nil {
			releaseArrays(cols)
			return nil, 0, err
		}
		cols[i] = out.(*compute.ArrayDatum).MakeArray()
		out.Release()
	}
	rec := array.NewRecordBatch(batch.Schema(), cols, int64(len(indices)))
	releaseArrays(cols)
	return rec, math.MaxInt64, nil
}

func evalExpr(ctx context.Context, engine *Engine, node *exprNode, batch arrow.RecordBatch) (arrow.Array, error) {
	if node == nil {
		return nil, fmt.Errorf("%w: nil expression", ErrInvalidPlan)
	}
	switch node.kind {
	case exprColumn:
		indices := batch.Schema().FieldIndices(node.name)
		if len(indices) == 0 {
			return nil, &ColumnNotFoundError{Name: node.name, Available: schemaNames(batch.Schema())}
		}
		arr := batch.Column(indices[0])
		arr.Retain()
		return arr, nil
	case exprLiteral:
		return literalArray(node.value, batch.NumRows(), engine.allocator)
	case exprAlias:
		return evalExpr(ctx, engine, node.args[0], batch)
	case exprCast:
		arr, err := evalExpr(ctx, engine, node.args[0], batch)
		if err != nil {
			return nil, err
		}
		defer arr.Release()
		return compute.CastToType(compute.WithAllocator(ctx, engine.allocator), arr, node.dtype.arrow)
	case exprCall:
		if node.name == "all_columns" {
			return nil, fmt.Errorf("%w: all() is only valid in Select", ErrInvalidPlan)
		}
		args := make([]arrow.Array, len(node.args))
		for i, arg := range node.args {
			arr, err := evalExpr(ctx, engine, arg, batch)
			if err != nil {
				releaseArrays(args)
				return nil, err
			}
			args[i] = arr
		}
		defer releaseArrays(args)
		if node.name == "coalesce" {
			return coalesceArrays(args[0], args[1], engine.allocator)
		}
		if strings.HasPrefix(node.name, "str_") {
			return evalStringCall(node.name, args, engine.allocator)
		}
		if strings.HasPrefix(node.name, "dt_") {
			return evalTemporalCall(node.name, args, engine.allocator)
		}
		data := make([]compute.Datum, len(args))
		for i, arg := range args {
			data[i] = compute.NewDatum(arg)
		}
		defer func() {
			for _, datum := range data {
				datum.Release()
			}
		}()
		out, err := compute.CallFunction(compute.WithAllocator(ctx, engine.allocator), node.name, nil, data...)
		if err != nil {
			return nil, err
		}
		arr, ok := out.(*compute.ArrayDatum)
		if !ok {
			out.Release()
			return nil, fmt.Errorf("golars: expression %s returned %T", node.name, out)
		}
		result := arr.MakeArray()
		out.Release()
		return result, nil
	case exprAggregate:
		return nil, fmt.Errorf("%w: aggregate %s outside GroupBy", ErrInvalidPlan, node.name)
	default:
		return nil, fmt.Errorf("%w: unknown expression", ErrInvalidPlan)
	}
}

func evalStringCall(name string, args []arrow.Array, mem *quotaAllocator) (arrow.Array, error) {
	if len(args) == 0 {
		return nil, fmt.Errorf("%w: %s needs an input", ErrInvalidPlan, name)
	}
	if name == "str_contains" || name == "str_starts_with" || name == "str_ends_with" {
		if len(args) != 2 || args[0].Len() != args[1].Len() {
			return nil, fmt.Errorf("%w: string predicate inputs must have equal lengths", ErrTypeMismatch)
		}
		if left, leftOK := args[0].(array.StringLike); leftOK {
			if right, rightOK := args[1].(array.StringLike); rightOK {
				return evalStringPredicateFast(name, left, right, mem)
			}
		}
		builder := array.NewBooleanBuilder(mem)
		defer builder.Release()
		for i := 0; i < args[0].Len(); i++ {
			left, leftErr := scalar.GetScalar(args[0], i)
			right, rightErr := scalar.GetScalar(args[1], i)
			if leftErr != nil || rightErr != nil || !left.IsValid() || !right.IsValid() {
				builder.AppendNull()
				continue
			}
			leftText, rightText := left.String(), right.String()
			matched := false
			switch name {
			case "str_contains":
				matched = strings.Contains(leftText, rightText)
			case "str_starts_with":
				matched = strings.HasPrefix(leftText, rightText)
			case "str_ends_with":
				matched = strings.HasSuffix(leftText, rightText)
			}
			builder.Append(matched)
		}
		return builder.NewBooleanArray(), nil
	}
	if name == "str_length" {
		if values, ok := args[0].(array.StringLike); ok {
			builder := array.NewInt64Builder(mem)
			defer builder.Release()
			for i := 0; i < values.Len(); i++ {
				if !values.IsValid(i) {
					builder.AppendNull()
					continue
				}
				builder.Append(int64(utf8.RuneCountInString(values.Value(i))))
			}
			return builder.NewInt64Array(), nil
		}
		builder := array.NewInt64Builder(mem)
		defer builder.Release()
		for i := 0; i < args[0].Len(); i++ {
			value, err := scalar.GetScalar(args[0], i)
			if err != nil || !value.IsValid() {
				builder.AppendNull()
				continue
			}
			builder.Append(int64(len([]rune(value.String()))))
		}
		return builder.NewInt64Array(), nil
	}
	builder := array.NewStringBuilder(mem)
	defer builder.Release()
	for i := 0; i < args[0].Len(); i++ {
		value, err := scalar.GetScalar(args[0], i)
		if err != nil || !value.IsValid() {
			builder.AppendNull()
			continue
		}
		text := value.String()
		if name == "str_lower" {
			text = strings.ToLower(text)
		} else if name == "str_upper" {
			text = strings.ToUpper(text)
		} else {
			return nil, fmt.Errorf("%w: unsupported string expression %s", ErrInvalidPlan, name)
		}
		builder.Append(text)
	}
	return builder.NewStringArray(), nil
}

func evalStringPredicateFast(name string, left, right array.StringLike, mem *quotaAllocator) (arrow.Array, error) {
	builder := array.NewBooleanBuilder(mem)
	defer builder.Release()
	for i := 0; i < left.Len(); i++ {
		if !left.IsValid(i) || !right.IsValid(i) {
			builder.AppendNull()
			continue
		}
		leftText, rightText := left.Value(i), right.Value(i)
		var matched bool
		switch name {
		case "str_contains":
			matched = strings.Contains(leftText, rightText)
		case "str_starts_with":
			matched = strings.HasPrefix(leftText, rightText)
		case "str_ends_with":
			matched = strings.HasSuffix(leftText, rightText)
		default:
			return nil, fmt.Errorf("%w: unsupported string predicate %s", ErrInvalidPlan, name)
		}
		builder.Append(matched)
	}
	return builder.NewBooleanArray(), nil
}

func evalTemporalCall(name string, args []arrow.Array, mem *quotaAllocator) (arrow.Array, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("%w: %s expects one input", ErrInvalidPlan, name)
	}
	builder := array.NewInt32Builder(mem)
	defer builder.Release()
	for i := 0; i < args[0].Len(); i++ {
		value, err := scalar.GetScalar(args[0], i)
		if err != nil || !value.IsValid() {
			builder.AppendNull()
			continue
		}
		calendar, ok := value.(interface{ ToTime() time.Time })
		if !ok {
			builder.AppendNull()
			continue
		}
		date := calendar.ToTime()
		switch name {
		case "dt_year":
			builder.Append(int32(date.Year()))
		case "dt_month":
			builder.Append(int32(date.Month()))
		case "dt_day":
			builder.Append(int32(date.Day()))
		default:
			return nil, fmt.Errorf("%w: unsupported temporal expression %s", ErrInvalidPlan, name)
		}
	}
	return builder.NewInt32Array(), nil
}

func literalArray(value any, length int64, mem *quotaAllocator) (arrow.Array, error) {
	if length < 0 || length > math.MaxInt32 {
		return nil, fmt.Errorf("golars: invalid literal length %d", length)
	}
	var sc scalar.Scalar
	if timestamp, ok := value.(time.Time); ok {
		sc, _ = scalar.MakeScalarParam(arrow.Timestamp(timestamp.UnixNano()), &arrow.TimestampType{Unit: arrow.Nanosecond})
	}
	if sc == nil {
		func() {
			defer func() {
				if recover() != nil {
					sc = nil
				}
			}()
			sc = scalar.MakeScalar(value)
		}()
	}
	if sc == nil {
		return nil, fmt.Errorf("%w: unsupported literal %T", ErrTypeMismatch, value)
	}
	return scalar.MakeArrayFromScalar(sc, int(length), mem)
}

func coalesceArrays(left, right arrow.Array, mem *quotaAllocator) (arrow.Array, error) {
	if left.DataType().ID() == arrow.NULL && right.DataType().ID() != arrow.NULL {
		left = scalar.MakeArrayOfNull(right.DataType(), left.Len(), mem)
		defer left.Release()
	}
	if right.DataType().ID() == arrow.NULL && left.DataType().ID() != arrow.NULL {
		right = scalar.MakeArrayOfNull(left.DataType(), right.Len(), mem)
		defer right.Release()
	}
	if !arrow.TypeEqual(left.DataType(), right.DataType()) {
		return nil, fmt.Errorf("%w: coalesce types %s and %s", ErrTypeMismatch, left.DataType(), right.DataType())
	}
	b := array.NewBuilder(mem, left.DataType())
	defer b.Release()
	for i := 0; i < left.Len(); i++ {
		leftValue, err := scalar.GetScalar(left, i)
		if err != nil {
			return nil, err
		}
		if left.IsValid(i) {
			if err := scalar.Append(b, leftValue); err != nil {
				return nil, err
			}
		} else {
			rightValue, err := scalar.GetScalar(right, i)
			if err != nil {
				return nil, err
			}
			if err := scalar.Append(b, rightValue); err != nil {
				return nil, err
			}
		}
	}
	return b.NewArray(), nil
}

func releaseArrays(arrays []arrow.Array) {
	for _, arr := range arrays {
		if arr != nil {
			arr.Release()
		}
	}
}
func schemaNames(schema *arrow.Schema) []string {
	fields := schema.Fields()
	names := make([]string, len(fields))
	for i, field := range fields {
		names[i] = field.Name
	}
	return names
}
func fieldIndices(fields []arrow.Field, name string) []int {
	var out []int
	for i, field := range fields {
		if field.Name == name {
			out = append(out, i)
		}
	}
	return out
}

func compareArrayValues(arr arrow.Array, left, right int) int {
	if arr.IsNull(left) {
		if arr.IsNull(right) {
			return 0
		}
		return -1
	}
	if arr.IsNull(right) {
		return 1
	}
	leftScalar, leftErr := scalar.GetScalar(arr, left)
	rightScalar, rightErr := scalar.GetScalar(arr, right)
	if leftErr != nil || rightErr != nil {
		return 0
	}
	if leftScalar.String() < rightScalar.String() {
		return -1
	}
	if leftScalar.String() > rightScalar.String() {
		return 1
	}
	return 0
}
