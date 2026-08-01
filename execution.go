package golars

import (
	"context"
	"fmt"
	"math"
	"sort"
	"strings"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/compute"
	"github.com/apache/arrow-go/v18/arrow/scalar"
)

type tableSource struct {
	reader *array.TableReader
}

type scanRequest struct {
	columns []string
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
	for _, node := range nodes[1 : len(nodes)-1] {
		switch node.kind {
		case planFilter:
			collectExprColumns(node.exprs[0].node, columns)
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
	}
	if len(columns) == 0 {
		return scanRequest{}
	}
	result := make([]string, 0, len(columns))
	for column := range columns {
		result = append(result, column)
	}
	sort.Strings(result)
	return scanRequest{columns: result}
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
	nodes := make([]*planNode, 0, 8)
	for node := lf.plan; node != nil; node = node.input {
		nodes = append(nodes, node)
	}
	if len(nodes) == 0 {
		return nil, ErrInvalidPlan
	}
	reversePlans(nodes)
	root := nodes[0]
	var source recordSource
	var err error
	ctx = withScanRequest(ctx, deriveScanRequest(nodes))
	switch root.kind {
	case planTable:
		source = newTableSource(root.table, lf.engine.batchSize)
	case planSource:
		source, err = root.source(ctx, lf.engine)
	default:
		return nil, fmt.Errorf("%w: plan has no source", ErrInvalidPlan)
	}
	if err != nil {
		return nil, err
	}
	defer source.Release()

	sourceSchema := source.Schema()
	var records []arrow.RecordBatch
	remaining := int64(math.MaxInt64)
	for source.Next() {
		if err := ctx.Err(); err != nil {
			for _, rec := range records {
				rec.Release()
			}
			return nil, err
		}
		batch := source.RecordBatch()
		batch.Retain()
		for _, node := range nodes[1:] {
			var next arrow.RecordBatch
			next, remaining, err = applyPlanNode(ctx, lf.engine, node, batch, remaining)
			batch.Release()
			if err != nil {
				for _, rec := range records {
					rec.Release()
				}
				return nil, err
			}
			if next == nil {
				batch = nil
				break
			}
			batch = next
		}
		if batch != nil {
			records = append(records, batch)
		}
		if remaining == 0 {
			break
		}
	}
	if err := source.Err(); err != nil {
		for _, rec := range records {
			rec.Release()
		}
		return nil, err
	}
	var schema *arrow.Schema
	if len(records) > 0 {
		schema = records[0].Schema()
	} else {
		schema = sourceSchema
		if schema == nil {
			return nil, fmt.Errorf("golars: source produced no schema")
		}
		// Run transformations over a zero-row batch so Select/WithColumns still
		// produce their projected schema for empty results.
		empty, emptyErr := emptyRecordBatch(schema, lf.engine.allocator)
		if emptyErr != nil {
			return nil, emptyErr
		}
		for _, node := range nodes[1:] {
			next, _, nodeErr := applyPlanNode(ctx, lf.engine, node, empty, math.MaxInt64)
			currentSchema := empty.Schema()
			empty.Release()
			if nodeErr != nil {
				return nil, nodeErr
			}
			if next == nil {
				var emptyErr error
				empty, emptyErr = emptyRecordBatch(currentSchema, lf.engine.allocator)
				if emptyErr != nil {
					return nil, emptyErr
				}
				continue
			}
			empty = next
		}
		if empty != nil {
			schema = empty.Schema()
			empty.Release()
		}
	}
	table, tableErr := safeTableFromRecords(schema, records)
	for _, rec := range records {
		rec.Release()
	}
	return table, tableErr
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
		filter, err := evalExpr(ctx, engine, node.exprs[0].node, batch)
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
