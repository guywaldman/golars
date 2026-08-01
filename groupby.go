package golars

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/scalar"
)

type groupState struct {
	keyValues []scalar.Scalar
	aggs      []aggState
}

type aggState struct {
	count    int64
	sum      float64
	validSum bool
	first    scalar.Scalar
	last     scalar.Scalar
	min      scalar.Scalar
	max      scalar.Scalar
	dataType arrow.DataType
}

func executeGroupBy(ctx context.Context, df *DataFrame, keys, exprs []Expr) (*DataFrame, error) {
	if err := df.ensureOpen(); err != nil {
		return nil, err
	}
	if len(keys) == 0 || len(exprs) == 0 {
		return nil, fmt.Errorf("%w: GroupBy.Agg needs keys and expressions", ErrInvalidPlan)
	}
	if spec, ok := makeFastGroupBySpec(keys, exprs); ok {
		lf := df.Lazy()
		defer lf.Close()
		return executeFastGroupBy(ctx, lf, scanRequest{}, spec)
	}
	for _, expr := range exprs {
		if node := unwrapAlias(expr.node); node == nil || node.kind != exprAggregate {
			return nil, fmt.Errorf("%w: GroupBy.Agg expects aggregate expressions, got %s", ErrInvalidPlan, expr)
		}
	}
	reader := newTableSource(df.table, df.engine.batchSize)
	defer reader.Release()
	buckets := make(map[string]*groupState)
	order := make([]*groupState, 0)
	var keyTypes []arrow.DataType
	var aggTypes []arrow.DataType
	for reader.Next() {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		batch := reader.RecordBatch()
		keyArrays := make([]arrow.Array, len(keys))
		for i, expr := range keys {
			arr, err := evalExpr(ctx, df.engine, expr.node, batch)
			if err != nil {
				releaseArrays(keyArrays)
				return nil, err
			}
			keyArrays[i] = arr
			if len(keyTypes) == i {
				keyTypes = append(keyTypes, arr.DataType())
			}
		}
		aggArrays := make([]arrow.Array, len(exprs))
		for i, expr := range exprs {
			node := unwrapAlias(expr.node)
			if node.kind == exprAggregate && node.name != "count_rows" {
				arr, err := evalExpr(ctx, df.engine, node.args[0], batch)
				if err != nil {
					releaseArrays(keyArrays)
					releaseArrays(aggArrays)
					return nil, err
				}
				aggArrays[i] = arr
				if len(aggTypes) == i {
					aggTypes = append(aggTypes, aggOutputType(node.name, arr.DataType()))
				}
			} else if len(aggTypes) == i {
				aggTypes = append(aggTypes, arrow.PrimitiveTypes.Int64)
			}
		}
		for row := 0; row < int(batch.NumRows()); row++ {
			parts := make([]string, len(keyArrays))
			keyValues := make([]scalar.Scalar, len(keyArrays))
			for i, arr := range keyArrays {
				value, err := scalar.GetScalar(arr, row)
				if err != nil {
					releaseArrays(keyArrays)
					releaseArrays(aggArrays)
					return nil, err
				}
				keyValues[i] = cloneScalar(value)
				parts[i] = value.DataType().String() + ":" + value.String()
			}
			key := strings.Join(parts, "\x1f")
			state := buckets[key]
			if state == nil {
				state = &groupState{keyValues: keyValues, aggs: make([]aggState, len(exprs))}
				for i := range state.aggs {
					state.aggs[i].dataType = aggTypes[i]
				}
				buckets[key] = state
				order = append(order, state)
			}
			for i, expr := range exprs {
				node := unwrapAlias(expr.node)
				stateAgg := &state.aggs[i]
				if node.kind != exprAggregate {
					return nil, fmt.Errorf("%w: GroupBy.Agg expects aggregate expressions, got %s", ErrInvalidPlan, expr)
				}
				if node.name == "count_rows" {
					stateAgg.count++
					continue
				}
				value, err := scalar.GetScalar(aggArrays[i], row)
				if err != nil {
					releaseArrays(keyArrays)
					releaseArrays(aggArrays)
					return nil, err
				}
				if !value.IsValid() {
					continue
				}
				stateAgg.count++
				switch node.name {
				case "count":
					// count is already incremented for each valid input value.
				case "sum", "mean":
					if numeric, ok := scalarNumber(value); ok {
						stateAgg.sum += numeric
						stateAgg.validSum = true
					}
				case "min":
					if stateAgg.min == nil || compareScalars(value, stateAgg.min) < 0 {
						stateAgg.min = cloneScalar(value)
					}
				case "max":
					if stateAgg.max == nil || compareScalars(value, stateAgg.max) > 0 {
						stateAgg.max = cloneScalar(value)
					}
				case "first":
					if stateAgg.first == nil {
						stateAgg.first = cloneScalar(value)
					}
				case "last":
					stateAgg.last = cloneScalar(value)
				default:
					return nil, fmt.Errorf("%w: unsupported aggregate %s", ErrInvalidPlan, node.name)
				}
			}
		}
		releaseArrays(keyArrays)
		releaseArrays(aggArrays)
	}
	if err := reader.Err(); err != nil {
		return nil, err
	}
	if len(keyTypes) == 0 {
		empty, err := emptyRecordBatch(df.table.Schema(), df.engine.allocator)
		if err != nil {
			return nil, err
		}
		for _, expr := range keys {
			arr, evalErr := evalExpr(ctx, df.engine, expr.node, empty)
			if evalErr != nil {
				empty.Release()
				return nil, evalErr
			}
			keyTypes = append(keyTypes, arr.DataType())
			arr.Release()
		}
		for _, expr := range exprs {
			node := unwrapAlias(expr.node)
			if node.kind == exprAggregate && node.name != "count_rows" {
				arr, evalErr := evalExpr(ctx, df.engine, node.args[0], empty)
				if evalErr != nil {
					empty.Release()
					return nil, evalErr
				}
				aggTypes = append(aggTypes, aggOutputType(node.name, arr.DataType()))
				arr.Release()
			} else {
				aggTypes = append(aggTypes, arrow.PrimitiveTypes.Int64)
			}
		}
		empty.Release()
	}
	fields := make([]arrow.Field, 0, len(keys)+len(exprs))
	builders := make([]array.Builder, 0, len(keys)+len(exprs))
	for i, expr := range keys {
		fields = append(fields, arrow.Field{Name: expr.outputName(), Type: keyTypes[i], Nullable: true})
		builders = append(builders, array.NewBuilder(df.engine.allocator, keyTypes[i]))
	}
	for i, expr := range exprs {
		fields = append(fields, arrow.Field{Name: expr.outputName(), Type: aggTypes[i], Nullable: true})
		builders = append(builders, array.NewBuilder(df.engine.allocator, aggTypes[i]))
	}
	defer func() {
		for _, builder := range builders {
			builder.Release()
		}
	}()
	for _, state := range order {
		for i, value := range state.keyValues {
			appendScalar(builders[i], value)
		}
		for i, expr := range exprs {
			node := unwrapAlias(expr.node)
			value := aggregateValue(state.aggs[i], node.name)
			appendScalar(builders[len(keys)+i], value)
		}
	}
	columns := make([]arrow.Array, len(builders))
	for i, builder := range builders {
		columns[i] = builder.NewArray()
	}
	defer releaseArrays(columns)
	table := array.NewTableFromSlice(arrow.NewSchema(fields, nil), arraysAsSlices(columns))
	return &DataFrame{table: table, engine: df.engine}, nil
}

func arraysAsSlices(arrays []arrow.Array) [][]arrow.Array {
	data := make([][]arrow.Array, len(arrays))
	for i, arr := range arrays {
		data[i] = []arrow.Array{arr}
	}
	return data
}

func unwrapAlias(node *exprNode) *exprNode {
	if node != nil && node.kind == exprAlias && len(node.args) > 0 {
		return node.args[0]
	}
	return node
}

func aggOutputType(name string, input arrow.DataType) arrow.DataType {
	if name == "mean" {
		return arrow.PrimitiveTypes.Float64
	}
	return input
}

func appendScalar(builder array.Builder, value scalar.Scalar) {
	if value == nil || !value.IsValid() {
		builder.AppendNull()
		return
	}
	_ = scalar.Append(builder, value)
}

func aggregateValue(state aggState, name string) scalar.Scalar {
	switch name {
	case "count", "count_rows":
		return scalar.NewInt64Scalar(state.count)
	case "sum":
		if !state.validSum {
			return scalar.MakeNullScalar(state.dataType)
		}
		value, err := scalar.MakeScalarParam(strconv.FormatFloat(state.sum, 'g', -1, 64), state.dataType)
		if err == nil {
			return value
		}
		return scalar.NewFloat64Scalar(state.sum)
	case "mean":
		if state.count == 0 || !state.validSum {
			return scalar.MakeNullScalar(arrow.PrimitiveTypes.Float64)
		}
		return scalar.NewFloat64Scalar(state.sum / float64(state.count))
	case "min":
		return state.min
	case "max":
		return state.max
	case "first":
		return state.first
	case "last":
		return state.last
	default:
		return scalar.MakeNullScalar(state.dataType)
	}
}

func scalarNumber(value scalar.Scalar) (float64, bool) {
	switch v := value.(type) {
	case *scalar.Int8:
		return float64(v.Value), true
	case *scalar.Int16:
		return float64(v.Value), true
	case *scalar.Int32:
		return float64(v.Value), true
	case *scalar.Int64:
		return float64(v.Value), true
	case *scalar.Uint8:
		return float64(v.Value), true
	case *scalar.Uint16:
		return float64(v.Value), true
	case *scalar.Uint32:
		return float64(v.Value), true
	case *scalar.Uint64:
		return float64(v.Value), true
	case *scalar.Float32:
		return float64(v.Value), true
	case *scalar.Float64:
		return v.Value, true
	default:
		value, err := strconv.ParseFloat(value.String(), 64)
		return value, err == nil
	}
}

func compareScalars(left, right scalar.Scalar) int {
	leftNumber, leftOK := scalarNumber(left)
	rightNumber, rightOK := scalarNumber(right)
	if leftOK && rightOK {
		if leftNumber < rightNumber {
			return -1
		}
		if leftNumber > rightNumber {
			return 1
		}
		return 0
	}
	if left.String() < right.String() {
		return -1
	}
	if left.String() > right.String() {
		return 1
	}
	return 0
}

func cloneScalar(value scalar.Scalar) scalar.Scalar {
	if value == nil || !value.IsValid() {
		if value == nil {
			return nil
		}
		return scalar.MakeNullScalar(value.DataType())
	}
	copy, err := scalar.MakeScalarParam(value.String(), value.DataType())
	if err == nil {
		return copy
	}
	return value
}
