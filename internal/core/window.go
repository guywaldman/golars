package core

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/scalar"
	"github.com/guywaldman/golars/internal/arrowutil"
)

func evalWindowExpr(ctx context.Context, engine *Engine, node *exprNode, batch arrow.RecordBatch) (arrow.Array, error) {
	partitions, err := windowPartitions(ctx, engine, node.windowKeys, batch)
	if err != nil {
		return nil, err
	}
	base := cloneExprNode(node)
	base.windowKeys = nil
	switch base.kind {
	case exprAggregate:
		return evalWindowAggregate(ctx, engine, base, partitions, batch)
	case exprCall:
		switch base.name {
		case "rank", "dense_rank", "row_number":
			return evalWindowRank(ctx, engine, base, partitions, batch)
		case "cum_sum":
			return evalWindowCumSum(ctx, engine, base, partitions, batch)
		}
	}
	return nil, fmt.Errorf("%w: unsupported window expression %s", ErrInvalidPlan, formatExpr(node))
}

func windowPartitions(ctx context.Context, engine *Engine, keys []*exprNode, batch arrow.RecordBatch) ([][]int, error) {
	if len(keys) == 0 {
		rows := make([]int, batch.NumRows())
		for i := range rows {
			rows[i] = i
		}
		return [][]int{rows}, nil
	}
	arrays := make([]arrow.Array, len(keys))
	for i, key := range keys {
		value, err := evalExpr(ctx, engine, key, batch)
		if err != nil {
			arrowutil.ReleaseArrays(arrays)
			return nil, err
		}
		arrays[i] = value
	}
	defer arrowutil.ReleaseArrays(arrays)
	byKey := make(map[string]int)
	partitions := make([][]int, 0)
	for row := 0; row < int(batch.NumRows()); row++ {
		var key strings.Builder
		for _, values := range arrays {
			value, err := scalar.GetScalar(values, row)
			if err != nil {
				return nil, err
			}
			if !value.IsValid() {
				key.WriteString("null|")
				continue
			}
			fmt.Fprintf(&key, "%d:%d:%s|", value.DataType().ID(), len(value.String()), value.String())
		}
		partitionKey := key.String()
		index, ok := byKey[partitionKey]
		if !ok {
			index = len(partitions)
			byKey[partitionKey] = index
			partitions = append(partitions, nil)
		}
		partitions[index] = append(partitions[index], row)
	}
	return partitions, nil
}

func evalWindowAggregate(ctx context.Context, engine *Engine, node *exprNode, partitions [][]int, batch arrow.RecordBatch) (arrow.Array, error) {
	var input arrow.Array
	var err error
	if node.name != "count_rows" {
		if len(node.args) != 1 {
			return nil, fmt.Errorf("%w: window aggregate %s expects one argument", ErrInvalidPlan, node.name)
		}
		input, err = evalExpr(ctx, engine, node.args[0], batch)
		if err != nil {
			return nil, err
		}
		defer input.Release()
	}
	outputType := arrow.PrimitiveTypes.Int64
	if input != nil {
		outputType = aggOutputType(node.name, input.DataType())
	}
	values := make([]scalar.Scalar, batch.NumRows())
	for _, partition := range partitions {
		state := aggState{dataType: outputType}
		if node.name == "count_rows" {
			state.count = int64(len(partition))
		} else {
			for _, row := range partition {
				value, valueErr := scalar.GetScalar(input, row)
				if valueErr != nil {
					return nil, valueErr
				}
				if !value.IsValid() {
					continue
				}
				state.count++
				switch node.name {
				case "count":
				case "sum", "mean":
					if numeric, ok := arrowutil.ScalarNumber(value); ok {
						state.sum += numeric
						state.validSum = true
					}
				case "min":
					if state.min == nil || compareScalars(value, state.min) < 0 {
						state.min = value
					}
				case "max":
					if state.max == nil || compareScalars(value, state.max) > 0 {
						state.max = value
					}
				case "first":
					if state.first == nil {
						state.first = value
					}
				case "last":
					state.last = value
				default:
					return nil, fmt.Errorf("%w: unsupported window aggregate %s", ErrInvalidPlan, node.name)
				}
			}
		}
		result := aggregateValue(state, node.name)
		for _, row := range partition {
			values[row] = result
		}
	}
	builder := array.NewBuilder(engine.allocator, outputType)
	defer builder.Release()
	for _, value := range values {
		appendScalar(builder, value)
	}
	return builder.NewArray(), nil
}

func evalWindowRank(ctx context.Context, engine *Engine, node *exprNode, partitions [][]int, batch arrow.RecordBatch) (arrow.Array, error) {
	var input arrow.Array
	var err error
	if node.name != "row_number" {
		if len(node.args) != 1 {
			return nil, fmt.Errorf("%w: %s expects one argument", ErrInvalidPlan, node.name)
		}
		input, err = evalExpr(ctx, engine, node.args[0], batch)
		if err != nil {
			return nil, err
		}
		defer input.Release()
	}
	ranks := make([]int64, batch.NumRows())
	for _, partition := range partitions {
		ordered := append([]int(nil), partition...)
		if input != nil {
			sort.SliceStable(ordered, func(i, j int) bool {
				left, _ := scalar.GetScalar(input, ordered[i])
				right, _ := scalar.GetScalar(input, ordered[j])
				return compareScalars(left, right) < 0
			})
		}
		lastRank := int64(0)
		for position, row := range ordered {
			rank := int64(position + 1)
			if node.name == "dense_rank" && position > 0 {
				previous, _ := scalar.GetScalar(input, ordered[position-1])
				current, _ := scalar.GetScalar(input, row)
				if compareScalars(previous, current) == 0 {
					rank = lastRank
				}
			}
			if node.name == "rank" && position > 0 {
				previous, _ := scalar.GetScalar(input, ordered[position-1])
				current, _ := scalar.GetScalar(input, row)
				if compareScalars(previous, current) == 0 {
					rank = lastRank
				}
			}
			lastRank = rank
			ranks[row] = rank
		}
	}
	builder := array.NewInt64Builder(engine.allocator)
	defer builder.Release()
	builder.AppendValues(ranks, nil)
	return builder.NewArray(), nil
}

func evalWindowCumSum(ctx context.Context, engine *Engine, node *exprNode, partitions [][]int, batch arrow.RecordBatch) (arrow.Array, error) {
	if len(node.args) != 1 {
		return nil, fmt.Errorf("%w: cum_sum expects one argument", ErrInvalidPlan)
	}
	input, err := evalExpr(ctx, engine, node.args[0], batch)
	if err != nil {
		return nil, err
	}
	defer input.Release()
	if !DataTypeFromArrow(input.DataType()).IsNumeric() {
		return nil, fmt.Errorf("%w: cum_sum requires a numeric input", ErrTypeMismatch)
	}
	values := make([]scalar.Scalar, batch.NumRows())
	for _, partition := range partitions {
		var total float64
		for _, row := range partition {
			value, valueErr := scalar.GetScalar(input, row)
			if valueErr != nil {
				return nil, valueErr
			}
			if !value.IsValid() {
				values[row] = scalar.MakeNullScalar(input.DataType())
				continue
			}
			numeric, ok := arrowutil.ScalarNumber(value)
			if !ok {
				return nil, fmt.Errorf("%w: cum_sum cannot read %s", ErrTypeMismatch, input.DataType())
			}
			total += numeric
			values[row], err = scalarForType(total, input.DataType())
			if err != nil {
				return nil, err
			}
		}
	}
	builder := array.NewBuilder(engine.allocator, input.DataType())
	defer builder.Release()
	for _, value := range values {
		appendScalar(builder, value)
	}
	return builder.NewArray(), nil
}
