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
	if len(arrays) == 1 {
		if partitions, ok := windowPartitionsFast(arrays[0], int(batch.NumRows())); ok {
			return partitions, nil
		}
	}
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

func windowPartitionsFast(values arrow.Array, rows int) ([][]int, bool) {
	byKey := make(map[string]int)
	partitions := make([][]int, 0)
	nullPartition := -1
	appendRow := func(index int, row int) {
		if index == len(partitions) {
			partitions = append(partitions, nil)
		}
		partitions[index] = append(partitions[index], row)
	}
	newPartition := func(row int) {
		partitions = append(partitions, []int{row})
	}
	if stringValues, ok := values.(array.StringLike); ok {
		for row := 0; row < rows; row++ {
			if !stringValues.IsValid(row) {
				if nullPartition < 0 {
					nullPartition = len(partitions)
					newPartition(row)
				} else {
					appendRow(nullPartition, row)
				}
				continue
			}
			key := stringValues.Value(row)
			index, exists := byKey[key]
			if !exists {
				index = len(partitions)
				byKey[key] = index
				newPartition(row)
				continue
			}
			appendRow(index, row)
		}
		return partitions, true
	}
	if intValues, ok := values.(*array.Int64); ok {
		byValue := make(map[int64]int)
		for row := 0; row < rows; row++ {
			if !intValues.IsValid(row) {
				if nullPartition < 0 {
					nullPartition = len(partitions)
					newPartition(row)
				} else {
					appendRow(nullPartition, row)
				}
				continue
			}
			key := intValues.Value(row)
			index, exists := byValue[key]
			if !exists {
				index = len(partitions)
				byValue[key] = index
				newPartition(row)
				continue
			}
			appendRow(index, row)
		}
		return partitions, true
	}
	return nil, false
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
	if result, ok, fastErr := evalWindowAggregateFast(engine, node, partitions, batch.NumRows(), input); ok {
		return result, fastErr
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

func evalWindowAggregateFast(engine *Engine, node *exprNode, partitions [][]int, rows int64, input arrow.Array) (arrow.Array, bool, error) {
	if node.name == "count_rows" {
		builder := array.NewInt64Builder(engine.allocator)
		defer builder.Release()
		builder.Reserve(int(rows))
		values := make([]int64, rows)
		for _, partition := range partitions {
			count := int64(len(partition))
			for _, row := range partition {
				values[row] = count
			}
		}
		builder.AppendValues(values, nil)
		return builder.NewArray(), true, nil
	}
	if input == nil {
		return nil, false, nil
	}
	if node.name == "count" {
		builder := array.NewInt64Builder(engine.allocator)
		defer builder.Release()
		builder.Reserve(int(rows))
		values := make([]int64, rows)
		for _, partition := range partitions {
			var count int64
			for _, row := range partition {
				if input.IsValid(int(row)) {
					count++
				}
			}
			for _, row := range partition {
				values[row] = count
			}
		}
		builder.AppendValues(values, nil)
		return builder.NewArray(), true, nil
	}
	if node.name != "sum" && node.name != "mean" {
		return nil, false, nil
	}
	values, ok := input.(*array.Float64)
	if !ok {
		return nil, false, nil
	}
	builder := array.NewFloat64Builder(engine.allocator)
	defer builder.Release()
	builder.Reserve(int(rows))
	output := make([]float64, rows)
	valid := make([]bool, rows)
	for _, partition := range partitions {
		var total float64
		var count int64
		for _, row := range partition {
			if !values.IsValid(row) {
				continue
			}
			total += values.Value(row)
			count++
		}
		if count == 0 {
			continue
		}
		if node.name == "mean" {
			total /= float64(count)
		}
		for _, row := range partition {
			output[row] = total
			valid[row] = true
		}
	}
	for row := range output {
		if valid[row] {
			builder.Append(output[row])
		} else {
			builder.AppendNull()
		}
	}
	return builder.NewArray(), true, nil
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
