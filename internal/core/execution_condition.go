package core

import (
	"context"
	"fmt"
	"math"
	"reflect"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/scalar"
	"github.com/guywaldman/golars/internal/arrowutil"
)

func evalWhenThen(ctx context.Context, engine *Engine, node *exprNode, batch arrow.RecordBatch) (arrow.Array, error) {
	if len(node.args) != 3 {
		return nil, fmt.Errorf("%w: when/then/otherwise requires three expressions", ErrInvalidPlan)
	}
	predicate, err := evalFilter(ctx, engine, node.args[0], batch)
	if err != nil {
		return nil, err
	}
	defer predicate.Release()
	thenValues, err := evalExpr(ctx, engine, node.args[1], batch)
	if err != nil {
		return nil, err
	}
	defer thenValues.Release()
	elseValues, err := evalExpr(ctx, engine, node.args[2], batch)
	if err != nil {
		return nil, err
	}
	defer elseValues.Release()
	thenCoerced, elseCoerced, err := coerceArrayPair(ctx, engine.allocator, thenValues, elseValues)
	if err != nil {
		return nil, err
	}
	defer thenCoerced.Release()
	defer elseCoerced.Release()
	builder := array.NewBuilder(engine.allocator, thenCoerced.DataType())
	defer builder.Release()
	for row := 0; row < int(batch.NumRows()); row++ {
		condition, conditionErr := scalar.GetScalar(predicate, row)
		if conditionErr != nil {
			return nil, conditionErr
		}
		if !condition.IsValid() {
			builder.AppendNull()
			continue
		}
		conditionValue, ok := condition.(*scalar.Boolean)
		if !ok {
			return nil, fmt.Errorf("%w: when predicate must be boolean", ErrTypeMismatch)
		}
		valueArray := elseCoerced
		if conditionValue.Value {
			valueArray = thenCoerced
		}
		value, valueErr := scalar.GetScalar(valueArray, row)
		if valueErr != nil {
			return nil, valueErr
		}
		if appendErr := scalar.Append(builder, value); appendErr != nil {
			return nil, appendErr
		}
	}
	return builder.NewArray(), nil
}

func evalPredicateExpr(ctx context.Context, engine *Engine, node *exprNode, batch arrow.RecordBatch) (arrow.Array, error) {
	switch node.name {
	case "is_nan", "is_not_nan", "is_finite", "is_infinite":
		if len(node.args) != 1 {
			return nil, fmt.Errorf("%w: %s expects one expression", ErrInvalidPlan, node.name)
		}
		input, err := evalExpr(ctx, engine, node.args[0], batch)
		if err != nil {
			return nil, err
		}
		defer input.Release()
		builder := array.NewBooleanBuilder(engine.allocator)
		defer builder.Release()
		for row := 0; row < input.Len(); row++ {
			value, valueErr := scalar.GetScalar(input, row)
			if valueErr != nil {
				return nil, valueErr
			}
			if !value.IsValid() {
				builder.AppendNull()
				continue
			}
			numeric, ok := arrowutil.ScalarNumber(value)
			if !ok {
				builder.Append(false)
				continue
			}
			matched := false
			switch node.name {
			case "is_nan":
				matched = math.IsNaN(numeric)
			case "is_not_nan":
				matched = !math.IsNaN(numeric)
			case "is_finite":
				matched = !math.IsInf(numeric, 0) && !math.IsNaN(numeric)
			case "is_infinite":
				matched = math.IsInf(numeric, 0)
			}
			builder.Append(matched)
		}
		return builder.NewArray(), nil
	case "is_between":
		if len(node.args) != 3 {
			return nil, fmt.Errorf("%w: is_between expects three expressions", ErrInvalidPlan)
		}
		values := make([]arrow.Array, 3)
		for i, arg := range node.args {
			value, err := evalExpr(ctx, engine, arg, batch)
			if err != nil {
				arrowutil.ReleaseArrays(values)
				return nil, err
			}
			values[i] = value
		}
		defer arrowutil.ReleaseArrays(values)
		left, lower, err := coerceArrayPair(ctx, engine.allocator, values[0], values[1])
		if err != nil {
			return nil, err
		}
		defer left.Release()
		defer lower.Release()
		upper, err := castArray(ctx, engine.allocator, values[2], left.DataType())
		if err != nil {
			return nil, err
		}
		defer upper.Release()
		builder := array.NewBooleanBuilder(engine.allocator)
		defer builder.Release()
		for row := 0; row < left.Len(); row++ {
			leftValue, leftErr := scalar.GetScalar(left, row)
			lowerValue, lowerErr := scalar.GetScalar(lower, row)
			upperValue, upperErr := scalar.GetScalar(upper, row)
			if leftErr != nil || lowerErr != nil || upperErr != nil {
				return nil, firstError(leftErr, lowerErr, upperErr)
			}
			if !leftValue.IsValid() || !lowerValue.IsValid() || !upperValue.IsValid() {
				builder.AppendNull()
				continue
			}
			builder.Append(compareScalars(leftValue, lowerValue) >= 0 && compareScalars(leftValue, upperValue) <= 0)
		}
		return builder.NewArray(), nil
	case "is_in":
		if len(node.args) != 2 {
			return nil, fmt.Errorf("%w: is_in expects two expressions", ErrInvalidPlan)
		}
		return evalIsIn(ctx, engine, node.args[0], node.args[1], batch)
	default:
		return nil, fmt.Errorf("%w: unsupported predicate %s", ErrInvalidPlan, node.name)
	}
}

func evalIsIn(ctx context.Context, engine *Engine, valueNode, setNode *exprNode, batch arrow.RecordBatch) (arrow.Array, error) {
	values, err := evalExpr(ctx, engine, valueNode, batch)
	if err != nil {
		return nil, err
	}
	defer values.Release()
	set := make([]scalar.Scalar, 0)
	if setNode.kind == exprLiteral {
		reflected := reflect.ValueOf(setNode.value)
		if reflected.IsValid() && (reflected.Kind() == reflect.Slice || reflected.Kind() == reflect.Array) {
			for i := 0; i < reflected.Len(); i++ {
				candidate, candidateErr := scalarForType(reflected.Index(i).Interface(), values.DataType())
				if candidateErr != nil {
					return nil, candidateErr
				}
				set = append(set, candidate)
			}
		} else {
			candidate, candidateErr := scalarForType(setNode.value, values.DataType())
			if candidateErr != nil {
				return nil, candidateErr
			}
			set = append(set, candidate)
		}
	} else {
		setValues, setErr := evalExpr(ctx, engine, setNode, batch)
		if setErr != nil {
			return nil, setErr
		}
		defer setValues.Release()
		for row := 0; row < setValues.Len(); row++ {
			candidate, candidateErr := scalar.GetScalar(setValues, row)
			if candidateErr != nil {
				return nil, candidateErr
			}
			set = append(set, candidate)
		}
	}
	builder := array.NewBooleanBuilder(engine.allocator)
	defer builder.Release()
	for row := 0; row < values.Len(); row++ {
		value, valueErr := scalar.GetScalar(values, row)
		if valueErr != nil {
			return nil, valueErr
		}
		if !value.IsValid() {
			builder.AppendNull()
			continue
		}
		matched := false
		for _, candidate := range set {
			if candidate.IsValid() && compareScalars(value, candidate) == 0 {
				matched = true
				break
			}
		}
		builder.Append(matched)
	}
	return builder.NewArray(), nil
}

func firstError(errors ...error) error {
	for _, err := range errors {
		if err != nil {
			return err
		}
	}
	return nil
}
