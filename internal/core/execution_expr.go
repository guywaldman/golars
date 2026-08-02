package core

import (
	"bytes"
	"context"
	"fmt"
	"math"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/compute"
	"github.com/apache/arrow-go/v18/arrow/scalar"
	"github.com/guywaldman/golars/internal/arrowutil"
)

func evalExpr(ctx context.Context, engine *Engine, node *exprNode, batch arrow.RecordBatch) (arrow.Array, error) {
	if node == nil {
		return nil, fmt.Errorf("%w: nil expression", ErrInvalidPlan)
	}
	if len(node.windowKeys) != 0 {
		return evalWindowExpr(ctx, engine, node, batch)
	}
	switch node.kind {
	case exprColumn:
		indices := batch.Schema().FieldIndices(node.name)
		if len(indices) == 0 {
			return nil, &ColumnNotFoundError{Name: node.name, Available: arrowutil.SchemaNames(batch.Schema())}
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
		if node.name == "when_then" {
			return evalWhenThen(ctx, engine, node, batch)
		}
		if node.name == "is_in" || node.name == "is_between" || node.name == "is_nan" || node.name == "is_not_nan" || node.name == "is_finite" || node.name == "is_infinite" {
			return evalPredicateExpr(ctx, engine, node, batch)
		}
		if isComparisonCall(node.name) {
			if result, handled, err := evalScalarComparison(ctx, engine, node, batch); handled {
				return result, err
			}
		}
		if isStringPredicate(node.name) && len(node.args) == 2 && node.args[1].kind == exprLiteral {
			if pattern, ok := node.args[1].value.(string); ok {
				input, err := evalExpr(ctx, engine, node.args[0], batch)
				if err != nil {
					return nil, err
				}
				defer input.Release()
				return evalStringPredicateLiteral(node.name, input, pattern, engine.allocator)
			}
		}
		args := make([]arrow.Array, len(node.args))
		for i, arg := range node.args {
			arr, err := evalExpr(ctx, engine, arg, batch)
			if err != nil {
				arrowutil.ReleaseArrays(args)
				return nil, err
			}
			args[i] = arr
		}
		defer arrowutil.ReleaseArrays(args)
		if len(args) == 2 && coercibleCall(node.name) {
			left, right, coerceErr := coerceArrayPair(ctx, engine.allocator, args[0], args[1])
			if coerceErr != nil {
				return nil, coerceErr
			}
			args[0].Release()
			args[1].Release()
			args[0], args[1] = left, right
		}
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

func coercibleCall(name string) bool {
	switch name {
	case "equal", "not_equal", "greater", "greater_equal", "less", "less_equal",
		"add", "subtract", "multiply", "divide", "coalesce":
		return true
	default:
		return false
	}
}

func isComparisonCall(name string) bool {
	switch name {
	case "equal", "not_equal", "greater", "greater_equal", "less", "less_equal":
		return true
	default:
		return false
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
		switch values := args[0].(type) {
		case *array.String:
			builder := array.NewInt64Builder(mem)
			defer builder.Release()
			builder.Reserve(values.Len())
			offsets, data := values.ValueOffsets(), values.ValueBytes()
			for i := 0; i < values.Len(); i++ {
				if !values.IsValid(i) {
					builder.UnsafeAppendBoolToBitmap(false)
					continue
				}
				builder.UnsafeAppend(int64(utf8.RuneCount(data[offsets[i]:offsets[i+1]])))
			}
			return builder.NewInt64Array(), nil
		case *array.LargeString:
			builder := array.NewInt64Builder(mem)
			defer builder.Release()
			builder.Reserve(values.Len())
			offsets, data := values.ValueOffsets(), values.ValueBytes()
			for i := 0; i < values.Len(); i++ {
				if !values.IsValid(i) {
					builder.UnsafeAppendBoolToBitmap(false)
					continue
				}
				builder.UnsafeAppend(int64(utf8.RuneCount(data[offsets[i]:offsets[i+1]])))
			}
			return builder.NewInt64Array(), nil
		}
		if values, ok := args[0].(array.StringLike); ok {
			builder := array.NewInt64Builder(mem)
			defer builder.Release()
			builder.Reserve(values.Len())
			for i := 0; i < values.Len(); i++ {
				if !values.IsValid(i) {
					builder.UnsafeAppendBoolToBitmap(false)
					continue
				}
				builder.UnsafeAppend(int64(utf8.RuneCountInString(values.Value(i))))
			}
			return builder.NewInt64Array(), nil
		}
		builder := array.NewInt64Builder(mem)
		defer builder.Release()
		builder.Reserve(args[0].Len())
		for i := 0; i < args[0].Len(); i++ {
			value, err := scalar.GetScalar(args[0], i)
			if err != nil || !value.IsValid() {
				builder.UnsafeAppendBoolToBitmap(false)
				continue
			}
			builder.UnsafeAppend(int64(len([]rune(value.String()))))
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

func isStringPredicate(name string) bool {
	switch name {
	case "str_contains", "str_starts_with", "str_ends_with":
		return true
	default:
		return false
	}
}

func evalStringPredicateLiteral(name string, input arrow.Array, pattern string, mem *quotaAllocator) (arrow.Array, error) {
	builder := array.NewBooleanBuilder(mem)
	defer builder.Release()
	builder.Reserve(input.Len())
	patternBytes := []byte(pattern)
	switch values := input.(type) {
	case *array.String:
		return evalStringPredicateBytes(name, values.Len(), values.IsValid, values.ValueOffsets(), values.ValueBytes(), patternBytes, builder), nil
	case *array.LargeString:
		return evalStringPredicateBytes(name, values.Len(), values.IsValid, values.ValueOffsets(), values.ValueBytes(), patternBytes, builder), nil
	}
	if values, ok := input.(array.StringLike); ok {
		for i := 0; i < values.Len(); i++ {
			if !values.IsValid(i) {
				builder.UnsafeAppendBoolToBitmap(false)
				continue
			}
			builder.UnsafeAppend(stringPredicateMatch(name, values.Value(i), pattern))
		}
		return builder.NewBooleanArray(), nil
	}
	for i := 0; i < input.Len(); i++ {
		value, err := scalar.GetScalar(input, i)
		if err != nil {
			return nil, err
		}
		if !value.IsValid() {
			builder.UnsafeAppendBoolToBitmap(false)
			continue
		}
		builder.UnsafeAppend(stringPredicateMatch(name, value.String(), pattern))
	}
	return builder.NewBooleanArray(), nil
}

func evalStringPredicateBytes[Offset int32 | int64](name string, length int, valid func(int) bool, offsets []Offset, data, pattern []byte, builder *array.BooleanBuilder) arrow.Array {
	for i := 0; i < length; i++ {
		if !valid(i) {
			builder.UnsafeAppendBoolToBitmap(false)
			continue
		}
		builder.UnsafeAppend(stringPredicateBytesMatch(name, data[offsets[i]:offsets[i+1]], pattern))
	}
	return builder.NewBooleanArray()
}

func stringPredicateBytesMatch(name string, value, pattern []byte) bool {
	switch name {
	case "str_contains":
		return bytes.Contains(value, pattern)
	case "str_starts_with":
		return bytes.HasPrefix(value, pattern)
	case "str_ends_with":
		return bytes.HasSuffix(value, pattern)
	default:
		return false
	}
}

func evalStringPredicateFast(name string, left, right array.StringLike, mem *quotaAllocator) (arrow.Array, error) {
	builder := array.NewBooleanBuilder(mem)
	defer builder.Release()
	builder.Reserve(left.Len())
	for i := 0; i < left.Len(); i++ {
		if !left.IsValid(i) || !right.IsValid(i) {
			builder.UnsafeAppendBoolToBitmap(false)
			continue
		}
		leftText, rightText := left.Value(i), right.Value(i)
		builder.UnsafeAppend(stringPredicateMatch(name, leftText, rightText))
	}
	return builder.NewBooleanArray(), nil
}

func stringPredicateMatch(name, value, pattern string) bool {
	switch name {
	case "str_contains":
		return strings.Contains(value, pattern)
	case "str_starts_with":
		return strings.HasPrefix(value, pattern)
	case "str_ends_with":
		return strings.HasSuffix(value, pattern)
	default:
		return false
	}
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
	sc, err := literalScalar(value)
	if err != nil {
		return nil, err
	}
	return scalar.MakeArrayFromScalar(sc, int(length), mem)
}

func literalScalar(value any) (scalar.Scalar, error) {
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
	return sc, nil
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
