package golars

import (
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
