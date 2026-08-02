package core

import (
	"context"
	"fmt"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/compute"
	"github.com/apache/arrow-go/v18/arrow/scalar"
)

func commonArrowType(left, right arrow.DataType) (arrow.DataType, error) {
	if left == nil || left.ID() == arrow.NULL {
		return right, nil
	}
	if right == nil || right.ID() == arrow.NULL {
		return left, nil
	}
	if arrow.TypeEqual(left, right) {
		return left, nil
	}
	if arrow.IsFloating(left.ID()) && arrow.IsFloating(right.ID()) {
		if left.ID() == arrow.FLOAT64 || right.ID() == arrow.FLOAT64 {
			return arrow.PrimitiveTypes.Float64, nil
		}
		return arrow.PrimitiveTypes.Float32, nil
	}
	if isNumericArrow(left.ID()) && isNumericArrow(right.ID()) {
		if arrow.IsFloating(left.ID()) || arrow.IsFloating(right.ID()) {
			return arrow.PrimitiveTypes.Float64, nil
		}
		return commonIntegerType(left, right), nil
	}
	if arrow.IsBaseBinary(left.ID()) && arrow.IsBaseBinary(right.ID()) {
		if left.ID() == arrow.LARGE_STRING || right.ID() == arrow.LARGE_STRING {
			return arrow.BinaryTypes.LargeString, nil
		}
		if left.ID() == arrow.LARGE_BINARY || right.ID() == arrow.LARGE_BINARY {
			return arrow.BinaryTypes.LargeBinary, nil
		}
		if left.ID() == arrow.BINARY || right.ID() == arrow.BINARY {
			return arrow.BinaryTypes.Binary, nil
		}
		return arrow.BinaryTypes.String, nil
	}
	return nil, fmt.Errorf("%w: cannot coerce %s and %s", ErrTypeMismatch, left, right)
}

func isNumericArrow(id arrow.Type) bool {
	return arrow.IsInteger(id) || arrow.IsFloating(id) || id == arrow.DECIMAL32 || id == arrow.DECIMAL64 || id == arrow.DECIMAL128 || id == arrow.DECIMAL256
}

func commonIntegerType(left, right arrow.DataType) arrow.DataType {
	leftUnsigned := arrow.IsUnsignedInteger(left.ID())
	rightUnsigned := arrow.IsUnsignedInteger(right.ID())
	if leftUnsigned != rightUnsigned {
		return arrow.PrimitiveTypes.Int64
	}
	bits := left.(arrow.FixedWidthDataType).BitWidth()
	if other := right.(arrow.FixedWidthDataType).BitWidth(); other > bits {
		bits = other
	}
	if bits <= 8 {
		if leftUnsigned {
			return arrow.PrimitiveTypes.Uint8
		}
		return arrow.PrimitiveTypes.Int8
	}
	if bits <= 16 {
		if leftUnsigned {
			return arrow.PrimitiveTypes.Uint16
		}
		return arrow.PrimitiveTypes.Int16
	}
	if bits <= 32 {
		if leftUnsigned {
			return arrow.PrimitiveTypes.Uint32
		}
		return arrow.PrimitiveTypes.Int32
	}
	if leftUnsigned {
		return arrow.PrimitiveTypes.Uint64
	}
	return arrow.PrimitiveTypes.Int64
}

func castArray(ctx context.Context, mem *quotaAllocator, values arrow.Array, target arrow.DataType) (arrow.Array, error) {
	if values == nil || arrow.TypeEqual(values.DataType(), target) {
		if values != nil {
			values.Retain()
		}
		return values, nil
	}
	result, err := compute.CastToType(compute.WithAllocator(ctx, mem), values, target)
	if err != nil {
		return nil, fmt.Errorf("%w: cast %s to %s: %v", ErrTypeMismatch, values.DataType(), target, err)
	}
	return result, nil
}

func coerceArrayPair(ctx context.Context, mem *quotaAllocator, left, right arrow.Array) (arrow.Array, arrow.Array, error) {
	target, err := commonArrowType(left.DataType(), right.DataType())
	if err != nil {
		return nil, nil, err
	}
	coercedLeft, err := castArray(ctx, mem, left, target)
	if err != nil {
		return nil, nil, err
	}
	coercedRight, err := castArray(ctx, mem, right, target)
	if err != nil {
		coercedLeft.Release()
		return nil, nil, err
	}
	return coercedLeft, coercedRight, nil
}

func scalarForType(value any, dtype arrow.DataType) (scalar.Scalar, error) {
	if dtype == nil {
		return nil, fmt.Errorf("%w: nil target type", ErrTypeMismatch)
	}
	if value == nil {
		return scalar.MakeNullScalar(dtype), nil
	}
	value = normalizeSeriesValue(value, dtype)
	result, err := scalar.MakeScalarParam(value, dtype)
	if err == nil && result != nil && arrow.TypeEqual(result.DataType(), dtype) {
		return result, nil
	}
	result = scalar.MakeScalar(value)
	if result == nil {
		return nil, fmt.Errorf("%w: unsupported scalar %T", ErrTypeMismatch, value)
	}
	casted, castErr := result.CastTo(dtype)
	if castErr != nil {
		return nil, fmt.Errorf("%w: cast literal %T to %s: %v", ErrTypeMismatch, value, dtype, castErr)
	}
	return casted, nil
}
