package golars

import (
	"context"
	"fmt"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/compute"
	"github.com/apache/arrow-go/v18/arrow/scalar"
	"github.com/guywaldman/golars/internal/arrowutil"
)

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
		return nil, true, &ColumnNotFoundError{Name: columnNode.name, Available: arrowutil.SchemaNames(batch.Schema())}
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
