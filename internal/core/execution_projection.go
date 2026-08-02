package core

import (
	"context"
	"math"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/guywaldman/golars/internal/arrowutil"
)

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
			arrowutil.ReleaseArrays(columns)
			return nil, 0, err
		}
		columns = append(columns, col)
		fields = append(fields, arrow.Field{Name: expr.outputName(), Type: col.DataType(), Nullable: true})
	}
	schema := arrow.NewSchema(fields, nil)
	out := array.NewRecordBatch(schema, columns, batch.NumRows())
	arrowutil.ReleaseArrays(columns)
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
			arrowutil.ReleaseArrays(columns)
			return nil, 0, err
		}
		name := expr.outputName()
		indices := arrowutil.FieldIndices(fields, name)
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
	arrowutil.ReleaseArrays(columns)
	return out, math.MaxInt64, nil
}
