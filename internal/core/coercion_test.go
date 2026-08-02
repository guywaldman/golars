package core

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/stretchr/testify/require"
)

func TestArrowTypesAndNumericCoercion(t *testing.T) {
	require.True(t, Float64.IsNumeric())
	require.True(t, Decimal128(20, 4).IsNumeric())
	require.True(t, ListOf(Int32).IsNested())
	require.True(t, Timestamp(1, "UTC").IsTemporal())
	require.True(t, Categorical.IsNested())
	require.True(t, UInt32.Equal(Uint32))

	series, err := NewSeries("id", []int32{1, 2, 3})
	require.NoError(t, err)
	defer series.Close()
	df, err := NewDataFrame(series)
	require.NoError(t, err)
	defer df.Close()
	filtered, err := df.Filter(Col("id").Gt(Lit(int64(1))))
	require.NoError(t, err)
	defer filtered.Close()
	require.Equal(t, int64(2), filtered.Height())
	list, err := NewSeries("items", [][]int64{{1, 2}, {3}})
	require.NoError(t, err)
	defer list.Close()
	require.True(t, list.DataType().Equal(ListOf(Int64)))
	type point struct {
		X int64 `compute:"x"`
		Y int64 `compute:"y"`
	}
	structs, err := NewSeries("point", []point{{X: 1, Y: 2}})
	require.NoError(t, err)
	defer structs.Close()
	require.Equal(t, arrow.StructOf(arrow.Field{Name: "x", Type: arrow.PrimitiveTypes.Int64, Nullable: true}, arrow.Field{Name: "y", Type: arrow.PrimitiveTypes.Int64, Nullable: true}), structs.DataType().ArrowType())
}
