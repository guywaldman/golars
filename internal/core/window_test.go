package core

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow/scalar"
	"github.com/stretchr/testify/require"
)

func TestWindowAggregatesAndRanks(t *testing.T) {
	group, err := NewSeries("group", []int64{1, 1, 2, 2})
	require.NoError(t, err)
	defer group.Close()
	value, err := NewSeries("value", []int64{3, 1, 5, 2})
	require.NoError(t, err)
	defer value.Close()
	df, err := NewDataFrame(group, value)
	require.NoError(t, err)
	defer df.Close()

	result, err := df.Select(
		Col("group"),
		Col("value").Sum().Over(Col("group")).Alias("total"),
		Col("value").Rank().Over(Col("group")).Alias("rank"),
		Col("value").CumSum().Over(Col("group")).Alias("running"),
	)
	require.NoError(t, err)
	defer result.Close()
	require.Equal(t, int64(4), result.Height())
	require.Equal(t, []int64{4, 4, 7, 7}, int64Column(t, result, "total"))
	require.Equal(t, []int64{2, 1, 2, 1}, int64Column(t, result, "rank"))
	require.Equal(t, []int64{3, 4, 5, 7}, int64Column(t, result, "running"))
}

func TestWindowFloat64SumPreservesRowOrder(t *testing.T) {
	group, err := NewSeries("group", []string{"b", "a", "b", "a"})
	require.NoError(t, err)
	defer group.Close()
	value, err := NewSeries("value", []float64{1.5, 2.0, 3.0, 4.0})
	require.NoError(t, err)
	defer value.Close()
	df, err := NewDataFrame(group, value)
	require.NoError(t, err)
	defer df.Close()

	result, err := df.Select(
		Col("group"),
		Col("value").Sum().Over(Col("group")).Alias("total"),
	)
	require.NoError(t, err)
	defer result.Close()
	require.Equal(t, []float64{4.5, 6.0, 4.5, 6.0}, float64Column(t, result, "total"))
}

func int64Column(t *testing.T, df *DataFrame, name string) []int64 {
	t.Helper()
	series, err := df.Column(name)
	require.NoError(t, err)
	defer series.Close()
	chunked := series.ArrowChunked()
	defer chunked.Release()
	result := make([]int64, 0, series.Len())
	for _, chunk := range chunked.Chunks() {
		for i := 0; i < chunk.Len(); i++ {
			value, err := scalar.GetScalar(chunk, i)
			require.NoError(t, err)
			result = append(result, value.(*scalar.Int64).Value)
		}
	}
	return result
}

func float64Column(t *testing.T, df *DataFrame, name string) []float64 {
	t.Helper()
	series, err := df.Column(name)
	require.NoError(t, err)
	defer series.Close()
	chunked := series.ArrowChunked()
	defer chunked.Release()
	result := make([]float64, 0, series.Len())
	for _, chunk := range chunked.Chunks() {
		for i := 0; i < chunk.Len(); i++ {
			value, err := scalar.GetScalar(chunk, i)
			require.NoError(t, err)
			result = append(result, value.(*scalar.Float64).Value)
		}
	}
	return result
}
