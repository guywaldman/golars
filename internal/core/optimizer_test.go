package core

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestOptimizerRewritesPlanWithoutChangingResult(t *testing.T) {
	id, err := NewSeries("id", []int64{1, 2, 3, 4})
	require.NoError(t, err)
	defer id.Close()
	value, err := NewSeries("value", []int64{10, 20, 30, 40})
	require.NoError(t, err)
	defer value.Close()
	df, err := NewDataFrame(id, value)
	require.NoError(t, err)
	defer df.Close()

	plan := df.Lazy().
		Select(Col("id").Alias("key"), Col("value")).
		Filter(Col("key").Gt(Lit(int64(1)))).
		Filter(Col("key").Lt(Lit(int64(4)))).
		Limit(10)
	defer plan.Close()

	explain := plan.ExplainOptimized()
	require.Contains(t, explain, "LIMIT 10\nSELECT")
	require.Equal(t, 1, strings.Count(explain, "FILTER"))
	result, err := plan.Collect(nil)
	require.NoError(t, err)
	defer result.Close()
	require.Equal(t, int64(2), result.Height())
}
