package core

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestConditionalAndPredicateExpressions(t *testing.T) {
	id, err := NewSeries("id", []int64{1, 2, 3, 4})
	require.NoError(t, err)
	defer id.Close()
	df, err := NewDataFrame(id)
	require.NoError(t, err)
	defer df.Close()

	result, err := df.Select(
		When(Col("id").IsBetween(Lit(int64(2)), Lit(int64(3)))).Then(Lit("yes")).Otherwise(Lit("no")).Alias("label"),
		Col("id").IsIn(Lit([]int64{1, 3})).Alias("selected"),
	)
	require.NoError(t, err)
	defer result.Close()
	require.Equal(t, int64(4), result.Height())

	filtered, err := df.Filter(Col("id").IsIn(Lit([]int64{2, 4})))
	require.NoError(t, err)
	defer filtered.Close()
	require.Equal(t, int64(2), filtered.Height())
}
