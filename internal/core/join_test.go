package core

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestJoinVariantsAndComputedKeys(t *testing.T) {
	leftID, err := NewSeries("id", []int64{1, 2, 2, 4})
	require.NoError(t, err)
	defer leftID.Close()
	leftValue, err := NewSeries("value", []string{"a", "b", "c", "d"})
	require.NoError(t, err)
	defer leftValue.Close()
	left, err := NewDataFrame(leftID, leftValue)
	require.NoError(t, err)
	defer left.Close()

	rightID, err := NewSeries("id", []int64{2, 3, 4})
	require.NoError(t, err)
	defer rightID.Close()
	rightValue, err := NewSeries("score", []int64{20, 30, 40})
	require.NoError(t, err)
	defer rightValue.Close()
	right, err := NewDataFrame(rightID, rightValue)
	require.NoError(t, err)
	defer right.Close()

	joined, err := left.Join(right, []Expr{Col("id")}, []Expr{Col("id")})
	require.NoError(t, err)
	defer joined.Close()
	require.Equal(t, int64(3), joined.Height())
	require.Equal(t, 4, joined.Width())
	require.Equal(t, []string{"id", "value", "id_right", "score"}, joined.Schema().Names())

	semi, err := left.Join(right, []Expr{Col("id")}, []Expr{Col("id")}, WithJoinType(JoinSemi))
	require.NoError(t, err)
	defer semi.Close()
	require.Equal(t, int64(3), semi.Height())
	require.Equal(t, 2, semi.Width())

	anti, err := left.Join(right, []Expr{Col("id")}, []Expr{Col("id")}, WithJoinType(JoinAnti))
	require.NoError(t, err)
	defer anti.Close()
	require.Equal(t, int64(1), anti.Height())

	computed, err := left.Lazy().Join(right.Lazy(), []Expr{Col("id").Add(Lit(int64(1)))}, []Expr{Col("id")}, WithJoinType(JoinLeft)).Collect(context.Background())
	require.NoError(t, err)
	defer computed.Close()
	require.Equal(t, int64(4), computed.Height())
}
