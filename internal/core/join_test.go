package core

import (
	"context"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/scalar"
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

	leftJoin, err := left.Join(right, []Expr{Col("id")}, []Expr{Col("id")}, WithJoinType(JoinLeft))
	require.NoError(t, err)
	defer leftJoin.Close()
	score, err := leftJoin.Column("score")
	require.NoError(t, err)
	defer score.Close()
	chunked := score.ArrowChunked()
	defer chunked.Release()
	chunk := chunked.Chunk(0)
	first, err := scalar.GetScalar(chunk, 0)
	require.NoError(t, err)
	require.False(t, first.IsValid())
	second, err := scalar.GetScalar(chunk, 1)
	require.NoError(t, err)
	require.Equal(t, int64(20), second.(*scalar.Int64).Value)
}

func TestJoinStringKeysPreservesLeftOrder(t *testing.T) {
	leftKey, err := NewSeries("key", []string{"b", "a", "b"})
	require.NoError(t, err)
	defer leftKey.Close()
	leftValue, err := NewSeries("value", []int64{1, 2, 3})
	require.NoError(t, err)
	defer leftValue.Close()
	left, err := NewDataFrame(leftKey, leftValue)
	require.NoError(t, err)
	defer left.Close()

	rightKey, err := NewSeries("key", []string{"a", "b"})
	require.NoError(t, err)
	defer rightKey.Close()
	rightValue, err := NewSeries("score", []int64{10, 20})
	require.NoError(t, err)
	defer rightValue.Close()
	right, err := NewDataFrame(rightKey, rightValue)
	require.NoError(t, err)
	defer right.Close()

	joined, err := left.Join(right, []Expr{Col("key")}, []Expr{Col("key")})
	require.NoError(t, err)
	defer joined.Close()
	require.Equal(t, int64(3), joined.Height())

	values, err := joined.Column("value")
	require.NoError(t, err)
	defer values.Close()
	chunked := values.ArrowChunked()
	defer chunked.Release()
	chunk := chunked.Chunk(0)
	got := make([]int64, chunk.Len())
	for i := range got {
		value, valueErr := scalar.GetScalar(chunk, i)
		require.NoError(t, valueErr)
		got[i] = value.(*scalar.Int64).Value
	}
	require.Equal(t, []int64{1, 2, 3}, got)
}
