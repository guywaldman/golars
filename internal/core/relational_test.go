package core

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestUniqueExplodeAndUnnest(t *testing.T) {
	ids, err := NewSeries("id", []int64{1, 1, 2})
	require.NoError(t, err)
	defer ids.Close()
	items, err := NewSeries("items", [][]int64{{1, 2}, {3}, nil})
	require.NoError(t, err)
	defer items.Close()
	df, err := NewDataFrame(ids, items)
	require.NoError(t, err)
	defer df.Close()

	unique, err := df.Unique("id")
	require.NoError(t, err)
	require.Equal(t, int64(2), unique.Height())
	unique.Close()

	exploded, err := df.Explode("items")
	require.NoError(t, err)
	defer exploded.Close()
	require.Equal(t, int64(4), exploded.Height())

	type point struct {
		X int64 `compute:"x"`
		Y int64 `compute:"y"`
	}
	points, err := NewSeries("point", []point{{X: 1, Y: 2}, {X: 3, Y: 4}})
	require.NoError(t, err)
	defer points.Close()
	pointDF, err := NewDataFrame(points)
	require.NoError(t, err)
	defer pointDF.Close()
	unnested, err := pointDF.Unnest("point")
	require.NoError(t, err)
	defer unnested.Close()
	require.Equal(t, []string{"x", "y"}, unnested.Schema().Names())
}
