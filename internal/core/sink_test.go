package core

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestLazySinksPreserveStreamingFormats(t *testing.T) {
	id, err := NewSeries("id", []int64{1, 2, 3})
	require.NoError(t, err)
	defer id.Close()
	value, err := NewSeries("value", []string{"a", "b", "c"})
	require.NoError(t, err)
	defer value.Close()
	df, err := NewDataFrame(id, value)
	require.NoError(t, err)
	defer df.Close()
	ctx := context.Background()
	plan := df.Lazy().Filter(Col("id").Gt(Lit(int64(1))))
	defer plan.Close()

	csvPath := filepath.Join(t.TempDir(), "out.csv")
	require.NoError(t, plan.Sink(ctx, csvPath))
	csv, err := ReadCSV(ctx, csvPath)
	require.NoError(t, err)
	require.Equal(t, int64(2), csv.Height())
	csv.Close()

	jsonPath := filepath.Join(t.TempDir(), "out.json")
	require.NoError(t, plan.Sink(ctx, jsonPath))
	json, err := ReadJSON(ctx, jsonPath)
	require.NoError(t, err)
	require.Equal(t, int64(2), json.Height())
	json.Close()

	ipcPath := filepath.Join(t.TempDir(), "out.arrow")
	require.NoError(t, plan.Sink(ctx, ipcPath))
	ipc, err := ReadIPCFile(ctx, ipcPath)
	require.NoError(t, err)
	require.Equal(t, int64(2), ipc.Height())
	ipc.Close()
}
