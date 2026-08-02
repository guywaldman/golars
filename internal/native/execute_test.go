package native_test

import (
	"bytes"
	"context"
	"encoding/json"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/guywaldman/golars"
	nativebackend "github.com/guywaldman/golars/internal/native"
)

func TestExecuteFrameReturnsArrowIPC(t *testing.T) {
	nativeRuntime, err := nativebackend.Open()
	if err != nil {
		t.Skipf("native backend unavailable: %v", err)
	}
	values, err := golars.NewSeries("value", []int64{3, 1, 2})
	if err != nil {
		t.Fatal(err)
	}
	df, err := golars.NewDataFrame(values)
	if err != nil {
		t.Fatal(err)
	}
	defer df.Close()
	path := t.TempDir() + "/input.parquet"
	if err := golars.WriteParquet(context.Background(), df, path, golars.WithOverwrite(true)); err != nil {
		t.Fatal(err)
	}

	request, err := json.Marshal(map[string]any{
		"path":   path,
		"format": "parquet",
	})
	if err != nil {
		t.Fatal(err)
	}
	frame, err := nativeRuntime.ExecuteFrame(request)
	if err != nil {
		t.Fatal(err)
	}
	defer frame.Close()
	data, err := frame.IPC()
	if err != nil {
		t.Fatal(err)
	}
	reader, err := ipc.NewFileReader(bytes.NewReader(data))
	if err != nil {
		t.Fatalf("open native IPC result: %v", err)
	}
	defer reader.Close()
	if got := reader.Schema().Field(0).Name; got != "value" {
		t.Fatalf("result column = %q, want value", got)
	}
	var rows int64
	for index := 0; index < reader.NumRecords(); index++ {
		batch, err := reader.RecordBatch(index)
		if err != nil {
			t.Fatal(err)
		}
		rows += batch.NumRows()
		batch.Release()
	}
	if rows != 3 {
		t.Fatalf("result rows = %d, want 3", rows)
	}
}

func TestExecuteRequestAppliesFilterAndProjection(t *testing.T) {
	nativeRuntime, err := nativebackend.Open()
	if err != nil {
		t.Skipf("native backend unavailable: %v", err)
	}
	id, err := golars.NewSeries("id", []int64{1, 2, 3})
	if err != nil {
		t.Fatal(err)
	}
	defer id.Close()
	value, err := golars.NewSeries("value", []int64{10, 20, 30})
	if err != nil {
		t.Fatal(err)
	}
	defer value.Close()
	df, err := golars.NewDataFrame(id, value)
	if err != nil {
		t.Fatal(err)
	}
	defer df.Close()
	path := t.TempDir() + "/input.parquet"
	if err := golars.WriteParquet(context.Background(), df, path, golars.WithOverwrite(true)); err != nil {
		t.Fatal(err)
	}
	request, err := json.Marshal(map[string]any{
		"path":   path,
		"format": "parquet",
		"operations": []map[string]any{
			{
				"kind": "filter",
				"exprs": []map[string]any{
					{
						"kind": "call",
						"name": "greater",
						"args": []map[string]any{
							{"kind": "column", "name": "id"},
							{"kind": "literal", "literal": map[string]any{"kind": "int", "value": 1}},
						},
					},
				},
			},
			{
				"kind":  "select",
				"exprs": []map[string]any{{"kind": "column", "name": "value"}},
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	frame, err := nativeRuntime.ExecuteFrame(request)
	if err != nil {
		t.Fatal(err)
	}
	defer frame.Close()
	data, err := frame.IPC()
	if err != nil {
		t.Fatal(err)
	}
	reader, err := ipc.NewFileReader(bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	if got := reader.Schema().Field(0).Name; got != "value" {
		t.Fatalf("result column = %q, want value", got)
	}
	var rows int64
	for index := 0; index < reader.NumRecords(); index++ {
		batch, err := reader.RecordBatch(index)
		if err != nil {
			t.Fatal(err)
		}
		rows += batch.NumRows()
		batch.Release()
	}
	if rows != 2 {
		t.Fatalf("result rows = %d, want 2", rows)
	}
}
