package core

import (
	"context"
	"path/filepath"
	"testing"
	"time"
)

func TestCoreSmoke(t *testing.T) {
	ids, err := NewSeries("id", []int64{1, 2, 3})
	if err != nil {
		t.Fatal(err)
	}
	defer ids.Close()
	values, err := NewSeries("value", []int64{10, 20, 30})
	if err != nil {
		t.Fatal(err)
	}
	defer values.Close()
	df, err := NewDataFrame(ids, values)
	if err != nil {
		t.Fatal(err)
	}
	defer df.Close()
	out, err := df.Lazy().Filter(Col("value").Gt(Lit(int64(10)))).Select(Col("id"), Col("value").Add(Lit(int64(1))).Alias("next")).Collect(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer out.Close()
	if out.Height() != 2 || out.Width() != 2 {
		t.Fatalf("shape=(%d,%d)", out.Height(), out.Width())
	}
	grouped, err := df.GroupBy(Col("id").Sub(Lit(int64(1))).Alias("bucket")).Agg(Col("value").Sum().Alias("total"))
	if err != nil {
		t.Fatal(err)
	}
	defer grouped.Close()
	if grouped.Height() != 3 {
		t.Fatalf("group height=%d", grouped.Height())
	}
	path := filepath.Join(t.TempDir(), "data.csv")
	if err := WriteCSV(context.Background(), df, path); err != nil {
		t.Fatal(err)
	}
	read, err := ReadCSV(context.Background(), path)
	if err != nil {
		t.Fatal(err)
	}
	defer read.Close()
	if read.Height() != 3 || read.Width() != 2 {
		t.Fatalf("read shape=(%d,%d)", read.Height(), read.Width())
	}
	csvProjected, err := ScanCSV(path).
		Filter(Col("value").Gt(Lit(int64(10)))).
		Select(Col("id")).
		Collect(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer csvProjected.Close()
	if csvProjected.Height() != 2 || csvProjected.Width() != 1 {
		t.Fatalf("projected CSV shape=(%d,%d)", csvProjected.Height(), csvProjected.Width())
	}
	parquetPath := filepath.Join(t.TempDir(), "data.parquet")
	if err := WriteParquet(context.Background(), df, parquetPath); err != nil {
		t.Fatal(err)
	}
	parquetDF, err := ReadParquet(context.Background(), parquetPath)
	if err != nil {
		t.Fatal(err)
	}
	defer parquetDF.Close()
	if parquetDF.Height() != 3 || parquetDF.Width() != 2 {
		t.Fatalf("parquet shape=(%d,%d)", parquetDF.Height(), parquetDF.Width())
	}
	optimized, err := ScanParquet(parquetPath).
		Filter(Col("value").Gt(Lit(int64(10)))).
		Select(Col("id")).
		Collect(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer optimized.Close()
	if optimized.Height() != 2 || optimized.Width() != 1 {
		t.Fatalf("optimized shape=(%d,%d)", optimized.Height(), optimized.Width())
	}
	ndjsonPath := filepath.Join(t.TempDir(), "data.ndjson")
	if err := WriteNDJSON(context.Background(), df, ndjsonPath); err != nil {
		t.Fatal(err)
	}
	ndjsonDF, err := ReadNDJSON(context.Background(), ndjsonPath)
	if err != nil {
		t.Fatal(err)
	}
	defer ndjsonDF.Close()
	if ndjsonDF.Height() != 3 || ndjsonDF.Width() != 2 {
		t.Fatalf("ndjson shape=(%d,%d)", ndjsonDF.Height(), ndjsonDF.Width())
	}
	ndjsonProjected, err := ScanNDJSON(ndjsonPath).Select(Col("id")).Collect(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer ndjsonProjected.Close()
	if ndjsonProjected.Height() != 3 || ndjsonProjected.Width() != 1 {
		t.Fatalf("projected NDJSON shape=(%d,%d)", ndjsonProjected.Height(), ndjsonProjected.Width())
	}
	jsonPath := filepath.Join(t.TempDir(), "data.json")
	if err := WriteJSON(context.Background(), df, jsonPath); err != nil {
		t.Fatal(err)
	}
	jsonDF, err := ReadJSON(context.Background(), jsonPath)
	if err != nil {
		t.Fatal(err)
	}
	defer jsonDF.Close()
	if jsonDF.Height() != 3 || jsonDF.Width() != 2 {
		t.Fatalf("json shape=(%d,%d)", jsonDF.Height(), jsonDF.Width())
	}
	jsonProjected, err := ScanJSON(jsonPath).Select(Col("id")).Collect(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer jsonProjected.Close()
	if jsonProjected.Height() != 3 || jsonProjected.Width() != 1 {
		t.Fatalf("projected JSON shape=(%d,%d)", jsonProjected.Height(), jsonProjected.Width())
	}
	ipcPath := filepath.Join(t.TempDir(), "data.arrow")
	if err := WriteIPCFile(context.Background(), df, ipcPath); err != nil {
		t.Fatal(err)
	}
	ipcDF, err := ReadIPCFile(context.Background(), ipcPath)
	if err != nil {
		t.Fatal(err)
	}
	defer ipcDF.Close()
	if ipcDF.Height() != 3 || ipcDF.Width() != 2 {
		t.Fatalf("ipc shape=(%d,%d)", ipcDF.Height(), ipcDF.Width())
	}
	ipcStreamPath := filepath.Join(t.TempDir(), "data.arrows")
	if err := WriteIPCStream(context.Background(), df, ipcStreamPath); err != nil {
		t.Fatal(err)
	}
	ipcStreamDF, err := ScanIPCStream(ipcStreamPath).Collect(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer ipcStreamDF.Close()
	if ipcStreamDF.Height() != 3 || ipcStreamDF.Width() != 2 {
		t.Fatalf("ipc stream shape=(%d,%d)", ipcStreamDF.Height(), ipcStreamDF.Width())
	}
	empty, err := df.Lazy().Filter(Col("value").Gt(Lit(int64(100)))).Select(Col("id"), Col("value").Alias("v")).Collect(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer empty.Close()
	if empty.Height() != 0 || empty.Width() != 2 {
		t.Fatalf("empty shape=(%d,%d)", empty.Height(), empty.Width())
	}
	emptyLimited, err := df.Lazy().Limit(0).Select(Col("id")).Collect(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer emptyLimited.Close()
	if emptyLimited.Height() != 0 || emptyLimited.Width() != 1 {
		t.Fatalf("empty limited shape=(%d,%d)", emptyLimited.Height(), emptyLimited.Width())
	}
	sorted, err := df.Lazy().Sort(Col("value")).Collect(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer sorted.Close()
	if sorted.Height() != 3 {
		t.Fatalf("sorted height=%d", sorted.Height())
	}
	lazyGrouped, err := df.Lazy().GroupBy(Col("id")).Agg(Col("value").Count()).Collect(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer lazyGrouped.Close()
	if lazyGrouped.Height() != 3 {
		t.Fatalf("lazy group height=%d", lazyGrouped.Height())
	}
	emptyGrouped, err := df.Lazy().Filter(Col("id").Gt(Lit(int64(100)))).GroupBy(Col("id")).Agg(Col("value").Sum()).Collect(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer emptyGrouped.Close()
	if emptyGrouped.Height() != 0 || emptyGrouped.Width() != 2 {
		t.Fatalf("empty grouped shape=(%d,%d)", emptyGrouped.Height(), emptyGrouped.Width())
	}
	names, err := NewSeries("name", []string{"Alpha", "beta", "gamma"})
	if err != nil {
		t.Fatal(err)
	}
	defer names.Close()
	nameDF, err := NewDataFrame(names)
	if err != nil {
		t.Fatal(err)
	}
	defer nameDF.Close()
	matched, err := nameDF.Filter(Col("name").Str().Contains(Lit("mm")))
	if err != nil {
		t.Fatal(err)
	}
	defer matched.Close()
	if matched.Height() != 1 {
		t.Fatalf("string filter height=%d", matched.Height())
	}
	stringGrouped, err := nameDF.GroupBy(Col("name")).Agg(Col("name").First())
	if err != nil {
		t.Fatal(err)
	}
	defer stringGrouped.Close()
	if stringGrouped.Height() != 3 {
		t.Fatalf("string group height=%d", stringGrouped.Height())
	}
	times, err := NewSeries("ts", []time.Time{time.Unix(1, 0).UTC(), time.Unix(2, 0).UTC()})
	if err != nil {
		t.Fatal(err)
	}
	defer times.Close()
	timeDF, err := NewDataFrame(times)
	if err != nil {
		t.Fatal(err)
	}
	defer timeDF.Close()
	if _, err := timeDF.Filter(Col("ts").Gt(Lit(time.Unix(1, 0).UTC()))); err != nil {
		t.Fatal(err)
	}
	calendar, err := timeDF.Select(Col("ts").Dt().Year().Alias("year"))
	if err != nil {
		t.Fatal(err)
	}
	defer calendar.Close()
	if calendar.Width() != 1 || calendar.Height() != 2 {
		t.Fatalf("calendar shape=(%d,%d)", calendar.Height(), calendar.Width())
	}
	datasetPath := filepath.Join(t.TempDir(), "dataset")
	if err := WriteParquetDataset(context.Background(), df, datasetPath, WithPartitionBy("id")); err != nil {
		t.Fatal(err)
	}
	datasetDF, err := ScanParquet(filepath.Join(datasetPath, "*") + "/*.parquet").Collect(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer datasetDF.Close()
	if datasetDF.Height() != 3 || datasetDF.Width() != 2 {
		t.Fatalf("dataset shape=(%d,%d)", datasetDF.Height(), datasetDF.Width())
	}
	if err := WriteParquetDataset(context.Background(), df, datasetPath, WithPartitionBy("id"), WithDatasetMode(DatasetAppend)); err != nil {
		t.Fatal(err)
	}
	appended, err := ScanParquet(filepath.Join(datasetPath, "*") + "/*.parquet").Collect(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer appended.Close()
	if appended.Height() != 6 {
		t.Fatalf("appended dataset height=%d", appended.Height())
	}
}
