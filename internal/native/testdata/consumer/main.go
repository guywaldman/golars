package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"

	"github.com/guywaldman/golars"
)

func main() {
	values, err := golars.NewSeries("value", []int64{1, 2, 3})
	if err != nil {
		panic(err)
	}
	df, err := golars.NewDataFrame(values)
	if err != nil {
		panic(err)
	}
	defer df.Close()
	directory, err := os.MkdirTemp("", "golars-consumer-")
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(directory)
	path := filepath.Join(directory, "input.parquet")
	if err := golars.WriteParquet(context.Background(), df, path); err != nil {
		panic(err)
	}
	result, err := golars.ScanParquet(path).
		Filter(golars.Col("value").Gt(golars.Lit(int64(1)))).
		Collect(context.Background())
	if err != nil {
		panic(err)
	}
	defer result.Close()
	fmt.Println(result.Shape())
}
