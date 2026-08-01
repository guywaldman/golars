package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"

	"github.com/guywaldman/golars/benchmarks"
)

func main() {
	output := flag.String("output", "benchmarks/data.parquet", "Parquet output path")
	rows := flag.Int64("rows", benchmarks.DefaultRows, "number of rows")
	rowGroupSize := flag.Int64("row-group-size", benchmarks.DefaultRowGroupSize, "rows per Parquet row group")
	flag.Parse()
	if err := os.MkdirAll(filepath.Dir(*output), 0o755); err != nil {
		fatal(err)
	}
	if err := benchmarks.WriteDataset(*output, *rows, *rowGroupSize); err != nil {
		fatal(err)
	}
	fmt.Printf("wrote %s (%d rows, row groups of %d)\n", *output, *rows, *rowGroupSize)
}
func fatal(err error) {
	fmt.Fprintln(os.Stderr, err)
	os.Exit(1)
}
