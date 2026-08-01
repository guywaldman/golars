package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"runtime"
	"time"

	golars "github.com/guywaldman/golars"
	"github.com/guywaldman/golars/benchmarks"
)

type result struct {
	Implementation  string `json:"implementation"`
	Query           string `json:"query"`
	Iterations      int    `json:"iterations"`
	Rows            int64  `json:"rows"`
	ElapsedNS       int64  `json:"elapsed_ns"`
	AvgNS           int64  `json:"avg_ns"`
	TotalAllocBytes uint64 `json:"total_alloc_bytes"`
	PeakHeapBytes   uint64 `json:"peak_heap_bytes"`
	MaxRSSBytes     uint64 `json:"max_rss_bytes"`
}

func main() {
	dataset := flag.String("dataset", "", "Parquet input path")
	queryName := flag.String("query", string(benchmarks.FilterSelect), "benchmark query")
	cutoff := flag.Int64("cutoff", benchmarks.DefaultCutoff, "inclusive id filter cutoff")
	batchSize := flag.Int64("batch-size", benchmarks.DefaultRowGroupSize, "execution batch size")
	iterations := flag.Int("iterations", 5, "measured iterations")
	warmup := flag.Int("warmup", 1, "warmup iterations")
	flag.Parse()
	if *dataset == "" {
		fatal(fmt.Errorf("-dataset is required"))
	}
	query := benchmarks.Query(*queryName)
	if !query.Valid() {
		fatal(fmt.Errorf("unknown query %q", *queryName))
	}
	if *iterations <= 0 || *warmup < 0 || *batchSize <= 0 {
		fatal(fmt.Errorf("iterations and batch size must be positive; warmup cannot be negative"))
	}
	if _, err := os.Stat(*dataset); err != nil {
		fatal(err)
	}
	engine, err := golars.NewEngine(
		golars.WithBatchSize(*batchSize),
		golars.WithParallelism(runtime.GOMAXPROCS(0)),
	)
	if err != nil {
		fatal(err)
	}
	defer engine.Close()
	ctx := context.Background()
	for i := 0; i < *warmup; i++ {
		df, runErr := benchmarks.Execute(ctx, engine, *dataset, query, *cutoff)
		if runErr != nil {
			fatal(runErr)
		}
		df.Close()
	}
	runtime.GC()
	var before runtime.MemStats
	runtime.ReadMemStats(&before)
	var total time.Duration
	var rows int64
	var peakHeap uint64 = before.HeapAlloc
	for i := 0; i < *iterations; i++ {
		started := time.Now()
		df, runErr := benchmarks.Execute(ctx, engine, *dataset, query, *cutoff)
		if runErr != nil {
			fatal(runErr)
		}
		total += time.Since(started)
		rows += df.Height()
		df.Close()
		var sample runtime.MemStats
		runtime.ReadMemStats(&sample)
		if sample.HeapAlloc > peakHeap {
			peakHeap = sample.HeapAlloc
		}
	}
	var after runtime.MemStats
	runtime.ReadMemStats(&after)
	output := result{
		Implementation:  "golars",
		Query:           string(query),
		Iterations:      *iterations,
		Rows:            rows / int64(*iterations),
		ElapsedNS:       total.Nanoseconds(),
		AvgNS:           total.Nanoseconds() / int64(*iterations),
		TotalAllocBytes: after.TotalAlloc - before.TotalAlloc,
		PeakHeapBytes:   peakHeap,
		MaxRSSBytes:     maxRSSBytes(),
	}
	encoded, err := json.Marshal(output)
	if err != nil {
		fatal(err)
	}
	fmt.Println(string(encoded))
}

func fatal(err error) {
	fmt.Fprintln(os.Stderr, err)
	os.Exit(1)
}
