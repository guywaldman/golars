package benchmarks

import (
	"context"
	"fmt"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	golars "github.com/guywaldman/golars"
)

const (
	DefaultRows         int64 = 1_000_000
	DefaultRowGroupSize int64 = 64 * 1024
	DefaultCutoff       int64 = DefaultRows / 2
)

// Query names the workloads shared by the Go and Rust runners.
type Query string

const (
	FilterSelect       Query = "filter_select"
	GroupedAggregation Query = "grouped_aggregation"
)

func (q Query) Valid() bool {
	return q == FilterSelect || q == GroupedAggregation
}

// WriteDataset creates a deterministic Parquet input with sorted row-group
// statistics so the filter workload can exercise predicate pruning.
func WriteDataset(path string, rows, rowGroupSize int64) error {
	if path == "" {
		return fmt.Errorf("benchmarks: dataset path is required")
	}
	if rows <= 0 || rowGroupSize <= 0 {
		return fmt.Errorf("benchmarks: rows and row-group size must be positive")
	}
	mem := memory.NewGoAllocator()
	idBuilder := array.NewInt64Builder(mem)
	groupBuilder := array.NewInt64Builder(mem)
	valueBuilder := array.NewFloat64Builder(mem)
	textBuilder := array.NewStringBuilder(mem)
	defer idBuilder.Release()
	defer groupBuilder.Release()
	defer valueBuilder.Release()
	defer textBuilder.Release()
	for i := int64(0); i < rows; i++ {
		idBuilder.Append(i)
		groupBuilder.Append(i % 128)
		valueBuilder.Append(float64(i%10_000) / 10.0)
		textBuilder.Append(fmt.Sprintf("payload-%04d", i%10_000))
	}
	id := idBuilder.NewInt64Array()
	groupID := groupBuilder.NewInt64Array()
	value := valueBuilder.NewFloat64Array()
	text := textBuilder.NewStringArray()
	defer id.Release()
	defer groupID.Release()
	defer value.Release()
	defer text.Release()

	idSeries, err := golars.NewSeriesFromArrow("id", id)
	if err != nil {
		return err
	}
	defer idSeries.Close()
	groupSeries, err := golars.NewSeriesFromArrow("group_id", groupID)
	if err != nil {
		return err
	}
	defer groupSeries.Close()
	valueSeries, err := golars.NewSeriesFromArrow("value", value)
	if err != nil {
		return err
	}
	defer valueSeries.Close()
	textSeries, err := golars.NewSeriesFromArrow("payload", text)
	if err != nil {
		return err
	}
	defer textSeries.Close()
	df, err := golars.NewDataFrame(idSeries, groupSeries, valueSeries, textSeries)
	if err != nil {
		return err
	}
	defer df.Close()
	return golars.WriteParquet(
		context.Background(),
		df,
		path,
		golars.WithOverwrite(true),
		golars.WithWriteBatchSize(rowGroupSize),
	)
}

// Execute builds the same logical query used by both benchmark runners.
func Execute(ctx context.Context, engine *golars.Engine, path string, query Query, cutoff int64) (*golars.DataFrame, error) {
	if !query.Valid() {
		return nil, fmt.Errorf("benchmarks: unknown query %q", query)
	}
	plan := engine.ScanParquet(path).Filter(golars.Col("id").Ge(golars.Lit(cutoff)))
	switch query {
	case FilterSelect:
		plan = plan.Select(golars.Col("group_id"), golars.Col("value"))
	case GroupedAggregation:
		plan = plan.GroupBy(golars.Col("group_id")).Agg(golars.Col("value").Sum().Alias("total"))
	}
	defer plan.Close()
	return plan.Collect(ctx)
}
