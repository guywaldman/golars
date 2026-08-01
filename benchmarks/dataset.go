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
	ProjectionOnly     Query = "projection_only"
	FilterSelect       Query = "filter_select"
	CompoundFilter     Query = "compound_filter"
	DerivedProjection  Query = "derived_projection"
	GroupedAggregation Query = "grouped_aggregation"
	MultiAggregation   Query = "multi_aggregation"
	StringFilter       Query = "string_filter"
)

func (q Query) Valid() bool {
	switch q {
	case ProjectionOnly, FilterSelect, CompoundFilter, DerivedProjection,
		GroupedAggregation, MultiAggregation, StringFilter:
		return true
	default:
		return false
	}
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
	quantityBuilder := array.NewInt64Builder(mem)
	eventDayBuilder := array.NewInt64Builder(mem)
	tenantBuilder := array.NewStringBuilder(mem)
	textBuilder := array.NewStringBuilder(mem)
	defer func() {
		idBuilder.Release()
		groupBuilder.Release()
		valueBuilder.Release()
		quantityBuilder.Release()
		eventDayBuilder.Release()
		tenantBuilder.Release()
		textBuilder.Release()
	}()
	tenants := make([]string, 32)
	for i := range tenants {
		tenants[i] = fmt.Sprintf("tenant-%02d", i)
	}
	for i := int64(0); i < rows; i++ {
		idBuilder.Append(i)
		groupBuilder.Append(i % 128)
		valueBuilder.Append(float64(i%10_000) / 10.0)
		quantityBuilder.Append((i % 20) + 1)
		eventDayBuilder.Append(i / 1_000)
		tenantBuilder.Append(tenants[i%int64(len(tenants))])
		textBuilder.Append(fmt.Sprintf("payload-%04d", i%10_000))
	}
	id := idBuilder.NewInt64Array()
	groupID := groupBuilder.NewInt64Array()
	value := valueBuilder.NewFloat64Array()
	quantity := quantityBuilder.NewInt64Array()
	eventDay := eventDayBuilder.NewInt64Array()
	tenant := tenantBuilder.NewStringArray()
	text := textBuilder.NewStringArray()
	defer id.Release()
	defer groupID.Release()
	defer value.Release()
	defer quantity.Release()
	defer eventDay.Release()
	defer tenant.Release()
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
	quantitySeries, err := golars.NewSeriesFromArrow("quantity", quantity)
	if err != nil {
		return err
	}
	defer quantitySeries.Close()
	eventDaySeries, err := golars.NewSeriesFromArrow("event_day", eventDay)
	if err != nil {
		return err
	}
	defer eventDaySeries.Close()
	tenantSeries, err := golars.NewSeriesFromArrow("tenant", tenant)
	if err != nil {
		return err
	}
	defer tenantSeries.Close()
	textSeries, err := golars.NewSeriesFromArrow("payload", text)
	if err != nil {
		return err
	}
	defer textSeries.Close()
	df, err := golars.NewDataFrame(idSeries, groupSeries, valueSeries, quantitySeries, eventDaySeries, tenantSeries, textSeries)
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
	switch query {
	case ProjectionOnly:
		return engine.ScanParquet(path).
			Select(golars.Col("group_id"), golars.Col("value"), golars.Col("payload")).
			Collect(ctx)
	case FilterSelect:
		return engine.ScanParquet(path).
			Filter(golars.Col("id").Ge(golars.Lit(cutoff))).
			Select(golars.Col("group_id"), golars.Col("value")).
			Collect(ctx)
	case CompoundFilter:
		return engine.ScanParquet(path).
			Filter(golars.Col("id").Ge(golars.Lit(cutoff)).And(golars.Col("tenant").Eq(golars.Lit("tenant-07")))).
			Select(golars.Col("tenant"), golars.Col("group_id"), golars.Col("value")).
			Collect(ctx)
	case DerivedProjection:
		return engine.ScanParquet(path).
			Filter(golars.Col("id").Ge(golars.Lit(cutoff))).
			WithColumns(
				golars.Col("value").Mul(golars.Col("quantity").Cast(golars.Float64)).Alias("revenue"),
				golars.Col("payload").Str().Lengths().Alias("payload_len"),
			).
			Select(golars.Col("tenant"), golars.Col("revenue"), golars.Col("payload_len")).
			Collect(ctx)
	case GroupedAggregation:
		return engine.ScanParquet(path).
			Filter(golars.Col("id").Ge(golars.Lit(cutoff))).
			GroupBy(golars.Col("group_id")).
			Agg(golars.Col("value").Sum().Alias("total")).
			Collect(ctx)
	case MultiAggregation:
		return engine.ScanParquet(path).
			Filter(golars.Col("id").Ge(golars.Lit(cutoff))).
			GroupBy(golars.Col("tenant")).
			Agg(
				golars.Col("value").Sum().Alias("value_sum"),
				golars.Col("value").Mean().Alias("value_mean"),
				golars.Col("value").Count().Alias("value_count"),
			).
			Collect(ctx)
	case StringFilter:
		return engine.ScanParquet(path).
			Filter(golars.Col("payload").Str().StartsWith(golars.Lit("payload-00"))).
			Select(golars.Col("tenant"), golars.Col("payload"), golars.Col("value")).
			Collect(ctx)
	}
	return nil, fmt.Errorf("benchmarks: unknown query %q", query)
}
