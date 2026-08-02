package core

import (
	"context"
	"fmt"
	"sync"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
)

// DataFrame is an eager, immutable Arrow-backed table. Operations return new
// frames and do not mutate their receiver.
type DataFrame struct {
	table  arrow.Table
	engine *Engine
	once   sync.Once
}

func NewDataFrame(columns ...*Series) (*DataFrame, error) {
	return newDataFrame(getDefaultEngine(), columns...)
}

func newDataFrame(engine *Engine, columns ...*Series) (*DataFrame, error) {
	if engine == nil {
		engine = getDefaultEngine()
	}
	if err := engine.checkOpen(); err != nil {
		return nil, err
	}
	if len(columns) == 0 {
		return nil, fmt.Errorf("golars: a DataFrame needs at least one column")
	}
	fields := make([]arrow.Field, len(columns))
	data := make([][]arrow.Array, len(columns))
	names := make(map[string]struct{}, len(columns))
	rows := int64(-1)
	for i, column := range columns {
		if column == nil || column.data == nil {
			return nil, fmt.Errorf("golars: column %d is nil or closed", i)
		}
		if _, exists := names[column.name]; exists {
			return nil, fmt.Errorf("golars: duplicate column name %q", column.name)
		}
		names[column.name] = struct{}{}
		if i > 0 && int64(column.data.Len()) != rows {
			return nil, fmt.Errorf("golars: column %q has %d rows, expected %d", column.name, column.data.Len(), rows)
		}
		rows = int64(column.data.Len())
		fields[i] = arrow.Field{Name: column.name, Type: column.data.DataType(), Nullable: true}
		chunks := column.data.Chunks()
		for _, chunk := range chunks {
			chunk.Retain()
		}
		data[i] = chunks
	}
	schema := arrow.NewSchema(fields, nil)
	table := array.NewTableFromSlice(schema, data)
	return &DataFrame{table: table, engine: engine}, nil
}

func DataFrameFromArrow(table arrow.Table) (*DataFrame, error) {
	if table == nil {
		return nil, fmt.Errorf("golars: nil Arrow table")
	}
	table.Retain()
	return &DataFrame{table: table, engine: getDefaultEngine()}, nil
}

func NewDataFrameFromArrow(table arrow.Table) (*DataFrame, error) { return DataFrameFromArrow(table) }

func (df *DataFrame) ensureOpen() error {
	if df == nil || df.table == nil {
		return ErrClosed
	}
	return df.engine.checkOpen()
}

func (df *DataFrame) Schema() Schema {
	if df == nil || df.table == nil {
		return Schema{}
	}
	return SchemaFromArrow(df.table.Schema())
}
func (df *DataFrame) Height() int64 {
	if df == nil || df.table == nil {
		return 0
	}
	return int64(df.table.NumRows())
}
func (df *DataFrame) Width() int {
	if df == nil || df.table == nil {
		return 0
	}
	return int(df.table.NumCols())
}
func (df *DataFrame) Shape() (int64, int) { return df.Height(), df.Width() }

// ArrowTable returns a retained Arrow table. The caller must Release it.
func (df *DataFrame) ArrowTable() (arrow.Table, error) {
	if err := df.ensureOpen(); err != nil {
		return nil, err
	}
	df.table.Retain()
	return df.table, nil
}

func (df *DataFrame) Column(name string) (*Series, error) {
	if err := df.ensureOpen(); err != nil {
		return nil, err
	}
	indices := df.table.Schema().FieldIndices(name)
	if len(indices) == 0 {
		return nil, &ColumnNotFoundError{Name: name, Available: df.Schema().Names()}
	}
	column := df.table.Column(indices[0]).Data()
	return SeriesFromArrowChunked(name, column), nil
}

func (df *DataFrame) Lazy() *LazyFrame {
	if df == nil || df.table == nil {
		return &LazyFrame{err: ErrClosed}
	}
	df.table.Retain()
	return &LazyFrame{engine: df.engine, plan: &planNode{kind: planTable, table: df.table}}
}

func (df *DataFrame) Select(exprs ...Expr) (*DataFrame, error) {
	return df.Lazy().Select(exprs...).Collect(context.Background())
}
func (df *DataFrame) Filter(predicate Expr) (*DataFrame, error) {
	return df.Lazy().Filter(predicate).Collect(context.Background())
}
func (df *DataFrame) WithColumns(exprs ...Expr) (*DataFrame, error) {
	return df.Lazy().WithColumns(exprs...).Collect(context.Background())
}
func (df *DataFrame) Limit(n int64) (*DataFrame, error) {
	return df.Lazy().Limit(n).Collect(context.Background())
}
func (df *DataFrame) Sort(exprs ...Expr) (*DataFrame, error) {
	return df.Lazy().Sort(exprs...).Collect(context.Background())
}
func (df *DataFrame) Head(n int64) (*DataFrame, error) { return df.Limit(n) }
func (df *DataFrame) Tail(n int64) (*DataFrame, error) {
	if err := df.ensureOpen(); err != nil {
		return nil, err
	}
	if n < 0 {
		return nil, fmt.Errorf("%w: tail must be non-negative", ErrInvalidPlan)
	}
	if n >= df.Height() {
		return DataFrameFromArrow(df.table)
	}
	reader := array.NewTableReader(df.table, df.Height())
	defer reader.Release()
	if !reader.Next() {
		return DataFrameFromArrow(df.table)
	}
	record := reader.RecordBatch()
	result := record.NewSlice(record.NumRows()-n, record.NumRows())
	defer result.Release()
	table := array.NewTableFromRecords(result.Schema(), []arrow.RecordBatch{result})
	return &DataFrame{table: table, engine: df.engine}, nil
}

func (df *DataFrame) Drop(names ...string) (*DataFrame, error) {
	if err := df.ensureOpen(); err != nil {
		return nil, err
	}
	drop := make(map[string]struct{}, len(names))
	for _, name := range names {
		drop[name] = struct{}{}
	}
	exprs := make([]Expr, 0, df.Width())
	for _, field := range df.table.Schema().Fields() {
		if _, ok := drop[field.Name]; !ok {
			exprs = append(exprs, Col(field.Name))
		}
	}
	if len(exprs) == 0 {
		return nil, fmt.Errorf("%w: Drop removed every column", ErrInvalidPlan)
	}
	return df.Select(exprs...)
}

func (df *DataFrame) Rename(mapping map[string]string) (*DataFrame, error) {
	if err := df.ensureOpen(); err != nil {
		return nil, err
	}
	exprs := make([]Expr, 0, df.Width())
	for _, field := range df.table.Schema().Fields() {
		name := field.Name
		if renamed, ok := mapping[name]; ok {
			name = renamed
		}
		exprs = append(exprs, Col(field.Name).Alias(name))
	}
	return df.Select(exprs...)
}

func (df *DataFrame) DropNulls(names ...string) (*DataFrame, error) {
	if len(names) == 0 {
		names = df.Schema().Names()
	}
	var predicate Expr
	for _, name := range names {
		condition := Col(name).IsNotNull()
		if !predicate.IsValid() {
			predicate = condition
		} else {
			predicate = predicate.And(condition)
		}
	}
	return df.Filter(predicate)
}

func (df *DataFrame) WriteParquet(ctx context.Context, destination string, options ...WriteOption) error {
	return WriteParquet(ctx, df, destination, options...)
}
func (df *DataFrame) WriteCSV(ctx context.Context, destination string, options ...WriteOption) error {
	return WriteCSV(ctx, df, destination, options...)
}
func (df *DataFrame) WriteJSON(ctx context.Context, destination string, options ...WriteOption) error {
	return WriteJSON(ctx, df, destination, options...)
}
func (df *DataFrame) WriteNDJSON(ctx context.Context, destination string, options ...WriteOption) error {
	return WriteNDJSON(ctx, df, destination, options...)
}
func (df *DataFrame) WriteIPCFile(ctx context.Context, destination string, options ...WriteOption) error {
	return WriteIPCFile(ctx, df, destination, options...)
}
func (df *DataFrame) GroupBy(exprs ...Expr) *GroupBy {
	return &GroupBy{df: df, keys: append([]Expr(nil), exprs...)}
}

func (df *DataFrame) Close() {
	if df == nil {
		return
	}
	df.once.Do(func() {
		if df.table != nil {
			df.table.Release()
			df.table = nil
		}
	})
}

// GroupBy is an eager grouped aggregation builder.
type GroupBy struct {
	df   *DataFrame
	keys []Expr
}

func (g *GroupBy) Agg(exprs ...Expr) (*DataFrame, error) {
	if g == nil || g.df == nil {
		return nil, ErrClosed
	}
	return executeGroupBy(context.Background(), g.df, g.keys, exprs)
}

// SortByColumns is a convenience for common column-name sorting.
func (df *DataFrame) SortByColumns(names ...string) (*DataFrame, error) {
	exprs := make([]Expr, len(names))
	for i, name := range names {
		exprs[i] = Col(name)
	}
	return df.Sort(exprs...)
}
