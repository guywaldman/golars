package core

import (
	"context"
	"fmt"
	"runtime"
	"sync"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	nativebackend "github.com/guywaldman/golars/internal/native"
)

// DataFrame is an eager, immutable Arrow-backed table. Operations return new
// frames and do not mutate their receiver. Close releases its Arrow reference
// deterministically; the finalizer is only a safety net.
type DataFrame struct {
	table          arrow.Table
	native         *nativebackend.Frame
	engine         *Engine
	once           sync.Once
	materializeMu  sync.Mutex
	nativeSchema   Schema
	nativeSchemaMu sync.Once
}

func newDataFrameWithTable(table arrow.Table, engine *Engine) *DataFrame {
	if engine == nil {
		engine = getDefaultEngine()
	}
	df := &DataFrame{table: table, engine: engine}
	runtime.SetFinalizer(df, finalizeDataFrame)
	return df
}

func newDataFrameWithNative(frame *nativebackend.Frame, engine *Engine) *DataFrame {
	if engine == nil {
		engine = getDefaultEngine()
	}
	df := &DataFrame{native: frame, engine: engine}
	runtime.SetFinalizer(df, finalizeDataFrame)
	return df
}

func finalizeDataFrame(df *DataFrame) { df.Close() }

// NewDataFrame creates an eager frame from columns.
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
	return newDataFrameWithTable(table, engine), nil
}

// DataFrameFromArrow wraps an Arrow table as a DataFrame.
func DataFrameFromArrow(table arrow.Table) (*DataFrame, error) {
	if table == nil {
		return nil, fmt.Errorf("golars: nil Arrow table")
	}
	table.Retain()
	return newDataFrameWithTable(table, getDefaultEngine()), nil
}

// NewDataFrameFromArrow creates a DataFrame from an Arrow table.
func NewDataFrameFromArrow(table arrow.Table) (*DataFrame, error) { return DataFrameFromArrow(table) }

// DataFrameFromRecordBatch creates a DataFrame from one record batch.
func DataFrameFromRecordBatch(batch arrow.RecordBatch) (*DataFrame, error) {
	if batch == nil {
		return nil, fmt.Errorf("golars: nil Arrow record batch")
	}
	return DataFrameFromRecordBatches([]arrow.RecordBatch{batch})
}

// NewDataFrameFromRecordBatch creates a DataFrame from one record batch.
func NewDataFrameFromRecordBatch(batch arrow.RecordBatch) (*DataFrame, error) {
	return DataFrameFromRecordBatch(batch)
}

// DataFrameFromRecordBatches creates a DataFrame from schema-compatible batches.
func DataFrameFromRecordBatches(batches []arrow.RecordBatch) (*DataFrame, error) {
	if len(batches) == 0 || batches[0] == nil {
		return nil, fmt.Errorf("golars: at least one Arrow record batch is required")
	}
	schema := batches[0].Schema()
	for i, batch := range batches {
		if batch == nil {
			return nil, fmt.Errorf("golars: Arrow record batch %d is nil", i)
		}
		if !schema.Equal(batch.Schema()) {
			return nil, fmt.Errorf("golars: Arrow record batch %d has a different schema", i)
		}
	}
	table := array.NewTableFromRecords(schema, batches)
	return newDataFrameWithTable(table, getDefaultEngine()), nil
}

// NewDataFrameFromRecordBatches creates a DataFrame from schema-compatible batches.
func NewDataFrameFromRecordBatches(batches []arrow.RecordBatch) (*DataFrame, error) {
	return DataFrameFromRecordBatches(batches)
}

func (df *DataFrame) ensureOpen() error {
	if df == nil {
		return ErrClosed
	}
	if df.engine == nil {
		return ErrClosed
	}
	if err := df.engine.checkOpen(); err != nil {
		return err
	}
	return df.materialize()
}

func (df *DataFrame) materialize() error {
	if df == nil {
		return ErrClosed
	}
	df.materializeMu.Lock()
	defer df.materializeMu.Unlock()
	if df.table != nil {
		return nil
	}
	if df.native == nil {
		return ErrClosed
	}
	data, err := df.native.IPC()
	if err != nil {
		return err
	}
	table, err := tableFromNativeIPC(data, df.engine.allocator)
	if err != nil {
		return err
	}
	df.table = table
	df.native.Close()
	df.native = nil
	return nil
}

// Schema returns the frame schema.
func (df *DataFrame) Schema() Schema {
	if df == nil {
		return Schema{}
	}
	if df.table != nil {
		return SchemaFromArrow(df.table.Schema())
	}
	if df.native == nil {
		return Schema{}
	}
	df.nativeSchemaMu.Do(func() {
		data, err := df.native.SchemaIPC()
		if err != nil {
			return
		}
		schema, err := schemaFromNativeIPC(data)
		if err == nil {
			df.nativeSchema = SchemaFromArrow(schema)
		}
	})
	return df.nativeSchema
}

// Height returns the number of rows.
func (df *DataFrame) Height() int64 {
	if df == nil {
		return 0
	}
	if df.table == nil {
		if df.native == nil {
			return 0
		}
		return df.native.Height()
	}
	return int64(df.table.NumRows())
}

// Width returns the number of columns.
func (df *DataFrame) Width() int {
	if df == nil {
		return 0
	}
	if df.table == nil {
		if df.native == nil {
			return 0
		}
		return df.native.Width()
	}
	return int(df.table.NumCols())
}

// Shape returns row and column counts.
func (df *DataFrame) Shape() (int64, int) { return df.Height(), df.Width() }

// ArrowTable returns a retained Arrow table. The caller must Release it.
func (df *DataFrame) ArrowTable() (arrow.Table, error) {
	if err := df.ensureOpen(); err != nil {
		return nil, err
	}
	df.table.Retain()
	return df.table, nil
}

// Column returns a named column.
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

// Lazy converts the eager frame to a lazy plan.
func (df *DataFrame) Lazy() *LazyFrame {
	if err := df.ensureOpen(); err != nil {
		return &LazyFrame{err: err}
	}
	return newLazyFrame(df.engine, &planNode{kind: planTable, table: df.table}, nil)
}

// Select evaluates a projection and returns a new frame.
func (df *DataFrame) Select(exprs ...Expr) (*DataFrame, error) {
	plan := df.Lazy().Select(exprs...)
	defer plan.Close()
	return plan.Collect(context.Background())
}

// Filter keeps rows matching predicate.
func (df *DataFrame) Filter(predicate Expr) (*DataFrame, error) {
	plan := df.Lazy().Filter(predicate)
	defer plan.Close()
	return plan.Collect(context.Background())
}

// WithColumns adds or replaces columns.
func (df *DataFrame) WithColumns(exprs ...Expr) (*DataFrame, error) {
	plan := df.Lazy().WithColumns(exprs...)
	defer plan.Close()
	return plan.Collect(context.Background())
}

// Limit returns at most n rows from the start of the frame.
func (df *DataFrame) Limit(n int64) (*DataFrame, error) {
	plan := df.Lazy().Limit(n)
	defer plan.Close()
	return plan.Collect(context.Background())
}

// Sort orders rows by the supplied expressions.
func (df *DataFrame) Sort(exprs ...Expr) (*DataFrame, error) {
	plan := df.Lazy().Sort(exprs...)
	defer plan.Close()
	return plan.Collect(context.Background())
}

// Head returns the first n rows.
func (df *DataFrame) Head(n int64) (*DataFrame, error) { return df.Limit(n) }

// Tail returns the last n rows.
func (df *DataFrame) Tail(n int64) (*DataFrame, error) {
	plan := df.Lazy().Tail(n)
	defer plan.Close()
	return plan.Collect(context.Background())
}

// Drop removes named columns.
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

// Rename changes column names using mapping.
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

// DropNulls removes rows with nulls in the named columns.
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

// Unique keeps one row for each distinct key.
func (df *DataFrame) Unique(columns ...string) (*DataFrame, error) {
	plan := df.Lazy().Unique(columns...)
	defer plan.Close()
	return plan.Collect(context.Background())
}

// Explode expands list values into separate rows.
func (df *DataFrame) Explode(columns ...string) (*DataFrame, error) {
	plan := df.Lazy().Explode(columns...)
	defer plan.Close()
	return plan.Collect(context.Background())
}

// Unnest expands struct fields into columns.
func (df *DataFrame) Unnest(columns ...string) (*DataFrame, error) {
	plan := df.Lazy().Unnest(columns...)
	defer plan.Close()
	return plan.Collect(context.Background())
}

// WriteParquet writes the frame as Parquet.
func (df *DataFrame) WriteParquet(ctx context.Context, destination string, options ...WriteOption) error {
	return WriteParquet(ctx, df, destination, options...)
}

// WriteCSV writes the frame as CSV.
func (df *DataFrame) WriteCSV(ctx context.Context, destination string, options ...WriteOption) error {
	return WriteCSV(ctx, df, destination, options...)
}

// WriteJSON writes the frame as a JSON array.
func (df *DataFrame) WriteJSON(ctx context.Context, destination string, options ...WriteOption) error {
	return WriteJSON(ctx, df, destination, options...)
}

// WriteNDJSON writes the frame as newline-delimited JSON.
func (df *DataFrame) WriteNDJSON(ctx context.Context, destination string, options ...WriteOption) error {
	return WriteNDJSON(ctx, df, destination, options...)
}

// WriteIPCFile writes the frame as an Arrow IPC file.
func (df *DataFrame) WriteIPCFile(ctx context.Context, destination string, options ...WriteOption) error {
	return WriteIPCFile(ctx, df, destination, options...)
}

// GroupBy starts an eager grouped aggregation.
func (df *DataFrame) GroupBy(exprs ...Expr) *GroupBy {
	return &GroupBy{df: df, keys: append([]Expr(nil), exprs...)}
}

// Close releases the frame's Arrow and native resources.
func (df *DataFrame) Close() {
	if df == nil {
		return
	}
	runtime.SetFinalizer(df, nil)
	df.once.Do(func() {
		df.materializeMu.Lock()
		table := df.table
		df.table = nil
		native := df.native
		df.native = nil
		df.materializeMu.Unlock()
		if table != nil {
			table.Release()
		}
		if native != nil {
			native.Close()
		}
	})
}

// Release is an alias for Close for consistency with Arrow's reference-counted
// objects. Close remains the preferred name in ordinary Go code.
func (df *DataFrame) Release() { df.Close() }

// GroupBy is an eager grouped aggregation builder.
type GroupBy struct {
	df   *DataFrame
	keys []Expr
}

// Agg evaluates grouped aggregate expressions.
func (g *GroupBy) Agg(exprs ...Expr) (*DataFrame, error) {
	if g == nil || g.df == nil {
		return nil, ErrClosed
	}
	if err := g.df.ensureOpen(); err != nil {
		return nil, err
	}
	lf := g.df.Lazy()
	defer lf.Close()
	plan := lf.GroupBy(g.keys...).Agg(exprs...)
	defer plan.Close()
	return plan.Collect(context.Background())
}

// SortByColumns is a convenience for common column-name sorting.
func (df *DataFrame) SortByColumns(names ...string) (*DataFrame, error) {
	exprs := make([]Expr, len(names))
	for i, name := range names {
		exprs[i] = Col(name)
	}
	return df.Sort(exprs...)
}
