// Package golars provides an Arrow-native, lazy DataFrame API for Go.
package golars

import (
	"context"
	"io"

	"cloud.google.com/go/storage"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	core "github.com/guywaldman/golars/internal/core"
	"google.golang.org/api/option"
)

// DefaultMemoryLimit is the default per-engine memory quota in bytes.
const DefaultMemoryLimit int64 = core.DefaultMemoryLimit

const (
	// DatasetCreate creates a new partitioned dataset and fails if it exists.
	DatasetCreate DatasetMode = core.DatasetCreate
	// DatasetAppend adds files to an existing partitioned dataset.
	DatasetAppend DatasetMode = core.DatasetAppend
)

var (
	// ErrClosed reports that a DataFrame, LazyFrame, Series, or Engine is closed.
	ErrClosed = core.ErrClosed
	// ErrColumnNotFound reports that an expression referenced an unknown column.
	ErrColumnNotFound = core.ErrColumnNotFound
	// ErrTypeMismatch reports an incompatible type operation.
	ErrTypeMismatch = core.ErrTypeMismatch
	// ErrInvalidPlan reports an invalid lazy or eager operation.
	ErrInvalidPlan = core.ErrInvalidPlan

	// Null is the Arrow null type.
	Null = core.Null
	// Boolean is the Arrow boolean type.
	Boolean = core.Boolean
	// Int8 is the signed 8-bit integer type.
	Int8 = core.Int8
	// Int16 is the signed 16-bit integer type.
	Int16 = core.Int16
	// Int32 is the signed 32-bit integer type.
	Int32 = core.Int32
	// Int64 is the signed 64-bit integer type.
	Int64 = core.Int64
	// Uint8 is the unsigned 8-bit integer type.
	Uint8 = core.Uint8
	// Uint16 is the unsigned 16-bit integer type.
	Uint16 = core.Uint16
	// Uint32 is the unsigned 32-bit integer type.
	Uint32 = core.Uint32
	// Uint64 is the unsigned 64-bit integer type.
	Uint64 = core.Uint64
	// UInt8 is an alias for Uint8.
	UInt8 = core.UInt8
	// UInt16 is an alias for Uint16.
	UInt16 = core.UInt16
	// UInt32 is an alias for Uint32.
	UInt32 = core.UInt32
	// UInt64 is an alias for Uint64.
	UInt64 = core.UInt64
	// Float32 is the 32-bit floating-point type.
	Float32 = core.Float32
	// Float64 is the 64-bit floating-point type.
	Float64 = core.Float64
	// Float16 is the 16-bit floating-point type.
	Float16 = core.Float16
	// String is the Arrow UTF-8 string type.
	String = core.String
	// LargeString is the 64-bit-offset UTF-8 string type.
	LargeString = core.LargeString
	// Binary is the variable-width binary type.
	Binary = core.Binary
	// LargeBinary is the 64-bit-offset binary type.
	LargeBinary = core.LargeBinary
	// StringView is the Arrow string-view type.
	StringView = core.StringView
	// BinaryView is the Arrow binary-view type.
	BinaryView = core.BinaryView
	// Date32 is a date stored as days since the epoch.
	Date32 = core.Date32
	// Date64 is a date stored as milliseconds since the epoch.
	Date64 = core.Date64
	// Date is an alias for Date32.
	Date = core.Date
	// Utf8 is an alias for String.
	Utf8 = core.Utf8
	// Categorical is an unordered dictionary type.
	Categorical = core.Categorical
	// Enum is an ordered dictionary type.
	Enum = core.Enum
	// IntervalMonth is a month interval type.
	IntervalMonth = core.IntervalMonth
	// IntervalDayTime is a day-time interval type.
	IntervalDayTime = core.IntervalDayTime
	// IntervalMonthDayNano is a month-day-nanosecond interval type.
	IntervalMonthDayNano = core.IntervalMonthDayNano
)

type (
	// DataType describes a column type and can be converted to Arrow.
	DataType = core.DataType
	// Row is a detached row represented by column name and value.
	Row = core.Row
	// Field describes one schema field.
	Field = core.Field
	// Schema describes a table's fields and metadata.
	Schema = core.Schema
	// Series is a named, immutable column.
	Series = core.Series
	// DataFrame is an eager, immutable table.
	DataFrame = core.DataFrame
	// GroupBy builds an eager grouped aggregation.
	GroupBy = core.GroupBy
	// LazyFrame is a deferred logical plan.
	LazyFrame = core.LazyFrame
	// LazyGroupBy builds a deferred grouped aggregation.
	LazyGroupBy = core.LazyGroupBy
	// Expr is an immutable expression tree.
	Expr = core.Expr
	// StrExpr provides string expressions.
	StrExpr = core.StrExpr
	// ListExpr provides operations on list-valued expressions.
	ListExpr = core.ListExpr
	// StructExpr provides access to fields in struct-valued expressions.
	StructExpr = core.StructExpr
	// DtExpr provides date and timestamp expressions.
	DtExpr = core.DtExpr
	// WhenThen builds a conditional expression.
	WhenThen = core.WhenThen
	// JSONErrorMode controls how invalid JSON rows are handled.
	JSONErrorMode = core.JSONErrorMode
	// JSONDecodeOption configures DecodeJSON.
	JSONDecodeOption = core.JSONDecodeOption
	// JoinType selects the join operation.
	JoinType = core.JoinType
	// JoinOption configures a join.
	JoinOption = core.JoinOption
	// AsOfJoinOption configures an as-of join.
	AsOfJoinOption = core.AsOfJoinOption
	// AsofStrategy selects nearest-key matching direction.
	AsofStrategy = core.AsofStrategy
	// AsOfStrategy is the conventional initialism spelling of AsofStrategy.
	AsOfStrategy = core.AsOfStrategy
	// RollingOption configures a fixed-size rolling expression.
	RollingOption = core.RollingOption
	// UnpivotOption configures an unpivot operation.
	UnpivotOption = core.UnpivotOption
	// Engine owns shared execution configuration and object stores.
	Engine = core.Engine
	// EngineOption configures an Engine.
	EngineOption = core.EngineOption
	// ScanOption configures a scan.
	ScanOption = core.ScanOption
	// WriteOption configures a file sink.
	WriteOption = core.WriteOption
	// DatasetMode selects dataset creation behavior.
	DatasetMode = core.DatasetMode
	// DatasetWriteOption configures a dataset sink.
	DatasetWriteOption = core.DatasetWriteOption
	// ObjectStore reads objects addressed by URI.
	ObjectStore = core.ObjectStore
	// ObjectInfo describes an object available from an ObjectStore.
	ObjectInfo = core.ObjectInfo
	// RangeObjectStore supports ranged object reads.
	RangeObjectStore = core.RangeObjectStore
	// S3Store is an ObjectStore backed by Amazon S3.
	S3Store = core.S3Store
	// GCSStore is an ObjectStore backed by Google Cloud Storage.
	GCSStore = core.GCSStore
	// MemoryLimitError reports an engine memory quota violation.
	MemoryLimitError = core.MemoryLimitError
	// ColumnNotFoundError reports an unknown column name.
	ColumnNotFoundError = core.ColumnNotFoundError
)

const (
	// JSONNullOnError replaces malformed or incompatible rows with null.
	JSONNullOnError = core.JSONNullOnError
	// JSONErrorOnError returns the first row-level decoding error.
	JSONErrorOnError = core.JSONErrorOnError
)

const (
	// JoinInner keeps rows with matching keys from both inputs.
	JoinInner = core.JoinInner
	// JoinLeft keeps every row from the left input.
	JoinLeft = core.JoinLeft
	// JoinFull keeps every row from both inputs.
	JoinFull = core.JoinFull
	// JoinRight keeps every row from the right input.
	JoinRight = core.JoinRight
	// JoinSemi keeps left rows with a matching right key.
	JoinSemi = core.JoinSemi
	// JoinAnti keeps left rows without a matching right key.
	JoinAnti = core.JoinAnti
	// JoinCross computes the Cartesian product.
	JoinCross = core.JoinCross
)

const (
	// AsofBackward matches the last right key less than or equal to the left key.
	AsofBackward = core.AsofBackward
	// AsofForward matches the first right key greater than or equal to the left key.
	AsofForward = core.AsofForward
	// AsofNearest matches the closest right key.
	AsofNearest = core.AsofNearest
	// AsOfBackward is an alias for AsofBackward.
	AsOfBackward = core.AsOfBackward
	// AsOfForward is an alias for AsofForward.
	AsOfForward = core.AsOfForward
	// AsOfNearest is an alias for AsofNearest.
	AsOfNearest = core.AsOfNearest
)

// Timestamp creates a timestamp type with the given unit and timezone.
func Timestamp(unit arrow.TimeUnit, timezone string) DataType { return core.Timestamp(unit, timezone) }

// Datetime creates a timestamp type; it is an alias for Timestamp.
func Datetime(unit arrow.TimeUnit, timezone string) DataType { return core.Datetime(unit, timezone) }

// Duration creates a duration type with the given unit.
func Duration(unit arrow.TimeUnit) DataType { return core.Duration(unit) }

// Time32 creates a 32-bit time type with the given unit.
func Time32(unit arrow.TimeUnit) DataType { return core.Time32(unit) }

// Time64 creates a 64-bit time type with the given unit.
func Time64(unit arrow.TimeUnit) DataType { return core.Time64(unit) }

// Time creates a time type using the appropriate Arrow width.
func Time(unit arrow.TimeUnit) DataType { return core.Time(unit) }

// Decimal32 creates a 32-bit decimal type.
func Decimal32(precision, scale int32) DataType { return core.Decimal32(precision, scale) }

// Decimal64 creates a 64-bit decimal type.
func Decimal64(precision, scale int32) DataType { return core.Decimal64(precision, scale) }

// Decimal128 creates a 128-bit decimal type.
func Decimal128(precision, scale int32) DataType { return core.Decimal128(precision, scale) }

// Decimal256 creates a 256-bit decimal type.
func Decimal256(precision, scale int32) DataType { return core.Decimal256(precision, scale) }

// FixedSizeBinary creates a fixed-width binary type.
func FixedSizeBinary(width int) DataType { return core.FixedSizeBinary(width) }

// ListOf creates a list type containing elem.
func ListOf(elem DataType) DataType { return core.ListOf(elem) }

// LargeListOf creates a 64-bit-offset list type containing elem.
func LargeListOf(elem DataType) DataType { return core.LargeListOf(elem) }

// ListViewOf creates a list-view type containing elem.
func ListViewOf(elem DataType) DataType { return core.ListViewOf(elem) }

// LargeListViewOf creates a 64-bit-offset list-view type containing elem.
func LargeListViewOf(elem DataType) DataType { return core.LargeListViewOf(elem) }

// FixedSizeListOf creates a fixed-length list type.
func FixedSizeListOf(length int32, elem DataType) DataType { return core.FixedSizeListOf(length, elem) }

// StructOf creates a struct type from fields.
func StructOf(fields ...Field) DataType { return core.StructOf(fields...) }

// MapOf creates a map type with key and item types.
func MapOf(key, item DataType) DataType { return core.MapOf(key, item) }

// DictionaryOf creates a dictionary type.
func DictionaryOf(index, value DataType, ordered bool) DataType {
	return core.DictionaryOf(index, value, ordered)
}

// RunEndEncodedOf creates a run-end encoded type.
func RunEndEncodedOf(runEnds, values DataType) DataType {
	return core.RunEndEncodedOf(runEnds, values)
}

// DataTypeFromArrow wraps an Arrow data type.
func DataTypeFromArrow(t arrow.DataType) DataType { return core.DataTypeFromArrow(t) }

// NewSchema creates a schema from fields.
func NewSchema(fields ...Field) Schema { return core.NewSchema(fields...) }

// NewSchemaWithMetadata creates a schema with fields and metadata.
func NewSchemaWithMetadata(fields []Field, metadata arrow.Metadata) Schema {
	return core.NewSchemaWithMetadata(fields, metadata)
}

// SchemaFromArrow wraps an Arrow schema.
func SchemaFromArrow(schema *arrow.Schema) Schema { return core.SchemaFromArrow(schema) }

// NewSeries creates a named series from Go or Arrow-compatible values.
func NewSeries(name string, values any) (*Series, error) {
	return core.NewSeries(name, values)
}

// SeriesFromArrow wraps an Arrow array as a series.
func SeriesFromArrow(name string, values arrow.Array) *Series {
	return core.SeriesFromArrow(name, values)
}

// NewSeriesFromArrow creates a series from an Arrow array.
func NewSeriesFromArrow(name string, values arrow.Array) (*Series, error) {
	return core.NewSeriesFromArrow(name, values)
}

// SeriesFromArrowChunked wraps an Arrow chunked array as a series.
func SeriesFromArrowChunked(name string, values *arrow.Chunked) *Series {
	return core.SeriesFromArrowChunked(name, values)
}

// NewDataFrame creates an eager frame from columns.
func NewDataFrame(columns ...*Series) (*DataFrame, error) {
	return core.NewDataFrame(columns...)
}

// DataFrameFromArrow wraps an Arrow table as a DataFrame.
func DataFrameFromArrow(table arrow.Table) (*DataFrame, error) {
	return core.DataFrameFromArrow(table)
}

// NewDataFrameFromArrow creates a DataFrame from an Arrow table.
func NewDataFrameFromArrow(table arrow.Table) (*DataFrame, error) {
	return core.NewDataFrameFromArrow(table)
}

// DataFrameFromRecordBatch creates a DataFrame from one record batch.
func DataFrameFromRecordBatch(batch arrow.RecordBatch) (*DataFrame, error) {
	return core.DataFrameFromRecordBatch(batch)
}

// NewDataFrameFromRecordBatch creates a DataFrame from one record batch.
func NewDataFrameFromRecordBatch(batch arrow.RecordBatch) (*DataFrame, error) {
	return core.NewDataFrameFromRecordBatch(batch)
}

// DataFrameFromRecordBatches creates a DataFrame from schema-compatible batches.
func DataFrameFromRecordBatches(batches []arrow.RecordBatch) (*DataFrame, error) {
	return core.DataFrameFromRecordBatches(batches)
}

// NewDataFrameFromRecordBatches creates a DataFrame from schema-compatible batches.
func NewDataFrameFromRecordBatches(batches []arrow.RecordBatch) (*DataFrame, error) {
	return core.NewDataFrameFromRecordBatches(batches)
}

// Col creates a column expression.
func Col(name string) Expr { return core.Col(name) }

// Lit creates a literal expression.
func Lit(value any) Expr { return core.Lit(value) }

// All selects all columns in a projection.
func All() Expr { return core.All() }

// Count creates a row-count aggregation expression.
func Count() Expr { return core.Count() }

// DecodeJSON parses a JSON string expression into the supplied Arrow type.
func DecodeJSON(expr Expr, schema DataType, options ...JSONDecodeOption) Expr {
	return core.DecodeJSON(expr, schema, options...)
}

// WithJSONErrorMode sets the error policy for DecodeJSON.
func WithJSONErrorMode(mode JSONErrorMode) JSONDecodeOption {
	return core.WithJSONErrorMode(mode)
}

// WithJSONCoerce enables or disables compatible JSON-to-type coercions.
func WithJSONCoerce(enabled bool) JSONDecodeOption { return core.WithJSONCoerce(enabled) }

// WithJSONStrictRequiredFields makes non-nullable struct fields required.
func WithJSONStrictRequiredFields(enabled bool) JSONDecodeOption {
	return core.WithJSONStrictRequiredFields(enabled)
}

// When starts a conditional expression.
func When(predicate Expr) WhenThen { return core.When(predicate) }

// NewEngine creates an execution engine with the supplied options.
func NewEngine(options ...EngineOption) (*Engine, error) {
	return core.NewEngine(options...)
}

// WithObjectStore adds an object store for a URI scheme to an engine.
func WithObjectStore(scheme string, store ObjectStore) EngineOption {
	return core.WithObjectStore(scheme, store)
}

// WithMemoryLimit sets the engine memory quota in bytes.
func WithMemoryLimit(bytes int64) EngineOption { return core.WithMemoryLimit(bytes) }

// WithParallelism sets the engine's parallelism hint.
func WithParallelism(n int) EngineOption { return core.WithParallelism(n) }

// WithBatchSize sets the preferred Arrow batch size.
func WithBatchSize(rows int64) EngineOption { return core.WithBatchSize(rows) }

// RegisterObjectStore registers a process-wide object store for a URI scheme.
func RegisterObjectStore(scheme string, store ObjectStore) error {
	return core.RegisterObjectStore(scheme, store)
}

// WithHasHeader controls whether delimited input has a header row.
func WithHasHeader(enabled bool) ScanOption { return core.WithHasHeader(enabled) }

// WithDelimiter sets the delimiter for delimited input.
func WithDelimiter(delimiter rune) ScanOption {
	return core.WithDelimiter(delimiter)
}

// WithScanBatchSize sets the preferred scan batch size.
func WithScanBatchSize(rows int64) ScanOption { return core.WithScanBatchSize(rows) }

// WithColumns limits a scan to the named columns.
func WithColumns(columns ...string) ScanOption { return core.WithColumns(columns...) }

// WithSkipRows skips rows before a CSV header/data section.
func WithSkipRows(rows int64) ScanOption { return core.WithSkipRows(rows) }

// WithNRows limits the number of rows read from a source.
func WithNRows(rows int64) ScanOption { return core.WithNRows(rows) }

// WithComment ignores CSV records beginning with comment.
func WithComment(comment rune) ScanOption { return core.WithComment(comment) }

// WithNullValues treats supplied CSV values as null.
func WithNullValues(values ...string) ScanOption { return core.WithNullValues(values...) }

// WithQuoteChar sets the CSV quote character; zero disables quoting.
func WithQuoteChar(quote rune) ScanOption { return core.WithQuoteChar(quote) }

// WithInferSchemaLength sets the number of CSV rows used for type inference.
func WithInferSchemaLength(rows int64) ScanOption { return core.WithInferSchemaLength(rows) }

// WithOverwrite controls whether an existing output is replaced.
func WithOverwrite(enabled bool) WriteOption { return core.WithOverwrite(enabled) }

// WithWriteHeader controls whether delimited output includes a header row.
func WithWriteHeader(enabled bool) WriteOption {
	return core.WithWriteHeader(enabled)
}

// WithWriteDelimiter sets the delimiter for delimited output.
func WithWriteDelimiter(delimiter rune) WriteOption {
	return core.WithWriteDelimiter(delimiter)
}

// WithWriteBatchSize sets the preferred sink batch size.
func WithWriteBatchSize(rows int64) WriteOption {
	return core.WithWriteBatchSize(rows)
}

// WithJoinType sets the join type.
func WithJoinType(how JoinType) JoinOption { return core.WithJoinType(how) }

// WithJoinSuffix sets the suffix for overlapping right-side columns.
func WithJoinSuffix(suffix string) JoinOption { return core.WithJoinSuffix(suffix) }

// WithAsofStrategy selects an as-of matching direction.
func WithAsofStrategy(strategy AsofStrategy) AsOfJoinOption {
	return core.WithAsofStrategy(strategy)
}

// WithAsOfStrategy is an alias for WithAsofStrategy.
func WithAsOfStrategy(strategy AsOfStrategy) AsOfJoinOption {
	return core.WithAsOfStrategy(strategy)
}

// WithAsofAllowExact controls whether equal keys may match.
func WithAsofAllowExact(enabled bool) AsOfJoinOption {
	return core.WithAsofAllowExact(enabled)
}

// WithAsOfAllowExact is an alias for WithAsofAllowExact.
func WithAsOfAllowExact(enabled bool) AsOfJoinOption {
	return core.WithAsOfAllowExact(enabled)
}

// WithAsofCheckSortedness asks the native engine to validate key ordering.
func WithAsofCheckSortedness(enabled bool) AsOfJoinOption {
	return core.WithAsofCheckSortedness(enabled)
}

// WithAsOfCheckSortedness is an alias for WithAsofCheckSortedness.
func WithAsOfCheckSortedness(enabled bool) AsOfJoinOption {
	return core.WithAsOfCheckSortedness(enabled)
}

// WithRollingMinPeriods sets the minimum values required by a rolling result.
func WithRollingMinPeriods(periods int64) RollingOption {
	return core.WithRollingMinPeriods(periods)
}

// WithRollingCenter centers a rolling window around each row.
func WithRollingCenter(enabled bool) RollingOption { return core.WithRollingCenter(enabled) }

// WithUnpivotVariableName sets the generated variable column name.
func WithUnpivotVariableName(name string) UnpivotOption {
	return core.WithUnpivotVariableName(name)
}

// WithUnpivotValueName sets the generated value column name.
func WithUnpivotValueName(name string) UnpivotOption { return core.WithUnpivotValueName(name) }

// WithPartitionBy sets dataset partition columns.
func WithPartitionBy(columns ...string) DatasetWriteOption {
	return core.WithPartitionBy(columns...)
}

// WithDatasetMode sets the dataset creation mode.
func WithDatasetMode(mode DatasetMode) DatasetWriteOption {
	return core.WithDatasetMode(mode)
}

// WithRowsPerFile sets the target rows per dataset file.
func WithRowsPerFile(rows int64) DatasetWriteOption {
	return core.WithRowsPerFile(rows)
}

// Scan creates a lazy scan whose format is inferred from path.
func Scan(path string, options ...ScanOption) *LazyFrame {
	return core.Scan(path, options...)
}

// ScanParquet creates a lazy Parquet scan.
func ScanParquet(path string, options ...ScanOption) *LazyFrame {
	return core.ScanParquet(path, options...)
}

// ScanCSV creates a lazy CSV scan.
func ScanCSV(path string, options ...ScanOption) *LazyFrame {
	return core.ScanCSV(path, options...)
}

// ScanNDJSON creates a lazy newline-delimited JSON scan.
func ScanNDJSON(path string, options ...ScanOption) *LazyFrame {
	return core.ScanNDJSON(path, options...)
}

// ScanJSON creates a lazy JSON array scan.
func ScanJSON(path string, options ...ScanOption) *LazyFrame {
	return core.ScanJSON(path, options...)
}

// ScanIPCFile creates a lazy Arrow IPC file scan.
func ScanIPCFile(path string, options ...ScanOption) *LazyFrame {
	return core.ScanIPCFile(path, options...)
}

// ScanArrow creates a lazy Arrow IPC file scan.
func ScanArrow(path string, options ...ScanOption) *LazyFrame {
	return core.ScanArrow(path, options...)
}

// ScanIPCStream creates a lazy Arrow IPC stream scan.
func ScanIPCStream(path string, options ...ScanOption) *LazyFrame {
	return core.ScanIPCStream(path, options...)
}

// ReadParquet reads a Parquet path into an eager DataFrame.
func ReadParquet(ctx context.Context, path string, options ...ScanOption) (*DataFrame, error) {
	return core.ReadParquet(ctx, path, options...)
}

// ReadCSV reads a CSV path into an eager DataFrame.
func ReadCSV(ctx context.Context, path string, options ...ScanOption) (*DataFrame, error) {
	return core.ReadCSV(ctx, path, options...)
}

// ReadNDJSON reads newline-delimited JSON into an eager DataFrame.
func ReadNDJSON(ctx context.Context, path string, options ...ScanOption) (*DataFrame, error) {
	return core.ReadNDJSON(ctx, path, options...)
}

// ReadJSON reads a JSON array into an eager DataFrame.
func ReadJSON(ctx context.Context, path string, options ...ScanOption) (*DataFrame, error) {
	return core.ReadJSON(ctx, path, options...)
}

// ReadIPCFile reads an Arrow IPC file into an eager DataFrame.
func ReadIPCFile(ctx context.Context, path string, options ...ScanOption) (*DataFrame, error) {
	return core.ReadIPCFile(ctx, path, options...)
}

// ReadArrow reads an Arrow IPC file into an eager DataFrame.
func ReadArrow(ctx context.Context, path string, options ...ScanOption) (*DataFrame, error) {
	return core.ReadArrow(ctx, path, options...)
}

// ScanCSVReader creates a lazy CSV scan from a reader.
func ScanCSVReader(reader io.Reader, options ...ScanOption) *LazyFrame {
	return core.ScanCSVReader(reader, options...)
}

// ScanNDJSONReader creates a lazy newline-delimited JSON scan from a reader.
func ScanNDJSONReader(reader io.Reader, options ...ScanOption) *LazyFrame {
	return core.ScanNDJSONReader(reader, options...)
}

// ScanJSONReader creates a lazy JSON array scan from a reader.
func ScanJSONReader(reader io.Reader, options ...ScanOption) *LazyFrame {
	return core.ScanJSONReader(reader, options...)
}

// ScanIPCStreamReader creates a lazy Arrow IPC stream scan from a reader.
func ScanIPCStreamReader(reader io.Reader, options ...ScanOption) *LazyFrame {
	return core.ScanIPCStreamReader(reader, options...)
}

// ReadCSVReader reads CSV data from a reader into an eager DataFrame.
func ReadCSVReader(ctx context.Context, reader io.Reader, options ...ScanOption) (*DataFrame, error) {
	return core.ReadCSVReader(ctx, reader, options...)
}

// ReadNDJSONReader reads newline-delimited JSON from a reader into a DataFrame.
func ReadNDJSONReader(ctx context.Context, reader io.Reader, options ...ScanOption) (*DataFrame, error) {
	return core.ReadNDJSONReader(ctx, reader, options...)
}

// ReadJSONReader reads a JSON array from a reader into a DataFrame.
func ReadJSONReader(ctx context.Context, reader io.Reader, options ...ScanOption) (*DataFrame, error) {
	return core.ReadJSONReader(ctx, reader, options...)
}

// ReadIPCStreamReader reads an Arrow IPC stream from a reader into a DataFrame.
func ReadIPCStreamReader(ctx context.Context, reader io.Reader, options ...ScanOption) (*DataFrame, error) {
	return core.ReadIPCStreamReader(ctx, reader, options...)
}

// WriteParquet writes a DataFrame as Parquet.
func WriteParquet(ctx context.Context, df *DataFrame, destination string, options ...WriteOption) error {
	return core.WriteParquet(ctx, df, destination, options...)
}

// WriteCSV writes a DataFrame as CSV.
func WriteCSV(ctx context.Context, df *DataFrame, destination string, options ...WriteOption) error {
	return core.WriteCSV(ctx, df, destination, options...)
}

// WriteNDJSON writes a DataFrame as newline-delimited JSON.
func WriteNDJSON(ctx context.Context, df *DataFrame, destination string, options ...WriteOption) error {
	return core.WriteNDJSON(ctx, df, destination, options...)
}

// WriteJSON writes a DataFrame as a JSON array.
func WriteJSON(ctx context.Context, df *DataFrame, destination string, options ...WriteOption) error {
	return core.WriteJSON(ctx, df, destination, options...)
}

// WriteIPCFile writes a DataFrame as an Arrow IPC file.
func WriteIPCFile(ctx context.Context, df *DataFrame, destination string, options ...WriteOption) error {
	return core.WriteIPCFile(ctx, df, destination, options...)
}

// WriteArrow writes a DataFrame as an Arrow IPC file.
func WriteArrow(ctx context.Context, df *DataFrame, destination string, options ...WriteOption) error {
	return core.WriteArrow(ctx, df, destination, options...)
}

// WriteIPCStream writes a DataFrame as an Arrow IPC stream.
func WriteIPCStream(ctx context.Context, df *DataFrame, destination string, options ...WriteOption) error {
	return core.WriteIPCStream(ctx, df, destination, options...)
}

// WriteParquetDataset writes a DataFrame as a partitioned Parquet dataset.
func WriteParquetDataset(ctx context.Context, df *DataFrame, destination string, options ...DatasetWriteOption) error {
	return core.WriteParquetDataset(ctx, df, destination, options...)
}

// NewS3Store creates an S3-backed ObjectStore.
func NewS3Store(ctx context.Context, options ...func(*s3.Options)) (*S3Store, error) {
	return core.NewS3Store(ctx, options...)
}

// NewS3StoreWithClient wraps an existing S3 client as an ObjectStore.
func NewS3StoreWithClient(client *s3.Client) (*S3Store, error) {
	return core.NewS3StoreWithClient(client)
}

// NewGCSStore creates a Google Cloud Storage-backed ObjectStore.
func NewGCSStore(ctx context.Context, options ...option.ClientOption) (*GCSStore, error) {
	return core.NewGCSStore(ctx, options...)
}

// NewGCSStoreWithClient wraps an existing Cloud Storage client as an ObjectStore.
func NewGCSStoreWithClient(client *storage.Client) (*GCSStore, error) {
	return core.NewGCSStoreWithClient(client)
}
