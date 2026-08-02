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

const DefaultMemoryLimit int64 = core.DefaultMemoryLimit

const (
	DatasetCreate DatasetMode = core.DatasetCreate
	DatasetAppend DatasetMode = core.DatasetAppend
)

var (
	ErrClosed         = core.ErrClosed
	ErrColumnNotFound = core.ErrColumnNotFound
	ErrTypeMismatch   = core.ErrTypeMismatch
	ErrInvalidPlan    = core.ErrInvalidPlan

	Null        = core.Null
	Boolean     = core.Boolean
	Int8        = core.Int8
	Int16       = core.Int16
	Int32       = core.Int32
	Int64       = core.Int64
	Uint8       = core.Uint8
	Uint16      = core.Uint16
	Uint32      = core.Uint32
	Uint64      = core.Uint64
	Float32     = core.Float32
	Float64     = core.Float64
	String      = core.String
	LargeString = core.LargeString
	Binary      = core.Binary
	Date32      = core.Date32
	Date64      = core.Date64
)

type (
	DataType            = core.DataType
	Field               = core.Field
	Schema              = core.Schema
	Series              = core.Series
	DataFrame           = core.DataFrame
	GroupBy             = core.GroupBy
	LazyFrame           = core.LazyFrame
	LazyGroupBy         = core.LazyGroupBy
	Expr                = core.Expr
	StrExpr             = core.StrExpr
	DtExpr              = core.DtExpr
	Engine              = core.Engine
	EngineOption        = core.EngineOption
	ScanOption          = core.ScanOption
	WriteOption         = core.WriteOption
	DatasetMode         = core.DatasetMode
	DatasetWriteOption  = core.DatasetWriteOption
	ObjectStore         = core.ObjectStore
	ObjectInfo          = core.ObjectInfo
	RangeObjectStore    = core.RangeObjectStore
	S3Store             = core.S3Store
	GCSStore            = core.GCSStore
	MemoryLimitError    = core.MemoryLimitError
	ColumnNotFoundError = core.ColumnNotFoundError
)

func Timestamp(unit arrow.TimeUnit, timezone string) DataType { return core.Timestamp(unit, timezone) }
func Duration(unit arrow.TimeUnit) DataType                   { return core.Duration(unit) }
func ListOf(elem DataType) DataType                           { return core.ListOf(elem) }
func StructOf(fields ...Field) DataType                       { return core.StructOf(fields...) }
func MapOf(key, item DataType) DataType                       { return core.MapOf(key, item) }
func DataTypeFromArrow(t arrow.DataType) DataType             { return core.DataTypeFromArrow(t) }
func NewSchema(fields ...Field) Schema                        { return core.NewSchema(fields...) }
func SchemaFromArrow(schema *arrow.Schema) Schema             { return core.SchemaFromArrow(schema) }

func NewSeries(name string, values any) (*Series, error) {
	return core.NewSeries(name, values)
}
func SeriesFromArrow(name string, values arrow.Array) *Series {
	return core.SeriesFromArrow(name, values)
}
func NewSeriesFromArrow(name string, values arrow.Array) (*Series, error) {
	return core.NewSeriesFromArrow(name, values)
}
func SeriesFromArrowChunked(name string, values *arrow.Chunked) *Series {
	return core.SeriesFromArrowChunked(name, values)
}

func NewDataFrame(columns ...*Series) (*DataFrame, error) {
	return core.NewDataFrame(columns...)
}
func DataFrameFromArrow(table arrow.Table) (*DataFrame, error) {
	return core.DataFrameFromArrow(table)
}
func NewDataFrameFromArrow(table arrow.Table) (*DataFrame, error) {
	return core.NewDataFrameFromArrow(table)
}

func Col(name string) Expr { return core.Col(name) }
func Lit(value any) Expr   { return core.Lit(value) }
func All() Expr            { return core.All() }
func Count() Expr          { return core.Count() }

func NewEngine(options ...EngineOption) (*Engine, error) {
	return core.NewEngine(options...)
}
func WithObjectStore(scheme string, store ObjectStore) EngineOption {
	return core.WithObjectStore(scheme, store)
}
func WithMemoryLimit(bytes int64) EngineOption { return core.WithMemoryLimit(bytes) }
func WithParallelism(n int) EngineOption       { return core.WithParallelism(n) }
func WithBatchSize(rows int64) EngineOption    { return core.WithBatchSize(rows) }
func RegisterObjectStore(scheme string, store ObjectStore) error {
	return core.RegisterObjectStore(scheme, store)
}

func WithHasHeader(enabled bool) ScanOption { return core.WithHasHeader(enabled) }
func WithDelimiter(delimiter rune) ScanOption {
	return core.WithDelimiter(delimiter)
}
func WithScanBatchSize(rows int64) ScanOption { return core.WithScanBatchSize(rows) }
func WithOverwrite(enabled bool) WriteOption  { return core.WithOverwrite(enabled) }
func WithWriteHeader(enabled bool) WriteOption {
	return core.WithWriteHeader(enabled)
}
func WithWriteDelimiter(delimiter rune) WriteOption {
	return core.WithWriteDelimiter(delimiter)
}
func WithWriteBatchSize(rows int64) WriteOption {
	return core.WithWriteBatchSize(rows)
}

func WithPartitionBy(columns ...string) DatasetWriteOption {
	return core.WithPartitionBy(columns...)
}
func WithDatasetMode(mode DatasetMode) DatasetWriteOption {
	return core.WithDatasetMode(mode)
}
func WithRowsPerFile(rows int64) DatasetWriteOption {
	return core.WithRowsPerFile(rows)
}

func Scan(path string, options ...ScanOption) *LazyFrame {
	return core.Scan(path, options...)
}
func ScanParquet(path string, options ...ScanOption) *LazyFrame {
	return core.ScanParquet(path, options...)
}
func ScanCSV(path string, options ...ScanOption) *LazyFrame {
	return core.ScanCSV(path, options...)
}
func ScanNDJSON(path string, options ...ScanOption) *LazyFrame {
	return core.ScanNDJSON(path, options...)
}
func ScanJSON(path string, options ...ScanOption) *LazyFrame {
	return core.ScanJSON(path, options...)
}
func ScanIPCFile(path string, options ...ScanOption) *LazyFrame {
	return core.ScanIPCFile(path, options...)
}
func ScanIPCStream(path string, options ...ScanOption) *LazyFrame {
	return core.ScanIPCStream(path, options...)
}

func ReadParquet(ctx context.Context, path string, options ...ScanOption) (*DataFrame, error) {
	return core.ReadParquet(ctx, path, options...)
}
func ReadCSV(ctx context.Context, path string, options ...ScanOption) (*DataFrame, error) {
	return core.ReadCSV(ctx, path, options...)
}
func ReadNDJSON(ctx context.Context, path string, options ...ScanOption) (*DataFrame, error) {
	return core.ReadNDJSON(ctx, path, options...)
}
func ReadJSON(ctx context.Context, path string, options ...ScanOption) (*DataFrame, error) {
	return core.ReadJSON(ctx, path, options...)
}
func ReadIPCFile(ctx context.Context, path string, options ...ScanOption) (*DataFrame, error) {
	return core.ReadIPCFile(ctx, path, options...)
}
func ScanCSVReader(reader io.Reader, options ...ScanOption) *LazyFrame {
	return core.ScanCSVReader(reader, options...)
}
func ScanNDJSONReader(reader io.Reader, options ...ScanOption) *LazyFrame {
	return core.ScanNDJSONReader(reader, options...)
}
func ScanJSONReader(reader io.Reader, options ...ScanOption) *LazyFrame {
	return core.ScanJSONReader(reader, options...)
}
func ScanIPCStreamReader(reader io.Reader, options ...ScanOption) *LazyFrame {
	return core.ScanIPCStreamReader(reader, options...)
}
func ReadCSVReader(ctx context.Context, reader io.Reader, options ...ScanOption) (*DataFrame, error) {
	return core.ReadCSVReader(ctx, reader, options...)
}
func ReadNDJSONReader(ctx context.Context, reader io.Reader, options ...ScanOption) (*DataFrame, error) {
	return core.ReadNDJSONReader(ctx, reader, options...)
}
func ReadJSONReader(ctx context.Context, reader io.Reader, options ...ScanOption) (*DataFrame, error) {
	return core.ReadJSONReader(ctx, reader, options...)
}
func ReadIPCStreamReader(ctx context.Context, reader io.Reader, options ...ScanOption) (*DataFrame, error) {
	return core.ReadIPCStreamReader(ctx, reader, options...)
}

func WriteParquet(ctx context.Context, df *DataFrame, destination string, options ...WriteOption) error {
	return core.WriteParquet(ctx, df, destination, options...)
}
func WriteCSV(ctx context.Context, df *DataFrame, destination string, options ...WriteOption) error {
	return core.WriteCSV(ctx, df, destination, options...)
}
func WriteNDJSON(ctx context.Context, df *DataFrame, destination string, options ...WriteOption) error {
	return core.WriteNDJSON(ctx, df, destination, options...)
}
func WriteJSON(ctx context.Context, df *DataFrame, destination string, options ...WriteOption) error {
	return core.WriteJSON(ctx, df, destination, options...)
}
func WriteIPCFile(ctx context.Context, df *DataFrame, destination string, options ...WriteOption) error {
	return core.WriteIPCFile(ctx, df, destination, options...)
}
func WriteIPCStream(ctx context.Context, df *DataFrame, destination string, options ...WriteOption) error {
	return core.WriteIPCStream(ctx, df, destination, options...)
}
func WriteParquetDataset(ctx context.Context, df *DataFrame, destination string, options ...DatasetWriteOption) error {
	return core.WriteParquetDataset(ctx, df, destination, options...)
}

func NewS3Store(ctx context.Context, options ...func(*s3.Options)) (*S3Store, error) {
	return core.NewS3Store(ctx, options...)
}
func NewS3StoreWithClient(client *s3.Client) (*S3Store, error) {
	return core.NewS3StoreWithClient(client)
}
func NewGCSStore(ctx context.Context, options ...option.ClientOption) (*GCSStore, error) {
	return core.NewGCSStore(ctx, options...)
}
func NewGCSStoreWithClient(client *storage.Client) (*GCSStore, error) {
	return core.NewGCSStoreWithClient(client)
}
