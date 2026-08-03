package core

import (
	"fmt"
	"reflect"
	"runtime"
	"sync"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/arrow/scalar"
)

// DataType is Golars' stable type descriptor. ArrowType enables zero-copy Arrow interop.
type DataType struct{ arrow arrow.DataType }

var (
	// Null is the Arrow null type.
	Null = DataType{arrow.Null}
	// Boolean is the Arrow boolean type.
	Boolean = DataType{arrow.FixedWidthTypes.Boolean}
	// Int8 is the signed 8-bit integer type.
	Int8 = DataType{arrow.PrimitiveTypes.Int8}
	// Int16 is the signed 16-bit integer type.
	Int16 = DataType{arrow.PrimitiveTypes.Int16}
	// Int32 is the signed 32-bit integer type.
	Int32 = DataType{arrow.PrimitiveTypes.Int32}
	// Int64 is the signed 64-bit integer type.
	Int64 = DataType{arrow.PrimitiveTypes.Int64}
	// Uint8 is the unsigned 8-bit integer type.
	Uint8 = DataType{arrow.PrimitiveTypes.Uint8}
	// Uint16 is the unsigned 16-bit integer type.
	Uint16 = DataType{arrow.PrimitiveTypes.Uint16}
	// Uint32 is the unsigned 32-bit integer type.
	Uint32 = DataType{arrow.PrimitiveTypes.Uint32}
	// Uint64 is the unsigned 64-bit integer type.
	Uint64 = DataType{arrow.PrimitiveTypes.Uint64}
	// Float16 is the 16-bit floating-point type.
	Float16 = DataType{arrow.FixedWidthTypes.Float16}
	// Float32 is the 32-bit floating-point type.
	Float32 = DataType{arrow.PrimitiveTypes.Float32}
	// Float64 is the 64-bit floating-point type.
	Float64 = DataType{arrow.PrimitiveTypes.Float64}
	// String is the Arrow UTF-8 string type.
	String = DataType{arrow.BinaryTypes.String}
	// LargeString is the 64-bit-offset UTF-8 string type.
	LargeString = DataType{arrow.BinaryTypes.LargeString}
	// Binary is the variable-width binary type.
	Binary = DataType{arrow.BinaryTypes.Binary}
	// LargeBinary is the 64-bit-offset binary type.
	LargeBinary = DataType{arrow.BinaryTypes.LargeBinary}
	// StringView is the Arrow string-view type.
	StringView = DataType{arrow.BinaryTypes.StringView}
	// BinaryView is the Arrow binary-view type.
	BinaryView = DataType{arrow.BinaryTypes.BinaryView}
	// Date32 is a date stored as days since the epoch.
	Date32 = DataType{arrow.FixedWidthTypes.Date32}
	// Date64 is a date stored as milliseconds since the epoch.
	Date64 = DataType{arrow.FixedWidthTypes.Date64}
	// UInt8 is an alias for Uint8.
	UInt8 = DataType{arrow.PrimitiveTypes.Uint8}
	// UInt16 is an alias for Uint16.
	UInt16 = DataType{arrow.PrimitiveTypes.Uint16}
	// UInt32 is an alias for Uint32.
	UInt32 = DataType{arrow.PrimitiveTypes.Uint32}
	// UInt64 is an alias for Uint64.
	UInt64 = DataType{arrow.PrimitiveTypes.Uint64}
	// Utf8 is an alias for String.
	Utf8 = DataType{arrow.BinaryTypes.String}
	// Date is an alias for Date32.
	Date = DataType{arrow.FixedWidthTypes.Date32}
	// Categorical is an unordered dictionary type.
	Categorical = DataType{&arrow.DictionaryType{IndexType: arrow.PrimitiveTypes.Uint32, ValueType: arrow.BinaryTypes.String}}
	// Enum is an ordered dictionary type.
	Enum = DataType{&arrow.DictionaryType{IndexType: arrow.PrimitiveTypes.Uint32, ValueType: arrow.BinaryTypes.String, Ordered: true}}
	// IntervalMonth is a month interval type.
	IntervalMonth = DataType{arrow.FixedWidthTypes.MonthInterval}
	// IntervalDayTime is a day-time interval type.
	IntervalDayTime = DataType{arrow.FixedWidthTypes.DayTimeInterval}
	// IntervalMonthDayNano is a month-day-nanosecond interval type.
	IntervalMonthDayNano = DataType{arrow.FixedWidthTypes.MonthDayNanoInterval}
)

// Timestamp creates a timestamp type with the given unit and timezone.
func Timestamp(unit arrow.TimeUnit, timezone string) DataType {
	return DataType{&arrow.TimestampType{Unit: unit, TimeZone: timezone}}
}

// Datetime creates a timestamp type; it is an alias for Timestamp.
func Datetime(unit arrow.TimeUnit, timezone string) DataType { return Timestamp(unit, timezone) }

// Duration creates a duration type with the given unit.
func Duration(unit arrow.TimeUnit) DataType { return DataType{&arrow.DurationType{Unit: unit}} }

// Time32 creates a 32-bit time type with the given unit.
func Time32(unit arrow.TimeUnit) DataType { return DataType{&arrow.Time32Type{Unit: unit}} }

// Time64 creates a 64-bit time type with the given unit.
func Time64(unit arrow.TimeUnit) DataType { return DataType{&arrow.Time64Type{Unit: unit}} }

// Time creates a time type using the appropriate Arrow width.
func Time(unit arrow.TimeUnit) DataType {
	if unit == arrow.Second || unit == arrow.Millisecond {
		return Time32(unit)
	}
	return Time64(unit)
}

// Decimal32 creates a 32-bit decimal type.
func Decimal32(precision, scale int32) DataType {
	return DataType{&arrow.Decimal32Type{Precision: precision, Scale: scale}}
}

// Decimal64 creates a 64-bit decimal type.
func Decimal64(precision, scale int32) DataType {
	return DataType{&arrow.Decimal64Type{Precision: precision, Scale: scale}}
}

// Decimal128 creates a 128-bit decimal type.
func Decimal128(precision, scale int32) DataType {
	return DataType{&arrow.Decimal128Type{Precision: precision, Scale: scale}}
}

// Decimal256 creates a 256-bit decimal type.
func Decimal256(precision, scale int32) DataType {
	return DataType{&arrow.Decimal256Type{Precision: precision, Scale: scale}}
}

// FixedSizeBinary creates a fixed-width binary type.
func FixedSizeBinary(width int) DataType {
	return DataType{&arrow.FixedSizeBinaryType{ByteWidth: width}}
}

// ListOf creates a list type containing elem.
func ListOf(elem DataType) DataType { return DataType{arrow.ListOf(elem.arrow)} }

// LargeListOf creates a 64-bit-offset list type containing elem.
func LargeListOf(elem DataType) DataType { return DataType{arrow.LargeListOf(elem.arrow)} }

// ListViewOf creates a list-view type containing elem.
func ListViewOf(elem DataType) DataType { return DataType{arrow.ListViewOf(elem.arrow)} }

// LargeListViewOf creates a 64-bit-offset list-view type containing elem.
func LargeListViewOf(elem DataType) DataType {
	return DataType{arrow.LargeListViewOf(elem.arrow)}
}

// FixedSizeListOf creates a fixed-length list type.
func FixedSizeListOf(length int32, elem DataType) DataType {
	return DataType{arrow.FixedSizeListOf(length, elem.arrow)}
}

// DictionaryOf creates a dictionary type.
func DictionaryOf(index, value DataType, ordered bool) DataType {
	return DataType{&arrow.DictionaryType{IndexType: index.arrow, ValueType: value.arrow, Ordered: ordered}}
}

// RunEndEncodedOf creates a run-end encoded type.
func RunEndEncodedOf(runEnds, values DataType) DataType {
	return DataType{arrow.RunEndEncodedOf(runEnds.arrow, values.arrow)}
}

// StructOf creates a struct type from fields.
func StructOf(fields ...Field) DataType {
	arrowFields := make([]arrow.Field, len(fields))
	for i, field := range fields {
		arrowFields[i] = field.toArrow()
	}
	return DataType{arrow.StructOf(arrowFields...)}
}

// MapOf creates a map type with key and item types.
func MapOf(key, item DataType) DataType { return DataType{arrow.MapOf(key.arrow, item.arrow)} }

// DataTypeFromArrow wraps an Arrow data type.
func DataTypeFromArrow(t arrow.DataType) DataType { return DataType{t} }

// ArrowType returns the underlying Arrow type.
func (t DataType) ArrowType() arrow.DataType { return t.arrow }

// ID returns the Arrow type identifier.
func (t DataType) ID() arrow.Type {
	if t.arrow == nil {
		return arrow.NULL
	}
	return t.arrow.ID()
}

// String returns the Arrow type name.
func (t DataType) String() string {
	if t.arrow == nil {
		return "<nil>"
	}
	return t.arrow.String()
}

// Equal reports whether two data types are equal.
func (t DataType) Equal(other DataType) bool {
	if t.arrow == nil || other.arrow == nil {
		return t.arrow == nil && other.arrow == nil
	}
	return arrow.TypeEqual(t.arrow, other.arrow)
}

// IsNumeric reports whether the type is numeric.
func (t DataType) IsNumeric() bool {
	switch t.ID() {
	case arrow.INT8, arrow.INT16, arrow.INT32, arrow.INT64,
		arrow.UINT8, arrow.UINT16, arrow.UINT32, arrow.UINT64,
		arrow.FLOAT16, arrow.FLOAT32, arrow.FLOAT64,
		arrow.DECIMAL32, arrow.DECIMAL64, arrow.DECIMAL128, arrow.DECIMAL256:
		return true
	default:
		return false
	}
}

// IsInteger reports whether the type is an integer.
func (t DataType) IsInteger() bool {
	switch t.ID() {
	case arrow.INT8, arrow.INT16, arrow.INT32, arrow.INT64,
		arrow.UINT8, arrow.UINT16, arrow.UINT32, arrow.UINT64:
		return true
	default:
		return false
	}
}

// IsFloat reports whether the type is floating point.
func (t DataType) IsFloat() bool {
	switch t.ID() {
	case arrow.FLOAT16, arrow.FLOAT32, arrow.FLOAT64:
		return true
	default:
		return false
	}
}

// IsTemporal reports whether the type represents time.
func (t DataType) IsTemporal() bool {
	switch t.ID() {
	case arrow.DATE32, arrow.DATE64, arrow.TIMESTAMP, arrow.TIME32, arrow.TIME64, arrow.DURATION,
		arrow.INTERVAL_MONTHS, arrow.INTERVAL_DAY_TIME, arrow.INTERVAL_MONTH_DAY_NANO:
		return true
	default:
		return false
	}
}

// IsNested reports whether the type contains nested values.
func (t DataType) IsNested() bool {
	switch t.ID() {
	case arrow.LIST, arrow.LARGE_LIST, arrow.LIST_VIEW, arrow.LARGE_LIST_VIEW,
		arrow.FIXED_SIZE_LIST, arrow.STRUCT, arrow.MAP, arrow.DICTIONARY:
		return true
	default:
		return false
	}
}

// IsCategorical reports whether the type is an unordered dictionary.
func (t DataType) IsCategorical() bool {
	dictionary, ok := t.arrow.(*arrow.DictionaryType)
	return ok && !dictionary.Ordered
}

// IsEnum reports whether the type is an ordered dictionary.
func (t DataType) IsEnum() bool {
	dictionary, ok := t.arrow.(*arrow.DictionaryType)
	return ok && dictionary.Ordered
}

// Elem returns the element type for a nested type.
func (t DataType) Elem() (DataType, bool) {
	if t.arrow == nil {
		return DataType{}, false
	}
	switch dtype := t.arrow.(type) {
	case interface{ Elem() arrow.DataType }:
		return DataTypeFromArrow(dtype.Elem()), true
	default:
		return DataType{}, false
	}
}

// Fields returns nested struct fields, if any.
func (t DataType) Fields() []Field {
	if t.arrow == nil {
		return nil
	}
	if nested, ok := t.arrow.(interface{ Fields() []arrow.Field }); ok {
		fields := nested.Fields()
		result := make([]Field, len(fields))
		for i, field := range fields {
			result[i] = fieldFromArrow(field)
		}
		return result
	}
	return nil
}

// IndexType returns a dictionary index type.
func (t DataType) IndexType() (DataType, bool) {
	if dictionary, ok := t.arrow.(*arrow.DictionaryType); ok {
		return DataTypeFromArrow(dictionary.IndexType), true
	}
	return DataType{}, false
}

// ValueType returns a dictionary or map value type.
func (t DataType) ValueType() (DataType, bool) {
	if dictionary, ok := t.arrow.(*arrow.DictionaryType); ok {
		return DataTypeFromArrow(dictionary.ValueType), true
	}
	if mapType, ok := t.arrow.(*arrow.MapType); ok {
		return DataTypeFromArrow(mapType.ItemType()), true
	}
	return DataType{}, false
}

// PrecisionScale returns decimal precision and scale.
func (t DataType) PrecisionScale() (precision, scale int32, ok bool) {
	switch dtype := t.arrow.(type) {
	case *arrow.Decimal32Type:
		return dtype.Precision, dtype.Scale, true
	case *arrow.Decimal64Type:
		return dtype.Precision, dtype.Scale, true
	case *arrow.Decimal128Type:
		return dtype.Precision, dtype.Scale, true
	case *arrow.Decimal256Type:
		return dtype.Precision, dtype.Scale, true
	default:
		return 0, 0, false
	}
}

// TimeUnit returns the temporal unit.
func (t DataType) TimeUnit() (arrow.TimeUnit, bool) {
	switch dtype := t.arrow.(type) {
	case *arrow.TimestampType:
		return dtype.Unit, true
	case *arrow.DurationType:
		return dtype.Unit, true
	case *arrow.Time32Type:
		return dtype.Unit, true
	case *arrow.Time64Type:
		return dtype.Unit, true
	default:
		return arrow.Second, false
	}
}

// TimeZone returns a timestamp timezone.
func (t DataType) TimeZone() (string, bool) {
	if timestamp, ok := t.arrow.(*arrow.TimestampType); ok {
		return timestamp.TimeZone, true
	}
	return "", false
}

// Field describes one schema field.
type Field struct {
	// Name is the field name.
	Name string
	// Type is the field data type.
	Type DataType
	// Nullable reports whether the field may contain nulls.
	Nullable bool
	// Metadata contains Arrow field metadata.
	Metadata arrow.Metadata
}

func (f Field) toArrow() arrow.Field {
	return arrow.Field{Name: f.Name, Type: f.Type.arrow, Nullable: f.Nullable, Metadata: f.Metadata}
}

func fieldFromArrow(f arrow.Field) Field {
	return Field{Name: f.Name, Type: DataTypeFromArrow(f.Type), Nullable: f.Nullable, Metadata: f.Metadata}
}

// Schema describes a table's fields and metadata.
type Schema struct{ arrow *arrow.Schema }

// NewSchema creates a schema from fields.
func NewSchema(fields ...Field) Schema {
	return NewSchemaWithMetadata(fields, arrow.Metadata{})
}

// NewSchemaWithMetadata creates a schema with fields and metadata.
func NewSchemaWithMetadata(fields []Field, metadata arrow.Metadata) Schema {
	afs := make([]arrow.Field, len(fields))
	for i, field := range fields {
		afs[i] = field.toArrow()
	}
	var metadataPtr *arrow.Metadata
	if metadata.Len() > 0 {
		metadataPtr = &metadata
	}
	return Schema{arrow: arrow.NewSchema(afs, metadataPtr)}
}

// SchemaFromArrow wraps an Arrow schema.
func SchemaFromArrow(schema *arrow.Schema) Schema { return Schema{arrow: schema} }

// ArrowSchema returns the underlying Arrow schema.
func (s Schema) ArrowSchema() *arrow.Schema { return s.arrow }

// Fields returns the schema fields.
func (s Schema) Fields() []Field {
	if s.arrow == nil {
		return nil
	}
	fields := make([]Field, len(s.arrow.Fields()))
	for i, field := range s.arrow.Fields() {
		fields[i] = fieldFromArrow(field)
	}
	return fields
}

// Names returns the schema field names.
func (s Schema) Names() []string {
	if s.arrow == nil {
		return nil
	}
	fields := s.arrow.Fields()
	names := make([]string, len(fields))
	for i, field := range fields {
		names[i] = field.Name
	}
	return names
}

// Metadata returns schema metadata.
func (s Schema) Metadata() arrow.Metadata {
	if s.arrow == nil {
		return arrow.Metadata{}
	}
	return s.arrow.Metadata()
}

// Field returns a named field.
func (s Schema) Field(name string) (Field, bool) {
	if s.arrow == nil {
		return Field{}, false
	}
	indices := s.arrow.FieldIndices(name)
	if len(indices) == 0 {
		return Field{}, false
	}
	return fieldFromArrow(s.arrow.Field(indices[0])), true
}

// String returns the Arrow schema representation.
func (s Schema) String() string {
	if s.arrow == nil {
		return "schema<>"
	}
	return s.arrow.String()
}

// Series is an immutable named Arrow chunked array. Close releases its Arrow
// reference deterministically; the finalizer is only a safety net.
type Series struct {
	name   string
	data   *arrow.Chunked
	once   sync.Once
	closed bool
}

func newSeriesWithData(name string, data *arrow.Chunked) *Series {
	series := &Series{name: name, data: data}
	runtime.SetFinalizer(series, finalizeSeries)
	return series
}

func finalizeSeries(series *Series) { series.Close() }

// NewSeries creates a named series from Go or Arrow-compatible values.
func NewSeries(name string, values any) (*Series, error) {
	return newSeriesWithAllocator(memory.NewGoAllocator(), name, values)
}

func newSeriesWithAllocator(mem memory.Allocator, name string, values any) (_ *Series, err error) {
	if name == "" {
		return nil, fmt.Errorf("golars: series name cannot be empty")
	}
	v := reflect.ValueOf(values)
	if !v.IsValid() || v.Kind() != reflect.Slice {
		return nil, fmt.Errorf("golars: NewSeries expects a slice, got %T", values)
	}
	dt, err := typeForGo(v.Type().Elem())
	if err != nil {
		return nil, err
	}
	b := array.NewBuilder(mem, dt)
	defer b.Release()
	for i := 0; i < v.Len(); i++ {
		item := v.Index(i)
		if item.Kind() == reflect.Pointer {
			if item.IsNil() {
				b.AppendNull()
				continue
			}
			item = item.Elem()
		}
		value := item.Interface()
		value = normalizeSeriesValue(value, dt)
		s, scalarErr := makeSeriesScalar(value, dt, mem)
		if scalarErr != nil {
			return nil, fmt.Errorf("golars: series %q value %d: %w", name, i, scalarErr)
		}
		if appendErr := scalar.Append(b, s); appendErr != nil {
			releaseScalar(s)
			return nil, fmt.Errorf("golars: series %q value %d: %w", name, i, appendErr)
		}
		releaseScalar(s)
	}
	arr := b.NewArray()
	defer arr.Release()
	return SeriesFromArrow(name, arr), nil
}

func makeSeriesScalar(value any, dtype arrow.DataType, mem memory.Allocator) (scalar.Scalar, error) {
	result, err := tryMakeScalarParam(value, dtype)
	if err == nil && result != nil && arrow.TypeEqual(result.DataType(), dtype) {
		return result, nil
	}
	if result != nil {
		releaseScalar(result)
	}
	result, err = tryToScalar(value, mem)
	if err != nil {
		return nil, err
	}
	if !arrow.TypeEqual(result.DataType(), dtype) {
		actual := result.DataType()
		releaseScalar(result)
		return nil, fmt.Errorf("golars: scalar type %s does not match %s", actual, dtype)
	}
	return result, nil
}

func tryMakeScalarParam(value any, dtype arrow.DataType) (result scalar.Scalar, err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			result = nil
			err = fmt.Errorf("golars: cannot build scalar of type %s from %T", dtype, value)
		}
	}()
	return scalar.MakeScalarParam(value, dtype)
}

func tryToScalar(value any, mem memory.Allocator) (result scalar.Scalar, err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			result = nil
			err = fmt.Errorf("golars: cannot build nested scalar from %T", value)
		}
	}()
	return scalar.ToScalar(value, mem)
}

func releaseScalar(value scalar.Scalar) {
	if releaser, ok := value.(interface{ Release() }); ok {
		releaser.Release()
	}
}

func normalizeSeriesValue(value any, dtype arrow.DataType) any {
	if duration, ok := value.(time.Duration); ok {
		if target, ok := dtype.(*arrow.DurationType); ok {
			return arrow.Duration(durationInUnit(duration, target.Unit))
		}
		return int64(duration)
	}
	if timestamp, ok := value.(time.Time); ok {
		if target, ok := dtype.(*arrow.TimestampType); ok {
			return arrow.Timestamp(timestampInUnit(timestamp, target.Unit))
		}
		if dtype.ID() == arrow.DATE64 {
			return arrow.Date64(timestamp.UnixMilli())
		}
	}
	return value
}

func timestampInUnit(value time.Time, unit arrow.TimeUnit) int64 {
	nanos := value.UnixNano()
	switch unit {
	case arrow.Second:
		return nanos / int64(time.Second)
	case arrow.Millisecond:
		return nanos / int64(time.Millisecond)
	case arrow.Microsecond:
		return nanos / int64(time.Microsecond)
	default:
		return nanos
	}
}

func durationInUnit(value time.Duration, unit arrow.TimeUnit) int64 {
	switch unit {
	case arrow.Second:
		return int64(value / time.Second)
	case arrow.Millisecond:
		return int64(value / time.Millisecond)
	case arrow.Microsecond:
		return int64(value / time.Microsecond)
	default:
		return int64(value)
	}
}

func typeForGo(t reflect.Type) (arrow.DataType, error) {
	if t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	if t == reflect.TypeOf(time.Duration(0)) {
		return &arrow.DurationType{Unit: arrow.Nanosecond}, nil
	}
	switch t.Kind() {
	case reflect.Bool:
		return arrow.FixedWidthTypes.Boolean, nil
	case reflect.Int:
		return arrow.PrimitiveTypes.Int64, nil
	case reflect.Int8:
		return arrow.PrimitiveTypes.Int8, nil
	case reflect.Int16:
		return arrow.PrimitiveTypes.Int16, nil
	case reflect.Int32:
		return arrow.PrimitiveTypes.Int32, nil
	case reflect.Int64:
		return arrow.PrimitiveTypes.Int64, nil
	case reflect.Uint:
		return arrow.PrimitiveTypes.Uint64, nil
	case reflect.Uint8:
		return arrow.PrimitiveTypes.Uint8, nil
	case reflect.Uint16:
		return arrow.PrimitiveTypes.Uint16, nil
	case reflect.Uint32:
		return arrow.PrimitiveTypes.Uint32, nil
	case reflect.Uint64:
		return arrow.PrimitiveTypes.Uint64, nil
	case reflect.Float32:
		return arrow.PrimitiveTypes.Float32, nil
	case reflect.Float64:
		return arrow.PrimitiveTypes.Float64, nil
	case reflect.String:
		return arrow.BinaryTypes.String, nil
	case reflect.Slice:
		if t.Elem().Kind() == reflect.Uint8 {
			return arrow.BinaryTypes.Binary, nil
		}
		elem, err := typeForGo(t.Elem())
		if err != nil {
			return nil, err
		}
		return arrow.ListOf(elem), nil
	case reflect.Array:
		if t.Elem().Kind() == reflect.Uint8 {
			return &arrow.FixedSizeBinaryType{ByteWidth: t.Len()}, nil
		}
		elem, err := typeForGo(t.Elem())
		if err != nil {
			return nil, err
		}
		return arrow.FixedSizeListOf(int32(t.Len()), elem), nil
	case reflect.Struct:
		if t == reflect.TypeOf(time.Time{}) {
			return &arrow.TimestampType{Unit: arrow.Nanosecond}, nil
		}
		fields := make([]arrow.Field, 0, t.NumField())
		for i := 0; i < t.NumField(); i++ {
			field := t.Field(i)
			if field.PkgPath != "" { // unexported
				continue
			}
			fieldType, err := typeForGo(field.Type)
			if err != nil {
				return nil, err
			}
			name := field.Name
			if tag := field.Tag.Get("compute"); tag != "" && tag != "-" {
				name = tag
			}
			fields = append(fields, arrow.Field{Name: name, Type: fieldType, Nullable: true})
		}
		return arrow.StructOf(fields...), nil
	default:
		return nil, fmt.Errorf("golars: unsupported Go series element type %v", t)
	}
}

// SeriesFromArrow wraps an Arrow array as a series.
func SeriesFromArrow(name string, values arrow.Array) *Series {
	values.Retain()
	return newSeriesWithData(name, arrow.NewChunked(values.DataType(), []arrow.Array{values}))
}

// NewSeriesFromArrow creates a series from an Arrow array.
func NewSeriesFromArrow(name string, values arrow.Array) (*Series, error) {
	if values == nil {
		return nil, fmt.Errorf("golars: nil Arrow array")
	}
	return SeriesFromArrow(name, values), nil
}

// SeriesFromArrowChunked wraps an Arrow chunked array as a series.
func SeriesFromArrowChunked(name string, values *arrow.Chunked) *Series {
	values.Retain()
	return newSeriesWithData(name, values)
}

// Name returns the series name.
func (s *Series) Name() string { return s.name }

// DataType returns the series data type.
func (s *Series) DataType() DataType {
	if s == nil || s.data == nil {
		return DataType{}
	}
	return DataTypeFromArrow(s.data.DataType())
}

// Len returns the number of values.
func (s *Series) Len() int64 {
	if s == nil || s.data == nil {
		return 0
	}
	return int64(s.data.Len())
}

// NullCount returns the number of null values.
func (s *Series) NullCount() int64 {
	if s == nil || s.data == nil {
		return 0
	}
	return int64(s.data.NullN())
}

// IsEmpty reports whether the series has no values.
func (s *Series) IsEmpty() bool { return s.Len() == 0 }

// NChunks returns the number of Arrow chunks backing the series.
func (s *Series) NChunks() int {
	if s == nil || s.data == nil {
		return 0
	}
	return len(s.data.Chunks())
}

// Values returns detached Go values in row order. Nested values are copied so
// the returned slice remains valid after the series is closed.
func (s *Series) Values() ([]any, error) {
	if s == nil || s.data == nil || s.closed {
		return nil, ErrClosed
	}
	values := make([]any, 0, s.data.Len())
	for _, chunk := range s.data.Chunks() {
		for i := 0; i < chunk.Len(); i++ {
			values = append(values, detachRowValue(chunk.GetOneForMarshal(i)))
		}
	}
	return values, nil
}

// ToSlice is an alias for Values.
func (s *Series) ToSlice() ([]any, error) { return s.Values() }

// Slice returns a zero-copy row slice. Negative offsets count from the end.
func (s *Series) Slice(offset, length int64) (*Series, error) {
	if s == nil || s.data == nil || s.closed {
		return nil, ErrClosed
	}
	if length < 0 {
		return nil, fmt.Errorf("%w: series slice length cannot be negative", ErrInvalidPlan)
	}
	start := offset
	if start < 0 {
		start = int64(s.data.Len()) + start
	}
	if start < 0 {
		start = 0
	}
	if start > int64(s.data.Len()) {
		start = int64(s.data.Len())
	}
	end := start + length
	if end < start || end > int64(s.data.Len()) {
		end = int64(s.data.Len())
	}
	data := array.NewChunkedSlice(s.data, start, end)
	return newSeriesWithData(s.name, data), nil
}

// Clone returns an independent series retaining the same Arrow data.
func (s *Series) Clone() (*Series, error) {
	if s == nil || s.data == nil || s.closed {
		return nil, ErrClosed
	}
	s.data.Retain()
	return newSeriesWithData(s.name, s.data), nil
}

// ArrowChunked returns a retained Arrow chunked array.
func (s *Series) ArrowChunked() *arrow.Chunked {
	if s == nil || s.data == nil {
		return nil
	}
	s.data.Retain()
	return s.data
}

// Close releases the series' Arrow resources.
func (s *Series) Close() {
	if s == nil {
		return
	}
	runtime.SetFinalizer(s, nil)
	s.once.Do(func() {
		s.closed = true
		if s.data != nil {
			s.data.Release()
			s.data = nil
		}
	})
}

// Release is an alias for Close for consistency with Arrow's reference-counted
// objects. Close remains the preferred name in ordinary Go code.
func (s *Series) Release() { s.Close() }
