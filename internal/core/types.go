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
	Null                 = DataType{arrow.Null}
	Boolean              = DataType{arrow.FixedWidthTypes.Boolean}
	Int8                 = DataType{arrow.PrimitiveTypes.Int8}
	Int16                = DataType{arrow.PrimitiveTypes.Int16}
	Int32                = DataType{arrow.PrimitiveTypes.Int32}
	Int64                = DataType{arrow.PrimitiveTypes.Int64}
	Uint8                = DataType{arrow.PrimitiveTypes.Uint8}
	Uint16               = DataType{arrow.PrimitiveTypes.Uint16}
	Uint32               = DataType{arrow.PrimitiveTypes.Uint32}
	Uint64               = DataType{arrow.PrimitiveTypes.Uint64}
	Float16              = DataType{arrow.FixedWidthTypes.Float16}
	Float32              = DataType{arrow.PrimitiveTypes.Float32}
	Float64              = DataType{arrow.PrimitiveTypes.Float64}
	String               = DataType{arrow.BinaryTypes.String}
	LargeString          = DataType{arrow.BinaryTypes.LargeString}
	Binary               = DataType{arrow.BinaryTypes.Binary}
	LargeBinary          = DataType{arrow.BinaryTypes.LargeBinary}
	StringView           = DataType{arrow.BinaryTypes.StringView}
	BinaryView           = DataType{arrow.BinaryTypes.BinaryView}
	Date32               = DataType{arrow.FixedWidthTypes.Date32}
	Date64               = DataType{arrow.FixedWidthTypes.Date64}
	UInt8                = DataType{arrow.PrimitiveTypes.Uint8}
	UInt16               = DataType{arrow.PrimitiveTypes.Uint16}
	UInt32               = DataType{arrow.PrimitiveTypes.Uint32}
	UInt64               = DataType{arrow.PrimitiveTypes.Uint64}
	Utf8                 = DataType{arrow.BinaryTypes.String}
	Date                 = DataType{arrow.FixedWidthTypes.Date32}
	Categorical          = DataType{&arrow.DictionaryType{IndexType: arrow.PrimitiveTypes.Uint32, ValueType: arrow.BinaryTypes.String}}
	Enum                 = DataType{&arrow.DictionaryType{IndexType: arrow.PrimitiveTypes.Uint32, ValueType: arrow.BinaryTypes.String, Ordered: true}}
	IntervalMonth        = DataType{arrow.FixedWidthTypes.MonthInterval}
	IntervalDayTime      = DataType{arrow.FixedWidthTypes.DayTimeInterval}
	IntervalMonthDayNano = DataType{arrow.FixedWidthTypes.MonthDayNanoInterval}
)

func Timestamp(unit arrow.TimeUnit, timezone string) DataType {
	return DataType{&arrow.TimestampType{Unit: unit, TimeZone: timezone}}
}

func Datetime(unit arrow.TimeUnit, timezone string) DataType { return Timestamp(unit, timezone) }

func Duration(unit arrow.TimeUnit) DataType { return DataType{&arrow.DurationType{Unit: unit}} }
func Time32(unit arrow.TimeUnit) DataType   { return DataType{&arrow.Time32Type{Unit: unit}} }
func Time64(unit arrow.TimeUnit) DataType   { return DataType{&arrow.Time64Type{Unit: unit}} }
func Time(unit arrow.TimeUnit) DataType {
	if unit == arrow.Second || unit == arrow.Millisecond {
		return Time32(unit)
	}
	return Time64(unit)
}

func Decimal32(precision, scale int32) DataType {
	return DataType{&arrow.Decimal32Type{Precision: precision, Scale: scale}}
}
func Decimal64(precision, scale int32) DataType {
	return DataType{&arrow.Decimal64Type{Precision: precision, Scale: scale}}
}
func Decimal128(precision, scale int32) DataType {
	return DataType{&arrow.Decimal128Type{Precision: precision, Scale: scale}}
}
func Decimal256(precision, scale int32) DataType {
	return DataType{&arrow.Decimal256Type{Precision: precision, Scale: scale}}
}

func FixedSizeBinary(width int) DataType {
	return DataType{&arrow.FixedSizeBinaryType{ByteWidth: width}}
}
func ListOf(elem DataType) DataType      { return DataType{arrow.ListOf(elem.arrow)} }
func LargeListOf(elem DataType) DataType { return DataType{arrow.LargeListOf(elem.arrow)} }
func ListViewOf(elem DataType) DataType  { return DataType{arrow.ListViewOf(elem.arrow)} }
func LargeListViewOf(elem DataType) DataType {
	return DataType{arrow.LargeListViewOf(elem.arrow)}
}
func FixedSizeListOf(length int32, elem DataType) DataType {
	return DataType{arrow.FixedSizeListOf(length, elem.arrow)}
}

func DictionaryOf(index, value DataType, ordered bool) DataType {
	return DataType{&arrow.DictionaryType{IndexType: index.arrow, ValueType: value.arrow, Ordered: ordered}}
}

func RunEndEncodedOf(runEnds, values DataType) DataType {
	return DataType{arrow.RunEndEncodedOf(runEnds.arrow, values.arrow)}
}

func StructOf(fields ...Field) DataType {
	arrowFields := make([]arrow.Field, len(fields))
	for i, field := range fields {
		arrowFields[i] = field.toArrow()
	}
	return DataType{arrow.StructOf(arrowFields...)}
}

func MapOf(key, item DataType) DataType { return DataType{arrow.MapOf(key.arrow, item.arrow)} }

func DataTypeFromArrow(t arrow.DataType) DataType { return DataType{t} }
func (t DataType) ArrowType() arrow.DataType      { return t.arrow }
func (t DataType) ID() arrow.Type {
	if t.arrow == nil {
		return arrow.NULL
	}
	return t.arrow.ID()
}
func (t DataType) String() string {
	if t.arrow == nil {
		return "<nil>"
	}
	return t.arrow.String()
}
func (t DataType) Equal(other DataType) bool {
	if t.arrow == nil || other.arrow == nil {
		return t.arrow == nil && other.arrow == nil
	}
	return arrow.TypeEqual(t.arrow, other.arrow)
}

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

func (t DataType) IsInteger() bool {
	switch t.ID() {
	case arrow.INT8, arrow.INT16, arrow.INT32, arrow.INT64,
		arrow.UINT8, arrow.UINT16, arrow.UINT32, arrow.UINT64:
		return true
	default:
		return false
	}
}

func (t DataType) IsFloat() bool {
	switch t.ID() {
	case arrow.FLOAT16, arrow.FLOAT32, arrow.FLOAT64:
		return true
	default:
		return false
	}
}

func (t DataType) IsTemporal() bool {
	switch t.ID() {
	case arrow.DATE32, arrow.DATE64, arrow.TIMESTAMP, arrow.TIME32, arrow.TIME64, arrow.DURATION,
		arrow.INTERVAL_MONTHS, arrow.INTERVAL_DAY_TIME, arrow.INTERVAL_MONTH_DAY_NANO:
		return true
	default:
		return false
	}
}

func (t DataType) IsNested() bool {
	switch t.ID() {
	case arrow.LIST, arrow.LARGE_LIST, arrow.LIST_VIEW, arrow.LARGE_LIST_VIEW,
		arrow.FIXED_SIZE_LIST, arrow.STRUCT, arrow.MAP, arrow.DICTIONARY:
		return true
	default:
		return false
	}
}

func (t DataType) IsCategorical() bool {
	dictionary, ok := t.arrow.(*arrow.DictionaryType)
	return ok && !dictionary.Ordered
}

func (t DataType) IsEnum() bool {
	dictionary, ok := t.arrow.(*arrow.DictionaryType)
	return ok && dictionary.Ordered
}

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

func (t DataType) IndexType() (DataType, bool) {
	if dictionary, ok := t.arrow.(*arrow.DictionaryType); ok {
		return DataTypeFromArrow(dictionary.IndexType), true
	}
	return DataType{}, false
}

func (t DataType) ValueType() (DataType, bool) {
	if dictionary, ok := t.arrow.(*arrow.DictionaryType); ok {
		return DataTypeFromArrow(dictionary.ValueType), true
	}
	if mapType, ok := t.arrow.(*arrow.MapType); ok {
		return DataTypeFromArrow(mapType.ItemType()), true
	}
	return DataType{}, false
}

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

func (t DataType) TimeZone() (string, bool) {
	if timestamp, ok := t.arrow.(*arrow.TimestampType); ok {
		return timestamp.TimeZone, true
	}
	return "", false
}

type Field struct {
	Name     string
	Type     DataType
	Nullable bool
	Metadata arrow.Metadata
}

func (f Field) toArrow() arrow.Field {
	return arrow.Field{Name: f.Name, Type: f.Type.arrow, Nullable: f.Nullable, Metadata: f.Metadata}
}

func fieldFromArrow(f arrow.Field) Field {
	return Field{Name: f.Name, Type: DataTypeFromArrow(f.Type), Nullable: f.Nullable, Metadata: f.Metadata}
}

type Schema struct{ arrow *arrow.Schema }

func NewSchema(fields ...Field) Schema {
	return NewSchemaWithMetadata(fields, arrow.Metadata{})
}

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

func SchemaFromArrow(schema *arrow.Schema) Schema { return Schema{arrow: schema} }
func (s Schema) ArrowSchema() *arrow.Schema       { return s.arrow }
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
func (s Schema) Metadata() arrow.Metadata {
	if s.arrow == nil {
		return arrow.Metadata{}
	}
	return s.arrow.Metadata()
}
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

func SeriesFromArrow(name string, values arrow.Array) *Series {
	values.Retain()
	return newSeriesWithData(name, arrow.NewChunked(values.DataType(), []arrow.Array{values}))
}

func NewSeriesFromArrow(name string, values arrow.Array) (*Series, error) {
	if values == nil {
		return nil, fmt.Errorf("golars: nil Arrow array")
	}
	return SeriesFromArrow(name, values), nil
}

func SeriesFromArrowChunked(name string, values *arrow.Chunked) *Series {
	values.Retain()
	return newSeriesWithData(name, values)
}

func (s *Series) Name() string { return s.name }
func (s *Series) DataType() DataType {
	if s == nil || s.data == nil {
		return DataType{}
	}
	return DataTypeFromArrow(s.data.DataType())
}
func (s *Series) Len() int64 {
	if s == nil || s.data == nil {
		return 0
	}
	return int64(s.data.Len())
}
func (s *Series) NullCount() int64 {
	if s == nil || s.data == nil {
		return 0
	}
	return int64(s.data.NullN())
}
func (s *Series) ArrowChunked() *arrow.Chunked {
	if s == nil || s.data == nil {
		return nil
	}
	s.data.Retain()
	return s.data
}
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
