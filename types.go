package golars

import (
	"fmt"
	"reflect"
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
	Null        = DataType{arrow.Null}
	Boolean     = DataType{arrow.FixedWidthTypes.Boolean}
	Int8        = DataType{arrow.PrimitiveTypes.Int8}
	Int16       = DataType{arrow.PrimitiveTypes.Int16}
	Int32       = DataType{arrow.PrimitiveTypes.Int32}
	Int64       = DataType{arrow.PrimitiveTypes.Int64}
	Uint8       = DataType{arrow.PrimitiveTypes.Uint8}
	Uint16      = DataType{arrow.PrimitiveTypes.Uint16}
	Uint32      = DataType{arrow.PrimitiveTypes.Uint32}
	Uint64      = DataType{arrow.PrimitiveTypes.Uint64}
	Float32     = DataType{arrow.PrimitiveTypes.Float32}
	Float64     = DataType{arrow.PrimitiveTypes.Float64}
	String      = DataType{arrow.BinaryTypes.String}
	LargeString = DataType{arrow.BinaryTypes.LargeString}
	Binary      = DataType{arrow.BinaryTypes.Binary}
	Date32      = DataType{arrow.FixedWidthTypes.Date32}
	Date64      = DataType{arrow.FixedWidthTypes.Date64}
)

func Timestamp(unit arrow.TimeUnit, timezone string) DataType {
	return DataType{&arrow.TimestampType{Unit: unit, TimeZone: timezone}}
}

func Duration(unit arrow.TimeUnit) DataType { return DataType{&arrow.DurationType{Unit: unit}} }
func ListOf(elem DataType) DataType         { return DataType{arrow.ListOf(elem.arrow)} }

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
func (t DataType) String() string {
	if t.arrow == nil {
		return "<nil>"
	}
	return t.arrow.String()
}
func (t DataType) Equal(other DataType) bool { return arrow.TypeEqual(t.arrow, other.arrow) }

type Field struct {
	Name     string
	Type     DataType
	Nullable bool
}

func (f Field) toArrow() arrow.Field {
	return arrow.Field{Name: f.Name, Type: f.Type.arrow, Nullable: f.Nullable}
}

func fieldFromArrow(f arrow.Field) Field {
	return Field{Name: f.Name, Type: DataTypeFromArrow(f.Type), Nullable: f.Nullable}
}

type Schema struct{ arrow *arrow.Schema }

func NewSchema(fields ...Field) Schema {
	afs := make([]arrow.Field, len(fields))
	for i, field := range fields {
		afs[i] = field.toArrow()
	}
	return Schema{arrow: arrow.NewSchema(afs, nil)}
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

// Series is an immutable named Arrow chunked array. Call Close when finished.
type Series struct {
	name   string
	data   *arrow.Chunked
	once   sync.Once
	closed bool
}

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
		if timestamp, ok := value.(time.Time); ok {
			value = arrow.Timestamp(timestamp.UnixNano())
		}
		s, scalarErr := scalar.MakeScalarParam(value, dt)
		if scalarErr != nil {
			return nil, fmt.Errorf("golars: series %q value %d: %w", name, i, scalarErr)
		}
		if appendErr := scalar.Append(b, s); appendErr != nil {
			return nil, fmt.Errorf("golars: series %q value %d: %w", name, i, appendErr)
		}
	}
	arr := b.NewArray()
	defer arr.Release()
	return SeriesFromArrow(name, arr), nil
}

func typeForGo(t reflect.Type) (arrow.DataType, error) {
	if t.Kind() == reflect.Pointer {
		t = t.Elem()
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
	case reflect.Struct:
		if t == reflect.TypeOf(time.Time{}) {
			return &arrow.TimestampType{Unit: arrow.Nanosecond}, nil
		}
		return nil, fmt.Errorf("golars: unsupported Go series element type %v", t)
	default:
		return nil, fmt.Errorf("golars: unsupported Go series element type %v", t)
	}
}

func SeriesFromArrow(name string, values arrow.Array) *Series {
	values.Retain()
	return &Series{name: name, data: arrow.NewChunked(values.DataType(), []arrow.Array{values})}
}

func NewSeriesFromArrow(name string, values arrow.Array) (*Series, error) {
	if values == nil {
		return nil, fmt.Errorf("golars: nil Arrow array")
	}
	return SeriesFromArrow(name, values), nil
}

func SeriesFromArrowChunked(name string, values *arrow.Chunked) *Series {
	values.Retain()
	return &Series{name: name, data: values}
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
	s.once.Do(func() {
		s.closed = true
		if s.data != nil {
			s.data.Release()
			s.data = nil
		}
	})
}
