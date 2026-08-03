package jsonio

import (
	"encoding/json"
	"fmt"
	"io"
	"reflect"
	"sort"
	"strings"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/arrow/scalar"
	"github.com/guywaldman/golars/internal/arrowutil"
)

// MissingColumnError constructs the caller's public missing-column error.
type MissingColumnError func(name string, available []string) error

// ScalarValue converts one Arrow value to a JSON-compatible value.
func ScalarValue(arr arrow.Array, row int) (any, error) {
	value, err := scalar.GetScalar(arr, row)
	if err != nil {
		return nil, err
	}
	if !value.IsValid() {
		return nil, nil
	}
	switch value := value.(type) {
	case *scalar.Boolean:
		return value.Value, nil
	}
	if number, ok := arrowutil.ScalarNumber(value); ok && (arrow.IsInteger(value.DataType().ID()) || arrow.IsFloating(value.DataType().ID())) {
		return number, nil
	}
	return value.String(), nil
}

// ReadTable decodes newline-delimited JSON or a JSON array of objects into an
// Arrow table. Projection is applied while constructing the table.
func ReadTable(reader io.Reader, mem memory.Allocator, arrayInput bool, columns []string, missingColumn MissingColumnError) (arrow.Table, error) {
	decoder := json.NewDecoder(reader)
	decoder.UseNumber()
	var values []map[string]any
	if arrayInput {
		token, err := decoder.Token()
		if err != nil {
			return nil, err
		}
		if delimiter, ok := token.(json.Delim); !ok || delimiter != '[' {
			return nil, fmt.Errorf("golars: JSON input must be an array of objects")
		}
		for decoder.More() {
			var object map[string]any
			if err := decoder.Decode(&object); err != nil {
				return nil, err
			}
			values = append(values, object)
		}
		if _, err := decoder.Token(); err != nil {
			return nil, err
		}
	} else {
		for {
			var object map[string]any
			err := decoder.Decode(&object)
			if err == io.EOF {
				break
			}
			if err != nil {
				return nil, err
			}
			values = append(values, object)
		}
	}
	return tableFromObjects(values, mem, columns, missingColumn)
}

// TableFromObjects builds an Arrow table from row-oriented Go maps. Missing
// keys are appended as nulls; columns are sorted unless an explicit order is
// supplied.
func TableFromObjects(values []map[string]any, mem memory.Allocator, columns []string, missingColumn MissingColumnError) (arrow.Table, error) {
	return tableFromObjects(values, mem, columns, missingColumn)
}

func tableFromObjects(values []map[string]any, mem memory.Allocator, projectedColumns []string, missingColumn MissingColumnError) (arrow.Table, error) {
	namesSet := map[string]struct{}{}
	for _, object := range values {
		for name := range object {
			namesSet[name] = struct{}{}
		}
	}
	names := make([]string, 0, len(namesSet))
	for name := range namesSet {
		names = append(names, name)
	}
	sort.Strings(names)
	if len(projectedColumns) > 0 {
		projected := make(map[string]struct{}, len(projectedColumns))
		for _, name := range projectedColumns {
			if _, exists := namesSet[name]; !exists {
				if missingColumn != nil {
					return nil, missingColumn(name, names)
				}
				return nil, fmt.Errorf("golars: column not found: %q", name)
			}
			projected[name] = struct{}{}
		}
		ordered := make([]string, 0, len(projectedColumns))
		seenOrdered := make(map[string]struct{}, len(projectedColumns))
		for _, name := range projectedColumns {
			if _, keep := projected[name]; !keep {
				continue
			}
			if _, seen := seenOrdered[name]; seen {
				continue
			}
			seenOrdered[name] = struct{}{}
			ordered = append(ordered, name)
		}
		names = ordered
	}
	fields := make([]arrow.Field, len(names))
	builders := make([]array.Builder, len(names))
	for i, name := range names {
		dt := inferType(values, name)
		fields[i] = arrow.Field{Name: name, Type: dt, Nullable: true}
		builders[i] = array.NewBuilder(mem, dt)
	}
	defer func() {
		for _, builder := range builders {
			builder.Release()
		}
	}()
	for _, object := range values {
		for i, name := range names {
			value, exists := object[name]
			value = indirectValue(value)
			if !exists || value == nil {
				builders[i].AppendNull()
				continue
			}
			sc, err := toScalar(value, fields[i].Type, mem)
			if err != nil {
				return nil, err
			}
			if err := scalar.Append(builders[i], sc); err != nil {
				if releaser, ok := sc.(interface{ Release() }); ok {
					releaser.Release()
				}
				return nil, err
			}
			if releaser, ok := sc.(interface{ Release() }); ok {
				releaser.Release()
			}
		}
	}
	columns := make([]arrow.Array, len(builders))
	for i, builder := range builders {
		columns[i] = builder.NewArray()
	}
	defer arrowutil.ReleaseArrays(columns)
	return array.NewTableFromSlice(arrow.NewSchema(fields, nil), arrowutil.ArraysAsSlices(columns)), nil
}

func inferType(values []map[string]any, name string) arrow.DataType {
	for _, object := range values {
		if value, ok := object[name]; ok && value != nil {
			if dtype, ok := typeOfValue(value); ok {
				return dtype
			}
		}
	}
	return arrow.BinaryTypes.String
}

func typeOfValue(value any) (arrow.DataType, bool) {
	value = indirectValue(value)
	if value == nil {
		return nil, false
	}
	switch value := value.(type) {
	case bool:
		return arrow.FixedWidthTypes.Boolean, true
	case json.Number:
		if strings.ContainsAny(value.String(), ".eE") {
			return arrow.PrimitiveTypes.Float64, true
		}
		return arrow.PrimitiveTypes.Int64, true
	case float32:
		return arrow.PrimitiveTypes.Float32, true
	case float64:
		return arrow.PrimitiveTypes.Float64, true
	case string:
		return arrow.BinaryTypes.String, true
	case []byte:
		return arrow.BinaryTypes.Binary, true
	case time.Time:
		return &arrow.TimestampType{Unit: arrow.Nanosecond}, true
	case time.Duration:
		return &arrow.DurationType{Unit: arrow.Nanosecond}, true
	}
	return typeForReflect(reflect.TypeOf(value))
}

func typeForReflect(typeOf reflect.Type) (arrow.DataType, bool) {
	for typeOf != nil && (typeOf.Kind() == reflect.Pointer || typeOf.Kind() == reflect.Interface) {
		typeOf = typeOf.Elem()
	}
	if typeOf == nil {
		return nil, false
	}
	if typeOf == reflect.TypeOf(time.Time{}) {
		return &arrow.TimestampType{Unit: arrow.Nanosecond}, true
	}
	if typeOf == reflect.TypeOf(time.Duration(0)) {
		return &arrow.DurationType{Unit: arrow.Nanosecond}, true
	}
	switch typeOf.Kind() {
	case reflect.Bool:
		return arrow.FixedWidthTypes.Boolean, true
	case reflect.Int:
		return arrow.PrimitiveTypes.Int64, true
	case reflect.Int8:
		return arrow.PrimitiveTypes.Int8, true
	case reflect.Int16:
		return arrow.PrimitiveTypes.Int16, true
	case reflect.Int32:
		return arrow.PrimitiveTypes.Int32, true
	case reflect.Int64:
		return arrow.PrimitiveTypes.Int64, true
	case reflect.Uint:
		return arrow.PrimitiveTypes.Uint64, true
	case reflect.Uint8:
		return arrow.PrimitiveTypes.Uint8, true
	case reflect.Uint16:
		return arrow.PrimitiveTypes.Uint16, true
	case reflect.Uint32:
		return arrow.PrimitiveTypes.Uint32, true
	case reflect.Uint64:
		return arrow.PrimitiveTypes.Uint64, true
	case reflect.Float32:
		return arrow.PrimitiveTypes.Float32, true
	case reflect.Float64:
		return arrow.PrimitiveTypes.Float64, true
	case reflect.String:
		return arrow.BinaryTypes.String, true
	case reflect.Slice:
		if typeOf.Elem().Kind() == reflect.Uint8 {
			return arrow.BinaryTypes.Binary, true
		}
		if elementType, ok := typeForReflect(typeOf.Elem()); ok {
			return arrow.ListOf(elementType), true
		}
	case reflect.Struct:
		fields := make([]arrow.Field, 0, typeOf.NumField())
		for index := 0; index < typeOf.NumField(); index++ {
			field := typeOf.Field(index)
			if field.PkgPath != "" {
				continue
			}
			name, include := structFieldName(field)
			if !include {
				continue
			}
			fieldType, ok := typeForReflect(field.Type)
			if !ok {
				return nil, false
			}
			fields = append(fields, arrow.Field{Name: name, Type: fieldType, Nullable: true})
		}
		if len(fields) > 0 {
			return arrow.StructOf(fields...), true
		}
	}
	return nil, false
}

func structFieldName(field reflect.StructField) (string, bool) {
	if tag, ok := field.Tag.Lookup("json"); ok {
		name := strings.Split(tag, ",")[0]
		if name == "-" {
			return "", false
		}
		if name != "" {
			return name, true
		}
	}
	if tag := field.Tag.Get("compute"); tag != "" {
		if tag == "-" {
			return "", false
		}
		return tag, true
	}
	return field.Name, true
}

func toScalar(value any, dt arrow.DataType, mem memory.Allocator) (result scalar.Scalar, err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			result = nil
			err = fmt.Errorf("golars: cannot build %s scalar from %T", dt, value)
		}
	}()
	value = indirectValue(value)
	if value == nil {
		return scalar.MakeNullScalar(dt), nil
	}
	value = normalizeScalarValue(value, dt)
	if dt.ID() == arrow.STRUCT {
		return structScalar(value, dt.(*arrow.StructType), mem)
	}
	switch value := value.(type) {
	case json.Number:
		return scalar.MakeScalarParam(value.String(), dt)
	case time.Time:
		if dt.ID() == arrow.TIMESTAMP {
			return scalar.MakeScalarParam(arrow.Timestamp(value.UnixNano()), dt)
		}
	case time.Duration:
		if dt.ID() == arrow.DURATION {
			return scalar.MakeScalarParam(arrow.Duration(value), dt)
		}
	case []any, map[string]any:
		encoded, err := json.Marshal(value)
		if err != nil {
			return nil, err
		}
		return scalar.MakeScalarParam(string(encoded), dt)
	default:
		valueType := reflect.TypeOf(value)
		if valueType != nil && (valueType.Kind() == reflect.Slice || valueType.Kind() == reflect.Struct) {
			parsed, err := scalar.ToScalar(value, mem)
			if err == nil && arrow.TypeEqual(parsed.DataType(), dt) {
				return parsed, nil
			}
			if parsed != nil {
				if releaser, ok := parsed.(interface{ Release() }); ok {
					releaser.Release()
				}
			}
		}
	}
	return scalar.MakeScalarParam(value, dt)
}

func structScalar(value any, dtype *arrow.StructType, mem memory.Allocator) (scalar.Scalar, error) {
	reflected := reflect.ValueOf(value)
	for reflected.IsValid() && (reflected.Kind() == reflect.Pointer || reflected.Kind() == reflect.Interface) {
		if reflected.IsNil() {
			return scalar.MakeNullScalar(dtype), nil
		}
		reflected = reflected.Elem()
	}
	if !reflected.IsValid() || reflected.Kind() != reflect.Struct {
		return nil, fmt.Errorf("expected a struct value, got %T", value)
	}
	values := make([]scalar.Scalar, len(dtype.Fields()))
	for index, field := range dtype.Fields() {
		fieldValue, ok := structValueField(reflected, field.Name)
		if !ok || !fieldValue.IsValid() || ((fieldValue.Kind() == reflect.Pointer || fieldValue.Kind() == reflect.Interface) && fieldValue.IsNil()) {
			values[index] = scalar.MakeNullScalar(field.Type)
			continue
		}
		child, err := toScalar(fieldValue.Interface(), field.Type, mem)
		if err != nil {
			return nil, err
		}
		values[index] = child
	}
	return scalar.NewStructScalar(values, dtype), nil
}

func structValueField(value reflect.Value, name string) (reflect.Value, bool) {
	typeOf := value.Type()
	for index := 0; index < typeOf.NumField(); index++ {
		field := typeOf.Field(index)
		if field.PkgPath != "" {
			continue
		}
		fieldName, include := structFieldName(field)
		if include && fieldName == name {
			return value.Field(index), true
		}
	}
	return reflect.Value{}, false
}

func normalizeScalarValue(value any, dt arrow.DataType) any {
	reflected := reflect.ValueOf(value)
	if !reflected.IsValid() {
		return value
	}
	switch dt.ID() {
	case arrow.BOOL:
		if reflected.Kind() == reflect.Bool {
			return reflected.Bool()
		}
	case arrow.STRING, arrow.LARGE_STRING:
		if reflected.Kind() == reflect.String {
			return reflected.String()
		}
	case arrow.FLOAT32:
		if reflected.Kind() == reflect.Float32 || reflected.Kind() == reflect.Float64 {
			return float32(reflected.Float())
		}
	case arrow.FLOAT64:
		if reflected.Kind() == reflect.Float32 || reflected.Kind() == reflect.Float64 {
			return reflected.Float()
		}
	}
	return value
}

func indirectValue(value any) any {
	reflected := reflect.ValueOf(value)
	for reflected.IsValid() && (reflected.Kind() == reflect.Pointer || reflected.Kind() == reflect.Interface) {
		if reflected.IsNil() {
			return nil
		}
		reflected = reflected.Elem()
	}
	if !reflected.IsValid() || !reflected.CanInterface() {
		return nil
	}
	return reflected.Interface()
}
