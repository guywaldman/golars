package core

import (
	"fmt"
	"reflect"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
)

// dataTypeFromGoValue derives an Arrow schema from a Go value. It is kept
// separate from the row constructor so expression APIs can use the same
// reflection rules without serializing through JSON.
func dataTypeFromGoValue(value any) (DataType, error) {
	if value == nil {
		return DataType{}, fmt.Errorf("golars: JSON decode target cannot be nil")
	}
	return dataTypeFromGoType(reflect.TypeOf(value))
}

func dataTypeFromGoType(typeOf reflect.Type) (DataType, error) {
	dtype, _, err := dataTypeFromGoTypeWithNullability(typeOf)
	return dtype, err
}

func dataTypeFromGoTypeWithNullability(typeOf reflect.Type) (DataType, bool, error) {
	if typeOf == nil {
		return DataType{}, false, fmt.Errorf("golars: JSON decode target cannot be nil")
	}

	nullable := false
	for typeOf.Kind() == reflect.Pointer {
		nullable = true
		typeOf = typeOf.Elem()
	}
	if typeOf == reflect.TypeOf(time.Time{}) {
		return Timestamp(arrow.Nanosecond, ""), nullable, nil
	}
	if typeOf == reflect.TypeOf(time.Duration(0)) {
		return Duration(arrow.Nanosecond), nullable, nil
	}

	var dtype DataType
	switch typeOf.Kind() {
	case reflect.Bool:
		dtype = Boolean
	case reflect.Int:
		dtype = Int64
	case reflect.Int8:
		dtype = Int8
	case reflect.Int16:
		dtype = Int16
	case reflect.Int32:
		dtype = Int32
	case reflect.Int64:
		dtype = Int64
	case reflect.Uint:
		dtype = Uint64
	case reflect.Uint8:
		dtype = Uint8
	case reflect.Uint16:
		dtype = Uint16
	case reflect.Uint32:
		dtype = Uint32
	case reflect.Uint64:
		dtype = Uint64
	case reflect.Float32:
		dtype = Float32
	case reflect.Float64:
		dtype = Float64
	case reflect.String:
		dtype = String
	case reflect.Slice:
		if typeOf.Elem().Kind() == reflect.Uint8 {
			dtype = Binary
			break
		}
		element, _, err := dataTypeFromGoTypeWithNullability(typeOf.Elem())
		if err != nil {
			return DataType{}, false, fmt.Errorf("golars: JSON decode list element %s: %w", typeOf.Elem(), err)
		}
		dtype = ListOf(element)
	case reflect.Struct:
		fields, err := dataTypeFieldsFromGoStruct(typeOf)
		if err != nil {
			return DataType{}, false, err
		}
		dtype = StructOf(fields...)
	default:
		return DataType{}, false, fmt.Errorf("golars: JSON decode does not support Go type %s", typeOf)
	}
	return dtype, nullable, nil
}

func dataTypeFieldsFromGoStruct(typeOf reflect.Type) ([]Field, error) {
	fields := make([]Field, 0, typeOf.NumField())
	seen := make(map[string]struct{}, typeOf.NumField())
	for index := 0; index < typeOf.NumField(); index++ {
		field := typeOf.Field(index)
		if field.PkgPath != "" {
			continue
		}
		name, include := structFieldName(field)
		if !include {
			continue
		}
		if _, exists := seen[name]; exists {
			return nil, fmt.Errorf("golars: duplicate struct field name %q", name)
		}
		seen[name] = struct{}{}
		fieldType, nullable, err := dataTypeFromGoTypeWithNullability(field.Type)
		if err != nil {
			return nil, fmt.Errorf("golars: JSON decode field %q: %w", name, err)
		}
		fields = append(fields, Field{Name: name, Type: fieldType, Nullable: nullable})
	}
	if len(fields) == 0 {
		return nil, fmt.Errorf("golars: JSON decode target %s has no exported fields", typeOf)
	}
	return fields, nil
}
