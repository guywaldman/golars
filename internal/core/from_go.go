package core

import (
	"fmt"
	"reflect"
	"sort"
	"strings"

	"github.com/guywaldman/golars/internal/jsonio"
)

// DataFrameFromMaps creates an eager frame from row-oriented maps. Columns are
// sorted by name, and a missing key is represented as null.
func DataFrameFromMaps(rows []map[string]any) (*DataFrame, error) {
	return dataFrameFromObjects(getDefaultEngine(), rows, nil)
}

// NewDataFrameFromMaps is an alias for DataFrameFromMaps.
func NewDataFrameFromMaps(rows []map[string]any) (*DataFrame, error) {
	return DataFrameFromMaps(rows)
}

// DataFrameFromStructs creates an eager frame from exported fields in rows.
// Field names use the json tag when present, falling back to the Go field
// name. A json:"-" field is omitted.
func DataFrameFromStructs[T any](rows []T) (*DataFrame, error) {
	engine := getDefaultEngine()
	if err := engine.checkOpen(); err != nil {
		return nil, err
	}
	if len(rows) == 0 {
		return nil, fmt.Errorf("golars: DataFrameFromStructs needs at least one row")
	}

	rowType := reflect.TypeOf((*T)(nil)).Elem()
	for rowType.Kind() == reflect.Pointer {
		rowType = rowType.Elem()
	}
	if rowType.Kind() != reflect.Struct {
		return nil, fmt.Errorf("golars: DataFrameFromStructs expects a struct type, got %s", rowType)
	}
	fields, err := exportedStructFields(rowType)
	if err != nil {
		return nil, err
	}
	objects := make([]map[string]any, len(rows))
	for index, row := range rows {
		object := make(map[string]any, len(fields))
		value := reflect.ValueOf(row)
		for value.IsValid() && (value.Kind() == reflect.Pointer || value.Kind() == reflect.Interface) {
			if value.IsNil() {
				value = reflect.Value{}
				break
			}
			value = value.Elem()
		}
		if value.IsValid() {
			for _, field := range fields {
				fieldValue := value.Field(field.index)
				object[field.name] = exportedValue(fieldValue)
			}
		}
		objects[index] = object
	}
	columns := make([]string, len(fields))
	for index, field := range fields {
		columns[index] = field.name
	}
	return dataFrameFromObjects(engine, objects, columns)
}

// NewDataFrameFromStructs is an alias for DataFrameFromStructs.
func NewDataFrameFromStructs[T any](rows []T) (*DataFrame, error) {
	return DataFrameFromStructs(rows)
}

type structField struct {
	index int
	name  string
}

func exportedStructFields(rowType reflect.Type) ([]structField, error) {
	fields := make([]structField, 0, rowType.NumField())
	seen := make(map[string]struct{}, rowType.NumField())
	for index := 0; index < rowType.NumField(); index++ {
		field := rowType.Field(index)
		if field.PkgPath != "" { // unexported
			continue
		}
		name, include := structFieldName(field)
		if !include {
			continue
		}
		if _, exists := seen[name]; exists {
			return nil, fmt.Errorf("golars: duplicate struct column name %q", name)
		}
		seen[name] = struct{}{}
		fields = append(fields, structField{index: index, name: name})
	}
	if len(fields) == 0 {
		return nil, fmt.Errorf("golars: DataFrameFromStructs found no exported fields")
	}
	return fields, nil
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

func exportedValue(value reflect.Value) any {
	for value.IsValid() && (value.Kind() == reflect.Pointer || value.Kind() == reflect.Interface) {
		if value.IsNil() {
			return nil
		}
		value = value.Elem()
	}
	if !value.IsValid() || !value.CanInterface() {
		return nil
	}
	return value.Interface()
}

func dataFrameFromObjects(engine *Engine, rows []map[string]any, columns []string) (*DataFrame, error) {
	if engine == nil {
		engine = getDefaultEngine()
	}
	if err := engine.checkOpen(); err != nil {
		return nil, err
	}
	if len(rows) == 0 {
		return nil, fmt.Errorf("golars: DataFrameFromMaps needs at least one row")
	}
	if len(columns) == 0 {
		for _, row := range rows {
			for name := range row {
				columns = append(columns, name)
			}
		}
		sort.Strings(columns)
		if len(columns) == 0 {
			return nil, fmt.Errorf("golars: DataFrameFromMaps found no columns")
		}
	}
	table, err := jsonio.TableFromObjects(rows, engine.allocator, columns, missingColumn)
	if err != nil {
		return nil, err
	}
	return newDataFrameWithTable(table, engine), nil
}
