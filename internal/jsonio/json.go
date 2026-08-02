package jsonio

import (
	"encoding/json"
	"fmt"
	"io"
	"sort"
	"strings"

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
		filtered := names[:0]
		for _, name := range names {
			if _, keep := projected[name]; keep {
				filtered = append(filtered, name)
			}
		}
		names = filtered
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
			if !exists || value == nil {
				builders[i].AppendNull()
				continue
			}
			sc, err := toScalar(value, fields[i].Type)
			if err != nil {
				return nil, err
			}
			if err := scalar.Append(builders[i], sc); err != nil {
				return nil, err
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
		value := object[name]
		switch value := value.(type) {
		case bool:
			return arrow.FixedWidthTypes.Boolean
		case json.Number:
			if strings.ContainsAny(value.String(), ".eE") {
				return arrow.PrimitiveTypes.Float64
			}
			return arrow.PrimitiveTypes.Int64
		case float64:
			return arrow.PrimitiveTypes.Float64
		case string:
			return arrow.BinaryTypes.String
		case []any, map[string]any:
			return arrow.BinaryTypes.String
		}
	}
	return arrow.BinaryTypes.String
}

func toScalar(value any, dt arrow.DataType) (scalar.Scalar, error) {
	switch value := value.(type) {
	case json.Number:
		return scalar.MakeScalarParam(value.String(), dt)
	case []any, map[string]any:
		encoded, err := json.Marshal(value)
		if err != nil {
			return nil, err
		}
		return scalar.MakeScalarParam(string(encoded), dt)
	default:
		return scalar.MakeScalarParam(value, dt)
	}
}
