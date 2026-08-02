package core

import (
	"fmt"

	"github.com/apache/arrow-go/v18/arrow"
)

// JSONErrorMode controls how invalid JSON rows are handled.
type JSONErrorMode uint8

const (
	// JSONNullOnError replaces malformed or incompatible rows with null.
	JSONNullOnError JSONErrorMode = iota
	// JSONErrorOnError returns the first row-level decoding error.
	JSONErrorOnError
)

// JSONDecodeOption configures DecodeJSON.
type JSONDecodeOption func(*jsonDecodeOptions)

type jsonDecodeOptions struct {
	errorMode      JSONErrorMode
	coerce         bool
	strictRequired bool
}

// WithJSONErrorMode sets the error policy for DecodeJSON.
func WithJSONErrorMode(mode JSONErrorMode) JSONDecodeOption {
	return func(options *jsonDecodeOptions) {
		options.errorMode = mode
	}
}

// WithJSONCoerce enables or disables compatible JSON-to-type coercions.
func WithJSONCoerce(enabled bool) JSONDecodeOption {
	return func(options *jsonDecodeOptions) {
		options.coerce = enabled
	}
}

// WithJSONStrictRequiredFields makes non-nullable struct fields required.
func WithJSONStrictRequiredFields(enabled bool) JSONDecodeOption {
	return func(options *jsonDecodeOptions) {
		options.strictRequired = enabled
	}
}

type jsonDecodeConfig struct {
	schema         DataType
	errorMode      JSONErrorMode
	coerce         bool
	strictRequired bool
}

// DecodeJSON parses a JSON string expression into the supplied Arrow type.
// Struct schemas project named object fields and may contain nested structs
// and lists. Invalid rows become null by default.
func DecodeJSON(expr Expr, schema DataType, options ...JSONDecodeOption) Expr {
	config := jsonDecodeOptions{coerce: true}
	for _, option := range options {
		if option != nil {
			option(&config)
		}
	}
	return Expr{node: &exprNode{
		kind: exprCall,
		name: "json_decode",
		args: []*exprNode{expr.node},
		jsonDecode: &jsonDecodeConfig{
			schema:         schema,
			errorMode:      config.errorMode,
			coerce:         config.coerce,
			strictRequired: config.strictRequired,
		},
	}}
}

type nativeJSONField struct {
	Name     string         `json:"name"`
	Type     nativeJSONType `json:"type"`
	Nullable bool           `json:"nullable"`
}

type nativeJSONType struct {
	Kind     string            `json:"kind"`
	Elem     *nativeJSONType   `json:"elem,omitempty"`
	Fields   []nativeJSONField `json:"fields,omitempty"`
	Unit     string            `json:"unit,omitempty"`
	Timezone string            `json:"timezone,omitempty"`
}

func nativeJSONTypeFor(dtype DataType) (nativeJSONType, error) {
	result := nativeJSONType{}
	switch dtype.ID() {
	case arrow.NULL:
		result.Kind = "null"
	case arrow.BOOL:
		result.Kind = "bool"
	case arrow.INT8:
		result.Kind = "int8"
	case arrow.INT16:
		result.Kind = "int16"
	case arrow.INT32:
		result.Kind = "int32"
	case arrow.INT64:
		result.Kind = "int64"
	case arrow.UINT8:
		result.Kind = "uint8"
	case arrow.UINT16:
		result.Kind = "uint16"
	case arrow.UINT32:
		result.Kind = "uint32"
	case arrow.UINT64:
		result.Kind = "uint64"
	case arrow.FLOAT16:
		return nativeJSONType{}, fmt.Errorf("golars: JSON decode does not support float16 schemas")
	case arrow.FLOAT32:
		result.Kind = "float32"
	case arrow.FLOAT64:
		result.Kind = "float64"
	case arrow.STRING, arrow.STRING_VIEW:
		result.Kind = "string"
	case arrow.LARGE_STRING:
		result.Kind = "large_string"
	case arrow.BINARY, arrow.BINARY_VIEW:
		result.Kind = "binary"
	case arrow.LARGE_BINARY:
		result.Kind = "large_binary"
	case arrow.DATE32:
		result.Kind = "date32"
	case arrow.DATE64:
		result.Kind = "date64"
	case arrow.TIMESTAMP:
		result.Kind = "timestamp"
		unit, _ := dtype.TimeUnit()
		result.Unit = nativeTimeUnit(unit)
		result.Timezone, _ = dtype.TimeZone()
	case arrow.DURATION:
		result.Kind = "duration"
		unit, _ := dtype.TimeUnit()
		result.Unit = nativeTimeUnit(unit)
	case arrow.LIST, arrow.LARGE_LIST:
		elem, ok := dtype.Elem()
		if !ok {
			return nativeJSONType{}, fmt.Errorf("golars: JSON decode list schema has no element type")
		}
		converted, err := nativeJSONTypeFor(elem)
		if err != nil {
			return nativeJSONType{}, err
		}
		result.Kind = "list"
		result.Elem = &converted
	case arrow.STRUCT:
		result.Kind = "struct"
		for _, field := range dtype.Fields() {
			converted, err := nativeJSONTypeFor(field.Type)
			if err != nil {
				return nativeJSONType{}, err
			}
			result.Fields = append(result.Fields, nativeJSONField{
				Name:     field.Name,
				Type:     converted,
				Nullable: field.Nullable,
			})
		}
	default:
		return nativeJSONType{}, fmt.Errorf("golars: JSON decode does not support schema %s", dtype)
	}
	return result, nil
}

func nativeTimeUnit(unit arrow.TimeUnit) string {
	switch unit {
	case arrow.Second:
		return "s"
	case arrow.Millisecond:
		return "ms"
	case arrow.Microsecond:
		return "us"
	case arrow.Nanosecond:
		return "ns"
	default:
		return "ns"
	}
}
