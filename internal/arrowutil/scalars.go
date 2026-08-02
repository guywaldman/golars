package arrowutil

import (
	"strconv"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/scalar"
)

// ArraysAsSlices wraps one array per chunk for table constructors that accept
// chunked columns.
func ArraysAsSlices(arrays []arrow.Array) [][]arrow.Array {
	data := make([][]arrow.Array, len(arrays))
	for i, arr := range arrays {
		data[i] = []arrow.Array{arr}
	}
	return data
}

// ScalarNumber converts Arrow numeric scalars to float64. The string fallback
// keeps custom numeric scalar implementations comparable.
func ScalarNumber(value scalar.Scalar) (float64, bool) {
	switch v := value.(type) {
	case *scalar.Int8:
		return float64(v.Value), true
	case *scalar.Int16:
		return float64(v.Value), true
	case *scalar.Int32:
		return float64(v.Value), true
	case *scalar.Int64:
		return float64(v.Value), true
	case *scalar.Uint8:
		return float64(v.Value), true
	case *scalar.Uint16:
		return float64(v.Value), true
	case *scalar.Uint32:
		return float64(v.Value), true
	case *scalar.Uint64:
		return float64(v.Value), true
	case *scalar.Float32:
		return float64(v.Value), true
	case *scalar.Float64:
		return v.Value, true
	default:
		number, err := strconv.ParseFloat(value.String(), 64)
		return number, err == nil
	}
}
