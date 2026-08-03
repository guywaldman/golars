package core

import (
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// newFastSeries handles the common concrete slice types without reflection or
// one scalar allocation per value. Less common and nullable forms continue
// through the general path in types.go.
func newFastSeries(mem memory.Allocator, name string, values any) (*Series, bool, error) {
	var builder array.Builder
	switch values := values.(type) {
	case []bool:
		b := array.NewBooleanBuilder(mem)
		b.AppendValues(values, nil)
		builder = b
	case []int:
		b := array.NewInt64Builder(mem)
		for _, value := range values {
			b.Append(int64(value))
		}
		builder = b
	case []int8:
		b := array.NewInt8Builder(mem)
		b.AppendValues(values, nil)
		builder = b
	case []int16:
		b := array.NewInt16Builder(mem)
		b.AppendValues(values, nil)
		builder = b
	case []int32:
		b := array.NewInt32Builder(mem)
		b.AppendValues(values, nil)
		builder = b
	case []int64:
		b := array.NewInt64Builder(mem)
		b.AppendValues(values, nil)
		builder = b
	case []uint:
		b := array.NewUint64Builder(mem)
		for _, value := range values {
			b.Append(uint64(value))
		}
		builder = b
	case []uint8:
		b := array.NewUint8Builder(mem)
		b.AppendValues(values, nil)
		builder = b
	case []uint16:
		b := array.NewUint16Builder(mem)
		b.AppendValues(values, nil)
		builder = b
	case []uint32:
		b := array.NewUint32Builder(mem)
		b.AppendValues(values, nil)
		builder = b
	case []uint64:
		b := array.NewUint64Builder(mem)
		b.AppendValues(values, nil)
		builder = b
	case []float32:
		b := array.NewFloat32Builder(mem)
		b.AppendValues(values, nil)
		builder = b
	case []float64:
		b := array.NewFloat64Builder(mem)
		b.AppendValues(values, nil)
		builder = b
	case []string:
		b := array.NewStringBuilder(mem)
		b.AppendValues(values, nil)
		builder = b
	case []time.Time:
		b := array.NewTimestampBuilder(mem, &arrow.TimestampType{Unit: arrow.Nanosecond})
		for _, value := range values {
			b.Append(arrow.Timestamp(value.UnixNano()))
		}
		builder = b
	case []time.Duration:
		b := array.NewDurationBuilder(mem, &arrow.DurationType{Unit: arrow.Nanosecond})
		for _, value := range values {
			b.Append(arrow.Duration(value))
		}
		builder = b
	default:
		return nil, false, nil
	}
	arrayValue := builder.NewArray()
	builder.Release()
	defer arrayValue.Release()
	return SeriesFromArrow(name, arrayValue), true, nil
}
