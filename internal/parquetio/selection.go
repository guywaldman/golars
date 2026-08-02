package parquetio

import (
	"strings"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/scalar"
	parquetfile "github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/metadata"
	"github.com/guywaldman/golars/internal/arrowutil"
)

func rowGroupSelection(reader *parquetfile.Reader, schema *arrow.Schema, filters []Predicate, applicable bool, filterOnlyColumns []string) ([]int, []bool, []bool, bool, error) {
	if !applicable || len(filters) == 0 || schema == nil {
		return nil, nil, nil, false, nil
	}
	indices := make([]int, 0, reader.NumRowGroups())
	fullMatches := make([]bool, 0, reader.NumRowGroups())
	projectionMatches := make([]bool, 0, reader.NumRowGroups())
	for rowGroup := 0; rowGroup < reader.NumRowGroups(); rowGroup++ {
		mayMatch := true
		fullyMatches := true
		projectionFullyMatches := len(filterOnlyColumns) > 0
		hasProjectionPredicate := false
		for _, predicate := range filters {
			if !rowGroupMayMatch(reader.RowGroup(rowGroup), schema, predicate) {
				mayMatch = false
				break
			}
			predicateFullyMatches := rowGroupFullyMatches(reader.RowGroup(rowGroup), schema, predicate)
			if !predicateFullyMatches {
				fullyMatches = false
			}
			if containsColumn(filterOnlyColumns, predicate.Column) {
				hasProjectionPredicate = true
				if !predicateFullyMatches {
					projectionFullyMatches = false
				}
			}
		}
		if mayMatch {
			indices = append(indices, rowGroup)
			fullMatches = append(fullMatches, fullyMatches)
			if !hasProjectionPredicate {
				projectionFullyMatches = fullyMatches
			}
			projectionMatches = append(projectionMatches, projectionFullyMatches)
		}
	}
	return indices, fullMatches, projectionMatches, true, nil
}

// RowGroupIndices exposes pruning for package-level tests without exposing the
// implementation types used by the public scan API.
func RowGroupIndices(reader *parquetfile.Reader, schema *arrow.Schema, filters []Predicate, applicable bool) ([]int, error) {
	indices, _, _, _, err := rowGroupSelection(reader, schema, filters, applicable, nil)
	return indices, err
}

func containsColumn(columns []string, name string) bool {
	for _, column := range columns {
		if column == name {
			return true
		}
	}
	return false
}

func hasFullMatch(fullMatches []bool) bool {
	for _, full := range fullMatches {
		if full {
			return true
		}
	}
	return false
}

func rowGroupFullyMatches(rowGroup *parquetfile.RowGroupReader, schema *arrow.Schema, predicate Predicate) bool {
	fields := schema.FieldIndices(predicate.Column)
	if len(fields) != 1 || fields[0] >= rowGroup.NumColumns() {
		return false
	}
	field := schema.Field(fields[0])
	column, err := rowGroup.MetaData().ColumnChunk(fields[0])
	if err != nil {
		return false
	}
	stats, err := column.Statistics()
	if err != nil || stats == nil {
		return false
	}
	if predicate.Op == "is_null" || predicate.Op == "is_valid" {
		if !stats.HasNullCount() {
			return false
		}
		nulls := stats.NullCount()
		if predicate.Op == "is_null" {
			return nulls == rowGroup.NumRows()
		}
		return nulls == 0
	}
	if !stats.HasMinMax() || !stats.HasNullCount() || stats.NullCount() != 0 {
		return false
	}
	switch field.Type.ID() {
	case arrow.BOOL,
		arrow.INT8, arrow.INT16, arrow.INT32, arrow.INT64,
		arrow.UINT8, arrow.UINT16, arrow.UINT32, arrow.UINT64,
		arrow.DATE32, arrow.DATE64, arrow.TIMESTAMP:
	default:
		return false
	}
	minimum, maximum, ok := parquetStatBounds(stats, field.Type)
	if !ok {
		return false
	}
	literal, err := parquetLiteralScalar(predicate.Value, field.Type)
	if err != nil || literal == nil || !literal.IsValid() {
		return false
	}
	minComparison := comparePruningScalars(minimum, literal)
	maxComparison := comparePruningScalars(maximum, literal)
	switch predicate.Op {
	case "equal":
		return minComparison == 0 && maxComparison == 0
	case "not_equal":
		return minComparison > 0 || maxComparison < 0
	case "greater":
		return minComparison > 0
	case "greater_equal":
		return minComparison >= 0
	case "less":
		return maxComparison < 0
	case "less_equal":
		return maxComparison <= 0
	default:
		return false
	}
}

func rowGroupMayMatch(rowGroup *parquetfile.RowGroupReader, schema *arrow.Schema, predicate Predicate) bool {
	fields := schema.FieldIndices(predicate.Column)
	if len(fields) != 1 || fields[0] >= rowGroup.NumColumns() {
		return true
	}
	field := schema.Field(fields[0])
	column, err := rowGroup.MetaData().ColumnChunk(fields[0])
	if err != nil {
		return true
	}
	stats, err := column.Statistics()
	if err != nil || stats == nil {
		return true
	}
	if predicate.Op == "is_null" || predicate.Op == "is_valid" {
		if !stats.HasNullCount() {
			return true
		}
		nulls := stats.NullCount()
		if predicate.Op == "is_null" {
			return nulls > 0
		}
		return nulls < rowGroup.NumRows()
	}
	if !stats.HasMinMax() {
		return true
	}
	minimum, maximum, ok := parquetStatBounds(stats, field.Type)
	if !ok {
		return true
	}
	literal, err := parquetLiteralScalar(predicate.Value, field.Type)
	if err != nil || literal == nil || !literal.IsValid() {
		return true
	}
	minComparison := comparePruningScalars(minimum, literal)
	maxComparison := comparePruningScalars(maximum, literal)
	switch predicate.Op {
	case "equal":
		return minComparison <= 0 && maxComparison >= 0
	case "not_equal":
		return minComparison != 0 || maxComparison != 0
	case "greater":
		return maxComparison > 0
	case "greater_equal":
		return maxComparison >= 0
	case "less":
		return minComparison < 0
	case "less_equal":
		return minComparison <= 0
	default:
		return true
	}
}

func comparePruningScalars(left, right scalar.Scalar) int {
	if arrow.IsBaseBinary(left.DataType().ID()) || arrow.IsBaseBinary(right.DataType().ID()) {
		return strings.Compare(left.String(), right.String())
	}
	leftNumber, leftOK := arrowutil.ScalarNumber(left)
	rightNumber, rightOK := arrowutil.ScalarNumber(right)
	if leftOK && rightOK {
		if leftNumber < rightNumber {
			return -1
		}
		if leftNumber > rightNumber {
			return 1
		}
		return 0
	}
	return strings.Compare(left.String(), right.String())
}

func parquetStatBounds(stats metadata.TypedStatistics, dtype arrow.DataType) (scalar.Scalar, scalar.Scalar, bool) {
	var minimum, maximum any
	switch values := stats.(type) {
	case *metadata.Int32Statistics:
		minimum, maximum = values.Min(), values.Max()
	case *metadata.Int64Statistics:
		minimum, maximum = values.Min(), values.Max()
	case *metadata.Float32Statistics:
		minimum, maximum = values.Min(), values.Max()
	case *metadata.Float64Statistics:
		minimum, maximum = values.Min(), values.Max()
	case *metadata.BooleanStatistics:
		minimum, maximum = values.Min(), values.Max()
	case *metadata.ByteArrayStatistics:
		minimum, maximum = []byte(values.Min()), []byte(values.Max())
	default:
		return nil, nil, false
	}
	minScalar, err := parquetStatScalar(minimum, dtype)
	if err != nil {
		return nil, nil, false
	}
	maxScalar, err := parquetStatScalar(maximum, dtype)
	if err != nil {
		return nil, nil, false
	}
	return minScalar, maxScalar, true
}

func parquetStatScalar(value any, dtype arrow.DataType) (scalar.Scalar, error) {
	switch dtype.ID() {
	case arrow.DATE32:
		if v, ok := value.(int32); ok {
			return scalar.MakeScalarParam(arrow.Date32(v), dtype)
		}
	case arrow.DATE64:
		if v, ok := value.(int64); ok {
			return scalar.MakeScalarParam(arrow.Date64(v), dtype)
		}
	case arrow.TIMESTAMP:
		if v, ok := value.(int64); ok {
			return scalar.MakeScalarParam(arrow.Timestamp(v), dtype)
		}
	case arrow.STRING, arrow.LARGE_STRING:
		if v, ok := value.([]byte); ok {
			return scalar.MakeScalarParam(string(v), dtype)
		}
	case arrow.BINARY, arrow.LARGE_BINARY:
		if v, ok := value.([]byte); ok {
			return scalar.MakeScalarParam(v, dtype)
		}
	}
	return scalar.MakeScalarParam(value, dtype)
}

func parquetLiteralScalar(value any, dtype arrow.DataType) (scalar.Scalar, error) {
	if timestamp, ok := value.(time.Time); ok {
		switch dtype.ID() {
		case arrow.TIMESTAMP:
			return scalar.MakeScalarParam(arrow.Timestamp(timestamp.UnixNano()), dtype)
		case arrow.DATE64:
			return scalar.MakeScalarParam(arrow.Date64(timestamp.UnixMilli()), dtype)
		}
	}
	return scalar.MakeScalarParam(value, dtype)
}
