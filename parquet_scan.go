package golars

import (
	"context"
	"io"
	"strings"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/scalar"
	"github.com/apache/arrow-go/v18/parquet"
	parquetfile "github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/metadata"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
)

type emptyRecordSource struct{ schema *arrow.Schema }

func (s *emptyRecordSource) Schema() *arrow.Schema          { return s.schema }
func (s *emptyRecordSource) Next() bool                     { return false }
func (s *emptyRecordSource) RecordBatch() arrow.RecordBatch { return nil }
func (s *emptyRecordSource) Err() error                     { return nil }
func (s *emptyRecordSource) Release()                       {}

func openRangedParquetSource(ctx context.Context, engine *Engine, store RangeObjectStore, path string, config scanConfig, request scanRequest) (recordSource, error) {
	info, err := store.Stat(ctx, path)
	if err != nil {
		return nil, err
	}
	ranged, err := newRangeReaderAtSeeker(ctx, store, path, info.Size)
	if err != nil {
		return nil, err
	}
	reader, err := parquetfile.NewParquetReader(ranged)
	if err != nil {
		_ = ranged.Close()
		return nil, err
	}
	props := pqarrow.ArrowReadProperties{BatchSize: config.batchSize, Parallel: engine.parallelism > 1}
	arrowReader, err := pqarrow.NewFileReader(reader, props, engine.allocator)
	if err != nil {
		_ = reader.Close()
		return nil, err
	}
	return newParquetRecordSource(ctx, engine, reader, arrowReader, request, false)
}

func newParquetRecordSource(ctx context.Context, engine *Engine, reader *parquetfile.Reader, arrowReader *pqarrow.FileReader, request scanRequest, local bool, closers ...io.Closer) (recordSource, error) {
	closeAll := func() {
		_ = reader.Close()
		for _, closer := range closers {
			_ = closer.Close()
		}
	}
	schema, err := arrowReader.Schema()
	if err != nil {
		closeAll()
		return nil, err
	}
	columnIndices, err := parquetColumnIndices(arrowReader, request)
	if err != nil {
		closeAll()
		return nil, err
	}
	rowGroups, fullMatches, selectionApplicable, err := parquetRowGroupSelectionWithGuarantees(reader, schema, request.filters)
	if err != nil {
		closeAll()
		return nil, err
	}
	fileClosers := make([]io.Closer, 0, len(closers)+1)
	fileClosers = append(fileClosers, reader)
	fileClosers = append(fileClosers, closers...)
	if len(request.filters) > 0 && selectionApplicable && len(rowGroups) == 0 {
		return &fileSource{reader: &emptyRecordSource{schema: schema}, closers: fileClosers}, nil
	}
	if local && selectionApplicable && len(request.filterOnlyColumns) > 0 && len(request.projectionColumns) > 0 && hasFullMatch(fullMatches) {
		projectionIndices, projectionErr := parquetColumnIndicesForNames(arrowReader, request.projectionColumns)
		if projectionErr != nil {
			closeAll()
			return nil, projectionErr
		}
		requestedSchema := parquetRequestedSchema(schema, columnIndices)
		projected := newParquetProjectionSource(ctx, engine, arrowReader, requestedSchema, columnIndices, projectionIndices, rowGroups, fullMatches)
		return &fileSource{reader: projected, closers: fileClosers}, nil
	}
	recordReader, err := arrowReader.GetRecordReader(ctx, columnIndices, rowGroups)
	if err != nil {
		closeAll()
		return nil, err
	}
	var source recordSource = recordReader
	filtersFullyMatch := len(fullMatches) > 0
	for _, full := range fullMatches {
		filtersFullyMatch = filtersFullyMatch && full
	}
	if len(request.filters) > 0 && selectionApplicable {
		source = newParquetFilterSource(recordReader, reader, rowGroups, fullMatches)
	}
	return &fileSource{reader: source, closers: fileClosers, filtersFullyMatch: filtersFullyMatch}, nil
}

type parquetFilter struct {
	column string
	op     string
	value  any
}

func parquetFilters(node *exprNode) ([]parquetFilter, bool) {
	if node == nil {
		return nil, false
	}
	if node.kind == exprAlias && len(node.args) == 1 {
		return parquetFilters(node.args[0])
	}
	if node.kind != exprCall {
		return nil, false
	}
	if node.name == "and_kleene" && len(node.args) == 2 {
		left, leftOK := parquetFilters(node.args[0])
		right, rightOK := parquetFilters(node.args[1])
		if !leftOK || !rightOK {
			return nil, false
		}
		return append(left, right...), true
	}
	if len(node.args) == 2 {
		if node.args[0].kind == exprColumn && node.args[1].kind == exprLiteral {
			if isParquetComparison(node.name) {
				return []parquetFilter{{column: node.args[0].name, op: node.name, value: node.args[1].value}}, true
			}
		}
		if node.args[0].kind == exprLiteral && node.args[1].kind == exprColumn {
			if isParquetComparison(node.name) {
				return []parquetFilter{{column: node.args[1].name, op: invertParquetComparison(node.name), value: node.args[0].value}}, true
			}
		}
	}
	if len(node.args) == 1 && (node.name == "is_null" || node.name == "is_valid") && node.args[0].kind == exprColumn {
		return []parquetFilter{{column: node.args[0].name, op: node.name}}, true
	}
	return nil, false
}

func isParquetComparison(name string) bool {
	switch name {
	case "equal", "not_equal", "greater", "greater_equal", "less", "less_equal":
		return true
	default:
		return false
	}
}

func invertParquetComparison(name string) string {
	switch name {
	case "greater":
		return "less"
	case "greater_equal":
		return "less_equal"
	case "less":
		return "greater"
	case "less_equal":
		return "greater_equal"
	default:
		return name
	}
}

func parquetRowGroupIndices(reader *parquetfile.Reader, schema *arrow.Schema, filters []*exprNode) ([]int, error) {
	indices, _, err := parquetRowGroupSelection(reader, schema, filters)
	return indices, err
}

func parquetRowGroupSelection(reader *parquetfile.Reader, schema *arrow.Schema, filters []*exprNode) ([]int, bool, error) {
	indices, fullMatches, _, err := parquetRowGroupSelectionWithGuarantees(reader, schema, filters)
	if err != nil {
		return nil, false, err
	}
	allMatch := len(fullMatches) > 0
	for _, full := range fullMatches {
		allMatch = allMatch && full
	}
	return indices, allMatch, nil
}

func parquetRowGroupSelectionWithGuarantees(reader *parquetfile.Reader, schema *arrow.Schema, filters []*exprNode) ([]int, []bool, bool, error) {
	if len(filters) == 0 || schema == nil {
		return nil, nil, false, nil
	}
	parsed := make([][]parquetFilter, 0, len(filters))
	for _, filter := range filters {
		predicates, ok := parquetFilters(filter)
		if ok {
			parsed = append(parsed, predicates)
		}
	}
	if len(parsed) == 0 || len(parsed) != len(filters) {
		return nil, nil, false, nil
	}
	indices := make([]int, 0, reader.NumRowGroups())
	fullMatches := make([]bool, 0, reader.NumRowGroups())
	for rowGroup := 0; rowGroup < reader.NumRowGroups(); rowGroup++ {
		mayMatch := true
		fullyMatches := true
		for _, predicates := range parsed {
			for _, predicate := range predicates {
				if !parquetRowGroupMayMatch(reader.RowGroup(rowGroup), schema, predicate) {
					mayMatch = false
					break
				}
				if !parquetRowGroupFullyMatches(reader.RowGroup(rowGroup), schema, predicate) {
					fullyMatches = false
				}
			}
			if !mayMatch {
				break
			}
		}
		if mayMatch {
			indices = append(indices, rowGroup)
			fullMatches = append(fullMatches, fullyMatches)
		}
	}
	return indices, fullMatches, true, nil
}

func hasFullMatch(fullMatches []bool) bool {
	for _, full := range fullMatches {
		if full {
			return true
		}
	}
	return false
}

func parquetRowGroupFullyMatches(rowGroup *parquetfile.RowGroupReader, schema *arrow.Schema, predicate parquetFilter) bool {
	fields := schema.FieldIndices(predicate.column)
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
	if predicate.op == "is_null" || predicate.op == "is_valid" {
		if !stats.HasNullCount() {
			return false
		}
		nulls := stats.NullCount()
		if predicate.op == "is_null" {
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
		// Keep full-match elimination conservative for floating-point NaNs,
		// truncated binary statistics, and types without exact bounds.
		return false
	}
	minimum, maximum, ok := parquetStatBounds(stats, field.Type)
	if !ok {
		return false
	}
	literal, err := parquetLiteralScalar(predicate.value, field.Type)
	if err != nil || literal == nil || !literal.IsValid() {
		return false
	}
	minComparison := comparePruningScalars(minimum, literal)
	maxComparison := comparePruningScalars(maximum, literal)
	switch predicate.op {
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

func parquetRowGroupMayMatch(rowGroup *parquetfile.RowGroupReader, schema *arrow.Schema, predicate parquetFilter) bool {
	fields := schema.FieldIndices(predicate.column)
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
	if predicate.op == "is_null" || predicate.op == "is_valid" {
		if !stats.HasNullCount() {
			return true
		}
		nulls := stats.NullCount()
		if predicate.op == "is_null" {
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
	literal, err := parquetLiteralScalar(predicate.value, field.Type)
	if err != nil || literal == nil || !literal.IsValid() {
		return true
	}
	minComparison := comparePruningScalars(minimum, literal)
	maxComparison := comparePruningScalars(maximum, literal)
	switch predicate.op {
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
	return compareScalars(left, right)
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

var _ parquet.ReaderAtSeeker = (*rangeReaderAtSeeker)(nil)
