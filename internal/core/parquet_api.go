package core

import (
	"context"

	"github.com/apache/arrow-go/v18/arrow"
	parquetfile "github.com/apache/arrow-go/v18/parquet/file"
	"github.com/guywaldman/golars/internal/parquetio"
)

func parquetConfig(engine *Engine, config scanConfig) parquetio.Config {
	return parquetio.Config{
		Allocator:     engine.allocator,
		BatchSize:     config.batchSize,
		Parallel:      engine.parallelism > 1,
		Parallelism:   engine.parallelism,
		MissingColumn: missingColumn,
	}
}

func missingColumn(name string, available []string) error {
	return &ColumnNotFoundError{Name: name, Available: available}
}

func parquetRequest(config scanConfig, request scanRequest) parquetio.Request {
	predicates, applicable := parquetPredicates(request.filters)
	return parquetio.Request{
		Columns:               scanColumns(config, request),
		ProjectionColumns:     request.projectionColumns,
		FilterOnlyColumns:     request.filterOnlyColumns,
		ProjectionOnlyBatches: request.projectionOnlyBatches,
		Limit:                 request.limit,
		Filters:               predicates,
		SelectionApplicable:   applicable && len(request.filters) > 0,
	}
}

func openRangedParquetSource(ctx context.Context, engine *Engine, store RangeObjectStore, path string, config scanConfig, request scanRequest) (recordSource, error) {
	return parquetio.OpenRanged(ctx, store, path, parquetConfig(engine, config), parquetRequest(config, request))
}

func parquetPredicates(filters []*exprNode) ([]parquetio.Predicate, bool) {
	if len(filters) == 0 {
		return nil, false
	}
	result := make([]parquetio.Predicate, 0, len(filters))
	for _, filter := range filters {
		predicates, ok := parquetPredicate(filter)
		if !ok {
			return nil, false
		}
		result = append(result, predicates...)
	}
	return result, true
}

func parquetPredicate(node *exprNode) ([]parquetio.Predicate, bool) {
	if node == nil {
		return nil, false
	}
	if node.kind == exprAlias && len(node.args) == 1 {
		return parquetPredicate(node.args[0])
	}
	if node.kind != exprCall {
		return nil, false
	}
	if node.name == "and_kleene" && len(node.args) == 2 {
		left, leftOK := parquetPredicate(node.args[0])
		right, rightOK := parquetPredicate(node.args[1])
		if !leftOK || !rightOK {
			return nil, false
		}
		return append(left, right...), true
	}
	if len(node.args) == 2 {
		if node.args[0].kind == exprColumn && node.args[1].kind == exprLiteral && isParquetComparison(node.name) {
			return []parquetio.Predicate{{Column: node.args[0].name, Op: node.name, Value: node.args[1].value}}, true
		}
		if node.args[0].kind == exprLiteral && node.args[1].kind == exprColumn && isParquetComparison(node.name) {
			return []parquetio.Predicate{{Column: node.args[1].name, Op: invertParquetComparison(node.name), Value: node.args[0].value}}, true
		}
	}
	if len(node.args) == 1 && (node.name == "is_null" || node.name == "is_valid") && node.args[0].kind == exprColumn {
		return []parquetio.Predicate{{Column: node.args[0].name, Op: node.name}}, true
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

// parquetRowGroupIndices is retained for package-level regression tests.
func parquetRowGroupIndices(reader *parquetfile.Reader, schema *arrow.Schema, filters []*exprNode) ([]int, error) {
	predicates, applicable := parquetPredicates(filters)
	return parquetio.RowGroupIndices(reader, schema, predicates, applicable)
}
