package core

import (
	"context"
	"sort"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
)

type tableSource struct {
	reader *array.TableReader
}

type scanRequest struct {
	columns               []string
	projectionColumns     []string
	filterOnlyColumns     []string
	projectionOnlyBatches bool
	limit                 int64
	filters               []*exprNode
}

type scanRequestContextKey struct{}

func withScanRequest(ctx context.Context, request scanRequest) context.Context {
	return context.WithValue(ctx, scanRequestContextKey{}, request)
}

func scanRequestFromContext(ctx context.Context) scanRequest {
	request, _ := ctx.Value(scanRequestContextKey{}).(scanRequest)
	return request
}

func deriveScanRequest(nodes []*planNode) scanRequest {
	if len(nodes) < 2 || nodes[len(nodes)-1].kind != planSelect {
		return scanRequest{}
	}
	columns := make(map[string]struct{})
	projectionColumns := make(map[string]struct{})
	filterColumns := make(map[string]struct{})
	resolver := scanColumnResolver{derived: make(map[string]*exprNode)}
	var requestFilters []*exprNode
	var limit int64
	for _, node := range nodes[1 : len(nodes)-1] {
		switch node.kind {
		case planFilter:
			filter := node.exprs[0].node
			if !resolver.collect(filter, columns) || !resolver.collect(filter, filterColumns) {
				return scanRequest{}
			}
			if filter != nil {
				requestFilters = append(requestFilters, filter)
			}
		case planWithColumns:
			for _, expr := range node.exprs {
				if !resolver.collect(expr.node, columns) || !resolver.collect(expr.node, projectionColumns) {
					return scanRequest{}
				}
			}
			for _, expr := range node.exprs {
				resolver.derived[expr.outputName()] = expr.node
			}
		case planLimit:
			if node.limit > 0 && (limit == 0 || node.limit < limit) {
				limit = node.limit
			}
		default:
			return scanRequest{}
		}
	}
	for _, expr := range nodes[len(nodes)-1].exprs {
		if !resolver.collect(expr.node, columns) || !resolver.collect(expr.node, projectionColumns) {
			return scanRequest{}
		}
	}
	if len(columns) == 0 {
		return scanRequest{}
	}
	result := make([]string, 0, len(columns))
	for column := range columns {
		result = append(result, column)
	}
	sort.Strings(result)
	return scanRequest{
		columns:               result,
		projectionColumns:     sortedColumnNames(projectionColumns),
		filterOnlyColumns:     filterOnlyColumnNames(filterColumns, projectionColumns),
		projectionOnlyBatches: true,
		limit:                 limit,
		filters:               requestFilters,
	}
}

type scanColumnResolver struct {
	derived   map[string]*exprNode
	resolving map[string]struct{}
}

func (r *scanColumnResolver) collect(node *exprNode, columns map[string]struct{}) bool {
	if node == nil {
		return true
	}
	if node.kind == exprColumn {
		derived, ok := r.derived[node.name]
		if !ok {
			columns[node.name] = struct{}{}
			return true
		}
		if r.resolving == nil {
			r.resolving = make(map[string]struct{})
		}
		if _, active := r.resolving[node.name]; active {
			return false
		}
		r.resolving[node.name] = struct{}{}
		ok = r.collect(derived, columns)
		delete(r.resolving, node.name)
		return ok
	}
	if node.kind == exprCall && node.name == "all_columns" {
		return false
	}
	for _, arg := range node.args {
		if !r.collect(arg, columns) {
			return false
		}
	}
	for _, key := range node.windowKeys {
		if !r.collect(key, columns) {
			return false
		}
	}
	return true
}

func deriveGroupByScanRequest(plan *planNode, keys, exprs []Expr) scanRequest {
	if plan == nil {
		return scanRequest{}
	}
	nodes := make([]*planNode, 0, 8)
	for node := plan; node != nil; node = node.input {
		nodes = append(nodes, node)
	}
	reversePlans(nodes)
	columns := make(map[string]struct{})
	projectionColumns := make(map[string]struct{})
	filterColumns := make(map[string]struct{})
	var limit int64
	for _, expr := range keys {
		collectExprColumns(expr.node, columns)
		collectExprColumns(expr.node, projectionColumns)
	}
	for _, expr := range exprs {
		collectExprColumns(expr.node, columns)
		collectExprColumns(expr.node, projectionColumns)
	}
	var filters []*exprNode
	for _, node := range nodes[1:] {
		switch node.kind {
		case planFilter:
			filter := node.exprs[0].node
			collectExprColumns(filter, columns)
			collectExprColumns(filter, filterColumns)
			if filter != nil {
				filters = append(filters, filter)
			}
		case planLimit:
			if node.limit > 0 && (limit == 0 || node.limit < limit) {
				limit = node.limit
			}
		default:
			return scanRequest{}
		}
	}
	if len(columns) == 0 {
		return scanRequest{}
	}
	result := make([]string, 0, len(columns))
	for column := range columns {
		result = append(result, column)
	}
	sort.Strings(result)
	return scanRequest{
		columns:           result,
		projectionColumns: sortedColumnNames(projectionColumns),
		filterOnlyColumns: filterOnlyColumnNames(filterColumns, projectionColumns),
		limit:             limit,
		filters:           filters,
	}
}

func sortedColumnNames(columns map[string]struct{}) []string {
	result := make([]string, 0, len(columns))
	for column := range columns {
		result = append(result, column)
	}
	sort.Strings(result)
	return result
}

func filterOnlyColumnNames(filters, projection map[string]struct{}) []string {
	result := make([]string, 0, len(filters))
	for column := range filters {
		if _, used := projection[column]; !used {
			result = append(result, column)
		}
	}
	sort.Strings(result)
	return result
}

func collectExprColumns(node *exprNode, columns map[string]struct{}) {
	if node == nil {
		return
	}
	if node.kind == exprColumn {
		columns[node.name] = struct{}{}
		return
	}
	for _, arg := range node.args {
		collectExprColumns(arg, columns)
	}
}

func newTableSource(table arrow.Table, batchSize int64) *tableSource {
	return &tableSource{reader: array.NewTableReader(table, batchSize)}
}
func (s *tableSource) Schema() *arrow.Schema          { return s.reader.Schema() }
func (s *tableSource) Next() bool                     { return s.reader.Next() }
func (s *tableSource) RecordBatch() arrow.RecordBatch { return s.reader.RecordBatch() }
func (s *tableSource) Err() error                     { return s.reader.Err() }
func (s *tableSource) Release()                       { s.reader.Release() }
