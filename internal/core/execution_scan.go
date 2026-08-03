package core

import (
	"context"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
)

// scanRequest carries optional source-level hints for Go I/O adapters. It is
// not an execution plan: Polars owns filtering, projection, and all other
// relational execution.
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

func scanRequestForPlan(root *planNode) scanRequest {
	request := scanRequest{}
	if root == nil {
		return request
	}
	var nodes []*planNode
	for node := root; node != nil; node = node.input {
		nodes = append(nodes, node)
	}
	var columns []string
	seen := make(map[string]struct{})
	allColumns := false
	projectionPushdown := true
	projectionRequested := false
	addExpr := func(expr *exprNode) {
		collectExprColumns(expr, seen, &columns, &allColumns)
	}
	clearProjection := func() {
		projectionPushdown = false
		projectionRequested = false
		columns = nil
		seen = make(map[string]struct{})
		allColumns = false
	}
	filterPushdown := true
	for index := len(nodes) - 1; index >= 0; index-- {
		node := nodes[index]
		switch node.kind {
		case planFilter:
			if len(node.exprs) == 1 {
				if projectionPushdown {
					addExpr(node.exprs[0].node)
				}
				if filterPushdown {
					request.filters = append(request.filters, node.exprs[0].node)
				}
			}
		case planSelect, planSelectSeq:
			derived := projectionHasDerivedColumns(node.exprs)
			if projectionPushdown && !derived {
				projectionRequested = true
				for _, expr := range node.exprs {
					addExpr(expr.node)
				}
				for _, expr := range node.keys {
					addExpr(expr.node)
				}
			} else {
				clearProjection()
			}
			if derived {
				filterPushdown = false
			}
		case planWithColumns, planWithColumnsSeq, planSort, planGroupBy:
			clearProjection()
			filterPushdown = false
		case planJoin:
			clearProjection()
			filterPushdown = false
		case planUnique, planExplode, planUnnest, planDropNans:
			clearProjection()
			filterPushdown = false
		case planUnpivot:
			clearProjection()
			filterPushdown = false
		default:
			filterPushdown = false
		}
	}
	if projectionPushdown && projectionRequested && !allColumns {
		request.columns = columns
	}
	return request
}

func projectionHasDerivedColumns(exprs []Expr) bool {
	for _, expr := range exprs {
		node := expr.node
		if node == nil {
			return true
		}
		if node.kind == exprColumn {
			continue
		}
		if node.kind == exprAlias && len(node.args) == 1 && node.args[0] != nil && node.args[0].kind == exprColumn && node.name == node.args[0].name {
			continue
		}
		if node.kind == exprCall && node.name == "all_columns" {
			continue
		}
		return true
	}
	return false
}

func collectExprColumns(node *exprNode, seen map[string]struct{}, columns *[]string, allColumns *bool) {
	if node == nil || *allColumns {
		return
	}
	if node.kind == exprColumn {
		if _, ok := seen[node.name]; !ok {
			seen[node.name] = struct{}{}
			*columns = append(*columns, node.name)
		}
	}
	if node.kind == exprCall && node.name == "all_columns" {
		*allColumns = true
		return
	}
	for _, arg := range node.args {
		collectExprColumns(arg, seen, columns, allColumns)
	}
	for _, key := range node.windowKeys {
		collectExprColumns(key, seen, columns, allColumns)
	}
}

type tableSource struct {
	reader *array.TableReader
}

func newTableSource(table arrow.Table, batchSize int64) *tableSource {
	return &tableSource{reader: array.NewTableReader(table, batchSize)}
}

func (s *tableSource) Schema() *arrow.Schema          { return s.reader.Schema() }
func (s *tableSource) Next() bool                     { return s.reader.Next() }
func (s *tableSource) RecordBatch() arrow.RecordBatch { return s.reader.RecordBatch() }
func (s *tableSource) Err() error                     { return s.reader.Err() }
func (s *tableSource) Release()                       { s.reader.Release() }

type boundedSource struct {
	source  recordSource
	skip    int64
	limit   int64
	skipped int64
	emitted int64
	current arrow.RecordBatch
	err     error
}

func withScanBounds(source recordSource, config scanConfig) recordSource {
	if source == nil || (config.skipRows <= 0 && config.nRows < 0) {
		return source
	}
	return &boundedSource{source: source, skip: config.skipRows, limit: config.nRows}
}

func (s *boundedSource) Schema() *arrow.Schema { return s.source.Schema() }

func (s *boundedSource) Next() bool {
	if s.current != nil {
		s.current.Release()
		s.current = nil
	}
	if s.limit >= 0 && s.emitted >= s.limit {
		return false
	}
	for s.source.Next() {
		batch := s.source.RecordBatch()
		start := int64(0)
		if s.skipped < s.skip {
			start = minInt64(s.skip-s.skipped, batch.NumRows())
			s.skipped += start
		}
		if start >= batch.NumRows() {
			continue
		}
		end := batch.NumRows()
		if s.limit >= 0 {
			end = minInt64(end, start+s.limit-s.emitted)
		}
		if end <= start {
			return false
		}
		s.current = batch.NewSlice(start, end)
		s.emitted += end - start
		return true
	}
	s.err = s.source.Err()
	return false
}

func (s *boundedSource) RecordBatch() arrow.RecordBatch { return s.current }
func (s *boundedSource) Err() error                     { return s.err }

func (s *boundedSource) Release() {
	if s.current != nil {
		s.current.Release()
		s.current = nil
	}
	if s.source != nil {
		s.source.Release()
		s.source = nil
	}
}

func minInt64(left, right int64) int64 {
	if left < right {
		return left
	}
	return right
}
