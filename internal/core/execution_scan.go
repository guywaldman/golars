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
