package golars

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"github.com/apache/arrow-go/v18/arrow"
)

type planKind uint8

const (
	planTable planKind = iota
	planSource
	planSelect
	planFilter
	planWithColumns
	planLimit
	planSort
	planGroupBy
)

type planNode struct {
	kind   planKind
	input  *planNode
	table  arrow.Table
	source sourceFactory
	exprs  []Expr
	keys   []Expr
	limit  int64
}

type sourceFactory func(context.Context, *Engine) (recordSource, error)

type recordSource interface {
	Schema() *arrow.Schema
	Next() bool
	RecordBatch() arrow.RecordBatch
	Err() error
	Release()
}

type filterGuaranteeSource interface {
	FiltersFullyMatch() bool
}

// LazyFrame is a deferred logical plan. Constructing transformations does not
// touch data; errors are returned by terminal methods.
type LazyFrame struct {
	engine *Engine
	plan   *planNode
	err    error
	once   sync.Once
}

func newLazyFrame(engine *Engine, plan *planNode, err error) *LazyFrame {
	if engine == nil {
		engine = getDefaultEngine()
	}
	return &LazyFrame{engine: engine, plan: plan, err: err}
}

func (lf *LazyFrame) append(kind planKind, exprs []Expr, limit int64) *LazyFrame {
	if lf == nil {
		return &LazyFrame{err: ErrClosed}
	}
	if lf.err != nil {
		return lf
	}
	return &LazyFrame{engine: lf.engine, plan: &planNode{kind: kind, input: lf.plan, exprs: exprs, limit: limit}}
}

func (lf *LazyFrame) Select(exprs ...Expr) *LazyFrame {
	if len(exprs) == 0 {
		return newLazyFrame(lf.engine, lf.plan, fmt.Errorf("%w: Select needs at least one expression", ErrInvalidPlan))
	}
	return lf.append(planSelect, append([]Expr(nil), exprs...), 0)
}
func (lf *LazyFrame) Filter(predicate Expr) *LazyFrame {
	if !predicate.IsValid() {
		return newLazyFrame(lf.engine, lf.plan, fmt.Errorf("%w: invalid filter expression", ErrInvalidPlan))
	}
	return lf.append(planFilter, []Expr{predicate}, 0)
}
func (lf *LazyFrame) WithColumns(exprs ...Expr) *LazyFrame {
	if len(exprs) == 0 {
		return newLazyFrame(lf.engine, lf.plan, fmt.Errorf("%w: WithColumns needs at least one expression", ErrInvalidPlan))
	}
	return lf.append(planWithColumns, append([]Expr(nil), exprs...), 0)
}
func (lf *LazyFrame) Limit(n int64) *LazyFrame {
	if n < 0 {
		return newLazyFrame(lf.engine, lf.plan, fmt.Errorf("%w: limit must be non-negative", ErrInvalidPlan))
	}
	return lf.append(planLimit, nil, n)
}
func (lf *LazyFrame) Head(n int64) *LazyFrame { return lf.Limit(n) }
func (lf *LazyFrame) Sort(exprs ...Expr) *LazyFrame {
	if len(exprs) == 0 {
		return newLazyFrame(lf.engine, lf.plan, fmt.Errorf("%w: Sort needs at least one expression", ErrInvalidPlan))
	}
	return lf.append(planSort, append([]Expr(nil), exprs...), 0)
}
func (lf *LazyFrame) GroupBy(keys ...Expr) *LazyGroupBy {
	return &LazyGroupBy{lf: lf, keys: append([]Expr(nil), keys...)}
}

type LazyGroupBy struct {
	lf   *LazyFrame
	keys []Expr
}

func (g *LazyGroupBy) Agg(exprs ...Expr) *LazyFrame {
	if g == nil || g.lf == nil {
		return &LazyFrame{err: ErrClosed}
	}
	if len(g.keys) == 0 || len(exprs) == 0 {
		return newLazyFrame(g.lf.engine, g.lf.plan, fmt.Errorf("%w: GroupBy.Agg needs keys and expressions", ErrInvalidPlan))
	}
	if g.lf.err != nil {
		return g.lf
	}
	return &LazyFrame{engine: g.lf.engine, plan: &planNode{kind: planGroupBy, input: g.lf.plan, keys: g.keys, exprs: exprs}}
}

func (lf *LazyFrame) Collect(ctx context.Context) (_ *DataFrame, err error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if lf == nil {
		return nil, ErrClosed
	}
	if lf.err != nil {
		return nil, lf.err
	}
	if err := lf.engine.checkOpen(); err != nil {
		return nil, err
	}
	defer recoverMemoryLimit(&err)
	if lf.plan != nil && lf.plan.kind == planGroupBy {
		base := &LazyFrame{engine: lf.engine, plan: lf.plan.input}
		request := deriveGroupByScanRequest(lf.plan.input, lf.plan.keys, lf.plan.exprs)
		if spec, ok := makeFastGroupBySpec(lf.plan.keys, lf.plan.exprs); ok {
			return executeFastGroupBy(ctx, base, request, spec)
		}
		input, collectErr := base.collectWithScanRequest(ctx, &request)
		if collectErr != nil {
			return nil, collectErr
		}
		grouped, groupErr := executeGroupBy(ctx, input, lf.plan.keys, lf.plan.exprs)
		input.Close()
		return grouped, groupErr
	}
	if lf.plan != nil && lf.plan.kind == planSort {
		base := &LazyFrame{engine: lf.engine, plan: lf.plan.input}
		input, collectErr := base.Collect(ctx)
		if collectErr != nil {
			return nil, collectErr
		}
		sorted, sortErr := sortDataFrame(ctx, input, lf.plan.exprs)
		input.Close()
		return sorted, sortErr
	}
	table, err := lf.execute(ctx)
	if err != nil {
		return nil, err
	}
	return &DataFrame{table: table, engine: lf.engine}, nil
}

// ForEachBatch executes a lazy plan incrementally. The callback receives a
// borrowed record batch that remains valid for the duration of the callback;
// retain it if it must outlive the callback. This is the streaming escape hatch
// for scans that should not first materialize a complete DataFrame.
func (lf *LazyFrame) ForEachBatch(ctx context.Context, callback func(arrow.RecordBatch) error) (err error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if lf == nil {
		return ErrClosed
	}
	if callback == nil {
		return fmt.Errorf("%w: nil batch callback", ErrInvalidPlan)
	}
	if lf.err != nil {
		return lf.err
	}
	if err := lf.engine.checkOpen(); err != nil {
		return err
	}
	defer recoverMemoryLimit(&err)
	if lf.plan == nil {
		return ErrInvalidPlan
	}
	if lf.plan.kind == planGroupBy || lf.plan.kind == planSort {
		df, collectErr := lf.Collect(ctx)
		if collectErr != nil {
			return collectErr
		}
		defer df.Close()
		reader := newTableSource(df.table, lf.engine.batchSize)
		defer reader.Release()
		emitted := false
		for reader.Next() {
			if err := ctx.Err(); err != nil {
				return err
			}
			batch := reader.RecordBatch()
			emitted = true
			batch.Retain()
			callbackErr := callback(batch)
			batch.Release()
			if callbackErr != nil {
				return callbackErr
			}
		}
		if err := reader.Err(); err != nil {
			return err
		}
		if emitted || reader.Schema() == nil {
			return nil
		}
		empty, err := emptyRecordBatch(reader.Schema(), lf.engine.allocator)
		if err != nil {
			return err
		}
		callbackErr := callback(empty)
		empty.Release()
		return callbackErr
	}
	return lf.executeBatches(ctx, callback)
}

func (lf *LazyFrame) Sink(ctx context.Context, destination string, options ...WriteOption) error {
	return writeParquetBatches(ctx, lf, destination, options...)
}

func (lf *LazyFrame) CollectBatches(ctx context.Context) ([]arrow.RecordBatch, error) {
	var batches []arrow.RecordBatch
	err := lf.ForEachBatch(ctx, func(batch arrow.RecordBatch) error {
		batch.Retain()
		batches = append(batches, batch)
		return nil
	})
	if err != nil {
		for _, batch := range batches {
			batch.Release()
		}
		return nil, err
	}
	return batches, nil
}

func (lf *LazyFrame) Schema() (Schema, error) {
	df, err := lf.Collect(context.Background())
	if err != nil {
		return Schema{}, err
	}
	defer df.Close()
	return df.Schema(), nil
}

func (lf *LazyFrame) Explain() string {
	if lf == nil {
		return "<closed>"
	}
	if lf.err != nil {
		return "ERROR: " + lf.err.Error()
	}
	var nodes []string
	for node := lf.plan; node != nil; node = node.input {
		switch node.kind {
		case planTable:
			nodes = append(nodes, "TABLE")
		case planSource:
			nodes = append(nodes, "SCAN")
		case planSelect:
			nodes = append(nodes, "SELECT "+formatExprList(node.exprs))
		case planFilter:
			nodes = append(nodes, "FILTER "+node.exprs[0].String())
		case planWithColumns:
			nodes = append(nodes, "WITH_COLUMNS "+formatExprList(node.exprs))
		case planLimit:
			nodes = append(nodes, fmt.Sprintf("LIMIT %d", node.limit))
		case planSort:
			nodes = append(nodes, "SORT "+formatExprList(node.exprs))
		case planGroupBy:
			nodes = append(nodes, "GROUP_BY "+formatExprList(node.keys)+" AGG "+formatExprList(node.exprs))
		}
	}
	for i, j := 0, len(nodes)-1; i < j; i, j = i+1, j-1 {
		nodes[i], nodes[j] = nodes[j], nodes[i]
	}
	return strings.Join(nodes, "\n")
}

func formatExprList(exprs []Expr) string {
	parts := make([]string, len(exprs))
	for i, expr := range exprs {
		parts[i] = expr.String()
	}
	return "[" + strings.Join(parts, ", ") + "]"
}

func (lf *LazyFrame) Close() {
	if lf == nil {
		return
	}
	lf.once.Do(func() { releasePlan(lf.plan) })
}

func releasePlan(node *planNode) {
	if node == nil {
		return
	}
	if node.input != nil {
		releasePlan(node.input)
		node.input = nil
	}
	if node.table != nil {
		node.table.Release()
		node.table = nil
	}
}
