package core

import (
	"context"
	"fmt"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"

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
	planTail
	planSort
	planGroupBy
	planJoin
	planUnique
	planExplode
	planUnnest
)

type planNode struct {
	kind         planKind
	input        *planNode
	right        *planNode
	table        arrow.Table
	source       sourceFactory
	sourcePath   string
	sourceFormat formatKind
	sourceConfig scanConfig
	exprs        []Expr
	keys         []Expr
	leftKeys     []Expr
	rightKeys    []Expr
	joinType     JoinType
	suffix       string
	columns      []string
	limit        int64
}

type sourceFactory func(context.Context, *Engine) (recordSource, error)

type recordSource interface {
	Schema() *arrow.Schema
	Next() bool
	RecordBatch() arrow.RecordBatch
	Err() error
	Release()
}

// LazyFrame is a deferred logical plan. Constructing transformations does not
// touch data; errors are returned by terminal methods. Close releases retained
// Arrow references deterministically; the finalizer is only a safety net.
type LazyFrame struct {
	engine    *Engine
	plan      *planNode
	err       error
	resources []arrow.Table
	once      sync.Once
	closed    atomic.Bool
}

func newLazyFrame(engine *Engine, plan *planNode, err error) *LazyFrame {
	if engine == nil {
		engine = getDefaultEngine()
	}
	lf := &LazyFrame{engine: engine, plan: plan, err: err}
	retainPlanTables(plan, &lf.resources)
	runtime.SetFinalizer(lf, finalizeLazyFrame)
	return lf
}

func finalizeLazyFrame(lf *LazyFrame) { lf.Close() }

func retainPlanTables(node *planNode, tables *[]arrow.Table) {
	if node == nil {
		return
	}
	if node.table != nil {
		node.table.Retain()
		*tables = append(*tables, node.table)
	}
	retainPlanTables(node.input, tables)
	retainPlanTables(node.right, tables)
}

func (lf *LazyFrame) append(kind planKind, exprs []Expr, limit int64) *LazyFrame {
	if lf == nil || lf.closed.Load() {
		return &LazyFrame{err: ErrClosed}
	}
	if lf.err != nil {
		return lf
	}
	return newLazyFrame(lf.engine, &planNode{kind: kind, input: lf.plan, exprs: exprs, limit: limit}, nil)
}

// Select adds a projection to the lazy plan.
func (lf *LazyFrame) Select(exprs ...Expr) *LazyFrame {
	if len(exprs) == 0 {
		return newLazyFrame(lf.engine, lf.plan, fmt.Errorf("%w: Select needs at least one expression", ErrInvalidPlan))
	}
	return lf.append(planSelect, append([]Expr(nil), exprs...), 0)
}

// Filter adds a row predicate to the lazy plan.
func (lf *LazyFrame) Filter(predicate Expr) *LazyFrame {
	if !predicate.IsValid() {
		return newLazyFrame(lf.engine, lf.plan, fmt.Errorf("%w: invalid filter expression", ErrInvalidPlan))
	}
	return lf.append(planFilter, []Expr{predicate}, 0)
}

// WithColumns adds or replaces columns in the lazy plan.
func (lf *LazyFrame) WithColumns(exprs ...Expr) *LazyFrame {
	if len(exprs) == 0 {
		return newLazyFrame(lf.engine, lf.plan, fmt.Errorf("%w: WithColumns needs at least one expression", ErrInvalidPlan))
	}
	return lf.append(planWithColumns, append([]Expr(nil), exprs...), 0)
}

// Unique keeps one row for each distinct key.
func (lf *LazyFrame) Unique(columns ...string) *LazyFrame {
	if len(columns) == 0 {
		columns = nil
	}
	return lf.appendColumns(planUnique, columns)
}

// Explode expands list values into separate rows.
func (lf *LazyFrame) Explode(columns ...string) *LazyFrame {
	return lf.appendColumns(planExplode, columns)
}

// Unnest expands struct fields into columns.
func (lf *LazyFrame) Unnest(columns ...string) *LazyFrame {
	return lf.appendColumns(planUnnest, columns)
}

func (lf *LazyFrame) appendColumns(kind planKind, columns []string) *LazyFrame {
	if lf == nil || lf.closed.Load() {
		return &LazyFrame{err: ErrClosed}
	}
	if lf.err != nil {
		return lf
	}
	if kind != planUnique && len(columns) == 0 {
		return newLazyFrame(lf.engine, lf.plan, fmt.Errorf("%w: operation requires at least one column", ErrInvalidPlan))
	}
	return newLazyFrame(lf.engine, &planNode{kind: kind, input: lf.plan, columns: append([]string(nil), columns...)}, nil)
}

// Limit adds a row limit to the lazy plan.
func (lf *LazyFrame) Limit(n int64) *LazyFrame {
	if n < 0 {
		return newLazyFrame(lf.engine, lf.plan, fmt.Errorf("%w: limit must be non-negative", ErrInvalidPlan))
	}
	return lf.append(planLimit, nil, n)
}

// Head adds a limit for the first n rows.
func (lf *LazyFrame) Head(n int64) *LazyFrame { return lf.Limit(n) }

// Tail adds a limit for the last n rows.
func (lf *LazyFrame) Tail(n int64) *LazyFrame {
	if n < 0 {
		return newLazyFrame(lf.engine, lf.plan, fmt.Errorf("%w: tail must be non-negative", ErrInvalidPlan))
	}
	return lf.append(planTail, nil, n)
}

// Sort adds ordering expressions to the lazy plan.
func (lf *LazyFrame) Sort(exprs ...Expr) *LazyFrame {
	if len(exprs) == 0 {
		return newLazyFrame(lf.engine, lf.plan, fmt.Errorf("%w: Sort needs at least one expression", ErrInvalidPlan))
	}
	return lf.append(planSort, append([]Expr(nil), exprs...), 0)
}

// GroupBy starts a lazy grouped aggregation.
func (lf *LazyFrame) GroupBy(keys ...Expr) *LazyGroupBy {
	return &LazyGroupBy{lf: lf, keys: append([]Expr(nil), keys...)}
}

// LazyGroupBy builds a deferred grouped aggregation.
type LazyGroupBy struct {
	lf   *LazyFrame
	keys []Expr
}

// Agg adds grouped aggregate expressions to the lazy plan.
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
	return newLazyFrame(g.lf.engine, &planNode{kind: planGroupBy, input: g.lf.plan, keys: g.keys, exprs: exprs}, nil)
}

// Collect executes the lazy plan and returns an eager DataFrame.
func (lf *LazyFrame) Collect(ctx context.Context) (_ *DataFrame, err error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if lf == nil {
		return nil, ErrClosed
	}
	if lf.closed.Load() {
		return nil, ErrClosed
	}
	if lf.err != nil {
		return nil, lf.err
	}
	if err := lf.engine.checkOpen(); err != nil {
		return nil, err
	}
	defer recoverMemoryLimit(&err)
	return collectNative(ctx, lf.engine, lf.plan)
}

// ForEachBatch executes the plan in Polars and iterates its Arrow result in
// batches. The callback receives a borrowed record batch that remains valid for
// the duration of the callback; retain it if it must outlive the callback.
func (lf *LazyFrame) ForEachBatch(ctx context.Context, callback func(arrow.RecordBatch) error) (err error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if lf == nil {
		return ErrClosed
	}
	if lf.closed.Load() {
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
	df, collectErr := lf.Collect(ctx)
	if collectErr != nil {
		return collectErr
	}
	defer df.Close()
	table, tableErr := df.ArrowTable()
	if tableErr != nil {
		return tableErr
	}
	defer table.Release()
	reader := newTableSource(table, lf.engine.batchSize)
	defer reader.Release()
	for reader.Next() {
		if err := ctx.Err(); err != nil {
			return err
		}
		batch := reader.RecordBatch()
		batch.Retain()
		callbackErr := callback(batch)
		batch.Release()
		if callbackErr != nil {
			return callbackErr
		}
	}
	return reader.Err()
}

// Sink executes the plan and chooses a writer from destination's extension.
func (lf *LazyFrame) Sink(ctx context.Context, destination string, options ...WriteOption) error {
	if format, ok := formatForPath(destination); ok {
		switch format {
		case formatCSV:
			return writeCSVBatches(ctx, lf, destination, options...)
		case formatJSON:
			return writeJSONBatches(ctx, lf, destination, true, options...)
		case formatNDJSON:
			return writeJSONBatches(ctx, lf, destination, false, options...)
		case formatIPCFile:
			return writeIPCBatches(ctx, lf, destination, true, options...)
		case formatIPCStream:
			return writeIPCBatches(ctx, lf, destination, false, options...)
		}
	}
	return writeParquetBatches(ctx, lf, destination, options...)
}

// SinkParquet executes the plan and writes Parquet.
func (lf *LazyFrame) SinkParquet(ctx context.Context, destination string, options ...WriteOption) error {
	return writeParquetBatches(ctx, lf, destination, options...)
}

// SinkCSV executes the plan and writes CSV.
func (lf *LazyFrame) SinkCSV(ctx context.Context, destination string, options ...WriteOption) error {
	return writeCSVBatches(ctx, lf, destination, options...)
}

// SinkJSON executes the plan and writes a JSON array.
func (lf *LazyFrame) SinkJSON(ctx context.Context, destination string, options ...WriteOption) error {
	return writeJSONBatches(ctx, lf, destination, true, options...)
}

// SinkNDJSON executes the plan and writes newline-delimited JSON.
func (lf *LazyFrame) SinkNDJSON(ctx context.Context, destination string, options ...WriteOption) error {
	return writeJSONBatches(ctx, lf, destination, false, options...)
}

// SinkIPCFile executes the plan and writes an Arrow IPC file.
func (lf *LazyFrame) SinkIPCFile(ctx context.Context, destination string, options ...WriteOption) error {
	return writeIPCBatches(ctx, lf, destination, true, options...)
}

// SinkIPCStream executes the plan and writes an Arrow IPC stream.
func (lf *LazyFrame) SinkIPCStream(ctx context.Context, destination string, options ...WriteOption) error {
	return writeIPCBatches(ctx, lf, destination, false, options...)
}

// CollectBatches executes the plan and returns retained Arrow record batches.
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

// Schema executes the plan and returns its result schema.
func (lf *LazyFrame) Schema() (Schema, error) {
	df, err := lf.Collect(context.Background())
	if err != nil {
		return Schema{}, err
	}
	defer df.Close()
	return df.Schema(), nil
}

// Explain returns the optimized-facing plan description.
func (lf *LazyFrame) Explain() string {
	return lf.ExplainOptimized()
}

// ExplainUnoptimized returns the Go-facing plan before native collection.
func (lf *LazyFrame) ExplainUnoptimized() string {
	return lf.explainPlan()
}

// ExplainOptimized returns the Go-facing plan sent to the native optimizer.
func (lf *LazyFrame) ExplainOptimized() string {
	return lf.explainPlan()
}

func (lf *LazyFrame) explainPlan() string {
	if lf == nil || lf.closed.Load() {
		return "<closed>"
	}
	if lf.err != nil {
		return "ERROR: " + lf.err.Error()
	}
	plan := lf.plan
	var nodes []string
	for node := plan; node != nil; node = node.input {
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
		case planTail:
			nodes = append(nodes, fmt.Sprintf("TAIL %d", node.limit))
		case planSort:
			nodes = append(nodes, "SORT "+formatExprList(node.exprs))
		case planGroupBy:
			nodes = append(nodes, "GROUP_BY "+formatExprList(node.keys)+" AGG "+formatExprList(node.exprs))
		case planJoin:
			nodes = append(nodes, "JOIN "+node.joinType.String()+" ON "+formatExprList(node.leftKeys)+" = "+formatExprList(node.rightKeys))
		case planUnique:
			nodes = append(nodes, "UNIQUE "+formatStringList(node.columns))
		case planExplode:
			nodes = append(nodes, "EXPLODE "+formatStringList(node.columns))
		case planUnnest:
			nodes = append(nodes, "UNNEST "+formatStringList(node.columns))
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

func formatStringList(values []string) string {
	return "[" + strings.Join(values, ", ") + "]"
}

// Close releases retained plan resources.
func (lf *LazyFrame) Close() {
	if lf == nil {
		return
	}
	runtime.SetFinalizer(lf, nil)
	lf.closed.Store(true)
	lf.once.Do(func() {
		for _, table := range lf.resources {
			table.Release()
		}
		lf.resources = nil
		lf.plan = nil
	})
}

// Release is an alias for Close for consistency with Arrow's reference-counted
// objects. Close remains the preferred name in ordinary Go code.
func (lf *LazyFrame) Release() { lf.Close() }
