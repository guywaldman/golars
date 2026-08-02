package core

import (
	"context"
	"fmt"
	"strings"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/scalar"
	"github.com/guywaldman/golars/internal/arrowutil"
)

// JoinType selects the relational join semantics.
type JoinType uint8

const (
	JoinInner JoinType = iota
	JoinLeft
	JoinFull
	JoinRight
	JoinSemi
	JoinAnti
	JoinCross
)

func (t JoinType) String() string {
	switch t {
	case JoinInner:
		return "inner"
	case JoinLeft:
		return "left"
	case JoinFull:
		return "full"
	case JoinRight:
		return "right"
	case JoinSemi:
		return "semi"
	case JoinAnti:
		return "anti"
	case JoinCross:
		return "cross"
	default:
		return "unknown"
	}
}

type JoinOption func(*joinConfig)

type joinConfig struct {
	how    JoinType
	suffix string
}

func WithJoinType(how JoinType) JoinOption { return func(c *joinConfig) { c.how = how } }
func WithJoinSuffix(suffix string) JoinOption {
	return func(c *joinConfig) {
		if suffix != "" {
			c.suffix = suffix
		}
	}
}

// Join adds a deferred relational join. Keys are evaluated as expressions on
// both inputs, so computed keys and temporal/numeric coercion use the same
// expression engine as Select and Filter.
func (lf *LazyFrame) Join(other *LazyFrame, leftOn, rightOn []Expr, options ...JoinOption) *LazyFrame {
	if lf == nil || other == nil {
		return &LazyFrame{err: ErrClosed}
	}
	if lf.err != nil {
		return lf
	}
	if other.err != nil {
		return newLazyFrame(lf.engine, lf.plan, other.err)
	}
	config := joinConfig{how: JoinInner, suffix: "_right"}
	for _, option := range options {
		if option != nil {
			option(&config)
		}
	}
	if config.how > JoinCross {
		return newLazyFrame(lf.engine, lf.plan, fmt.Errorf("%w: unknown join type %d", ErrInvalidPlan, config.how))
	}
	if config.how == JoinCross {
		if len(leftOn) != 0 || len(rightOn) != 0 {
			return newLazyFrame(lf.engine, lf.plan, fmt.Errorf("%w: cross join does not accept keys", ErrInvalidPlan))
		}
	} else if len(leftOn) == 0 || len(leftOn) != len(rightOn) {
		return newLazyFrame(lf.engine, lf.plan, fmt.Errorf("%w: join requires matching non-empty key expressions", ErrInvalidPlan))
	}
	return newLazyFrame(lf.engine, &planNode{
		kind:      planJoin,
		input:     lf.plan,
		right:     other.plan,
		leftKeys:  cloneExprList(leftOn),
		rightKeys: cloneExprList(rightOn),
		joinType:  config.how,
		suffix:    config.suffix,
	}, nil)
}

func (df *DataFrame) Join(other *DataFrame, leftOn, rightOn []Expr, options ...JoinOption) (*DataFrame, error) {
	if err := df.ensureOpen(); err != nil {
		return nil, err
	}
	if err := other.ensureOpen(); err != nil {
		return nil, err
	}
	left := df.Lazy()
	right := other.Lazy()
	defer left.Close()
	defer right.Close()
	return left.Join(right, leftOn, rightOn, options...).Collect(context.Background())
}

func collectBlockingPlan(ctx context.Context, engine *Engine, node *planNode) (*DataFrame, error) {
	if node == nil {
		return nil, ErrInvalidPlan
	}
	switch node.kind {
	case planTable, planSource:
		lf := &LazyFrame{engine: engine, plan: node}
		table, err := lf.collectTable(ctx, nil)
		if err != nil {
			return nil, err
		}
		return newDataFrameWithTable(table, engine), nil
	case planJoin:
		left, err := collectBlockingPlan(ctx, engine, node.input)
		if err != nil {
			return nil, err
		}
		defer left.Close()
		right, err := collectBlockingPlan(ctx, engine, node.right)
		if err != nil {
			return nil, err
		}
		defer right.Close()
		return joinDataFrames(ctx, engine, left, right, node)
	case planGroupBy:
		if !containsJoin(node.input) && !containsWindow(node.input) && !containsMaterializing(node.input) {
			base := &LazyFrame{engine: engine, plan: node.input}
			request := deriveGroupByScanRequest(node.input, node.keys, node.exprs)
			if spec, ok := makeFastGroupBySpec(node.keys, node.exprs); ok {
				return executeFastGroupBy(ctx, base, request, spec)
			}
			if spec, ok := makeSimpleGroupBySpec(node.keys, node.exprs); ok {
				return executeSimpleGroupBy(ctx, base, request, spec)
			}
			input, collectErr := base.collectWithScanRequest(ctx, &request)
			if collectErr != nil {
				return nil, collectErr
			}
			grouped, groupErr := executeGroupBy(ctx, input, node.keys, node.exprs)
			input.Close()
			return grouped, groupErr
		}
	case planSort:
		input, err := collectBlockingPlan(ctx, engine, node.input)
		if err != nil {
			return nil, err
		}
		defer input.Close()
		return sortDataFrame(ctx, input, node.exprs)
	}
	input, err := collectBlockingPlan(ctx, engine, node.input)
	if err != nil {
		return nil, err
	}
	defer input.Close()
	switch node.kind {
	case planSelect:
		if containsWindowExprList(node.exprs) {
			return selectWindowFrame(ctx, engine, input, node.exprs)
		}
		return input.Select(node.exprs...)
	case planFilter:
		if len(node.exprs) != 1 {
			return nil, fmt.Errorf("%w: filter expression missing", ErrInvalidPlan)
		}
		return input.Filter(node.exprs[0])
	case planWithColumns:
		if containsWindowExprList(node.exprs) {
			return withColumnsWindowFrame(ctx, engine, input, node.exprs)
		}
		return input.WithColumns(node.exprs...)
	case planUnique:
		return uniqueDataFrame(input, node.columns, engine)
	case planExplode:
		return explodeDataFrame(input, node.columns, engine)
	case planUnnest:
		return unnestDataFrame(input, node.columns, engine)
	case planLimit:
		return input.Limit(node.limit)
	case planSort:
		return sortDataFrame(ctx, input, node.exprs)
	case planGroupBy:
		return executeGroupBy(ctx, input, node.keys, node.exprs)
	default:
		return nil, fmt.Errorf("%w: unsupported blocking plan node %d", ErrInvalidPlan, node.kind)
	}
}

func containsWindowExprList(exprs []Expr) bool {
	for _, expr := range exprs {
		if containsWindowExpr(expr.node) {
			return true
		}
	}
	return false
}

func selectWindowFrame(ctx context.Context, engine *Engine, input *DataFrame, exprs []Expr) (*DataFrame, error) {
	batch, reader, err := wholeTableBatch(input.table, engine)
	if err != nil {
		return nil, err
	}
	defer reader.Release()
	defer batch.Release()
	result, _, err := applySelect(ctx, engine, batch, exprs)
	if err != nil {
		return nil, err
	}
	defer result.Release()
	table := array.NewTableFromRecords(result.Schema(), []arrow.RecordBatch{result})
	return newDataFrameWithTable(table, engine), nil
}

func withColumnsWindowFrame(ctx context.Context, engine *Engine, input *DataFrame, exprs []Expr) (*DataFrame, error) {
	batch, reader, err := wholeTableBatch(input.table, engine)
	if err != nil {
		return nil, err
	}
	defer reader.Release()
	defer batch.Release()
	result, _, err := applyWithColumns(ctx, engine, batch, exprs)
	if err != nil {
		return nil, err
	}
	defer result.Release()
	table := array.NewTableFromRecords(result.Schema(), []arrow.RecordBatch{result})
	return newDataFrameWithTable(table, engine), nil
}

type joinPair struct {
	left  int64
	right int64
}

func joinDataFrames(ctx context.Context, engine *Engine, left, right *DataFrame, node *planNode) (*DataFrame, error) {
	if err := left.ensureOpen(); err != nil {
		return nil, err
	}
	if err := right.ensureOpen(); err != nil {
		return nil, err
	}
	leftBatch, leftReader, err := wholeTableBatch(left.table, engine)
	if err != nil {
		return nil, err
	}
	defer leftReader.Release()
	defer leftBatch.Release()
	rightBatch, rightReader, err := wholeTableBatch(right.table, engine)
	if err != nil {
		return nil, err
	}
	defer rightReader.Release()
	defer rightBatch.Release()

	if node.joinType == JoinCross {
		return buildJoinedFrame(engine, leftBatch, rightBatch, crossPairs(leftBatch.NumRows(), rightBatch.NumRows()), node)
	}
	leftKeys, err := evalJoinKeys(ctx, engine, leftBatch, node.leftKeys)
	if err != nil {
		return nil, err
	}
	defer arrowutil.ReleaseArrays(leftKeys)
	rightKeys, err := evalJoinKeys(ctx, engine, rightBatch, node.rightKeys)
	if err != nil {
		arrowutil.ReleaseArrays(leftKeys)
		return nil, err
	}
	defer arrowutil.ReleaseArrays(rightKeys)
	if len(leftKeys) != len(rightKeys) {
		return nil, fmt.Errorf("%w: join key count mismatch", ErrInvalidPlan)
	}
	for i := range leftKeys {
		coercedLeft, coercedRight, coerceErr := coerceArrayPair(ctx, engine.allocator, leftKeys[i], rightKeys[i])
		if coerceErr != nil {
			return nil, coerceErr
		}
		leftKeys[i].Release()
		rightKeys[i].Release()
		leftKeys[i], rightKeys[i] = coercedLeft, coercedRight
	}

	rightIndex := make(map[string][]int64, rightBatch.NumRows())
	for row := int64(0); row < rightBatch.NumRows(); row++ {
		key, ok, keyErr := joinKey(rightKeys, row)
		if keyErr != nil {
			return nil, keyErr
		}
		if ok {
			rightIndex[key] = append(rightIndex[key], row)
		}
	}
	pairs := make([]joinPair, 0)
	matchedRight := make([]bool, rightBatch.NumRows())
	for row := int64(0); row < leftBatch.NumRows(); row++ {
		key, ok, keyErr := joinKey(leftKeys, row)
		if keyErr != nil {
			return nil, keyErr
		}
		matches := rightIndex[key]
		if !ok || len(matches) == 0 {
			switch node.joinType {
			case JoinLeft, JoinFull:
				pairs = append(pairs, joinPair{left: row, right: -1})
			case JoinAnti:
				pairs = append(pairs, joinPair{left: row, right: -1})
			}
			continue
		}
		if node.joinType == JoinAnti {
			continue
		}
		if node.joinType == JoinSemi {
			pairs = append(pairs, joinPair{left: row, right: -1})
			continue
		}
		for _, rightRow := range matches {
			matchedRight[rightRow] = true
			pairs = append(pairs, joinPair{left: row, right: rightRow})
		}
	}
	if node.joinType == JoinFull || node.joinType == JoinRight {
		for row, matched := range matchedRight {
			if !matched {
				pairs = append(pairs, joinPair{left: -1, right: int64(row)})
			}
		}
	}
	return buildJoinedFrame(engine, leftBatch, rightBatch, pairs, node)
}

func wholeTableBatch(table arrow.Table, engine *Engine) (arrow.RecordBatch, *array.TableReader, error) {
	reader := array.NewTableReader(table, engine.batchSize)
	var batches []arrow.RecordBatch
	for reader.Next() {
		batch := reader.RecordBatch()
		batch.Retain()
		batches = append(batches, batch)
	}
	if len(batches) == 0 {
		empty, err := emptyRecordBatch(table.Schema(), engine.allocator)
		if err != nil {
			reader.Release()
			return nil, nil, err
		}
		return empty, reader, nil
	}
	columns := make([]arrow.Array, table.NumCols())
	for column := range columns {
		parts := make([]arrow.Array, 0, len(batches))
		for _, batch := range batches {
			part := batch.Column(column)
			part.Retain()
			parts = append(parts, part)
		}
		combined, err := array.Concatenate(parts, engine.allocator)
		arrowutil.ReleaseArrays(parts)
		if err != nil {
			arrowutil.ReleaseArrays(columns)
			for _, batch := range batches {
				batch.Release()
			}
			reader.Release()
			return nil, nil, err
		}
		columns[column] = combined
	}
	for _, batch := range batches {
		batch.Release()
	}
	result := array.NewRecordBatch(table.Schema(), columns, table.NumRows())
	arrowutil.ReleaseArrays(columns)
	return result, reader, nil
}

func evalJoinKeys(ctx context.Context, engine *Engine, batch arrow.RecordBatch, keys []Expr) ([]arrow.Array, error) {
	result := make([]arrow.Array, len(keys))
	for i, key := range keys {
		arr, err := evalExpr(ctx, engine, key.node, batch)
		if err != nil {
			arrowutil.ReleaseArrays(result)
			return nil, err
		}
		result[i] = arr
	}
	return result, nil
}

func joinKey(keys []arrow.Array, row int64) (string, bool, error) {
	var builder strings.Builder
	for _, key := range keys {
		value, err := scalar.GetScalar(key, int(row))
		if err != nil {
			return "", false, err
		}
		if !value.IsValid() {
			return "", false, nil
		}
		text := value.String()
		fmt.Fprintf(&builder, "%d:%d:%s|", value.DataType().ID(), len(text), text)
	}
	return builder.String(), true, nil
}

func crossPairs(leftRows, rightRows int64) []joinPair {
	result := make([]joinPair, 0, leftRows*rightRows)
	for left := int64(0); left < leftRows; left++ {
		for right := int64(0); right < rightRows; right++ {
			result = append(result, joinPair{left: left, right: right})
		}
	}
	return result
}

func buildJoinedFrame(engine *Engine, left, right arrow.RecordBatch, pairs []joinPair, node *planNode) (*DataFrame, error) {
	leftFields := left.Schema().Fields()
	rightFields := right.Schema().Fields()
	fields := make([]arrow.Field, 0, len(leftFields)+len(rightFields))
	builders := make([]array.Builder, 0, cap(fields))
	used := make(map[string]struct{}, len(leftFields)+len(rightFields))
	for _, field := range leftFields {
		fields = append(fields, field)
		builders = append(builders, array.NewBuilder(engine.allocator, field.Type))
		used[field.Name] = struct{}{}
	}
	includeRight := node.joinType != JoinSemi && node.joinType != JoinAnti
	if includeRight {
		for _, field := range rightFields {
			name := field.Name
			if _, exists := used[name]; exists {
				suffix := node.suffix
				if suffix == "" {
					suffix = "_right"
				}
				name += suffix
				for {
					if _, exists := used[name]; !exists {
						break
					}
					name += suffix
				}
			}
			used[name] = struct{}{}
			field.Name = name
			fields = append(fields, field)
			builders = append(builders, array.NewBuilder(engine.allocator, field.Type))
		}
	}
	defer func() {
		for _, builder := range builders {
			builder.Release()
		}
	}()
	for _, pair := range pairs {
		for column, builder := range builders {
			var source arrow.Array
			var row int64
			if column < len(leftFields) {
				source, row = left.Column(column), pair.left
			} else {
				source, row = right.Column(column-len(leftFields)), pair.right
			}
			if row < 0 {
				builder.AppendNull()
				continue
			}
			value, err := scalar.GetScalar(source, int(row))
			if err != nil {
				return nil, err
			}
			if err := scalar.Append(builder, value); err != nil {
				return nil, err
			}
		}
	}
	columns := make([]arrow.Array, len(builders))
	for i, builder := range builders {
		columns[i] = builder.NewArray()
	}
	defer arrowutil.ReleaseArrays(columns)
	table := array.NewTableFromSlice(arrow.NewSchema(fields, nil), arrowutil.ArraysAsSlices(columns))
	return newDataFrameWithTable(table, engine), nil
}
