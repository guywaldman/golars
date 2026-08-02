package core

import (
	"context"
	"fmt"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/guywaldman/golars/internal/arrowutil"
)

type fastGroupBySpec struct {
	keyName   string
	keyOutput string
	valueName string
	aggName   string
	aggOutput string
}

func fastGroupBySpecFor(keys, exprs []Expr) (fastGroupBySpec, bool) {
	if len(keys) != 1 || len(exprs) != 1 {
		return fastGroupBySpec{}, false
	}
	key := unwrapAlias(keys[0].node)
	if key == nil || key.kind != exprColumn {
		return fastGroupBySpec{}, false
	}
	agg := unwrapAlias(exprs[0].node)
	if agg == nil || agg.kind != exprAggregate || len(agg.args) != 1 {
		return fastGroupBySpec{}, false
	}
	value := unwrapAlias(agg.args[0])
	if value == nil || value.kind != exprColumn {
		return fastGroupBySpec{}, false
	}
	switch agg.name {
	case "sum", "count", "mean":
	default:
		return fastGroupBySpec{}, false
	}
	return fastGroupBySpec{
		keyName:   key.name,
		keyOutput: keys[0].outputName(),
		valueName: value.name,
		aggName:   agg.name,
		aggOutput: exprs[0].outputName(),
	}, true
}

func makeFastGroupBySpec(keys, exprs []Expr) (fastGroupBySpec, bool) {
	return fastGroupBySpecFor(keys, exprs)
}

type fastValueKind uint8

const (
	fastFloat64 fastValueKind = iota
	fastInt64
)

type fastInt64GroupBy struct {
	engine      *Engine
	spec        fastGroupBySpec
	keyIndex    int
	valueIndex  int
	valueKind   fastValueKind
	groups      map[int64]int
	denseGroups [1024]int
	denseSeen   [1024]bool
	keys        []int64
	validKeys   []bool
	nullGroup   int
	floatSums   []float64
	intSums     []int64
	counts      []int64
}

func newFastInt64GroupBy(engine *Engine, schema *arrow.Schema, spec fastGroupBySpec) (*fastInt64GroupBy, error) {
	keyFields := schema.FieldIndices(spec.keyName)
	valueFields := schema.FieldIndices(spec.valueName)
	if len(keyFields) == 0 {
		return nil, &ColumnNotFoundError{Name: spec.keyName, Available: arrowutil.SchemaNames(schema)}
	}
	if len(valueFields) == 0 {
		return nil, &ColumnNotFoundError{Name: spec.valueName, Available: arrowutil.SchemaNames(schema)}
	}
	if schema.Field(keyFields[0]).Type.ID() != arrow.INT64 {
		return nil, fmt.Errorf("golars: fast group-by requires an int64 key, got %s", schema.Field(keyFields[0]).Type)
	}
	valueKind, ok := fastValueKindFor(schema.Field(valueFields[0]).Type.ID())
	if !ok {
		return nil, fmt.Errorf("golars: fast group-by requires int64 or float64 values, got %s", schema.Field(valueFields[0]).Type)
	}
	return &fastInt64GroupBy{
		engine:     engine,
		spec:       spec,
		keyIndex:   keyFields[0],
		valueIndex: valueFields[0],
		valueKind:  valueKind,
		groups:     make(map[int64]int),
		nullGroup:  -1,
	}, nil
}

func fastValueKindFor(id arrow.Type) (fastValueKind, bool) {
	switch id {
	case arrow.FLOAT64:
		return fastFloat64, true
	case arrow.INT64:
		return fastInt64, true
	default:
		return 0, false
	}
}

func (g *fastInt64GroupBy) add(batch arrow.RecordBatch) error {
	keys, ok := batch.Column(g.keyIndex).(*array.Int64)
	if !ok {
		return fmt.Errorf("golars: fast group-by key changed type to %T", batch.Column(g.keyIndex))
	}
	values := batch.Column(g.valueIndex)
	var floatValues *array.Float64
	var intValues *array.Int64
	switch g.valueKind {
	case fastFloat64:
		var ok bool
		floatValues, ok = values.(*array.Float64)
		if !ok {
			return fmt.Errorf("golars: fast group-by value changed type to %T", values)
		}
	case fastInt64:
		var ok bool
		intValues, ok = values.(*array.Int64)
		if !ok {
			return fmt.Errorf("golars: fast group-by value changed type to %T", values)
		}
	}
	for row := 0; row < keys.Len(); row++ {
		group := g.groupIndex(keys, row)
		valueValid := values.IsValid(row)
		if g.spec.aggName == "count" {
			if valueValid {
				g.counts[group]++
			}
			continue
		}
		if !valueValid {
			continue
		}
		g.counts[group]++
		switch g.valueKind {
		case fastFloat64:
			g.floatSums[group] += floatValues.Value(row)
		case fastInt64:
			value := intValues.Value(row)
			if g.spec.aggName == "mean" {
				g.floatSums[group] += float64(value)
			} else {
				g.intSums[group] += value
			}
		}
	}
	return nil
}

func (g *fastInt64GroupBy) groupIndex(keys *array.Int64, row int) int {
	if !keys.IsValid(row) {
		if g.nullGroup >= 0 {
			return g.nullGroup
		}
		g.nullGroup = g.newGroup(0, false)
		return g.nullGroup
	}
	key := keys.Value(row)
	if key >= 0 && key < int64(len(g.denseGroups)) {
		index := int(key)
		if g.denseSeen[index] {
			return g.denseGroups[index]
		}
		group := g.newGroup(key, true)
		g.denseSeen[index] = true
		g.denseGroups[index] = group
		return group
	}
	if group, ok := g.groups[key]; ok {
		return group
	}
	group := g.newGroup(key, true)
	g.groups[key] = group
	return group
}

func (g *fastInt64GroupBy) newGroup(key int64, valid bool) int {
	group := len(g.keys)
	g.keys = append(g.keys, key)
	g.validKeys = append(g.validKeys, valid)
	g.floatSums = append(g.floatSums, 0)
	g.intSums = append(g.intSums, 0)
	g.counts = append(g.counts, 0)
	return group
}

func (g *fastInt64GroupBy) result() (*DataFrame, error) {
	keyBuilder := array.NewInt64Builder(g.engine.allocator)
	defer keyBuilder.Release()
	for i, key := range g.keys {
		if g.validKeys[i] {
			keyBuilder.Append(key)
		} else {
			keyBuilder.AppendNull()
		}
	}
	keyArray := keyBuilder.NewInt64Array()
	defer keyArray.Release()

	var valueArray arrow.Array
	switch {
	case g.spec.aggName == "count":
		builder := array.NewInt64Builder(g.engine.allocator)
		for _, count := range g.counts {
			builder.Append(count)
		}
		valueArray = builder.NewInt64Array()
		builder.Release()
	case g.spec.aggName == "mean":
		builder := array.NewFloat64Builder(g.engine.allocator)
		for i, sum := range g.floatSums {
			if g.counts[i] == 0 {
				builder.AppendNull()
			} else {
				builder.Append(sum / float64(g.counts[i]))
			}
		}
		valueArray = builder.NewFloat64Array()
		builder.Release()
	case g.valueKind == fastFloat64:
		builder := array.NewFloat64Builder(g.engine.allocator)
		for i, sum := range g.floatSums {
			if g.counts[i] == 0 {
				builder.AppendNull()
			} else {
				builder.Append(sum)
			}
		}
		valueArray = builder.NewFloat64Array()
		builder.Release()
	case g.valueKind == fastInt64:
		builder := array.NewInt64Builder(g.engine.allocator)
		for i, sum := range g.intSums {
			if g.counts[i] == 0 {
				builder.AppendNull()
			} else {
				builder.Append(sum)
			}
		}
		valueArray = builder.NewInt64Array()
		builder.Release()
	default:
		return nil, fmt.Errorf("golars: unsupported fast group-by value kind")
	}
	defer valueArray.Release()
	fields := []arrow.Field{
		{Name: g.spec.keyOutput, Type: arrow.PrimitiveTypes.Int64, Nullable: true},
		{Name: g.spec.aggOutput, Type: valueArray.DataType(), Nullable: true},
	}
	table := array.NewTableFromSlice(arrow.NewSchema(fields, nil), [][]arrow.Array{{keyArray}, {valueArray}})
	return newDataFrameWithTable(table, g.engine), nil
}

func executeFastGroupBy(ctx context.Context, base *LazyFrame, request scanRequest, spec fastGroupBySpec) (*DataFrame, error) {
	var groups *fastInt64GroupBy
	err := base.executeBatchesWithScanRequest(ctx, &request, func(batch arrow.RecordBatch) error {
		if groups == nil {
			var err error
			groups, err = newFastInt64GroupBy(base.engine, batch.Schema(), spec)
			if err != nil {
				return err
			}
		}
		return groups.add(batch)
	})
	if err != nil {
		return nil, err
	}
	if groups == nil {
		return nil, fmt.Errorf("golars: group-by source produced no schema")
	}
	return groups.result()
}
