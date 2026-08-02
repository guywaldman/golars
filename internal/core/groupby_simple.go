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

type simpleGroupBySpec struct {
	keyName   string
	keyOutput string
	aggs      []simpleAggSpec
}

type simpleAggSpec struct {
	name      string
	output    string
	valueName string
}

func makeSimpleGroupBySpec(keys, exprs []Expr) (simpleGroupBySpec, bool) {
	if len(keys) != 1 || len(exprs) == 0 {
		return simpleGroupBySpec{}, false
	}
	key := unwrapAlias(keys[0].node)
	if key == nil || key.kind != exprColumn {
		return simpleGroupBySpec{}, false
	}
	spec := simpleGroupBySpec{
		keyName:   key.name,
		keyOutput: keys[0].outputName(),
		aggs:      make([]simpleAggSpec, 0, len(exprs)),
	}
	for _, expr := range exprs {
		agg := unwrapAlias(expr.node)
		if agg == nil || agg.kind != exprAggregate {
			return simpleGroupBySpec{}, false
		}
		switch agg.name {
		case "sum", "mean", "count":
			if len(agg.args) != 1 {
				return simpleGroupBySpec{}, false
			}
			value := unwrapAlias(agg.args[0])
			if value == nil || value.kind != exprColumn {
				return simpleGroupBySpec{}, false
			}
			spec.aggs = append(spec.aggs, simpleAggSpec{name: agg.name, output: expr.outputName(), valueName: value.name})
		case "count_rows":
			spec.aggs = append(spec.aggs, simpleAggSpec{name: agg.name, output: expr.outputName()})
		default:
			return simpleGroupBySpec{}, false
		}
	}
	return spec, true
}

type simpleKeyKind uint8

const (
	simpleInt64Key simpleKeyKind = iota
	simpleStringKey
)

type simpleAggState struct {
	valueKind fastValueKind
	valueType arrow.DataType
	counts    []int64
	floatSums []float64
	intSums   []int64
}

type simpleGroupBy struct {
	engine       *Engine
	spec         simpleGroupBySpec
	keyIndex     int
	keyType      arrow.DataType
	keyKind      simpleKeyKind
	valueIndices []int
	valueKinds   []fastValueKind
	valueTypes   []arrow.DataType
	intGroups    map[int64]int
	stringGroups map[string]int
	keysInt      []int64
	keysString   []string
	validKeys    []bool
	nullGroup    int
	aggs         []simpleAggState
}

func newSimpleGroupBy(engine *Engine, schema *arrow.Schema, spec simpleGroupBySpec) (*simpleGroupBy, error) {
	keyFields := schema.FieldIndices(spec.keyName)
	if len(keyFields) == 0 {
		return nil, &ColumnNotFoundError{Name: spec.keyName, Available: arrowutil.SchemaNames(schema)}
	}
	keyType := schema.Field(keyFields[0]).Type
	var keyKind simpleKeyKind
	switch keyType.ID() {
	case arrow.INT64:
		keyKind = simpleInt64Key
	case arrow.STRING, arrow.LARGE_STRING:
		keyKind = simpleStringKey
	default:
		return nil, fmt.Errorf("golars: simple group-by requires an int64 or string key, got %s", keyType)
	}
	group := &simpleGroupBy{
		engine:       engine,
		spec:         spec,
		keyIndex:     keyFields[0],
		keyType:      keyType,
		keyKind:      keyKind,
		valueIndices: make([]int, len(spec.aggs)),
		valueKinds:   make([]fastValueKind, len(spec.aggs)),
		valueTypes:   make([]arrow.DataType, len(spec.aggs)),
		intGroups:    make(map[int64]int),
		stringGroups: make(map[string]int),
		nullGroup:    -1,
		aggs:         make([]simpleAggState, len(spec.aggs)),
	}
	for i, agg := range spec.aggs {
		group.valueIndices[i] = -1
		if agg.name == "count_rows" {
			group.aggs[i] = simpleAggState{valueKind: fastInt64, valueType: arrow.PrimitiveTypes.Int64}
			continue
		}
		valueFields := schema.FieldIndices(agg.valueName)
		if len(valueFields) == 0 {
			return nil, &ColumnNotFoundError{Name: agg.valueName, Available: arrowutil.SchemaNames(schema)}
		}
		valueType := schema.Field(valueFields[0]).Type
		group.valueIndices[i] = valueFields[0]
		group.valueTypes[i] = valueType
		if agg.name == "count" {
			group.aggs[i] = simpleAggState{valueKind: fastInt64, valueType: valueType}
			continue
		}
		valueKind, ok := fastValueKindFor(valueType.ID())
		if !ok {
			return nil, fmt.Errorf("golars: simple group-by requires int64 or float64 values, got %s", valueType)
		}
		group.valueKinds[i] = valueKind
		group.aggs[i] = simpleAggState{valueKind: valueKind, valueType: valueType}
	}
	return group, nil
}

func (g *simpleGroupBy) add(batch arrow.RecordBatch) error {
	if batch.Schema().Field(g.keyIndex).Type.ID() != g.keyType.ID() {
		return fmt.Errorf("golars: simple group-by key changed type to %s", batch.Schema().Field(g.keyIndex).Type)
	}
	var intKeys *array.Int64
	var stringKeys array.StringLike
	if g.keyKind == simpleInt64Key {
		var ok bool
		intKeys, ok = batch.Column(g.keyIndex).(*array.Int64)
		if !ok {
			return fmt.Errorf("golars: simple group-by key changed type to %T", batch.Column(g.keyIndex))
		}
	} else {
		var ok bool
		stringKeys, ok = batch.Column(g.keyIndex).(array.StringLike)
		if !ok {
			return fmt.Errorf("golars: simple group-by key changed type to %T", batch.Column(g.keyIndex))
		}
	}
	values := make([]arrow.Array, len(g.valueIndices))
	floatValues := make([]*array.Float64, len(g.valueIndices))
	intValues := make([]*array.Int64, len(g.valueIndices))
	for i, index := range g.valueIndices {
		if index < 0 {
			continue
		}
		values[i] = batch.Column(index)
		if g.spec.aggs[i].name == "count" {
			continue
		}
		switch g.valueKinds[i] {
		case fastFloat64:
			var ok bool
			floatValues[i], ok = values[i].(*array.Float64)
			if !ok {
				return fmt.Errorf("golars: simple group-by value changed type to %T", values[i])
			}
		case fastInt64:
			var ok bool
			intValues[i], ok = values[i].(*array.Int64)
			if !ok {
				return fmt.Errorf("golars: simple group-by value changed type to %T", values[i])
			}
		}
	}
	for row := 0; row < int(batch.NumRows()); row++ {
		group := g.groupIndex(intKeys, stringKeys, row)
		for i, agg := range g.spec.aggs {
			state := &g.aggs[i]
			if agg.name == "count_rows" {
				state.counts[group]++
				continue
			}
			if !values[i].IsValid(row) {
				continue
			}
			state.counts[group]++
			switch agg.name {
			case "count":
			case "sum", "mean":
				switch state.valueKind {
				case fastFloat64:
					state.floatSums[group] += floatValues[i].Value(row)
				case fastInt64:
					if agg.name == "mean" {
						state.floatSums[group] += float64(intValues[i].Value(row))
					} else {
						state.intSums[group] += intValues[i].Value(row)
					}
				}
			}
		}
	}
	return nil
}

func (g *simpleGroupBy) groupIndex(intKeys *array.Int64, stringKeys array.StringLike, row int) int {
	if (g.keyKind == simpleInt64Key && !intKeys.IsValid(row)) ||
		(g.keyKind == simpleStringKey && !stringKeys.IsValid(row)) {
		if g.nullGroup >= 0 {
			return g.nullGroup
		}
		g.nullGroup = g.newGroup(0, "", false)
		return g.nullGroup
	}
	if g.keyKind == simpleInt64Key {
		key := intKeys.Value(row)
		if group, ok := g.intGroups[key]; ok {
			return group
		}
		group := g.newGroup(key, "", true)
		g.intGroups[key] = group
		return group
	}
	key := stringKeys.Value(row)
	if group, ok := g.stringGroups[key]; ok {
		return group
	}
	group := g.newGroup(0, strings.Clone(key), true)
	g.stringGroups[key] = group
	return group
}

func (g *simpleGroupBy) newGroup(key int64, text string, valid bool) int {
	group := len(g.validKeys)
	g.validKeys = append(g.validKeys, valid)
	g.keysInt = append(g.keysInt, key)
	g.keysString = append(g.keysString, text)
	for i := range g.aggs {
		g.aggs[i].counts = append(g.aggs[i].counts, 0)
		g.aggs[i].floatSums = append(g.aggs[i].floatSums, 0)
		g.aggs[i].intSums = append(g.aggs[i].intSums, 0)
	}
	return group
}

func (g *simpleGroupBy) result() (*DataFrame, error) {
	keyBuilder := array.NewBuilder(g.engine.allocator, g.keyType)
	for i := range g.validKeys {
		if !g.validKeys[i] {
			keyBuilder.AppendNull()
			continue
		}
		if g.keyKind == simpleInt64Key {
			if err := scalar.Append(keyBuilder, scalar.NewInt64Scalar(g.keysInt[i])); err != nil {
				keyBuilder.Release()
				return nil, err
			}
			continue
		}
		value, err := scalar.MakeScalarParam(g.keysString[i], g.keyType)
		if err != nil {
			keyBuilder.Release()
			return nil, err
		}
		if err := scalar.Append(keyBuilder, value); err != nil {
			keyBuilder.Release()
			return nil, err
		}
	}
	keyArray := keyBuilder.NewArray()
	keyBuilder.Release()
	columns := []arrow.Array{keyArray}
	fields := []arrow.Field{{Name: g.spec.keyOutput, Type: g.keyType, Nullable: true}}
	defer func() { arrowutil.ReleaseArrays(columns) }()
	for i, spec := range g.spec.aggs {
		var valueArray arrow.Array
		var valueType arrow.DataType
		switch spec.name {
		case "count", "count_rows":
			builder := array.NewInt64Builder(g.engine.allocator)
			for _, count := range g.aggs[i].counts {
				builder.Append(count)
			}
			valueArray = builder.NewInt64Array()
			builder.Release()
			valueType = arrow.PrimitiveTypes.Int64
		case "mean":
			builder := array.NewFloat64Builder(g.engine.allocator)
			for group, sum := range g.aggs[i].floatSums {
				if g.aggs[i].counts[group] == 0 {
					builder.AppendNull()
				} else {
					builder.Append(sum / float64(g.aggs[i].counts[group]))
				}
			}
			valueArray = builder.NewFloat64Array()
			builder.Release()
			valueType = arrow.PrimitiveTypes.Float64
		case "sum":
			switch g.aggs[i].valueKind {
			case fastFloat64:
				builder := array.NewFloat64Builder(g.engine.allocator)
				for group, sum := range g.aggs[i].floatSums {
					if g.aggs[i].counts[group] == 0 {
						builder.AppendNull()
					} else {
						builder.Append(sum)
					}
				}
				valueArray = builder.NewFloat64Array()
				builder.Release()
			case fastInt64:
				builder := array.NewInt64Builder(g.engine.allocator)
				for group, sum := range g.aggs[i].intSums {
					if g.aggs[i].counts[group] == 0 {
						builder.AppendNull()
					} else {
						builder.Append(sum)
					}
				}
				valueArray = builder.NewInt64Array()
				builder.Release()
			default:
				return nil, fmt.Errorf("golars: unsupported simple group-by sum type %s", g.aggs[i].valueType)
			}
			valueType = g.aggs[i].valueType
		}
		columns = append(columns, valueArray)
		fields = append(fields, arrow.Field{Name: spec.output, Type: valueType, Nullable: true})
	}
	table := array.NewTableFromSlice(arrow.NewSchema(fields, nil), arrowutil.ArraysAsSlices(columns))
	return &DataFrame{table: table, engine: g.engine}, nil
}

func executeSimpleGroupBy(ctx context.Context, base *LazyFrame, request scanRequest, spec simpleGroupBySpec) (*DataFrame, error) {
	var groups *simpleGroupBy
	err := base.executeBatchesWithScanRequest(ctx, &request, func(batch arrow.RecordBatch) error {
		if groups == nil {
			var err error
			groups, err = newSimpleGroupBy(base.engine, batch.Schema(), spec)
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
