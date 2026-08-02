package core

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"reflect"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/guywaldman/golars/internal/storageuri"
)

// nativeQuery is the small, versioned boundary between the Go facade and the
// bundled Polars runtime. A query is recursively composable: joins carry a
// second query, while in-memory and reader sources carry Arrow IPC bytes.
type nativeQuery struct {
	Path       string            `json:"path,omitempty"`
	Format     string            `json:"format"`
	Input      string            `json:"input,omitempty"`
	HasHeader  bool              `json:"has_header,omitempty"`
	Delimiter  string            `json:"delimiter,omitempty"`
	Operations []nativeOperation `json:"operations,omitempty"`
}

type nativeOperation struct {
	Kind      string       `json:"kind"`
	Exprs     []nativeExpr `json:"exprs,omitempty"`
	Keys      []nativeExpr `json:"keys,omitempty"`
	RightKeys []nativeExpr `json:"right_keys,omitempty"`
	Columns   []string     `json:"columns,omitempty"`
	JoinType  string       `json:"join_type,omitempty"`
	Suffix    string       `json:"suffix,omitempty"`
	Right     *nativeQuery `json:"right,omitempty"`
	Limit     uint64       `json:"limit,omitempty"`
}

type nativeExpr struct {
	Kind       string            `json:"kind"`
	Name       string            `json:"name,omitempty"`
	Literal    *nativeLiteral    `json:"literal,omitempty"`
	Args       []nativeExpr      `json:"args,omitempty"`
	WindowKeys []nativeExpr      `json:"window_keys,omitempty"`
	DType      string            `json:"dtype,omitempty"`
	JSONDecode *nativeJSONDecode `json:"json_decode,omitempty"`
	Descending bool              `json:"descending,omitempty"`
}

type nativeJSONDecode struct {
	Schema         nativeJSONType `json:"schema"`
	OnError        string         `json:"on_error"`
	Coerce         bool           `json:"coerce"`
	StrictRequired bool           `json:"strict_required_fields"`
}

type nativeLiteral struct {
	Kind  string `json:"kind"`
	Value any    `json:"value,omitempty"`
}

func marshalNativePlan(ctx context.Context, engine *Engine, root *planNode) ([]byte, error) {
	query, ok, err := nativeQueryForPlan(ctx, engine, root)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, fmt.Errorf("golars: plan is not supported by the native Polars backend")
	}
	data, err := json.Marshal(query)
	if err != nil {
		return nil, fmt.Errorf("golars: encode native plan: %w", err)
	}
	return data, nil
}

func nativeQueryForPlan(ctx context.Context, engine *Engine, root *planNode) (nativeQuery, bool, error) {
	if root == nil {
		return nativeQuery{}, false, nil
	}

	var query nativeQuery
	var ok bool
	var err error
	switch root.kind {
	case planTable:
		if root.table == nil {
			return nativeQuery{}, false, nil
		}
		input, inputErr := tableIPC(root.table, engine)
		if inputErr != nil {
			return nativeQuery{}, true, inputErr
		}
		query = nativeQuery{Format: "ipc", Input: base64.StdEncoding.EncodeToString(input)}
		ok = true
	case planSource:
		query, ok, err = nativeSourceQuery(ctx, engine, root)
	default:
		query, ok, err = nativeQueryForPlan(ctx, engine, root.input)
		if err != nil || !ok {
			return nativeQuery{}, ok, err
		}
		operation := nativeOperation{}
		switch root.kind {
		case planFilter:
			if len(root.exprs) != 1 {
				return nativeQuery{}, false, nil
			}
			expr, exprOK, exprErr := marshalNativeExpr(root.exprs[0].node)
			if exprErr != nil || !exprOK {
				return nativeQuery{}, false, exprErr
			}
			operation.Kind = "filter"
			operation.Exprs = []nativeExpr{expr}
		case planSelect:
			operation.Kind = "select"
			exprs, exprOK, exprErr := marshalNativeExprs(root.exprs)
			if exprErr != nil || !exprOK {
				return nativeQuery{}, false, exprErr
			}
			operation.Exprs = exprs
		case planWithColumns:
			operation.Kind = "with_columns"
			exprs, exprOK, exprErr := marshalNativeExprs(root.exprs)
			if exprErr != nil || !exprOK {
				return nativeQuery{}, false, exprErr
			}
			operation.Exprs = exprs
		case planLimit:
			operation.Kind = "limit"
			operation.Limit = uint64(root.limit)
		case planTail:
			operation.Kind = "tail"
			operation.Limit = uint64(root.limit)
		case planSort:
			operation.Kind = "sort"
			exprs, exprOK, exprErr := marshalNativeExprs(root.exprs)
			if exprErr != nil || !exprOK {
				return nativeQuery{}, false, exprErr
			}
			operation.Exprs = exprs
		case planGroupBy:
			operation.Kind = "group_by"
			keys, exprOK, exprErr := marshalNativeExprs(root.keys)
			if exprErr != nil || !exprOK {
				return nativeQuery{}, false, exprErr
			}
			exprs, exprOK, exprErr := marshalNativeExprs(root.exprs)
			if exprErr != nil || !exprOK {
				return nativeQuery{}, false, exprErr
			}
			operation.Keys = keys
			operation.Exprs = exprs
		case planJoin:
			if root.right == nil {
				return nativeQuery{}, false, nil
			}
			leftKeys, exprOK, exprErr := marshalNativeExprs(root.leftKeys)
			if exprErr != nil || !exprOK {
				return nativeQuery{}, false, exprErr
			}
			rightKeys, exprOK, exprErr := marshalNativeExprs(root.rightKeys)
			if exprErr != nil || !exprOK {
				return nativeQuery{}, false, exprErr
			}
			right, rightOK, rightErr := nativeQueryForPlan(ctx, engine, root.right)
			if rightErr != nil || !rightOK {
				return nativeQuery{}, rightOK, rightErr
			}
			operation.Kind = "join"
			operation.Exprs = leftKeys
			operation.RightKeys = rightKeys
			operation.Right = &right
			operation.JoinType = root.joinType.String()
			operation.Suffix = root.suffix
		case planUnique:
			operation.Kind = "unique"
			operation.Columns = append([]string(nil), root.columns...)
		case planExplode:
			operation.Kind = "explode"
			operation.Columns = append([]string(nil), root.columns...)
		case planUnnest:
			operation.Kind = "unnest"
			operation.Columns = append([]string(nil), root.columns...)
		default:
			return nativeQuery{}, false, nil
		}
		query.Operations = append(query.Operations, operation)
	}
	return query, ok, err
}

func nativeSourceQuery(ctx context.Context, engine *Engine, node *planNode) (nativeQuery, bool, error) {
	format, ok := nativeFormat(node.sourceFormat)
	if !ok {
		return nativeQuery{}, false, nil
	}
	// Keep custom Go object stores and JSON-array readers at the Go I/O
	// boundary, then hand the resulting Arrow IPC to Polars for all execution.
	// Local Parquet/CSV/NDJSON/IPC paths stay lazy and are opened directly by
	// Polars.
	if node.sourcePath != "" && storageuri.Scheme(node.sourcePath) == "" && node.sourceFormat != formatJSON && node.sourceFormat != formatNDJSON {
		query := nativeQuery{Path: node.sourcePath, Format: format}
		if node.sourceFormat == formatCSV {
			query.HasHeader = node.sourceConfig.hasHeader
			query.Delimiter = string(node.sourceConfig.delimiter)
		}
		return query, true, nil
	}
	if node.source == nil || engine == nil {
		return nativeQuery{}, false, nil
	}
	source, err := node.source(ctx, engine)
	if err != nil {
		return nativeQuery{}, true, err
	}
	defer source.Release()
	input, err := sourceIPC(source, engine)
	if err != nil {
		return nativeQuery{}, true, err
	}
	return nativeQuery{Format: "ipc", Input: base64.StdEncoding.EncodeToString(input)}, true, nil
}

func nativeFormat(format formatKind) (string, bool) {
	switch format {
	case formatParquet:
		return "parquet", true
	case formatCSV:
		return "csv", true
	case formatNDJSON:
		return "ndjson", true
	case formatJSON:
		return "json", true
	case formatIPCFile:
		return "ipc_file", true
	case formatIPCStream:
		return "ipc_stream", true
	default:
		return "", false
	}
}

func tableIPC(table arrow.Table, engine *Engine) ([]byte, error) {
	reader := newTableSource(table, engine.batchSize)
	defer reader.Release()
	return sourceIPC(reader, engine)
}

func sourceIPC(source recordSource, engine *Engine) ([]byte, error) {
	if source == nil || source.Schema() == nil || engine == nil {
		return nil, fmt.Errorf("golars: native source has no schema")
	}
	var output bytes.Buffer
	writer, err := ipc.NewFileWriter(&output, ipc.WithSchema(source.Schema()), ipc.WithAllocator(engine.allocator))
	if err != nil {
		return nil, fmt.Errorf("golars: create native IPC input: %w", err)
	}
	for source.Next() {
		if err := writer.Write(source.RecordBatch()); err != nil {
			_ = writer.Close()
			return nil, fmt.Errorf("golars: encode native IPC input: %w", err)
		}
	}
	if err := source.Err(); err != nil {
		_ = writer.Close()
		return nil, err
	}
	if err := writer.Close(); err != nil {
		return nil, fmt.Errorf("golars: close native IPC input: %w", err)
	}
	return output.Bytes(), nil
}

func marshalNativeExprs(exprs []Expr) ([]nativeExpr, bool, error) {
	result := make([]nativeExpr, len(exprs))
	for i, expr := range exprs {
		marshaled, ok, err := marshalNativeExpr(expr.node)
		if err != nil || !ok {
			return nil, ok, err
		}
		result[i] = marshaled
	}
	return result, true, nil
}

func marshalNativeExpr(node *exprNode) (nativeExpr, bool, error) {
	if node == nil {
		return nativeExpr{}, false, nil
	}
	result := nativeExpr{Descending: node.descending}
	if len(node.windowKeys) != 0 {
		keys, ok, err := marshalNativeExprNodes(node.windowKeys)
		if err != nil || !ok {
			return nativeExpr{}, ok, err
		}
		result.WindowKeys = keys
	}
	switch node.kind {
	case exprColumn:
		result.Kind = "column"
		result.Name = node.name
	case exprLiteral:
		literal, ok, err := marshalNativeLiteral(node.value)
		if err != nil || !ok {
			return nativeExpr{}, ok, err
		}
		result.Kind = "literal"
		result.Literal = &literal
	case exprAlias:
		if len(node.args) != 1 {
			return nativeExpr{}, false, nil
		}
		arg, ok, err := marshalNativeExpr(node.args[0])
		if err != nil || !ok {
			return nativeExpr{}, ok, err
		}
		result.Kind = "alias"
		result.Name = node.name
		result.Args = []nativeExpr{arg}
	case exprCast:
		dtype, ok := nativeDataType(node.dtype)
		if !ok || len(node.args) != 1 {
			return nativeExpr{}, false, nil
		}
		arg, ok, err := marshalNativeExpr(node.args[0])
		if err != nil || !ok {
			return nativeExpr{}, ok, err
		}
		result.Kind = "cast"
		result.DType = dtype
		result.Args = []nativeExpr{arg}
	case exprAggregate:
		result.Kind = "aggregate"
		result.Name = node.name
		if len(node.args) > 1 {
			return nativeExpr{}, false, nil
		}
		if len(node.args) == 1 {
			arg, ok, err := marshalNativeExpr(node.args[0])
			if err != nil || !ok {
				return nativeExpr{}, ok, err
			}
			result.Args = []nativeExpr{arg}
		}
	case exprCall:
		if node.name == "modulo" && len(node.args) == 2 && literalZero(node.args[1]) {
			return nativeExpr{}, true, fmt.Errorf("golars: modulo by zero")
		}
		result.Kind = "call"
		result.Name = node.name
		args, ok, err := marshalNativeExprNodes(node.args)
		if err != nil || !ok {
			return nativeExpr{}, ok, err
		}
		result.Args = args
		if node.jsonDecode != nil {
			if len(node.args) != 1 {
				return nativeExpr{}, false, nil
			}
			if node.jsonDecode.errorMode != JSONNullOnError && node.jsonDecode.errorMode != JSONErrorOnError {
				return nativeExpr{}, true, fmt.Errorf("golars: invalid JSON error mode %d", node.jsonDecode.errorMode)
			}
			schema, schemaErr := nativeJSONTypeFor(node.jsonDecode.schema)
			if schemaErr != nil {
				return nativeExpr{}, true, schemaErr
			}
			mode := "null"
			if node.jsonDecode.errorMode == JSONErrorOnError {
				mode = "error"
			}
			result.JSONDecode = &nativeJSONDecode{
				Schema:         schema,
				OnError:        mode,
				Coerce:         node.jsonDecode.coerce,
				StrictRequired: node.jsonDecode.strictRequired,
			}
		}
	default:
		return nativeExpr{}, false, nil
	}
	return result, true, nil
}

func literalZero(node *exprNode) bool {
	if node == nil || node.kind != exprLiteral {
		return false
	}
	switch value := node.value.(type) {
	case int:
		return value == 0
	case int8:
		return value == 0
	case int16:
		return value == 0
	case int32:
		return value == 0
	case int64:
		return value == 0
	case uint:
		return value == 0
	case uint8:
		return value == 0
	case uint16:
		return value == 0
	case uint32:
		return value == 0
	case uint64:
		return value == 0
	case float32:
		return value == 0
	case float64:
		return value == 0
	default:
		return false
	}
}

func marshalNativeExprNodes(nodes []*exprNode) ([]nativeExpr, bool, error) {
	result := make([]nativeExpr, len(nodes))
	for i, node := range nodes {
		expr, ok, err := marshalNativeExpr(node)
		if err != nil || !ok {
			return nil, ok, err
		}
		result[i] = expr
	}
	return result, true, nil
}

func marshalNativeLiteral(value any) (nativeLiteral, bool, error) {
	switch value := value.(type) {
	case nil:
		return nativeLiteral{Kind: "null"}, true, nil
	case bool:
		return nativeLiteral{Kind: "bool", Value: value}, true, nil
	case string:
		return nativeLiteral{Kind: "string", Value: value}, true, nil
	case int:
		return nativeLiteral{Kind: "int", Value: int64(value)}, true, nil
	case int8:
		return nativeLiteral{Kind: "int", Value: int64(value)}, true, nil
	case int16:
		return nativeLiteral{Kind: "int", Value: int64(value)}, true, nil
	case int32:
		return nativeLiteral{Kind: "int", Value: int64(value)}, true, nil
	case int64:
		return nativeLiteral{Kind: "int", Value: value}, true, nil
	case uint:
		return nativeLiteral{Kind: "uint", Value: uint64(value)}, true, nil
	case uint8:
		return nativeLiteral{Kind: "uint", Value: uint64(value)}, true, nil
	case uint16:
		return nativeLiteral{Kind: "uint", Value: uint64(value)}, true, nil
	case uint32:
		return nativeLiteral{Kind: "uint", Value: uint64(value)}, true, nil
	case uint64:
		return nativeLiteral{Kind: "uint", Value: value}, true, nil
	case float32:
		return nativeLiteral{Kind: "float", Value: float64(value)}, true, nil
	case float64:
		return nativeLiteral{Kind: "float", Value: value}, true, nil
	case time.Time:
		return nativeLiteral{Kind: "timestamp", Value: value.UnixNano()}, true, nil
	default:
		rv := reflect.ValueOf(value)
		if rv.IsValid() && (rv.Kind() == reflect.Slice || rv.Kind() == reflect.Array) {
			values := make([]any, rv.Len())
			for index := range values {
				values[index] = rv.Index(index).Interface()
			}
			return nativeLiteral{Kind: "list", Value: values}, true, nil
		}
		return nativeLiteral{}, false, nil
	}
}

func nativeDataType(dtype DataType) (string, bool) {
	switch dtype.ID() {
	case arrow.BOOL:
		return "bool", true
	case arrow.INT8:
		return "int8", true
	case arrow.INT16:
		return "int16", true
	case arrow.INT32:
		return "int32", true
	case arrow.INT64:
		return "int64", true
	case arrow.UINT8:
		return "uint8", true
	case arrow.UINT16:
		return "uint16", true
	case arrow.UINT32:
		return "uint32", true
	case arrow.UINT64:
		return "uint64", true
	case arrow.FLOAT32:
		return "float32", true
	case arrow.FLOAT64:
		return "float64", true
	case arrow.STRING:
		return "string", true
	default:
		return "", false
	}
}
