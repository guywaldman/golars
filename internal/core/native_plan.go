package core

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/guywaldman/golars/internal/storageuri"
)

type nativeQuery struct {
	Path       string            `json:"path"`
	Format     string            `json:"format"`
	Operations []nativeOperation `json:"operations,omitempty"`
}

type nativeOperation struct {
	Kind  string       `json:"kind"`
	Exprs []nativeExpr `json:"exprs,omitempty"`
	Keys  []nativeExpr `json:"keys,omitempty"`
	Limit uint64       `json:"limit,omitempty"`
}

type nativeExpr struct {
	Kind       string         `json:"kind"`
	Name       string         `json:"name,omitempty"`
	Literal    *nativeLiteral `json:"literal,omitempty"`
	Args       []nativeExpr   `json:"args,omitempty"`
	DType      string         `json:"dtype,omitempty"`
	Descending bool           `json:"descending,omitempty"`
}

type nativeLiteral struct {
	Kind  string `json:"kind"`
	Value any    `json:"value,omitempty"`
}

func marshalNativePlan(root *planNode) ([]byte, bool, error) {
	query, ok, err := nativeQueryForPlan(root)
	if !ok || err != nil {
		return nil, ok, err
	}
	data, err := json.Marshal(query)
	if err != nil {
		return nil, true, fmt.Errorf("golars: encode native plan: %w", err)
	}
	return data, true, nil
}

func nativeQueryForPlan(root *planNode) (nativeQuery, bool, error) {
	if root == nil {
		return nativeQuery{}, false, nil
	}
	nodes := make([]*planNode, 0, 8)
	for node := root; node != nil; node = node.input {
		if node.right != nil || node.table != nil {
			return nativeQuery{}, false, nil
		}
		nodes = append(nodes, node)
	}
	if len(nodes) == 0 || nodes[len(nodes)-1].kind != planSource {
		return nativeQuery{}, false, nil
	}
	for left, right := 0, len(nodes)-1; left < right; left, right = left+1, right-1 {
		nodes[left], nodes[right] = nodes[right], nodes[left]
	}
	source := nodes[0]
	if source.sourceFormat != formatParquet || source.sourcePath == "" || storageuri.Scheme(source.sourcePath) != "" || strings.ContainsAny(source.sourcePath, "*?[") {
		return nativeQuery{}, false, nil
	}
	info, err := os.Stat(source.sourcePath)
	if err != nil || !info.Mode().IsRegular() {
		return nativeQuery{}, false, nil
	}
	query := nativeQuery{Path: source.sourcePath, Format: "parquet"}
	for _, node := range nodes[1:] {
		operation := nativeOperation{}
		switch node.kind {
		case planFilter:
			if len(node.exprs) != 1 {
				return nativeQuery{}, false, nil
			}
			expr, ok, err := marshalNativeExpr(node.exprs[0].node)
			if err != nil || !ok {
				return nativeQuery{}, false, err
			}
			operation.Kind = "filter"
			operation.Exprs = []nativeExpr{expr}
		case planSelect:
			operation.Kind = "select"
			exprs, ok, err := marshalNativeExprs(node.exprs)
			if err != nil || !ok {
				return nativeQuery{}, false, err
			}
			operation.Exprs = exprs
		case planWithColumns:
			operation.Kind = "with_columns"
			exprs, ok, err := marshalNativeExprs(node.exprs)
			if err != nil || !ok {
				return nativeQuery{}, false, err
			}
			operation.Exprs = exprs
		case planLimit:
			operation.Kind = "limit"
			operation.Limit = uint64(node.limit)
		case planSort:
			operation.Kind = "sort"
			exprs, ok, err := marshalNativeExprs(node.exprs)
			if err != nil || !ok {
				return nativeQuery{}, false, err
			}
			operation.Exprs = exprs
		case planGroupBy:
			operation.Kind = "group_by"
			keys, ok, err := marshalNativeExprs(node.keys)
			if err != nil || !ok {
				return nativeQuery{}, false, err
			}
			exprs, ok, err := marshalNativeExprs(node.exprs)
			if err != nil || !ok {
				return nativeQuery{}, false, err
			}
			operation.Keys = keys
			operation.Exprs = exprs
		default:
			return nativeQuery{}, false, nil
		}
		query.Operations = append(query.Operations, operation)
	}
	return query, true, nil
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
	if node == nil || len(node.windowKeys) != 0 {
		return nativeExpr{}, false, nil
	}
	result := nativeExpr{Descending: node.descending}
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
		// The portable executor owns modulo semantics until the bundled native
		// backend exposes the same operation.
		if node.name == "modulo" {
			return nativeExpr{}, false, nil
		}
		result.Kind = "call"
		result.Name = node.name
		args, ok, err := marshalNativeExprNodes(node.args)
		if err != nil || !ok {
			return nativeExpr{}, ok, err
		}
		result.Args = args
	default:
		return nativeExpr{}, false, nil
	}
	return result, true, nil
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
	default:
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
