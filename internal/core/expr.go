package core

import (
	"fmt"
	"strings"
)

type exprKind uint8

const (
	exprColumn exprKind = iota
	exprLiteral
	exprCall
	exprAlias
	exprCast
	exprAggregate
)

type exprNode struct {
	kind       exprKind
	name       string
	field      string
	value      any
	args       []*exprNode
	windowKeys []*exprNode
	dtype      DataType
	jsonDecode *jsonDecodeConfig
	distinct   bool
	descending bool
}

// Expr is an immutable expression tree used by both eager and lazy operations.
type Expr struct{ node *exprNode }

func cloneExprList(exprs []Expr) []Expr {
	result := make([]Expr, len(exprs))
	for i, expr := range exprs {
		result[i] = Expr{node: cloneExprNode(expr.node)}
	}
	return result
}

func cloneExprNode(node *exprNode) *exprNode {
	if node == nil {
		return nil
	}
	copy := *node
	copy.args = make([]*exprNode, len(node.args))
	for i, arg := range node.args {
		copy.args[i] = cloneExprNode(arg)
	}
	copy.windowKeys = make([]*exprNode, len(node.windowKeys))
	for i, key := range node.windowKeys {
		copy.windowKeys[i] = cloneExprNode(key)
	}
	return &copy
}

// Col creates a column expression.
func Col(name string) Expr { return Expr{&exprNode{kind: exprColumn, name: name}} }

// Lit creates a literal expression.
func Lit(value any) Expr { return Expr{&exprNode{kind: exprLiteral, value: value}} }

// All selects all columns in a projection.
func All() Expr { return Expr{&exprNode{kind: exprCall, name: "all_columns"}} }

func (e Expr) binary(name string, rhs Expr) Expr {
	return Expr{&exprNode{kind: exprCall, name: name, args: []*exprNode{e.node, rhs.node}}}
}
func (e Expr) unary(name string) Expr {
	return Expr{&exprNode{kind: exprCall, name: name, args: []*exprNode{e.node}}}
}

func (e Expr) call(name string, args ...Expr) Expr {
	nodes := make([]*exprNode, len(args)+1)
	nodes[0] = e.node
	for i, arg := range args {
		nodes[i+1] = arg.node
	}
	return Expr{&exprNode{kind: exprCall, name: name, args: nodes}}
}

// Eq compares two expressions for equality.
func (e Expr) Eq(rhs Expr) Expr { return e.binary("equal", rhs) }

// Ne compares two expressions for inequality.
func (e Expr) Ne(rhs Expr) Expr { return e.binary("not_equal", rhs) }

// Gt compares whether e is greater than rhs.
func (e Expr) Gt(rhs Expr) Expr { return e.binary("greater", rhs) }

// Ge compares whether e is greater than or equal to rhs.
func (e Expr) Ge(rhs Expr) Expr { return e.binary("greater_equal", rhs) }

// Lt compares whether e is less than rhs.
func (e Expr) Lt(rhs Expr) Expr { return e.binary("less", rhs) }

// Le compares whether e is less than or equal to rhs.
func (e Expr) Le(rhs Expr) Expr { return e.binary("less_equal", rhs) }

// Add adds two expressions.
func (e Expr) Add(rhs Expr) Expr { return e.binary("add", rhs) }

// Sub subtracts rhs from e.
func (e Expr) Sub(rhs Expr) Expr { return e.binary("subtract", rhs) }

// Mul multiplies two expressions.
func (e Expr) Mul(rhs Expr) Expr { return e.binary("multiply", rhs) }

// Div divides e by rhs.
func (e Expr) Div(rhs Expr) Expr { return e.binary("divide", rhs) }

// Mod computes the remainder of e divided by rhs.
func (e Expr) Mod(rhs Expr) Expr { return e.binary("modulo", rhs) }

// FloorDiv divides e by rhs and rounds the result toward negative infinity.
func (e Expr) FloorDiv(rhs Expr) Expr { return e.binary("floor_divide", rhs) }

// Pow raises e to the power rhs.
func (e Expr) Pow(rhs Expr) Expr { return e.binary("power", rhs) }

// Neg negates numeric values.
func (e Expr) Neg() Expr { return e.unary("negate") }

// Abs returns absolute values.
func (e Expr) Abs() Expr { return e.unary("abs") }

// Sqrt returns square roots.
func (e Expr) Sqrt() Expr { return e.unary("sqrt") }

// Exp returns e raised to the natural exponential.
func (e Expr) Exp() Expr { return e.unary("exp") }

// Log returns natural logarithms.
func (e Expr) Log() Expr { return e.unary("log") }

// Log10 returns base-10 logarithms.
func (e Expr) Log10() Expr { return e.unary("log10") }

// Sin returns sine values.
func (e Expr) Sin() Expr { return e.unary("sin") }

// Cos returns cosine values.
func (e Expr) Cos() Expr { return e.unary("cos") }

// Tan returns tangent values.
func (e Expr) Tan() Expr { return e.unary("tan") }

// Floor rounds values down to the nearest integer.
func (e Expr) Floor() Expr { return e.unary("floor") }

// Ceil rounds values up to the nearest integer.
func (e Expr) Ceil() Expr { return e.unary("ceil") }

// Round rounds numeric values to decimals places.
func (e Expr) Round(decimals int) Expr {
	return e.call("round", Lit(int64(decimals)))
}

// Clip limits values to the inclusive range [min, max].
func (e Expr) Clip(min, max Expr) Expr { return e.call("clip", min, max) }

// And combines boolean expressions with Kleene logic.
func (e Expr) And(rhs Expr) Expr { return e.binary("and_kleene", rhs) }

// Or combines boolean expressions with Kleene logic.
func (e Expr) Or(rhs Expr) Expr { return e.binary("or_kleene", rhs) }

// Not inverts a boolean expression.
func (e Expr) Not() Expr { return e.unary("invert") }

// IsNull tests for null values.
func (e Expr) IsNull() Expr { return e.unary("is_null") }

// IsNotNull tests for non-null values.
func (e Expr) IsNotNull() Expr { return e.unary("is_valid") }

// FillNull replaces null values with value.
func (e Expr) FillNull(value Expr) Expr { return e.binary("coalesce", value) }

// FillNaN replaces NaN values with value.
func (e Expr) FillNaN(value Expr) Expr { return e.binary("fill_nan", value) }

// DropNulls removes null values from an expression result.
func (e Expr) DropNulls() Expr { return e.unary("drop_nulls") }

// DropNaNs removes NaN values from an expression result.
func (e Expr) DropNaNs() Expr { return e.unary("drop_nans") }

// Alias assigns an output name to an expression.
func (e Expr) Alias(name string) Expr {
	return Expr{&exprNode{kind: exprAlias, name: name, args: []*exprNode{e.node}}}
}

// Desc sorts an expression in descending order.
func (e Expr) Desc() Expr {
	if e.node == nil {
		return e
	}
	copy := *e.node
	copy.descending = true
	return Expr{node: &copy}
}

// Cast converts an expression to dtype.
func (e Expr) Cast(dtype DataType) Expr {
	return Expr{&exprNode{kind: exprCast, dtype: dtype, args: []*exprNode{e.node}}}
}

// Over evaluates an expression independently for each partition. Aggregate
// expressions (for example Col("value").Sum().Over(Col("group"))) return one
// value per input row, matching the shape of the input frame.
func (e Expr) Over(partitionBy ...Expr) Expr {
	if e.node == nil {
		return e
	}
	copy := *e.node
	copy.windowKeys = make([]*exprNode, len(partitionBy))
	for i, key := range partitionBy {
		copy.windowKeys[i] = cloneExprNode(key.node)
	}
	return Expr{node: &copy}
}

// Rank returns the rank within each window partition.
func (e Expr) Rank() Expr { return e.unary("rank") }

// DenseRank returns dense rank within each window partition.
func (e Expr) DenseRank() Expr { return e.unary("dense_rank") }

// RowNumber returns the row number within each window partition.
func (e Expr) RowNumber() Expr { return e.unary("row_number") }

// CumSum computes a cumulative sum.
func (e Expr) CumSum() Expr { return e.unary("cum_sum") }

func (e Expr) aggregate(name string) Expr {
	return Expr{&exprNode{kind: exprAggregate, name: name, args: []*exprNode{e.node}}}
}

// Sum computes a sum aggregation.
func (e Expr) Sum() Expr { return e.aggregate("sum") }

// Min computes a minimum aggregation.
func (e Expr) Min() Expr { return e.aggregate("min") }

// Max computes a maximum aggregation.
func (e Expr) Max() Expr { return e.aggregate("max") }

// Mean computes a mean aggregation.
func (e Expr) Mean() Expr { return e.aggregate("mean") }

// Count counts non-null values in an expression.
func (e Expr) Count() Expr { return e.aggregate("count") }

// First returns the first value in an aggregation.
func (e Expr) First() Expr { return e.aggregate("first") }

// Last returns the last value in an aggregation.
func (e Expr) Last() Expr { return e.aggregate("last") }

// Median computes the median aggregation.
func (e Expr) Median() Expr { return e.aggregate("median") }

// Product computes the product aggregation.
func (e Expr) Product() Expr { return e.aggregate("product") }

// NUnique counts distinct values in an aggregation.
func (e Expr) NUnique() Expr { return e.aggregate("n_unique") }

// Len returns the number of values in an aggregation.
func (e Expr) Len() Expr { return e.aggregate("len") }

// Any reports whether any value in an aggregation is true.
func (e Expr) Any() Expr { return e.aggregate("any") }

// All reports whether all values in an aggregation are true.
func (e Expr) All() Expr { return e.aggregate("all") }

// ArgMax returns the index of the maximum value in an aggregation.
func (e Expr) ArgMax() Expr { return e.aggregate("arg_max") }

// ArgMin returns the index of the minimum value in an aggregation.
func (e Expr) ArgMin() Expr { return e.aggregate("arg_min") }

// Implode collects an aggregation into a list.
func (e Expr) Implode() Expr { return e.aggregate("implode") }

// Shift shifts values by periods, inserting nulls.
func (e Expr) Shift(periods Expr) Expr { return e.binary("shift", periods) }

// ShiftAndFill shifts values and fills the introduced nulls.
func (e Expr) ShiftAndFill(periods, fill Expr) Expr { return e.call("shift_and_fill", periods, fill) }

// Slice selects a contiguous range from each expression result.
func (e Expr) Slice(offset, length Expr) Expr { return e.call("slice", offset, length) }

// Reverse reverses each expression result.
func (e Expr) Reverse() Expr { return e.unary("reverse") }

// Unique keeps distinct values in an expression result.
func (e Expr) Unique() Expr { return e.unary("unique") }

// Explode expands list or string values into rows.
func (e Expr) Explode() Expr { return e.unary("explode") }

// StrExpr provides string kernels while keeping the main Expr namespace small.
type StrExpr struct{ expr Expr }

// Str returns string operations for an expression.
func (e Expr) Str() StrExpr { return StrExpr{expr: e} }

// Contains tests whether a string contains pattern.
func (s StrExpr) Contains(pattern Expr) Expr {
	return s.expr.binary("str_contains", pattern)
}

// StartsWith tests whether a string starts with pattern.
func (s StrExpr) StartsWith(pattern Expr) Expr {
	return s.expr.binary("str_starts_with", pattern)
}

// EndsWith tests whether a string ends with pattern.
func (s StrExpr) EndsWith(pattern Expr) Expr { return s.expr.binary("str_ends_with", pattern) }

// ToLowerCase converts strings to lower case.
func (s StrExpr) ToLowerCase() Expr { return s.expr.unary("str_lower") }

// ToUpperCase converts strings to upper case.
func (s StrExpr) ToUpperCase() Expr { return s.expr.unary("str_upper") }

// Lengths returns string lengths.
func (s StrExpr) Lengths() Expr { return s.expr.unary("str_length") }

// StructExpr provides access to fields in struct-valued expressions.
type StructExpr struct{ expr Expr }

// Struct returns struct field operations for an expression.
func (e Expr) Struct() StructExpr { return StructExpr{expr: e} }

// Element selects a named field from a struct-valued expression.
func (s StructExpr) Element(name string) Expr {
	return Expr{node: &exprNode{kind: exprCall, name: "struct_field", field: name, args: []*exprNode{s.expr.node}}}
}

// Unnest expands struct fields into separate columns.
func (s StructExpr) Unnest() Expr { return s.expr.unary("struct_unnest") }

// JSONEncode serializes struct values as JSON strings.
func (s StructExpr) JSONEncode() Expr { return s.expr.unary("struct_json_encode") }

// DtExpr provides calendar extraction from Arrow date/timestamp columns.
type DtExpr struct{ expr Expr }

// Dt returns date and timestamp operations for an expression.
func (e Expr) Dt() DtExpr { return DtExpr{expr: e} }

// Year extracts the year component.
func (d DtExpr) Year() Expr { return d.expr.unary("dt_year") }

// Month extracts the month component.
func (d DtExpr) Month() Expr { return d.expr.unary("dt_month") }

// Day extracts the day component.
func (d DtExpr) Day() Expr { return d.expr.unary("dt_day") }

// Count creates a row-count aggregation expression.
func Count() Expr { return Expr{&exprNode{kind: exprAggregate, name: "count_rows"}} }

// WhenThen builds a conditional expression.
type WhenThen struct {
	predicate Expr
	value     Expr
}

// When starts a conditional expression.
func When(predicate Expr) WhenThen { return WhenThen{predicate: predicate} }

// Then supplies the value for a true predicate.
func (w WhenThen) Then(value Expr) WhenThen {
	w.value = value
	return w
}

// Otherwise supplies the value for a false predicate.
func (w WhenThen) Otherwise(value Expr) Expr {
	return Expr{node: &exprNode{kind: exprCall, name: "when_then", args: []*exprNode{w.predicate.node, w.value.node, value.node}}}
}

// IsIn tests membership in values.
func (e Expr) IsIn(values Expr) Expr { return e.binary("is_in", values) }

// IsBetween tests whether e is between lower and upper.
func (e Expr) IsBetween(lower, upper Expr) Expr {
	return Expr{node: &exprNode{kind: exprCall, name: "is_between", args: []*exprNode{e.node, lower.node, upper.node}}}
}

// IsNaN tests for NaN values.
func (e Expr) IsNaN() Expr { return e.unary("is_nan") }

// IsNotNaN tests for values that are not NaN.
func (e Expr) IsNotNaN() Expr { return e.unary("is_not_nan") }

// IsFinite tests for finite numeric values.
func (e Expr) IsFinite() Expr { return e.unary("is_finite") }

// IsInfinite tests for infinite numeric values.
func (e Expr) IsInfinite() Expr { return e.unary("is_infinite") }

// IsValid reports whether the expression contains a node.
func (e Expr) IsValid() bool { return e.node != nil }

func (e Expr) outputName() string {
	if e.node == nil {
		return ""
	}
	return nodeOutputName(e.node)
}

func nodeOutputName(n *exprNode) string {
	switch n.kind {
	case exprColumn, exprAlias:
		return n.name
	case exprAggregate:
		if n.name == "count_rows" {
			return "count"
		}
		return fmt.Sprintf("%s_%s", nodeOutputName(n.args[0]), n.name)
	case exprCast:
		return nodeOutputName(n.args[0])
	case exprCall:
		if len(n.args) > 0 {
			return nodeOutputName(n.args[0])
		}
	}
	return "literal"
}

// String formats the expression for display and plan explanations.
func (e Expr) String() string {
	if e.node == nil {
		return "<invalid>"
	}
	return formatExpr(e.node)
}

func formatExpr(n *exprNode) string {
	switch n.kind {
	case exprColumn:
		return fmt.Sprintf("col(%q)", n.name)
	case exprLiteral:
		return fmt.Sprintf("lit(%v)", n.value)
	case exprAlias:
		return fmt.Sprintf("%s.alias(%q)", formatExpr(n.args[0]), n.name)
	case exprCast:
		return fmt.Sprintf("%s.cast(%s)", formatExpr(n.args[0]), n.dtype)
	case exprAggregate:
		if len(n.args) == 0 {
			return n.name + "()"
		}
		return fmt.Sprintf("%s(%s)", n.name, formatExpr(n.args[0]))
	case exprCall:
		if n.name == "struct_field" && len(n.args) == 1 {
			return fmt.Sprintf("%s.struct().element(%q)", formatExpr(n.args[0]), n.field)
		}
		args := make([]string, len(n.args))
		for i, arg := range n.args {
			args[i] = formatExpr(arg)
		}
		return fmt.Sprintf("%s(%s)", n.name, strings.Join(args, ", "))
	default:
		return "<?>"
	}
}
