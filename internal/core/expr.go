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
	value      any
	args       []*exprNode
	windowKeys []*exprNode
	dtype      DataType
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
		args := make([]string, len(n.args))
		for i, arg := range n.args {
			args[i] = formatExpr(arg)
		}
		return fmt.Sprintf("%s(%s)", n.name, strings.Join(args, ", "))
	default:
		return "<?>"
	}
}
