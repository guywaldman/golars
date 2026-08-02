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
	dtype      DataType
	distinct   bool
	descending bool
}

// Expr is an immutable expression tree used by both eager and lazy operations.
type Expr struct{ node *exprNode }

func Col(name string) Expr { return Expr{&exprNode{kind: exprColumn, name: name}} }
func Lit(value any) Expr   { return Expr{&exprNode{kind: exprLiteral, value: value}} }
func All() Expr            { return Expr{&exprNode{kind: exprCall, name: "all_columns"}} }

func (e Expr) binary(name string, rhs Expr) Expr {
	return Expr{&exprNode{kind: exprCall, name: name, args: []*exprNode{e.node, rhs.node}}}
}
func (e Expr) unary(name string) Expr {
	return Expr{&exprNode{kind: exprCall, name: name, args: []*exprNode{e.node}}}
}

func (e Expr) Eq(rhs Expr) Expr         { return e.binary("equal", rhs) }
func (e Expr) Ne(rhs Expr) Expr         { return e.binary("not_equal", rhs) }
func (e Expr) Gt(rhs Expr) Expr         { return e.binary("greater", rhs) }
func (e Expr) Ge(rhs Expr) Expr         { return e.binary("greater_equal", rhs) }
func (e Expr) Lt(rhs Expr) Expr         { return e.binary("less", rhs) }
func (e Expr) Le(rhs Expr) Expr         { return e.binary("less_equal", rhs) }
func (e Expr) Add(rhs Expr) Expr        { return e.binary("add", rhs) }
func (e Expr) Sub(rhs Expr) Expr        { return e.binary("subtract", rhs) }
func (e Expr) Mul(rhs Expr) Expr        { return e.binary("multiply", rhs) }
func (e Expr) Div(rhs Expr) Expr        { return e.binary("divide", rhs) }
func (e Expr) And(rhs Expr) Expr        { return e.binary("and_kleene", rhs) }
func (e Expr) Or(rhs Expr) Expr         { return e.binary("or_kleene", rhs) }
func (e Expr) Not() Expr                { return e.unary("invert") }
func (e Expr) IsNull() Expr             { return e.unary("is_null") }
func (e Expr) IsNotNull() Expr          { return e.unary("is_valid") }
func (e Expr) FillNull(value Expr) Expr { return e.binary("coalesce", value) }

func (e Expr) Alias(name string) Expr {
	return Expr{&exprNode{kind: exprAlias, name: name, args: []*exprNode{e.node}}}
}
func (e Expr) Desc() Expr {
	if e.node == nil {
		return e
	}
	copy := *e.node
	copy.descending = true
	return Expr{node: &copy}
}
func (e Expr) Cast(dtype DataType) Expr {
	return Expr{&exprNode{kind: exprCast, dtype: dtype, args: []*exprNode{e.node}}}
}

func (e Expr) aggregate(name string) Expr {
	return Expr{&exprNode{kind: exprAggregate, name: name, args: []*exprNode{e.node}}}
}
func (e Expr) Sum() Expr   { return e.aggregate("sum") }
func (e Expr) Min() Expr   { return e.aggregate("min") }
func (e Expr) Max() Expr   { return e.aggregate("max") }
func (e Expr) Mean() Expr  { return e.aggregate("mean") }
func (e Expr) Count() Expr { return e.aggregate("count") }
func (e Expr) First() Expr { return e.aggregate("first") }
func (e Expr) Last() Expr  { return e.aggregate("last") }

// StrExpr provides string kernels while keeping the main Expr namespace small.
type StrExpr struct{ expr Expr }

func (e Expr) Str() StrExpr { return StrExpr{expr: e} }
func (s StrExpr) Contains(pattern Expr) Expr {
	return s.expr.binary("str_contains", pattern)
}
func (s StrExpr) StartsWith(pattern Expr) Expr {
	return s.expr.binary("str_starts_with", pattern)
}
func (s StrExpr) EndsWith(pattern Expr) Expr { return s.expr.binary("str_ends_with", pattern) }
func (s StrExpr) ToLowerCase() Expr          { return s.expr.unary("str_lower") }
func (s StrExpr) ToUpperCase() Expr          { return s.expr.unary("str_upper") }
func (s StrExpr) Lengths() Expr              { return s.expr.unary("str_length") }

// DtExpr provides calendar extraction from Arrow date/timestamp columns.
type DtExpr struct{ expr Expr }

func (e Expr) Dt() DtExpr    { return DtExpr{expr: e} }
func (d DtExpr) Year() Expr  { return d.expr.unary("dt_year") }
func (d DtExpr) Month() Expr { return d.expr.unary("dt_month") }
func (d DtExpr) Day() Expr   { return d.expr.unary("dt_day") }

func Count() Expr { return Expr{&exprNode{kind: exprAggregate, name: "count_rows"}} }

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
