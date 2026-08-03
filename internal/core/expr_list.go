package core

// ListExpr provides operations on list-valued expressions.
type ListExpr struct{ expr Expr }

// List returns list operations for an expression.
func (e Expr) List() ListExpr { return ListExpr{expr: e} }

// Len returns the length of each list.
func (l ListExpr) Len() Expr { return l.expr.unary("list_len") }

// Max returns the maximum value in each list.
func (l ListExpr) Max() Expr { return l.expr.unary("list_max") }

// Min returns the minimum value in each list.
func (l ListExpr) Min() Expr { return l.expr.unary("list_min") }

// Sum returns the sum of each list.
func (l ListExpr) Sum() Expr { return l.expr.unary("list_sum") }

// Mean returns the mean of each list.
func (l ListExpr) Mean() Expr { return l.expr.unary("list_mean") }

// Reverse reverses each list.
func (l ListExpr) Reverse() Expr { return l.expr.unary("list_reverse") }

// Unique keeps distinct values in each list.
func (l ListExpr) Unique() Expr { return l.expr.unary("list_unique") }

// NUnique counts distinct values in each list.
func (l ListExpr) NUnique() Expr { return l.expr.unary("list_n_unique") }

// First returns the first value in each list.
func (l ListExpr) First() Expr { return l.Get(Lit(int64(0))) }

// Last returns the last value in each list.
func (l ListExpr) Last() Expr { return l.Get(Lit(int64(-1))) }

// Get selects an item from each list. Out-of-bounds values become null.
func (l ListExpr) Get(index Expr) Expr { return l.expr.call("list_get", index) }

// Contains tests whether each list contains value.
func (l ListExpr) Contains(value Expr) Expr { return l.expr.call("list_contains", value) }

// Slice selects a range from each list.
func (l ListExpr) Slice(offset, length Expr) Expr {
	return l.expr.call("list_slice", offset, length)
}

// Head returns the first n values from each list.
func (l ListExpr) Head(n Expr) Expr { return l.Slice(Lit(int64(0)), n) }

// Tail returns the last n values from each list.
func (l ListExpr) Tail(n Expr) Expr {
	return l.Slice(Lit(int64(0)).Sub(n), n)
}

// Explode expands each list item into a row.
func (l ListExpr) Explode() Expr { return l.expr.unary("list_explode") }
