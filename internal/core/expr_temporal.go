package core

// Hour extracts the hour component.
func (d DtExpr) Hour() Expr { return d.expr.unary("dt_hour") }

// Minute extracts the minute component.
func (d DtExpr) Minute() Expr { return d.expr.unary("dt_minute") }

// Second extracts the second component.
func (d DtExpr) Second() Expr { return d.expr.unary("dt_second") }

// Week extracts the ISO week number.
func (d DtExpr) Week() Expr { return d.expr.unary("dt_week") }

// Weekday extracts the ISO weekday number.
func (d DtExpr) Weekday() Expr { return d.expr.unary("dt_weekday") }

// Quarter extracts the quarter number.
func (d DtExpr) Quarter() Expr { return d.expr.unary("dt_quarter") }

// Date extracts the date component.
func (d DtExpr) Date() Expr { return d.expr.unary("dt_date") }

// Time extracts the time component.
func (d DtExpr) Time() Expr { return d.expr.unary("dt_time") }

// Strftime formats temporal values with a Polars strftime pattern.
func (d DtExpr) Strftime(format string) Expr {
	return d.expr.call("dt_to_string", Lit(format))
}
