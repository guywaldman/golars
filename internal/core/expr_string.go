package core

// Slice extracts a substring using byte offsets represented by expressions.
func (s StrExpr) Slice(offset, length Expr) Expr {
	return s.expr.call("str_slice", offset, length)
}

// LenBytes returns the byte length of each string.
func (s StrExpr) LenBytes() Expr { return s.expr.unary("str_length_bytes") }

// Replace replaces the first matching substring.
func (s StrExpr) Replace(pattern, value Expr) Expr {
	return s.expr.call("str_replace", pattern, value)
}

// ReplaceAll replaces all matching substrings.
func (s StrExpr) ReplaceAll(pattern, value Expr) Expr {
	return s.expr.call("str_replace_all", pattern, value)
}

// Split splits each string by a delimiter.
func (s StrExpr) Split(delimiter Expr) Expr {
	return s.expr.call("str_split", delimiter)
}

// StripChars removes leading and trailing characters. An empty expression
// uses the default Unicode whitespace set.
func (s StrExpr) StripChars(characters Expr) Expr {
	return s.expr.call("str_strip_chars", characters)
}
