package core

// optimizePlan applies rewrites that preserve row order and null semantics.
// Scan-specific pushdown is derived from the resulting plan in
// execution_scan.go; keeping these rewrites separate makes the optimized
// logical plan inspectable without coupling it to a storage format.
func optimizePlan(root *planNode) *planNode {
	if root == nil {
		return nil
	}
	input := optimizePlan(root.input)
	node := clonePlanShallow(root)
	node.input = input
	node.exprs = simplifyExprList(root.exprs)
	node.keys = simplifyExprList(root.keys)

	if node.kind == planJoin && root.right != nil {
		node.right = optimizePlan(root.right)
	}

	// Adjacent filters are equivalent to a single conjunction. This gives
	// storage pushdown one predicate tree and reduces per-batch expression work.
	if node.kind == planFilter && input != nil && input.kind == planFilter && len(node.exprs) == 1 && len(input.exprs) == 1 {
		return optimizePlan(&planNode{
			kind:  planFilter,
			input: input.input,
			exprs: []Expr{{node: &exprNode{
				kind: exprCall,
				name: "and_kleene",
				args: []*exprNode{cloneExprNode(input.exprs[0].node), cloneExprNode(node.exprs[0].node)},
			}}},
		})
	}

	// A second limit can never increase the result. Keeping the smaller limit
	// also lets callers avoid opening more source batches than necessary.
	if node.kind == planLimit && input != nil && input.kind == planLimit {
		return optimizePlan(&planNode{kind: planLimit, input: input.input, limit: minInt64(node.limit, input.limit)})
	}

	// Projection and with-columns are row-independent. Move a limit below them
	// so the source stops early while preserving the projected schema.
	if node.kind == planLimit && input != nil && (input.kind == planSelect || input.kind == planWithColumns) {
		lowered := &planNode{kind: planLimit, input: input.input, limit: node.limit}
		return optimizePlan(&planNode{kind: input.kind, input: lowered, exprs: cloneExprList(input.exprs), keys: cloneExprList(input.keys)})
	}

	// Push a predicate through a projection only when every projected name has
	// a deterministic expression mapping. This covers the common col("x").Alias
	// ("y") case while avoiding changes to expressions that add or remove rows.
	if node.kind == planFilter && input != nil && input.kind == planSelect && len(node.exprs) == 1 {
		if pushed, ok := rewriteThroughProjection(node.exprs[0].node, input.exprs); ok {
			lowered := &planNode{kind: planFilter, input: input.input, exprs: []Expr{{node: pushed}}}
			return optimizePlan(&planNode{kind: planSelect, input: lowered, exprs: cloneExprList(input.exprs)})
		}
	}

	// Collapse simple adjacent projections. A projection containing all()
	// remains a barrier because its output depends on the input schema.
	if node.kind == planSelect && input != nil && input.kind == planSelect {
		if exprs, ok := collapseProjections(input.exprs, node.exprs); ok {
			return optimizePlan(&planNode{kind: planSelect, input: input.input, exprs: exprs})
		}
	}

	return node
}

func clonePlanShallow(node *planNode) *planNode {
	copy := *node
	copy.exprs = nil
	copy.keys = nil
	copy.input = nil
	copy.right = nil
	return &copy
}

func cloneExprList(exprs []Expr) []Expr {
	if len(exprs) == 0 {
		return nil
	}
	result := make([]Expr, len(exprs))
	for i, expr := range exprs {
		result[i] = Expr{node: cloneExprNode(expr.node)}
	}
	return result
}

func simplifyExprList(exprs []Expr) []Expr {
	if len(exprs) == 0 {
		return nil
	}
	result := make([]Expr, len(exprs))
	for i, expr := range exprs {
		result[i] = Expr{node: simplifyExpr(expr.node)}
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

func simplifyExpr(node *exprNode) *exprNode {
	if node == nil {
		return nil
	}
	copy := *node
	copy.args = make([]*exprNode, len(node.args))
	for i, arg := range node.args {
		copy.args[i] = simplifyExpr(arg)
	}
	copy.windowKeys = make([]*exprNode, len(node.windowKeys))
	for i, key := range node.windowKeys {
		copy.windowKeys[i] = simplifyExpr(key)
	}
	if len(copy.windowKeys) != 0 {
		return &copy
	}
	if folded, ok := foldLiteralExpr(&copy); ok {
		return folded
	}
	return &copy
}

func foldLiteralExpr(node *exprNode) (*exprNode, bool) {
	if node.kind != exprCall || len(node.args) == 0 {
		return nil, false
	}
	for _, arg := range node.args {
		if arg == nil || arg.kind != exprLiteral {
			return nil, false
		}
	}
	if node.name == "and_kleene" || node.name == "or_kleene" {
		left, lok := node.args[0].value.(bool)
		right, rok := node.args[1].value.(bool)
		if !lok || !rok {
			return nil, false
		}
		value := left && right
		if node.name == "or_kleene" {
			value = left || right
		}
		return &exprNode{kind: exprLiteral, value: value}, true
	}
	if node.name == "invert" {
		value, ok := node.args[0].value.(bool)
		if ok {
			return &exprNode{kind: exprLiteral, value: !value}, true
		}
	}
	if len(node.args) != 2 {
		return nil, false
	}
	left, lok := literalNumber(node.args[0].value)
	right, rok := literalNumber(node.args[1].value)
	if !lok || !rok {
		return nil, false
	}
	switch node.name {
	case "add":
		return &exprNode{kind: exprLiteral, value: left + right}, true
	case "subtract":
		return &exprNode{kind: exprLiteral, value: left - right}, true
	case "multiply":
		return &exprNode{kind: exprLiteral, value: left * right}, true
	case "divide":
		if right == 0 {
			return nil, false
		}
		return &exprNode{kind: exprLiteral, value: left / right}, true
	case "equal":
		return &exprNode{kind: exprLiteral, value: left == right}, true
	case "not_equal":
		return &exprNode{kind: exprLiteral, value: left != right}, true
	case "greater":
		return &exprNode{kind: exprLiteral, value: left > right}, true
	case "greater_equal":
		return &exprNode{kind: exprLiteral, value: left >= right}, true
	case "less":
		return &exprNode{kind: exprLiteral, value: left < right}, true
	case "less_equal":
		return &exprNode{kind: exprLiteral, value: left <= right}, true
	default:
		return nil, false
	}
}

func literalNumber(value any) (float64, bool) {
	switch value := value.(type) {
	case int:
		return float64(value), true
	case int8:
		return float64(value), true
	case int16:
		return float64(value), true
	case int32:
		return float64(value), true
	case int64:
		return float64(value), true
	case uint:
		return float64(value), true
	case uint8:
		return float64(value), true
	case uint16:
		return float64(value), true
	case uint32:
		return float64(value), true
	case uint64:
		return float64(value), true
	case float32:
		return float64(value), true
	case float64:
		return value, true
	default:
		return 0, false
	}
}

func rewriteThroughProjection(predicate *exprNode, projection []Expr) (*exprNode, bool) {
	mapping := make(map[string]*exprNode, len(projection))
	for _, expr := range projection {
		if expr.node == nil || expr.node.kind == exprCall && expr.node.name == "all_columns" || containsAggregate(expr.node) || containsWindowExpr(expr.node) {
			return nil, false
		}
		mapping[expr.outputName()] = unwrapAlias(expr.node)
	}
	return rewriteColumns(predicate, mapping)
}

func rewriteColumns(node *exprNode, mapping map[string]*exprNode) (*exprNode, bool) {
	if node == nil {
		return nil, false
	}
	if node.kind == exprColumn {
		replacement, ok := mapping[node.name]
		if !ok {
			return nil, false
		}
		return cloneExprNode(replacement), true
	}
	copy := *node
	copy.args = make([]*exprNode, len(node.args))
	for i, arg := range node.args {
		rewritten, ok := rewriteColumns(arg, mapping)
		if !ok {
			return nil, false
		}
		copy.args[i] = rewritten
	}
	return &copy, true
}

func collapseProjections(first, second []Expr) ([]Expr, bool) {
	mapping := make(map[string]*exprNode, len(first))
	for _, expr := range first {
		if expr.node == nil || containsAllColumns(expr.node) || containsAggregate(expr.node) || containsWindowExpr(expr.node) {
			return nil, false
		}
		mapping[expr.outputName()] = unwrapAlias(expr.node)
	}
	result := make([]Expr, len(second))
	for i, expr := range second {
		rewritten, ok := rewriteColumns(expr.node, mapping)
		if !ok {
			return nil, false
		}
		result[i] = Expr{node: rewritten}
	}
	return result, true
}

func containsAllColumns(node *exprNode) bool {
	if node == nil {
		return false
	}
	if node.kind == exprCall && node.name == "all_columns" {
		return true
	}
	for _, arg := range node.args {
		if containsAllColumns(arg) {
			return true
		}
	}
	return false
}

func containsAggregate(node *exprNode) bool {
	if node == nil {
		return false
	}
	if node.kind == exprAggregate {
		return true
	}
	for _, arg := range node.args {
		if containsAggregate(arg) {
			return true
		}
	}
	return false
}

func minInt64(left, right int64) int64 {
	if left < right {
		return left
	}
	return right
}
