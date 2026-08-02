package core

import (
	"context"
	"fmt"
)

// JoinType selects the relational join semantics.
type JoinType uint8

const (
	// JoinInner keeps rows with matching keys from both inputs.
	JoinInner JoinType = iota
	// JoinLeft keeps every row from the left input.
	JoinLeft
	// JoinFull keeps every row from both inputs.
	JoinFull
	// JoinRight keeps every row from the right input.
	JoinRight
	// JoinSemi keeps left rows with a matching right key.
	JoinSemi
	// JoinAnti keeps left rows without a matching right key.
	JoinAnti
	// JoinCross computes the Cartesian product.
	JoinCross
)

// String returns the join type name.
func (t JoinType) String() string {
	switch t {
	case JoinInner:
		return "inner"
	case JoinLeft:
		return "left"
	case JoinFull:
		return "full"
	case JoinRight:
		return "right"
	case JoinSemi:
		return "semi"
	case JoinAnti:
		return "anti"
	case JoinCross:
		return "cross"
	default:
		return "unknown"
	}
}

// JoinOption configures a join.
type JoinOption func(*joinConfig)

type joinConfig struct {
	how    JoinType
	suffix string
}

// WithJoinType sets the join type.
func WithJoinType(how JoinType) JoinOption { return func(c *joinConfig) { c.how = how } }

// WithJoinSuffix sets the suffix for overlapping right-side columns.
func WithJoinSuffix(suffix string) JoinOption {
	return func(c *joinConfig) {
		if suffix != "" {
			c.suffix = suffix
		}
	}
}

// Join adds a deferred relational join. Keys are evaluated as expressions on
// both inputs, so computed keys and temporal/numeric coercion use the same
// expression engine as Select and Filter.
func (lf *LazyFrame) Join(other *LazyFrame, leftOn, rightOn []Expr, options ...JoinOption) *LazyFrame {
	if lf == nil || other == nil {
		return &LazyFrame{err: ErrClosed}
	}
	if lf.err != nil {
		return lf
	}
	if other.err != nil {
		return newLazyFrame(lf.engine, lf.plan, other.err)
	}
	config := joinConfig{how: JoinInner, suffix: "_right"}
	for _, option := range options {
		if option != nil {
			option(&config)
		}
	}
	if config.how > JoinCross {
		return newLazyFrame(lf.engine, lf.plan, fmt.Errorf("%w: unknown join type %d", ErrInvalidPlan, config.how))
	}
	if config.how == JoinCross {
		if len(leftOn) != 0 || len(rightOn) != 0 {
			return newLazyFrame(lf.engine, lf.plan, fmt.Errorf("%w: cross join does not accept keys", ErrInvalidPlan))
		}
	} else if len(leftOn) == 0 || len(leftOn) != len(rightOn) {
		return newLazyFrame(lf.engine, lf.plan, fmt.Errorf("%w: join requires matching non-empty key expressions", ErrInvalidPlan))
	}
	return newLazyFrame(lf.engine, &planNode{
		kind:      planJoin,
		input:     lf.plan,
		right:     other.plan,
		leftKeys:  cloneExprList(leftOn),
		rightKeys: cloneExprList(rightOn),
		joinType:  config.how,
		suffix:    config.suffix,
	}, nil)
}

// Join evaluates a join between two eager frames.
func (df *DataFrame) Join(other *DataFrame, leftOn, rightOn []Expr, options ...JoinOption) (*DataFrame, error) {
	if err := df.ensureOpen(); err != nil {
		return nil, err
	}
	if err := other.ensureOpen(); err != nil {
		return nil, err
	}
	left := df.Lazy()
	right := other.Lazy()
	defer left.Close()
	defer right.Close()
	return left.Join(right, leftOn, rightOn, options...).Collect(context.Background())
}
