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

// AsofStrategy selects the nearest-key direction for an as-of join.
type AsofStrategy uint8

// AsOfStrategy is the conventional initialism spelling of AsofStrategy.
type AsOfStrategy = AsofStrategy

const (
	// AsofBackward matches the last right key less than or equal to the left key.
	AsofBackward AsofStrategy = iota
	// AsofForward matches the first right key greater than or equal to the left key.
	AsofForward
	// AsofNearest matches the closest right key.
	AsofNearest
)

const (
	// AsOfBackward is an alias for AsofBackward.
	AsOfBackward = AsofBackward
	// AsOfForward is an alias for AsofForward.
	AsOfForward = AsofForward
	// AsOfNearest is an alias for AsofNearest.
	AsOfNearest = AsofNearest
)

func (strategy AsofStrategy) String() string {
	switch strategy {
	case AsofBackward:
		return "backward"
	case AsofForward:
		return "forward"
	case AsofNearest:
		return "nearest"
	default:
		return "unknown"
	}
}

// AsOfJoinOption configures an as-of join.
type AsOfJoinOption func(*asofJoinConfig)

type asofJoinConfig struct {
	strategy        AsofStrategy
	allowExact      bool
	checkSortedness bool
}

// WithAsofStrategy selects backward, forward, or nearest matching.
func WithAsofStrategy(strategy AsofStrategy) AsOfJoinOption {
	return func(config *asofJoinConfig) { config.strategy = strategy }
}

// WithAsOfStrategy is an alias for WithAsofStrategy.
func WithAsOfStrategy(strategy AsOfStrategy) AsOfJoinOption {
	return WithAsofStrategy(strategy)
}

// WithAsofAllowExact controls whether equal keys may match.
func WithAsofAllowExact(enabled bool) AsOfJoinOption {
	return func(config *asofJoinConfig) { config.allowExact = enabled }
}

// WithAsOfAllowExact is an alias for WithAsofAllowExact.
func WithAsOfAllowExact(enabled bool) AsOfJoinOption {
	return WithAsofAllowExact(enabled)
}

// WithAsofCheckSortedness asks the native engine to validate key ordering.
func WithAsofCheckSortedness(enabled bool) AsOfJoinOption {
	return func(config *asofJoinConfig) { config.checkSortedness = enabled }
}

// WithAsOfCheckSortedness is an alias for WithAsofCheckSortedness.
func WithAsOfCheckSortedness(enabled bool) AsOfJoinOption {
	return WithAsofCheckSortedness(enabled)
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

// JoinAsOf adds a deferred nearest-key join. Both inputs must be sorted by
// their single join expression unless sortedness checks are disabled.
func (lf *LazyFrame) JoinAsOf(other *LazyFrame, leftOn, rightOn []Expr, options ...AsOfJoinOption) *LazyFrame {
	if lf == nil || other == nil {
		return &LazyFrame{err: ErrClosed}
	}
	if lf.err != nil {
		return lf
	}
	if other.err != nil {
		return newLazyFrame(lf.engine, lf.plan, other.err)
	}
	if len(leftOn) != 1 || len(rightOn) != 1 {
		return newLazyFrame(lf.engine, lf.plan, fmt.Errorf("%w: as-of join requires one key expression on each side", ErrInvalidPlan))
	}
	config := asofJoinConfig{strategy: AsofBackward, allowExact: true, checkSortedness: true}
	for _, option := range options {
		if option != nil {
			option(&config)
		}
	}
	if config.strategy > AsofNearest {
		return newLazyFrame(lf.engine, lf.plan, fmt.Errorf("%w: unknown as-of strategy %d", ErrInvalidPlan, config.strategy))
	}
	return newLazyFrame(lf.engine, &planNode{
		kind:            planJoin,
		input:           lf.plan,
		right:           other.plan,
		leftKeys:        cloneExprList(leftOn),
		rightKeys:       cloneExprList(rightOn),
		joinType:        JoinInner,
		suffix:          "_right",
		asof:            true,
		asofStrategy:    config.strategy,
		asofAllowExact:  config.allowExact,
		asofCheckSorted: config.checkSortedness,
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

// JoinAsOf evaluates a nearest-key join between two eager frames.
func (df *DataFrame) JoinAsOf(other *DataFrame, leftOn, rightOn []Expr, options ...AsOfJoinOption) (*DataFrame, error) {
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
	return left.JoinAsOf(right, leftOn, rightOn, options...).Collect(context.Background())
}
