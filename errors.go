package golars

import (
	"errors"
	"fmt"
)

var (
	ErrClosed         = errors.New("golars: resource is closed")
	ErrColumnNotFound = errors.New("golars: column not found")
	ErrTypeMismatch   = errors.New("golars: type mismatch")
	ErrInvalidPlan    = errors.New("golars: invalid lazy plan")
)

// MemoryLimitError reports an allocation rejected by an Engine memory limit.
type MemoryLimitError struct {
	Limit     int64
	Allocated int64
	Requested int64
}

func (e *MemoryLimitError) Error() string {
	return fmt.Sprintf("golars: memory limit exceeded: limit=%d allocated=%d requested=%d", e.Limit, e.Allocated, e.Requested)
}

// ColumnNotFoundError identifies the missing column and the available schema.
type ColumnNotFoundError struct {
	Name      string
	Available []string
}

func (e *ColumnNotFoundError) Error() string {
	return fmt.Sprintf("%v: %q (available: %v)", ErrColumnNotFound, e.Name, e.Available)
}

func (e *ColumnNotFoundError) Unwrap() error { return ErrColumnNotFound }
