package core

import (
	"errors"
	"fmt"
)

var (
	// ErrClosed reports that a resource is closed.
	ErrClosed = errors.New("golars: resource is closed")
	// ErrColumnNotFound reports that a column is missing.
	ErrColumnNotFound = errors.New("golars: column not found")
	// ErrTypeMismatch reports an incompatible type operation.
	ErrTypeMismatch = errors.New("golars: type mismatch")
	// ErrInvalidPlan reports an invalid lazy or eager operation.
	ErrInvalidPlan = errors.New("golars: invalid lazy plan")
)

// MemoryLimitError reports an allocation rejected by an Engine memory limit.
type MemoryLimitError struct {
	// Limit is the configured memory quota in bytes.
	Limit int64
	// Allocated is the accounted usage before the rejected allocation.
	Allocated int64
	// Requested is the rejected allocation size in bytes.
	Requested int64
}

// Error returns a description of the rejected allocation.
func (e *MemoryLimitError) Error() string {
	return fmt.Sprintf("golars: memory limit exceeded: limit=%d allocated=%d requested=%d", e.Limit, e.Allocated, e.Requested)
}

// ColumnNotFoundError identifies the missing column and the available schema.
type ColumnNotFoundError struct {
	// Name is the missing column name.
	Name string
	// Available lists the columns present in the frame.
	Available []string
}

// Error returns a description of the missing column.
func (e *ColumnNotFoundError) Error() string {
	return fmt.Sprintf("%v: %q (available: %v)", ErrColumnNotFound, e.Name, e.Available)
}

// Unwrap returns ErrColumnNotFound for errors.Is checks.
func (e *ColumnNotFoundError) Unwrap() error { return ErrColumnNotFound }
