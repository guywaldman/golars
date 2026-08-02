package parquetio

import (
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// Source is the batch-oriented reader contract returned by Parquet sources.
type Source interface {
	Schema() *arrow.Schema
	Next() bool
	RecordBatch() arrow.RecordBatch
	Err() error
	Release()
}

// FilterGuaranteeSource reports whether the current batch is known to satisfy
// all predicates from row-group statistics.
type FilterGuaranteeSource interface {
	FiltersFullyMatch() bool
}

// Config contains execution settings needed by the Parquet reader.
type Config struct {
	Allocator     memory.Allocator
	BatchSize     int64
	Parallel      bool
	MissingColumn MissingColumnError
}

// Request describes projected columns and row-group predicates.
type Request struct {
	Columns               []string
	ProjectionColumns     []string
	FilterOnlyColumns     []string
	ProjectionOnlyBatches bool
	Filters               []Predicate
	SelectionApplicable   bool
}

// Predicate is a row-group-prunable comparison.
type Predicate struct {
	Column string
	Op     string
	Value  any
}

// MissingColumnError lets the public package preserve its exported error type.
type MissingColumnError func(name string, available []string) error
