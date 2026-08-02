package hive

import (
	"fmt"
	"path/filepath"
	"sort"
	"strings"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/guywaldman/golars/internal/arrowutil"
)

// Source is the batch-oriented reader contract decorated by partition fields.
type Source interface {
	Schema() *arrow.Schema
	Next() bool
	RecordBatch() arrow.RecordBatch
	Err() error
	Release()
}

// PartitionKeys returns the sorted union of Hive partition keys.
func PartitionKeys(paths []string) []string {
	seen := make(map[string]struct{})
	for _, path := range paths {
		for key := range PartitionValues(path, nil) {
			seen[key] = struct{}{}
		}
	}
	keys := make([]string, 0, len(seen))
	for key := range seen {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

// PartitionValues extracts key=value path segments, optionally restricted to keys.
func PartitionValues(path string, keys []string) map[string]string {
	values := make(map[string]string)
	parts := strings.FieldsFunc(filepath.ToSlash(path), func(r rune) bool { return r == '/' })
	for index, part := range parts {
		if index == len(parts)-1 {
			continue
		}
		key, value, found := strings.Cut(part, "=")
		if !found || key == "" || value == "" {
			continue
		}
		values[key] = value
	}
	if keys == nil {
		return values
	}
	filtered := make(map[string]string, len(keys))
	for _, key := range keys {
		filtered[key] = values[key]
	}
	return filtered
}

type partitionSource struct {
	base      Source
	values    map[string]string
	allocator memory.Allocator
	keys      []string
	current   arrow.RecordBatch
	err       error
}

// NewPartitionSource decorates batches with constant partition columns.
func NewPartitionSource(base Source, values map[string]string, allocator memory.Allocator) Source {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return &partitionSource{base: base, values: values, allocator: allocator, keys: keys}
}

func (s *partitionSource) Schema() *arrow.Schema {
	baseSchema := s.base.Schema()
	if baseSchema == nil {
		return nil
	}
	fields := append([]arrow.Field(nil), baseSchema.Fields()...)
	for _, key := range s.keys {
		if len(baseSchema.FieldIndices(key)) == 0 {
			fields = append(fields, arrow.Field{Name: key, Type: arrow.BinaryTypes.String, Nullable: false})
		}
	}
	return arrow.NewSchema(fields, nil)
}

func (s *partitionSource) Next() bool {
	if s.current != nil {
		s.current.Release()
		s.current = nil
	}
	if !s.base.Next() {
		s.err = s.base.Err()
		return false
	}
	raw := s.base.RecordBatch()
	baseSchema := raw.Schema()
	fields := append([]arrow.Field(nil), baseSchema.Fields()...)
	columns := make([]arrow.Array, 0, int(raw.NumCols())+len(s.keys))
	for i := 0; i < int(raw.NumCols()); i++ {
		column := raw.Column(i)
		column.Retain()
		columns = append(columns, column)
	}
	for _, key := range s.keys {
		if len(baseSchema.FieldIndices(key)) > 0 {
			continue
		}
		builder := array.NewStringBuilder(s.allocator)
		for i := int64(0); i < raw.NumRows(); i++ {
			builder.Append(s.values[key])
		}
		column := builder.NewStringArray()
		builder.Release()
		columns = append(columns, column)
		fields = append(fields, arrow.Field{Name: key, Type: arrow.BinaryTypes.String, Nullable: false})
	}
	s.current = array.NewRecordBatch(arrow.NewSchema(fields, nil), columns, raw.NumRows())
	arrowutil.ReleaseArrays(columns)
	return true
}

func (s *partitionSource) RecordBatch() arrow.RecordBatch { return s.current }
func (s *partitionSource) Err() error                     { return s.err }
func (s *partitionSource) Release() {
	if s.current != nil {
		s.current.Release()
		s.current = nil
	}
	if s.base != nil {
		s.base.Release()
		s.base = nil
	}
}

// ValidatePartitionNames validates public partition column names.
func ValidatePartitionNames(names []string) error {
	seen := map[string]struct{}{}
	for _, name := range names {
		if name == "" || strings.ContainsAny(name, `/\\=`) {
			return fmt.Errorf("golars: invalid partition column %q", name)
		}
		if _, ok := seen[name]; ok {
			return fmt.Errorf("golars: duplicate partition column %q", name)
		}
		seen[name] = struct{}{}
	}
	return nil
}
