package parquetio

import (
	"context"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"github.com/guywaldman/golars/internal/arrowutil"
)

// projectionSource reads projected columns from fully matching row groups and
// keeps filter columns available for partially matching groups.
type projectionSource struct {
	ctx               context.Context
	allocator         memory.Allocator
	reader            *pqarrow.FileReader
	outputSchema      *arrow.Schema
	allColumns        []int
	projectedColumns  []int
	projectionOnly    bool
	runs              []projectionRun
	runIndex          int
	currentReader     Source
	current           arrow.RecordBatch
	currentOwned      bool
	currentFull       bool
	currentProjection bool
	filterOnlyColumns []string
	parallelism       int
	err               error
}

type projectionRun struct {
	rowGroups  []int
	full       bool
	projection bool
}

func newProjectionSource(
	ctx context.Context,
	allocator memory.Allocator,
	reader *pqarrow.FileReader,
	outputSchema *arrow.Schema,
	allColumns, projectedColumns, rowGroups []int,
	fullMatches []bool,
	projectionMatches []bool,
	filterOnlyColumns []string,
	projectionOnly bool,
	parallelism int,
) *projectionSource {
	runs := make([]projectionRun, 0, len(rowGroups))
	for i, rowGroup := range rowGroups {
		full := i < len(fullMatches) && fullMatches[i]
		projection := i < len(projectionMatches) && projectionMatches[i]
		if len(runs) > 0 && runs[len(runs)-1].full == full && runs[len(runs)-1].projection == projection {
			runs[len(runs)-1].rowGroups = append(runs[len(runs)-1].rowGroups, rowGroup)
			continue
		}
		runs = append(runs, projectionRun{rowGroups: []int{rowGroup}, full: full, projection: projection})
	}
	return &projectionSource{
		ctx:               ctx,
		allocator:         allocator,
		reader:            reader,
		outputSchema:      outputSchema,
		allColumns:        append([]int(nil), allColumns...),
		projectedColumns:  append([]int(nil), projectedColumns...),
		projectionOnly:    projectionOnly,
		filterOnlyColumns: append([]string(nil), filterOnlyColumns...),
		parallelism:       parallelism,
		runs:              runs,
	}
}

func (s *projectionSource) Schema() *arrow.Schema { return s.outputSchema }

func (s *projectionSource) Next() bool {
	s.releaseCurrent()
	for {
		if s.currentReader != nil {
			if s.currentReader.Next() {
				batch := s.currentReader.RecordBatch()
				if s.currentFull && !s.projectionOnly {
					projected, err := s.addMissingColumns(batch)
					if err != nil {
						s.err = err
						return false
					}
					s.current = projected
					s.currentOwned = true
				} else {
					s.current = batch
				}
				return true
			}
			if err := s.currentReader.Err(); err != nil {
				s.err = err
				s.currentReader.Release()
				s.currentReader = nil
				return false
			}
			s.currentReader.Release()
			s.currentReader = nil
		}
		if s.runIndex >= len(s.runs) {
			return false
		}
		run := s.runs[s.runIndex]
		s.runIndex++
		columns := s.allColumns
		project := run.full || (s.projectionOnly && run.projection)
		if project {
			columns = s.projectedColumns
		}
		if (run.full || project) && s.parallelism > 1 && len(run.rowGroups) > 1 {
			s.currentReader = newParallelSource(s.ctx, s.reader, s.outputSchema, columns, run.rowGroups, s.parallelism)
		} else {
			reader, err := s.reader.GetRecordReader(s.ctx, columns, run.rowGroups)
			if err != nil {
				s.err = err
				return false
			}
			s.currentReader = reader
		}
		s.currentFull = run.full
		s.currentProjection = project
	}
}

func (s *projectionSource) addMissingColumns(batch arrow.RecordBatch) (arrow.RecordBatch, error) {
	projectionPositions := make(map[int]int, len(s.projectedColumns))
	for position, column := range s.projectedColumns {
		projectionPositions[column] = position
	}
	columns := make([]arrow.Array, len(s.allColumns))
	for position, column := range s.allColumns {
		if projectedPosition, ok := projectionPositions[column]; ok {
			value := batch.Column(projectedPosition)
			value.Retain()
			columns[position] = value
			continue
		}
		columns[position] = array.MakeArrayOfNull(s.allocator, s.outputSchema.Field(position).Type, int(batch.NumRows()))
	}
	result := array.NewRecordBatch(s.outputSchema, columns, batch.NumRows())
	arrowutil.ReleaseArrays(columns)
	return result, nil
}

func (s *projectionSource) RecordBatch() arrow.RecordBatch { return s.current }
func (s *projectionSource) Err() error                     { return s.err }
func (s *projectionSource) FiltersFullyMatch() bool        { return s.currentFull }
func (s *projectionSource) PrefetchesBatches() bool {
	if s.parallelism <= 1 {
		return false
	}
	for _, run := range s.runs {
		project := run.full || (s.projectionOnly && run.projection)
		if (run.full || project) && len(run.rowGroups) > 1 {
			return true
		}
	}
	return false
}
func (s *projectionSource) GuaranteedFilterColumns() []string {
	if s.projectionOnly && s.currentProjection && !s.currentFull {
		return s.filterOnlyColumns
	}
	return nil
}

func (s *projectionSource) releaseCurrent() {
	if s.currentOwned && s.current != nil {
		s.current.Release()
	}
	s.current = nil
	s.currentOwned = false
}

func (s *projectionSource) Release() {
	s.releaseCurrent()
	if s.currentReader != nil {
		s.currentReader.Release()
		s.currentReader = nil
	}
	s.runs = nil
}
