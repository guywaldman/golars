package golars

import (
	"context"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
)

// parquetProjectionSource reads only projected columns from row groups whose
// filters are proven true. Partial groups still read every column needed by
// the filter and go through the normal exact filter evaluation. Filter-only
// columns in proven groups are represented by null arrays because execution
// skips those filter nodes for that batch and the scan request guarantees that
// downstream expressions do not reference them.
type parquetProjectionSource struct {
	ctx              context.Context
	engine           *Engine
	reader           *pqarrow.FileReader
	outputSchema     *arrow.Schema
	allColumns       []int
	projectedColumns []int
	runs             []parquetProjectionRun
	runIndex         int
	currentReader    pqarrow.RecordReader
	current          arrow.RecordBatch
	currentOwned     bool
	currentFull      bool
	err              error
}

type parquetProjectionRun struct {
	rowGroups []int
	full      bool
}

func newParquetProjectionSource(
	ctx context.Context,
	engine *Engine,
	reader *pqarrow.FileReader,
	outputSchema *arrow.Schema,
	allColumns, projectedColumns, rowGroups []int,
	fullMatches []bool,
) *parquetProjectionSource {
	runs := make([]parquetProjectionRun, 0, len(rowGroups))
	for i, rowGroup := range rowGroups {
		full := i < len(fullMatches) && fullMatches[i]
		if len(runs) > 0 && runs[len(runs)-1].full == full {
			runs[len(runs)-1].rowGroups = append(runs[len(runs)-1].rowGroups, rowGroup)
			continue
		}
		runs = append(runs, parquetProjectionRun{rowGroups: []int{rowGroup}, full: full})
	}
	return &parquetProjectionSource{
		ctx:              ctx,
		engine:           engine,
		reader:           reader,
		outputSchema:     outputSchema,
		allColumns:       append([]int(nil), allColumns...),
		projectedColumns: append([]int(nil), projectedColumns...),
		runs:             runs,
	}
}

func (s *parquetProjectionSource) Schema() *arrow.Schema { return s.outputSchema }

func (s *parquetProjectionSource) Next() bool {
	s.releaseCurrent()
	for {
		if s.currentReader != nil {
			if s.currentReader.Next() {
				batch := s.currentReader.RecordBatch()
				if s.currentFull {
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
		full := run.full
		columns := s.allColumns
		if full {
			columns = s.projectedColumns
		}
		reader, err := s.reader.GetRecordReader(s.ctx, columns, run.rowGroups)
		s.runIndex++
		if err != nil {
			s.err = err
			return false
		}
		s.currentReader = reader
		s.currentFull = full
	}
}

func (s *parquetProjectionSource) addMissingColumns(batch arrow.RecordBatch) (arrow.RecordBatch, error) {
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
		columns[position] = array.MakeArrayOfNull(s.engine.allocator, s.outputSchema.Field(position).Type, int(batch.NumRows()))
	}
	result := array.NewRecordBatch(s.outputSchema, columns, batch.NumRows())
	releaseArrays(columns)
	return result, nil
}

func (s *parquetProjectionSource) RecordBatch() arrow.RecordBatch { return s.current }
func (s *parquetProjectionSource) Err() error                     { return s.err }
func (s *parquetProjectionSource) FiltersFullyMatch() bool        { return s.currentFull }

func (s *parquetProjectionSource) releaseCurrent() {
	if s.currentOwned && s.current != nil {
		s.current.Release()
	}
	s.current = nil
	s.currentOwned = false
}

func (s *parquetProjectionSource) Release() {
	s.releaseCurrent()
	if s.currentReader != nil {
		s.currentReader.Release()
		s.currentReader = nil
	}
	s.runs = nil
}
