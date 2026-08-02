package parquetio

import (
	"github.com/apache/arrow-go/v18/arrow"
	parquetfile "github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
)

type filterSource struct {
	reader       pqarrow.RecordReader
	groupRows    []int64
	fullMatches  []bool
	groupIndex   int
	rowsInGroup  int64
	current      arrow.RecordBatch
	fullyMatches bool
}

func newFilterSource(reader pqarrow.RecordReader, file *parquetfile.Reader, rowGroups []int, fullMatches []bool) *filterSource {
	groupRows := make([]int64, len(rowGroups))
	for i, rowGroup := range rowGroups {
		groupRows[i] = file.RowGroup(rowGroup).NumRows()
	}
	return &filterSource{reader: reader, groupRows: groupRows, fullMatches: append([]bool(nil), fullMatches...)}
}

func (s *filterSource) Schema() *arrow.Schema { return s.reader.Schema() }

func (s *filterSource) Next() bool {
	if !s.reader.Next() {
		s.current = nil
		return false
	}
	s.current = s.reader.RecordBatch()
	s.fullyMatches = s.batchFullyMatches(s.current.NumRows())
	return true
}

func (s *filterSource) batchFullyMatches(rows int64) bool {
	if rows == 0 || len(s.fullMatches) != len(s.groupRows) {
		return false
	}
	fullyMatches := true
	remaining := rows
	for remaining > 0 && s.groupIndex < len(s.groupRows) {
		groupRemaining := s.groupRows[s.groupIndex] - s.rowsInGroup
		if groupRemaining <= 0 {
			s.groupIndex++
			s.rowsInGroup = 0
			continue
		}
		if !s.fullMatches[s.groupIndex] {
			fullyMatches = false
		}
		consumed := remaining
		if consumed > groupRemaining {
			consumed = groupRemaining
		}
		s.rowsInGroup += consumed
		remaining -= consumed
	}
	return remaining == 0 && fullyMatches
}

func (s *filterSource) RecordBatch() arrow.RecordBatch { return s.current }
func (s *filterSource) Err() error                     { return s.reader.Err() }
func (s *filterSource) Release()                       { s.reader.Release() }
func (s *filterSource) FiltersFullyMatch() bool        { return s.fullyMatches }
