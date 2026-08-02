package ipcio

import (
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/ipc"
)

// Source is the batch-oriented Arrow IPC file reader contract.
type Source interface {
	Schema() *arrow.Schema
	Next() bool
	RecordBatch() arrow.RecordBatch
	Err() error
	Release()
}

type fileSource struct {
	reader  *ipc.FileReader
	index   int
	current arrow.RecordBatch
	err     error
}

// NewFileSource adapts an Arrow IPC file reader to the batch source contract.
func NewFileSource(reader *ipc.FileReader) Source { return &fileSource{reader: reader} }

func (s *fileSource) Schema() *arrow.Schema { return s.reader.Schema() }

func (s *fileSource) Next() bool {
	// FileReader owns the current batch and releases it when RecordBatch is
	// called for the next index. Callers may retain the previous batch.
	s.current = nil
	if s.index >= s.reader.NumRecords() {
		return false
	}
	var err error
	s.current, err = s.reader.RecordBatch(s.index)
	if err != nil {
		s.err = err
		return false
	}
	s.index++
	return true
}

func (s *fileSource) RecordBatch() arrow.RecordBatch { return s.current }
func (s *fileSource) Err() error                     { return s.err }
func (s *fileSource) Release() {
	if s.reader != nil {
		_ = s.reader.Close()
		s.reader = nil
	}
}
