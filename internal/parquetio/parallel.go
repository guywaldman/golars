package parquetio

import (
	"context"
	"sync"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
)

const maxParallelRowGroupReaders = 4

type parallelGroup struct {
	rowGroups []int
	batches   chan parallelBatch
}

type parallelBatch struct {
	batch arrow.RecordBatch
	err   error
}

type parallelSource struct {
	schema     *arrow.Schema
	groups     []parallelGroup
	groupIndex int
	current    arrow.RecordBatch
	err        error
	cancel     context.CancelFunc
	done       chan struct{}
	once       sync.Once
}

func newParallelSource(
	ctx context.Context,
	reader *pqarrow.FileReader,
	schema *arrow.Schema,
	columns, rowGroups []int,
	workers int,
) *parallelSource {
	if workers < 1 {
		workers = 1
	}
	if workers > maxParallelRowGroupReaders {
		workers = maxParallelRowGroupReaders
	}
	if workers > len(rowGroups) {
		workers = len(rowGroups)
	}
	readCtx, cancel := context.WithCancel(ctx)
	groupRows := make([][]int, workers)
	for i, rowGroup := range rowGroups {
		groupIndex := i * workers / len(rowGroups)
		groupRows[groupIndex] = append(groupRows[groupIndex], rowGroup)
	}
	source := &parallelSource{
		schema: schema,
		groups: make([]parallelGroup, workers),
		cancel: cancel,
		done:   make(chan struct{}),
	}
	for i := range source.groups {
		source.groups[i].rowGroups = groupRows[i]
		source.groups[i].batches = make(chan parallelBatch, 1)
	}

	jobs := make(chan int)
	var workersWait sync.WaitGroup
	workersWait.Add(workers)
	for i := 0; i < workers; i++ {
		go func() {
			defer workersWait.Done()
			for {
				select {
				case groupIndex, ok := <-jobs:
					if !ok {
						return
					}
					source.readGroup(readCtx, reader, columns, groupIndex)
				case <-readCtx.Done():
					return
				}
			}
		}()
	}

	go func() {
		defer close(jobs)
		for groupIndex := range source.groups {
			select {
			case jobs <- groupIndex:
			case <-readCtx.Done():
				workersWait.Wait()
				close(source.done)
				return
			}
		}
		workersWait.Wait()
		close(source.done)
	}()
	return source
}

func (s *parallelSource) readGroup(
	ctx context.Context,
	reader *pqarrow.FileReader,
	columns []int,
	groupIndex int,
) {
	group := &s.groups[groupIndex]
	defer close(group.batches)
	groupReader, err := reader.GetRecordReader(ctx, columns, group.rowGroups)
	if err != nil {
		s.sendGroup(ctx, group, parallelBatch{err: err})
		return
	}
	defer groupReader.Release()
	for groupReader.Next() {
		batch := groupReader.RecordBatch()
		if batch == nil {
			continue
		}
		batch.Retain()
		if !s.sendGroup(ctx, group, parallelBatch{batch: batch}) {
			return
		}
	}
	if err := groupReader.Err(); err != nil {
		s.sendGroup(ctx, group, parallelBatch{err: err})
	}
}

func (s *parallelSource) sendGroup(ctx context.Context, group *parallelGroup, item parallelBatch) bool {
	select {
	case group.batches <- item:
		return true
	case <-ctx.Done():
		if item.batch != nil {
			item.batch.Release()
		}
		return false
	}
}

func (s *parallelSource) Schema() *arrow.Schema { return s.schema }

func (s *parallelSource) Next() bool {
	if s.current != nil {
		s.current.Release()
		s.current = nil
	}
	for s.groupIndex < len(s.groups) {
		item, ok := <-s.groups[s.groupIndex].batches
		if !ok {
			s.groupIndex++
			continue
		}
		if item.err != nil {
			s.err = item.err
			s.cancel()
			return false
		}
		s.current = item.batch
		return true
	}
	return false
}

func (s *parallelSource) RecordBatch() arrow.RecordBatch { return s.current }
func (s *parallelSource) Err() error                     { return s.err }

func (s *parallelSource) Release() {
	if s == nil {
		return
	}
	s.once.Do(func() {
		s.cancel()
		<-s.done
		if s.current != nil {
			s.current.Release()
			s.current = nil
		}
		for i := range s.groups {
			for item := range s.groups[i].batches {
				if item.batch != nil {
					item.batch.Release()
				}
			}
		}
	})
}
