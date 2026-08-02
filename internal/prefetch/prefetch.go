package prefetch

import (
	"context"
	"sync"

	"github.com/apache/arrow-go/v18/arrow"
)

// Source is the batch-oriented reader contract consumed by prefetching.
type Source interface {
	Next() bool
	RecordBatch() arrow.RecordBatch
	Err() error
}

type filterGuaranteeSource interface {
	FiltersFullyMatch() bool
}

type filterColumnGuaranteeSource interface {
	GuaranteedFilterColumns() []string
}

// Batch is one prefetched record batch and its row-group guarantee.
type Batch struct {
	RecordBatch             arrow.RecordBatch
	FiltersFullyMatch       bool
	GuaranteedFilterColumns []string
}

// Prefetch reads one batch ahead while keeping the queue bounded at one batch.
type Prefetch struct {
	batches chan Batch
	done    chan struct{}
	cancel  context.CancelFunc
	once    sync.Once
}

func New(ctx context.Context, source Source) *Prefetch {
	readCtx, cancel := context.WithCancel(ctx)
	prefetch := &Prefetch{
		batches: make(chan Batch, 1),
		done:    make(chan struct{}),
		cancel:  cancel,
	}
	go func() {
		defer close(prefetch.batches)
		defer close(prefetch.done)
		for source.Next() {
			if err := readCtx.Err(); err != nil {
				return
			}
			batch := source.RecordBatch()
			if batch == nil {
				continue
			}
			batch.Retain()
			filtersFullyMatch := false
			if guaranteed, ok := source.(filterGuaranteeSource); ok {
				filtersFullyMatch = guaranteed.FiltersFullyMatch()
			}
			var guaranteedColumns []string
			if guaranteed, ok := source.(filterColumnGuaranteeSource); ok {
				guaranteedColumns = guaranteed.GuaranteedFilterColumns()
			}
			select {
			case prefetch.batches <- Batch{RecordBatch: batch, FiltersFullyMatch: filtersFullyMatch, GuaranteedFilterColumns: guaranteedColumns}:
			case <-readCtx.Done():
				batch.Release()
				return
			}
		}
	}()
	return prefetch
}

func (p *Prefetch) Next() (Batch, bool) {
	batch, ok := <-p.batches
	return batch, ok
}

func (p *Prefetch) Close() {
	if p == nil {
		return
	}
	p.once.Do(func() {
		p.cancel()
		<-p.done
		for item := range p.batches {
			item.RecordBatch.Release()
		}
	})
}
