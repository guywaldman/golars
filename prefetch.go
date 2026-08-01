package golars

import (
	"context"
	"sync"

	"github.com/apache/arrow-go/v18/arrow"
)

// batchPrefetch reads one batch ahead of the execution loop. Parquet decoding
// and expression evaluation can then overlap without allowing an unbounded
// number of decoded batches to accumulate.
type batchPrefetch struct {
	batches chan prefetchedBatch
	done    chan struct{}
	cancel  context.CancelFunc
	once    sync.Once
}

type prefetchedBatch struct {
	batch             arrow.RecordBatch
	filtersFullyMatch bool
}

func newBatchPrefetch(ctx context.Context, source recordSource) *batchPrefetch {
	readCtx, cancel := context.WithCancel(ctx)
	prefetch := &batchPrefetch{
		batches: make(chan prefetchedBatch, 1),
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
			select {
			case prefetch.batches <- prefetchedBatch{batch: batch, filtersFullyMatch: filtersFullyMatch}:
			case <-readCtx.Done():
				batch.Release()
				return
			}
		}
	}()
	return prefetch
}

func (p *batchPrefetch) close() {
	if p == nil {
		return
	}
	p.once.Do(func() {
		p.cancel()
		<-p.done
		for item := range p.batches {
			item.batch.Release()
		}
	})
}
