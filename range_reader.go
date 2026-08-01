package golars

import (
	"context"
	"fmt"
	"io"
	"sync"
)

// rangeReaderAtSeeker adapts ranged object reads to Parquet's ReaderAtSeeker
// contract. Each ReadAt becomes one bounded object-store request; the Parquet
// reader therefore fetches only footer and column-chunk bytes it needs.
type rangeReaderAtSeeker struct {
	ctx   context.Context
	store RangeObjectStore
	uri   string
	size  int64

	mu     sync.Mutex
	offset int64
	closed bool
}

func newRangeReaderAtSeeker(ctx context.Context, store RangeObjectStore, uri string, size int64) (*rangeReaderAtSeeker, error) {
	if store == nil {
		return nil, fmt.Errorf("golars: nil range object store")
	}
	if size < 0 {
		return nil, fmt.Errorf("golars: invalid object size %d", size)
	}
	return &rangeReaderAtSeeker{ctx: ctx, store: store, uri: uri, size: size}, nil
}

func (r *rangeReaderAtSeeker) ReadAt(p []byte, offset int64) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	if offset < 0 {
		return 0, fmt.Errorf("golars: negative object read offset %d", offset)
	}
	r.mu.Lock()
	closed := r.closed
	size := r.size
	r.mu.Unlock()
	if closed {
		return 0, io.ErrClosedPipe
	}
	if offset >= size {
		return 0, io.EOF
	}
	wanted := len(p)
	if remaining := size - offset; remaining < int64(wanted) {
		wanted = int(remaining)
	}
	reader, err := r.store.OpenRange(r.ctx, r.uri, offset, int64(wanted))
	if err != nil {
		return 0, err
	}
	n, readErr := io.ReadFull(reader, p[:wanted])
	closeErr := reader.Close()
	if readErr == io.EOF && n == wanted {
		readErr = nil
	}
	if readErr == nil && n < wanted {
		readErr = io.ErrUnexpectedEOF
	}
	if readErr == nil && wanted < len(p) {
		readErr = io.EOF
	}
	if readErr == nil && closeErr != nil {
		readErr = closeErr
	}
	return n, readErr
}

func (r *rangeReaderAtSeeker) Read(p []byte) (int, error) {
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return 0, io.ErrClosedPipe
	}
	offset := r.offset
	r.mu.Unlock()
	n, err := r.ReadAt(p, offset)
	r.mu.Lock()
	r.offset += int64(n)
	r.mu.Unlock()
	return n, err
}

func (r *rangeReaderAtSeeker) Seek(offset int64, whence int) (int64, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return 0, io.ErrClosedPipe
	}
	var next int64
	switch whence {
	case io.SeekStart:
		next = offset
	case io.SeekCurrent:
		next = r.offset + offset
	case io.SeekEnd:
		next = r.size + offset
	default:
		return 0, fmt.Errorf("golars: invalid seek whence %d", whence)
	}
	if next < 0 {
		return 0, fmt.Errorf("golars: negative seek position %d", next)
	}
	r.offset = next
	return next, nil
}

func (r *rangeReaderAtSeeker) Close() error {
	r.mu.Lock()
	r.closed = true
	r.mu.Unlock()
	return nil
}
