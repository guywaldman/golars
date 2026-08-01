package golars

import (
	"context"
	"fmt"
	"io"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/apache/arrow-go/v18/arrow/memory"
)

const DefaultMemoryLimit int64 = 1 << 30

type EngineOption func(*engineConfig) error

type engineConfig struct {
	memoryLimit int64
	parallelism int
	batchSize   int64
	stores      map[string]ObjectStore
}

// ObjectStore is the minimal storage contract used by scans and sinks. Stores
// are registered by URI scheme (for example "s3" or "gs"); no session object
// is required for ordinary package-level use.
type ObjectStore interface {
	Open(context.Context, string) (io.ReadCloser, error)
	Create(context.Context, string) (io.WriteCloser, error)
	Glob(context.Context, string) ([]string, error)
}

// ObjectInfo contains the metadata needed for random-access scans.
type ObjectInfo struct {
	Size int64
}

// RangeObjectStore is an optional extension used by Parquet scans. Stores that
// implement it allow the engine to read metadata and column chunks with HTTP
// range requests instead of downloading the whole object. Existing ObjectStore
// implementations remain valid and use the materializing fallback.
type RangeObjectStore interface {
	ObjectStore
	Stat(context.Context, string) (ObjectInfo, error)
	OpenRange(context.Context, string, int64, int64) (io.ReadCloser, error)
}

func WithObjectStore(scheme string, store ObjectStore) EngineOption {
	return func(c *engineConfig) error {
		scheme = strings.ToLower(strings.TrimSuffix(scheme, "://"))
		if scheme == "" || store == nil {
			return fmt.Errorf("golars: object store scheme and store are required")
		}
		if c.stores == nil {
			c.stores = make(map[string]ObjectStore)
		}
		c.stores[scheme] = store
		return nil
	}
}

func WithMemoryLimit(bytes int64) EngineOption {
	return func(c *engineConfig) error {
		if bytes <= 0 {
			return fmt.Errorf("golars: memory limit must be positive")
		}
		c.memoryLimit = bytes
		return nil
	}
}

func WithParallelism(n int) EngineOption {
	return func(c *engineConfig) error {
		if n <= 0 {
			return fmt.Errorf("golars: parallelism must be positive")
		}
		c.parallelism = n
		return nil
	}
}

func WithBatchSize(rows int64) EngineOption {
	return func(c *engineConfig) error {
		if rows <= 0 {
			return fmt.Errorf("golars: batch size must be positive")
		}
		c.batchSize = rows
		return nil
	}
}

type allocationLimitPanic struct{ err *MemoryLimitError }

type quotaAllocator struct {
	base  memory.Allocator
	limit int64
	used  atomic.Int64
}

func (a *quotaAllocator) Allocate(size int) []byte {
	if size <= 0 {
		return nil
	}
	a.reserve(int64(size))
	return a.base.Allocate(size)
}
func (a *quotaAllocator) Reallocate(size int, b []byte) []byte {
	delta := int64(size - cap(b))
	if delta > 0 {
		a.reserve(delta)
	} else if delta < 0 {
		a.used.Add(delta)
	}
	return a.base.Reallocate(size, b)
}
func (a *quotaAllocator) Free(b []byte) {
	a.used.Add(-int64(cap(b)))
	a.base.Free(b)
}
func (a *quotaAllocator) reserve(size int64) {
	used := a.used.Add(size)
	if used > a.limit {
		a.used.Add(-size)
		panic(allocationLimitPanic{&MemoryLimitError{Limit: a.limit, Allocated: used - size, Requested: size}})
	}
}

// Engine owns execution configuration and memory accounting. The package API uses
// a lazily-created default Engine; applications only need Engine for isolation or tuning.
type Engine struct {
	allocator   *quotaAllocator
	parallelism int
	batchSize   int64
	stores      map[string]ObjectStore
	storeMu     sync.RWMutex
	closed      atomic.Bool
}

func NewEngine(options ...EngineOption) (*Engine, error) {
	config := engineConfig{memoryLimit: DefaultMemoryLimit, parallelism: runtime.GOMAXPROCS(0), batchSize: 64 * 1024, stores: make(map[string]ObjectStore)}
	for _, option := range options {
		if err := option(&config); err != nil {
			return nil, err
		}
	}
	return &Engine{
		allocator:   &quotaAllocator{base: memory.NewGoAllocator(), limit: config.memoryLimit},
		parallelism: config.parallelism,
		batchSize:   config.batchSize,
		stores:      config.stores,
	}, nil
}

func (e *Engine) MemoryLimit() int64 { return e.allocator.limit }
func (e *Engine) MemoryUsed() int64  { return e.allocator.used.Load() }
func (e *Engine) Parallelism() int   { return e.parallelism }
func (e *Engine) BatchSize() int64   { return e.batchSize }
func (e *Engine) Close()             { e.closed.Store(true) }

func (e *Engine) ObjectStore(scheme string) (ObjectStore, bool) {
	if e == nil {
		return nil, false
	}
	e.storeMu.RLock()
	store, ok := e.stores[strings.ToLower(strings.TrimSuffix(scheme, "://"))]
	e.storeMu.RUnlock()
	return store, ok
}

func (e *Engine) RegisterObjectStore(scheme string, store ObjectStore) error {
	if e == nil || store == nil {
		return fmt.Errorf("golars: object store and engine are required")
	}
	scheme = strings.ToLower(strings.TrimSuffix(scheme, "://"))
	if scheme == "" {
		return fmt.Errorf("golars: object store scheme is required")
	}
	e.storeMu.Lock()
	if e.stores == nil {
		e.stores = make(map[string]ObjectStore)
	}
	e.stores[scheme] = store
	e.storeMu.Unlock()
	return nil
}

// RegisterObjectStore adds a store to the package default engine. This is an
// integration hook, not a required session lifecycle.
func RegisterObjectStore(scheme string, store ObjectStore) error {
	return getDefaultEngine().RegisterObjectStore(scheme, store)
}

func (e *Engine) checkOpen() error {
	if e == nil || e.closed.Load() {
		return ErrClosed
	}
	return nil
}

var (
	defaultEngineOnce sync.Once
	defaultEngine     *Engine
)

func getDefaultEngine() *Engine {
	defaultEngineOnce.Do(func() {
		defaultEngine, _ = NewEngine()
	})
	return defaultEngine
}

func recoverMemoryLimit(errp *error) {
	if recovered := recover(); recovered != nil {
		if limit, ok := recovered.(allocationLimitPanic); ok {
			*errp = limit.err
			return
		}
		panic(recovered)
	}
}
