package core

import (
	"fmt"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/apache/arrow-go/v18/arrow/memory"
	internalstorage "github.com/guywaldman/golars/internal/storage"
)

const DefaultMemoryLimit int64 = 1 << 30

type EngineOption func(*engineConfig) error

type engineConfig struct {
	memoryLimit int64
	parallelism int
	batchSize   int64
	native      bool
	stores      map[string]internalstorage.ObjectStore
}

func WithObjectStore(scheme string, store ObjectStore) EngineOption {
	return func(c *engineConfig) error {
		scheme = strings.ToLower(strings.TrimSuffix(scheme, "://"))
		if scheme == "" || store == nil {
			return fmt.Errorf("golars: object store scheme and store are required")
		}
		if c.stores == nil {
			c.stores = make(map[string]internalstorage.ObjectStore)
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

// WithNativeBackend controls use of the bundled native execution backend for
// plans it supports. Unsupported platforms and plans always use the Go path.
func WithNativeBackend(enabled bool) EngineOption {
	return func(c *engineConfig) error {
		c.native = enabled
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
	native      bool
	stores      map[string]internalstorage.ObjectStore
	storeMu     sync.RWMutex
	closed      atomic.Bool
}

func NewEngine(options ...EngineOption) (*Engine, error) {
	config := engineConfig{memoryLimit: DefaultMemoryLimit, parallelism: runtime.GOMAXPROCS(0), batchSize: 64 * 1024, native: true, stores: make(map[string]internalstorage.ObjectStore)}
	for _, option := range options {
		if err := option(&config); err != nil {
			return nil, err
		}
	}
	return &Engine{
		allocator:   &quotaAllocator{base: memory.NewGoAllocator(), limit: config.memoryLimit},
		parallelism: config.parallelism,
		batchSize:   config.batchSize,
		native:      config.native,
		stores:      config.stores,
	}, nil
}

func (e *Engine) MemoryLimit() int64 { return e.allocator.limit }
func (e *Engine) MemoryUsed() int64  { return e.allocator.used.Load() }
func (e *Engine) Parallelism() int   { return e.parallelism }
func (e *Engine) BatchSize() int64   { return e.batchSize }
func (e *Engine) NativeBackendEnabled() bool {
	return e != nil && e.native
}
func (e *Engine) Close() { e.closed.Store(true) }

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
