package core

import (
	"context"
	"fmt"
	"io"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/apache/arrow-go/v18/arrow/memory"
	internalstorage "github.com/guywaldman/golars/internal/storage"
	"google.golang.org/api/option"
)

// DefaultMemoryLimit is the default per-engine memory quota in bytes.
const DefaultMemoryLimit int64 = 1 << 30

// EngineOption configures an Engine.
type EngineOption func(*engineConfig) error

type engineConfig struct {
	memoryLimit int64
	parallelism int
	batchSize   int64
	stores      map[string]internalstorage.ObjectStore
}

// WithObjectStore registers a store for a URI scheme on an Engine.
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

// WithMemoryLimit sets the engine memory quota in bytes.
func WithMemoryLimit(bytes int64) EngineOption {
	return func(c *engineConfig) error {
		if bytes <= 0 {
			return fmt.Errorf("golars: memory limit must be positive")
		}
		c.memoryLimit = bytes
		return nil
	}
}

// WithParallelism sets the engine's parallelism hint.
func WithParallelism(n int) EngineOption {
	return func(c *engineConfig) error {
		if n <= 0 {
			return fmt.Errorf("golars: parallelism must be positive")
		}
		c.parallelism = n
		return nil
	}
}

// WithBatchSize sets the preferred Arrow batch size.
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
	oldCapacity := cap(b)
	delta := int64(size - oldCapacity)
	if delta > 0 {
		a.reserve(delta)
	} else if delta < 0 {
		a.used.Add(delta)
	}
	result := a.base.Reallocate(size, b)
	if size <= cap(result) {
		// Arrow's Go allocator keeps the old capacity when shrinking. Cap the
		// returned slice so Free observes the same size that was accounted for.
		result = result[:size:size]
	}
	return result
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
	stores      map[string]internalstorage.ObjectStore
	managed     []io.Closer
	storeMu     sync.RWMutex
	closed      atomic.Bool
}

// NewEngine creates an execution engine with the supplied options.
func NewEngine(options ...EngineOption) (*Engine, error) {
	config := engineConfig{memoryLimit: DefaultMemoryLimit, parallelism: runtime.GOMAXPROCS(0), batchSize: 64 * 1024, stores: make(map[string]internalstorage.ObjectStore)}
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

// MemoryLimit returns the engine memory quota in bytes.
func (e *Engine) MemoryLimit() int64 { return e.allocator.limit }

// MemoryUsed returns the currently accounted bytes.
func (e *Engine) MemoryUsed() int64 { return e.allocator.used.Load() }

// Parallelism returns the configured parallelism hint.
func (e *Engine) Parallelism() int { return e.parallelism }

// BatchSize returns the preferred Arrow batch size.
func (e *Engine) BatchSize() int64 { return e.batchSize }

// Close releases engine-owned stores and prevents new work.
func (e *Engine) Close() {
	if e == nil || e.closed.Swap(true) {
		return
	}
	e.storeMu.Lock()
	managed := e.managed
	e.managed = nil
	e.storeMu.Unlock()
	for _, store := range managed {
		_ = store.Close()
	}
}

// ObjectStore returns the store registered for scheme.
func (e *Engine) ObjectStore(scheme string) (ObjectStore, bool) {
	if e == nil {
		return nil, false
	}
	e.storeMu.RLock()
	store, ok := e.stores[strings.ToLower(strings.TrimSuffix(scheme, "://"))]
	e.storeMu.RUnlock()
	return store, ok
}

// RegisterObjectStore adds a store to this engine.
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

// ensureObjectStore returns a registered store, creating the standard cloud
// adapter on first use when no custom store was supplied. Custom stores remain
// authoritative, which keeps emulators and S3-compatible endpoints working.
func (e *Engine) ensureObjectStore(ctx context.Context, scheme string) (ObjectStore, error) {
	if e == nil {
		return nil, ErrClosed
	}
	if ctx == nil {
		ctx = context.Background()
	}
	scheme = strings.ToLower(strings.TrimSuffix(scheme, "://"))

	e.storeMu.Lock()
	defer e.storeMu.Unlock()
	if store, ok := e.stores[scheme]; ok {
		return store, nil
	}

	var (
		store   ObjectStore
		managed io.Closer
		err     error
	)
	switch scheme {
	case "gs":
		var gcs *GCSStore
		gcs, err = NewGCSStore(ctx)
		if err != nil {
			// Public buckets do not need ADC. Retry anonymously when the
			// standard credential chain is unavailable.
			gcs, err = NewGCSStore(ctx, option.WithoutAuthentication())
		}
		store = gcs
		managed = gcs
	case "s3":
		store, err = NewS3Store(ctx)
	default:
		return nil, fmt.Errorf("golars: no object store registered for %q", scheme)
	}
	if err != nil {
		return nil, fmt.Errorf("golars: initialize %s object store: %w", scheme, err)
	}
	e.stores[scheme] = store
	if managed != nil {
		e.managed = append(e.managed, managed)
	}
	return store, nil
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
