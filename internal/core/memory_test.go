package core

import (
	"context"
	"path/filepath"
	"runtime"
	"testing"

	nativebackend "github.com/guywaldman/golars/internal/native"
)

func TestClosedFramesReleaseArrowAllocations(t *testing.T) {
	path := writeMemoryTestParquet(t)
	engine, err := NewEngine(WithBatchSize(8))
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close()
	baseline := engine.MemoryUsed()
	for iteration := 0; iteration < 8; iteration++ {
		plan := engine.ScanParquet(path).
			Filter(Col("id").Ge(Lit(int64(2)))).
			Select(Col("value"))
		result, err := plan.Collect(context.Background())
		plan.Close()
		if err != nil {
			t.Fatal(err)
		}
		result.Close()
		runtime.GC()
		if got := engine.MemoryUsed(); got != baseline {
			t.Fatalf("iteration %d retained %d allocator bytes, baseline %d", iteration, got, baseline)
		}
	}
}

func TestQuotaAllocatorReallocateTracksShrunkCapacity(t *testing.T) {
	engine, err := NewEngine()
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close()

	buffer := engine.allocator.Allocate(256)
	if got := engine.MemoryUsed(); got != 256 {
		t.Fatalf("after allocate used=%d, want 256", got)
	}
	buffer = engine.allocator.Reallocate(128, buffer)
	if got := engine.MemoryUsed(); got != 128 {
		t.Fatalf("after shrink used=%d, want 128", got)
	}
	engine.allocator.Free(buffer)
	if got := engine.MemoryUsed(); got != 0 {
		t.Fatalf("after free used=%d, want 0", got)
	}
}

func TestNativeFrameCloseDoesNotRetainGoHeap(t *testing.T) {
	if _, err := nativebackend.Open(); err != nil {
		t.Skipf("native backend unavailable: %v", err)
	}
	path := writeMemoryTestParquet(t)
	engine, err := NewEngine(WithBatchSize(8))
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close()
	run := func(iterations int) {
		for iteration := 0; iteration < iterations; iteration++ {
			plan := engine.ScanParquet(path).
				Filter(Col("id").Ge(Lit(int64(2)))).
				Select(Col("value"))
			result, err := plan.Collect(context.Background())
			plan.Close()
			if err != nil {
				t.Fatal(err)
			}
			result.Close()
		}
	}
	run(4)
	runtime.GC()
	var before runtime.MemStats
	runtime.ReadMemStats(&before)
	run(24)
	runtime.GC()
	var after runtime.MemStats
	runtime.ReadMemStats(&after)
	const allowedHeapGrowth = 8 << 20
	if after.HeapAlloc > before.HeapAlloc+allowedHeapGrowth {
		t.Fatalf("native frame loop retained %d Go heap bytes", after.HeapAlloc-before.HeapAlloc)
	}
}

func writeMemoryTestParquet(t *testing.T) string {
	t.Helper()
	ids, err := NewSeries("id", []int64{1, 2, 3, 4})
	if err != nil {
		t.Fatal(err)
	}
	defer ids.Close()
	values, err := NewSeries("value", []int64{10, 20, 30, 40})
	if err != nil {
		t.Fatal(err)
	}
	defer values.Close()
	input, err := NewDataFrame(ids, values)
	if err != nil {
		t.Fatal(err)
	}
	defer input.Close()
	path := filepath.Join(t.TempDir(), "input.parquet")
	if err := WriteParquet(context.Background(), input, path, WithOverwrite(true)); err != nil {
		t.Fatal(err)
	}
	return path
}
