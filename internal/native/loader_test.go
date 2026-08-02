package native

import (
	"errors"
	"runtime"
	"testing"
)

func TestEmbeddedRuntime(t *testing.T) {
	nativeRuntime, err := Open()
	if runtime.GOOS != "darwin" || runtime.GOARCH != "arm64" {
		if !errors.Is(err, ErrUnavailable) {
			t.Fatalf("Open() error = %v, want ErrUnavailable on unsupported target", err)
		}
		return
	}
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	if got := nativeRuntime.ABIVersion(); got != 1 {
		t.Fatalf("ABIVersion() = %d, want 1", got)
	}
	if got := nativeRuntime.Probe(); got != 0 {
		t.Fatalf("Probe() = %d, want 0", got)
	}
	if got, err := nativeRuntime.PolarsVersion(); err != nil || got == "" {
		t.Fatalf("PolarsVersion() = %q, %v", got, err)
	}
}
