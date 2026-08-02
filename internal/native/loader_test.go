package native

import (
	"errors"
	"testing"
)

func TestEmbeddedRuntime(t *testing.T) {
	nativeRuntime, err := Open()
	if err != nil {
		if errors.Is(err, ErrUnavailable) {
			t.Skipf("native backend unavailable: %v", err)
		}
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
