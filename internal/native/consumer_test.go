package native

import (
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"testing"
)

func TestConsumerBuildsWithoutCGO(t *testing.T) {
	repoRoot, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatalf("resolve repository root: %v", err)
	}
	consumer := t.TempDir()
	mainSource, err := os.ReadFile(filepath.Join("testdata", "consumer", "main.go"))
	if err != nil {
		t.Fatalf("read consumer source: %v", err)
	}
	if err := os.WriteFile(filepath.Join(consumer, "main.go"), mainSource, 0o600); err != nil {
		t.Fatalf("write consumer source: %v", err)
	}
	module := "module example.com/golars-consumer\n\ngo 1.25.0\n\nrequire github.com/guywaldman/golars v0.0.0\n\nreplace github.com/guywaldman/golars => " + repoRoot + "\n"
	if err := os.WriteFile(filepath.Join(consumer, "go.mod"), []byte(module), 0o600); err != nil {
		t.Fatalf("write consumer module: %v", err)
	}
	cache := t.TempDir()
	cmd := exec.Command("go", "run", ".")
	cmd.Dir = consumer
	cmd.Env = append(os.Environ(),
		"CGO_ENABLED=0",
		"GOWORK=off",
		"GOCACHE="+cache,
		"GOFLAGS=-mod=mod",
	)
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("consumer failed on %s/%s: %v\n%s", runtime.GOOS, runtime.GOARCH, err, output)
	}
}
