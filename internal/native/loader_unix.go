//go:build (darwin || linux) && !android && !faketime

package native

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/ebitengine/purego"
)

func open() (*Runtime, error) {
	asset, err := loadEmbeddedLibrary()
	if err != nil {
		return nil, err
	}
	path, err := materialize(asset)
	if err != nil {
		return nil, err
	}
	handle, err := purego.Dlopen(path, purego.RTLD_NOW|purego.RTLD_LOCAL)
	if err != nil {
		_ = os.RemoveAll(filepath.Dir(path))
		return nil, fmt.Errorf("golars: load native backend: %w", err)
	}
	runtime, err := registerRuntime(handle)
	if err != nil {
		_ = purego.Dlclose(handle)
		_ = os.RemoveAll(filepath.Dir(path))
		return nil, err
	}
	// The dynamic loader keeps the mapped library alive after Dlopen returns;
	// remove the temporary copy so each process does not leave native files
	// behind on disk.
	_ = os.RemoveAll(filepath.Dir(path))
	return runtime, nil
}
