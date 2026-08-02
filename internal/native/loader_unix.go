//go:build (darwin || linux) && !android && !faketime

package native

import (
	"fmt"

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
		return nil, fmt.Errorf("golars: load native backend: %w", err)
	}

	return registerRuntime(handle)
}
