//go:build windows && !faketime

package native

import (
	"fmt"
	"syscall"
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
	handle, err := syscall.LoadLibrary(path)
	if err != nil {
		return nil, fmt.Errorf("golars: load native backend: %w", err)
	}
	return registerRuntime(uintptr(handle))
}
