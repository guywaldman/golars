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

	runtime := &Runtime{handle: handle}
	purego.RegisterLibFunc(&runtime.abiVersion, handle, "golars_native_abi_version")
	purego.RegisterLibFunc(&runtime.polarsProbe, handle, "golars_native_probe")
	purego.RegisterLibFunc(&runtime.versionBytes, handle, "golars_native_polars_version")
	purego.RegisterLibFunc(&runtime.executeFrame, handle, "golars_native_execute_frame")
	purego.RegisterLibFunc(&runtime.frameFree, handle, "golars_native_dataframe_free")
	purego.RegisterLibFunc(&runtime.frameHeight, handle, "golars_native_dataframe_height")
	purego.RegisterLibFunc(&runtime.frameWidth, handle, "golars_native_dataframe_width")
	purego.RegisterLibFunc(&runtime.frameIPC, handle, "golars_native_dataframe_ipc")
	purego.RegisterLibFunc(&runtime.frameSchemaIPC, handle, "golars_native_dataframe_schema_ipc")
	purego.RegisterLibFunc(&runtime.freeBuffer, handle, "golars_native_free")
	if runtime.ABIVersion() != 1 {
		return nil, fmt.Errorf("golars: unsupported native ABI version %d", runtime.ABIVersion())
	}
	return runtime, nil
}
