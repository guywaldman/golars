//go:build linux && amd64

package native

import _ "embed"

//go:embed assets/linux_amd64/libgolars_native.so.gz
var generatedNativeLibraryCompressed []byte

func init() {
	embeddedLibraryCompressed = generatedNativeLibraryCompressed
	embeddedLibraryExtension = ".so"
}
