//go:build darwin && arm64

package native

import _ "embed"

//go:embed assets/darwin_arm64/libgolars_native.dylib.gz
var generatedNativeLibraryCompressed []byte

func init() {
	embeddedLibraryCompressed = generatedNativeLibraryCompressed
	embeddedLibraryExtension = ".dylib"
}
