//go:build windows && arm64

package native

import _ "embed"

//go:embed assets/windows_arm64/libgolars_native.dll.gz
var generatedNativeLibraryCompressed []byte

func init() {
	embeddedLibraryCompressed = generatedNativeLibraryCompressed
	embeddedLibraryExtension = ".dll"
}
