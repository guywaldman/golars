//go:build windows && amd64

package native

import _ "embed"

//go:embed assets/windows_amd64/libgolars_native.dll.gz
var generatedNativeLibraryCompressed []byte

func init() {
	embeddedLibraryCompressed = generatedNativeLibraryCompressed
	embeddedLibraryExtension = ".dll"
}
