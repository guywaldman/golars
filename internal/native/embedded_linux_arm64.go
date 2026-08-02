//go:build linux && arm64

package native

import _ "embed"

//go:embed assets/linux_arm64/libgolars_native.so.gz
var linuxARM64LibraryCompressed []byte

func loadEmbeddedLibrary() (embeddedLibrary, error) {
	return decompressEmbeddedLibrary(linuxARM64LibraryCompressed, ".so")
}
