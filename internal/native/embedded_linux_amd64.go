//go:build linux && amd64

package native

import _ "embed"

//go:embed assets/linux_amd64/libgolars_native.so.gz
var linuxAMD64LibraryCompressed []byte

func loadEmbeddedLibrary() (embeddedLibrary, error) {
	return decompressEmbeddedLibrary(linuxAMD64LibraryCompressed, ".so")
}
