//go:build darwin && arm64

package native

import _ "embed"

//go:embed assets/darwin_arm64/libgolars_native.dylib.gz
var darwinArm64LibraryCompressed []byte

func loadEmbeddedLibrary() (embeddedLibrary, error) {
	return decompressEmbeddedLibrary(darwinArm64LibraryCompressed, ".dylib")
}
