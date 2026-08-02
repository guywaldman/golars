//go:build darwin && arm64 && !faketime

package native

import _ "embed"

//go:embed assets/darwin_arm64/libgolars_native.dylib.gz
var darwinArm64Library []byte

func init() {
	embeddedLibraryCompressed = darwinArm64Library
	embeddedLibraryExtension = ".dylib"
}
