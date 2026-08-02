//go:build linux && arm64 && !android && !faketime

package native

import _ "embed"

//go:embed assets/linux_arm64/libgolars_native.so.gz
var linuxARM64Library []byte

func init() {
	embeddedLibraryCompressed = linuxARM64Library
	embeddedLibraryExtension = ".so"
}
