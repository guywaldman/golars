//go:build linux && amd64 && !android && !faketime

package native

import _ "embed"

//go:embed assets/linux_amd64/libgolars_native.so.gz
var linuxAMD64Library []byte

func init() {
	embeddedLibraryCompressed = linuxAMD64Library
	embeddedLibraryExtension = ".so"
}
