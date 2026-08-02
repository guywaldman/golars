//go:build !(darwin && arm64) && !(linux && (amd64 || arm64))

package native

func loadEmbeddedLibrary() (embeddedLibrary, error) {
	return unsupportedLibrary()
}
