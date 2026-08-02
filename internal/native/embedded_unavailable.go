//go:build !darwin || !arm64

package native

func loadEmbeddedLibrary() (embeddedLibrary, error) {
	return unsupportedLibrary()
}
