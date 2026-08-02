//go:build darwin && arm64

package native

import (
	"bytes"
	"compress/gzip"
	_ "embed"
	"fmt"
	"io"
)

//go:embed assets/darwin_arm64/libgolars_native.dylib.gz
var darwinArm64LibraryCompressed []byte

func loadEmbeddedLibrary() (embeddedLibrary, error) {
	reader, err := gzip.NewReader(bytes.NewReader(darwinArm64LibraryCompressed))
	if err != nil {
		return embeddedLibrary{}, fmt.Errorf("golars: open embedded native backend: %w", err)
	}
	data, err := io.ReadAll(reader)
	closeErr := reader.Close()
	if err != nil {
		return embeddedLibrary{}, fmt.Errorf("golars: decompress embedded native backend: %w", err)
	}
	if closeErr != nil {
		return embeddedLibrary{}, fmt.Errorf("golars: close embedded native backend: %w", closeErr)
	}
	return embeddedLibrary{data: data, extension: ".dylib"}, nil
}
