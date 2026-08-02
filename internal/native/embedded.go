package native

import (
	"bytes"
	"compress/gzip"
	"errors"
	"fmt"
	"io"
)

type embeddedLibrary struct {
	data      []byte
	extension string
}

func unsupportedLibrary() (embeddedLibrary, error) {
	return embeddedLibrary{}, errors.Join(ErrUnavailable, errors.New("no embedded native library for this target"))
}

func decompressEmbeddedLibrary(compressed []byte, extension string) (embeddedLibrary, error) {
	reader, err := gzip.NewReader(bytes.NewReader(compressed))
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
	return embeddedLibrary{data: data, extension: extension}, nil
}
