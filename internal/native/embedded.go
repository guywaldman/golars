package native

import "errors"

type embeddedLibrary struct {
	data      []byte
	extension string
}

func unsupportedLibrary() (embeddedLibrary, error) {
	return embeddedLibrary{}, errors.Join(ErrUnavailable, errors.New("no embedded native library for this target"))
}
