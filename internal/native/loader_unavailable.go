//go:build !(darwin || linux || windows) || android || faketime

package native

func open() (*Runtime, error) {
	return nil, ErrUnavailable
}
