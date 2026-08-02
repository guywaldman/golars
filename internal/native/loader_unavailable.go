//go:build !(darwin || linux) || android || faketime

package native

func open() (*Runtime, error) {
	return nil, ErrUnavailable
}
