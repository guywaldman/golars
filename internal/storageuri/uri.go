package storageuri

import (
	"fmt"
	"strings"
)

// Scheme returns the lower-case URI scheme, or an empty string for local paths.
func Scheme(value string) string {
	if index := strings.Index(value, "://"); index > 0 {
		return strings.ToLower(value[:index])
	}
	return ""
}

// Parse splits a bucket-style object URI after validating its scheme and key.
func Parse(uri, scheme string) (string, string, error) {
	prefix := scheme + "://"
	if !strings.HasPrefix(strings.ToLower(uri), prefix) {
		return "", "", fmt.Errorf("golars: expected %s URI, got %q", prefix, uri)
	}
	value := uri[len(prefix):]
	separator := strings.IndexByte(value, '/')
	if separator <= 0 || separator == len(value)-1 {
		return "", "", fmt.Errorf("golars: invalid %s URI %q", scheme, uri)
	}
	return value[:separator], value[separator+1:], nil
}

// ValidateRange checks the offset and length used for a ranged object read.
func ValidateRange(offset, length int64) error {
	if offset < 0 || length <= 0 {
		return fmt.Errorf("golars: invalid object range offset=%d length=%d", offset, length)
	}
	if offset > int64(^uint64(0)>>1)-length {
		return fmt.Errorf("golars: object range overflows int64: offset=%d length=%d", offset, length)
	}
	return nil
}
