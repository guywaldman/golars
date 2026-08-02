package storage

import (
	"context"
	"io"
)

// ObjectStore is the minimal storage contract used by scans and sinks.
type ObjectStore interface {
	Open(context.Context, string) (io.ReadCloser, error)
	Create(context.Context, string) (io.WriteCloser, error)
	Glob(context.Context, string) ([]string, error)
}

// ObjectInfo contains the metadata needed for random-access scans.
type ObjectInfo struct {
	Size int64
}

// RangeObjectStore extends ObjectStore with metadata and byte-range reads.
type RangeObjectStore interface {
	ObjectStore
	Stat(context.Context, string) (ObjectInfo, error)
	OpenRange(context.Context, string, int64, int64) (io.ReadCloser, error)
}
