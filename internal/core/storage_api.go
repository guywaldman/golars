package core

import (
	"context"

	"cloud.google.com/go/storage"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	internalstorage "github.com/guywaldman/golars/internal/storage"
	"google.golang.org/api/option"
)

// ObjectStore is the minimal storage contract used by scans and sinks. The
// built-in s3:// and gs:// adapters are created lazily; custom stores can be
// registered by URI scheme without introducing a session lifecycle.
type ObjectStore = internalstorage.ObjectStore

// ObjectInfo contains the metadata needed for random-access scans.
type ObjectInfo = internalstorage.ObjectInfo

// RangeObjectStore is an optional extension used by Parquet scans. Stores that
// implement it allow metadata and column chunks to be read with range requests.
type RangeObjectStore = internalstorage.RangeObjectStore

// S3Store adapts an AWS S3-compatible object store to the scan/sink API.
type S3Store = internalstorage.S3Store

// NewS3Store creates an S3-backed ObjectStore.
func NewS3Store(ctx context.Context, options ...func(*s3.Options)) (*S3Store, error) {
	return internalstorage.NewS3Store(ctx, options...)
}

// NewS3StoreWithClient wraps an existing S3 client as an ObjectStore.
func NewS3StoreWithClient(client *s3.Client) (*S3Store, error) {
	return internalstorage.NewS3StoreWithClient(client)
}

// GCSStore adapts Google Cloud Storage to the scan/sink API.
type GCSStore = internalstorage.GCSStore

// NewGCSStore creates a Google Cloud Storage-backed ObjectStore.
func NewGCSStore(ctx context.Context, options ...option.ClientOption) (*GCSStore, error) {
	return internalstorage.NewGCSStore(ctx, options...)
}

// NewGCSStoreWithClient wraps an existing Cloud Storage client as an ObjectStore.
func NewGCSStoreWithClient(client *storage.Client) (*GCSStore, error) {
	return internalstorage.NewGCSStoreWithClient(client)
}
