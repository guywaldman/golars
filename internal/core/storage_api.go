package core

import (
	"context"

	"cloud.google.com/go/storage"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	internalstorage "github.com/guywaldman/golars/internal/storage"
	"google.golang.org/api/option"
)

// ObjectStore is the minimal storage contract used by scans and sinks. Stores
// are registered by URI scheme; no session object is required for ordinary use.
type ObjectStore = internalstorage.ObjectStore

// ObjectInfo contains the metadata needed for random-access scans.
type ObjectInfo = internalstorage.ObjectInfo

// RangeObjectStore is an optional extension used by Parquet scans. Stores that
// implement it allow metadata and column chunks to be read with range requests.
type RangeObjectStore = internalstorage.RangeObjectStore

// S3Store adapts an AWS S3-compatible object store to the scan/sink API.
type S3Store = internalstorage.S3Store

func NewS3Store(ctx context.Context, options ...func(*s3.Options)) (*S3Store, error) {
	return internalstorage.NewS3Store(ctx, options...)
}

func NewS3StoreWithClient(client *s3.Client) (*S3Store, error) {
	return internalstorage.NewS3StoreWithClient(client)
}

// GCSStore adapts Google Cloud Storage to the scan/sink API.
type GCSStore = internalstorage.GCSStore

func NewGCSStore(ctx context.Context, options ...option.ClientOption) (*GCSStore, error) {
	return internalstorage.NewGCSStore(ctx, options...)
}

func NewGCSStoreWithClient(client *storage.Client) (*GCSStore, error) {
	return internalstorage.NewGCSStoreWithClient(client)
}
