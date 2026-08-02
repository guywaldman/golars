package storage

import (
	"context"
	"fmt"
	"io"
	"path"
	"strings"

	"cloud.google.com/go/storage"
	"github.com/guywaldman/golars/internal/storageuri"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
)

// GCSStore adapts Google Cloud Storage. Authentication follows the standard
// Google client-library environment and workload-identity configuration.
type GCSStore struct{ client *storage.Client }

func NewGCSStore(ctx context.Context, options ...option.ClientOption) (*GCSStore, error) {
	client, err := storage.NewClient(ctx, options...)
	if err != nil {
		return nil, err
	}
	return &GCSStore{client: client}, nil
}

func NewGCSStoreWithClient(client *storage.Client) (*GCSStore, error) {
	if client == nil {
		return nil, fmt.Errorf("golars: nil GCS client")
	}
	return &GCSStore{client: client}, nil
}

func (s *GCSStore) Open(ctx context.Context, uri string) (io.ReadCloser, error) {
	bucket, key, err := storageuri.Parse(uri, "gs")
	if err != nil {
		return nil, err
	}
	return s.client.Bucket(bucket).Object(key).NewReader(ctx)
}

func (s *GCSStore) Stat(ctx context.Context, uri string) (ObjectInfo, error) {
	bucket, key, err := storageuri.Parse(uri, "gs")
	if err != nil {
		return ObjectInfo{}, err
	}
	attrs, err := s.client.Bucket(bucket).Object(key).Attrs(ctx)
	if err != nil {
		return ObjectInfo{}, err
	}
	return ObjectInfo{Size: attrs.Size}, nil
}

func (s *GCSStore) OpenRange(ctx context.Context, uri string, offset, length int64) (io.ReadCloser, error) {
	if err := storageuri.ValidateRange(offset, length); err != nil {
		return nil, err
	}
	bucket, key, err := storageuri.Parse(uri, "gs")
	if err != nil {
		return nil, err
	}
	return s.client.Bucket(bucket).Object(key).NewRangeReader(ctx, offset, length)
}

func (s *GCSStore) Create(ctx context.Context, uri string) (io.WriteCloser, error) {
	bucket, key, err := storageuri.Parse(uri, "gs")
	if err != nil {
		return nil, err
	}
	return s.client.Bucket(bucket).Object(key).NewWriter(ctx), nil
}

func (s *GCSStore) Glob(ctx context.Context, pattern string) ([]string, error) {
	bucket, keyPattern, err := storageuri.Parse(pattern, "gs")
	if err != nil {
		return nil, err
	}
	prefix := keyPattern
	if index := strings.IndexAny(prefix, "*?["); index >= 0 {
		prefix = prefix[:index]
	}
	query := &storage.Query{Prefix: prefix}
	it := s.client.Bucket(bucket).Objects(ctx, query)
	result := make([]string, 0)
	for {
		object, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, err
		}
		matched, matchErr := path.Match(keyPattern, object.Name)
		if matchErr != nil {
			return nil, matchErr
		}
		if matched {
			result = append(result, "gs://"+bucket+"/"+object.Name)
		}
	}
	return result, nil
}

func (s *GCSStore) Close() error {
	if s == nil || s.client == nil {
		return nil
	}
	return s.client.Close()
}
