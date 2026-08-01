package golars

import (
	"context"
	"fmt"
	"io"
	"path"
	"strings"

	"cloud.google.com/go/storage"
	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
)

// S3Store adapts an AWS S3-compatible object store to the scan/sink API.
type S3Store struct{ client *s3.Client }

func NewS3Store(ctx context.Context, options ...func(*s3.Options)) (*S3Store, error) {
	config, err := awsconfig.LoadDefaultConfig(ctx)
	if err != nil {
		return nil, err
	}
	return &S3Store{client: s3.NewFromConfig(config, options...)}, nil
}
func NewS3StoreWithClient(client *s3.Client) (*S3Store, error) {
	if client == nil {
		return nil, fmt.Errorf("golars: nil S3 client")
	}
	return &S3Store{client: client}, nil
}

func (s *S3Store) Open(ctx context.Context, uri string) (io.ReadCloser, error) {
	bucket, key, err := parseObjectURI(uri, "s3")
	if err != nil {
		return nil, err
	}
	object, err := s.client.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(bucket), Key: aws.String(key)})
	if err != nil {
		return nil, err
	}
	return object.Body, nil
}

func (s *S3Store) Create(ctx context.Context, uri string) (io.WriteCloser, error) {
	bucket, key, err := parseObjectURI(uri, "s3")
	if err != nil {
		return nil, err
	}
	reader, writer := io.Pipe()
	done := make(chan error, 1)
	go func() {
		_, putErr := s.client.PutObject(ctx, &s3.PutObjectInput{Bucket: aws.String(bucket), Key: aws.String(key), Body: reader})
		done <- putErr
		_ = reader.CloseWithError(putErr)
	}()
	return &s3WriteCloser{writer: writer, done: done}, nil
}

func (s *S3Store) Glob(ctx context.Context, pattern string) ([]string, error) {
	bucket, keyPattern, err := parseObjectURI(pattern, "s3")
	if err != nil {
		return nil, err
	}
	prefix := keyPattern
	if index := strings.IndexAny(prefix, "*?["); index >= 0 {
		prefix = prefix[:index]
	}
	result := make([]string, 0)
	var token *string
	for {
		page, err := s.client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{Bucket: aws.String(bucket), Prefix: aws.String(prefix), ContinuationToken: token})
		if err != nil {
			return nil, err
		}
		for _, object := range page.Contents {
			if object.Key == nil {
				continue
			}
			matched, matchErr := path.Match(keyPattern, *object.Key)
			if matchErr != nil {
				return nil, matchErr
			}
			if matched {
				result = append(result, "s3://"+bucket+"/"+*object.Key)
			}
		}
		if !aws.ToBool(page.IsTruncated) || page.NextContinuationToken == nil {
			break
		}
		token = page.NextContinuationToken
	}
	return result, nil
}

type s3WriteCloser struct {
	writer *io.PipeWriter
	done   chan error
	closed bool
}

func (w *s3WriteCloser) Write(data []byte) (int, error) { return w.writer.Write(data) }
func (w *s3WriteCloser) Close() error {
	if w.closed {
		return nil
	}
	w.closed = true
	if err := w.writer.Close(); err != nil {
		return err
	}
	return <-w.done
}

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
	bucket, key, err := parseObjectURI(uri, "gs")
	if err != nil {
		return nil, err
	}
	return s.client.Bucket(bucket).Object(key).NewReader(ctx)
}
func (s *GCSStore) Create(ctx context.Context, uri string) (io.WriteCloser, error) {
	bucket, key, err := parseObjectURI(uri, "gs")
	if err != nil {
		return nil, err
	}
	return s.client.Bucket(bucket).Object(key).NewWriter(ctx), nil
}
func (s *GCSStore) Glob(ctx context.Context, pattern string) ([]string, error) {
	bucket, keyPattern, err := parseObjectURI(pattern, "gs")
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

func parseObjectURI(uri, scheme string) (string, string, error) {
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
