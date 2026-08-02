package storage

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"path"
	"strings"
	"sync"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	smithyhttp "github.com/aws/smithy-go/transport/http"
	"github.com/guywaldman/golars/internal/storageuri"
)

// S3Store adapts an AWS S3-compatible object store to the scan/sink API.
type S3Store struct {
	client  *s3.Client
	mu      sync.RWMutex
	clients map[string]*s3.Client
}

func NewS3Store(ctx context.Context, options ...func(*s3.Options)) (*S3Store, error) {
	config, err := awsconfig.LoadDefaultConfig(ctx)
	if err != nil {
		return nil, err
	}
	return newS3Store(s3.NewFromConfig(config, options...)), nil
}

func NewS3StoreWithClient(client *s3.Client) (*S3Store, error) {
	if client == nil {
		return nil, fmt.Errorf("golars: nil S3 client")
	}
	return newS3Store(client), nil
}

func newS3Store(client *s3.Client) *S3Store {
	return &S3Store{client: client, clients: make(map[string]*s3.Client)}
}

func (s *S3Store) clientFor(bucket string) *s3.Client {
	s.mu.RLock()
	client := s.clients[bucket]
	s.mu.RUnlock()
	if client != nil {
		return client
	}
	return s.client
}

func (s *S3Store) clientForRegion(bucket, region string) *s3.Client {
	s.mu.Lock()
	defer s.mu.Unlock()
	if client := s.clients[bucket]; client != nil {
		return client
	}
	options := s.client.Options()
	options.Region = region
	client := s3.New(options)
	s.clients[bucket] = client
	return client
}

func withS3Client[T any](s *S3Store, bucket string, operation func(*s3.Client) (T, error)) (T, error) {
	result, err := operation(s.clientFor(bucket))
	if region := redirectRegion(err); region != "" {
		result, err = operation(s.clientForRegion(bucket, region))
	}
	return result, err
}

func redirectRegion(err error) string {
	if err == nil {
		return ""
	}
	var responseErr interface {
		HTTPStatusCode() int
		HTTPResponse() *smithyhttp.Response
	}
	if !errors.As(err, &responseErr) || responseErr.HTTPStatusCode() != http.StatusMovedPermanently {
		return ""
	}
	return strings.TrimSpace(responseErr.HTTPResponse().Header.Get("x-amz-bucket-region"))
}

func (s *S3Store) Open(ctx context.Context, uri string) (io.ReadCloser, error) {
	bucket, key, err := storageuri.Parse(uri, "s3")
	if err != nil {
		return nil, err
	}
	object, err := withS3Client(s, bucket, func(client *s3.Client) (*s3.GetObjectOutput, error) {
		return client.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(bucket), Key: aws.String(key)})
	})
	if err != nil {
		return nil, err
	}
	return object.Body, nil
}

func (s *S3Store) Stat(ctx context.Context, uri string) (ObjectInfo, error) {
	bucket, key, err := storageuri.Parse(uri, "s3")
	if err != nil {
		return ObjectInfo{}, err
	}
	object, err := withS3Client(s, bucket, func(client *s3.Client) (*s3.HeadObjectOutput, error) {
		return client.HeadObject(ctx, &s3.HeadObjectInput{Bucket: aws.String(bucket), Key: aws.String(key)})
	})
	if err != nil {
		return ObjectInfo{}, err
	}
	if object.ContentLength == nil {
		return ObjectInfo{}, fmt.Errorf("golars: S3 object %q has no content length", uri)
	}
	return ObjectInfo{Size: *object.ContentLength}, nil
}

func (s *S3Store) OpenRange(ctx context.Context, uri string, offset, length int64) (io.ReadCloser, error) {
	if err := storageuri.ValidateRange(offset, length); err != nil {
		return nil, err
	}
	bucket, key, err := storageuri.Parse(uri, "s3")
	if err != nil {
		return nil, err
	}
	end := offset + length - 1
	rangeValue := fmt.Sprintf("bytes=%d-%d", offset, end)
	object, err := withS3Client(s, bucket, func(client *s3.Client) (*s3.GetObjectOutput, error) {
		return client.GetObject(ctx, &s3.GetObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(key),
			Range:  aws.String(rangeValue),
		})
	})
	if err != nil {
		return nil, err
	}
	return object.Body, nil
}

func (s *S3Store) Create(ctx context.Context, uri string) (io.WriteCloser, error) {
	bucket, key, err := storageuri.Parse(uri, "s3")
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
	bucket, keyPattern, err := storageuri.Parse(pattern, "s3")
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
		page, err := withS3Client(s, bucket, func(client *s3.Client) (*s3.ListObjectsV2Output, error) {
			return client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{Bucket: aws.String(bucket), Prefix: aws.String(prefix), ContinuationToken: token})
		})
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
