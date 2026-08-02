package storage

import (
	"context"
	"io"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

func TestS3StoreFollowsBucketRegionRedirect(t *testing.T) {
	var requests atomic.Int32
	transport := roundTripFunc(func(r *http.Request) (*http.Response, error) {
		if requests.Add(1) == 1 {
			return &http.Response{
				StatusCode: http.StatusMovedPermanently,
				Status:     "301 Moved Permanently",
				Header:     http.Header{"X-Amz-Bucket-Region": []string{"us-east-1"}},
				Body:       io.NopCloser(strings.NewReader("")),
				Request:    r,
			}, nil
		}
		if authorization := r.Header.Get("Authorization"); !strings.Contains(authorization, "/us-east-1/") {
			t.Errorf("request was not resigned for redirected region: %q", authorization)
		}
		return &http.Response{
			StatusCode: http.StatusOK,
			Status:     "200 OK",
			Header:     http.Header{"Content-Type": []string{"application/xml"}},
			Body: io.NopCloser(strings.NewReader(`<?xml version="1.0" encoding="UTF-8"?>
<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Name>bucket</Name><KeyCount>1</KeyCount><MaxKeys>1000</MaxKeys><IsTruncated>false</IsTruncated><Contents><Key>prefix/data.parquet</Key></Contents></ListBucketResult>`)),
			Request: r,
		}, nil
	})

	ctx := context.Background()
	config, err := awsconfig.LoadDefaultConfig(ctx,
		awsconfig.WithRegion("us-west-2"),
		awsconfig.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("access", "secret", "")),
		awsconfig.WithHTTPClient(&http.Client{Transport: transport}),
	)
	if err != nil {
		t.Fatal(err)
	}
	client := s3.NewFromConfig(config, func(options *s3.Options) {
		options.BaseEndpoint = aws.String("https://s3.test")
		options.UsePathStyle = true
		options.Retryer = aws.NopRetryer{}
	})
	store, err := NewS3StoreWithClient(client)
	if err != nil {
		t.Fatal(err)
	}

	paths, err := store.Glob(ctx, "s3://bucket/prefix/*.parquet")
	if err != nil {
		t.Fatal(err)
	}
	if want := []string{"s3://bucket/prefix/data.parquet"}; len(paths) != len(want) || paths[0] != want[0] {
		t.Fatalf("unexpected paths: %#v", paths)
	}

	if _, err := store.Glob(ctx, "s3://bucket/prefix/*.parquet"); err != nil {
		t.Fatal(err)
	}
	if got := requests.Load(); got != 3 {
		t.Fatalf("expected redirect retry plus cached request, got %d requests", got)
	}
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) {
	return f(r)
}
