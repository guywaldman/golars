//go:build e2e

package golars

import (
	"context"
	"testing"
	"time"

	"cloud.google.com/go/storage"
	aws "github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go/middleware"
	dockerclient "github.com/moby/moby/client"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"google.golang.org/api/option"
)

func TestE2ES3LocalStack(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	requireDocker(t, ctx)
	container, endpoint := runEmulator(t, ctx, "localstack/localstack:3.8.1", "4566/tcp", wait.ForHTTP("/_localstack/health").WithPort("4566/tcp"), map[string]string{"SERVICES": "s3", "AWS_DEFAULT_REGION": "us-east-1"})
	defer terminateContainer(t, container)

	config, err := awsconfig.LoadDefaultConfig(ctx,
		awsconfig.WithRegion("us-east-1"),
		awsconfig.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "test")),
		awsconfig.WithRequestChecksumCalculation(aws.RequestChecksumCalculationWhenRequired),
	)
	if err != nil {
		t.Fatal(err)
	}
	client := s3.NewFromConfig(config, func(options *s3.Options) {
		options.BaseEndpoint = aws.String(endpoint)
		options.UsePathStyle = true
		options.APIOptions = append(options.APIOptions, func(stack *middleware.Stack) error {
			if err := v4.RemoveComputePayloadSHA256Middleware(stack); err != nil {
				return err
			}
			return v4.AddUnsignedPayloadMiddleware(stack)
		})
	})
	bucket := "golars-e2e-s3"
	if _, err := client.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(bucket)}); err != nil {
		t.Fatal(err)
	}
	store, err := NewS3StoreWithClient(client)
	if err != nil {
		t.Fatal(err)
	}
	engine, err := NewEngine(WithObjectStore("s3", store), WithBatchSize(2))
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close()
	df := e2eFrame(t, engine)
	defer df.Close()
	path := "s3://" + bucket + "/events/data.parquet"
	if err := WriteParquet(ctx, df, path); err != nil {
		t.Fatal(err)
	}
	result, err := engine.ScanParquet("s3://" + bucket + "/events/*.parquet").
		Filter(Col("amount").Gt(Lit(int64(10)))).
		Select(Col("id")).
		Collect(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer result.Close()
	if result.Height() != 2 || result.Width() != 1 {
		t.Fatalf("unexpected S3 result shape: (%d,%d)", result.Height(), result.Width())
	}
}

func TestE2EGCSFakeServer(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	requireDocker(t, ctx)
	container, endpoint := runEmulator(t, ctx, "fsouza/fake-gcs-server:1.52.3", "4443/tcp", wait.ForHTTP("/storage/v1/b").WithPort("4443/tcp"), nil, "-scheme", "http")
	defer terminateContainer(t, container)
	t.Setenv("STORAGE_EMULATOR_HOST", endpoint)
	client, err := storage.NewClient(ctx, option.WithoutAuthentication(), option.WithEndpoint(endpoint+"/storage/v1/"), storage.WithJSONReads())
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	bucket := "golars-e2e-gcs"
	if err := client.Bucket(bucket).Create(ctx, "golars-e2e", nil); err != nil {
		t.Fatal(err)
	}
	store, err := NewGCSStoreWithClient(client)
	if err != nil {
		t.Fatal(err)
	}
	engine, err := NewEngine(WithObjectStore("gs", store), WithBatchSize(2))
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close()
	df := e2eFrame(t, engine)
	defer df.Close()
	path := "gs://" + bucket + "/events/data.parquet"
	if err := WriteParquet(ctx, df, path); err != nil {
		t.Fatal(err)
	}
	result, err := engine.ScanParquet("gs://" + bucket + "/events/*.parquet").
		Filter(Col("amount").Gt(Lit(int64(10)))).
		Select(Col("id")).
		Collect(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer result.Close()
	if result.Height() != 2 || result.Width() != 1 {
		t.Fatalf("unexpected GCS result shape: (%d,%d)", result.Height(), result.Width())
	}
}

func e2eFrame(t *testing.T, engine *Engine) *DataFrame {
	t.Helper()
	id, err := NewSeries("id", []int64{1, 2, 3})
	if err != nil {
		t.Fatal(err)
	}
	amount, err := NewSeries("amount", []int64{5, 15, 25})
	if err != nil {
		id.Close()
		t.Fatal(err)
	}
	df, err := newDataFrame(engine, id, amount)
	id.Close()
	amount.Close()
	if err != nil {
		t.Fatal(err)
	}
	return df
}

func requireDocker(t *testing.T, ctx context.Context) {
	t.Helper()
	checkCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	provider, err := testcontainers.NewDockerProvider()
	if err != nil {
		t.Fatalf("Docker is required for E2E tests: %v", err)
	}
	defer provider.Close()
	if _, err := provider.Client().Ping(checkCtx, dockerclient.PingOptions{}); err != nil {
		t.Fatalf("Docker is required for E2E tests: %v", err)
	}
	if _, err := provider.DaemonHost(checkCtx); err != nil {
		t.Fatalf("Docker is required for E2E tests: %v", err)
	}
}

func runEmulator(t *testing.T, ctx context.Context, image, port string, strategy wait.Strategy, env map[string]string, command ...string) (testcontainers.Container, string) {
	t.Helper()
	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        image,
			ExposedPorts: []string{port},
			Env:          env,
			Cmd:          command,
			WaitingFor:   strategy,
		},
		Started: true,
	})
	if err != nil {
		t.Fatalf("start emulator %s: %v", image, err)
	}
	endpoint, err := container.PortEndpoint(ctx, port, "http")
	if err != nil {
		_ = testcontainers.TerminateContainer(container)
		t.Fatal(err)
	}
	return container, endpoint
}

func terminateContainer(t *testing.T, container testcontainers.Container) {
	t.Helper()
	if err := testcontainers.TerminateContainer(container); err != nil {
		t.Logf("failed to terminate emulator: %v", err)
	}
}
