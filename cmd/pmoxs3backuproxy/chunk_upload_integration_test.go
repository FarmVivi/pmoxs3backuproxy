package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

type countingChunkStore struct {
	client *minio.Client
	puts   atomic.Int64
}

func (s *countingChunkStore) StatObject(
	ctx context.Context,
	bucket string,
	object string,
	opts minio.StatObjectOptions,
) (minio.ObjectInfo, error) {
	return s.client.StatObject(ctx, bucket, object, opts)
}

func (s *countingChunkStore) PutObject(
	ctx context.Context,
	bucket string,
	object string,
	r io.Reader,
	size int64,
	opts minio.PutObjectOptions,
) (minio.UploadInfo, error) {
	s.puts.Add(1)
	return s.client.PutObject(ctx, bucket, object, r, size, opts)
}

func TestIntegrationConcurrentChunkUpload(t *testing.T) {
	endpoint := os.Getenv("S3TEST_ENDPOINT")
	bucket := os.Getenv("S3TEST_BUCKET")
	access := os.Getenv("S3TEST_ACCESSKEY")
	secret := os.Getenv("S3TEST_SECRETKEY")
	if endpoint == "" || bucket == "" || access == "" || secret == "" {
		t.Skip("S3TEST_ENDPOINT, S3TEST_BUCKET, S3TEST_ACCESSKEY and S3TEST_SECRETKEY are required")
	}
	secure, err := strconv.ParseBool(defaultString(os.Getenv("S3TEST_SECURE"), "false"))
	if err != nil {
		t.Fatalf("parse S3TEST_SECURE: %v", err)
	}
	client, err := minio.New(endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(access, secret, ""),
		Secure: secure,
	})
	if err != nil {
		t.Fatalf("create MinIO client: %v", err)
	}

	body := bytes.Repeat([]byte("concurrent duplicate chunk\n"), 160000)
	digestSeed := append(bytes.Clone(body), []byte(time.Now().UTC().Format(time.RFC3339Nano))...)
	digestBytes := sha256.Sum256(digestSeed)
	digest := hex.EncodeToString(digestBytes[:])
	request := chunkRequest{
		Digest:      digest,
		EncodedSize: int64(len(body)),
		Size:        4 << 20,
		WriterID:    1,
		ObjectName:  "chunks/" + digest[0:2] + "/" + digest[2:4] + "/" + digest[4:],
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	bucketExists, err := client.BucketExists(ctx, bucket)
	if err != nil {
		t.Fatalf("check integration bucket: %v", err)
	}
	createdBucket := false
	if !bucketExists {
		if err := client.MakeBucket(ctx, bucket, minio.MakeBucketOptions{}); err != nil {
			t.Fatalf("create integration bucket: %v", err)
		}
		createdBucket = true
	}
	client.RemoveObject(ctx, bucket, request.ObjectName, minio.RemoveObjectOptions{})
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cleanupCancel()
		if err := client.RemoveObject(cleanupCtx, bucket, request.ObjectName, minio.RemoveObjectOptions{}); err != nil {
			t.Errorf("remove integration chunk: %v", err)
		}
		if createdBucket {
			if err := client.RemoveBucket(cleanupCtx, bucket); err != nil {
				t.Errorf("remove integration bucket: %v", err)
			}
		}
	})

	store := &countingChunkStore{client: client}
	var flights chunkFlightGroup
	const workers = 64
	start := make(chan struct{})
	results := make(chan struct {
		known bool
		err   error
	}, workers)
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			reader := bytes.NewReader(body)
			known, err := flights.Do(
				ctx,
				bucket+"/"+request.ObjectName,
				func() (bool, error) {
					return storeChunk(ctx, store, bucket, request, reader, "")
				},
				func() error {
					return drainChunkBody(reader, request.EncodedSize)
				},
			)
			results <- struct {
				known bool
				err   error
			}{known, err}
		}()
	}
	close(start)
	wg.Wait()
	close(results)

	newChunks := 0
	for result := range results {
		if result.err != nil {
			t.Fatalf("concurrent upload failed: %v", result.err)
		}
		if !result.known {
			newChunks++
		}
	}
	if newChunks != 1 {
		t.Fatalf("reported %d new chunks, want exactly one", newChunks)
	}
	if got := store.puts.Load(); got != 1 {
		t.Fatalf("issued %d S3 PUTs, want exactly one", got)
	}

	object, err := client.GetObject(ctx, bucket, request.ObjectName, minio.GetObjectOptions{})
	if err != nil {
		t.Fatalf("get uploaded chunk: %v", err)
	}
	defer object.Close()
	stored, err := io.ReadAll(object)
	if err != nil {
		t.Fatalf("read uploaded chunk: %v", err)
	}
	if !bytes.Equal(stored, body) {
		t.Fatalf("stored chunk differs: got %d bytes, want %d", len(stored), len(body))
	}
}

func defaultString(value string, fallback string) string {
	if value != "" {
		return value
	}
	return fallback
}
