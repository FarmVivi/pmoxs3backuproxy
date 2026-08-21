package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"
	"os"
	"strconv"
	"testing"
	"time"
	"tizbac/pmoxs3backuproxy/internal/s3pmoxcommon"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/minio/minio-go/v7/pkg/tags"
)

// TestIntegrationGCSweep uses its own disposable bucket and exercises the
// complete mark/sweep path against a real S3 implementation. Normal offline
// test runs skip it unless the same S3TEST_* variables as CI are provided.
func TestIntegrationGCSweep(t *testing.T) {
	endpoint := os.Getenv("S3TEST_ENDPOINT")
	access := os.Getenv("S3TEST_ACCESSKEY")
	secret := os.Getenv("S3TEST_SECRETKEY")
	if endpoint == "" || access == "" || secret == "" {
		t.Skip("S3TEST_ENDPOINT, S3TEST_ACCESSKEY and S3TEST_SECRETKEY are required")
	}
	secure, _ := strconv.ParseBool(os.Getenv("S3TEST_SECURE"))
	client, err := minio.New(endpoint, &minio.Options{
		Creds: credentials.NewStaticV4(access, secret, ""), Secure: secure,
		BucketLookup: s3pmoxcommon.GetLookupType("path"),
	})
	if err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()
	bucket := fmt.Sprintf("gc-integration-%d", time.Now().UnixNano())
	if err := client.MakeBucket(ctx, bucket, minio.MakeBucketOptions{}); err != nil {
		t.Fatalf("create disposable bucket: %v", err)
	}
	t.Cleanup(func() {
		objects, listErr := listObjectsFully(ctx, client, bucket, minio.ListObjectsOptions{Recursive: true})
		if listErr == nil {
			_ = removeObjectsFully(ctx, client, bucket, objects)
		}
		_ = client.RemoveBucket(ctx, bucket)
	})

	shared := sha256.Sum256([]byte("shared integration chunk"))
	expiredOnly := sha256.Sum256([]byte("expired integration chunk"))
	protectedOnly := sha256.Sum256([]byte("protected integration chunk"))
	now := time.Now()
	retainedPrefix := fmt.Sprintf("backups/100|%d|vm", now.Unix())
	expiredPrefix := "backups/200|1|vm"
	protectedPrefix := "backups/300|1|vm"

	putIndex := func(key string, digest [32]byte) {
		t.Helper()
		data, checksum := makeIndex(t, false, digest)
		_, err := client.PutObject(ctx, bucket, key, bytes.NewReader(data), int64(len(data)), minio.PutObjectOptions{
			UserMetadata: map[string]string{"csum": checksum},
		})
		if err != nil {
			t.Fatalf("put %s: %v", key, err)
		}
	}
	putIndex(retainedPrefix+"/disk.fidx", shared)
	putIndex(expiredPrefix+"/disk.fidx", expiredOnly)
	putIndex(protectedPrefix+"/disk.fidx", protectedOnly)
	for _, prefix := range []string{retainedPrefix, expiredPrefix, protectedPrefix} {
		if _, err := client.PutObject(ctx, bucket, prefix+"/index.json.blob", bytes.NewReader(nil), 0, minio.PutObjectOptions{}); err != nil {
			t.Fatalf("put manifest %s: %v", prefix, err)
		}
	}
	protectedTags, err := tags.NewTags(map[string]string{"protected": "true"}, false)
	if err != nil {
		t.Fatal(err)
	}
	if err := client.PutObjectTagging(ctx, bucket, protectedPrefix+"/index.json.blob", protectedTags, minio.PutObjectTaggingOptions{}); err != nil {
		t.Fatalf("protect snapshot: %v", err)
	}
	for digest, body := range map[string]string{
		formatDigest(shared):        "shared",
		formatDigest(expiredOnly):   "expired",
		formatDigest(protectedOnly): "protected",
	} {
		object := chunkObject(digest, time.Time{}, int64(len(body)))
		if _, err := client.PutObject(ctx, bucket, object.Key, bytes.NewBufferString(body), int64(len(body)), minio.PutObjectOptions{}); err != nil {
			t.Fatalf("put %s: %v", object.Key, err)
		}
	}

	backups, err := listObjectsFully(ctx, client, bucket, minio.ListObjectsOptions{
		Prefix: "backups/", Recursive: true, WithMetadata: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	snapshots, err := s3pmoxcommon.SnapshotsFromObjects(backups, bucket, true)
	if err != nil {
		t.Fatal(err)
	}
	if err := refreshExpiredSnapshotProtection(ctx, client, bucket, snapshots, backups, now, 1); err != nil {
		t.Fatalf("refresh protection: %v", err)
	}
	chunks, err := listObjectsFully(ctx, client, bucket, minio.ListObjectsOptions{Prefix: "chunks/", Recursive: true})
	if err != nil {
		t.Fatal(err)
	}
	plan, err := buildGCPlan(ctx, snapshots, backups, nil, chunks, now, 1, 0,
		func(ctx context.Context, object minio.ObjectInfo) (parsedIndex, error) {
			return loadIndexFromS3(ctx, client, bucket, object)
		}, func(context.Context, minio.ObjectInfo) (string, error) { return "", nil })
	if err != nil {
		t.Fatalf("build plan: %v", err)
	}
	if len(plan.missingChunks) != 0 || len(plan.expiredSnapshots) != 1 || len(plan.chunkObjects) != 1 {
		t.Fatalf("unexpected plan: expired=%d chunks=%d missing=%+v", len(plan.expiredSnapshots), len(plan.chunkObjects), plan.missingChunks)
	}
	if err := executeGCPlan(ctx, client, bucket, plan); err != nil {
		t.Fatalf("execute plan: %v", err)
	}

	if _, err := client.StatObject(ctx, bucket, retainedPrefix+"/disk.fidx", minio.StatObjectOptions{}); err != nil {
		t.Fatalf("retained index was removed: %v", err)
	}
	if _, err := client.StatObject(ctx, bucket, chunkObject(formatDigest(shared), time.Time{}, 0).Key, minio.StatObjectOptions{}); err != nil {
		t.Fatalf("shared retained chunk was removed: %v", err)
	}
	if _, err := client.StatObject(ctx, bucket, protectedPrefix+"/disk.fidx", minio.StatObjectOptions{}); err != nil {
		t.Fatalf("protected index was removed: %v", err)
	}
	if _, err := client.StatObject(ctx, bucket, chunkObject(formatDigest(protectedOnly), time.Time{}, 0).Key, minio.StatObjectOptions{}); err != nil {
		t.Fatalf("protected chunk was removed: %v", err)
	}
	if _, err := client.StatObject(ctx, bucket, expiredPrefix+"/disk.fidx", minio.StatObjectOptions{}); minio.ToErrorResponse(err).Code != "NoSuchKey" {
		t.Fatalf("expired index still exists or unexpected error: %v", err)
	}
	if _, err := client.StatObject(ctx, bucket, chunkObject(formatDigest(expiredOnly), time.Time{}, 0).Key, minio.StatObjectOptions{}); minio.ToErrorResponse(err).Code != "NoSuchKey" {
		t.Fatalf("expired-only chunk still exists or unexpected error: %v", err)
	}

	// Cancellation/forget cleanup must stop exactly at the snapshot boundary,
	// even when another prefix starts with the same characters.
	deleteTarget := s3pmoxcommon.Snapshot{BackupID: "400", BackupTime: 1, BackupType: "vm", Datastore: bucket}
	similarPrefix := deleteTarget.S3Prefix() + "-extra/file"
	for _, key := range []string{deleteTarget.S3Prefix() + "/file", similarPrefix} {
		if _, err := client.PutObject(ctx, bucket, key, bytes.NewReader(nil), 0, minio.PutObjectOptions{}); err != nil {
			t.Fatalf("put deletion-boundary fixture: %v", err)
		}
	}
	if err := deleteTarget.Delete(client); err != nil {
		t.Fatalf("delete exact snapshot: %v", err)
	}
	if _, err := client.StatObject(ctx, bucket, similarPrefix, minio.StatObjectOptions{}); err != nil {
		t.Fatalf("snapshot deletion crossed its prefix boundary: %v", err)
	}
}
