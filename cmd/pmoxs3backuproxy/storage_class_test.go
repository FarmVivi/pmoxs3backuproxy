package main

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

func TestPutOptionsStorageClassIsOptIn(t *testing.T) {
	defaultOpts := putOptions("", nil)
	if defaultOpts.StorageClass != "" {
		t.Fatalf("default storage class = %q, want provider default", defaultOpts.StorageClass)
	}

	metadata := map[string]string{"csum": "abc"}
	opts := putOptions("STANDARD_IA", metadata)
	if opts.StorageClass != "STANDARD_IA" {
		t.Fatalf("storage class = %q, want STANDARD_IA", opts.StorageClass)
	}
	if opts.UserMetadata["csum"] != "abc" {
		t.Fatalf("checksum metadata was not preserved: %#v", opts.UserMetadata)
	}
}

func TestIndexedCopyOptionsPreserveHistoricalDefault(t *testing.T) {
	opts := indexedCopyOptions("bucket", "indexed/abc.fidx", "abc", "")
	if opts.Bucket != "bucket" || opts.Object != "indexed/abc.fidx" {
		t.Fatalf("unexpected destination: %#v", opts)
	}
	if opts.ReplaceMetadata || opts.UserMetadata != nil {
		t.Fatalf("default path changed copy metadata semantics: %#v", opts)
	}
}

func TestIndexedCopyOptionsSetClassAndChecksum(t *testing.T) {
	opts := indexedCopyOptions("bucket", "indexed/abc.fidx", "abc", "STANDARD")
	if !opts.ReplaceMetadata {
		t.Fatal("storage class copy must replace metadata to send the S3 header")
	}
	if opts.UserMetadata["x-amz-storage-class"] != "STANDARD" {
		t.Fatalf("storage class metadata = %#v", opts.UserMetadata)
	}
	if opts.UserMetadata["csum"] != "abc" {
		t.Fatalf("checksum metadata was lost: %#v", opts.UserMetadata)
	}
}

func TestIndexedCopySendsStorageClassHeader(t *testing.T) {
	requestHeaders := make(chan http.Header, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestHeaders <- r.Header.Clone()
		w.Header().Set("Content-Type", "application/xml")
		_, _ = io.WriteString(w, "<CopyObjectResult><ETag>etag</ETag><LastModified>2026-08-21T00:00:00Z</LastModified></CopyObjectResult>")
	}))
	defer server.Close()

	client, err := minio.New(strings.TrimPrefix(server.URL, "http://"), &minio.Options{
		Creds:  credentials.NewStaticV4("access", "secret", ""),
		Secure: false,
		Region: "us-east-1",
	})
	if err != nil {
		t.Fatalf("create client: %v", err)
	}

	_, err = client.CopyObject(
		context.Background(),
		indexedCopyOptions("bucket", "indexed/abc.fidx", "abc", "STANDARD_IA"),
		minio.CopySrcOptions{Bucket: "bucket", Object: "backups/snapshot/disk.fidx"},
	)
	if err != nil {
		t.Fatalf("copy index: %v", err)
	}

	headers := <-requestHeaders
	if got := headers.Get("x-amz-storage-class"); got != "STANDARD_IA" {
		t.Fatalf("x-amz-storage-class = %q, want STANDARD_IA", got)
	}
	if got := headers.Get("x-amz-meta-csum"); got != "abc" {
		t.Fatalf("x-amz-meta-csum = %q, want abc", got)
	}
	if got := headers.Get("x-amz-metadata-directive"); got != "REPLACE" {
		t.Fatalf("x-amz-metadata-directive = %q, want REPLACE", got)
	}
}
