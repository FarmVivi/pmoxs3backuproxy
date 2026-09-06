package main

import (
	"context"
	"crypto/sha256"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

func TestChecksumFromMetadataAcceptsBothHeaderForms(t *testing.T) {
	listing := minio.ObjectInfo{UserMetadata: map[string]string{"X-Amz-Meta-Csum": "from-listing"}}
	if got := checksumFromMetadata(listing); got != "from-listing" {
		t.Fatalf("listing metadata: got %q", got)
	}
	// GET and HEAD responses go through ToObjectInfo, which strips the prefix.
	response := minio.ObjectInfo{UserMetadata: map[string]string{"Csum": "from-response"}}
	if got := checksumFromMetadata(response); got != "from-response" {
		t.Fatalf("response metadata: got %q", got)
	}
	if got := checksumFromMetadata(minio.ObjectInfo{}); got != "" {
		t.Fatalf("missing metadata: got %q", got)
	}
}

// TestLoadIndexFromS3TakesChecksumFromGet pins the behaviour that motivated
// this code: on a backend that ignores the MinIO metadata listing extension,
// reading an index must cost exactly one request. The server fails any HEAD so
// that a regression cannot pass silently.
func TestLoadIndexFromS3TakesChecksumFromGet(t *testing.T) {
	digest := sha256.Sum256([]byte("chunk"))
	data, checksum := makeIndex(t, false, digest)

	var heads, gets int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodHead:
			heads++
			http.Error(w, "HEAD is not allowed in this test", http.StatusInternalServerError)
		case http.MethodGet:
			gets++
			w.Header().Set("x-amz-meta-csum", checksum)
			w.Header().Set("Content-Length", strconv.Itoa(len(data)))
			w.Header().Set("Last-Modified", time.Now().UTC().Format(http.TimeFormat))
			w.Header().Set("ETag", `"0f4d3c2b1a"`)
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write(data)
		default:
			http.Error(w, "unexpected method "+r.Method, http.StatusBadRequest)
		}
	}))
	defer server.Close()

	client, err := minio.New(strings.TrimPrefix(server.URL, "http://"), &minio.Options{
		Creds:        credentials.NewStaticV4("access", "secret", ""),
		Secure:       false,
		Region:       "us-east-1",
		BucketLookup: minio.BucketLookupPath,
	})
	if err != nil {
		t.Fatal(err)
	}

	// No UserMetadata at all: exactly what a listing returns off MinIO.
	object := minio.ObjectInfo{Key: "backups/100|1|vm/drive-scsi0.img.fidx"}
	index, err := loadIndexFromS3(context.Background(), client, "bucket", object)
	if err != nil {
		t.Fatalf("load index: %v", err)
	}
	if index.checksum != checksum {
		t.Fatalf("checksum: got %q want %q", index.checksum, checksum)
	}
	if len(index.digests) != 1 || index.digests[0] != formatDigest(digest) {
		t.Fatalf("digests: got %+v", index.digests)
	}
	if heads != 0 {
		t.Fatalf("index read issued %d HEAD request(s); the GET response already carries the metadata", heads)
	}
	if gets != 1 {
		t.Fatalf("index read issued %d GET request(s), want 1", gets)
	}
}
