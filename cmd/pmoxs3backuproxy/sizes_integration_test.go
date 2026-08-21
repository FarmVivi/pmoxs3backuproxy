package main

import (
	"os"
	"testing"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"tizbac/pmoxs3backuproxy/internal/s3pmoxcommon"
)

// Opt-in integration test against a real endpoint. It is skipped unless the
// four variables below are set, so the normal test run stays offline:
//
//	S3TEST_ENDPOINT=s3.example.net:443 S3TEST_BUCKET=... \
//	S3TEST_ACCESSKEY=... S3TEST_SECRETKEY=... go test ./cmd/pmoxs3backuproxy/ -run Integration -v
//
// It answers the only question a unit test cannot: whether the accounting
// shortcut is actually accepted and answered by the endpoint in use, and
// whether the figure it returns agrees with counting the objects one by one.
func newIntegrationClient(t *testing.T) (TicketEntry, string) {
	t.Helper()

	endpoint := os.Getenv("S3TEST_ENDPOINT")
	bucket := os.Getenv("S3TEST_BUCKET")
	access := os.Getenv("S3TEST_ACCESSKEY")
	secret := os.Getenv("S3TEST_SECRETKEY")
	if endpoint == "" || bucket == "" || access == "" || secret == "" {
		t.Skip("S3TEST_ENDPOINT, S3TEST_BUCKET, S3TEST_ACCESSKEY and S3TEST_SECRETKEY are required")
	}

	client, err := minio.New(endpoint, &minio.Options{
		Creds:        credentials.NewStaticV4(access, secret, ""),
		Secure:       true,
		BucketLookup: s3pmoxcommon.GetLookupType("auto"),
	})
	if err != nil {
		t.Fatalf("unable to create the client: %v", err)
	}

	return TicketEntry{
		AccessKeyID:     access,
		SecretAccessKey: secret,
		Endpoint:        endpoint,
		Client:          client,
	}, bucket
}

func TestIntegrationBucketHeaderUsage(t *testing.T) {
	C, bucket := newIntegrationClient(t)

	usage, ok := usageFromBucketHeaders(C, true, bucket)
	if !ok {
		t.Skip("this endpoint does not report bucket accounting headers, the listing walk is used instead")
	}
	t.Logf("bucket headers report %d objects and %d bytes", usage.Objects, usage.Bytes)

	if usage.Bytes == 0 {
		t.Fatal("the endpoint reported a size of zero for a bucket that holds backups")
	}

	walked, err := walkDataStoreUsage(C.Client, bucket)
	if err != nil {
		t.Fatalf("walking the bucket failed: %v", err)
	}
	t.Logf("walking the bucket found %d objects and %d bytes", walked.Objects, walked.Bytes)

	if usage.Bytes != walked.Bytes {
		t.Errorf("bucket headers report %d bytes, walking the bucket found %d", usage.Bytes, walked.Bytes)
	}
	if usage.Objects != walked.Objects {
		t.Errorf("bucket headers report %d objects, walking the bucket found %d", usage.Objects, walked.Objects)
	}
}

func TestIntegrationArchiveSizes(t *testing.T) {
	C, bucket := newIntegrationClient(t)

	snapshots, err := s3pmoxcommon.ListSnapshots(*C.Client, bucket, false)
	if err != nil {
		t.Fatalf("unable to list snapshots: %v", err)
	}
	if len(snapshots) == 0 {
		t.Skip("no snapshot in this bucket")
	}

	archiveSizeCache = newTTLCache[uint64](0)
	FillArchiveSizes(C.Client, snapshots)

	sized := 0
	for _, s := range snapshots {
		if s.Size > 0 {
			sized++
		}
	}
	t.Logf("%d snapshots out of %d report a size, largest first snapshot: %s = %d bytes",
		sized, len(snapshots), snapshots[0].S3Prefix(), snapshots[0].Size)

	if sized == 0 {
		t.Fatal("no snapshot got a size, the index headers were not read correctly")
	}

	// A guest disk is a whole number of mebibytes; a size that is not is a
	// sign the wrong offset was read out of the index header.
	for _, s := range snapshots {
		if s.Size != 0 && s.Size%(1024*1024) != 0 {
			t.Errorf("%s has a size of %d bytes, which is not a whole number of MiB", s.S3Prefix(), s.Size)
		}
	}

}
