package main

import (
	"testing"
	"time"
	"tizbac/pmoxs3backuproxy/internal/s3pmoxcommon"
)

func TestOptionalReportingIsDisabledByDefault(t *testing.T) {
	if reportDataStoreUsage || reportArchiveSize || reportEncryption || reportGCStats {
		t.Fatalf(
			"optional reporting defaults changed: usage=%v archive=%v encryption=%v gc=%v",
			reportDataStoreUsage, reportArchiveSize, reportEncryption, reportGCStats,
		)
	}

	// A nil client would panic as soon as one of the S3-backed enrichments was
	// called. With default options this must remain a no-op.
	snapshots := []s3pmoxcommon.Snapshot{{
		Datastore: "bucket",
		Files:     []s3pmoxcommon.SnapshotFile{{Filename: "disk.fidx", Size: 123}},
	}}
	applyOptionalSnapshotReports(nil, "bucket", snapshots)
	if snapshots[0].Files[0].CryptMode != "" || snapshots[0].Size != 0 {
		t.Fatalf("disabled reporting changed snapshot: %+v", snapshots[0])
	}
}

func TestDisabledDataStoreUsageDoesNotFetch(t *testing.T) {
	reportDataStoreUsage = false
	datastoreUsageCache = newTTLCache[DataStoreUsage](time.Hour)
	usage, known := readDataStoreUsageForStatus("bucket", func() (DataStoreUsage, error) {
		t.Fatal("disabled datastore usage issued an upstream request")
		return DataStoreUsage{}, nil
	})
	if known || usage != (DataStoreUsage{}) {
		t.Fatalf("disabled usage returned %+v, known=%v", usage, known)
	}
}
