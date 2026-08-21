package main

import (
	"testing"

	"tizbac/pmoxs3backuproxy/internal/s3pmoxcommon"
)

func snapshotsForTest() []s3pmoxcommon.Snapshot {
	return []s3pmoxcommon.Snapshot{
		{BackupID: "100", BackupTime: 1000, BackupType: "vm", Size: 8 << 30},
		{BackupID: "100", BackupTime: 2000, BackupType: "vm", Size: 8 << 30},
		// Taken after the last collector run, so absent from the report.
		{BackupID: "100", BackupTime: 3000, BackupType: "vm", Size: 8 << 30},
	}
}

func statsForTest() *s3pmoxcommon.UsageStats {
	return &s3pmoxcommon.UsageStats{
		Snapshots: []s3pmoxcommon.SnapshotUsage{
			{Snapshot: "backups/100|1000|vm", LogicalBytes: 8 << 30, ReferencedBytes: 6 << 30, ExclusiveBytes: 5 << 30},
			{Snapshot: "backups/100|2000|vm", LogicalBytes: 8 << 30, ReferencedBytes: 6 << 30, ExclusiveBytes: 1 << 28},
		},
	}
}

func TestApplySnapshotSizeModeLogicalLeavesSizesAlone(t *testing.T) {
	snapshots := snapshotsForTest()
	ApplySnapshotSizeMode(SnapshotSizeLogical, snapshots, statsForTest())

	for _, s := range snapshots {
		if s.Size != 8<<30 {
			t.Errorf("%s has size %d, want the logical size untouched", s.S3Prefix(), s.Size)
		}
	}
}

func TestApplySnapshotSizeModeReferenced(t *testing.T) {
	snapshots := snapshotsForTest()
	ApplySnapshotSizeMode(SnapshotSizeReferenced, snapshots, statsForTest())

	if snapshots[0].Size != 6<<30 || snapshots[1].Size != 6<<30 {
		t.Errorf("got %d and %d, want both at %d", snapshots[0].Size, snapshots[1].Size, uint64(6)<<30)
	}
}

func TestApplySnapshotSizeModeExclusive(t *testing.T) {
	snapshots := snapshotsForTest()
	ApplySnapshotSizeMode(SnapshotSizeExclusive, snapshots, statsForTest())

	if snapshots[0].Size != 5<<30 {
		t.Errorf("first snapshot has size %d, want %d", snapshots[0].Size, uint64(5)<<30)
	}
	// The incremental one only holds what it added, which is the whole point
	// of showing the exclusive figure.
	if snapshots[1].Size != 1<<28 {
		t.Errorf("second snapshot has size %d, want %d", snapshots[1].Size, uint64(1)<<28)
	}
}

// A snapshot the collector has never seen must keep a plausible size rather
// than be reported as empty, which would read as a broken backup.
func TestApplySnapshotSizeModeKeepsLogicalForUnknownSnapshot(t *testing.T) {
	snapshots := snapshotsForTest()
	ApplySnapshotSizeMode(SnapshotSizeExclusive, snapshots, statsForTest())

	if snapshots[2].Size != 8<<30 {
		t.Errorf("snapshot missing from the report has size %d, want its logical size %d",
			snapshots[2].Size, uint64(8)<<30)
	}
}

// Same reasoning when no report exists at all, on a fresh datastore.
func TestApplySnapshotSizeModeWithoutReport(t *testing.T) {
	snapshots := snapshotsForTest()
	ApplySnapshotSizeMode(SnapshotSizeExclusive, snapshots, nil)

	for _, s := range snapshots {
		if s.Size != 8<<30 {
			t.Errorf("%s has size %d, want its logical size", s.S3Prefix(), s.Size)
		}
	}
}

func TestValidSnapshotSizeMode(t *testing.T) {
	for _, mode := range []string{"logical", "referenced", "exclusive"} {
		if !ValidSnapshotSizeMode(mode) {
			t.Errorf("%q rejected, want accepted", mode)
		}
	}
	for _, mode := range []string{"", "physical", "Logical", "real"} {
		if ValidSnapshotSizeMode(mode) {
			t.Errorf("%q accepted, want rejected", mode)
		}
	}
}
