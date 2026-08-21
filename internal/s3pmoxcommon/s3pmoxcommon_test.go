package s3pmoxcommon

import (
	"encoding/base64"
	"errors"
	"testing"

	"github.com/minio/minio-go/v7"
)

func TestSnapshotsFromObjectsInterleaved(t *testing.T) {
	note := base64.RawStdEncoding.EncodeToString([]byte("important"))
	objects := []minio.ObjectInfo{
		{Key: "backups/100|10|vm/disk-a.fidx", Size: 10},
		{Key: "backups/200|20|ct/root.pxar.didx", Size: 20},
		{Key: "backups/100|10|vm/index.json.blob", Size: 30, UserTags: map[string]string{"protected": "true", "note": note}},
		{Key: "backups/200|20|ct/root.pxar.didx.csjson", Size: 40},
	}

	snapshots, err := SnapshotsFromObjects(objects, "bucket", false)
	if err != nil {
		t.Fatalf("parse snapshots: %v", err)
	}
	if len(snapshots) != 2 {
		t.Fatalf("got %d snapshots, want 2", len(snapshots))
	}
	if len(snapshots[0].Files) != 2 {
		t.Fatalf("first snapshot has %d files, want 2", len(snapshots[0].Files))
	}
	if !snapshots[0].Protected || snapshots[0].Comment != "important" {
		t.Fatalf("snapshot tags not retained: %+v", snapshots[0])
	}
	if len(snapshots[1].Files) != 1 {
		t.Fatalf("csjson helper leaked into file list: %+v", snapshots[1].Files)
	}
}

func TestSnapshotsFromObjectsCorruptedFilter(t *testing.T) {
	objects := []minio.ObjectInfo{
		{Key: "backups/100|10|vm/index.json.blob"},
		{Key: "backups/100|10|vm/corrupted"},
	}
	visible, err := SnapshotsFromObjects(objects, "bucket", false)
	if err != nil {
		t.Fatal(err)
	}
	if len(visible) != 0 {
		t.Fatalf("corrupted snapshot returned: %+v", visible)
	}
	all, err := SnapshotsFromObjects(objects, "bucket", true)
	if err != nil || len(all) != 1 {
		t.Fatalf("returnCorrupted got snapshots=%+v err=%v", all, err)
	}
}

func TestSnapshotsFromObjectsFailsClosed(t *testing.T) {
	listErr := errors.New("second page failed")
	tests := []struct {
		name    string
		objects []minio.ObjectInfo
	}{
		{"listing error", []minio.ObjectInfo{{Key: "backups/100|10|vm/a.fidx"}, {Err: listErr}}},
		{"malformed key", []minio.ObjectInfo{{Key: "backups/not-a-snapshot/file"}}},
		{"invalid timestamp", []minio.ObjectInfo{{Key: "backups/100|invalid|vm/file"}}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := SnapshotsFromObjects(tc.objects, "bucket", true); err == nil {
				t.Fatal("unsafe input was accepted")
			}
		})
	}
}

func TestSnapshotsFromObjectsIgnoresMalformedCosmeticNote(t *testing.T) {
	snapshots, err := SnapshotsFromObjects([]minio.ObjectInfo{{
		Key: "backups/100|10|vm/file", UserTags: map[string]string{"note": "!"},
	}}, "bucket", true)
	if err != nil || len(snapshots) != 1 || snapshots[0].Comment != "" {
		t.Fatalf("snapshots=%+v err=%v", snapshots, err)
	}
}
