package main

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"strings"
	"testing"
	"time"
	"tizbac/pmoxs3backuproxy/internal/s3pmoxcommon"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/tags"
)

type fakeLister struct {
	objects []minio.ObjectInfo
}

type fakeTagReader struct {
	values map[string]map[string]string
	err    error
	calls  []string
}

func (f *fakeTagReader) GetObjectTagging(_ context.Context, _ string, object string, _ minio.GetObjectTaggingOptions) (*tags.Tags, error) {
	f.calls = append(f.calls, object)
	if f.err != nil {
		return nil, f.err
	}
	return tags.NewTags(f.values[object], false)
}

func (f fakeLister) ListObjects(context.Context, string, minio.ListObjectsOptions) <-chan minio.ObjectInfo {
	ch := make(chan minio.ObjectInfo, len(f.objects))
	for _, object := range f.objects {
		ch <- object
	}
	close(ch)
	return ch
}

func TestListObjectsFullyRejectsPartialListing(t *testing.T) {
	wantErr := errors.New("page two unavailable")
	objects, err := listObjectsFully(context.Background(), fakeLister{objects: []minio.ObjectInfo{
		{Key: "backups/100|10|vm/a.fidx"},
		{Err: wantErr},
	}}, "bucket", minio.ListObjectsOptions{Prefix: "backups/"})
	if !errors.Is(err, wantErr) {
		t.Fatalf("got err %v, want %v", err, wantErr)
	}
	if objects != nil {
		t.Fatalf("partial listing escaped: %+v", objects)
	}
}

func makeIndex(t *testing.T, dynamic bool, digests ...[32]byte) ([]byte, string) {
	t.Helper()
	recordSize := 32
	if dynamic {
		recordSize = 40
	}
	data := make([]byte, 4096+recordSize*len(digests))
	if dynamic {
		copy(data[:8], s3pmoxcommon.PROXMOX_INDEX_MAGIC_DYNAMIC[:])
		for i, digest := range digests {
			binary.LittleEndian.PutUint64(data[4096+i*40:], uint64((i+1)*1024))
			copy(data[4096+i*40+8:], digest[:])
		}
	} else {
		copy(data[:8], s3pmoxcommon.PROXMOX_INDEX_MAGIC_FIXED[:])
		binary.LittleEndian.PutUint64(data[64:72], uint64(len(digests))*1024)
		for i, digest := range digests {
			copy(data[4096+i*32:], digest[:])
		}
	}
	sum := sha256.Sum256(data[4096:])
	copy(data[32:64], sum[:])
	return data, strings.Repeat("", 0) + formatDigest(sum)
}

func formatDigest(digest [32]byte) string {
	const hexDigits = "0123456789abcdef"
	result := make([]byte, 64)
	for i, value := range digest {
		result[2*i] = hexDigits[value>>4]
		result[2*i+1] = hexDigits[value&0xf]
	}
	return string(result)
}

func TestParseIndexVariantsAndValidation(t *testing.T) {
	digestA := sha256.Sum256([]byte("a"))
	digestB := sha256.Sum256([]byte("b"))

	fixedData, fixedChecksum := makeIndex(t, false, digestA, digestB)
	fixed, err := parseIndex("disk.fidx", fixedData, fixedChecksum)
	if err != nil || fixed.logicalSize != 2048 || len(fixed.digests) != 2 || fixed.digests[0] != formatDigest(digestA) {
		t.Fatalf("fixed=%+v err=%v", fixed, err)
	}

	dynamicData, dynamicChecksum := makeIndex(t, true, digestA, digestB)
	dynamic, err := parseIndex("archive.didx", dynamicData, dynamicChecksum)
	if err != nil || dynamic.logicalSize != 2048 || len(dynamic.digests) != 2 {
		t.Fatalf("dynamic=%+v err=%v", dynamic, err)
	}

	emptyData, emptyChecksum := makeIndex(t, true)
	empty, err := parseIndex("empty.didx", emptyData, emptyChecksum)
	if err != nil || empty.logicalSize != 0 || len(empty.digests) != 0 {
		t.Fatalf("empty dynamic=%+v err=%v", empty, err)
	}

	tests := []struct {
		name string
		key  string
		data []byte
		csum string
	}{
		{"short", "x.fidx", make([]byte, 10), ""},
		{"unsupported", "x.blob", fixedData, fixedChecksum},
		{"bad checksum", "x.fidx", fixedData, strings.Repeat("0", 64)},
		{"unaligned fixed", "x.fidx", append(append([]byte(nil), fixedData...), 0), fixedChecksum},
		{"unaligned dynamic", "x.didx", append(append([]byte(nil), dynamicData...), 0), dynamicChecksum},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := parseIndex(tc.key, tc.data, tc.csum); err == nil {
				t.Fatal("invalid index accepted")
			}
		})
	}
}

func chunkObject(digest string, modified time.Time, size int64) minio.ObjectInfo {
	return minio.ObjectInfo{
		Key:          "chunks/" + digest[:2] + "/" + digest[2:4] + "/" + digest[4:],
		LastModified: modified,
		Size:         size,
	}
}

func TestBuildGCPlanKeepsEveryRetainedReference(t *testing.T) {
	now := time.Unix(2_000_000_000, 0)
	old := uint64(now.Add(-60 * 24 * time.Hour).Unix())
	current := uint64(now.Add(-time.Hour).Unix())
	shared := formatDigest(sha256.Sum256([]byte("shared")))
	expiredOnly := formatDigest(sha256.Sum256([]byte("expired")))
	protectedOnly := formatDigest(sha256.Sum256([]byte("protected")))
	orphanOld := formatDigest(sha256.Sum256([]byte("orphan-old")))
	orphanRecent := formatDigest(sha256.Sum256([]byte("orphan-recent")))

	snapshots := []s3pmoxcommon.Snapshot{
		{BackupID: "expired", BackupTime: old, BackupType: "vm"},
		{BackupID: "protected", BackupTime: old, BackupType: "vm", Protected: true},
		{BackupID: "current", BackupTime: current, BackupType: "vm"},
	}
	backupObjects := []minio.ObjectInfo{
		{Key: snapshots[0].S3Prefix() + "/disk.fidx"},
		{Key: snapshots[0].S3Prefix() + "/index.json.blob"},
		{Key: snapshots[1].S3Prefix() + "/disk.fidx"},
		{Key: snapshots[2].S3Prefix() + "/disk.fidx"},
	}
	indexes := map[string]parsedIndex{
		backupObjects[0].Key: {checksum: "expired-index", digests: []string{shared, expiredOnly}},
		backupObjects[2].Key: {checksum: "protected-index", digests: []string{shared, protectedOnly}},
		backupObjects[3].Key: {checksum: "current-index", digests: []string{shared}},
	}
	indexedObjects := []minio.ObjectInfo{
		{Key: "indexed/kept", UserMetadata: map[string]string{"checksum": "current-index"}},
		{Key: "indexed/orphan", UserMetadata: map[string]string{"checksum": "orphan-index"}},
	}
	chunks := []minio.ObjectInfo{
		chunkObject(shared, now.Add(-48*time.Hour), 100),
		chunkObject(expiredOnly, now.Add(-48*time.Hour), 200),
		chunkObject(protectedOnly, now.Add(-48*time.Hour), 300),
		chunkObject(orphanOld, now.Add(-48*time.Hour), 400),
		chunkObject(orphanRecent, now.Add(-time.Hour), 500),
	}

	plan, err := buildGCPlan(context.Background(), snapshots, backupObjects, indexedObjects, chunks,
		now, 45, 24*time.Hour,
		func(_ context.Context, object minio.ObjectInfo) (parsedIndex, error) {
			index, ok := indexes[object.Key]
			if !ok {
				return parsedIndex{}, errors.New("unexpected index")
			}
			return index, nil
		},
		func(_ context.Context, object minio.ObjectInfo) (string, error) {
			return object.UserMetadata["checksum"], nil
		})
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.expiredSnapshots) != 1 || len(plan.backupObjects) != 2 {
		t.Fatalf("expired snapshots=%d objects=%d", len(plan.expiredSnapshots), len(plan.backupObjects))
	}
	if len(plan.indexedObjects) != 1 || plan.indexedObjects[0].Key != "indexed/orphan" {
		t.Fatalf("copied index plan=%+v", plan.indexedObjects)
	}
	if len(plan.chunkObjects) != 2 {
		t.Fatalf("chunk deletes=%+v, want expired-only and old orphan", plan.chunkObjects)
	}
	deleted := map[string]bool{}
	for _, object := range plan.chunkObjects {
		deleted[object.Key] = true
	}
	if !deleted[chunkObject(expiredOnly, time.Time{}, 0).Key] || !deleted[chunkObject(orphanOld, time.Time{}, 0).Key] {
		t.Fatalf("wrong chunks selected: %+v", deleted)
	}
	if deleted[chunkObject(shared, time.Time{}, 0).Key] || deleted[chunkObject(protectedOnly, time.Time{}, 0).Key] {
		t.Fatalf("referenced chunks selected: %+v", deleted)
	}
	if plan.protectedByGrace != 1 || len(plan.missingChunks) != 0 {
		t.Fatalf("grace=%d missing=%+v", plan.protectedByGrace, plan.missingChunks)
	}
}

func TestRefreshExpiredSnapshotProtectionUsesAuthoritativeTags(t *testing.T) {
	now := time.Unix(2_000_000_000, 0)
	snapshots := []s3pmoxcommon.Snapshot{
		{BackupID: "old", BackupTime: 1, BackupType: "vm"},
		{BackupID: "current", BackupTime: uint64(now.Unix()), BackupType: "vm"},
		{BackupID: "incomplete", BackupTime: 1, BackupType: "vm"},
	}
	manifest := snapshots[0].S3Prefix() + "/index.json.blob"
	objects := []minio.ObjectInfo{
		{Key: manifest},
		{Key: snapshots[1].S3Prefix() + "/index.json.blob"},
		{Key: snapshots[2].S3Prefix() + "/disk.fidx"},
	}
	store := &fakeTagReader{values: map[string]map[string]string{
		manifest: {"protected": "true"},
	}}
	if err := refreshExpiredSnapshotProtection(context.Background(), store, "bucket", snapshots, objects, now, 45); err != nil {
		t.Fatal(err)
	}
	if !snapshots[0].Protected {
		t.Fatal("authoritative protected tag was ignored")
	}
	if len(store.calls) != 1 || store.calls[0] != manifest {
		t.Fatalf("tag calls=%+v, want only the expired complete snapshot", store.calls)
	}

	wantErr := errors.New("tagging unavailable")
	store = &fakeTagReader{err: wantErr}
	if err := refreshExpiredSnapshotProtection(context.Background(), store, "bucket", snapshots[:1], objects[:1], now, 45); !errors.Is(err, wantErr) {
		t.Fatalf("got %v, want %v", err, wantErr)
	}
}

func TestBuildGCPlanFailsBeforeSweepOnUnsafeInputs(t *testing.T) {
	now := time.Now()
	snapshot := s3pmoxcommon.Snapshot{BackupID: "100", BackupTime: uint64(now.Unix()), BackupType: "vm"}
	backup := minio.ObjectInfo{Key: snapshot.S3Prefix() + "/disk.fidx"}
	loadErr := errors.New("index unavailable")
	_, err := buildGCPlan(context.Background(), []s3pmoxcommon.Snapshot{snapshot}, []minio.ObjectInfo{backup}, nil, nil,
		now, 45, 24*time.Hour,
		func(context.Context, minio.ObjectInfo) (parsedIndex, error) { return parsedIndex{}, loadErr },
		func(context.Context, minio.ObjectInfo) (string, error) { return "", nil })
	if !errors.Is(err, loadErr) {
		t.Fatalf("got %v, want loader error", err)
	}

	_, err = buildGCPlan(context.Background(), nil, nil, nil,
		[]minio.ObjectInfo{{Key: "chunks/not/a-valid-digest"}}, now, 45, 24*time.Hour,
		func(context.Context, minio.ObjectInfo) (parsedIndex, error) { return parsedIndex{}, nil },
		func(context.Context, minio.ObjectInfo) (string, error) { return "", nil })
	if err == nil {
		t.Fatal("malformed chunk key accepted")
	}
}

func TestBuildGCPlanReportsMissingReferencedChunk(t *testing.T) {
	now := time.Now()
	digest := formatDigest(sha256.Sum256([]byte("missing")))
	snapshot := s3pmoxcommon.Snapshot{BackupID: "100", BackupTime: uint64(now.Unix()), BackupType: "vm"}
	object := minio.ObjectInfo{Key: snapshot.S3Prefix() + "/disk.fidx"}
	plan, err := buildGCPlan(context.Background(), []s3pmoxcommon.Snapshot{snapshot}, []minio.ObjectInfo{object}, nil, nil,
		now, 45, 24*time.Hour,
		func(context.Context, minio.ObjectInfo) (parsedIndex, error) {
			return parsedIndex{checksum: "index", digests: []string{digest}}, nil
		}, func(context.Context, minio.ObjectInfo) (string, error) { return "", nil })
	if err != nil || len(plan.missingChunks[digest]) != 1 || len(plan.chunkObjects) != 0 {
		t.Fatalf("plan=%+v err=%v", plan, err)
	}
}

type fakeRemover struct {
	calls      [][]string
	failOnCall int
}

func (f *fakeRemover) RemoveObjects(_ context.Context, _ string, objects <-chan minio.ObjectInfo, _ minio.RemoveObjectsOptions) <-chan minio.RemoveObjectError {
	keys := make([]string, 0)
	for object := range objects {
		keys = append(keys, object.Key)
	}
	f.calls = append(f.calls, keys)
	result := make(chan minio.RemoveObjectError, 1)
	if len(f.calls) == f.failOnCall {
		result <- minio.RemoveObjectError{ObjectName: keys[0], Err: errors.New("delete failed")}
	}
	close(result)
	return result
}

func TestExecuteGCPlanStopsBeforeChunkSweepAfterEarlierFailure(t *testing.T) {
	store := &fakeRemover{failOnCall: 2}
	plan := gcPlan{
		backupObjects:  []minio.ObjectInfo{{Key: "backups/old/file"}},
		indexedObjects: []minio.ObjectInfo{{Key: "indexed/orphan"}},
		chunkObjects:   []minio.ObjectInfo{{Key: "chunks/aa/bb/" + strings.Repeat("c", 60)}},
	}
	if err := executeGCPlan(context.Background(), store, "bucket", plan); err == nil {
		t.Fatal("delete failure ignored")
	}
	if len(store.calls) != 2 {
		t.Fatalf("got %d delete phases, chunk phase must not run: %+v", len(store.calls), store.calls)
	}
}

func TestDurationAndRetentionOverflowGuards(t *testing.T) {
	if got, err := hoursDuration(24); err != nil || got != 24*time.Hour {
		t.Fatalf("duration=%s err=%v", got, err)
	}
	maxHours := uint64((1<<63 - 1) / int64(time.Hour))
	if uint64(^uint(0)) > maxHours {
		if _, err := hoursDuration(uint(maxHours + 1)); err == nil {
			t.Fatal("overflowing duration accepted")
		}
	}
	if _, err := snapshotExpired(s3pmoxcommon.Snapshot{}, time.Now(), ^uint64(0)); err == nil {
		t.Fatal("overflowing retention accepted")
	}
}
