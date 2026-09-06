package main

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"math"
	"strings"
	"time"
	"tizbac/pmoxs3backuproxy/internal/s3backuplog"
	"tizbac/pmoxs3backuproxy/internal/s3pmoxcommon"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/tags"
)

type objectLister interface {
	ListObjects(context.Context, string, minio.ListObjectsOptions) <-chan minio.ObjectInfo
}

type objectRemover interface {
	RemoveObjects(context.Context, string, <-chan minio.ObjectInfo, minio.RemoveObjectsOptions) <-chan minio.RemoveObjectError
}

type objectTagReader interface {
	GetObjectTagging(context.Context, string, string, minio.GetObjectTaggingOptions) (*tags.Tags, error)
}

// isDirectoryMarker reports whether an object is the empty placeholder an S3
// browser writes when someone "creates a folder".
//
// S3 has no directories, so consoles emulate them with a zero-byte object whose
// key ends in a slash. Such a key can never be a snapshot, an index or a chunk,
// and refusing to parse it would abort the whole run: one click in a web
// console would stop garbage collection until a human noticed.
func isDirectoryMarker(object minio.ObjectInfo) bool {
	return object.Size == 0 && strings.HasSuffix(object.Key, "/")
}

func listObjectsFully(
	ctx context.Context,
	store objectLister,
	bucket string,
	opts minio.ListObjectsOptions,
) ([]minio.ObjectInfo, error) {
	objects := make([]minio.ObjectInfo, 0)
	for object := range store.ListObjects(ctx, bucket, opts) {
		if object.Err != nil {
			return nil, fmt.Errorf("list %q under prefix %q: %w", bucket, opts.Prefix, object.Err)
		}
		if isDirectoryMarker(object) {
			s3backuplog.DebugPrint("Ignoring directory marker %s", object.Key)
			continue
		}
		objects = append(objects, object)
	}
	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("list %q under prefix %q: %w", bucket, opts.Prefix, err)
	}
	return objects, nil
}

func removeObjectsFully(
	ctx context.Context,
	store objectRemover,
	bucket string,
	objects []minio.ObjectInfo,
) error {
	if len(objects) == 0 {
		return nil
	}
	objectsCh := make(chan minio.ObjectInfo)
	go func() {
		defer close(objectsCh)
		for _, object := range objects {
			select {
			case objectsCh <- object:
			case <-ctx.Done():
				return
			}
		}
	}()

	var removeErrors []error
	for result := range store.RemoveObjects(ctx, bucket, objectsCh, minio.RemoveObjectsOptions{}) {
		removeErrors = append(removeErrors, fmt.Errorf("remove %s: %w", result.ObjectName, result.Err))
	}
	if err := ctx.Err(); err != nil {
		removeErrors = append(removeErrors, err)
	}
	return errors.Join(removeErrors...)
}

type parsedIndex struct {
	checksum    string
	digests     []string
	logicalSize uint64
}

func parseIndex(key string, data []byte, metadataChecksum string) (parsedIndex, error) {
	if len(data) < 4096 {
		return parsedIndex{}, fmt.Errorf("index %s is too small: %d bytes", key, len(data))
	}
	payload := data[4096:]
	if err := compareSum(data[32:64], payload, metadataChecksum); err != nil {
		return parsedIndex{}, fmt.Errorf("index %s: %w", key, err)
	}

	record := parsedIndex{checksum: metadataChecksum}
	switch {
	case strings.HasSuffix(key, ".fidx"):
		if !bytes.Equal(data[0:8], s3pmoxcommon.PROXMOX_INDEX_MAGIC_FIXED[:]) {
			return parsedIndex{}, fmt.Errorf("fixed index %s has wrong magic", key)
		}
		if len(payload)%32 != 0 {
			return parsedIndex{}, fmt.Errorf("fixed index %s payload is not 32-byte aligned", key)
		}
		record.logicalSize = binary.LittleEndian.Uint64(data[64:72])
		record.digests = make([]string, 0, len(payload)/32)
		for offset := 0; offset < len(payload); offset += 32 {
			record.digests = append(record.digests, hex.EncodeToString(payload[offset:offset+32]))
		}
	case strings.HasSuffix(key, ".didx"):
		if !bytes.Equal(data[0:8], s3pmoxcommon.PROXMOX_INDEX_MAGIC_DYNAMIC[:]) {
			return parsedIndex{}, fmt.Errorf("dynamic index %s has wrong magic", key)
		}
		if len(payload)%40 != 0 {
			return parsedIndex{}, fmt.Errorf("dynamic index %s payload is not 40-byte aligned", key)
		}
		record.digests = make([]string, 0, len(payload)/40)
		for offset := 0; offset < len(payload); offset += 40 {
			record.logicalSize = binary.LittleEndian.Uint64(payload[offset : offset+8])
			record.digests = append(record.digests, hex.EncodeToString(payload[offset+8:offset+40]))
		}
	default:
		return parsedIndex{}, fmt.Errorf("unsupported index object %s", key)
	}
	return record, nil
}

type gcPlan struct {
	expiredSnapshots []s3pmoxcommon.Snapshot
	backupObjects    []minio.ObjectInfo
	indexedObjects   []minio.ObjectInfo
	chunkObjects     []minio.ObjectInfo
	knownChunks      map[string][]string
	existingChunks   map[string]bool
	chunkSizes       map[string]uint64
	archiveSizes     map[string]uint64
	missingChunks    map[string][]string
	protectedByGrace uint64
}

type indexLoader func(context.Context, minio.ObjectInfo) (parsedIndex, error)
type checksumLoader func(context.Context, minio.ObjectInfo) (string, error)

// refreshExpiredSnapshotProtection reads the authoritative tag from each
// expired, complete snapshot. S3's standard ListObjects response does not
// guarantee object tags, so trusting ObjectInfo.UserTags could delete a backup
// that the user explicitly protected.
func refreshExpiredSnapshotProtection(
	ctx context.Context,
	store objectTagReader,
	bucket string,
	snapshots []s3pmoxcommon.Snapshot,
	backupObjects []minio.ObjectInfo,
	now time.Time,
	retentionDays uint64,
) error {
	manifests := make(map[string]bool)
	for _, object := range backupObjects {
		if strings.HasSuffix(object.Key, "/index.json.blob") {
			prefix, err := snapshotPrefixForObject(object.Key)
			if err != nil {
				return err
			}
			manifests[prefix] = true
		}
	}
	for index := range snapshots {
		expired, err := snapshotExpired(snapshots[index], now, retentionDays)
		if err != nil {
			return err
		}
		if !expired || !manifests[snapshots[index].S3Prefix()] {
			continue
		}
		objectName := snapshots[index].S3Prefix() + "/index.json.blob"
		objectTags, err := store.GetObjectTagging(ctx, bucket, objectName, minio.GetObjectTaggingOptions{})
		if err != nil {
			return fmt.Errorf("read protection tag on %s: %w", objectName, err)
		}
		snapshots[index].Protected = objectTags.ToMap()["protected"] == "true"
	}
	return nil
}

func hoursDuration(hours uint) (time.Duration, error) {
	if uint64(hours) > uint64(math.MaxInt64/int64(time.Hour)) {
		return 0, fmt.Errorf("grace period %d hours overflows time.Duration", hours)
	}
	return time.Duration(hours) * time.Hour, nil
}

func snapshotExpired(snapshot s3pmoxcommon.Snapshot, now time.Time, retentionDays uint64) (bool, error) {
	if retentionDays > math.MaxUint64/86400 {
		return false, fmt.Errorf("retention period %d days overflows seconds", retentionDays)
	}
	retentionSeconds := retentionDays * 86400
	nowUnix := uint64(now.Unix())
	if now.Unix() < 0 || nowUnix < retentionSeconds {
		return false, nil
	}
	return snapshot.BackupTime < nowUnix-retentionSeconds, nil
}

// executeGCPlan deliberately orders deletes from least to most dangerous.
// In particular, no chunk is removed after a failure deleting snapshot or
// copied-index objects.
func executeGCPlan(ctx context.Context, store objectRemover, bucket string, plan gcPlan) error {
	if err := removeObjectsFully(ctx, store, bucket, plan.backupObjects); err != nil {
		return fmt.Errorf("remove expired snapshot objects: %w", err)
	}
	if err := removeObjectsFully(ctx, store, bucket, plan.indexedObjects); err != nil {
		return fmt.Errorf("remove orphaned copied indexes: %w", err)
	}
	if err := removeObjectsFully(ctx, store, bucket, plan.chunkObjects); err != nil {
		return fmt.Errorf("remove orphaned chunks: %w", err)
	}
	return nil
}

func snapshotPrefixForObject(key string) (string, error) {
	if !strings.HasPrefix(key, "backups/") {
		return "", fmt.Errorf("object %q is outside backups/", key)
	}
	rest := strings.TrimPrefix(key, "backups/")
	slash := strings.IndexByte(rest, '/')
	if slash <= 0 {
		return "", fmt.Errorf("invalid backup object key %q", key)
	}
	return "backups/" + rest[:slash], nil
}

func chunkDigestFromKey(key string) (string, error) {
	parts := strings.Split(key, "/")
	if len(parts) != 4 || parts[0] != "chunks" ||
		len(parts[1]) != 2 || len(parts[2]) != 2 || len(parts[3]) != 60 {
		return "", fmt.Errorf("invalid chunk object key %q", key)
	}
	digest := parts[1] + parts[2] + parts[3]
	decoded, err := hex.DecodeString(digest)
	if err != nil || len(decoded) != 32 {
		return "", fmt.Errorf("invalid chunk digest in key %q", key)
	}
	return digest, nil
}

func buildGCPlan(
	ctx context.Context,
	snapshots []s3pmoxcommon.Snapshot,
	backups []minio.ObjectInfo,
	indexed []minio.ObjectInfo,
	chunks []minio.ObjectInfo,
	now time.Time,
	retentionDays uint64,
	grace time.Duration,
	loadIndex indexLoader,
	loadChecksum checksumLoader,
) (gcPlan, error) {
	plan := gcPlan{
		knownChunks:    make(map[string][]string),
		existingChunks: make(map[string]bool),
		chunkSizes:     make(map[string]uint64),
		archiveSizes:   make(map[string]uint64),
		missingChunks:  make(map[string][]string),
	}

	expiredPrefixes := make(map[string]bool)
	for _, snapshot := range snapshots {
		expired, err := snapshotExpired(snapshot, now, retentionDays)
		if err != nil {
			return gcPlan{}, err
		}
		if expired && !snapshot.Protected {
			plan.expiredSnapshots = append(plan.expiredSnapshots, snapshot)
			expiredPrefixes[snapshot.S3Prefix()] = true
		}
	}

	knownHashes := make(map[string]bool)
	for _, object := range backups {
		prefix, err := snapshotPrefixForObject(object.Key)
		if err != nil {
			return gcPlan{}, err
		}
		if expiredPrefixes[prefix] {
			plan.backupObjects = append(plan.backupObjects, object)
			continue
		}
		if !strings.HasSuffix(object.Key, ".fidx") && !strings.HasSuffix(object.Key, ".didx") {
			continue
		}
		index, err := loadIndex(ctx, object)
		if err != nil {
			return gcPlan{}, err
		}
		if strings.HasSuffix(object.Key, ".fidx") {
			knownHashes[index.checksum] = true
		}
		plan.archiveSizes[object.Key] = index.logicalSize
		for _, digest := range index.digests {
			plan.knownChunks[digest] = append(plan.knownChunks[digest], object.Key)
		}
	}

	for _, object := range indexed {
		checksum, err := loadChecksum(ctx, object)
		if err != nil {
			return gcPlan{}, err
		}
		if !knownHashes[checksum] {
			plan.indexedObjects = append(plan.indexedObjects, object)
		}
	}

	for _, object := range chunks {
		digest, err := chunkDigestFromKey(object.Key)
		if err != nil {
			return gcPlan{}, err
		}
		if _, referenced := plan.knownChunks[digest]; referenced {
			plan.existingChunks[digest] = true
			plan.chunkSizes[digest] = uint64(object.Size)
			continue
		}
		if withinGracePeriod(object.LastModified, now, grace) {
			plan.protectedByGrace++
			continue
		}
		plan.chunkObjects = append(plan.chunkObjects, object)
	}

	for digest, references := range plan.knownChunks {
		if !plan.existingChunks[digest] {
			plan.missingChunks[digest] = references
		}
	}
	return plan, nil
}

// loadIndexFromS3 downloads an index object and validates it against the csum
// stored in its user metadata.
//
// The metadata is taken from the listing when the backend supplied it, and
// otherwise from the GET response itself: a GET carries the same x-amz-meta-*
// headers as a HEAD, and minio-go caches them, so Stat() after the body has
// been read is served from memory. Backends without the MinIO 
// listing extension therefore cost one request per index here instead of two.
func loadIndexFromS3(
	ctx context.Context,
	client *minio.Client,
	bucket string,
	object minio.ObjectInfo,
) (parsedIndex, error) {
	reader, err := client.GetObject(ctx, bucket, object.Key, minio.GetObjectOptions{})
	if err != nil {
		return parsedIndex{}, fmt.Errorf("get index %s: %w", object.Key, err)
	}
	defer reader.Close()
	data, err := io.ReadAll(reader)
	if err != nil {
		return parsedIndex{}, fmt.Errorf("read index %s: %w", object.Key, err)
	}

	checksum := checksumFromMetadata(object)
	if checksum == "" {
		// Stat() must come after the body has been read: on an untouched
		// object minio-go answers it with a separate StatObject call.
		info, statErr := reader.Stat()
		if statErr != nil {
			return parsedIndex{}, fmt.Errorf("stat index %s: %w", object.Key, statErr)
		}
		checksum = checksumFromMetadata(info)
	}
	if checksum == "" {
		return parsedIndex{}, fmt.Errorf("%s: object has no csum metadata flag set", object.Key)
	}

	return parseIndex(object.Key, data, checksum)
}
