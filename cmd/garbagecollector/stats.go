package main

import (
	"bytes"
	"context"
	"encoding/json"
	"sort"
	"strings"
	"time"
	"tizbac/pmoxs3backuproxy/internal/s3backuplog"
	"tizbac/pmoxs3backuproxy/internal/s3pmoxcommon"

	"github.com/minio/minio-go/v7"
)

// snapshotOfIndex maps backups/<id>|<time>|<type>/<archive> to its snapshot.
func snapshotOfIndex(indexKey string) string {
	parts := strings.Split(indexKey, "/")
	if len(parts) < 3 {
		return indexKey
	}
	return parts[0] + "/" + parts[1]
}

// computeUsageStats turns the maps the collector already built into the report.
//
// knownChunks maps a chunk digest to the index objects referencing it, with
// repetitions when an index points at the same chunk several times, so the
// snapshots of a digest are deduplicated here.
func computeUsageStats(
	knownChunks map[string][]string,
	chunkSizes map[string]uint64,
	archiveSizes map[string]uint64,
) s3pmoxcommon.UsageStats {
	perSnapshot := make(map[string]*s3pmoxcommon.SnapshotUsage)
	get := func(name string) *s3pmoxcommon.SnapshotUsage {
		u, ok := perSnapshot[name]
		if !ok {
			u = &s3pmoxcommon.SnapshotUsage{Snapshot: name}
			perSnapshot[name] = u
		}
		return u
	}

	for indexKey, size := range archiveSizes {
		get(snapshotOfIndex(indexKey)).LogicalBytes += size
	}

	stats := s3pmoxcommon.UsageStats{Generated: time.Now().Unix()}

	for digest, indexes := range knownChunks {
		size := chunkSizes[digest]
		stats.Chunks++
		stats.ChunkBytes += size

		owners := make(map[string]bool, len(indexes))
		for _, indexKey := range indexes {
			owners[snapshotOfIndex(indexKey)] = true
		}
		for owner := range owners {
			u := get(owner)
			u.ReferencedBytes += size
			u.Chunks++
			if len(owners) == 1 {
				u.ExclusiveBytes += size
			}
		}
	}

	stats.Snapshots = make([]s3pmoxcommon.SnapshotUsage, 0, len(perSnapshot))
	for _, u := range perSnapshot {
		stats.LogicalBytes += u.LogicalBytes
		stats.Snapshots = append(stats.Snapshots, *u)
	}
	sort.Slice(stats.Snapshots, func(i, j int) bool {
		return stats.Snapshots[i].Snapshot < stats.Snapshots[j].Snapshot
	})

	return stats
}

// writeUsageStats stores the report in the bucket and logs a summary.
//
// A failure here is never fatal: the collection itself succeeded, and a stale
// or missing report only means the proxy has no deduplication figures to show.
func writeUsageStats(
	ctx context.Context,
	c *minio.Client,
	bucket string,
	knownChunks map[string][]string,
	chunkSizes map[string]uint64,
	archiveSizes map[string]uint64,
) {
	stats := computeUsageStats(knownChunks, chunkSizes, archiveSizes)

	var exclusive uint64
	for _, s := range stats.Snapshots {
		exclusive += s.ExclusiveBytes
	}
	ratio := float64(0)
	if stats.ChunkBytes > 0 {
		ratio = float64(stats.LogicalBytes) / float64(stats.ChunkBytes)
	}
	s3backuplog.InfoPrint(
		"Usage: %d snapshots, %d bytes of logical data stored in %d chunks of %d bytes (deduplication factor %.1f), %d bytes held exclusively by a single snapshot",
		len(stats.Snapshots), stats.LogicalBytes, stats.Chunks, stats.ChunkBytes, ratio, exclusive,
	)

	payload, err := json.Marshal(stats)
	if err != nil {
		s3backuplog.WarnPrint("Unable to encode the usage report: %s", err.Error())
		return
	}
	_, err = c.PutObject(
		ctx, bucket, s3pmoxcommon.UsageStatsObject,
		bytes.NewReader(payload), int64(len(payload)),
		minio.PutObjectOptions{ContentType: "application/json"},
	)
	if err != nil {
		s3backuplog.WarnPrint("Unable to store the usage report: %s", err.Error())
		return
	}
	s3backuplog.InfoPrint("Usage report written to %s", s3pmoxcommon.UsageStatsObject)
}
