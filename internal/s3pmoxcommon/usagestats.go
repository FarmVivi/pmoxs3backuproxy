package s3pmoxcommon

// UsageStatsObject is where the deduplication report lives inside the bucket.
// It is written by the garbage collector, which is the only component in a
// position to compute it, and read back by the proxy.
const UsageStatsObject = "usage-stats.json"

// SnapshotUsage describes what one snapshot represents and what it actually
// costs.
//
// The three figures answer three different questions, and confusing them is
// the usual way to misread a deduplicating store:
//
//   - Logical is the size of the guest disks or streams it contains. It is
//     what the backup restores, and the sum over all snapshots is far larger
//     than the bucket, which is precisely the point of deduplication.
//   - Referenced is the size of the chunks it points at, counting a shared
//     chunk in full. It is what the snapshot would cost on its own.
//   - Exclusive is the size of the chunks no other snapshot points at, which
//     is what deleting this snapshot actually frees.
type SnapshotUsage struct {
	Snapshot        string `json:"snapshot"`
	LogicalBytes    uint64 `json:"logical-bytes"`
	ReferencedBytes uint64 `json:"referenced-bytes"`
	ExclusiveBytes  uint64 `json:"exclusive-bytes"`
	Chunks          uint64 `json:"chunks"`
}

// UsageStats is the whole report.
type UsageStats struct {
	Generated    int64           `json:"generated"`
	ChunkBytes   uint64          `json:"chunk-bytes"`
	Chunks       uint64          `json:"chunks"`
	LogicalBytes uint64          `json:"logical-bytes"`
	Snapshots    []SnapshotUsage `json:"snapshots"`
}
