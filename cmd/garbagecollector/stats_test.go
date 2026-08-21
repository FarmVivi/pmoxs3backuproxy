package main

import "testing"

// Two snapshots of the same guest: they share most of their chunks, each has
// one of its own. This is the shape the report has to get right, because it is
// the shape of every incremental backup chain.
func TestComputeUsageStatsSharedAndExclusiveChunks(t *testing.T) {
	const (
		older = "backups/100|1000|vm"
		newer = "backups/100|2000|vm"
	)
	oldIndex := older + "/drive-scsi0.img.fidx"
	newIndex := newer + "/drive-scsi0.img.fidx"

	knownChunks := map[string][]string{
		"aa": {oldIndex, newIndex}, // shared
		"bb": {oldIndex, newIndex}, // shared
		"cc": {oldIndex},           // only in the older snapshot
		"dd": {newIndex},           // only in the newer one
	}
	chunkSizes := map[string]uint64{"aa": 100, "bb": 200, "cc": 30, "dd": 40}
	archiveSizes := map[string]uint64{oldIndex: 8 << 30, newIndex: 8 << 30}

	stats := computeUsageStats(knownChunks, chunkSizes, archiveSizes)

	if stats.Chunks != 4 {
		t.Errorf("counted %d chunks, want 4", stats.Chunks)
	}
	if stats.ChunkBytes != 370 {
		t.Errorf("chunk bytes %d, want 370", stats.ChunkBytes)
	}
	if stats.LogicalBytes != 16<<30 {
		t.Errorf("logical bytes %d, want %d", stats.LogicalBytes, uint64(16)<<30)
	}
	if len(stats.Snapshots) != 2 {
		t.Fatalf("got %d snapshots, want 2", len(stats.Snapshots))
	}

	byName := map[string]SnapshotUsage{}
	for _, s := range stats.Snapshots {
		byName[s.Snapshot] = s
	}

	if got := byName[older].ReferencedBytes; got != 330 {
		t.Errorf("older referenced %d bytes, want 330 (100+200+30)", got)
	}
	if got := byName[older].ExclusiveBytes; got != 30 {
		t.Errorf("older exclusive %d bytes, want 30, only the chunk no one else points at", got)
	}
	if got := byName[newer].ExclusiveBytes; got != 40 {
		t.Errorf("newer exclusive %d bytes, want 40", got)
	}
	if got := byName[newer].LogicalBytes; got != 8<<30 {
		t.Errorf("newer logical %d bytes, want %d", got, uint64(8)<<30)
	}
}

// An index pointing at the same chunk several times - an all zero region of a
// disk, typically - must not have it counted several times.
func TestComputeUsageStatsCountsRepeatedDigestOnce(t *testing.T) {
	index := "backups/101|1000|vm/drive-scsi0.img.fidx"

	stats := computeUsageStats(
		map[string][]string{"aa": {index, index, index}},
		map[string]uint64{"aa": 4 << 20},
		map[string]uint64{index: 1 << 30},
	)

	if len(stats.Snapshots) != 1 {
		t.Fatalf("got %d snapshots, want 1", len(stats.Snapshots))
	}
	s := stats.Snapshots[0]
	if s.ReferencedBytes != 4<<20 {
		t.Errorf("referenced %d bytes, want %d, the chunk must be counted once", s.ReferencedBytes, uint64(4)<<20)
	}
	if s.ExclusiveBytes != 4<<20 {
		t.Errorf("exclusive %d bytes, want %d", s.ExclusiveBytes, uint64(4)<<20)
	}
	if s.Chunks != 1 {
		t.Errorf("counted %d chunks for the snapshot, want 1", s.Chunks)
	}
}

// A snapshot whose archives are all shared with others frees nothing when
// deleted, and the report must say so rather than showing its logical size.
func TestComputeUsageStatsFullyDeduplicatedSnapshot(t *testing.T) {
	a := "backups/102|1000|vm/drive-scsi0.img.fidx"
	b := "backups/102|2000|vm/drive-scsi0.img.fidx"

	stats := computeUsageStats(
		map[string][]string{"aa": {a, b}},
		map[string]uint64{"aa": 1 << 20},
		map[string]uint64{a: 1 << 30, b: 1 << 30},
	)

	for _, s := range stats.Snapshots {
		if s.ExclusiveBytes != 0 {
			t.Errorf("%s reports %d exclusive bytes, want 0", s.Snapshot, s.ExclusiveBytes)
		}
		if s.ReferencedBytes != 1<<20 {
			t.Errorf("%s references %d bytes, want %d", s.Snapshot, s.ReferencedBytes, uint64(1)<<20)
		}
	}
}

func TestSnapshotOfIndex(t *testing.T) {
	cases := map[string]string{
		"backups/100|1787184095|vm/drive-scsi0.img.fidx": "backups/100|1787184095|vm",
		"backups/ct1|1|ct/root.pxar.didx":                "backups/ct1|1|ct",
		"unexpected":                                     "unexpected",
	}
	for in, want := range cases {
		if got := snapshotOfIndex(in); got != want {
			t.Errorf("snapshotOfIndex(%q) = %q, want %q", in, got, want)
		}
	}
}
