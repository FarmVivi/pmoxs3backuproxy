package main

import (
	"testing"
	"time"
	"tizbac/pmoxs3backuproxy/internal/s3pmoxcommon"
)

func initialiseInvalidationTestCaches() {
	snapshotListCache = newTTLCache[[]s3pmoxcommon.Snapshot](time.Hour)
	datastoreUsageCache = newTTLCache[DataStoreUsage](time.Hour)
	usageStatsCache = newTTLCache[[]byte](time.Hour)
	archiveSizeCache = newTTLCache[uint64](time.Hour)
	manifestCryptModeCache = newTTLCache[map[string]string](time.Hour)
}

func TestInvalidateDataStoreCachesLocalCoversEveryDerivedView(t *testing.T) {
	initialiseInvalidationTestCaches()
	ds := "bucket"
	snapshotListCache.Store(ds, []s3pmoxcommon.Snapshot{{BackupID: "old"}})
	datastoreUsageCache.Store(ds, DataStoreUsage{Bytes: 1})
	usageStatsCache.Store(ds, []byte("old"))
	archiveSizeCache.Store(ds+"/backups/old/disk.fidx", 1)
	manifestCryptModeCache.Store(ds+"/backups/old/index.json.blob", map[string]string{"disk.fidx": "none"})
	archiveSizeCache.Store("other/backups/keep/disk.fidx", 7)

	invalidateDataStoreCachesLocal(ds)

	assertRefetched := func(name string, fetch func() (bool, error)) {
		t.Helper()
		fresh, err := fetch()
		if err != nil || !fresh {
			t.Fatalf("%s was not invalidated: fresh=%v err=%v", name, fresh, err)
		}
	}
	assertRefetched("snapshots", func() (bool, error) {
		_, fresh, err := snapshotListCache.Get(ds, func() ([]s3pmoxcommon.Snapshot, error) { return nil, nil })
		return fresh, err
	})
	assertRefetched("usage", func() (bool, error) {
		_, fresh, err := datastoreUsageCache.Get(ds, func() (DataStoreUsage, error) { return DataStoreUsage{}, nil })
		return fresh, err
	})
	assertRefetched("GC stats", func() (bool, error) {
		_, fresh, err := usageStatsCache.Get(ds, func() ([]byte, error) { return nil, nil })
		return fresh, err
	})
	assertRefetched("archive size", func() (bool, error) {
		_, fresh, err := archiveSizeCache.Get(ds+"/backups/old/disk.fidx", func() (uint64, error) { return 2, nil })
		return fresh, err
	})
	assertRefetched("encryption", func() (bool, error) {
		_, fresh, err := manifestCryptModeCache.Get(ds+"/backups/old/index.json.blob", func() (map[string]string, error) { return nil, nil })
		return fresh, err
	})
	if value, fresh, err := archiveSizeCache.Get("other/backups/keep/disk.fidx", func() (uint64, error) {
		t.Fatal("unrelated datastore cache was invalidated")
		return 0, nil
	}); err != nil || fresh || value != 7 {
		t.Fatalf("unrelated datastore value=%d fresh=%v err=%v", value, fresh, err)
	}
}

func TestSharedTokenInvalidatesProxyCaches(t *testing.T) {
	initialiseInvalidationTestCaches()
	directory := t.TempDir()
	configureCacheInvalidations(directory, "s3.example:443")
	ds := "bucket"
	snapshotListCache.Store(ds, []s3pmoxcommon.Snapshot{{Files: []s3pmoxcommon.SnapshotFile{{Filename: "old"}}}})
	if _, err := s3pmoxcommon.PublishCacheInvalidation(directory, "s3.example:443", ds); err != nil {
		t.Fatalf("collector publish: %v", err)
	}
	syncDataStoreCacheInvalidation(ds)
	_, fresh, err := snapshotListCache.Get(ds, func() ([]s3pmoxcommon.Snapshot, error) {
		return []s3pmoxcommon.Snapshot{{Files: []s3pmoxcommon.SnapshotFile{{Filename: "new"}}}}, nil
	})
	if err != nil || !fresh {
		t.Fatalf("shared token did not invalidate snapshots: fresh=%v err=%v", fresh, err)
	}
}
