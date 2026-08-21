package main

import (
	"sync"
	"tizbac/pmoxs3backuproxy/internal/s3backuplog"
	"tizbac/pmoxs3backuproxy/internal/s3pmoxcommon"
)

var cacheInvalidations = struct {
	sync.Mutex
	directory string
	endpoint  string
	seen      map[string]string
}{seen: make(map[string]string)}

func configureCacheInvalidations(directory string, endpoint string) {
	cacheInvalidations.Lock()
	defer cacheInvalidations.Unlock()
	cacheInvalidations.directory = directory
	cacheInvalidations.endpoint = endpoint
}

func invalidateDataStoreCachesLocal(datastore string) {
	snapshotListCache.Invalidate(datastore)
	datastoreUsageCache.Expire(datastore)
	usageStatsCache.Invalidate(datastore)
	archiveSizeCache.InvalidatePrefix(datastore + "/")
	manifestCryptModeCache.InvalidatePrefix(datastore + "/")
}

// publishDataStoreCacheInvalidation is called after every terminal mutation,
// including incomplete backup cleanup. The local invalidation is immediate;
// the optional file token also reaches a collector/proxy in another process.
func publishDataStoreCacheInvalidation(datastore string) {
	invalidateDataStoreCachesLocal(datastore)

	cacheInvalidations.Lock()
	directory := cacheInvalidations.directory
	endpoint := cacheInvalidations.endpoint
	cacheInvalidations.Unlock()
	token, err := s3pmoxcommon.PublishCacheInvalidation(
		directory, endpoint, datastore,
	)
	if err != nil {
		s3backuplog.WarnPrint("Unable to publish cache invalidation for [%s]: %s", datastore, err)
		return
	}
	if token != "" {
		cacheInvalidations.Lock()
		cacheInvalidations.seen[datastore] = token
		cacheInvalidations.Unlock()
	}
}

// syncDataStoreCacheInvalidation costs one local file read and no S3 request.
func syncDataStoreCacheInvalidation(datastore string) {
	cacheInvalidations.Lock()
	directory := cacheInvalidations.directory
	endpoint := cacheInvalidations.endpoint
	seen := cacheInvalidations.seen[datastore]
	cacheInvalidations.Unlock()
	token, err := s3pmoxcommon.ReadCacheInvalidation(
		directory, endpoint, datastore,
	)
	if err != nil {
		s3backuplog.WarnPrint("Unable to read cache invalidation for [%s]: %s", datastore, err)
		return
	}
	if token == "" || token == seen {
		return
	}
	cacheInvalidations.Lock()
	cacheInvalidations.seen[datastore] = token
	cacheInvalidations.Unlock()
	invalidateDataStoreCachesLocal(datastore)
	s3backuplog.DebugPrint("Invalidated caches for [%s] from shared token", datastore)
}
