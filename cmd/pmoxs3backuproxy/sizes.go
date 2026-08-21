/*
PMOX S3 Backup Proxy

	Copyright (C) 2024  Tiziano Bacocco
	Copyright (C) 2024  Michael Ablassmeier <abi@grinser.de>

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program.  If not, see <http://www.gnu.org/licenses/>.
*/
package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"net/http"
	"strconv"
	"sync"
	"time"
	"tizbac/pmoxs3backuproxy/internal/s3backuplog"
	"tizbac/pmoxs3backuproxy/internal/s3pmoxcommon"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/signer"
)

// archiveSizeCache maps a backup index object to the logical size of the
// archive it describes.
//
// The entries never expire in practice: an index under backups/ is written
// once and its key contains the backup timestamp, so it is immutable. The TTL
// only bounds the memory of a very long lived process.
var archiveSizeCache *ttlCache[uint64]

// manifestCryptModeCache avoids downloading immutable index.json.blob files
// again whenever the short-lived snapshot list cache refreshes.
var manifestCryptModeCache *ttlCache[map[string]string]

// datastoreUsageCache holds the result of walking a whole bucket, which is far
// too expensive to do on the PVE polling path. It is always read through
// GetAsync.
var datastoreUsageCache *ttlCache[DataStoreUsage]

// Reporting enhancements are opt-in because they issue S3 reads which the
// historical proxy did not perform. Operators of providers billing per API
// call or byte must make that trade-off explicitly.
var reportDataStoreUsage bool
var reportArchiveSize bool
var reportEncryption bool
var reportGCStats bool

// sizeLookupConcurrency bounds the parallel ranged GETs issued when sizes are
// not cached yet, so that a cold cache does not open hundreds of connections
// to the object store at once.
const sizeLookupConcurrency = 16

// bucketHeaderProbeTimeout bounds the accounting shortcut. It is deliberately
// short: failing it costs nothing but a fallback to the listing walk.
const bucketHeaderProbeTimeout = 10 * time.Second

// coldUsageCacheWait stays well below PVE's hard seven-second PBS timeout but
// lets fast provider-side counters populate the very first status response
// after a proxy restart. Without it, the health placeholder becomes a false
// low RRD sample even though the real value arrives milliseconds later.
const coldUsageCacheWait = 2 * time.Second

// DataStoreUsage is what a full walk of a bucket tells us. Every figure comes
// from object listings only: no object is ever downloaded to produce it.
type DataStoreUsage struct {
	Bytes      uint64 // sum of the size of every object in the bucket
	ChunkBytes uint64 // of which chunks, the deduplicated payload
	Objects    uint64
	Snapshots  uint64
}

func waitForDataStoreUsage(key string, timeout time.Duration) (DataStoreUsage, bool) {
	deadline := time.NewTimer(timeout)
	defer deadline.Stop()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			if usage, ok := datastoreUsageCache.Peek(key); ok {
				return usage, true
			}
		case <-deadline.C:
			return DataStoreUsage{}, false
		}
	}
}

// FillArchiveSizes sets the Size field of each snapshot to the sum of the
// logical sizes of its archives, which is the figure Proxmox VE displays in
// the size column of the backup list (PVE::Storage::PBSPlugin::list_volumes
// reads the "size" field of each snapshot).
//
// The size of an archive is stated by its index, so it is read with a ranged
// GET of a few dozen bytes per index. Nothing else is downloaded: reading the
// chunks themselves would cost egress for a figure the index already knows.
func FillArchiveSizes(c *minio.Client, snapshots []s3pmoxcommon.Snapshot) {
	type job struct {
		snapshot int
		key      string
		objSize  int64
	}

	jobs := make([]job, 0)
	for i := range snapshots {
		for _, f := range snapshots[i].Files {
			if !s3pmoxcommon.IsIndex(f.Filename) {
				continue
			}
			jobs = append(jobs, job{
				snapshot: i,
				key:      snapshots[i].S3Prefix() + "/" + f.Filename,
				objSize:  int64(f.Size),
			})
		}
	}
	if len(jobs) == 0 {
		return
	}

	sizes := make([]uint64, len(jobs))
	jobCh := make(chan int)
	var wg sync.WaitGroup

	for w := 0; w < sizeLookupConcurrency; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for idx := range jobCh {
				j := jobs[idx]
				ds := snapshots[j.snapshot].Datastore
				size, _, err := archiveSizeCache.Get(ds+"/"+j.key, func() (uint64, error) {
					return s3pmoxcommon.ArchiveSize(context.Background(), c, ds, j.key, j.objSize)
				})
				if err != nil {
					s3backuplog.WarnPrint("Unable to determine the size of %s: %s", j.key, err.Error())
					continue
				}
				sizes[idx] = size
			}
		}()
	}

	for i := range jobs {
		jobCh <- i
	}
	close(jobCh)
	wg.Wait()

	for i, j := range jobs {
		snapshots[j.snapshot].Size += sizes[i]
	}
}

// FillSnapshotCryptModes reads uncached manifests with bounded concurrency and
// applies their authoritative encryption modes to the S3 object listing.
func FillSnapshotCryptModes(c *minio.Client, snapshots []s3pmoxcommon.Snapshot) {
	jobs := make(chan int)
	var wg sync.WaitGroup
	workerCount := sizeLookupConcurrency
	if len(snapshots) < workerCount {
		workerCount = len(snapshots)
	}
	for range workerCount {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range jobs {
				key := snapshots[i].Datastore + "/" + snapshots[i].S3Prefix() + "/index.json.blob"
				modes, fresh, err := manifestCryptModeCache.Get(key, func() (map[string]string, error) {
					return s3pmoxcommon.ReadSnapshotCryptModes(context.Background(), c, snapshots[i])
				})
				if err != nil {
					s3backuplog.WarnPrint("Unable to read snapshot encryption modes from %s: %s", key, err)
					continue
				}
				if fresh {
					s3backuplog.DebugPrint("Cached snapshot encryption modes from %s", key)
				}
				s3pmoxcommon.ApplySnapshotCryptModes(&snapshots[i], modes)
			}
		}()
	}
	for i := range snapshots {
		jobs <- i
	}
	close(jobs)
	wg.Wait()
}

// SnapshotSizeMode selects which figure the snapshot listing reports as the
// size of a backup, which is what PVE shows in the size column.
//
// There is no single honest answer on a deduplicating store, so the choice is
// left to the operator:
//
//   - "logical" is the size of the guest disks the backup contains. It is what
//     Proxmox Backup Server itself reports, it is always available, and it is
//     the only figure that does not depend on the other snapshots. It says
//     nothing about the space used in the bucket.
//   - "referenced" is the size of the chunks the backup points at, counting
//     shared chunks in full: what this backup would cost if it were alone.
//   - "exclusive" is the size of the chunks no other backup points at: what
//     deleting this backup would actually free. Summed over a datastore it
//     approaches the size of the bucket, which makes it the closest answer to
//     "how much does this backup cost me".
//
// The last two are only known from the report the garbage collector writes, so
// they are at most one collector run old, and a snapshot taken since the last
// run has no entry. Both cases fall back to the logical size rather than
// showing zero.
type SnapshotSizeMode string

const (
	SnapshotSizeLogical    SnapshotSizeMode = "logical"
	SnapshotSizeReferenced SnapshotSizeMode = "referenced"
	SnapshotSizeExclusive  SnapshotSizeMode = "exclusive"
)

// ValidSnapshotSizeMode reports whether a mode name is one we implement.
func ValidSnapshotSizeMode(mode string) bool {
	switch SnapshotSizeMode(mode) {
	case SnapshotSizeLogical, SnapshotSizeReferenced, SnapshotSizeExclusive:
		return true
	}
	return false
}

// ApplySnapshotSizeMode replaces the logical sizes already filled in by
// FillArchiveSizes with the figures from the collector report, for the modes
// that need it. Snapshots missing from the report keep their logical size.
func ApplySnapshotSizeMode(
	mode SnapshotSizeMode,
	snapshots []s3pmoxcommon.Snapshot,
	stats *s3pmoxcommon.UsageStats,
) {
	if mode == SnapshotSizeLogical || stats == nil {
		return
	}

	byPrefix := make(map[string]s3pmoxcommon.SnapshotUsage, len(stats.Snapshots))
	for _, s := range stats.Snapshots {
		byPrefix[s.Snapshot] = s
	}

	for i := range snapshots {
		u, ok := byPrefix[snapshots[i].S3Prefix()]
		if !ok {
			// Taken since the last collector run: nothing to say about its
			// footprint yet, the logical size stands.
			continue
		}
		switch mode {
		case SnapshotSizeReferenced:
			snapshots[i].Size = u.ReferencedBytes
		case SnapshotSizeExclusive:
			snapshots[i].Size = u.ExclusiveBytes
		}
	}
}

// ComputeDataStoreUsage reports what a datastore holds.
//
// Some S3 implementations already know the answer and state it in the headers
// of a HEAD request on the bucket, which turns the whole question into a
// single round trip; that path is tried first. Otherwise the bucket is walked
// with a listing, which is still metadata only - the size of every object is
// part of the listing response, so no object is ever downloaded - but costs
// one request per thousand objects.
//
// Either way callers must reach this through GetAsync: the walk takes seconds
// on a large bucket, and the endpoints that expose the result are polled by
// pvestatd under a 7 second timeout.
func ComputeDataStoreUsage(C TicketEntry, secure bool, datastore string) (DataStoreUsage, error) {
	if usage, ok := usageFromBucketHeaders(C, secure, datastore); ok {
		s3backuplog.InfoPrint(
			"Datastore [%s] usage read from the bucket headers: %d objects, %d bytes",
			datastore, usage.Objects, usage.Bytes,
		)
		return usage, nil
	}
	return walkDataStoreUsage(C.Client, datastore)
}

// usageFromBucketHeaders asks the endpoint whether it already accounts the
// bucket, instead of counting its objects one page at a time.
//
// There is no such thing as a standard bucket usage call in the S3 API, but
// several implementations answer a HEAD on the bucket with their own counters.
// OVH Object Storage returns x-ovh-bucket-size and x-ovh-bucket-object-count,
// which were verified to match a full walk of the bucket to the byte.
//
// Anything unexpected - another provider, a header that disappears, a bucket
// policy that forbids the call - simply reports false and the caller falls
// back to walking the bucket.
func usageFromBucketHeaders(C TicketEntry, secure bool, datastore string) (DataStoreUsage, bool) {
	if C.Client == nil || C.AccessKeyID == "" {
		return DataStoreUsage{}, false
	}

	scheme := "http"
	if secure {
		scheme = "https"
	}
	location, err := C.Client.GetBucketLocation(context.Background(), datastore)
	if err != nil {
		s3backuplog.DebugPrint("Unable to determine the region of [%s]: %s", datastore, err.Error())
		return DataStoreUsage{}, false
	}

	req, err := http.NewRequest(http.MethodHead, scheme+"://"+C.Endpoint+"/"+datastore, nil)
	if err != nil {
		return DataStoreUsage{}, false
	}
	// The header has to be set before signing: it is part of the signature,
	// and some endpoints reject a signed request that omits it.
	empty := sha256.Sum256(nil)
	req.Header.Set("X-Amz-Content-Sha256", hex.EncodeToString(empty[:]))
	signed := signer.SignV4(*req, C.AccessKeyID, C.SecretAccessKey, "", location)

	client := &http.Client{Timeout: bucketHeaderProbeTimeout}
	resp, err := client.Do(signed)
	if err != nil {
		s3backuplog.DebugPrint("Bucket header probe on [%s] failed: %s", datastore, err.Error())
		return DataStoreUsage{}, false
	}
	defer resp.Body.Close()
	io.Copy(io.Discard, resp.Body)

	if resp.StatusCode != http.StatusOK {
		s3backuplog.DebugPrint("Bucket header probe on [%s] returned HTTP %d", datastore, resp.StatusCode)
		return DataStoreUsage{}, false
	}

	size, err := strconv.ParseUint(resp.Header.Get("x-ovh-bucket-size"), 10, 64)
	if err != nil {
		return DataStoreUsage{}, false
	}
	usage := DataStoreUsage{Bytes: size}
	if count, err := strconv.ParseUint(resp.Header.Get("x-ovh-bucket-object-count"), 10, 64); err == nil {
		usage.Objects = count
	}
	return usage, true
}

func walkDataStoreUsage(c *minio.Client, datastore string) (DataStoreUsage, error) {
	usage := DataStoreUsage{}
	snapshotPrefixes := make(map[string]bool)

	for object := range c.ListObjects(context.Background(), datastore, minio.ListObjectsOptions{Recursive: true}) {
		if object.Err != nil {
			return DataStoreUsage{}, object.Err
		}
		usage.Objects++
		usage.Bytes += uint64(object.Size)

		switch {
		case len(object.Key) > 7 && object.Key[:7] == "chunks/":
			usage.ChunkBytes += uint64(object.Size)
		case len(object.Key) > 8 && object.Key[:8] == "backups/":
			// backups/<id>|<time>|<type>/<file>
			rest := object.Key[8:]
			for i := 0; i < len(rest); i++ {
				if rest[i] == '/' {
					snapshotPrefixes[rest[:i]] = true
					break
				}
			}
		}
	}
	usage.Snapshots = uint64(len(snapshotPrefixes))

	s3backuplog.InfoPrint(
		"Datastore [%s] usage refreshed: %d objects, %d bytes (%d bytes of chunks), %d snapshots",
		datastore, usage.Objects, usage.Bytes, usage.ChunkBytes, usage.Snapshots,
	)
	return usage, nil
}
