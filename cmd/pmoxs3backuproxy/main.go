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
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"flag"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"os"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"tizbac/pmoxs3backuproxy/internal/s3backuplog"
	"tizbac/pmoxs3backuproxy/internal/s3pmoxcommon"

	"github.com/google/uuid"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/minio/minio-go/v7/pkg/tags"
)

var connectionList = make(map[string]*minio.Client)
var writer_mux sync.RWMutex
var chunkUploads chunkFlightGroup

// bucketListCache backs GET /api2/json/admin/datastore.
//
// PVE calls that endpoint to activate the storage, and it is on the critical
// path of every backup job: PVE::Storage::PBSPlugin::activate_storage lists
// the datastores and aborts the job when the call does not answer within the
// 7 second timeout hardcoded in pbs_api_connect. Before this cache every call
// - including the pvestatd poll that happens every 10 seconds - performed a
// ListBuckets round trip to the object store, so a transient slowdown of the
// endpoint was enough to fail a whole night of backups with
// "could not activate storage - 500 read timeout".
//
// The list of buckets of an account changes on human timescales, so a short
// TTL plus serve-stale-on-error removes that failure mode entirely.
var bucketListCache *ttlCache[[]minio.BucketInfo]

// snapshotListCache holds the snapshot listing of a datastore. Listing it
// means walking every object under backups/, which the web UI would otherwise
// trigger on every refresh. It is invalidated as soon as the proxy itself
// changes the content of the datastore.
var snapshotListCache *ttlCache[[]s3pmoxcommon.Snapshot]

// snapshotSizeMode selects which figure is reported as the size of a backup.
var snapshotSizeMode = SnapshotSizeLogical

// usageStatsCache holds the deduplication report produced by the garbage
// collector. It changes once a day at most.
var usageStatsCache *ttlCache[[]byte]

// datastoreCapacity is the announced size of the datastore in bytes. An S3
// bucket usually has no quota to read back, so when it is left at zero the
// proxy reports a large free space rather than pretending the store is full.
var datastoreCapacity uint64

// unknownDataStoreCapacity is the free space reported when no capacity was
// configured. Reporting zero free space would make the PVE gauge show a full
// storage, which is both wrong and alarming for a store that has no quota.
const unknownDataStoreCapacity = uint64(1) << 50 // 1 PiB

const letterBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"

var (
	version = "dev"
	commit  = "none"
	date    = "unknown"
)

const defaultCacheTTLSeconds uint64 = 3600

func writeBinary(buf *bytes.Buffer, data interface{}) {
	err := binary.Write(buf, binary.LittleEndian, data)
	if err != nil {
		panic(fmt.Sprintf("Error writing binary data: %s", err))
	}
}

func RandStringBytes(n int) string {
	b := make([]byte, n)
	for i := range b {
		b[i] = letterBytes[rand.Intn(len(letterBytes))]
	}
	return string(b)
}

func certFingeprint(cfile string) *string {
	certData, err := os.ReadFile(cfile)
	if err != nil {
		s3backuplog.ErrorPrint("Failed to read certificate file: %s", err)
		return nil
	}

	certblock, _ := pem.Decode(certData)

	if certblock == nil {
		s3backuplog.ErrorPrint("Failed to decode PEM file")
		return nil
	}

	cert, err := x509.ParseCertificate(certblock.Bytes)

	if err != nil {
		s3backuplog.ErrorPrint("Failed to parse certificate file: %s", err)
		return nil
	}
	fingerprint := sha256.Sum256(cert.Raw)

	a := make([]string, 0)

	for i := 0; i < len(fingerprint); i++ {
		a = append(a, hex.EncodeToString(fingerprint[i:i+1]))
	}

	s := strings.Join(a, ":")

	s = strings.ToUpper(s)

	return &s
}

func main() {
	var printVersion bool
	certFlag := flag.String("cert", "server.crt", "Server SSL certificate file")
	keyFlag := flag.String("key", "server.key", "Server SSL key file")
	endpointFlag := flag.String("endpoint", "", "S3 Endpoint without https/http , host:port")
	bindAddress := flag.String("bind", "127.0.0.1:8007", "PBS Protocol bind address, recommended 127.0.0.1:8007, use :8007 for all")
	insecureFlag := flag.Bool("usessl", false, "Use SSL for endpoint connection: default: false")
	ticketExpireFlag := flag.Uint64("ticketexpire", 3600, "API Ticket expire time in seconds")
	lookupTypeFlag := flag.String("lookuptype", "auto", "Bucket lookup type: auto,dns,path")
	bucketCacheTTLFlag := flag.Uint64(
		"bucketcachettl", defaultCacheTTLSeconds,
		"Seconds a datastore (bucket) listing is reused before asking the S3 endpoint again, 0 disables caching",
	)
	snapshotCacheTTLFlag := flag.Uint64(
		"snapshotcachettl", defaultCacheTTLSeconds,
		"Seconds a snapshot listing is reused before listing the bucket again, 0 disables caching",
	)
	usageCacheTTLFlag := flag.Uint64(
		"usagecachettl", defaultCacheTTLSeconds,
		"Seconds before the used space of a datastore is recomputed in the background",
	)
	metadataCacheTTLFlag := flag.Uint64(
		"metadatacachettl", defaultCacheTTLSeconds,
		"Seconds archive sizes and encryption modes are cached after an opt-in metadata read",
	)
	gcStatsCacheTTLFlag := flag.Uint64(
		"gcstatscachettl", defaultCacheTTLSeconds,
		"Seconds a garbage-collector usage report is cached after an opt-in read",
	)
	reportDataStoreUsageFlag := flag.Bool(
		"reportdatastoreusage", false,
		"Report real bucket usage in datastore status (adds periodic S3 metadata requests)",
	)
	reportArchiveSizeFlag := flag.Bool(
		"reportarchivesize", false,
		"Report logical archive sizes (adds one small ranged GET per uncached archive)",
	)
	reportEncryptionFlag := flag.Bool(
		"reportencryption", false,
		"Report client-side encryption modes (adds one manifest GET per uncached snapshot)",
	)
	reportGCStatsFlag := flag.Bool(
		"reportgcstats", false,
		"Read the garbage-collector usage report for referenced/exclusive snapshot sizes",
	)
	cacheInvalidationDirFlag := flag.String(
		"cacheinvalidatedir", "",
		"Shared local directory for proxy/collector cache invalidation tokens; empty disables cross-process invalidation",
	)
	datastoreSizeFlag := flag.Uint64(
		"datastoresize", 0,
		"Capacity of the datastore in bytes, used to report free space, 0 if the bucket has no quota",
	)
	snapshotSizeFlag := flag.String(
		"snapshotsize", string(SnapshotSizeLogical),
		"Size reported for a backup: logical (guest disk size), referenced (chunks it points at) or exclusive (chunks only it points at)",
	)
	debug := flag.Bool("debug", false, "Debug logging")
	chunkS3TimeoutFlag := flag.Uint64(
		"chunks3timeout", 300,
		"Maximum seconds for one chunk S3 operation, 0 disables the deadline",
	)
	chunkStorageClassFlag := flag.String(
		"chunkstorageclass", "",
		"S3 storage class for newly uploaded chunks; empty uses the provider default",
	)
	backupStorageClassFlag := flag.String(
		"backupstorageclass", "",
		"S3 storage class for newly uploaded backup indexes, manifests, configs and logs; empty uses the provider default",
	)
	indexedStorageClassFlag := flag.String(
		"indexedstorageclass", "",
		"S3 storage class for newly created reusable index copies; empty uses the provider default",
	)
	flag.BoolVar(&printVersion, "version", false, "Show version and exit")
	flag.BoolVar(&printVersion, "v", false, "Show version and exit")
	flag.Parse()
	if printVersion {
		fmt.Println(version)
		os.Exit(0)
	}
	s3backuplog.InfoPrint("%s %s %s %s", os.Args[0], version, commit, date)
	if *endpointFlag == "" {
		flag.Usage()
		os.Exit(1)
	}
	if *debug {
		s3backuplog.EnableDebug()
	}

	bucketListCache = newTTLCache[[]minio.BucketInfo](time.Duration(*bucketCacheTTLFlag) * time.Second)
	snapshotListCache = newTTLCache[[]s3pmoxcommon.Snapshot](time.Duration(*snapshotCacheTTLFlag) * time.Second)
	datastoreUsageCache = newTTLCache[DataStoreUsage](time.Duration(*usageCacheTTLFlag) * time.Second)
	// An index under backups/ never changes, so its size is cached for as long
	// as the process is expected to run between restarts.
	archiveSizeCache = newTTLCache[uint64](time.Duration(*metadataCacheTTLFlag) * time.Second)
	manifestCryptModeCache = newTTLCache[map[string]string](time.Duration(*metadataCacheTTLFlag) * time.Second)
	usageStatsCache = newTTLCache[[]byte](time.Duration(*gcStatsCacheTTLFlag) * time.Second)
	reportDataStoreUsage = *reportDataStoreUsageFlag
	reportArchiveSize = *reportArchiveSizeFlag
	reportEncryption = *reportEncryptionFlag
	reportGCStats = *reportGCStatsFlag
	configureCacheInvalidations(*cacheInvalidationDirFlag, *endpointFlag)
	datastoreCapacity = *datastoreSizeFlag
	if !ValidSnapshotSizeMode(*snapshotSizeFlag) {
		s3backuplog.FatalPrint(
			"Unknown -snapshotsize %q, expected logical, referenced or exclusive",
			*snapshotSizeFlag,
		)
	}
	snapshotSizeMode = SnapshotSizeMode(*snapshotSizeFlag)
	if snapshotSizeMode != SnapshotSizeLogical && !reportGCStats {
		s3backuplog.FatalPrint("-snapshotsize %s requires the explicit -reportgcstats option", snapshotSizeMode)
	}
	if snapshotSizeMode != SnapshotSizeLogical {
		s3backuplog.InfoPrint(
			"Backups are reported with their %s size, which the garbage collector computes; "+
				"snapshots missing from its last report fall back to their logical size",
			snapshotSizeMode,
		)
	}

	S := &Server{
		S3Endpoint:     *endpointFlag,
		SecureFlag:     *insecureFlag,
		TicketExpire:   *ticketExpireFlag,
		LookupTypeFlag: *lookupTypeFlag,
		ChunkS3Timeout: time.Duration(*chunkS3TimeoutFlag) * time.Second,
		StorageClasses: storageClassPolicy{
			Chunks:  strings.TrimSpace(*chunkStorageClassFlag),
			Backups: strings.TrimSpace(*backupStorageClassFlag),
			Indexed: strings.TrimSpace(*indexedStorageClassFlag),
		},
	}
	if S.StorageClasses != (storageClassPolicy{}) {
		s3backuplog.InfoPrint(
			"S3 storage classes for new objects: chunks=%q backups=%q indexed=%q (empty means provider default)",
			S.StorageClasses.Chunks,
			S.StorageClasses.Backups,
			S.StorageClasses.Indexed,
		)
	}
	srv := &http.Server{Addr: *bindAddress, Handler: S}
	srv.SetKeepAlivesEnabled(true)
	go S.ticketGC()
	S.handleSignal()
	s3backuplog.InfoPrint(
		"Starting PBS api server on [%s], upstream: [%s] ssl: [%t] lookup type: [%s]",
		*bindAddress,
		*endpointFlag,
		*insecureFlag,
		*lookupTypeFlag,
	)

	certFing := certFingeprint(*certFlag)
	if certFing != nil {
		s3backuplog.InfoPrint("Server certificate fingerprint is: %s", *certFing)
	}

	if certFing != nil && *certFing == "55:BC:29:4B:BA:B6:A1:03:42:A9:D8:51:14:9D:BD:00:D2:2A:9C:A1:B8:4A:85:E1:AF:B2:0C:48:40:D6:CC:A4" {
		//Warn the user about MITM
		s3backuplog.WarnPrint("You are using default supplied certificate!, do not run PVE->S3PROXY on untrusted network!!!")
	}

	err := srv.ListenAndServeTLS(*certFlag, *keyFlag)
	if err != nil {
		panic(err)
	}
}

// listSnapshotsCached returns the snapshots of a datastore, going to the object
// store at most once per TTL. Sizes are resolved before the value is cached so
// that every reader gets them for free.
//
// The incremental backup path deliberately does not go through this cache:
// GetLatestSnapshot must always observe the real state of the bucket.
func listSnapshotsCached(c *minio.Client, datastore string) ([]s3pmoxcommon.Snapshot, error) {
	snapshots, _, err := snapshotListCache.Get(datastore, func() ([]s3pmoxcommon.Snapshot, error) {
		s, err := s3pmoxcommon.ListSnapshots(*c, datastore, false)
		if err != nil {
			return nil, err
		}
		applyOptionalSnapshotReports(c, datastore, s)
		return s, nil
	})
	return snapshots, err
}

func applyOptionalSnapshotReports(c *minio.Client, datastore string, snapshots []s3pmoxcommon.Snapshot) {
	if reportEncryption {
		FillSnapshotCryptModes(c, snapshots)
	}
	if reportArchiveSize {
		FillArchiveSizes(c, snapshots)
	}
	if reportGCStats {
		ApplySnapshotSizeMode(snapshotSizeMode, snapshots, readUsageStats(c, datastore))
	}
}

func readDataStoreUsageForStatus(
	datastore string,
	fetch func() (DataStoreUsage, error),
) (DataStoreUsage, bool) {
	if !reportDataStoreUsage {
		return DataStoreUsage{}, false
	}
	usage, known := datastoreUsageCache.GetAsync(datastore, fetch)
	if known {
		return usage, true
	}
	return waitForDataStoreUsage(datastore, coldUsageCacheWait)
}

// groupSize keeps the historical placeholder when the real size is unknown, as
// a size of zero makes proxmox-backup-manager treat the group as an unfinished
// backup and skip it during a sync.
func groupSize(size uint64) uint64 {
	if size == 0 {
		return 200
	}
	return size
}

// readUsageStats returns the collector report of a datastore, or nil when
// there is none yet. Callers must treat a nil report as "no footprint figures
// available", never as an error: the report is a nicety, the listing it
// decorates is not.
func readUsageStats(c *minio.Client, datastore string) *s3pmoxcommon.UsageStats {
	if snapshotSizeMode == SnapshotSizeLogical {
		return nil
	}
	raw, _, err := usageStatsCache.Get(datastore, func() ([]byte, error) {
		obj, err := c.GetObject(
			context.Background(), datastore, s3pmoxcommon.UsageStatsObject, minio.GetObjectOptions{},
		)
		if err != nil {
			return nil, err
		}
		defer obj.Close()
		return io.ReadAll(obj)
	})
	if err != nil {
		s3backuplog.WarnPrint(
			"No usage report for [%s], backups keep their logical size until the garbage collector runs: %s",
			datastore, err.Error(),
		)
		return nil
	}
	stats := &s3pmoxcommon.UsageStats{}
	if err := json.Unmarshal(raw, stats); err != nil {
		s3backuplog.WarnPrint("Unable to decode the usage report of [%s]: %s", datastore, err.Error())
		return nil
	}
	return stats
}

func (s *Server) ticketGC() {
	for {
		var cnt int64
		now := uint64(time.Now().Unix())
		s.Auth.Range(func(k, v interface{}) bool {
			te, _ := s.Auth.Load(k)
			C := te.(TicketEntry)
			if C.Expire < now {
				s3backuplog.DebugPrint("Expired ticket %s", k)
				s.Auth.Delete(k)
				cnt++
			}
			return true
		})
		s3backuplog.InfoPrint("Expired %v tickets", cnt)
		time.Sleep(30 * time.Second)
	}
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	cookiere := regexp.MustCompile(`PBSAuthCookie=([^;]+)`)
	matches := cookiere.FindStringSubmatch(r.Header.Get("Cookie"))
	auth := false

	C := TicketEntry{}
	if len(matches) >= 2 {
		entry, ok := s.Auth.Load(matches[1])
		if ok {
			C = entry.(TicketEntry)
			auth = true
		}
	}

	s3backuplog.DebugPrint("Request:" + r.RequestURI + " Method: " + r.Method)
	path := strings.Split(r.RequestURI, "/")
	if strings.HasPrefix(r.RequestURI, "/api2/json/version") && auth {
		w.Header().Add("Content-Type", "application/json")
		resp, _ := json.Marshal(Response{
			Data: VersionInfo{
				Release: "0",
				RepoID:  "f418479aaaa13ca794e2743296f9b909fe52516c",
				Version: "3.3",
			},
		})
		w.Write(resp)
		return
	}
	if len(path) >= 7 && strings.HasPrefix(r.RequestURI, "/api2/json/admin/datastore/") && auth {
		ds := path[5]
		action := path[6]
		syncDataStoreCacheInvalidation(ds)
		if strings.HasPrefix(action, "notes") && r.Method == "GET" {
			var ss s3pmoxcommon.Snapshot
			ss.InitWithQuery(r.URL.Query())
			ss.Datastore = ds
			existingTags, err := ss.ReadTags(*C.Client)
			if err != nil {
				w.WriteHeader(http.StatusInternalServerError)
				w.Write([]byte(err.Error()))
				return
			}
			var note []byte
			tagvalue, ok := existingTags["note"]
			if ok {
				note, _ = base64.RawStdEncoding.DecodeString(tagvalue)
			}
			w.Header().Add("Content-Type", "application/json")
			resp, _ := json.Marshal(Response{
				Data: string(note),
			})
			w.Write(resp)
			return
		}
		if strings.HasPrefix(action, "notes") && r.Method == "PUT" {
			var ss s3pmoxcommon.Snapshot
			var tag *tags.Tags
			ss.InitWithQuery(r.URL.Query())
			ss.Datastore = ds
			note := r.FormValue("notes")
			note = base64.RawStdEncoding.EncodeToString([]byte(note))
			existingTags, _ := ss.ReadTags(*C.Client)
			existingTags["note"] = note
			tag, _ = tags.NewTags(existingTags, false)
			err := C.Client.PutObjectTagging(
				context.Background(),
				ds,
				ss.S3Prefix()+"/index.json.blob",
				tag,
				minio.PutObjectTaggingOptions{},
			)
			if err != nil {
				s3backuplog.ErrorPrint("Unable to set note tag for object: %s: %s", ss.S3Prefix(), err.Error())
				w.WriteHeader(http.StatusInternalServerError)
				w.Write([]byte(err.Error()))
				return
			}
			publishDataStoreCacheInvalidation(ds)
			w.WriteHeader(http.StatusOK)
		}
		if strings.HasPrefix(action, "files") && r.Method == "GET" {
			var ss s3pmoxcommon.Snapshot
			ss.InitWithQuery(r.URL.Query())
			ss.Datastore = ds
			ss.GetFiles(*C.Client)
			filesSnapshot := []s3pmoxcommon.Snapshot{ss}
			if reportEncryption {
				FillSnapshotCryptModes(C.Client, filesSnapshot)
			}
			ss = filesSnapshot[0]
			w.Header().Add("Content-Type", "application/json")
			resp, _ := json.Marshal(Response{
				Data: ss.Files,
			})
			w.Write(resp)
			return
		}

		if strings.HasPrefix(action, "protected") && r.Method == "GET" {
			var ss s3pmoxcommon.Snapshot
			var protected bool = false
			ss.InitWithQuery(r.URL.Query())
			ss.Datastore = ds
			w.Header().Add("Content-Type", "application/json")
			existingTags, err := ss.ReadTags(*C.Client)
			if err != nil {
				w.WriteHeader(http.StatusInternalServerError)
				w.Write([]byte(err.Error()))
				return
			}
			_, ok := existingTags["protected"]
			if ok {
				if existingTags["protected"] == "true" {
					protected = true
				}
			}
			resp, _ := json.Marshal(Response{
				Data: protected,
			})
			s3backuplog.ErrorPrint("%s", resp)
			w.Write(resp)
			return
		}

		if strings.HasPrefix(action, "protected") && r.Method == "PUT" {
			var ss s3pmoxcommon.Snapshot
			var tag *tags.Tags
			ss.InitWithForm(r)
			ss.Datastore = ds
			existingTags, err := ss.ReadTags(*C.Client)
			if err != nil {
				w.WriteHeader(http.StatusInternalServerError)
				w.Write([]byte(err.Error()))
				return
			}
			tagvalue, ok := existingTags["protected"]
			if ok {
				s3backuplog.InfoPrint("Toggle protection for snapshot: %s", ss.S3Prefix())
				if tagvalue == "true" {
					existingTags["protected"] = "false"
				} else {
					existingTags["protected"] = "true"
				}
			} else {
				s3backuplog.InfoPrint("Enable protection for snapshot: %s", ss.S3Prefix())
				existingTags["protected"] = "true"
			}

			tag, _ = tags.NewTags(existingTags, false)
			err = C.Client.PutObjectTagging(
				context.Background(),
				ds,
				ss.S3Prefix()+"/index.json.blob",
				tag,
				minio.PutObjectTaggingOptions{},
			)
			if err != nil {
				s3backuplog.ErrorPrint("Protection: Unable to set tag for object: %s: %s", ss.S3Prefix(), err.Error())
			} else {
				publishDataStoreCacheInvalidation(ds)
			}
			w.Header().Add("Content-Type", "application/json")
			resp, _ := json.Marshal(Response{
				Data: ss,
			})
			w.Write(resp)
			return
		}
		if action == "gc" || action == "prune" {
			/**
			 * proxmox-backup-client garbage-collect and PVE frontend "prune
			 * group" endpoints are not implemented
			 **/
			w.WriteHeader(http.StatusNotImplemented)
			w.Write([]byte("Not implemented"))
			return
		}
		if action == "s3stats" && r.Method == "GET" {
			/**
			 * Deduplication figures, as computed by the garbage collector
			 * during its nightly mark and sweep and left in the bucket. They
			 * cannot be produced on demand: knowing what a snapshot holds
			 * exclusively means resolving every chunk reference of every
			 * other snapshot. Reading them back is a single small GET.
			 *
			 * This endpoint is not part of the PBS API, PVE never calls it.
			 * It exists so a human or a script can ask what a snapshot really
			 * costs, which no column of the PVE interface can show.
			 **/
			if !reportGCStats {
				http.Error(w, "GC usage reporting is disabled; start the proxy with -reportgcstats", http.StatusNotFound)
				return
			}
			stats, _, err := usageStatsCache.Get(ds, func() ([]byte, error) {
				obj, err := C.Client.GetObject(
					context.Background(), ds, s3pmoxcommon.UsageStatsObject, minio.GetObjectOptions{},
				)
				if err != nil {
					return nil, err
				}
				defer obj.Close()
				return io.ReadAll(obj)
			})
			if err != nil {
				w.WriteHeader(http.StatusNotFound)
				io.WriteString(w, "no usage report yet, it is written by the garbage collector: "+err.Error())
				return
			}
			w.Header().Add("Content-Type", "application/json")
			resp, _ := json.Marshal(Response{Data: json.RawMessage(stats)})
			w.Write(resp)
			return
		}

		if action == "status" {
			/**
			 * The used space is the sum of the size of every object of the
			 * bucket, which a listing already reports: no object is
			 * downloaded to produce this figure.
			 *
			 * It is read asynchronously because walking a bucket takes
			 * seconds while pvestatd, which polls this endpoint every 10
			 * seconds, gives the whole request 7 seconds before declaring
			 * the storage unusable. Until the first walk completes, the
			 * placeholder below keeps the storage reported as healthy.
			 **/
			usage, known := readDataStoreUsageForStatus(ds, func() (DataStoreUsage, error) {
				return ComputeDataStoreUsage(C, s.SecureFlag, ds)
			})
			if reportDataStoreUsage && !known {
				s3backuplog.WarnPrint(
					"Datastore [%s] usage is not available after %s; returning the startup placeholder while refresh continues",
					ds, coldUsageCacheWait,
				)
			}

			status := DataStoreStatus{
				Used:    10000,
				Avail:   10000000,
				Total:   10000 + 10000000,
				Counts:  0,
				GCState: true, // todo
			}
			if known {
				if usage.Snapshots == 0 {
					// The accounting shortcut does not count snapshots. Report
					// them when a listing happens to be cached, rather than
					// walking the bucket for a cosmetic figure.
					if snapshots, ok := snapshotListCache.Peek(ds); ok {
						usage.Snapshots = uint64(len(snapshots))
					}
				}
				avail := unknownDataStoreCapacity
				if datastoreCapacity > usage.Bytes {
					avail = datastoreCapacity - usage.Bytes
				} else if datastoreCapacity > 0 {
					avail = 0
				}
				status = DataStoreStatus{
					Used:    int64(usage.Bytes),
					Avail:   int64(avail),
					Total:   int64(usage.Bytes + avail),
					Counts:  int64(usage.Snapshots),
					GCState: true, // todo
				}
			}

			resp, _ := json.Marshal(Response{Data: status})
			w.Header().Add("Content-Type", "application/json")
			w.Write(resp)
		}

		if strings.HasPrefix(action, "namespace") {
			/** We dont have namespaces implemented now, so we just return
			 *	the default namespace
			**/
			if r.Method != "GET" {
				w.WriteHeader(http.StatusNotImplemented)
				w.Write([]byte("Not implemented"))
				return
			}
			namespaces := make([]Namespace, 0)
			namespaces = append(namespaces, Namespace{Name: "Root"})
			resp, _ := json.Marshal(Response{
				Data: namespaces,
			})
			w.Header().Add("Content-Type", "application/json")
			w.Write(resp)
		}

		if strings.HasPrefix(action, "groups") {
			/**
				Return a backup group (usually for a given namespace)
				used by proxmox-backup-client list and proxmox-backup-manager pull
			**/
			ns := r.URL.Query().Get("ns")
			if ns != "" {
				s3backuplog.DebugPrint("Request for namespace: %s", ns)
			}
			snapshots, _ := listSnapshotsCached(C.Client, ds)
			groups := make([]Group, 0)
			for _, snap := range snapshots {
				var filelist []string
				for _, file := range snap.Files {
					filelist = append(filelist, file.Filename)
				}
				g := Group{
					Count:      1,
					BackupID:   snap.BackupID,
					BackupTime: snap.BackupTime,
					Files:      filelist,
					BackupType: snap.BackupType,
					/* During proxmox-backup-manager pull the sync job expects
					 * a size field, otherwise it assumes the backup to be active
					 * and ignores it during sync. The real size is used when it
					 * is known, the historical placeholder otherwise.
					 **/
					Size:  groupSize(snap.Size),
					Owner: C.AccessKeyID + "@pbs",
				}
				var exists bool = false
				for k := range groups {
					/**
					 * The group entry already exists, so just update it.
					 **/
					if groups[k].BackupType == g.BackupType && groups[k].BackupID == g.BackupID {
						groups[k].Count += 1
						groups[k].BackupTime = g.BackupTime
						exists = true
					}
				}
				if !exists {
					groups = append(groups, g)
				}
			}
			resp, _ := json.Marshal(Response{
				Data: groups,
			})
			w.Header().Add("Content-Type", "application/json")
			w.Write(resp)
		}

		if strings.HasPrefix(action, "upload-backup-log") && r.Method == "POST" {
			var ss s3pmoxcommon.Snapshot
			ss.InitWithQuery(r.URL.Query())
			tgtfile := ss.S3Prefix() + "/client.log.blob"
			_, err := C.Client.PutObject(
				context.Background(),
				ds,
				tgtfile,
				r.Body,
				r.ContentLength,
				putOptions(s.StorageClasses.Backups, nil),
			)
			if err != nil {
				w.WriteHeader(http.StatusInternalServerError)
				w.Write([]byte(err.Error()))
				s3backuplog.ErrorPrint("%s failed to upload backup log: %s", err.Error())
				return
			}
			s3backuplog.InfoPrint("Saved backup log: %s", tgtfile)
			publishDataStoreCacheInvalidation(ds)
			w.WriteHeader(http.StatusOK)
		}

		if strings.HasPrefix(action, "snapshots") {
			if r.Method == "DELETE" {
				var ss s3pmoxcommon.Snapshot
				ss.InitWithForm(r)
				ss.Datastore = ds
				s3backuplog.InfoPrint("Removing snapshot: %s as requested by user", ss.S3Prefix())
				invalidateDataStoreCachesLocal(ds)
				if err := ss.Delete(*C.Client); err == nil {
					publishDataStoreCacheInvalidation(ds)
					w.Header().Add("Content-Type", "application/json")
					resp, _ := json.Marshal(Response{
						Data: ss,
					})
					w.Write(resp)
				} else {
					// RemoveObjects may have deleted only part of the snapshot before
					// reporting an error. Invalidate even on failure.
					publishDataStoreCacheInvalidation(ds)
					w.WriteHeader(http.StatusInternalServerError)
					io.WriteString(w, err.Error())
				}
			}

			if r.Method == "GET" {
				/**
				 * Based on the request, return either a list of
				 * snapshots or a specific one
				 **/
				var snapshots []s3pmoxcommon.Snapshot
				var err error
				var returned = make([]s3pmoxcommon.Snapshot, 0)

				// single snapshot requested without datastore
				id := r.URL.Query().Get("backup-id")
				bcktype := r.URL.Query().Get("backup-type")

				snapshots, err = listSnapshotsCached(C.Client, ds)
				if err != nil {
					w.WriteHeader(http.StatusInternalServerError)
					io.WriteString(w, err.Error())
				}

				/**
				 * Go through all snapshots and return the right one
				 **/
				if id != "" {
					for k := range snapshots {
						if snapshots[k].BackupID == id && snapshots[k].BackupType == bcktype {
							returned = append(returned, snapshots[k])
						}
					}
				}

				if len(returned) > 0 || id != "" {
					snapshots = returned
				}

				resp, _ := json.Marshal(Response{
					Data: snapshots,
				})
				w.Header().Add("Content-Type", "application/json")
				w.Write(resp)
			}
		}

	}
	/*Backup HTTP2 Api*/
	if strings.HasPrefix(r.RequestURI, "/finish") && s.H2Ticket != nil && r.Method == "POST" {
		s.Finished = true
		if s.SelectedDataStore != nil {
			// A new snapshot has just landed: let the UI see it without
			// waiting out the listing TTL.
			publishDataStoreCacheInvalidation(*s.SelectedDataStore)
		}
	}
	if strings.HasPrefix(r.RequestURI, "/previous_backup_time") && s.H2Ticket != nil && r.Method == "GET" {
		mostRecent, err := s3pmoxcommon.GetLatestSnapshot(
			*s.H2Ticket.Client,
			*s.SelectedDataStore,
			s.Snapshot.BackupID,
			s.Snapshot.BackupTime,
		)
		if err != nil {
			s3backuplog.DebugPrint("Unable to get latest snapshot: [%s]", err.Error())
			w.WriteHeader(http.StatusInternalServerError)
			io.WriteString(w, err.Error())
			return
		}
		if mostRecent == nil {
			s3backuplog.DebugPrint("Latest snapshot not found, returning StatusNotFound")
			w.WriteHeader(http.StatusNotFound)
			return
		}
		w.WriteHeader(http.StatusOK)
		s3backuplog.DebugPrint("Return previous backup time: [%d]", mostRecent.BackupTime)

		r := Response{Data: mostRecent.BackupTime}
		responsedata, _ := json.Marshal(r)
		w.Header().Add("Content-Type", "application/json")
		w.Write(responsedata)
	}

	if strings.HasPrefix(r.RequestURI, "/previous?") && s.H2Ticket != nil && r.Method == "GET" {
		s3backuplog.InfoPrint("Handling get request for previous (%s)", r.URL.Query().Get("archive-name"))
		mostRecent, err := s3pmoxcommon.GetLatestSnapshot(
			*s.H2Ticket.Client,
			*s.SelectedDataStore,
			s.Snapshot.BackupID,
			s.Snapshot.BackupTime,
		)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			io.WriteString(w, err.Error())
			return
		}

		if mostRecent == nil {
			s3backuplog.DebugPrint("No previous snapshot found")
			w.WriteHeader(http.StatusNotFound)
			return
		}

		for _, f := range mostRecent.Files {
			if f.Filename == r.URL.Query().Get("archive-name") {
				obj, err := s.H2Ticket.Client.GetObject(
					context.Background(),
					*s.SelectedDataStore,
					mostRecent.S3Prefix()+"/"+f.Filename,
					minio.GetObjectOptions{},
				)
				if err != nil {
					w.WriteHeader(http.StatusNotFound)
					s3backuplog.ErrorPrint(err.Error() + " " + mostRecent.S3Prefix() + "/" + f.Filename)
					io.WriteString(w, err.Error())
					return
				}
				stat, err := obj.Stat()
				if err != nil {
					w.WriteHeader(http.StatusNotFound)
					s3backuplog.ErrorPrint(err.Error() + " " + mostRecent.S3Prefix() + "/" + f.Filename)
					io.WriteString(w, err.Error())
					return
				}

				objcsjson, err := s.H2Ticket.Client.GetObject(
					context.Background(),
					*s.SelectedDataStore,
					mostRecent.S3Prefix()+"/"+f.Filename+".csjson",
					minio.GetObjectOptions{},
				)
				if err == nil {
					_, err := objcsjson.Stat()
					if err == nil {
						s3backuplog.DebugPrint("Found JSON Chunk size index loading it")
						data, err := io.ReadAll(objcsjson)
						if err == nil {
							M := make(map[string]uint64)
							json.Unmarshal(data, &M)
							s3backuplog.DebugPrint("Loaded %d sizes", len(M))
							for k, v := range M {
								s.KnownChunksSizes.Store(k, v)
							}
						}
					}
				}
				s3backuplog.InfoPrint(
					"Returning previous snapshot object: %s",
					mostRecent.S3Prefix()+"/"+f.Filename,
				)
				w.Header().Add("Content-Length", fmt.Sprintf("%d", stat.Size))
				w.WriteHeader(http.StatusOK)
				io.Copy(w, obj)
				return
			}
		}
		s3backuplog.WarnPrint(
			"File %s not found in snapshot %s (%s)",
			r.URL.Query().Get("archive-name"),
			mostRecent.S3Prefix(),
			mostRecent.Files,
		)
		w.WriteHeader(http.StatusNotFound)
		return
	}

	if strings.HasPrefix(r.RequestURI, "/fixed_index?") && s.H2Ticket != nil && r.Method == "POST" {
		fidxname := r.URL.Query().Get("archive-name")
		reusecsum := r.URL.Query().Get("reuse-csum")
		size := r.URL.Query().Get("size")
		S, _ := strconv.ParseUint(size, 10, 64)
		s3backuplog.InfoPrint("Archive name : %s, size: %s\n", fidxname, size)
		wid := atomic.AddInt32(&s.CurWriter, 1)
		resp, _ := json.Marshal(Response{
			Data: wid,
		})
		writer_mux.Lock()
		s.Writers[wid] = &Writer{
			Assignments: make(map[int64][]byte),
			FidxName:    fidxname,
			Size:        S,
			ReuseCSUM:   reusecsum,
			Chunksize:   4194304,
		}
		writer_mux.Unlock()
		w.Header().Add("Content-Type", "application/json")
		w.Write(resp)
	}

	if strings.HasPrefix(r.RequestURI, "/dynamic_index?") && s.H2Ticket != nil && r.Method == "POST" {
		fidxname := r.URL.Query().Get("archive-name")
		wid := atomic.AddInt32(&s.CurWriter, 1)
		resp, _ := json.Marshal(Response{
			Data: wid,
		})
		writer_mux.Lock()
		s.Writers[wid] = &Writer{Assignments: make(map[int64][]byte), FidxName: fidxname}
		writer_mux.Unlock()
		w.Header().Add("Content-Type", "application/json")
		w.Write(resp)
	}

	if strings.HasPrefix(r.RequestURI, "/fixed_close?") && s.H2Ticket != nil && r.Method == "POST" {
		wid, _ := strconv.ParseInt(r.URL.Query().Get("wid"), 10, 32)
		csumindex, _ := hex.DecodeString(r.URL.Query().Get("csum"))
		outFile := make([]byte, 0)
		//FIDX format is documented on Proxmox Backup docs pdf
		if s.Writers[int32(wid)].ReuseCSUM != "" {
			//In that case we load from S3 the specified reuse index
			obj, err := s.H2Ticket.Client.GetObject(
				context.Background(),
				*s.SelectedDataStore,
				"indexed/"+s.Writers[int32(wid)].ReuseCSUM+".fidx",
				minio.GetObjectOptions{},
			)
			if err != nil {
				w.WriteHeader(http.StatusInternalServerError)
				w.Write([]byte(err.Error()))
				s3backuplog.ErrorPrint("Failed to find index %s to be reused: %s", s.Writers[int32(wid)].ReuseCSUM, err.Error())
				return
			}
			outFile, err = io.ReadAll(obj)
			if err != nil {
				w.WriteHeader(http.StatusInternalServerError)
				w.Write([]byte(err.Error()))
				s3backuplog.ErrorPrint("Failed to find index %s to be reused: %s", s.Writers[int32(wid)].ReuseCSUM, err.Error())
				return
			}
			//Chunk size and size cannot be known since incremental may potentially upload 0 chunks, so we take them from reused index
			s.Writers[int32(wid)].Size = binary.LittleEndian.Uint64(outFile[64:72])
			s.Writers[int32(wid)].Chunksize = binary.NativeEndian.Uint64(outFile[72:80])

			s3backuplog.DebugPrint("Reusing old index")
		} else {
			//In that case a new index is allocated, 4096 is the header, then size/chunksize blocks follow of 32 bytes ( chunk digest sha 256 )
			outFile = make([]byte, 4096+32*len(s.Writers[int32(wid)].Assignments))
			copy(outFile[0:8], s3pmoxcommon.PROXMOX_INDEX_MAGIC_FIXED[:])
			//Chunksize in that case is derived from at least one chunk having been uploaded
			sl := binary.LittleEndian.AppendUint64(make([]byte, 0), s.Writers[int32(wid)].Size)
			copy(outFile[64:72], sl)
			sl = binary.LittleEndian.AppendUint64(make([]byte, 0), s.Writers[int32(wid)].Chunksize)
			copy(outFile[72:80], sl)

		}
		copy(outFile[32:64], csumindex[0:32]) //Checksum is almost never the same , so it is changed with new backup
		u := uuid.New()                       //Generate a new uuid too
		b, _ := u.MarshalBinary()
		copy(outFile[8:24], b)
		sl := binary.LittleEndian.AppendUint64(make([]byte, 0), uint64(time.Now().Unix()))
		copy(outFile[24:32], sl)
		k := uint64(0)

		for i := uint64(0); i < s.Writers[int32(wid)].Size; i += s.Writers[int32(wid)].Chunksize {
			val, ok := s.Writers[int32(wid)].Assignments[int64(i)]
			if !ok {
				if s.Writers[int32(wid)].ReuseCSUM == "" {
					w.WriteHeader(http.StatusInternalServerError)
					io.WriteString(w, "Hole in index")
					s3backuplog.ErrorPrint("Backup failed because of hole in fixed index")
					return
				}
			} else {
				//4096 bytes is the header, being each element 32 bytes (sha256) after the header
				copy(outFile[4096+32*k:4096+32*k+32], val)
			}

			k++
		}
		R := bytes.NewReader(outFile)
		_, err := s.H2Ticket.Client.PutObject(
			context.Background(),
			*s.SelectedDataStore,
			s.Snapshot.S3Prefix()+"/"+s.Writers[int32(wid)].FidxName,
			R,
			int64(len(outFile)),
			putOptions(
				s.StorageClasses.Backups,
				map[string]string{"csum": r.URL.Query().Get("csum")},
			),
		)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte(err.Error()))
			s3backuplog.ErrorPrint("%s failed to upload to S3 bucket: %s", s.Writers[int32(wid)].FidxName, err.Error())
			return
		}
		//This copy of the object is later used to lookup when reuse-csum is in play ( incremental backup )
		//It will waste a bit of space, but indexes overall are much smaller than actual data , so for now is a price that can be paid to avoid going thru all the files
		_, err = s.H2Ticket.Client.CopyObject(
			context.Background(),
			indexedCopyOptions(
				*s.SelectedDataStore,
				"indexed/"+r.URL.Query().Get("csum")+".fidx",
				r.URL.Query().Get("csum"),
				s.StorageClasses.Indexed,
			),
			minio.CopySrcOptions{
				Bucket: *s.SelectedDataStore,
				Object: s.Snapshot.S3Prefix() + "/" + s.Writers[int32(wid)].FidxName,
			},
		)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte(err.Error()))
			s3backuplog.ErrorPrint("%s failed to make a copy of the index on S3 bucket: %s", s.Writers[int32(wid)].FidxName, err.Error())
			return
		}
	}

	if strings.HasPrefix(r.RequestURI, "/dynamic_close?") && s.H2Ticket != nil && r.Method == "POST" {
		wid, _ := strconv.ParseInt(r.URL.Query().Get("wid"), 10, 32)
		csumindex, _ := hex.DecodeString(r.URL.Query().Get("csum"))

		header := new(bytes.Buffer)
		writeBinary(header, s3pmoxcommon.PROXMOX_INDEX_MAGIC_DYNAMIC)
		u := uuid.New()
		b, _ := u.MarshalBinary()
		writeBinary(header, b)
		ctime := uint64(time.Now().Unix())
		writeBinary(header, ctime)
		writeBinary(header, csumindex)

		reserved := [4032]byte{}
		writeBinary(header, reserved)

		//Load globally known chunk sizes
		s.KnownChunksSizes.Range(func(key, value any) bool {
			s.Writers[int32(wid)].DynamicChunkSizes.Store(key, value)
			return true
		})

		var keys []int64
		for k := range s.Writers[int32(wid)].Assignments {
			keys = append(keys, k)
		}
		sort.Slice(keys, func(i, j int) bool { return keys[i] < keys[j] })

		var offset uint64 = 0

		jsonMap := make(map[string]uint64)
		for _, k := range keys {
			var origsize uint64
			digest := hex.EncodeToString(s.Writers[int32(wid)].Assignments[k])
			entry, ok := s.Writers[int32(wid)].DynamicChunkSizes.Load(digest)
			if ok {
				origsize = uint64(entry.(uint64))
			} else {
				panic(fmt.Sprintf("Missing size info for chunk %s", digest))
			}
			offset = uint64(k) + origsize

			s3backuplog.DebugPrint("Offset: %d, Size: %d", offset, origsize)
			writeBinary(header, offset)
			writeBinary(header, s.Writers[int32(wid)].Assignments[k])
			jsonMap[digest] = origsize
		}

		finalData := header.Bytes()

		R := bytes.NewReader(finalData)
		_, err := s.H2Ticket.Client.PutObject(
			context.Background(),
			*s.SelectedDataStore,
			s.Snapshot.S3Prefix()+"/"+s.Writers[int32(wid)].FidxName,
			R,
			int64(R.Len()),
			putOptions(
				s.StorageClasses.Backups,
				map[string]string{"csum": r.URL.Query().Get("csum")},
			),
		)

		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte(err.Error()))
			s3backuplog.ErrorPrint("%s failed to upload to S3 bucket: %s", s.Writers[int32(wid)].FidxName, err.Error())
			return
		}

		chunksizeinfo, _ := json.Marshal(jsonMap)

		R = bytes.NewReader(chunksizeinfo)
		_, err = s.H2Ticket.Client.PutObject(
			context.Background(),
			*s.SelectedDataStore,
			s.Snapshot.S3Prefix()+"/"+s.Writers[int32(wid)].FidxName+".csjson",
			R,
			int64(R.Len()),
			putOptions(
				s.StorageClasses.Backups,
				map[string]string{"csum": r.URL.Query().Get("csum")},
			),
		)

		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte(err.Error()))
			s3backuplog.ErrorPrint("%s failed to upload to S3 bucket: %s", s.Writers[int32(wid)].FidxName, err.Error())
			return
		}

	}

	if strings.HasPrefix(r.RequestURI, "/dynamic_index") && s.H2Ticket != nil && r.Method == "PUT" {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			s3backuplog.ErrorPrint("Unable to read body: %s", err.Error())
			w.WriteHeader(http.StatusBadRequest)
			w.Write([]byte(err.Error()))
			return
		}
		req := AssignmentRequest{}
		err = json.Unmarshal(body, &req)
		if err != nil {
			s3backuplog.ErrorPrint("Unable to unmarshal json: %s", err.Error())
			w.WriteHeader(http.StatusBadRequest)
			w.Write([]byte(err.Error()))
			return
		}
		for i := 0; i < len(req.DigestList); i++ {
			b, err := hex.DecodeString(req.DigestList[i])
			if err != nil {
				s3backuplog.ErrorPrint("Unable to decode digest: %s", err.Error())
				w.WriteHeader(http.StatusBadRequest)
				io.WriteString(w, err.Error())
			}
			s.Writers[req.Wid].Assignments[int64(req.OffsetList[i])] = b
		}

	}

	if strings.HasPrefix(r.RequestURI, "/fixed_index") && s.H2Ticket != nil && r.Method == "PUT" {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			s3backuplog.ErrorPrint("Unable to read body: %s", err.Error())
			w.WriteHeader(http.StatusBadRequest)
			w.Write([]byte(err.Error()))
			return
		}
		req := AssignmentRequest{}
		err = json.Unmarshal(body, &req)
		if err != nil {
			s3backuplog.ErrorPrint("Unable to unmarshal json: %s", err.Error())
			w.WriteHeader(http.StatusBadRequest)
			w.Write([]byte(err.Error()))
			return
		}
		if len(req.DigestList) != len(req.OffsetList) {
			w.WriteHeader(http.StatusBadRequest)
			io.WriteString(w, "Digest list and Offset list size does not match")
			s3backuplog.ErrorPrint("%s: Digest list and Offset list size does not match", s.Writers[req.Wid].FidxName)
			return
		}
		for i := 0; i < len(req.DigestList); i++ {
			if req.OffsetList[i]%s.Writers[req.Wid].Chunksize != 0 {
				w.WriteHeader(http.StatusBadRequest)
				io.WriteString(w, "Chunk offset not at chunk-size boundary")
				s3backuplog.ErrorPrint("%s: Chunk offset not at chunk-size boundary", s.Writers[req.Wid].FidxName)
				return
			}
		}

		for i := 0; i < len(req.DigestList); i++ {
			b, err := hex.DecodeString(req.DigestList[i])
			if err != nil {
				s3backuplog.ErrorPrint("Unable to decode digest: %s", err.Error())
				w.WriteHeader(http.StatusBadRequest)
				io.WriteString(w, err.Error())
			}
			s.Writers[req.Wid].Assignments[int64(req.OffsetList[i])] = b
		}
	}

	if strings.HasPrefix(r.RequestURI, "/fixed_chunk?") || strings.HasPrefix(r.RequestURI, "/dynamic_chunk?") {
		if s.H2Ticket == nil || s.H2Ticket.Client == nil || s.SelectedDataStore == nil {
			http.Error(w, "chunk upload requires an authenticated backup session", http.StatusUnauthorized)
			return
		}
		request, err := parseChunkRequest(r)
		if err != nil {
			s3backuplog.WarnPrint("Rejected invalid chunk request: %s", err)
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		writer_mux.RLock()
		writer := s.Writers[request.WriterID]
		writer_mux.RUnlock()
		if writer == nil {
			err := fmt.Errorf("unknown writer id %d for chunk %s", request.WriterID, request.Digest)
			s3backuplog.ErrorPrint("%s", err)
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		ctx := r.Context()
		if s.ChunkS3Timeout > 0 {
			var cancel context.CancelFunc
			ctx, cancel = context.WithTimeout(ctx, s.ChunkS3Timeout)
			defer cancel()
		}

		started := time.Now()
		role := "leader"
		s3backuplog.DebugPrint(
			"chunk request started datastore=%s digest=%s writer=%d encoded_bytes=%d logical_bytes=%d",
			*s.SelectedDataStore,
			request.Digest,
			request.WriterID,
			request.EncodedSize,
			request.Size,
		)
		known, uploadErr := chunkUploads.Do(
			ctx,
			*s.SelectedDataStore+"/"+request.ObjectName,
			func() (bool, error) {
				known, err := storeChunk(
					ctx,
					s.H2Ticket.Client,
					*s.SelectedDataStore,
					request,
					r.Body,
					s.StorageClasses.Chunks,
				)
				s3backuplog.DebugPrint(
					"chunk leader finished datastore=%s digest=%s known=%t duration=%s err=%v",
					*s.SelectedDataStore,
					request.Digest,
					known,
					time.Since(started).Round(time.Millisecond),
					err,
				)
				return known, err
			},
			func() error {
				role = "follower"
				s3backuplog.DebugPrint(
					"duplicate chunk draining body datastore=%s digest=%s encoded_bytes=%d",
					*s.SelectedDataStore,
					request.Digest,
					request.EncodedSize,
				)
				return drainChunkBody(r.Body, request.EncodedSize)
			},
		)
		duration := time.Since(started)

		if uploadErr != nil {
			s3backuplog.ErrorPrint(
				"chunk request failed datastore=%s digest=%s writer=%d role=%s duration=%s err=%s",
				*s.SelectedDataStore,
				request.Digest,
				request.WriterID,
				role,
				duration.Round(time.Millisecond),
				uploadErr,
			)
			http.Error(w, uploadErr.Error(), http.StatusInternalServerError)
			return
		}
		if duration >= 30*time.Second {
			s3backuplog.WarnPrint(
				"slow chunk request datastore=%s digest=%s writer=%d role=%s duration=%s known=%t",
				*s.SelectedDataStore,
				request.Digest,
				request.WriterID,
				role,
				duration.Round(time.Millisecond),
				known,
			)
		} else {
			s3backuplog.DebugPrint(
				"chunk request completed datastore=%s digest=%s writer=%d role=%s duration=%s known=%t",
				*s.SelectedDataStore,
				request.Digest,
				request.WriterID,
				role,
				duration.Round(time.Millisecond),
				known,
			)
		}
		// Here chunk size is derived. Several chunks may arrive concurrently.
		atomic.CompareAndSwapUint64(&writer.Chunksize, 0, request.Size)

		// save the sizes required for offset calculation
		if strings.HasPrefix(r.RequestURI, "/dynamic_chunk?") {
			s3backuplog.DebugPrint("Adding digest %s to dynamic chunks list, size: %d", request.Digest, request.Size)
			writer.DynamicChunkSizes.Store(request.Digest, request.Size)
		}
		info := ChunkUploadInfo{}
		info.Digest = request.Digest
		info.Offset = 0 // todo
		info.Size = int64(request.Size)
		info.Known = known

		r := Response{Data: info}
		responsedata, _ := json.Marshal(r)
		w.WriteHeader(http.StatusOK)
		w.Header().Add("Content-Type", "application/json")
		w.Write(responsedata)
	}

	if strings.HasPrefix(r.RequestURI, "/blob?") && s.H2Ticket != nil {
		blobname := r.URL.Query().Get("file-name")
		esize, _ := strconv.Atoi(r.URL.Query().Get("encoded-size"))
		_, err := s.H2Ticket.Client.PutObject(
			context.Background(),
			*s.SelectedDataStore,
			s.Snapshot.S3Prefix()+"/"+blobname,
			r.Body,
			int64(esize),
			putOptions(s.StorageClasses.Backups, nil),
		)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte(err.Error()))
			s3backuplog.ErrorPrint("%s failed to upload blob to S3 bucket: %s", blobname, err.Error())
		}
	}

	if strings.HasPrefix(r.RequestURI, "/speedtest") && s.H2Ticket != nil {
		/*
		 * No data is uploaded during speedtest, its copied
		 * using io.Discard for less memory footprint
		 */
		io.Copy(io.Discard, r.Body)
		w.WriteHeader(http.StatusOK)
	}

	/* End of HTTP2 Backup API */

	/* HTTP2 Restore API */
	if strings.HasPrefix(r.RequestURI, "/download?") && s.H2Ticket != nil {
		if s.H2Ticket.Client == nil || s.SelectedDataStore == nil {
			http.Error(w, "download requires an authenticated restore session", http.StatusUnauthorized)
			return
		}
		blobname := r.URL.Query().Get("file-name")
		if blobname == "" {
			http.Error(w, "missing file-name", http.StatusBadRequest)
			return
		}
		obj, err := s.H2Ticket.Client.GetObject(
			r.Context(),
			*s.SelectedDataStore,
			s.Snapshot.S3Prefix()+"/"+blobname,
			minio.GetObjectOptions{},
		)
		if err != nil {
			w.WriteHeader(http.StatusNotFound)
			w.Write([]byte(err.Error()))
			return
		}
		defer obj.Close()
		st, err := obj.Stat()
		if err != nil {
			w.WriteHeader(http.StatusNotFound)
			s3backuplog.ErrorPrint(err.Error() + " " + s.Snapshot.S3Prefix() + "/" + blobname)
			io.WriteString(w, err.Error())
			return
		}

		w.Header().Add("Content-Length", fmt.Sprintf("%d", st.Size))
		w.WriteHeader(http.StatusOK)
		if _, err := io.Copy(w, obj); err != nil {
			s3backuplog.ErrorPrint("Restore download failed datastore=%s object=%s: %s", *s.SelectedDataStore, blobname, err)
		}
		return
	}

	if strings.HasPrefix(r.RequestURI, "/chunk?") && s.H2Ticket != nil {
		if s.H2Ticket.Client == nil || s.SelectedDataStore == nil {
			http.Error(w, "chunk download requires an authenticated restore session", http.StatusUnauthorized)
			return
		}
		digest := r.URL.Query().Get("digest")
		s3name, err := chunkObjectName(digest)
		if err != nil {
			s3backuplog.WarnPrint("Rejected invalid restore chunk request: %s", err)
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		obj, err := s.H2Ticket.Client.GetObject(
			r.Context(),
			*s.SelectedDataStore,
			s3name,
			minio.GetObjectOptions{},
		)
		if err != nil {
			w.WriteHeader(http.StatusNotFound)
			w.Write([]byte(err.Error()))
			s3backuplog.ErrorPrint("%s: Critical: Missing chunk on S3 bucket: %s", digest, err.Error())
			return
		}
		defer obj.Close()
		st, err := obj.Stat()
		if err != nil {
			w.WriteHeader(http.StatusNotFound)
			s3backuplog.ErrorPrint(err.Error() + " " + s3name)
			io.WriteString(w, err.Error())
			s3backuplog.ErrorPrint("%s: Critical: Missing chunk on S3 bucket: %s", digest, err.Error())
			return
		}

		w.Header().Add("Content-Length", fmt.Sprintf("%d", st.Size))
		w.WriteHeader(http.StatusOK)
		if _, err := io.Copy(w, obj); err != nil {
			s3backuplog.ErrorPrint("Restore chunk stream failed datastore=%s digest=%s: %s", *s.SelectedDataStore, digest, err)
		}
		return
	}

	/* End of HTTP 2 Restore API */

	if r.RequestURI == "/api2/json/admin/datastore" && r.Method == "GET" && auth {
		s3backuplog.DebugPrint("List buckets")
		bckts, fresh, err := bucketListCache.Get(
			C.AccessKeyID+"@"+C.Endpoint,
			func() ([]minio.BucketInfo, error) {
				return C.Client.ListBuckets(context.Background())
			},
		)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			s3backuplog.ErrorPrint("Failed to list buckets: %s", err.Error())
			io.WriteString(w, err.Error())
			return
		}
		if fresh {
			s3backuplog.DebugPrint("Refreshed bucket list from S3 endpoint")
		}

		datastores := make([]DataStore, 0)

		for _, b := range bckts {
			datastores = append(datastores, DataStore{Store: b.Name})
		}

		r := Response{Data: datastores}
		responsedata, _ := json.Marshal(r)
		w.Header().Add("Content-Type", "application/json")
		w.Write(responsedata)

	}

	if strings.HasPrefix(r.RequestURI, "//api2/json/backup") && auth {
		hj, ok := w.(http.Hijacker)
		if !ok {
			http.Error(w, "protocol upgrade is not supported by this connection", http.StatusInternalServerError)
			return
		}
		w.Header().Add("Upgrade", "proxmox-backup-protocol-v1")
		w.WriteHeader(http.StatusSwitchingProtocols)
		conn, _, err := hj.Hijack() //Here SSL/TCP connection is deowned from the HTTP1.1 server and passed to HTTP2 handler after sending headers telling the client that we are switching protocols
		if err != nil {
			s3backuplog.ErrorPrint("Backup protocol upgrade failed: %s", err)
			return
		}
		ss := s3pmoxcommon.Snapshot{}
		ss.InitWithQuery(r.URL.Query())
		store := r.URL.Query().Get("store")
		bcktype := r.URL.Query().Get("backup-type")
		s3backuplog.InfoPrint(
			"New Backup request from host [%s], ID: [%s], Type: [%s], Bucket: [%s], Timestamp: [%d]",
			r.RemoteAddr,
			ss.BackupID,
			bcktype,
			store,
			ss.BackupTime,
		)
		go s.backup(conn, C, store, ss)
	}

	if strings.HasPrefix(r.RequestURI, "//api2/json/reader") && auth {
		hj, ok := w.(http.Hijacker)
		if !ok {
			http.Error(w, "protocol upgrade is not supported by this connection", http.StatusInternalServerError)
			return
		}
		w.Header().Add("Upgrade", "proxmox-backup-protocol-v1")
		w.WriteHeader(http.StatusSwitchingProtocols)
		conn, _, err := hj.Hijack() //Here SSL/TCP connection is deowned from the HTTP1.1 server and passed to HTTP2 handler after sending headers telling the client that we are switching protocols
		if err != nil {
			s3backuplog.ErrorPrint("Restore protocol upgrade failed: %s", err)
			return
		}
		ss := s3pmoxcommon.Snapshot{}
		ss.InitWithQuery(r.URL.Query())
		store := r.URL.Query().Get("store")
		bcktype := r.URL.Query().Get("backup-type")
		s3backuplog.InfoPrint(
			"New Restore request from host [%s], ID: [%s], Type: [%s], Bucket: [%s]",
			r.RemoteAddr,
			ss.BackupID,
			bcktype,
			store,
		)
		go s.restore(conn, C, store, ss)
	}

	if (r.RequestURI == "//api2/json/access/ticket" || r.RequestURI == "/api2/json/access/ticket") && r.Method == "POST" {
		req := AccessTicketRequest{}
		if r.Header.Get("Content-Type") == "application/json" {
			body, err := io.ReadAll(r.Body)
			if err != nil {
				w.WriteHeader(http.StatusBadRequest)
				s3backuplog.ErrorPrint(err.Error())
				return
			}
			json.Unmarshal(body, &req)
		} else {
			err := r.ParseForm()
			if err != nil {
				w.WriteHeader(http.StatusBadRequest)
				s3backuplog.ErrorPrint("Failed to parse form: %s", err.Error())
				return
			}
			req.Password = r.FormValue("password")
			req.Username = r.FormValue("username")
		}
		ticket := AuthTicketResponsePayload{
			CSRFPreventionToken: "35h235h23yh23", //Not used at all being that used only for API
			Ticket:              RandStringBytes(64),
			Username:            req.Username,
		}
		resp := Response{Data: ticket}

		username := strings.Split(req.Username, "@")[0]
		te := TicketEntry{
			AccessKeyID:     username,
			SecretAccessKey: req.Password,
			Endpoint:        s.S3Endpoint,
			Expire:          uint64(time.Now().Unix()) + s.TicketExpire,
		}

		_, ok := connectionList[username]
		if ok {
			s3backuplog.DebugPrint("Re-using minio connection for username: %s", username)
		} else {
			s3backuplog.DebugPrint("Setup NEW minio connection for username: %s", username)
			minioClient, err := minio.New(te.Endpoint, &minio.Options{
				Creds:        credentials.NewStaticV4(te.AccessKeyID, te.SecretAccessKey, ""),
				Secure:       s.SecureFlag,
				BucketLookup: s3pmoxcommon.GetLookupType(s.LookupTypeFlag),
			})
			if err != nil {
				s3backuplog.ErrorPrint("Failed to initialize S3 Object: %s", err.Error())
				w.WriteHeader(http.StatusForbidden)
				w.Write([]byte(err.Error()))
				return
			}
			connectionList[username] = minioClient

			buckets, listerr := connectionList[username].ListBuckets(context.Background())
			if listerr != nil {
				delete(connectionList, username)
				w.WriteHeader(http.StatusForbidden)
				s3backuplog.ErrorPrint("Failed to list buckets: %s", listerr.Error())
				w.Write([]byte(listerr.Error()))
				return
			}
			bucketListCache.Store(username+"@"+te.Endpoint, buckets)

		}

		te.Client = connectionList[username]
		s.Auth.Store(ticket.Ticket, te)

		respbody, _ := json.Marshal(resp)
		s3backuplog.DebugPrint(string(respbody))
		w.Header().Add("Content-Type", "application/json")
		//w.Header().Add("Connection", "Close")
		w.WriteHeader(http.StatusOK)
		w.Write(respbody)
	}
}
