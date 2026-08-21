package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"
	"tizbac/pmoxs3backuproxy/internal/s3backuplog"
	"tizbac/pmoxs3backuproxy/internal/s3pmoxcommon"

	"github.com/juju/clock"
	"github.com/juju/mutex/v2"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

var (
	version = "dev"
	commit  = "none"
	date    = "unknown"
)

func compareSum(csum []byte, index []byte, metadatasum string) error {
	fileChecksum := hex.EncodeToString(csum)
	shaSum := sha256.Sum256(index)
	checksum := hex.EncodeToString(shaSum[:])

	if fileChecksum != checksum || fileChecksum != metadatasum {
		return errors.New(
			fmt.Sprintf(
				"Corrupted index file: Checksum in index [%s] or metadata sum [%s] does not match calculated checksum [%s]",
				fileChecksum,
				metadatasum,
				checksum,
			),
		)
	}

	return nil
}

func getObjectMetadata(ctx context.Context, bucketFlag string, object minio.ObjectInfo, minioClient *minio.Client) (string, error) {
	s3backuplog.DebugPrint("User Metadata content: [%s]", object.UserMetadata)
	csum := object.UserMetadata["X-Amz-Meta-Csum"]
	if csum == "" {
		s3backuplog.WarnPrint("No metadata found for %s, retry with StatObject", object.Key)

		statObject, err := minioClient.StatObject(ctx, bucketFlag, object.Key, minio.StatObjectOptions{})
		if err != nil {
			return "", fmt.Errorf("%s: unable to stat object: %w", object.Key, err)
		}
		s3backuplog.DebugPrint("StatObject User Metadata content: [%s]", statObject.UserMetadata)
		csum = statObject.UserMetadata["Csum"]
	}

	if csum == "" {
		return "", fmt.Errorf("%s: object has no csum metadata flag set", object.Key)
	}

	return csum, nil
}

// withinGracePeriod reports whether an unreferenced chunk is too young to be
// removed safely.
//
// Chunks are uploaded before the index that references them is written, so a
// chunk belonging to a running backup is indistinguishable from an orphan.
// Keeping the recent ones lets the next run collect them, once their index
// exists.
func withinGracePeriod(lastModified time.Time, now time.Time, grace time.Duration) bool {
	if grace <= 0 {
		return false
	}
	return lastModified.After(now.Add(-grace))
}

func main() {
	var printVersion bool
	endpointFlag := flag.String("endpoint", "", "S3 Endpoint without https/http , host:port")
	secureFlag := flag.Bool("usessl", false, "Use SSL for endpoint connection: default: false")
	bucketFlag := flag.String("bucket", "", "Bucket to perform garbage collection on")
	accessKeyID := flag.String("accesskey", "", "S3 Access Key ID")
	secretKey := flag.String("secretkey", "", "S3 Secret Key, discouraged , use a file if possible")
	secretKeyFile := flag.String("secretkeyfile", "", "S3 Secret Key File")
	retentionDays := flag.Uint("retention", 60, "Number of days to keep backups for")
	chunkGraceHours := flag.Uint(
		"chunkgrace", 24,
		"Hours a chunk is protected from orphan removal after being written, 0 disables the protection",
	)
	flag.BoolVar(&printVersion, "version", false, "Show version and exit")
	flag.BoolVar(&printVersion, "v", false, "Show version and exit")

	lookupTypeFlag := flag.String("lookuptype", "auto", "Bucket lookup type: auto,dns,path")
	debug := flag.Bool("debug", false, "Debug logging")
	flag.Parse()
	if printVersion {
		fmt.Println(version)
		os.Exit(0)
	}
	s3backuplog.InfoPrint("%s %s %s %s", os.Args[0], version, commit, date)
	if *endpointFlag == "" || *accessKeyID == "" || (*secretKey == "" && *secretKeyFile == "") || *bucketFlag == "" {
		flag.Usage()
		os.Exit(1)
	}
	if *debug {
		s3backuplog.EnableDebug()
	}
	gracePeriod, err := hoursDuration(*chunkGraceHours)
	if err != nil {
		s3backuplog.FatalPrint("Invalid chunk grace period: %s", err)
	}

	skey := *secretKey
	if skey == "" {
		data, err := os.ReadFile(*secretKeyFile)
		if err != nil {
			s3backuplog.FatalPrint("Reading key file %s : %s", *secretKeyFile, err.Error())
		}
		skey = string(data)
		skey = strings.Trim(skey, " \r\t\n")
	}

	minioClient, minioerr := minio.New(*endpointFlag, &minio.Options{
		Creds:        credentials.NewStaticV4(*accessKeyID, skey, ""),
		Secure:       (*secureFlag),
		BucketLookup: s3pmoxcommon.GetLookupType(*lookupTypeFlag),
	})
	if minioerr != nil {
		s3backuplog.FatalPrint("Creating S3 Client: %s", minioerr.Error())
	}

	s3backuplog.InfoPrint("Acquire Lock")
	lockname := s3pmoxcommon.DataStoreLockName(*endpointFlag, *bucketFlag)
	sp := mutex.Spec{
		Clock:   clock.WallClock,
		Name:    lockname,
		Delay:   time.Millisecond,
		Timeout: time.Second * 30,
	}

	SessionsRelease, err := mutex.Acquire(sp)
	if err != nil {
		s3backuplog.FatalPrint("Failed to acquire Lock for %s: %s", lockname, err.Error())
	}
	s3backuplog.DebugPrint("Locked %s", lockname)

	ctx := context.Background()
	bucket, staterr := minioClient.BucketExists(ctx, *bucketFlag)
	if staterr != nil {
		s3backuplog.FatalPrint("Unable to access specified bucket: %s", staterr.Error())
	}
	if !bucket {
		s3backuplog.FatalPrint("Specified bucket [%s] does not exist", *bucketFlag)
	}

	// Inventory and mark phase. Every listing is fully materialised and every
	// index validated before the first DELETE request is allowed.
	s3backuplog.InfoPrint("Building complete garbage-collection inventory")
	backupObjects, err := listObjectsFully(ctx, minioClient, *bucketFlag, minio.ListObjectsOptions{
		Recursive: true, Prefix: "backups/", WithMetadata: true,
	})
	if err != nil {
		s3backuplog.FatalPrint("Unable to list backup objects safely: %s", err)
	}
	snapshots, err := s3pmoxcommon.SnapshotsFromObjects(backupObjects, *bucketFlag, true)
	if err != nil {
		s3backuplog.FatalPrint("Unable to parse snapshots safely: %s", err)
	}
	if len(snapshots) == 0 {
		s3backuplog.InfoPrint("No snapshots found in bucket; refusing to infer that every chunk is orphaned")
		SessionsRelease.Release()
		return
	}
	indexedObjects, err := listObjectsFully(ctx, minioClient, *bucketFlag, minio.ListObjectsOptions{
		Recursive: true, Prefix: "indexed/", WithMetadata: true,
	})
	if err != nil {
		s3backuplog.FatalPrint("Unable to list copied indexes safely: %s", err)
	}
	chunkObjects, err := listObjectsFully(ctx, minioClient, *bucketFlag, minio.ListObjectsOptions{
		Recursive: true, Prefix: "chunks/",
	})
	if err != nil {
		s3backuplog.FatalPrint("Unable to list chunks safely: %s", err)
	}
	markTime := time.Now()
	if err := refreshExpiredSnapshotProtection(
		ctx, minioClient, *bucketFlag, snapshots, backupObjects, markTime, uint64(*retentionDays),
	); err != nil {
		s3backuplog.FatalPrint("Unable to verify snapshot protection; nothing was deleted: %s", err)
	}

	plan, err := buildGCPlan(
		ctx,
		snapshots,
		backupObjects,
		indexedObjects,
		chunkObjects,
		markTime,
		uint64(*retentionDays),
		gracePeriod,
		func(ctx context.Context, object minio.ObjectInfo) (parsedIndex, error) {
			return loadIndexFromS3(ctx, minioClient, *bucketFlag, object)
		},
		func(ctx context.Context, object minio.ObjectInfo) (string, error) {
			return getObjectMetadata(ctx, *bucketFlag, object, minioClient)
		},
	)
	if err != nil {
		s3backuplog.FatalPrint("Garbage-collection mark phase failed; nothing was deleted: %s", err)
	}

	// Missing referenced chunks mean the inventory is corrupt or inconsistent.
	// Mark affected snapshots, but never sweep anything during that run.
	if len(plan.missingChunks) > 0 {
		for digest, references := range plan.missingChunks {
			s3backuplog.ErrorPrint(
				"Corruption detected, chunk %s, referenced by %s is missing",
				digest,
				strings.Join(references, ","),
			)
			for _, objectName := range references {
				parts := strings.Split(objectName, "/")
				basePath := strings.Join(parts[:len(parts)-1], "/")
				reader := strings.NewReader("CORRUPTED")
				if _, err := minioClient.PutObject(
					ctx, *bucketFlag, basePath+"/corrupted", reader, 9, minio.PutObjectOptions{},
				); err != nil {
					s3backuplog.FatalPrint("Error tagging %s as corrupt: %s", objectName, err)
				}
			}
		}
		s3backuplog.FatalPrint(
			"Integrity check found %d missing referenced chunks; nothing was deleted",
			len(plan.missingChunks),
		)
	}

	s3backuplog.InfoPrint(
		"GC plan validated: %d snapshots (%d objects), %d copied indexes and %d chunks to remove; %d chunks referenced",
		len(plan.expiredSnapshots),
		len(plan.backupObjects),
		len(plan.indexedObjects),
		len(plan.chunkObjects),
		len(plan.knownChunks),
	)
	for _, snapshot := range plan.expiredSnapshots {
		s3backuplog.InfoPrint("Backup %s is older than %d days, deleting", snapshot.S3Prefix(), *retentionDays)
	}
	if err := executeGCPlan(ctx, minioClient, *bucketFlag, plan); err != nil {
		s3backuplog.FatalPrint("Garbage-collection sweep failed: %s", err)
	}
	if plan.protectedByGrace > 0 {
		s3backuplog.InfoPrint(
			"%d unreferenced chunks kept, written less than %d hours ago",
			plan.protectedByGrace,
			*chunkGraceHours,
		)
	}
	writeUsageStats(
		ctx,
		minioClient,
		*bucketFlag,
		plan.knownChunks,
		plan.chunkSizes,
		plan.archiveSizes,
	)

	s3backuplog.InfoPrint("Finished")
	SessionsRelease.Release()
}
