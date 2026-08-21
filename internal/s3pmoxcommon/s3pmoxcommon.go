package s3pmoxcommon

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"tizbac/pmoxs3backuproxy/internal/s3backuplog"

	"github.com/minio/minio-go/v7"
)

// DataStoreLockName is shared by the proxy and collector. Both processes must
// also use a common TMPDIR because juju/mutex stores this named lock there.
func DataStoreLockName(endpoint string, datastore string) string {
	h := sha256.Sum256([]byte(endpoint + "|" + datastore))
	return "PBSS3" + hex.EncodeToString(h[:])[:16]
}

func ListSnapshots(c minio.Client, datastore string, returnCorrupted bool) ([]Snapshot, error) {
	ctx := context.Background()
	objects := make([]minio.ObjectInfo, 0)
	for object := range c.ListObjects(
		ctx, datastore,
		minio.ListObjectsOptions{Recursive: true, Prefix: "backups/",
			WithMetadata: true,
		}) {
		if object.Err != nil {
			return nil, fmt.Errorf("list snapshots in %s: %w", datastore, object.Err)
		}
		objects = append(objects, object)
	}
	return SnapshotsFromObjects(objects, datastore, returnCorrupted)
}

// SnapshotsFromObjects builds snapshots from a complete backups/ listing.
// Callers must never pass a partial listing: garbage collection decisions rely
// on every index being visible.
func SnapshotsFromObjects(objects []minio.ObjectInfo, datastore string, returnCorrupted bool) ([]Snapshot, error) {
	byPrefix := make(map[string]*Snapshot)
	order := make([]string, 0)
	for _, object := range objects {
		if object.Err != nil {
			return nil, fmt.Errorf("snapshot listing contains an error: %w", object.Err)
		}
		if !strings.HasPrefix(object.Key, "backups/") {
			continue
		}
		rest := strings.TrimPrefix(object.Key, "backups/")
		parts := strings.SplitN(rest, "/", 2)
		if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
			return nil, fmt.Errorf("invalid snapshot object key %q", object.Key)
		}
		fields := strings.Split(parts[0], "|")
		if len(fields) != 3 || fields[0] == "" || fields[2] == "" {
			return nil, fmt.Errorf("invalid snapshot prefix %q", parts[0])
		}
		backupTime, err := strconv.ParseUint(fields[1], 10, 64)
		if err != nil {
			return nil, fmt.Errorf("invalid snapshot timestamp in %q: %w", parts[0], err)
		}

		snapshot, ok := byPrefix[parts[0]]
		if !ok {
			snapshot = &Snapshot{
				BackupID:   fields[0],
				BackupTime: backupTime,
				BackupType: fields[2],
				Files:      make([]SnapshotFile, 0),
				Datastore:  datastore,
			}
			byPrefix[parts[0]] = snapshot
			order = append(order, parts[0])
		}
		if parts[1] == "corrupted" {
			snapshot.corrupted = true
		}
		if object.UserTags["protected"] == "true" {
			snapshot.Protected = true
		}
		if encodedNote := object.UserTags["note"]; encodedNote != "" {
			note, err := base64.RawStdEncoding.DecodeString(encodedNote)
			// Notes are cosmetic metadata and older S3 implementations may expose
			// a malformed value. Preserve the historical behaviour: a bad note
			// must not hide an otherwise valid backup or block the GC mark phase.
			if err == nil {
				snapshot.Comment = string(note)
			}
		}
		if !strings.HasSuffix(parts[1], ".csjson") {
			snapshot.Files = append(snapshot.Files, SnapshotFile{
				Filename:  parts[1],
				CryptMode: "none", // TODO: expose the actual encryption mode.
				Size:      uint64(object.Size),
			})
		}
	}

	result := make([]Snapshot, 0, len(order))
	for _, prefix := range order {
		snapshot := byPrefix[prefix]
		if returnCorrupted || !snapshot.corrupted {
			result = append(result, *snapshot)
		}
	}
	return result, nil
}

func GetLatestSnapshot(c minio.Client, ds string, id string, time uint64) (*Snapshot, error) {
	snapshots, err := ListSnapshots(c, ds, false)
	if err != nil {
		s3backuplog.ErrorPrint(err.Error())
		return nil, err
	}

	if len(snapshots) == 0 {
		return nil, nil
	}

	var mostRecent = &Snapshot{}
	mostRecent = nil
	for _, sl := range snapshots {
		if sl.BackupTime == time && sl.BackupID == id {
			s3backuplog.DebugPrint("GetLatestSnapshot: ignoring currently processed snapshot.")
			continue
		}
		if (mostRecent == nil || sl.BackupTime > mostRecent.BackupTime) && id == sl.BackupID {
			mostRecent = &sl
		}
	}

	if mostRecent == nil {
		return nil, nil
	}

	return mostRecent, nil
}

func (S *Snapshot) InitWithQuery(v url.Values) {
	S.BackupID = v.Get("backup-id")
	S.BackupTime, _ = strconv.ParseUint(v.Get("backup-time"), 10, 64)
	S.BackupType = v.Get("backup-type")
}

func (S *Snapshot) InitWithForm(r *http.Request) {
	S.BackupID = r.FormValue("backup-id")
	S.BackupTime, _ = strconv.ParseUint(r.FormValue("backup-time"), 10, 64)
	S.BackupType = r.FormValue("backup-type")
}

func (S *Snapshot) S3Prefix() string {
	return fmt.Sprintf("backups/%s|%d|%s", S.BackupID, S.BackupTime, S.BackupType)
}

func (S *Snapshot) GetFiles(c minio.Client) {
	for object := range c.ListObjects(
		context.Background(), S.Datastore,
		minio.ListObjectsOptions{Recursive: true, Prefix: S.S3Prefix()},
	) {
		file := SnapshotFile{}
		path := strings.Split(object.Key, "/")
		file.Filename = path[2]
		file.CryptMode = "none"
		file.Size = uint64(object.Size)
		S.Files = append(S.Files, file)
	}
}

func (S *Snapshot) ReadTags(c minio.Client) (map[string]string, error) {
	existingTags, err := c.GetObjectTagging(
		context.Background(),
		S.Datastore,
		S.S3Prefix()+"/index.json.blob",
		minio.GetObjectTaggingOptions{},
	)
	if err != nil {
		s3backuplog.ErrorPrint("Unable to get tags: %s", err.Error())
		return nil, err
	}
	return existingTags.ToMap(), nil
}

func (S *Snapshot) Delete(c minio.Client) error {
	ctx := context.Background()
	objects := make([]minio.ObjectInfo, 0)
	// The trailing slash makes the boundary explicit. Without it, an unusual
	// backup type whose name starts with another type could share the prefix.
	prefix := S.S3Prefix() + "/"
	for object := range c.ListObjects(ctx, S.Datastore, minio.ListObjectsOptions{Prefix: prefix, Recursive: true}) {
		if object.Err != nil {
			return fmt.Errorf("list snapshot %s before deletion: %w", S.S3Prefix(), object.Err)
		}
		objects = append(objects, object)
	}

	objectsCh := make(chan minio.ObjectInfo)
	go func() {
		defer close(objectsCh)
		for _, object := range objects {
			objectsCh <- object
		}
	}()
	errorCh := c.RemoveObjects(ctx, S.Datastore, objectsCh, minio.RemoveObjectsOptions{})
	var deleteErrors []error
	for e := range errorCh {
		s3backuplog.ErrorPrint("Failed to remove " + e.ObjectName + ", error: " + e.Err.Error())
		deleteErrors = append(deleteErrors, fmt.Errorf("remove %s: %w", e.ObjectName, e.Err))
	}
	return errors.Join(deleteErrors...)
}

func GetLookupType(Typeflag string) minio.BucketLookupType {
	switch Typeflag {
	case "path":
		return minio.BucketLookupPath
	case "dns":
		return minio.BucketLookupDNS
	default:
		return minio.BucketLookupAuto
	}
}
