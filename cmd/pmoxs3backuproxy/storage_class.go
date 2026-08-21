package main

import "github.com/minio/minio-go/v7"

// storageClassPolicy controls the S3 storage class of newly-created objects.
// Empty values deliberately let the provider apply its own default, preserving
// the proxy's historical behaviour.
type storageClassPolicy struct {
	Chunks  string
	Backups string
	Indexed string
}

func putOptions(storageClass string, userMetadata map[string]string) minio.PutObjectOptions {
	return minio.PutObjectOptions{
		StorageClass: storageClass,
		UserMetadata: userMetadata,
	}
}

// indexedCopyOptions preserves the checksum metadata used by incremental
// backups while optionally selecting a class for the indexed/ destination.
// MinIO exposes x-amz-storage-class through CopyDestOptions.UserMetadata.
func indexedCopyOptions(bucket, object, checksum, storageClass string) minio.CopyDestOptions {
	opts := minio.CopyDestOptions{Bucket: bucket, Object: object}
	if storageClass == "" {
		return opts
	}
	opts.ReplaceMetadata = true
	opts.UserMetadata = map[string]string{
		"csum":                checksum,
		"x-amz-storage-class": storageClass,
	}
	return opts
}
