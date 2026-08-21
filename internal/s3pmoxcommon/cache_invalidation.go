package s3pmoxcommon

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync/atomic"
	"time"
)

var invalidationSequence atomic.Uint64

// CacheInvalidationTokenPath identifies a datastore without exposing endpoint
// or bucket names in the filesystem. The directory is intentionally supplied
// by the operator because the proxy and collector may run as different users
// or in different containers.
func CacheInvalidationTokenPath(directory string, endpoint string, datastore string) string {
	return filepath.Join(directory, DataStoreLockName(endpoint, datastore)+".invalidate")
}

// PublishCacheInvalidation atomically changes a small local token. It performs
// no S3 operation; another proxy process sharing the directory observes the
// token on its next API request.
func PublishCacheInvalidation(directory string, endpoint string, datastore string) (string, error) {
	if directory == "" {
		return "", nil
	}
	if err := os.MkdirAll(directory, 0o750); err != nil {
		return "", fmt.Errorf("create cache invalidation directory: %w", err)
	}
	token := fmt.Sprintf("%d-%d-%d", time.Now().UnixNano(), os.Getpid(), invalidationSequence.Add(1))
	temporary, err := os.CreateTemp(directory, ".invalidate-*")
	if err != nil {
		return "", fmt.Errorf("create cache invalidation token: %w", err)
	}
	temporaryName := temporary.Name()
	defer os.Remove(temporaryName)
	if err := temporary.Chmod(0o640); err != nil {
		temporary.Close()
		return "", fmt.Errorf("secure cache invalidation token: %w", err)
	}
	if _, err := temporary.WriteString(token); err != nil {
		temporary.Close()
		return "", fmt.Errorf("write cache invalidation token: %w", err)
	}
	if err := temporary.Close(); err != nil {
		return "", fmt.Errorf("close cache invalidation token: %w", err)
	}
	if err := os.Rename(temporaryName, CacheInvalidationTokenPath(directory, endpoint, datastore)); err != nil {
		return "", fmt.Errorf("publish cache invalidation token: %w", err)
	}
	return token, nil
}

func ReadCacheInvalidation(directory string, endpoint string, datastore string) (string, error) {
	if directory == "" {
		return "", nil
	}
	data, err := os.ReadFile(CacheInvalidationTokenPath(directory, endpoint, datastore))
	if errors.Is(err, os.ErrNotExist) {
		return "", nil
	}
	if err != nil {
		return "", fmt.Errorf("read cache invalidation token: %w", err)
	}
	return string(data), nil
}
