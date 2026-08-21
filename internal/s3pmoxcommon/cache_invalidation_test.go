package s3pmoxcommon

import (
	"os"
	"path/filepath"
	"testing"
)

func TestCacheInvalidationRoundTrip(t *testing.T) {
	directory := t.TempDir()
	token, err := PublishCacheInvalidation(directory, "s3.example:443", "bucket")
	if err != nil {
		t.Fatalf("publish invalidation: %v", err)
	}
	got, err := ReadCacheInvalidation(directory, "s3.example:443", "bucket")
	if err != nil {
		t.Fatalf("read invalidation: %v", err)
	}
	if got != token || got == "" {
		t.Fatalf("got token %q, want %q", got, token)
	}
	if info, err := os.Stat(CacheInvalidationTokenPath(directory, "s3.example:443", "bucket")); err != nil {
		t.Fatalf("stat invalidation: %v", err)
	} else if info.Mode().Perm() != 0o640 {
		t.Fatalf("token mode is %o, want 640", info.Mode().Perm())
	}
	leftovers, err := filepath.Glob(filepath.Join(directory, ".invalidate-*"))
	if err != nil || len(leftovers) != 0 {
		t.Fatalf("temporary invalidation files left behind: %v, err=%v", leftovers, err)
	}
}

func TestCacheInvalidationDisabled(t *testing.T) {
	if token, err := PublishCacheInvalidation("", "endpoint", "bucket"); err != nil || token != "" {
		t.Fatalf("disabled publish returned token=%q err=%v", token, err)
	}
	if token, err := ReadCacheInvalidation("", "endpoint", "bucket"); err != nil || token != "" {
		t.Fatalf("disabled read returned token=%q err=%v", token, err)
	}
}
