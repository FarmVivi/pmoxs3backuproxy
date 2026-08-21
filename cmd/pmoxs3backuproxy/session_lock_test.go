package main

import (
	"errors"
	"testing"

	"github.com/juju/mutex/v2"
)

type countingReleaser struct {
	releases int
}

func (r *countingReleaser) Release() { r.releases++ }

func TestDataStoreSessionLocksAreScopedAndReferenceCounted(t *testing.T) {
	originalAcquire := acquireProcessMutex
	defer func() { acquireProcessMutex = originalAcquire }()

	acquired := make(map[string]int)
	releasers := make(map[string]*countingReleaser)
	acquireProcessMutex = func(spec mutex.Spec) (mutex.Releaser, error) {
		acquired[spec.Name]++
		releaser := &countingReleaser{}
		releasers[spec.Name] = releaser
		return releaser, nil
	}

	server := &Server{}
	first, err := server.beginDataStoreSession("s3.example:443", "bucket-a")
	if err != nil {
		t.Fatal(err)
	}
	second, err := server.beginDataStoreSession("s3.example:443", "bucket-a")
	if err != nil {
		t.Fatal(err)
	}
	other, err := server.beginDataStoreSession("s3.example:443", "bucket-b")
	if err != nil {
		t.Fatal(err)
	}
	if first != second || first == other {
		t.Fatalf("lock names first=%q second=%q other=%q", first, second, other)
	}
	if acquired[first] != 1 || acquired[other] != 1 || server.Sessions != 3 {
		t.Fatalf("acquired=%+v sessions=%d", acquired, server.Sessions)
	}

	server.endDataStoreSession(first)
	if releasers[first].releases != 0 || server.Sessions != 2 {
		t.Fatalf("shared lock released early: releases=%d sessions=%d", releasers[first].releases, server.Sessions)
	}
	server.endDataStoreSession(second)
	server.endDataStoreSession(other)
	if releasers[first].releases != 1 || releasers[other].releases != 1 || server.Sessions != 0 || len(server.SessionLocks) != 0 {
		t.Fatalf("releasers=%+v sessions=%d locks=%+v", releasers, server.Sessions, server.SessionLocks)
	}
}

func TestDataStoreSessionAcquireFailureDoesNotCountSession(t *testing.T) {
	originalAcquire := acquireProcessMutex
	defer func() { acquireProcessMutex = originalAcquire }()
	wantErr := errors.New("busy")
	acquireProcessMutex = func(mutex.Spec) (mutex.Releaser, error) { return nil, wantErr }

	server := &Server{}
	if _, err := server.beginDataStoreSession("s3.example:443", "bucket"); !errors.Is(err, wantErr) {
		t.Fatalf("got %v, want %v", err, wantErr)
	}
	if server.Sessions != 0 || len(server.SessionLocks) != 0 {
		t.Fatalf("failed acquisition changed state: sessions=%d locks=%+v", server.Sessions, server.SessionLocks)
	}
}
