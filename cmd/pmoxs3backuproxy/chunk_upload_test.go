package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/minio/minio-go/v7"
)

const testDigest = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"

func chunkTestRequest(t *testing.T, encodedSize string, size string, wid string, digest string, contentLength int64) *http.Request {
	t.Helper()
	q := make(url.Values)
	q.Set("encoded-size", encodedSize)
	q.Set("size", size)
	q.Set("wid", wid)
	q.Set("digest", digest)
	return &http.Request{
		URL:           &url.URL{RawQuery: q.Encode()},
		ContentLength: contentLength,
	}
}

func TestParseChunkRequest(t *testing.T) {
	r := chunkTestRequest(t, "1024", "4194304", "7", testDigest, 1024)
	got, err := parseChunkRequest(r)
	if err != nil {
		t.Fatalf("parse valid request: %v", err)
	}
	if got.Digest != testDigest || got.EncodedSize != 1024 || got.Size != 4194304 || got.WriterID != 7 {
		t.Fatalf("unexpected parsed request: %+v", got)
	}
	wantObject := "chunks/01/23/456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
	if got.ObjectName != wantObject {
		t.Fatalf("object name %q, want %q", got.ObjectName, wantObject)
	}
}

func TestParseChunkRequestRejectsMalformedInput(t *testing.T) {
	tests := []struct {
		name          string
		encodedSize   string
		size          string
		wid           string
		digest        string
		contentLength int64
	}{
		{"empty digest", "1", "1", "1", "", 1},
		{"short digest", "1", "1", "1", "00", 1},
		{"non hexadecimal digest", "1", "1", "1", strings.Repeat("z", 64), 1},
		{"missing encoded size", "", "1", "1", testDigest, -1},
		{"negative encoded size", "-1", "1", "1", testDigest, -1},
		{"zero encoded size", "0", "1", "1", testDigest, 0},
		{"content length mismatch", "2", "1", "1", testDigest, 1},
		{"missing logical size", "1", "", "1", testDigest, 1},
		{"zero logical size", "1", "0", "1", testDigest, 1},
		{"invalid writer", "1", "1", "invalid", testDigest, 1},
		{"negative writer", "1", "1", "-1", testDigest, 1},
		{"writer overflow", "1", "1", "2147483648", testDigest, 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := chunkTestRequest(t, tt.encodedSize, tt.size, tt.wid, tt.digest, tt.contentLength)
			if _, err := parseChunkRequest(r); err == nil {
				t.Fatal("expected malformed request to fail")
			}
		})
	}
}

func TestDrainChunkBody(t *testing.T) {
	if err := drainChunkBody(strings.NewReader("abcd"), 4); err != nil {
		t.Fatalf("exact body rejected: %v", err)
	}
	if err := drainChunkBody(strings.NewReader("abc"), 4); err == nil {
		t.Fatal("short body accepted")
	}
	if err := drainChunkBody(strings.NewReader("abcde"), 4); err == nil {
		t.Fatal("long body accepted")
	}
	if err := drainChunkBody(errorReader{}, 1); err == nil {
		t.Fatal("reader error accepted")
	}
}

type errorReader struct{}

func (errorReader) Read([]byte) (int, error) { return 0, errors.New("read failed") }

type fakeChunkStore struct {
	mu       sync.Mutex
	exists   bool
	statErr  error
	putErr   error
	puts     int
	putBytes []byte
	putStart chan struct{}
	putWait  chan struct{}
}

type contextChunkStore struct {
	blockStat bool
}

func (s contextChunkStore) StatObject(
	ctx context.Context,
	_ string,
	_ string,
	_ minio.StatObjectOptions,
) (minio.ObjectInfo, error) {
	if s.blockStat {
		<-ctx.Done()
		return minio.ObjectInfo{}, ctx.Err()
	}
	return minio.ObjectInfo{}, minio.ErrorResponse{Code: "NoSuchKey", StatusCode: http.StatusNotFound}
}

func (contextChunkStore) PutObject(
	ctx context.Context,
	_ string,
	_ string,
	_ io.Reader,
	_ int64,
	_ minio.PutObjectOptions,
) (minio.UploadInfo, error) {
	<-ctx.Done()
	return minio.UploadInfo{}, ctx.Err()
}

func (s *fakeChunkStore) StatObject(
	context.Context,
	string,
	string,
	minio.StatObjectOptions,
) (minio.ObjectInfo, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.statErr != nil {
		return minio.ObjectInfo{}, s.statErr
	}
	if !s.exists {
		return minio.ObjectInfo{}, minio.ErrorResponse{Code: "NoSuchKey", StatusCode: http.StatusNotFound}
	}
	return minio.ObjectInfo{Key: "existing"}, nil
}

func (s *fakeChunkStore) PutObject(
	_ context.Context,
	_ string,
	_ string,
	r io.Reader,
	size int64,
	_ minio.PutObjectOptions,
) (minio.UploadInfo, error) {
	if s.putStart != nil {
		close(s.putStart)
	}
	if s.putWait != nil {
		<-s.putWait
	}
	b, err := io.ReadAll(io.LimitReader(r, size))
	if err != nil {
		return minio.UploadInfo{}, err
	}
	s.mu.Lock()
	s.puts++
	s.putBytes = append([]byte(nil), b...)
	putErr := s.putErr
	if putErr == nil {
		s.exists = true
	}
	s.mu.Unlock()
	return minio.UploadInfo{}, putErr
}

func requestForBody(size int) chunkRequest {
	return chunkRequest{
		Digest:      testDigest,
		EncodedSize: int64(size),
		Size:        4 << 20,
		WriterID:    1,
		ObjectName:  "chunks/01/23/rest",
	}
}

func TestStoreChunkUploadsMissingObject(t *testing.T) {
	store := &fakeChunkStore{}
	body := []byte("new chunk")
	known, err := storeChunk(context.Background(), store, "bucket", requestForBody(len(body)), bytes.NewReader(body))
	if err != nil || known {
		t.Fatalf("got known=%v err=%v, want new chunk success", known, err)
	}
	if store.puts != 1 || !bytes.Equal(store.putBytes, body) {
		t.Fatalf("put state: count=%d body=%q", store.puts, store.putBytes)
	}
}

func TestStoreChunkDrainsKnownObjectWithoutPut(t *testing.T) {
	store := &fakeChunkStore{exists: true}
	body := []byte("known chunk")
	known, err := storeChunk(context.Background(), store, "bucket", requestForBody(len(body)), bytes.NewReader(body))
	if err != nil || !known {
		t.Fatalf("got known=%v err=%v, want known chunk success", known, err)
	}
	if store.puts != 0 {
		t.Fatalf("known object uploaded %d times", store.puts)
	}
}

func TestStoreChunkPropagatesEveryFailure(t *testing.T) {
	t.Run("unexpected stat error", func(t *testing.T) {
		store := &fakeChunkStore{statErr: minio.ErrorResponse{Code: "AccessDenied", StatusCode: http.StatusForbidden}}
		if _, err := storeChunk(context.Background(), store, "bucket", requestForBody(1), strings.NewReader("x")); err == nil || !strings.Contains(err.Error(), "stat chunk") {
			t.Fatalf("unexpected error: %v", err)
		}
		if store.puts != 0 {
			t.Fatal("put attempted after stat failure")
		}
	})
	t.Run("put error", func(t *testing.T) {
		store := &fakeChunkStore{putErr: errors.New("put failed")}
		if _, err := storeChunk(context.Background(), store, "bucket", requestForBody(1), strings.NewReader("x")); err == nil || !strings.Contains(err.Error(), "put chunk") {
			t.Fatalf("unexpected error: %v", err)
		}
	})
	t.Run("short uploaded body", func(t *testing.T) {
		store := &fakeChunkStore{}
		if _, err := storeChunk(context.Background(), store, "bucket", requestForBody(2), strings.NewReader("x")); err == nil || !strings.Contains(err.Error(), "consumed 1") {
			t.Fatalf("unexpected error: %v", err)
		}
	})
	t.Run("trailing uploaded body", func(t *testing.T) {
		store := &fakeChunkStore{}
		if _, err := storeChunk(context.Background(), store, "bucket", requestForBody(1), strings.NewReader("xy")); err == nil || !strings.Contains(err.Error(), "trailing encoded bytes") {
			t.Fatalf("unexpected error: %v", err)
		}
	})
	t.Run("known body read error", func(t *testing.T) {
		store := &fakeChunkStore{exists: true}
		if _, err := storeChunk(context.Background(), store, "bucket", requestForBody(1), errorReader{}); err == nil || !strings.Contains(err.Error(), "drain known") {
			t.Fatalf("unexpected error: %v", err)
		}
	})
}

func TestStoreChunkHonorsContextCancellation(t *testing.T) {
	for _, tt := range []struct {
		name  string
		store contextChunkStore
	}{
		{"stat", contextChunkStore{blockStat: true}},
		{"put", contextChunkStore{}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
			defer cancel()
			_, err := storeChunk(ctx, tt.store, "bucket", requestForBody(1), strings.NewReader("x"))
			if !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("got %v, want context deadline", err)
			}
		})
	}
}

func TestChunkFlightCoalescesSameKeyAndDrainsFollower(t *testing.T) {
	var group chunkFlightGroup
	leaderStarted := make(chan struct{})
	releaseLeader := make(chan struct{})
	followerDrained := make(chan struct{})

	leaderResult := make(chan error, 1)
	go func() {
		_, err := group.Do(context.Background(), "same", func() (bool, error) {
			close(leaderStarted)
			<-releaseLeader
			return false, nil
		}, func() error {
			return errors.New("leader used follower callback")
		})
		leaderResult <- err
	}()
	<-leaderStarted

	followerResult := make(chan struct {
		known bool
		err   error
	}, 1)
	go func() {
		known, err := group.Do(context.Background(), "same", func() (bool, error) {
			return false, errors.New("follower became leader")
		}, func() error {
			close(followerDrained)
			return nil
		})
		followerResult <- struct {
			known bool
			err   error
		}{known, err}
	}()

	select {
	case <-followerDrained:
	case <-time.After(time.Second):
		t.Fatal("follower waited without draining its body")
	}
	select {
	case <-followerResult:
		t.Fatal("follower returned before leader completed")
	default:
	}

	close(releaseLeader)
	if err := <-leaderResult; err != nil {
		t.Fatalf("leader failed: %v", err)
	}
	result := <-followerResult
	if result.err != nil || !result.known {
		t.Fatalf("follower got known=%v err=%v", result.known, result.err)
	}
}

func TestChunkFlightAllowsDifferentKeys(t *testing.T) {
	var group chunkFlightGroup
	release := make(chan struct{})
	firstStarted := make(chan struct{})
	go group.Do(context.Background(), "a", func() (bool, error) {
		close(firstStarted)
		<-release
		return false, nil
	}, func() error { return nil })
	<-firstStarted

	secondDone := make(chan struct{})
	go func() {
		group.Do(context.Background(), "b", func() (bool, error) { return false, nil }, func() error { return nil })
		close(secondDone)
	}()
	select {
	case <-secondDone:
	case <-time.After(time.Second):
		t.Fatal("different chunk keys blocked one another")
	}
	close(release)
}

func TestChunkFlightSharesLeaderError(t *testing.T) {
	var group chunkFlightGroup
	started := make(chan struct{})
	release := make(chan struct{})
	followerDrained := make(chan struct{})
	want := errors.New("leader failed")
	go group.Do(context.Background(), "same", func() (bool, error) {
		close(started)
		<-release
		return false, want
	}, func() error { return nil })
	<-started

	result := make(chan error, 1)
	go func() {
		_, err := group.Do(context.Background(), "same", func() (bool, error) { return false, nil }, func() error {
			close(followerDrained)
			return nil
		})
		result <- err
	}()
	<-followerDrained
	close(release)
	if err := <-result; !errors.Is(err, want) {
		t.Fatalf("follower got %v, want %v", err, want)
	}
}

func TestChunkFlightFollowerCancellationAndDrainFailure(t *testing.T) {
	for _, tt := range []struct {
		name  string
		drain func() error
	}{
		{"canceled", func() error { return nil }},
		{"drain failure", func() error { return errors.New("broken body") }},
	} {
		t.Run(tt.name, func(t *testing.T) {
			var group chunkFlightGroup
			started := make(chan struct{})
			release := make(chan struct{})
			go group.Do(context.Background(), "same", func() (bool, error) {
				close(started)
				<-release
				return false, nil
			}, func() error { return nil })
			<-started

			ctx, cancel := context.WithCancel(context.Background())
			if tt.name == "canceled" {
				cancel()
			} else {
				defer cancel()
			}
			_, err := group.Do(ctx, "same", func() (bool, error) { return false, nil }, tt.drain)
			if err == nil {
				t.Fatal("expected follower failure")
			}
			close(release)
		})
	}
}

func TestChunkFlightCleansUpAfterCompletionAndPanic(t *testing.T) {
	var group chunkFlightGroup
	if _, err := group.Do(context.Background(), "normal", func() (bool, error) { return false, nil }, func() error { return nil }); err != nil {
		t.Fatal(err)
	}
	group.mu.Lock()
	if len(group.calls) != 0 {
		t.Fatalf("normal completion leaked %d calls", len(group.calls))
	}
	group.mu.Unlock()

	func() {
		defer func() {
			if recover() == nil {
				t.Fatal("leader panic was swallowed")
			}
		}()
		group.Do(context.Background(), "panic", func() (bool, error) { panic("boom") }, func() error { return nil })
	}()
	group.mu.Lock()
	defer group.mu.Unlock()
	if len(group.calls) != 0 {
		t.Fatalf("panic leaked %d calls", len(group.calls))
	}
}

func TestChunkFlightHighConcurrency(t *testing.T) {
	var group chunkFlightGroup
	var leaders atomic.Int64
	const workers = 256
	start := make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			_, err := group.Do(context.Background(), "same", func() (bool, error) {
				leaders.Add(1)
				time.Sleep(20 * time.Millisecond)
				return false, nil
			}, func() error { return nil })
			if err != nil {
				t.Errorf("flight failed: %v", err)
			}
		}()
	}
	close(start)
	wg.Wait()
	if got := leaders.Load(); got != 1 {
		t.Fatalf("ran %d leaders, want 1", got)
	}
}

func TestChunkFlightDoesNotDeadlockHTTP2Bodies(t *testing.T) {
	var group chunkFlightGroup
	leaderEntered := make(chan struct{})

	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		known, err := group.Do(r.Context(), "same", func() (bool, error) {
			close(leaderEntered)
			// Keep the leader streaming while the duplicate arrives. A small
			// delay per read makes overlap deterministic without introducing a
			// dependency from the leader to the follower.
			_, err := io.Copy(slowDiscardWriter{delay: time.Millisecond}, r.Body)
			return false, err
		}, func() error {
			_, err := io.Copy(io.Discard, r.Body)
			return err
		})
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		fmt.Fprintf(w, "%t", known)
	})

	server := httptest.NewUnstartedServer(handler)
	server.EnableHTTP2 = true
	server.StartTLS()
	defer server.Close()
	client := server.Client()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	body := bytes.Repeat([]byte("x"), 3<<20)
	result := make(chan error, 2)
	go func() {
		req, _ := http.NewRequestWithContext(ctx, http.MethodPost, server.URL, bytes.NewReader(body))
		resp, err := client.Do(req)
		if err == nil {
			if resp.ProtoMajor != 2 {
				err = fmt.Errorf("protocol is %s, want HTTP/2", resp.Proto)
			}
			io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
		}
		result <- err
	}()
	select {
	case <-leaderEntered:
	case <-ctx.Done():
		t.Fatal("leader request did not reach handler")
	}
	go func() {
		req, _ := http.NewRequestWithContext(ctx, http.MethodPost, server.URL, bytes.NewReader(body))
		resp, err := client.Do(req)
		if err == nil {
			io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
		}
		result <- err
	}()
	for i := 0; i < 2; i++ {
		select {
		case err := <-result:
			if err != nil {
				t.Fatalf("HTTP/2 request failed: %v", err)
			}
		case <-ctx.Done():
			t.Fatal("concurrent duplicate bodies deadlocked HTTP/2 connection")
		}
	}
}

type slowDiscardWriter struct {
	delay time.Duration
}

func (w slowDiscardWriter) Write(p []byte) (int, error) {
	time.Sleep(w.delay)
	return len(p), nil
}
