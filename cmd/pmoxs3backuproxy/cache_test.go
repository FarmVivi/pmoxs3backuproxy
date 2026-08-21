package main

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// A value cached less than the TTL ago must not trigger an upstream call.
func TestTTLCacheServesWithinTTL(t *testing.T) {
	var calls int32
	c := newTTLCache[int](time.Minute)

	fetch := func() (int, error) {
		atomic.AddInt32(&calls, 1)
		return 42, nil
	}

	for i := 0; i < 10; i++ {
		v, _, err := c.Get("k", fetch)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if v != 42 {
			t.Fatalf("got %d, want 42", v)
		}
	}

	if got := atomic.LoadInt32(&calls); got != 1 {
		t.Fatalf("upstream called %d times, want 1", got)
	}
}

// Once the TTL has elapsed the next call must refresh, and the "fresh" flag
// must tell the two cases apart.
func TestTTLCacheRefreshesAfterTTL(t *testing.T) {
	var calls int32
	c := newTTLCache[int](10 * time.Millisecond)

	fetch := func() (int, error) {
		return int(atomic.AddInt32(&calls, 1)), nil
	}

	if v, fresh, _ := c.Get("k", fetch); v != 1 || !fresh {
		t.Fatalf("first call: got (%d, %v), want (1, true)", v, fresh)
	}
	if v, fresh, _ := c.Get("k", fetch); v != 1 || fresh {
		t.Fatalf("cached call: got (%d, %v), want (1, false)", v, fresh)
	}

	time.Sleep(20 * time.Millisecond)

	if v, fresh, _ := c.Get("k", fetch); v != 2 || !fresh {
		t.Fatalf("after TTL: got (%d, %v), want (2, true)", v, fresh)
	}
}

// The whole point of the cache: when the object store misbehaves, a value that
// was observed once keeps being served instead of failing the caller. This is
// what stops a transient S3 slowdown from aborting a backup job.
func TestTTLCacheServesStaleOnError(t *testing.T) {
	c := newTTLCache[string](time.Nanosecond)

	if v, _, err := c.Get("k", func() (string, error) { return "good", nil }); err != nil || v != "good" {
		t.Fatalf("warmup: got (%q, %v), want (\"good\", nil)", v, err)
	}

	v, fresh, err := c.Get("k", func() (string, error) { return "", errors.New("read timeout") })
	if err != nil {
		t.Fatalf("stale read returned an error: %v", err)
	}
	if v != "good" {
		t.Fatalf("got %q, want the stale value %q", v, "good")
	}
	if fresh {
		t.Fatal("stale value reported as fresh")
	}
}

// With nothing ever cached there is nothing to serve, so the error must reach
// the caller rather than be masked by a zero value.
func TestTTLCacheReturnsErrorWhenNeverPopulated(t *testing.T) {
	c := newTTLCache[string](time.Minute)

	if _, _, err := c.Get("k", func() (string, error) { return "", errors.New("boom") }); err == nil {
		t.Fatal("expected an error on a cold cache, got nil")
	}
}

// A burst of concurrent misses must collapse into a single upstream call,
// otherwise a slow endpoint would be hit by every pvestatd poll at once.
func TestTTLCacheSingleFlight(t *testing.T) {
	var calls int32
	c := newTTLCache[int](time.Minute)

	fetch := func() (int, error) {
		atomic.AddInt32(&calls, 1)
		time.Sleep(20 * time.Millisecond)
		return 7, nil
	}

	var wg sync.WaitGroup
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if v, _, err := c.Get("k", fetch); err != nil || v != 7 {
				t.Errorf("got (%d, %v), want (7, nil)", v, err)
			}
		}()
	}
	wg.Wait()

	if got := atomic.LoadInt32(&calls); got != 1 {
		t.Fatalf("upstream called %d times, want 1", got)
	}
}

// Distinct keys must not share an entry: two credentials see two bucket lists.
func TestTTLCacheKeysAreIndependent(t *testing.T) {
	c := newTTLCache[string](time.Minute)

	a, _, _ := c.Get("a", func() (string, error) { return "A", nil })
	b, _, _ := c.Get("b", func() (string, error) { return "B", nil })

	if a != "A" || b != "B" {
		t.Fatalf("got (%q, %q), want (\"A\", \"B\")", a, b)
	}
}

// Invalidate must force the next call to go upstream even inside the TTL.
func TestTTLCacheInvalidate(t *testing.T) {
	var calls int32
	c := newTTLCache[int](time.Minute)

	fetch := func() (int, error) { return int(atomic.AddInt32(&calls, 1)), nil }

	c.Get("k", fetch)
	c.Invalidate("k")

	if v, fresh, _ := c.Get("k", fetch); v != 2 || !fresh {
		t.Fatalf("after Invalidate: got (%d, %v), want (2, true)", v, fresh)
	}
}

func TestTTLCacheExpirePreservesStaleValueDuringAsyncRefresh(t *testing.T) {
	c := newTTLCache[int](time.Minute)
	if _, _, err := c.Get("k", func() (int, error) { return 7, nil }); err != nil {
		t.Fatal(err)
	}
	c.Expire("k")

	started := make(chan struct{})
	release := make(chan struct{})
	v, known := c.GetAsync("k", func() (int, error) {
		close(started)
		<-release
		return 8, nil
	})
	if v != 7 || !known {
		t.Fatalf("during refresh: got (%d, %v), want stale (7, true)", v, known)
	}
	<-started
	close(release)

	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if v, ok := c.Peek("k"); ok && v == 8 {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("background refresh did not replace the stale value")
}

func TestWaitForDataStoreUsage(t *testing.T) {
	datastoreUsageCache = newTTLCache[DataStoreUsage](time.Minute)
	want := DataStoreUsage{Bytes: 42, Objects: 3}
	go func() {
		time.Sleep(20 * time.Millisecond)
		datastoreUsageCache.Get("bucket", func() (DataStoreUsage, error) { return want, nil })
	}()
	got, ok := waitForDataStoreUsage("bucket", time.Second)
	if !ok || got != want {
		t.Fatalf("got (%+v, %v), want (%+v, true)", got, ok, want)
	}
}

func TestWaitForDataStoreUsageTimesOut(t *testing.T) {
	datastoreUsageCache = newTTLCache[DataStoreUsage](time.Minute)
	if _, ok := waitForDataStoreUsage("missing", 20*time.Millisecond); ok {
		t.Fatal("missing usage unexpectedly became available")
	}
}
