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
	"sync"
	"time"
	"tizbac/pmoxs3backuproxy/internal/s3backuplog"
)

// ttlCache memoises the result of an expensive upstream call, keyed by an
// arbitrary string.
//
// Why this exists: Proxmox VE polls the proxy constantly. pvestatd refreshes
// the status of every storage every 10 seconds, and the web UI adds its own
// requests on top. Answering each of those with a round trip to the S3
// endpoint makes the response time of the proxy depend on the round trip time
// of a remote object store, which is exactly what a hypervisor cannot afford:
// PVE hardcodes a 7 second timeout on the PBS API client
// (PVE::Storage::PBSPlugin, pbs_api_connect), and there is no setting to
// raise it. A transient slowdown of the object store therefore turns into
// "could not activate storage - 500 read timeout", and when it lands on the
// pre-flight check of a backup job, the whole job aborts without saving a
// single guest.
//
// Two properties matter more than the hit ratio:
//
//   - single flight: concurrent misses collapse into one upstream call, so a
//     burst of polls cannot multiply the load on the object store;
//   - serve stale on error: once a value has been observed, a later upstream
//     failure returns the last known value instead of an error. The bucket
//     list of an S3 account is stable over minutes; answering from a slightly
//     old copy is strictly better than failing a backup job.
type ttlCache[T any] struct {
	mu      sync.Mutex
	ttl     time.Duration
	entries map[string]*ttlCacheEntry[T]
}

type ttlCacheEntry[T any] struct {
	mu         sync.Mutex // held while refreshing, gives the single flight
	value      T
	fetched    time.Time
	valid      bool
	refreshing bool // a background refresh is in flight, see GetAsync
}

func newTTLCache[T any](ttl time.Duration) *ttlCache[T] {
	return &ttlCache[T]{ttl: ttl, entries: make(map[string]*ttlCacheEntry[T])}
}

func (c *ttlCache[T]) entry(key string) *ttlCacheEntry[T] {
	c.mu.Lock()
	defer c.mu.Unlock()
	e, ok := c.entries[key]
	if !ok {
		e = &ttlCacheEntry[T]{}
		c.entries[key] = e
	}
	return e
}

// Get returns the cached value for key, calling fetch only when the entry is
// missing or older than the TTL. A fetch error is returned to the caller only
// when nothing was ever cached for that key; otherwise the stale value is
// returned and the error is logged.
//
// The boolean reports whether the returned value comes from a fresh upstream
// call, which callers may use for logging.
func (c *ttlCache[T]) Get(key string, fetch func() (T, error)) (T, bool, error) {
	e := c.entry(key)

	e.mu.Lock()
	defer e.mu.Unlock()

	if e.valid && time.Since(e.fetched) < c.ttl {
		return e.value, false, nil
	}

	value, err := fetch()
	if err != nil {
		if e.valid {
			s3backuplog.WarnPrint(
				"Refresh of cached entry [%s] failed, serving value cached %s ago: %s",
				key, time.Since(e.fetched).Truncate(time.Second), err.Error(),
			)
			return e.value, false, nil
		}
		var zero T
		return zero, false, err
	}

	e.value = value
	e.fetched = time.Now()
	e.valid = true
	return value, true, nil
}

// GetAsync never blocks on the upstream call: it returns whatever is cached,
// fresh or stale, and refreshes in the background when the entry is missing or
// expired. The boolean reports whether a value was available at all.
//
// This is what endpoints on the critical path of PVE must use. Computing the
// usage of a datastore means listing every object of the bucket, which takes
// seconds on a large bucket, and pvestatd will not wait: it gives the PBS API
// 7 seconds for the whole request. Answering with the previous value and
// refreshing behind is the only way to expose an expensive figure without ever
// risking that timeout.
func (c *ttlCache[T]) GetAsync(key string, fetch func() (T, error)) (T, bool) {
	e := c.entry(key)

	e.mu.Lock()
	if e.valid && time.Since(e.fetched) < c.ttl {
		defer e.mu.Unlock()
		return e.value, true
	}
	value, valid := e.value, e.valid
	refreshing := e.refreshing
	if !refreshing {
		e.refreshing = true
	}
	e.mu.Unlock()

	if !refreshing {
		go func() {
			v, err := fetch()

			e.mu.Lock()
			defer e.mu.Unlock()
			e.refreshing = false
			if err != nil {
				s3backuplog.WarnPrint("Background refresh of cached entry [%s] failed: %s", key, err.Error())
				return
			}
			e.value = v
			e.fetched = time.Now()
			e.valid = true
		}()
	}

	return value, valid
}

// Invalidate drops the entry for key, so the next Get calls upstream again.
// Used when the proxy itself mutated the underlying state and does not want to
// wait out the TTL.
func (c *ttlCache[T]) Invalidate(key string) {
	e := c.entry(key)
	e.mu.Lock()
	defer e.mu.Unlock()
	e.valid = false
}
