package main

import (
	"sync"
	"testing"
)

// Le meme digest ne doit JAMAIS etre en section critique deux fois a la fois :
// c'est exactement la course qui produisait deux PUT concurrents sur une cle.
func TestKeyedMutexExcludesSameKey(t *testing.T) {
	var k keyedMutex
	var mu sync.Mutex
	inFlight := make(map[string]int)
	maxSeen := 0

	var wg sync.WaitGroup
	for i := 0; i < 200; i++ {
		for _, key := range []string{"chunk-a", "chunk-b"} {
			wg.Add(1)
			go func(key string) {
				defer wg.Done()
				defer k.Lock(key)()

				mu.Lock()
				inFlight[key]++
				if inFlight[key] > maxSeen {
					maxSeen = inFlight[key]
				}
				n := inFlight[key]
				mu.Unlock()

				if n > 1 {
					t.Errorf("%s: %d entrees simultanees en section critique", key, n)
				}

				mu.Lock()
				inFlight[key]--
				mu.Unlock()
			}(key)
		}
	}
	wg.Wait()

	if maxSeen != 1 {
		t.Fatalf("exclusion non respectee: max %d", maxSeen)
	}
}

// Des cles differentes ne doivent pas se bloquer entre elles.
func TestKeyedMutexAllowsDifferentKeysConcurrently(t *testing.T) {
	var k keyedMutex
	release1 := k.Lock("cle-1")
	done := make(chan struct{})
	go func() {
		k.Lock("cle-2")()
		close(done)
	}()
	<-done // bloquerait si les cles se verrouillaient mutuellement
	release1()
}

// La map ne doit pas grossir indefiniment : chaque cle liberee disparait.
func TestKeyedMutexReleasesEntries(t *testing.T) {
	var k keyedMutex
	for i := 0; i < 1000; i++ {
		k.Lock("ephemere")()
	}
	k.mu.Lock()
	n := len(k.entries)
	k.mu.Unlock()
	if n != 0 {
		t.Fatalf("fuite memoire: %d entrees restantes", n)
	}
}

// Le double appel de la fonction de liberation ne doit pas paniquer.
func TestKeyedMutexUnlockIsIdempotent(t *testing.T) {
	var k keyedMutex
	release := k.Lock("x")
	release()
	release()
}
