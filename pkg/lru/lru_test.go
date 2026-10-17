package lru

import (
	"fmt"
	"sync"
	"testing"
)

func TestLRU_Eviction(t *testing.T) {
	cache := New(3)

	cache.Put("a", "1")
	cache.Put("b", "2")
	cache.Put("c", "3")

	if cache.Len() != 3 {
		t.Fatalf("expected len 3, got %d", cache.Len())
	}

	// Access "a" so "b" becomes the LRU item
	if val, ok := cache.Get("a"); !ok || val != "1" {
		t.Fatalf("expected to get 1 for a")
	}

	// Insert "d" -> "b" should be evicted
	evKey, evVal, evicted := cache.Put("d", "4")
	if !evicted || evKey != "b" || evVal != "2" {
		t.Fatalf("expected b=2 to be evicted, got evicted=%v, key=%s, val=%s", evicted, evKey, evVal)
	}

	if _, ok := cache.Get("b"); ok {
		t.Fatalf("expected b to be evicted")
	}

	// Ensure a, c, d are present
	for _, k := range []string{"a", "c", "d"} {
		if _, ok := cache.Get(k); !ok {
			t.Fatalf("expected key %s to exist in cache", k)
		}
	}
}

func TestLRU_UpdateAndEviction(t *testing.T) {
	cache := New(2)

	cache.Put("x", "10")
	cache.Put("y", "20")

	// Update x
	_, _, evicted := cache.Put("x", "100")
	if evicted {
		t.Fatalf("update should not trigger eviction")
	}

	// Insert z -> y should be evicted because x was updated (MRU)
	evKey, _, evicted := cache.Put("z", "30")
	if !evicted || evKey != "y" {
		t.Fatalf("expected y to be evicted, got %s", evKey)
	}

	if val, ok := cache.Get("x"); !ok || val != "100" {
		t.Fatalf("expected x=100, got %s (ok=%v)", val, ok)
	}
}

func TestLRU_Delete(t *testing.T) {
	cache := New(2)
	cache.Put("a", "1")
	cache.Put("b", "2")

	if !cache.Delete("a") {
		t.Fatalf("expected delete to return true")
	}
	if cache.Delete("a") {
		t.Fatalf("expected delete of missing key to return false")
	}
	if cache.Len() != 1 {
		t.Fatalf("expected len 1, got %d", cache.Len())
	}
}

func TestLRU_Concurrency(t *testing.T) {
	cache := New(50)
	var wg sync.WaitGroup
	workers := 20
	opsPerWorker := 500

	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for i := 0; i < opsPerWorker; i++ {
				k := fmt.Sprintf("key-%d", (workerID*10+i)%80)
				cache.Put(k, fmt.Sprintf("val-%d", i))
				cache.Get(k)
				if i%5 == 0 {
					cache.Delete(k)
				}
			}
		}(w)
	}
	wg.Wait()

	if cache.Len() > 50 {
		t.Fatalf("cache len %d exceeded capacity 50", cache.Len())
	}
}
