package skiplist

import (
	"fmt"
	"sync"
	"testing"
)

func TestSkipList_BasicCRUD(t *testing.T) {
	sl := New(DefaultMaxLevel, DefaultProbability)

	// Test Empty
	if sl.Len() != 0 {
		t.Fatalf("expected empty skiplist, got len %d", sl.Len())
	}
	if _, ok := sl.Get("key1"); ok {
		t.Fatalf("expected key1 to not be found")
	}

	// Test Put
	replaced := sl.Put("beta", "val_beta")
	if replaced {
		t.Fatalf("expected replaced=false for new key")
	}
	if sl.Len() != 1 {
		t.Fatalf("expected len 1, got %d", sl.Len())
	}

	sl.Put("alpha", "val_alpha")
	sl.Put("gamma", "val_gamma")

	if sl.Len() != 3 {
		t.Fatalf("expected len 3, got %d", sl.Len())
	}

	// Test Get
	if val, ok := sl.Get("alpha"); !ok || val != "val_alpha" {
		t.Fatalf("expected val_alpha, got %s (ok=%v)", val, ok)
	}
	if val, ok := sl.Get("beta"); !ok || val != "val_beta" {
		t.Fatalf("expected val_beta, got %s (ok=%v)", val, ok)
	}
	if val, ok := sl.Get("gamma"); !ok || val != "val_gamma" {
		t.Fatalf("expected val_gamma, got %s (ok=%v)", val, ok)
	}

	// Test Update
	replaced = sl.Put("beta", "val_beta_updated")
	if !replaced {
		t.Fatalf("expected replaced=true on existing key update")
	}
	if val, ok := sl.Get("beta"); !ok || val != "val_beta_updated" {
		t.Fatalf("expected val_beta_updated, got %s", val)
	}
	if sl.Len() != 3 {
		t.Fatalf("len should remain 3 after update, got %d", sl.Len())
	}

	// Test Delete
	if deleted := sl.Delete("beta"); !deleted {
		t.Fatalf("expected beta to be deleted")
	}
	if _, ok := sl.Get("beta"); ok {
		t.Fatalf("expected beta to not exist after deletion")
	}
	if sl.Len() != 2 {
		t.Fatalf("expected len 2, got %d", sl.Len())
	}

	// Deleting non-existent key
	if deleted := sl.Delete("non_existent"); deleted {
		t.Fatalf("expected false deleting non-existent key")
	}
}

func TestSkipList_Scan(t *testing.T) {
	sl := New(16, 0.5)

	keys := []string{"k10", "k20", "k30", "k40", "k50", "k60"}
	for _, k := range keys {
		sl.Put(k, "v_"+k)
	}

	// Scan exact range
	res := sl.Scan("k20", "k40")
	if len(res) != 3 {
		t.Fatalf("expected 3 results for [k20, k40], got %d", len(res))
	}
	if res[0].Key != "k20" || res[1].Key != "k30" || res[2].Key != "k40" {
		t.Fatalf("unexpected scan results: %+v", res)
	}

	// Scan with startKey lower than minimum
	res = sl.Scan("k00", "k25")
	if len(res) != 2 || res[0].Key != "k10" || res[1].Key != "k20" {
		t.Fatalf("unexpected scan results for [k00, k25]: %+v", res)
	}

	// Scan without endKey (unbounded right)
	res = sl.Scan("k40", "")
	if len(res) != 3 || res[0].Key != "k40" || res[1].Key != "k50" || res[2].Key != "k60" {
		t.Fatalf("unexpected scan results for [k40, ]: %+v", res)
	}

	// Scan out of bounds
	res = sl.Scan("k70", "k90")
	if len(res) != 0 {
		t.Fatalf("expected 0 results, got %d", len(res))
	}
}

func TestSkipList_Concurrency(t *testing.T) {
	sl := New(DefaultMaxLevel, DefaultProbability)
	var wg sync.WaitGroup
	numWorkers := 50
	itemsPerWorker := 200

	// Concurrent writes
	for w := 0; w < numWorkers; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for i := 0; i < itemsPerWorker; i++ {
				k := fmt.Sprintf("worker-%02d-key-%04d", workerID, i)
				v := fmt.Sprintf("val-%d", i)
				sl.Put(k, v)
			}
		}(w)
	}
	wg.Wait()

	expectedTotal := numWorkers * itemsPerWorker
	if sl.Len() != expectedTotal {
		t.Fatalf("expected %d elements, got %d", expectedTotal, sl.Len())
	}

	// Concurrent reads, scans, and deletes
	for w := 0; w < numWorkers; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for i := 0; i < itemsPerWorker; i++ {
				k := fmt.Sprintf("worker-%02d-key-%04d", workerID, i)
				val, ok := sl.Get(k)
				if !ok || val != fmt.Sprintf("val-%d", i) {
					t.Errorf("read failed for %s", k)
				}
				if i%10 == 0 {
					sl.Scan(k, fmt.Sprintf("worker-%02d-key-%04d", workerID, i+5))
				}
			}
		}(w)
	}
	wg.Wait()
}
