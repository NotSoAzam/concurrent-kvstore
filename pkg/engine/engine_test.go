package engine

import (
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func createTempEngine(t *testing.T, opts Options) (*Engine, string) {
	tempDir, err := os.MkdirTemp("", "engine_test_*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	opts.WALPath = filepath.Join(tempDir, "storage.wal")

	eng, err := New(opts)
	if err != nil {
		os.RemoveAll(tempDir)
		t.Fatalf("failed to create engine: %v", err)
	}
	return eng, tempDir
}

func TestEngine_BasicCRUDAndTieredLookup(t *testing.T) {
	opts := DefaultOptions("")
	opts.LRUCapacity = 2 // Small LRU cache to easily test evictions and cache hits
	eng, tempDir := createTempEngine(t, opts)
	defer os.RemoveAll(tempDir)
	defer eng.Close()

	// 1. Put
	if err := eng.Put("k1", "v1"); err != nil {
		t.Fatalf("Put failed: %v", err)
	}
	if err := eng.Put("k2", "v2"); err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	// 2. Get (should hit LRU cache)
	val, ok, err := eng.Get("k1")
	if err != nil || !ok || val != "v1" {
		t.Fatalf("Get k1 failed: val=%s, ok=%v, err=%v", val, ok, err)
	}

	stats := eng.Stats()
	if stats.LRUHits != 1 {
		t.Fatalf("expected 1 LRU hit, got %d", stats.LRUHits)
	}

	// 3. Put k3 (LRU capacity is 2, k2 was least recently used, so k2 gets evicted from LRU, but remains in SkipList)
	if err := eng.Put("k3", "v3"); err != nil {
		t.Fatalf("Put k3 failed: %v", err)
	}

	// 4. Get k2 (LRU miss -> Bloom filter hit -> SkipList hit -> re-populates LRU)
	val, ok, err = eng.Get("k2")
	if err != nil || !ok || val != "v2" {
		t.Fatalf("Get k2 failed: val=%s, ok=%v, err=%v", val, ok, err)
	}

	stats = eng.Stats()
	if stats.LRUMisses < 1 || stats.SkipListLookups < 1 {
		t.Fatalf("expected LRU miss and SkipList lookup for k2: %+v", stats)
	}

	// 5. Negative lookup: non-existent key (Bloom filter rejection)
	val, ok, err = eng.Get("definitely_not_existing_key_9999")
	if err != nil || ok || val != "" {
		t.Fatalf("expected negative lookup, got ok=%v, val=%s", ok, val)
	}

	stats = eng.Stats()
	if stats.BloomFilterRejections < 1 {
		t.Fatalf("expected BloomFilterRejections >= 1, got %d", stats.BloomFilterRejections)
	}

	// 6. Delete
	if err := eng.Delete("k1"); err != nil {
		t.Fatalf("Delete failed: %v", err)
	}
	_, ok, _ = eng.Get("k1")
	if ok {
		t.Fatalf("expected k1 to be deleted")
	}
}

func TestEngine_Scan(t *testing.T) {
	opts := DefaultOptions("")
	eng, tempDir := createTempEngine(t, opts)
	defer os.RemoveAll(tempDir)
	defer eng.Close()

	for i := 1; i <= 10; i++ {
		eng.Put(fmt.Sprintf("user:%03d", i), fmt.Sprintf("profile_%d", i))
	}

	pairs, err := eng.Scan("user:003", "user:007")
	if err != nil {
		t.Fatalf("Scan failed: %v", err)
	}

	if len(pairs) != 5 {
		t.Fatalf("expected 5 pairs, got %d", len(pairs))
	}

	expectedFirst := "user:003"
	expectedLast := "user:007"
	if pairs[0].Key != expectedFirst || pairs[4].Key != expectedLast {
		t.Fatalf("unexpected range scan bounds: first=%s, last=%s", pairs[0].Key, pairs[4].Key)
	}
}

func TestEngine_CrashRecovery(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "engine_crash_*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	walPath := filepath.Join(tempDir, "storage.wal")
	opts := DefaultOptions(walPath)
	opts.SyncWAL = true

	// Stage 1: Write initial dataset and abruptly close (simulating crash)
	eng1, err := New(opts)
	if err != nil {
		t.Fatalf("failed creating engine: %v", err)
	}

	for i := 0; i < 100; i++ {
		_ = eng1.Put(fmt.Sprintf("key-%03d", i), fmt.Sprintf("val-%d", i))
	}
	// Delete even keys 0, 2, 4, ... 18
	for i := 0; i < 20; i += 2 {
		_ = eng1.Delete(fmt.Sprintf("key-%03d", i))
	}
	// Update key-001
	_ = eng1.Put("key-001", "val-updated-1")

	// Flush and close abruptly
	_ = eng1.Close()

	// Stage 2: Reopen engine using the same WAL file and verify replay
	eng2, err := New(opts)
	if err != nil {
		t.Fatalf("failed recovering engine from WAL: %v", err)
	}
	defer eng2.Close()

	// Verify updated key
	val, ok, err := eng2.Get("key-001")
	if err != nil || !ok || val != "val-updated-1" {
		t.Fatalf("recovered key-001 incorrect: ok=%v, val=%s", ok, val)
	}

	// Verify deleted keys are absent
	for i := 0; i < 20; i += 2 {
		k := fmt.Sprintf("key-%03d", i)
		if _, ok, _ := eng2.Get(k); ok {
			t.Fatalf("expected key %s to be deleted after recovery", k)
		}
	}

	// Verify intact odd keys
	for i := 1; i < 20; i += 2 {
		k := fmt.Sprintf("key-%03d", i)
		expected := fmt.Sprintf("val-%d", i)
		if i == 1 {
			expected = "val-updated-1"
		}
		if v, ok, _ := eng2.Get(k); !ok || v != expected {
			t.Fatalf("expected key %s to have value %s, got %s", k, expected, v)
		}
	}

	// Verify remaining keys from 20 to 99
	for i := 20; i < 100; i++ {
		k := fmt.Sprintf("key-%03d", i)
		if v, ok, _ := eng2.Get(k); !ok || v != fmt.Sprintf("val-%d", i) {
			t.Fatalf("expected key %s to exist with val-%d, got %s (ok=%v)", k, i, v, ok)
		}
	}
}

func TestEngine_Checkpoint(t *testing.T) {
	opts := DefaultOptions("")
	eng, tempDir := createTempEngine(t, opts)
	defer os.RemoveAll(tempDir)
	defer eng.Close()

	for i := 0; i < 50; i++ {
		eng.Put("reused_key", fmt.Sprintf("val_%d", i))
	}

	statsBefore := eng.Stats()
	if err := eng.Checkpoint(); err != nil {
		t.Fatalf("Checkpoint failed: %v", err)
	}
	statsAfter := eng.Stats()

	if statsAfter.WALSizeBytes >= statsBefore.WALSizeBytes {
		t.Fatalf("expected compacted WAL size < initial WAL size, got %d vs %d", statsAfter.WALSizeBytes, statsBefore.WALSizeBytes)
	}

	val, ok, err := eng.Get("reused_key")
	if err != nil || !ok || val != "val_49" {
		t.Fatalf("value after checkpoint incorrect: ok=%v, val=%s", ok, val)
	}
}

func TestEngine_ConcurrentStress100Goroutines(t *testing.T) {
	opts := DefaultOptions("")
	opts.LRUCapacity = 500
	opts.BloomExpectedKeys = 50000
	eng, tempDir := createTempEngine(t, opts)
	defer os.RemoveAll(tempDir)
	defer eng.Close()

	numGoroutines := 100
	opsPerGoroutine := 100
	var wg sync.WaitGroup

	for g := 0; g < numGoroutines; g++ {
		wg.Add(1)
		go func(gid int) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(time.Now().UnixNano() + int64(gid)))

			for i := 0; i < opsPerGoroutine; i++ {
				op := rng.Intn(4)
				key := fmt.Sprintf("stress:key:%04d", rng.Intn(200))
				val := fmt.Sprintf("payload-%d-%d", gid, i)

				switch op {
				case 0, 1: // 50% Puts
					_ = eng.Put(key, val)
				case 2: // 25% Gets
					_, _, _ = eng.Get(key)
				case 3: // 25% Scans or Deletes
					if i%2 == 0 {
						_ = eng.Delete(key)
					} else {
						_, _ = eng.Scan("stress:key:0050", "stress:key:0100")
					}
				}
			}
		}(g)
	}

	wg.Wait()

	// Engine must remain valid and operational after high-concurrency stress
	testKey := "stress:final:key"
	testVal := "final_value"
	if err := eng.Put(testKey, testVal); err != nil {
		t.Fatalf("Put failed after stress test: %v", err)
	}
	if v, ok, err := eng.Get(testKey); err != nil || !ok || v != testVal {
		t.Fatalf("Get failed after stress test: val=%s, ok=%v, err=%v", v, ok, err)
	}
}

// ----------------- Benchmark Suite -----------------

func BenchmarkPut(b *testing.B) {
	tempDir, err := os.MkdirTemp("", "bench_put_*")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(tempDir)

	opts := DefaultOptions(filepath.Join(tempDir, "bench.wal"))
	opts.SyncWAL = false // standard high throughput async WAL
	eng, err := New(opts)
	if err != nil {
		b.Fatal(err)
	}
	defer eng.Close()

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		k := fmt.Sprintf("bench-key-%08d", i)
		v := "benchmark-payload-value-12345"
		_ = eng.Put(k, v)
	}
}

func BenchmarkGet(b *testing.B) {
	tempDir, err := os.MkdirTemp("", "bench_get_*")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(tempDir)

	opts := DefaultOptions(filepath.Join(tempDir, "bench.wal"))
	opts.LRUCapacity = 50000
	eng, err := New(opts)
	if err != nil {
		b.Fatal(err)
	}
	defer eng.Close()

	// Pre-populate 10,000 keys
	totalKeys := 10000
	for i := 0; i < totalKeys; i++ {
		_ = eng.Put(fmt.Sprintf("bench-key-%08d", i), "benchmark-payload-value")
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		k := fmt.Sprintf("bench-key-%08d", i%totalKeys)
		_, _, _ = eng.Get(k)
	}
}

func BenchmarkScan(b *testing.B) {
	tempDir, err := os.MkdirTemp("", "bench_scan_*")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(tempDir)

	opts := DefaultOptions(filepath.Join(tempDir, "bench.wal"))
	eng, err := New(opts)
	if err != nil {
		b.Fatal(err)
	}
	defer eng.Close()

	for i := 0; i < 5000; i++ {
		_ = eng.Put(fmt.Sprintf("scan-key-%06d", i), "scan-val")
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, _ = eng.Scan("scan-key-001000", "scan-key-001100")
	}
}
