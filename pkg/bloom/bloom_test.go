package bloom

import (
	"fmt"
	"sync"
	"testing"
)

func TestBloomFilter_NoFalseNegatives(t *testing.T) {
	n := uint64(5000)
	bf := New(n, 0.01)

	// Add elements
	for i := uint64(0); i < n; i++ {
		key := fmt.Sprintf("existing-key-%d", i)
		bf.Add(key)
	}

	// Verify all elements are found (Bloom filters have 0% false negative rate)
	for i := uint64(0); i < n; i++ {
		key := fmt.Sprintf("existing-key-%d", i)
		if !bf.Contains(key) {
			t.Fatalf("false negative detected for key %s", key)
		}
	}
}

func TestBloomFilter_FalsePositiveRate(t *testing.T) {
	n := uint64(10000)
	targetFPR := 0.01
	bf := New(n, targetFPR)

	for i := uint64(0); i < n; i++ {
		bf.Add(fmt.Sprintf("inserted-%d", i))
	}

	// Test with 10000 disjoint keys
	falsePositives := 0
	testCount := 10000
	for i := 0; i < testCount; i++ {
		key := fmt.Sprintf("disjoint-%d", i)
		if bf.Contains(key) {
			falsePositives++
		}
	}

	actualFPR := float64(falsePositives) / float64(testCount)
	t.Logf("Target FPR: %.4f | Actual FPR: %.4f (%d/%d false positives)", targetFPR, actualFPR, falsePositives, testCount)

	// Allow a reasonable statistical tolerance (e.g., up to 2.5% for 10k items)
	if actualFPR > 0.025 {
		t.Fatalf("actual FPR %.4f exceeds expected tolerance around target %.4f", actualFPR, targetFPR)
	}
}

func TestBloomFilter_Reset(t *testing.T) {
	bf := New(100, 0.01)
	bf.Add("hello")
	if !bf.Contains("hello") {
		t.Fatalf("expected hello to be present")
	}

	bf.Reset()
	if bf.Contains("hello") {
		t.Fatalf("expected hello to be absent after reset")
	}
	if bf.Count() != 0 {
		t.Fatalf("expected count 0 after reset, got %d", bf.Count())
	}
}

func TestBloomFilter_Concurrency(t *testing.T) {
	bf := New(10000, 0.01)
	var wg sync.WaitGroup
	workers := 20
	opsPerWorker := 500

	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for i := 0; i < opsPerWorker; i++ {
				key := fmt.Sprintf("worker-%d-%d", id, i)
				bf.Add(key)
				if !bf.Contains(key) {
					t.Errorf("expected key %s to exist immediately after Add", key)
				}
			}
		}(w)
	}
	wg.Wait()
}
