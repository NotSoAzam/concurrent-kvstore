package bloom

import (
	"hash/fnv"
	"math"
	"sync"
)

// Filter represents a thread-safe, space-efficient Bloom Filter.
// It uses a uint64 bitset and Kirsch-Mitzenmacher dual hashing (combining FNV-1a and Murmur-style mixing)
// to achieve the target false-positive rate (default 1%).
type Filter struct {
	mu     sync.RWMutex
	bits   []uint64
	m      uint64  // total number of bits
	k      uint32  // number of hash functions
	n      uint64  // expected number of elements
	count  uint64  // number of elements inserted
	target float64 // target false positive probability
}

// New creates an optimal Bloom Filter for expectedElements and targetFalsePositiveRate.
// If targetFalsePositiveRate <= 0 or >= 1, it defaults to 0.01 (1%).
func New(expectedElements uint64, targetFalsePositiveRate float64) *Filter {
	if expectedElements == 0 {
		expectedElements = 10000
	}
	if targetFalsePositiveRate <= 0 || targetFalsePositiveRate >= 1 {
		targetFalsePositiveRate = 0.01
	}

	// Optimal m = - (n * ln(p)) / (ln(2)^2)
	ln2 := math.Ln2
	mFloat := -float64(expectedElements) * math.Log(targetFalsePositiveRate) / (ln2 * ln2)
	m := uint64(math.Ceil(mFloat))

	// Ensure m is at least 64 and aligned to 64-bit boundaries
	if m < 64 {
		m = 64
	}
	words := (m + 63) / 64
	m = words * 64

	// Optimal k = (m / n) * ln(2)
	kFloat := (float64(m) / float64(expectedElements)) * ln2
	k := uint32(math.Round(kFloat))
	if k < 1 {
		k = 1
	}

	return &Filter{
		bits:   make([]uint64, words),
		m:      m,
		k:      k,
		n:      expectedElements,
		target: targetFalsePositiveRate,
	}
}

// hash1 computes a standard 64-bit FNV-1a hash of the data.
func hash1(data []byte) uint64 {
	h := fnv.New64a()
	_, _ = h.Write(data)
	return h.Sum64()
}

// hash2 computes a secondary independent 64-bit hash using a Murmur3/SplitMix64-style avalanche mixer.
func hash2(data []byte) uint64 {
	// Seed with length and alternating prime
	var h uint64 = 0x9e3779b97f4a7c15 ^ uint64(len(data))
	for _, b := range data {
		h ^= uint64(b)
		h = (h ^ (h >> 30)) * 0xbf58476d1ce4e5b9
		h = (h ^ (h >> 27)) * 0x94d049bb133111eb
	}
	h ^= (h >> 31)
	if h == 0 {
		h = 1 // Prevent 0 step size in dual hashing
	}
	return h
}

// Add inserts a key into the Bloom Filter.
func (f *Filter) Add(key string) {
	data := []byte(key)
	h1 := hash1(data)
	h2 := hash2(data)

	f.mu.Lock()
	defer f.mu.Unlock()

	for i := uint32(0); i < f.k; i++ {
		// Kirsch-Mitzenmacher optimization: g_i(x) = h1(x) + i * h2(x)
		idx := (h1 + uint64(i)*h2) % f.m
		f.bits[idx/64] |= 1 << (idx % 64)
	}
	f.count++
}

// Contains tests if the key is potentially in the set.
// If it returns false, the key is GUARANTEED not in the set.
// If it returns true, the key may be present with probability <= target false positive rate.
func (f *Filter) Contains(key string) bool {
	data := []byte(key)
	h1 := hash1(data)
	h2 := hash2(data)

	f.mu.RLock()
	defer f.mu.RUnlock()

	for i := uint32(0); i < f.k; i++ {
		idx := (h1 + uint64(i)*h2) % f.m
		if (f.bits[idx/64] & (1 << (idx % 64))) == 0 {
			return false
		}
	}
	return true
}

// Reset clears all bits in the filter.
func (f *Filter) Reset() {
	f.mu.Lock()
	defer f.mu.Unlock()

	for i := range f.bits {
		f.bits[i] = 0
	}
	f.count = 0
}

// Capacity returns the expected capacity (n) the filter was initialized with.
func (f *Filter) Capacity() uint64 {
	return f.n
}

// NumHashes returns the number of hash functions k used.
func (f *Filter) NumHashes() uint32 {
	return f.k
}

// BitSize returns the total number of bits m allocated in the bitset.
func (f *Filter) BitSize() uint64 {
	return f.m
}

// Count returns the number of elements added so far.
func (f *Filter) Count() uint64 {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.count
}
