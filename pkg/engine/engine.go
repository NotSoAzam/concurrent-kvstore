package engine

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/systems-architect/kvstore/pkg/bloom"
	"github.com/systems-architect/kvstore/pkg/lru"
	"github.com/systems-architect/kvstore/pkg/skiplist"
	"github.com/systems-architect/kvstore/pkg/wal"
)

var (
	// ErrEngineClosed is returned when an operation is attempted on a closed engine.
	ErrEngineClosed = errors.New("engine: storage engine is closed")
	// ErrEmptyKey is returned when an empty key is supplied.
	ErrEmptyKey = errors.New("engine: key cannot be empty")
)

// Pair represents a key-value record returned by scans.
type Pair struct {
	Key   string
	Value string
}

// Options configures the storage engine subsystems.
type Options struct {
	// WALPath is the filesystem path for the Write-Ahead Log.
	WALPath string
	// SyncWAL determines whether every write is fsync'd immediately.
	SyncWAL bool
	// LRUCapacity is the maximum number of hot entries retained in cache.
	LRUCapacity int
	// BloomExpectedKeys is the anticipated capacity for the bloom filter.
	BloomExpectedKeys uint64
	// BloomFPR is the target false positive probability for the bloom filter (e.g. 0.01 = 1%).
	BloomFPR float64
	// SkipListMaxLevel configures skip list height (default: 32).
	SkipListMaxLevel int
	// SkipListP configures skip list branching probability (default: 0.5).
	SkipListP float64
}

// DefaultOptions provides sensible production defaults for the engine.
func DefaultOptions(walPath string) Options {
	return Options{
		WALPath:           walPath,
		SyncWAL:           false, // true for strict durability, false for high throughput
		LRUCapacity:       10000,
		BloomExpectedKeys: 100000,
		BloomFPR:          0.01,
		SkipListMaxLevel:  skiplist.DefaultMaxLevel,
		SkipListP:         skiplist.DefaultProbability,
	}
}

// Stats captures live runtime operational statistics.
type Stats struct {
	TotalKeys             int64  `json:"total_keys"`
	LRUSize               int    `json:"lru_size"`
	LRUCapacity           int    `json:"lru_capacity"`
	LRUHits               uint64 `json:"lru_hits"`
	LRUMisses             uint64 `json:"lru_misses"`
	BloomFilterRejections uint64 `json:"bloom_filter_rejections"`
	SkipListLookups       uint64 `json:"skiplist_lookups"`
	WALSizeBytes          int64  `json:"wal_size_bytes"`
}

// Engine coordinates the concurrent SkipList, LRU cache, Bloom Filter, and WAL.
type Engine struct {
	mu     sync.RWMutex
	closed bool
	opts   Options

	skipList *skiplist.SkipList
	lruCache *lru.Cache
	bloom    *bloom.Filter
	walLog   *wal.WAL

	// Metrics
	lruHits        uint64
	lruMisses      uint64
	bloomRejects   uint64
	skiplistReads  uint64
}

// New initializes the engine, opens the WAL, and replays historical state for crash recovery.
func New(opts Options) (*Engine, error) {
	if opts.WALPath == "" {
		opts.WALPath = "kvstore.wal"
	}
	if opts.LRUCapacity <= 0 {
		opts.LRUCapacity = 10000
	}
	if opts.BloomExpectedKeys == 0 {
		opts.BloomExpectedKeys = 100000
	}
	if opts.BloomFPR <= 0 || opts.BloomFPR >= 1 {
		opts.BloomFPR = 0.01
	}

	sl := skiplist.New(opts.SkipListMaxLevel, opts.SkipListP)
	cache := lru.New(opts.LRUCapacity)
	bf := bloom.New(opts.BloomExpectedKeys, opts.BloomFPR)

	w, err := wal.Open(opts.WALPath, opts.SyncWAL)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize wal: %w", err)
	}

	// Replay WAL to reconstruct in-memory state
	entries, err := w.Replay()
	if err != nil {
		_ = w.Close()
		return nil, fmt.Errorf("failed to replay wal during recovery: %w", err)
	}

	for _, entry := range entries {
		switch entry.Type {
		case wal.OpPut:
			sl.Put(entry.Key, entry.Value)
			cache.Put(entry.Key, entry.Value)
			bf.Add(entry.Key)
		case wal.OpDelete:
			sl.Delete(entry.Key)
			cache.Delete(entry.Key)
		}
	}

	return &Engine{
		opts:     opts,
		skipList: sl,
		lruCache: cache,
		bloom:    bf,
		walLog:   w,
	}, nil
}

// Put writes an entry durably to the WAL first, then updates SkipList, LRU cache, and Bloom filter.
func (e *Engine) Put(key, value string) error {
	if key == "" {
		return ErrEmptyKey
	}

	e.mu.RLock()
	if e.closed {
		e.mu.RUnlock()
		return ErrEngineClosed
	}
	defer e.mu.RUnlock()

	// 1. Write Ahead Log append
	if err := e.walLog.WritePut(key, value); err != nil {
		return fmt.Errorf("wal write failed: %w", err)
	}

	// 2. Primary ordered store
	e.skipList.Put(key, value)

	// 3. Fast cache
	e.lruCache.Put(key, value)

	// 4. Negative lookup filter
	e.bloom.Add(key)

	return nil
}

// Get retrieves a key using the multi-tier query path:
// Tier 1: O(1) LRU Cache Hit
// Tier 2: O(k) Bloom Filter Rejection (skips SkipList traversal on negative lookups)
// Tier 3: O(log n) SkipList Lookup with LRU cache warm-up
func (e *Engine) Get(key string) (string, bool, error) {
	if key == "" {
		return "", false, ErrEmptyKey
	}

	e.mu.RLock()
	if e.closed {
		e.mu.RUnlock()
		return "", false, ErrEngineClosed
	}
	defer e.mu.RUnlock()

	// Tier 1: Check LRU Cache
	if val, found := e.lruCache.Get(key); found {
		atomic.AddUint64(&e.lruHits, 1)
		return val, true, nil
	}
	atomic.AddUint64(&e.lruMisses, 1)

	// Tier 2: Check Bloom Filter (Negative Lookup optimization)
	if !e.bloom.Contains(key) {
		atomic.AddUint64(&e.bloomRejects, 1)
		return "", false, nil
	}

	// Tier 3: Query SkipList
	atomic.AddUint64(&e.skiplistReads, 1)
	val, found := e.skipList.Get(key)
	if !found {
		return "", false, nil
	}

	// Warm up LRU Cache for subsequent reads
	e.lruCache.Put(key, val)
	return val, true, nil
}

// Delete removes a key by appending a tombstone to the WAL, deleting from SkipList and LRU cache.
func (e *Engine) Delete(key string) error {
	if key == "" {
		return ErrEmptyKey
	}

	e.mu.RLock()
	if e.closed {
		e.mu.RUnlock()
		return ErrEngineClosed
	}
	defer e.mu.RUnlock()

	// 1. Log tombstone to WAL
	if err := e.walLog.WriteDelete(key); err != nil {
		return fmt.Errorf("wal delete failed: %w", err)
	}

	// 2. Remove from SkipList
	e.skipList.Delete(key)

	// 3. Invalidate LRU Cache
	e.lruCache.Delete(key)

	return nil
}

// Scan returns an ordered slice of key-value pairs where startKey <= Key <= endKey.
// If endKey is empty, it scans from startKey to the end of the sorted index.
func (e *Engine) Scan(startKey, endKey string) ([]Pair, error) {
	e.mu.RLock()
	if e.closed {
		e.mu.RUnlock()
		return nil, ErrEngineClosed
	}
	defer e.mu.RUnlock()

	pairs := e.skipList.Scan(startKey, endKey)
	res := make([]Pair, len(pairs))
	for i, p := range pairs {
		res[i] = Pair{Key: p.Key, Value: p.Value}
	}
	return res, nil
}

// Checkpoint takes a full snapshot of live keys from SkipList and truncates the WAL,
// rebuilding the Bloom filter to eliminate false positive artifacts from deleted keys.
func (e *Engine) Checkpoint() error {
	e.mu.Lock()
	defer e.mu.Unlock()

	if e.closed {
		return ErrEngineClosed
	}

	allPairs := e.skipList.All()
	walPairs := make([]wal.Pair, len(allPairs))
	for i, p := range allPairs {
		walPairs[i] = wal.Pair{Key: p.Key, Value: p.Value}
	}

	// Compact WAL
	if err := e.walLog.Checkpoint(walPairs); err != nil {
		return fmt.Errorf("wal checkpoint failed: %w", err)
	}

	// Rebuild clean Bloom filter from live data
	newBf := bloom.New(e.opts.BloomExpectedKeys, e.opts.BloomFPR)
	for _, p := range allPairs {
		newBf.Add(p.Key)
	}
	e.bloom = newBf

	return nil
}

// Stats returns a snapshot of runtime metrics.
func (e *Engine) Stats() Stats {
	e.mu.RLock()
	defer e.mu.RUnlock()

	walSize := int64(0)
	if e.walLog != nil {
		if sz, err := e.walLog.Size(); err == nil {
			walSize = sz
		}
	}

	return Stats{
		TotalKeys:             int64(e.skipList.Len()),
		LRUSize:               e.lruCache.Len(),
		LRUCapacity:           e.lruCache.Capacity(),
		LRUHits:               atomic.LoadUint64(&e.lruHits),
		LRUMisses:             atomic.LoadUint64(&e.lruMisses),
		BloomFilterRejections: atomic.LoadUint64(&e.bloomRejects),
		SkipListLookups:       atomic.LoadUint64(&e.skiplistReads),
		WALSizeBytes:          walSize,
	}
}

// Sync commits any buffered WAL writes to stable disk storage.
func (e *Engine) Sync() error {
	e.mu.RLock()
	defer e.mu.RUnlock()

	if e.closed {
		return ErrEngineClosed
	}
	return e.walLog.Sync()
}

// Close gracefully flushes data and closes the WAL.
func (e *Engine) Close() error {
	e.mu.Lock()
	defer e.mu.Unlock()

	if e.closed {
		return nil
	}

	e.closed = true
	return e.walLog.Close()
}
