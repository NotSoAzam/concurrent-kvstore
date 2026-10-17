package skiplist

import (
	"math/rand"
	"sync"
	"time"
)

const (
	// DefaultMaxLevel is the maximum height of the skip list.
	// 32 levels can easily accommodate 2^32 elements with O(log n) efficiency.
	DefaultMaxLevel = 32
	// DefaultProbability is the branching factor probability p = 0.5.
	DefaultProbability = 0.5
)

// Pair holds an ordered key-value entry.
type Pair struct {
	Key   string
	Value string
}

// Node represents an individual element inside the skip list with multi-level forward pointers.
type Node struct {
	Key     string
	Value   string
	forward []*Node
}

// SkipList represents a concurrent, probabilistic, ordered index.
// It provides O(log n) expected time complexity for search, insertion, and deletion,
// as well as efficient O(log n + k) range scans.
type SkipList struct {
	mu       sync.RWMutex
	head     *Node
	maxLevel int
	p        float64
	level    int
	size     int
	rng      *rand.Rand
	rngMu    sync.Mutex
}

// New creates and initializes a new SkipList with custom maxLevel and probability.
func New(maxLevel int, p float64) *SkipList {
	if maxLevel <= 0 {
		maxLevel = DefaultMaxLevel
	}
	if p <= 0 || p >= 1 {
		p = DefaultProbability
	}

	head := &Node{
		forward: make([]*Node, maxLevel),
	}

	return &SkipList{
		head:     head,
		maxLevel: maxLevel,
		p:        p,
		level:    1,
		size:     0,
		rng:      rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

// randomLevel generates a level using a geometric distribution parameterized by p.
func (sl *SkipList) randomLevel() int {
	sl.rngMu.Lock()
	defer sl.rngMu.Unlock()

	lvl := 1
	for lvl < sl.maxLevel && sl.rng.Float64() < sl.p {
		lvl++
	}
	return lvl
}

// Put inserts or updates a key-value pair in the skip list.
// Returns true if an existing key was updated, or false if a new key was inserted.
func (sl *SkipList) Put(key, value string) bool {
	sl.mu.Lock()
	defer sl.mu.Unlock()

	update := make([]*Node, sl.maxLevel)
	curr := sl.head

	// Traverse from top level down to find insert position at each level
	for i := sl.level - 1; i >= 0; i-- {
		for curr.forward[i] != nil && curr.forward[i].Key < key {
			curr = curr.forward[i]
		}
		update[i] = curr
	}

	curr = curr.forward[0]

	// Key already exists: update in-place
	if curr != nil && curr.Key == key {
		curr.Value = value
		return true
	}

	// Key does not exist: generate level and splice in new node
	lvl := sl.randomLevel()
	if lvl > sl.level {
		for i := sl.level; i < lvl; i++ {
			update[i] = sl.head
		}
		sl.level = lvl
	}

	newNode := &Node{
		Key:     key,
		Value:   value,
		forward: make([]*Node, lvl),
	}

	for i := 0; i < lvl; i++ {
		newNode.forward[i] = update[i].forward[i]
		update[i].forward[i] = newNode
	}

	sl.size++
	return false
}

// Get retrieves the value associated with key.
// Returns (value, true) if found, or ("", false) if not present.
func (sl *SkipList) Get(key string) (string, bool) {
	sl.mu.RLock()
	defer sl.mu.RUnlock()

	curr := sl.head
	for i := sl.level - 1; i >= 0; i-- {
		for curr.forward[i] != nil && curr.forward[i].Key < key {
			curr = curr.forward[i]
		}
	}

	curr = curr.forward[0]
	if curr != nil && curr.Key == key {
		return curr.Value, true
	}
	return "", false
}

// Delete removes the key from the skip list.
// Returns true if the key was found and removed, false otherwise.
func (sl *SkipList) Delete(key string) bool {
	sl.mu.Lock()
	defer sl.mu.Unlock()

	update := make([]*Node, sl.maxLevel)
	curr := sl.head

	for i := sl.level - 1; i >= 0; i-- {
		for curr.forward[i] != nil && curr.forward[i].Key < key {
			curr = curr.forward[i]
		}
		update[i] = curr
	}

	curr = curr.forward[0]
	if curr == nil || curr.Key != key {
		return false
	}

	// Remove target node from all levels it is part of
	for i := 0; i < sl.level; i++ {
		if update[i].forward[i] != curr {
			break
		}
		update[i].forward[i] = curr.forward[i]
	}

	// Shrink level if the highest levels are now empty
	for sl.level > 1 && sl.head.forward[sl.level-1] == nil {
		sl.level--
	}

	sl.size--
	return true
}

// Scan returns an ordered slice of key-value pairs where startKey <= Key <= endKey.
// If endKey is empty (""), it scans from startKey to the end of the list.
func (sl *SkipList) Scan(startKey, endKey string) []Pair {
	sl.mu.RLock()
	defer sl.mu.RUnlock()

	var results []Pair

	curr := sl.head
	for i := sl.level - 1; i >= 0; i-- {
		for curr.forward[i] != nil && curr.forward[i].Key < startKey {
			curr = curr.forward[i]
		}
	}

	// Advance to candidate node at level 0
	curr = curr.forward[0]

	for curr != nil {
		if endKey != "" && curr.Key > endKey {
			break
		}
		results = append(results, Pair{
			Key:   curr.Key,
			Value: curr.Value,
		})
		curr = curr.forward[0]
	}

	return results
}

// Len returns the current number of elements in the skip list.
func (sl *SkipList) Len() int {
	sl.mu.RLock()
	defer sl.mu.RUnlock()
	return sl.size
}

// All returns all key-value pairs stored in the skip list in ascending order.
func (sl *SkipList) All() []Pair {
	return sl.Scan("", "")
}
