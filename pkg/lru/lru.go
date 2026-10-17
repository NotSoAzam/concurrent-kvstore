package lru

import (
	"sync"
)

// Node represents an element in the custom doubly linked list.
type Node struct {
	key   string
	value string
	prev  *Node
	next  *Node
}

// Cache implements an O(1) thread-safe Least Recently Used (LRU) eviction cache
// using a custom doubly linked list and a hash map.
type Cache struct {
	mu       sync.Mutex
	capacity int
	items    map[string]*Node
	head     *Node // Sentinel dummy head (head.next is Most Recently Used)
	tail     *Node // Sentinel dummy tail (tail.prev is Least Recently Used)
}

// New constructs an LRU cache with the specified positive capacity.
func New(capacity int) *Cache {
	if capacity <= 0 {
		capacity = 128
	}

	c := &Cache{
		capacity: capacity,
		items:    make(map[string]*Node, capacity),
		head:     &Node{},
		tail:     &Node{},
	}

	c.head.next = c.tail
	c.tail.prev = c.head

	return c
}

// Get retrieves a key's value and marks it as the most recently used.
func (c *Cache) Get(key string) (string, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	node, exists := c.items[key]
	if !exists {
		return "", false
	}

	c.moveToHead(node)
	return node.value, true
}

// Peek retrieves a key's value without modifying its recency.
func (c *Cache) Peek(key string) (string, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	node, exists := c.items[key]
	if !exists {
		return "", false
	}
	return node.value, true
}

// Put inserts or updates a key-value pair.
// If capacity is reached, it evicts the least recently used element and returns its key, value, and evicted=true.
func (c *Cache) Put(key, value string) (evictedKey string, evictedVal string, evicted bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if node, exists := c.items[key]; exists {
		node.value = value
		c.moveToHead(node)
		return "", "", false
	}

	newNode := &Node{
		key:   key,
		value: value,
	}

	c.items[key] = newNode
	c.addToHead(newNode)

	if len(c.items) > c.capacity {
		lruNode := c.removeTail()
		delete(c.items, lruNode.key)
		return lruNode.key, lruNode.value, true
	}

	return "", "", false
}

// Delete explicitly removes an element from the cache.
func (c *Cache) Delete(key string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	node, exists := c.items[key]
	if !exists {
		return false
	}

	c.removeNode(node)
	delete(c.items, key)
	return true
}

// Len returns the current number of items stored in the cache.
func (c *Cache) Len() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.items)
}

// Capacity returns the maximum capacity of the cache.
func (c *Cache) Capacity() int {
	return c.capacity
}

// Clear flushes all items from the cache.
func (c *Cache) Clear() {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.items = make(map[string]*Node, c.capacity)
	c.head.next = c.tail
	c.tail.prev = c.head
}

// Internal linked list manipulation helpers (must be called with c.mu held)

func (c *Cache) addToHead(node *Node) {
	node.prev = c.head
	node.next = c.head.next
	c.head.next.prev = node
	c.head.next = node
}

func (c *Cache) removeNode(node *Node) {
	node.prev.next = node.next
	node.next.prev = node.prev
	node.prev = nil
	node.next = nil
}

func (c *Cache) moveToHead(node *Node) {
	c.removeNode(node)
	c.addToHead(node)
}

func (c *Cache) removeTail() *Node {
	lruNode := c.tail.prev
	c.removeNode(lruNode)
	return lruNode
}
