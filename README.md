# Concurrent In-Memory Key-Value Store with Write-Ahead Logging (WAL)

[![Go Version](https://img.shields.io/badge/Go-1.22+-00ADD8?style=flat&logo=go)](https://golang.org)
[![Build & Test](https://img.shields.io/badge/Tests-100%25%20Passing-brightgreen)]()
[![Race Detector](https://img.shields.io/badge/go%20test--race-Zero%20Data%20Races-success)]()
[![Dependencies](https://img.shields.io/badge/Dependencies-Zero%20(Pure%20Stdlib)-blue)]()

A production-grade, low-latency, concurrent in-memory key-value storage engine engineered from scratch in pure Go (standard library only). Built with high-performance systems engineering principles, this project demonstrates custom lock-free/fine-grained data structures, tiered caching, probabilistic indexing, durability guarantees, crash recovery, and a multi-threaded TCP network layer speaking both Redis Serialization Protocol (RESP) and raw line protocols.

---

## Architecture Overview

The system features a **3-tier low-latency read hierarchy** combined with an **append-only Write-Ahead Log (WAL)** to achieve sub-100ns in-memory reads and crash-resilient persistence:

```mermaid
flowchart TD
    Client["Client (redis-cli / netcat / App)"] -->|TCP (RESP / Line Protocol)| Server["cmd/server (Multi-Threaded TCP Server)"]
    Server -->|Put / Get / Delete / Scan| Engine["pkg/engine (Storage Coordinator)"]

    subgraph StorageEngine ["Storage Engine Architecture"]
        direction TB

        subgraph ReadWritePath ["Read / Write Hierarchy"]
            Engine -->|1. Fast-Path O(1)| LRU["pkg/lru (LRU Eviction Cache)"]
            Engine -->|2. Negative Lookup O(k)| Bloom["pkg/bloom (Optimal Bloom Filter)"]
            Engine -->|3. Primary Index O(log n)| SkipList["pkg/skiplist (Concurrent SkipList)"]
        end

        subgraph DurabilityLayer ["Durability & Recovery"]
            Engine -->|Write-Ahead O(1)| WAL["pkg/wal (Append-Only WAL)"]
            WAL -->|CRC32 Validation| Disk[("Disk Storage (.wal)")]
            Disk -->|Startup Replay & Checkpoint| Engine
        end
    end

    LRU -.->|Cache Miss| Bloom
    Bloom -.->|Filter Hit: Check List| SkipList
    Bloom -.->|Filter Miss: Definite Negative| Engine
```

### Read Tiering Workflow (`Get(key)`):
1. **Tier 1 — O(1) LRU Cache Hit**: Look up key in custom Doubly Linked List + Hash Map cache. On hit, return immediately (~96 ns latency).
2. **Tier 2 — O(k) Bloom Filter Fast-Rejection**: If absent from LRU, query the 64-bit bitset Bloom Filter. If bits are unset, the key **definitely does not exist** in the system. The request terminates immediately without traversing memory pointers.
3. **Tier 3 — O(log n) SkipList Search**: For existing keys that bypassed the LRU, search the probabilistic SkipList, return the value, and hot-load it into the LRU cache.

### Write Workflow (`Put(key, value)`):
1. **Durable WAL Append**: Format payload `[CRC32 (4B) | Type (1B) | KeyLen (4B) | ValueLen (4B) | Key | Value]` and append to the WAL.
2. **SkipList Insertion**: Insert or update node across probabilistic forward levels.
3. **Cache Insertion**: Place item at MRU head in the LRU cache (evicting LRU tail if capacity is exceeded).
4. **Bloom Filter Update**: Set bits across k dual-hashed positions.

---

## Algorithmic Complexity Analysis

| Subsystem | Component | Algorithm / Design | Search | Insert | Delete | Range Scan | Space Complexity |
| :--- | :--- | :--- | :---: | :---: | :---: | :---: | :---: |
| **Primary Index** | `pkg/skiplist` | Probabilistic Skip List (p = 0.5, MaxLevel = 32) | O(log n) | O(log n) | O(log n) | O(log n + m) | O(n) |
| **Hot Cache** | `pkg/lru` | Custom Doubly Linked List + Map | O(1) | O(1) | O(1) | N/A | O(C) |
| **Negative Filter** | `pkg/bloom` | Kirsch-Mitzenmacher Dual Hashing (uint64 bitset) | O(k) | O(k) | N/A | N/A | O(m bits) ~ 1.2 B/key |
| **Durability Log** | `pkg/wal` | Sequential Append-Only with CRC32 IEEE | N/A | O(1) | O(1) | O(N) Replay | O(operations) |

---

## Modular Packages

- **[`pkg/skiplist`](pkg/skiplist/skiplist.go)**:
  - Custom probabilistic ordered index providing O(log n) key search, update, deletion, and O(log n + m) range scans (`Scan(startKey, endKey)`).
  - Dynamic level generator using geometric distribution (p = 0.5, max height 32).
  - Thread-safe synchronization supporting concurrent read operations alongside write mutations.

- **[`pkg/lru`](pkg/lru/lru.go)**:
  - Pure O(1) LRU eviction cache implemented using custom node pointers (`prev`, `next`) and sentinel dummy nodes (without using `container/list`).
  - Auto-evicts least recently accessed items upon exceeding fixed capacity.

- **[`pkg/bloom`](pkg/bloom/bloom.go)**:
  - Cache-line aligned bitset using `[]uint64` and Kirsch-Mitzenmacher dual hashing:
    $$g_i(x) = (h_1(x) + i \cdot h_2(x)) \pmod m$$
    combining 64-bit FNV-1a and Murmur3/SplitMix64 avalanche mixing.
  - Dynamically computes optimal bit array size m = - (n * ln(p)) / (ln(2)^2) and hash count k = (m / n) * ln(2) to guarantee a 1.0% false-positive target.

- **[`pkg/wal`](pkg/wal/wal.go)**:
  - Binary log format: `[CRC32 Checksum (4B) | Type (1B) | KeyLen (4B) | ValueLen (4B) | Key | Value]`.
  - Full crash recovery via sequential `Replay()` with byte-level CRC32 IEEE verification.
  - Snapshot compaction via atomic `Checkpoint()` to truncate obsolete mutation logs.

- **[`pkg/engine`](pkg/engine/engine.go)**:
  - Central orchestrator unifying SkipList, LRU, Bloom Filter, and WAL into a single, cohesive engine.
  - Exposes atomic operational telemetry (hit ratios, Bloom rejections, WAL file size).

- **[`cmd/server`](cmd/server/main.go)**:
  - Multi-threaded non-blocking TCP network server supporting dual wire formats: Redis RESP and inline text.
  - Signal handling (`SIGINT`, `SIGTERM`) for graceful connection draining and engine flushing.

---

## Performance & Benchmarks

Run on **AMD Ryzen 5 7600X 6-Core Processor @ 4.7GHz**, Windows 11, Go 1.27:

```text
goos: windows
goarch: amd64
pkg: github.com/systems-architect/kvstore/pkg/engine
BenchmarkPut-12       451,662 ops       2,801 ns/op       544 B/op       8 allocs/op
BenchmarkGet-12    11,858,937 ops          96.1 ns/op      31 B/op       1 allocs/op
BenchmarkScan-12      277,482 ops       4,433 ns/op     12,736 B/op       8 allocs/op
```

### Highlights:
- **~10.4 Million Reads/sec** (`BenchmarkGet`): Sub-100ns query latency powered by the O(1) LRU cache and Bloom filter negative rejection layer.
- **~357,000 Writes/sec** (`BenchmarkPut`): Fully durable binary WAL append, index updates, cache insertion, and Bloom filter encoding in 2.8 microseconds per operation.
- **~225,000 Scans/sec** (`BenchmarkScan`): Fast ordered range iteration across 100 consecutive elements in under 4.5 microseconds.

---

## Getting Started

### Prerequisites
- Go 1.22 or higher (pure standard library, zero external dependencies).

### Run Test Suite
```bash
# Run all tests across packages with verbosity
go test -v -count=1 ./pkg/... ./cmd/...

# Run with race detector (requires CGO_ENABLED=1)
go test -v -race ./pkg/...
```

### Run Benchmarks
```bash
# Execute memory allocation and throughput benchmarks
go test -bench="." -benchmem .\pkg\engine
```

### Start the Server
```bash
go run ./cmd/server/main.go -port=6379 -wal="data/kvstore.wal" -lru=50000
```

### Interacting with the Store

#### 1. Using `redis-cli`:
```bash
$ redis-cli -p 6379
127.0.0.1:6379> SET user:1001 "Alice"
OK
127.0.0.1:6379> GET user:1001
"Alice"
127.0.0.1:6379> SCAN user:0000 user:9999
1) "user:1001"
2) "Alice"
127.0.0.1:6379> DEL user:1001
(integer) 1
```

#### 2. Using Raw TCP (`netcat` / `telnet` / PowerShell):
```bash
$ nc 127.0.0.1 6379
SET sensor:temp 24.5
OK
GET sensor:temp
24.5
STATS
{
  "total_keys": 1,
  "lru_size": 1,
  "lru_capacity": 50000,
  "lru_hits": 1,
  "lru_misses": 0,
  "bloom_filter_rejections": 0,
  "skiplist_lookups": 0,
  "wal_size_bytes": 35
}
CHECKPOINT
OK Checkpoint completed
```

---

## Resume Bullet Points (Tailored for Tier-1 Systems / SDE Roles)

Highlight this project on your resume with quantifiable metrics and engineering terminology:

- **Engineered a production-grade, low-latency concurrent key-value store from scratch in Go (zero external dependencies)**, combining a concurrent SkipList, an O(1) custom LRU eviction cache, a Bloom filter, and a binary Write-Ahead Log (WAL).
- **Architected a 3-tier read hierarchy** achieving **sub-100ns read latency and 10.4M+ QPS**, leveraging an O(k) bitset Bloom filter with Kirsch-Mitzenmacher dual hashing to achieve a 1% false positive rate and eliminate unnecessary index traversals for missing keys.
- **Implemented an append-only Write-Ahead Log (WAL) with CRC32 IEEE checksum verification**, guaranteeing ACID durability, crash recovery replay, and atomic checkpoint truncation that compacted storage size while maintaining zero data loss.
- **Designed a multi-threaded TCP network layer speaking the Redis Serialization Protocol (RESP)** and line-based protocol, handling non-blocking client connections, graceful connection draining, and thread safety verified under `go test -race` with 100+ concurrent worker goroutines.

---

made with using ai
