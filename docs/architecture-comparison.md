# Architecture Comparison: UltimaDB vs RocksDB vs LMDB

This document compares the internal architecture of UltimaDB with two widely-used embedded storage engines: RocksDB (Facebook/Meta) and LMDB (Symas/OpenLDAP). The goal is to understand where UltimaDB sits in the design space and what can be learned from production systems.

---

## At a glance

| | UltimaDB | LMDB | RocksDB |
|---|---|---|---|
| **Language** | Rust | C | C++ |
| **Storage** | In-memory only | Memory-mapped file (B+ tree) | On-disk LSM tree + WAL |
| **Data structure** | Persistent CoW B-tree (Arc nodes) | CoW B+ tree (page-level CoW) | LSM tree (memtable → SST files) |
| **Concurrency** | Single-writer (design convention) | Single-writer, multi-reader (mutex-enforced) | Concurrent writers (group commit) + lock-free readers |
| **MVCC mechanism** | Immutable snapshots via Arc | Two alternating meta pages | Sequence numbers on every key |
| **Isolation level** | Snapshot Isolation | Serializable (single writer) | Snapshot Isolation (default); Serializable (optional) |
| **Durability** | None (in-memory) | fsync on commit; OS page cache | WAL + configurable fsync modes |
| **Max DB size** | Process memory | Address space (mmap) | Disk |
| **Dependencies** | `thiserror` only | None (single .c file) | ~100K+ lines of C++ |

---

## Storage engine design

### UltimaDB: Persistent functional B-tree

UltimaDB uses a persistent (functional) B-tree where every mutation returns a new tree root. Unchanged subtrees are shared between versions via `Arc<BTreeNode<R>>`. Values are stored as `Arc<R>` to avoid requiring `R: Clone`.

```
insert(key=7) on tree with root A:

     A [3, 5, 9]              A' [3, 5, 9]
    / |   |   \              / |   |    \
   B  C   D    E            B  C   D'    E    ← only D' is new
```

The old root A and all its children remain accessible. This is how snapshot isolation works: a `ReadTx` holds `Arc<Snapshot>` which holds `Arc` pointers to the old roots.

**Trade-offs:**
- (+) O(1) snapshot creation (clone an Arc)
- (+) No locks needed between readers and writers
- (+) Multiple versions coexist naturally
- (-) Memory overhead from Arc reference counts and pointer indirection
- (-) No locality — nodes are heap-allocated, not contiguous in memory
- (-) No persistence to disk

### LMDB: Page-level copy-on-write B+ tree

LMDB uses a B+ tree stored in a memory-mapped file. The entire database is accessible via `mmap`, so reads are zero-copy: `mdb_get()` returns a pointer directly into mapped memory.

MVCC is achieved through page-level copy-on-write. When a write transaction modifies a page:

1. Allocate a new page from the free list
2. Copy the old page contents to the new page
3. Update the parent to point to the new page
4. Record the old page number for reclamation

Two **meta pages** (pages 0 and 1) serve as the snapshot roots. Each commit writes to meta page #(txnid % 2). Readers pick the meta page with the highest txnid. This gives atomic snapshot transitions without locks on the data.

**Trade-offs:**
- (+) Zero-copy reads (return pointers into mmap)
- (+) No application-level page cache — the OS handles it
- (+) Crash-safe without recovery procedures (CoW + ordered writes)
- (+) Extremely simple — single C file (~11K lines)
- (-) Single writer at a time (mutex-enforced)
- (-) Database size limited by address space (though large on 64-bit)
- (-) Write amplification: modifying one key copies O(tree height) pages
- (-) Long-lived readers prevent page reclamation, causing DB bloat

### RocksDB: Log-Structured Merge tree

RocksDB uses an LSM tree. Writes go to a WAL (Write-Ahead Log), then to an in-memory memtable (skip list). When the memtable fills, it becomes immutable and is flushed to an on-disk SST (Sorted String Table) file at Level 0. Background compaction merges SST files into deeper levels.

```
Write → WAL → Memtable → flush → L0 SST → compact → L1 → L2 → ...
```

Reads check the memtable, then immutable memtables, then each SST level. Bloom filters and block indexes avoid unnecessary I/O.

**Trade-offs:**
- (+) High write throughput (sequential WAL writes, batched flushes)
- (+) Concurrent writers via group commit with lock-free leader election
- (+) Scales to terabytes on disk
- (+) Tunable compaction strategies (Level, Universal, FIFO)
- (-) Read amplification (must check multiple levels)
- (-) Write amplification from compaction (data rewritten across levels)
- (-) Complex — hundreds of thousands of lines of C++
- (-) Needs careful tuning (memtable size, level ratios, bloom filters, etc.)

---

## Concurrency and MVCC

### UltimaDB

- **Writers** get a lazy clone of the latest snapshot. Each table is cloned on first access (O(1) — Arc bump on BTree root). Mutations build new tree paths; old paths are untouched.
- **Readers** hold `Arc<Snapshot>`, which keeps the entire version alive via reference counting. No locks, no coordination with writers.
- **Single-writer constraint** is by design convention — no runtime enforcement. Two `WriteTx` instances can coexist and both commit, producing divergent histories. This is sufficient for single-threaded use but would need a mutex or optimistic concurrency control for multi-threaded scenarios.
- **Version visibility**: `ReadTx` sees exactly the snapshot it was opened with. No other version's data can leak through.

### LMDB

- **Writers** hold an exclusive mutex (`wmutex`). Only one write transaction exists at a time, system-wide across processes.
- **Readers** record their txnid in a shared reader lock table (memory-mapped, cache-line aligned per slot). No locks on data pages — readers access the mmap directly.
- **Snapshot reclamation**: pages freed by a write transaction cannot be reused until no reader holds a txnid older than the transaction that freed them. Long-lived readers block reclamation and cause the database to grow.
- **Nested transactions**: LMDB supports child transactions that can abort independently. On child commit, dirty pages merge into the parent. UltimaDB has no equivalent.

### RocksDB

- **Writers** use a lock-free group commit protocol. One writer becomes the leader (via CAS), waits briefly for other writers to join the group, then batches all WAL writes into a single fsync. This amortizes the cost of durable writes across many concurrent writers.
- **Readers** use a lock-free **SuperVersion** mechanism. Each thread caches a pointer to the current SuperVersion (memtable + immutable memtables + SST file set) in thread-local storage. Reads proceed without any mutex or atomic operation on the fast path.
- **Snapshots** are captured by recording the current sequence number. A reader only sees keys with `seq <= snapshot_seq`. Old SST files and memtable entries are retained as long as any snapshot references them.
- **Transactions** (optional): RocksDB supports pessimistic transactions (WritePreparedTxn, WriteUnpreparedTxn with 2PC) and optimistic transactions (conflict detection at commit time).

---

## Isolation guarantees

| Anomaly | UltimaDB | LMDB | RocksDB (default) | RocksDB (pessimistic txn) |
|---|---|---|---|---|
| Dirty read | Prevented | Prevented | Prevented | Prevented |
| Nonrepeatable read | Prevented | Prevented | Prevented | Prevented |
| Phantom read | Prevented | Prevented | Prevented | Prevented |
| Write skew | **Possible** | Prevented* | **Possible** | Prevented |

\* LMDB prevents write skew trivially: only one writer can exist at a time, so all write transactions are effectively serialized. There is no concurrent write to conflict with.

UltimaDB's Snapshot Isolation is detailed in [isolation-levels.md](isolation-levels.md), including what Serializable Snapshot Isolation (SSI) would require.

RocksDB's default mode is similar to UltimaDB's: snapshot isolation without conflict detection. Its optional `TransactionDB` adds pessimistic or optimistic concurrency control for serializable transactions.

---

## Persistence and crash recovery

### UltimaDB

No persistence. All data lives in process memory. When the process exits, everything is lost. This is a deliberate design scope choice, not a limitation of the architecture — the CoW B-tree and snapshot model could support disk persistence in the future.

### LMDB

Persistence comes for free from mmap. Dirty pages are written to the memory-mapped file; `fsync` on commit ensures durability. Because of CoW semantics, the database is always in a consistent state on disk:

1. New pages are written to free space (old pages untouched)
2. Meta page is updated last, atomically (single page write)
3. On crash, the previous meta page is still valid — readers see the last committed snapshot

No WAL. No recovery log. No repair tools needed.

### RocksDB

Persistence is explicit via WAL + SST files:

1. Writes go to WAL first (32 KB block format, CRC32C checksums)
2. Memtable flushes produce SST files
3. Compaction reorganizes SST files for read efficiency

Crash recovery replays the WAL to reconstruct in-flight memtable writes. Three durability modes:
- **sync=true**: fsync per write group (highest durability)
- **sync=false**: flush to OS buffer cache (risk: last writes lost on crash)
- **manual WAL flush**: application controls `FlushWAL()` calls

---

## Memory management

### UltimaDB

All allocations are Rust heap allocations via `Arc` and `Vec`. Each B-tree node is a separate heap object. Old versions are kept alive by `Arc` reference counting — when the last `ReadTx` for a version drops, the `Arc<Snapshot>` refcount reaches zero and all unreferenced nodes are freed.

**Current gap**: `Store` retains all snapshots in `HashMap<u64, Arc<Snapshot>>` forever. Even if no `ReadTx` references an old version, the snapshot persists. A future GC pass would drop unreferenced snapshots.

### LMDB

The OS manages memory via mmap. LMDB uses a single mmap for the entire database file. Read transactions access data directly from mapped pages — no copies, no application-level cache. Write transactions allocate new pages from a free list (tracked in a special "free space" database within the B+ tree). Pages are reclaimed when no reader's txnid is older than the transaction that freed them.

### RocksDB

Complex multi-tier memory management:
- **Memtable**: arena-based allocation (ConcurrentArena allocates contiguous blocks). Entire memtable discarded atomically on flush — no individual delete operations.
- **Block cache**: LRU cache for decompressed SST data blocks (configurable size, sharded for concurrency).
- **OS page cache**: SST files are also cached by the OS.
- **Write buffer manager**: limits total memtable memory across column families.

---

## What UltimaDB can learn

### From LMDB

1. **LMDB's CoW B+ tree is the closest relative to UltimaDB's design.** Both use copy-on-write trees for MVCC. The key difference: LMDB does CoW at the page level on a memory-mapped file; UltimaDB does CoW at the node level in heap memory via Arc. LMDB's approach gives it zero-copy reads and crash recovery for free.

2. **Single-writer enforcement matters.** LMDB enforces single-writer with a mutex. UltimaDB's design-convention approach works for single-threaded use but is a foot-gun waiting to happen. A runtime write lock (even a `Mutex<()>` guarding `begin_write`) would be cheap and prevent subtle bugs.

3. **Reader-induced bloat is a real problem.** LMDB's documentation warns extensively about long-lived readers preventing page reclamation. UltimaDB has an even worse version of this: old snapshots are retained in a HashMap *even if no reader references them*. Implementing snapshot GC (drop snapshots with `Arc::strong_count() == 1`) would be a straightforward improvement.

4. **Nested transactions** are useful for implementing savepoints in application-level logic. LMDB supports them via child transactions with independent abort. UltimaDB could add this by forking the dirty map.

### From RocksDB

1. **Group commit** is the key to write throughput under concurrency. If UltimaDB adds multi-threaded write support, RocksDB's approach (lock-free leader election, batched WAL writes, parallel memtable insertion) is the gold standard, though far more than UltimaDB needs.

2. **SuperVersion for lock-free reads** is elegant: a reference-counted bundle of (memtable, immutable memtables, SST version) cached in thread-local storage. UltimaDB's `Arc<Snapshot>` is conceptually similar but simpler — no thread-local caching, no fast/slow path distinction.

3. **Sequence-number-based MVCC** (rather than version-per-snapshot) scales better when many transactions are in flight. Each key carries its birth sequence number; visibility is a simple comparison. UltimaDB's approach (separate snapshot per version) is simpler but stores redundant copies of unchanged tables across versions.

4. **Configurable durability** (sync modes, WAL recycling, manual flush control) would be relevant if UltimaDB adds persistence. The WAL block format with CRC checksums and recyclable headers is well-proven.

---

## Design space positioning

```
                        Write throughput →
                    Low                         High
               ┌────────────────────────────────────────┐
        Simple │  LMDB              UltimaDB*           │
               │  (single writer,   (single writer,     │
               │   disk-backed)      in-memory)          │
   Complexity  │                                        │
               │                                        │
               │                    RocksDB             │
       Complex │                    (concurrent writers, │
               │                     LSM, tunable)       │
               └────────────────────────────────────────┘

  * UltimaDB's in-memory nature gives it high throughput for the
    narrow case of single-threaded, ephemeral workloads.
```

UltimaDB occupies a niche: an in-memory, single-threaded MVCC store with Snapshot Isolation. It is closest in spirit to LMDB (CoW tree, single writer, simple API) but trades durability for simplicity. RocksDB targets a fundamentally different use case (high-throughput persistent storage with concurrent access) and is orders of magnitude more complex.
