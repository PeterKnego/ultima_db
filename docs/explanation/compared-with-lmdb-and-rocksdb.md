# UltimaDB compared with LMDB and RocksDB

This page compares the internal architecture of UltimaDB with two
widely-used embedded storage engines: RocksDB (Facebook/Meta) and LMDB
(Symas/OpenLDAP). The goal is to understand where UltimaDB sits in the
design space — why a B+tree on mmap, an LSM tree on disk, and a
copy-on-write tree in heap memory lead to genuinely different behavior —
and which of those differences are fundamental versus incidental.

UltimaDB (`ultima-db` 0.3.0 on crates.io) is an MVCC store built on a
persistent copy-on-write B-tree. Data lives in memory; durability is
opt-in via WAL + checkpoints. Tables are typed, can be keyed by `String`,
byte strings, any integer width, or tuples, and carry secondary, custom,
and full-text indexes. Vector search lives in the companion
`ultima-vector` crate.

---

## At a glance

| | UltimaDB | LMDB | RocksDB |
|---|---|---|---|
| **Language** | Rust | C | C++ |
| **Storage** | In-memory CoW B-tree; opt-in WAL + checkpoints on disk | Memory-mapped file (B+ tree) | On-disk LSM tree + WAL |
| **Data structure** | Persistent CoW B-tree (Arc nodes) | CoW B+ tree (page-level CoW) | LSM tree (memtable → SST files) |
| **Concurrency** | Single writer (enforced) or multi-writer with key-level OCC | Single-writer, multi-reader (mutex-enforced) | Concurrent writers (group commit) + lock-free readers |
| **MVCC mechanism** | Immutable snapshots via Arc | Two alternating meta pages | Sequence numbers on every key |
| **Isolation level** | Snapshot Isolation (default); Serializable/SSI (opt-in) | Serializable (single writer) | Snapshot Isolation (default); Serializable (optional) |
| **Durability** | Opt-in: WAL + checkpoints with fsync-acknowledged or async commits; checkpoint-only SMR mode | fsync on commit; OS page cache | WAL + configurable fsync modes |
| **Max DB size** | Process memory | Address space (mmap) | Disk |
| **Core dependencies** | `thiserror`, `dashmap`, `crc32fast`, `parking_lot` (`serde`/`bincode` behind the `persistence` feature) | None (single .c file) | ~100K+ lines of C++ |

The durability and concurrency options behind the UltimaDB column are
enumerated in the [configuration reference](../reference/configuration.md);
this page is about why the columns differ, not how to set them.

---

## Storage engine design

### UltimaDB: Persistent functional B-tree

UltimaDB uses a persistent (functional) B-tree where every mutation
returns a new tree root. Unchanged subtrees are shared between versions
via `Arc<BTreeNode<K, R>>`. Values are stored as `Arc<R>` to avoid
requiring `R: Clone`. The default fanout is T=32 (63 keys per node), with
a narrower T=8 build available behind the `fanout-t8` cargo feature for
write-dominated deployments — wider nodes make reads faster (shorter
trees) but make each CoW node clone more expensive, so the right fanout
depends on the read/write mix.

```
insert(key=7) on tree with root A:

     A [3, 5, 9]              A' [3, 5, 9]
    / |   |   \              / |   |    \
   B  C   D    E            B  C   D'    E    ← only D' is new
```

The old root A and all its children remain accessible. This is how
snapshot isolation works: a `ReadTx` holds `Arc<Snapshot>` which holds
`Arc` pointers to the old roots.

**Trade-offs:**
- (+) O(1) snapshot creation (clone an Arc)
- (+) No locks needed between readers and writers
- (+) Multiple versions coexist naturally
- (-) Memory overhead from Arc reference counts and pointer indirection
- (-) No locality — nodes are heap-allocated, not contiguous in memory
- (-) The working set must fit in RAM; durability is a separate mechanism (WAL + checkpoints), not a property of the tree itself

### LMDB: Page-level copy-on-write B+ tree

LMDB uses a B+ tree stored in a memory-mapped file. The entire database
is accessible via `mmap`, so reads are zero-copy: `mdb_get()` returns a
pointer directly into mapped memory.

MVCC is achieved through page-level copy-on-write. When a write
transaction modifies a page:

1. Allocate a new page from the free list
2. Copy the old page contents to the new page
3. Update the parent to point to the new page
4. Record the old page number for reclamation

Two **meta pages** (pages 0 and 1) serve as the snapshot roots. Each
commit writes to meta page #(txnid % 2). Readers pick the meta page with
the highest txnid. This gives atomic snapshot transitions without locks
on the data.

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

RocksDB uses an LSM tree. Writes go to a WAL (Write-Ahead Log), then to
an in-memory memtable (skip list). When the memtable fills, it becomes
immutable and is flushed to an on-disk SST (Sorted String Table) file at
Level 0. Background compaction merges SST files into deeper levels.

```
Write → WAL → Memtable → flush → L0 SST → compact → L1 → L2 → ...
```

Reads check the memtable, then immutable memtables, then each SST level.
Bloom filters and block indexes avoid unnecessary I/O.

**Trade-offs:**
- (+) High write throughput (sequential WAL writes, batched flushes)
- (+) Concurrent writers via group commit with lock-free leader election
- (+) Scales to terabytes on disk
- (+) Tunable compaction strategies (Level, Universal, FIFO)
- (-) Read amplification (must check multiple levels)
- (-) Write amplification from compaction (data rewritten across levels)
- (-) Complex — hundreds of thousands of lines of C++
- (-) Needs careful tuning (memtable size, level ratios, bloom filters, etc.)

The deeper pattern: LMDB couples its data structure to its durability
story (the mmap *is* the file), RocksDB couples its data structure to its
write path (the LSM *is* a durability log, reorganized), while UltimaDB
keeps the data structure purely in-memory and bolts durability on beside
it. That decoupling is why UltimaDB can offer the same tree with no
persistence at all, with its own WAL, or with checkpoints only under an
external consensus log — and why its read path never touches the durability
machinery.

---

## Concurrency and MVCC

### UltimaDB

- **Writers** get a lazy clone of the latest snapshot. Each table is
  cloned on first access (O(1) — Arc bump on BTree root). Mutations build
  new tree paths; old paths are untouched.
- **Readers** hold `Arc<Snapshot>`, which keeps the entire version alive
  via reference counting. No locks, no coordination with writers.
- **Writer modes are enforced at runtime.** In the default `SingleWriter`
  mode, `begin_write` takes the store's writer slot; a second concurrent
  writer is refused rather than allowed to fork history. In `MultiWriter`
  mode, concurrent writers proceed optimistically and are validated at
  commit with key-level OCC: two writers conflict only if their modified
  row sets overlap on the same table, and the loser gets a
  `WriteConflict` to retry (see
  [handling write conflicts](../how-to/handle-write-conflicts.md)).
  Commits rebase onto the latest snapshot and merge per-table, so
  disjoint writers to the same table both land.
- **Version visibility**: `ReadTx` sees exactly the snapshot it was
  opened with. No other version's data can leak through.

### LMDB

- **Writers** hold an exclusive mutex (`wmutex`). Only one write
  transaction exists at a time, system-wide across processes.
- **Readers** record their txnid in a shared reader lock table
  (memory-mapped, cache-line aligned per slot). No locks on data pages —
  readers access the mmap directly.
- **Snapshot reclamation**: pages freed by a write transaction cannot be
  reused until no reader holds a txnid older than the transaction that
  freed them. Long-lived readers block reclamation and cause the database
  to grow.
- **Nested transactions**: LMDB supports child transactions that can
  abort independently. On child commit, dirty pages merge into the
  parent. UltimaDB has no equivalent.

### RocksDB

- **Writers** use a lock-free group commit protocol. One writer becomes
  the leader (via CAS), waits briefly for other writers to join the
  group, then batches all WAL writes into a single fsync. This amortizes
  the cost of durable writes across many concurrent writers.
- **Readers** use a lock-free **SuperVersion** mechanism. Each thread
  caches a pointer to the current SuperVersion (memtable + immutable
  memtables + SST file set) in thread-local storage. Reads proceed
  without any mutex or atomic operation on the fast path.
- **Snapshots** are captured by recording the current sequence number. A
  reader only sees keys with `seq <= snapshot_seq`. Old SST files and
  memtable entries are retained as long as any snapshot references them.
- **Transactions** (optional): RocksDB supports pessimistic transactions
  (WritePreparedTxn, WriteUnpreparedTxn with 2PC) and optimistic
  transactions (conflict detection at commit time).

---

## Isolation guarantees

| Anomaly | UltimaDB (default) | UltimaDB (Serializable) | LMDB | RocksDB (default) | RocksDB (pessimistic txn) |
|---|---|---|---|---|---|
| Dirty read | Prevented | Prevented | Prevented | Prevented | Prevented |
| Nonrepeatable read | Prevented | Prevented | Prevented | Prevented | Prevented |
| Phantom read | Prevented | Prevented | Prevented | Prevented | Prevented |
| Write skew | **Possible** | Prevented (SSI) | Prevented* | **Possible** | Prevented |

\* LMDB prevents write skew trivially: only one writer can exist at a
time, so all write transactions are effectively serialized. There is no
concurrent write to conflict with.

UltimaDB defaults to Snapshot Isolation and offers Serializable Snapshot
Isolation (read-set tracking, write-skew detection at commit) as an
opt-in `IsolationLevel` — see the
[isolation levels reference](../reference/isolation-levels.md) and
[preventing write skew](../how-to/prevent-write-skew.md). This mirrors
the shape of RocksDB's offering: snapshot isolation by default, stronger
guarantees when you ask for them. The interesting difference is *where*
the serializability comes from — LMDB gets it for free from its single
writer, RocksDB from lock-based transactions, UltimaDB from optimistic
validation over tracked read sets.

---

## Persistence and crash recovery

### UltimaDB

Durability is opt-in (the `persistence` cargo feature) and deliberately
layered on beside the in-memory tree rather than woven into it. Two
modes:

- **Standalone** — UltimaDB owns durability: commits append to a
  CRC-checked WAL, and checkpoints periodically capture full snapshots
  and prune the log. Recovery loads the latest checkpoint and replays the
  WAL. When a commit is *acknowledged* as durable is a policy choice —
  fsync asynchronously after `commit()` returns, block the commit until
  the WAL background thread has fsynced (group commit: one fsync covers
  every commit that joined the batch), or have the committing thread
  fsync inline. The write-path/fsync-policy split and the compatibility
  rules live in the [configuration reference](../reference/configuration.md);
  practical setup in
  [setting up durable persistence](../how-to/set-up-durable-persistence.md).
- **SMR (checkpoint-only)** — for Raft/Paxos deployments where the
  consensus log already provides durability, UltimaDB writes no WAL of
  its own and persists via checkpoints alone.

This is the RocksDB durability model (explicit WAL + checkpoint/flush +
replay-on-recovery, CRC-protected formats, configurable sync policy)
grafted onto an LMDB-shaped data structure — with the twist that the tree
itself never lives on disk, so recovery is a rebuild into memory rather
than an mmap.

### LMDB

Persistence comes for free from mmap. Dirty pages are written to the
memory-mapped file; `fsync` on commit ensures durability. Because of CoW
semantics, the database is always in a consistent state on disk:

1. New pages are written to free space (old pages untouched)
2. Meta page is updated last, atomically (single page write)
3. On crash, the previous meta page is still valid — readers see the last
   committed snapshot

No WAL. No recovery log. No repair tools needed.

### RocksDB

Persistence is explicit via WAL + SST files:

1. Writes go to WAL first (32 KB block format, CRC32C checksums)
2. Memtable flushes produce SST files
3. Compaction reorganizes SST files for read efficiency

Crash recovery replays the WAL to reconstruct in-flight memtable writes.
Three durability modes:
- **sync=true**: fsync per write group (highest durability)
- **sync=false**: flush to OS buffer cache (risk: last writes lost on crash)
- **manual WAL flush**: application controls `FlushWAL()` calls

---

## Memory management

### UltimaDB

All allocations are Rust heap allocations via `Arc`. Each B-tree node is
a separate heap object with inline fixed-capacity key/value storage, so a
CoW node clone is one allocation. Old versions are kept alive by `Arc`
reference counting — when the last reference to a version drops, all
nodes unique to it are freed.

Snapshot retention is bounded: the store keeps a configurable window of
recent versions (10 by default) and garbage-collects older ones,
automatically after each commit or on demand via `gc()`. A `VersionPin`
keeps a specific version alive across GC for as long as a caller needs it
(the handoff primitive for consistent backups and snapshot streaming).
This is UltimaDB's answer to the same problem LMDB documents as
reader-induced bloat — old versions cost memory as long as something can
still see them — except the pressure lands on the heap instead of the
database file, and the release valve is refcounts plus a retention
window instead of page reclamation.

### LMDB

The OS manages memory via mmap. LMDB uses a single mmap for the entire
database file. Read transactions access data directly from mapped pages —
no copies, no application-level cache. Write transactions allocate new
pages from a free list (tracked in a special "free space" database within
the B+ tree). Pages are reclaimed when no reader's txnid is older than
the transaction that freed them.

### RocksDB

Complex multi-tier memory management:
- **Memtable**: arena-based allocation (ConcurrentArena allocates
  contiguous blocks). Entire memtable discarded atomically on flush — no
  individual delete operations.
- **Block cache**: LRU cache for decompressed SST data blocks
  (configurable size, sharded for concurrency).
- **OS page cache**: SST files are also cached by the OS.
- **Write buffer manager**: limits total memtable memory across column
  families.

---

## What UltimaDB has taken from each — and what it hasn't

Earlier revisions of this page listed lessons UltimaDB should learn from
these systems. Several have since shipped, which is itself informative
about which ideas transfer across the design space:

- **Runtime single-writer enforcement** (LMDB's `wmutex`): adopted. The
  `SingleWriter` default refuses a second concurrent writer instead of
  relying on convention.
- **Snapshot GC** (LMDB's reclamation problem, inverted): adopted —
  bounded retention plus explicit pins, as described above.
- **Group commit** (RocksDB): adopted in spirit. Under fsync-acknowledged
  durability, the WAL background thread batches concurrent commits into
  shared fsyncs.
- **Configurable durability with CRC-protected formats** (RocksDB's WAL
  discipline): adopted — multiple fsync policies and WAL write strategies,
  CRC-checked WAL and checkpoints.

What remains genuinely different, deliberately:

1. **LMDB's zero-copy reads.** `mdb_get` returns a pointer into the mmap;
   UltimaDB deserializes nothing on read either (values are live Rust
   objects behind `Arc`), but it pays heap-allocation and pointer-chasing
   costs LMDB's contiguous pages avoid. This is the price of holding
   typed values rather than byte pages.
2. **LMDB's nested transactions** (child transactions with independent
   abort) have no UltimaDB equivalent; batch operations with atomic
   rollback cover the common case but are not savepoints.
3. **RocksDB's SuperVersion** — thread-local caching of the current
   version handle for mutex-free read startup. UltimaDB's `Arc<Snapshot>`
   is conceptually similar but simpler: no thread-local fast path.
4. **RocksDB's sequence-number MVCC.** Tagging every key with a birth
   sequence number scales visibility checks to huge numbers of in-flight
   transactions. UltimaDB's version-per-snapshot model is simpler and
   makes whole-version operations (time travel, pinning, streaming a
   snapshot) trivial, at the cost of a per-version table map — cheap
   because tables are Arc-shared, but not free.

---

## Design space positioning

```
                        Write throughput →
                    Low                          High
               ┌─────────────────────────────────────────┐
        Simple │  LMDB               UltimaDB*           │
               │  (mmap B+ tree,     (in-memory CoW tree, │
               │   single writer)     opt-in WAL)         │
   Complexity  │                                         │
               │                                         │
               │                     RocksDB             │
       Complex │                     (concurrent writers, │
               │                      LSM, tunable)       │
               └─────────────────────────────────────────┘

  * UltimaDB's throughput comes from keeping the working set in RAM;
    with durability enabled, commit latency is bounded by the chosen
    fsync policy, not by the tree. See the performance reference for
    measured numbers.
```

UltimaDB occupies a deliberate niche: an in-memory MVCC store with
typed tables, arbitrary ordered primary keys, secondary/full-text/custom
indexes, optional key-level-OCC concurrent writers, opt-in SSI, and
opt-in WAL/checkpoint durability. It is closest in spirit to LMDB — a
copy-on-write tree with cheap snapshots and a simple API — but does CoW
at the node level in heap memory rather than the page level in a mapped
file, trading LMDB's for-free crash consistency and address-space-sized
databases for RAM-speed typed access and O(1) whole-version snapshots.
RocksDB targets a fundamentally different corner (datasets much larger
than memory, sustained concurrent write ingestion) and pays for it in
complexity and tuning surface. Measured comparisons against RocksDB and
others are in the [performance reference](../reference/performance.md).
