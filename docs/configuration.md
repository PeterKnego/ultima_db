# UltimaDB Configuration Reference

Every UltimaDB store is constructed from a `StoreConfig` (`Store::new(StoreConfig)`,
or `Store::default()` for all defaults). This document lists every config setting,
what it does, when to use it, its default, and which settings can/can't be combined.

> **Defaults at a glance:** `StoreConfig::default()` is an **in-memory, single-writer,
> snapshot-isolation** store with no persistence and auto snapshot GC. It's the right
> starting point; change only what you need.

---

## 1. `StoreConfig` fields

| Field | Type | Default | What it does |
|---|---|---|---|
| `num_snapshots_retained` | `usize` | **`10`** | How many most-recent snapshots `Store::gc()` keeps. The latest is always kept regardless. |
| `auto_snapshot_gc` | `bool` | **`true`** | Run `gc()` automatically after each `commit()`. |
| `writer_mode` | `WriterMode` | **`SingleWriter`** | Writer concurrency model (§2). |
| `isolation_level` | `IsolationLevel` | **`SnapshotIsolation`** | Transaction isolation (§3). |
| `require_explicit_version` | `bool` | **`false`** | If `true`, `begin_write(None)` errors (`ExplicitVersionRequired`) — you must pass `Some(v)`. For SMR (§6). |
| `persistence` | `Persistence` | **`None`** (in-memory) | Durability mode (§4); requires the `persistence` cargo feature. |

`StoreConfig` is `#[non_exhaustive]`; construct it with the builder (fields keep
their `Default` unless set):

```rust
let store = Store::new(
    StoreConfig::builder()
        .persistence(Persistence::standalone_fast(dir))  // override one field
        .build(),
)?;
```

---

## 2. `WriterMode` — writer concurrency

| Variant | Default | What / when |
|---|---|---|
| `SingleWriter` | **✓ default** | At most one active `WriteTx`; a second `begin_write` returns `WriterBusy`. Zero tracking overhead. Use for single-threaded writers or when writes are externally serialized. |
| `MultiWriter` | | Multiple concurrent `WriteTx` with key-level optimistic concurrency; overlapping modified-key sets → `WriteConflict` (retry/rebase). Disjoint keys both commit. Use when several threads write concurrently. |

To use concurrent writers, clone the `Store` and call `begin_write` per thread (`Store: Send + Sync + Clone`).

---

## 3. `IsolationLevel` — transaction isolation

| Variant | Default | What / when |
|---|---|---|
| `SnapshotIsolation` | **✓ default** | Prevents dirty / non-repeatable / phantom reads; does **not** prevent write skew. Reads are untracked (zero overhead). |
| `Serializable` | | SSI: every `WriteTx` read is tracked; commit fails with `SerializationFailure` if a read was invalidated since the tx's base. Prevents write skew. Cost: read-set tracking per read (point reads precise; range/scan/index reads recorded as a coarse "table-touched" flag → possible false positives). |

> **Only effective under `MultiWriter`.** Under `SingleWriter` there are no concurrent writers, so `Serializable` is equivalent to `SnapshotIsolation` (no validation happens). `ReadTx` always gets SI guarantees in both modes.

---

## 4. `Persistence` — durability mode (requires `persistence` feature)

| Variant | Default | What / when |
|---|---|---|
| `None` | **✓ default** | In-memory only, no disk I/O. Fastest; data lost on exit. |
| `Standalone { dir, durability, wal_write }` | | UltimaDB owns durability: WAL + checkpoints in `dir`. Tune via `durability` (§4.1) and `wal_write` (§4.2). |
| `Smr { dir }` | | Checkpoint-only, **no WAL**. For Raft/Paxos deployments where the consensus log provides durability. Pair with `require_explicit_version: true` (§6). |

`Persistence::standalone_fast(dir)` is a **preset** = `Standalone { dir, durability: ConsistentInline, wal_write: CoalescedPrealloc }` — the fastest durable single-writer config (§7).

### 4.1 `Durability` — when `commit()` becomes durable (Standalone only)

`Durability` has **no default** — you must name it inside `Standalone`.

| Variant | What / when |
|---|---|
| `Eventual` | `commit()` returns immediately; a background thread fsyncs asynchronously. Highest throughput; last unflushed commits can be lost on crash. Use when some loss window is acceptable. |
| `Consistent` | `commit()` blocks until the entry is fsynced (via the WAL background thread). No data loss on crash. The standard durable choice; works with any `WriterMode`. |
| `ConsistentInline` | Same guarantee as `Consistent`, but the **committing thread does the fsync itself, off the store lock** — no cross-thread handoff (~20 µs/commit saved; biggest win on fast disk). **SingleWriter only** (see §5). See `docs/tasks/task38_wal_inline_fsync.md`. |

### 4.2 `WalWrite` — how a committed batch is written to disk (Standalone only)

Orthogonal to `Durability` — any combination is valid (within the SingleWriter rule for `ConsistentInline`).

| Variant | Default | What / when |
|---|---|---|
| `PerEntry` | **✓ default** | One `write` per entry + `sync_all` per batch. The original behavior. |
| `Coalesced` | | Whole batch coalesced into one `write` + `sync_all`. Same durability, fewer syscalls — better group-commit throughput under Eventual / high concurrency. See task31. |
| `CoalescedPrealloc` | | Positioned writes into a physically pre-zero-filled `wal.bin`, so each commit's fsync is a metadata-free `fdatasync` (~2.5× on durable writes). Recovery uses a tail-tolerant scan. See `docs/tasks/task37_wal_preallocation.md`. |

(WAL/checkpoint CRC uses hardware-accelerated `crc32fast` regardless of `wal_write`.)

---

## 5. Compatibility — which settings combine

**Hard rule (enforced):**
- **`Durability::ConsistentInline` requires `WriterMode::SingleWriter`.** `Store::new` returns `Err(Persistence("...requires WriterMode::SingleWriter"))` for the `ConsistentInline` + `MultiWriter` combo. (Inline appends in lock-acquisition order, which only equals version order under a single writer.)

**Soft rules (no error, but the setting is inert):**
- **`IsolationLevel::Serializable` is a no-op under `SingleWriter`** (equivalent to `SnapshotIsolation`). It only does work under `MultiWriter`.
- **`Durability` and `WalWrite` apply only to `Persistence::Standalone`.** They're ignored by `None` (no WAL) and `Smr` (no WAL).
- **`require_explicit_version` is meaningful with `Smr`** (the consensus layer assigns versions); it works in any mode but is intended for SMR.

**Independent / freely combinable:**
- `wal_write` × `durability` — all combinations valid inside `Standalone` (subject to the inline+SingleWriter rule). E.g. `MultiWriter + Consistent + CoalescedPrealloc` is valid and recommended for concurrent durable writers on a real disk.
- `num_snapshots_retained`, `auto_snapshot_gc` — independent of everything else.
- `writer_mode` × `isolation_level` — any pairing (Serializable just needs MultiWriter to matter).

### Quick matrix (Durability × WriterMode)

| | `SingleWriter` | `MultiWriter` |
|---|---|---|
| `Eventual` | ✓ | ✓ |
| `Consistent` | ✓ | ✓ |
| `ConsistentInline` | ✓ | ✗ **error** |

---

## 6. SMR (state-machine replication) deployments

`Persistence::Smr { dir }` + `require_explicit_version: true`: UltimaDB does
checkpoints only; durability and version assignment come from the external
consensus log (Raft/Paxos). `begin_write(Some(log_index))` records the consensus
version; `begin_write(None)` errors. See `docs/tasks/task15_three_phase_consistent_persistence.md`.

---

## 7. Recommended recipes

| Goal | Config |
|---|---|
| **Fastest, ephemeral** (cache, tests) | `StoreConfig::default()` (`Persistence::None`) |
| **Durable, single writer, fast disk** *(lowest commit latency)* | `Persistence::standalone_fast(dir)` = `Standalone { ConsistentInline, CoalescedPrealloc }` |
| **Durable, single writer, portable** | `Standalone { dir, Consistent, CoalescedPrealloc }` |
| **Durable, concurrent writers** | `MultiWriter` + `Standalone { dir, Consistent, CoalescedPrealloc }` |
| **High write throughput, bounded loss OK** | `Standalone { dir, Eventual, Coalesced }` |
| **Write-skew prevention** | `MultiWriter` + `isolation_level: Serializable` |
| **Raft/Paxos cluster** | `Smr { dir }` + `require_explicit_version: true` |

For larger snapshot history (long-lived readers / time-travel), raise `num_snapshots_retained` (default 10); set `auto_snapshot_gc: false` to control GC timing manually.
