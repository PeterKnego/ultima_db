# How to choose a configuration for your workload

Build the `StoreConfig` from your durability and concurrency needs, not the
other way around. Field-by-field details are in the
[configuration reference](../reference/configuration.md).

## Start from the goal

| If you need | Use |
|---|---|
| Fastest possible, data is disposable (cache, tests) | `StoreConfig::default()` — in-memory, no persistence |
| Durable, one writer, lowest commit latency on fast disk | `Persistence::standalone_fast(dir)` |
| Durable, one writer, portable across writer modes later | `Persistence::standalone(dir, Durability::Consistent, WalWrite::CoalescedPrealloc)` |
| Durable, several concurrent writer threads | `WriterMode::MultiWriter` + `Persistence::standalone(dir, Durability::Consistent, WalWrite::CoalescedPrealloc)` |
| Maximum write throughput, a small loss window on crash is acceptable | `Persistence::standalone(dir, Durability::Eventual, WalWrite::Coalesced)` |
| Serializable read-modify-write (write-skew prevention) | `WriterMode::MultiWriter` + `IsolationLevel::Serializable` |
| A Raft/Paxos consensus log above the store | `Persistence::smr(dir)` + `require_explicit_version(true)` |

Assemble with the builder:

```rust
let store = Store::new(
    StoreConfig::builder()
        .writer_mode(WriterMode::MultiWriter)
        .persistence(Persistence::standalone(
            dir,
            Durability::Consistent,
            WalWrite::CoalescedPrealloc,
        ))
        .build(),
)?;
```

## Decide each axis

**Writer mode.** Stay on the default `SingleWriter` if one thread writes, or
your application already serializes writes — it has zero conflict-tracking
overhead. Switch to `MultiWriter` only when several threads must hold write
transactions concurrently; then be ready to retry on `Error::WriteConflict`
(see [How to handle write conflicts](handle-write-conflicts.md)).

**Durability.** If every acknowledged commit must survive a crash, use
`Consistent` — or `ConsistentInline` when you are certain the store stays
SingleWriter (it is rejected under MultiWriter). If you can tolerate losing
the last unflushed commits, `Eventual` removes the fsync from the commit
path entirely.

**WAL write mode.** Prefer `CoalescedPrealloc` on a real disk — it makes each
commit's fsync a metadata-free `fdatasync`. Use `Coalesced` if preallocating
a WAL file up front is undesirable. `PerEntry` is the default but has no
advantage other than being the oldest code path.

**Isolation.** Keep the default `SnapshotIsolation` unless transactions make
decisions based on reads of rows they don't write — that pattern is write
skew, and only `Serializable` (under `MultiWriter`) detects it. See
[How to prevent write skew](prevent-write-skew.md). Setting `Serializable`
under `SingleWriter` is harmless but does nothing.

**Snapshot retention.** If long-lived readers or time-travel reads need more
history, raise `num_snapshots_retained` (default 10). To control GC timing
yourself — for example, batching GC off the commit path — set
`auto_snapshot_gc(false)` and call `Store::gc()` when convenient. To keep one
specific version alive across GC regardless of retention, use
`Store::pin_version`.

**B-tree fanout.** For write-dominated SMR deployments, the `fanout-t8` cargo
feature trades read latency for roughly 1.8× contended write throughput.
Leave it off otherwise.

## Combinations to avoid

- `ConsistentInline` + `MultiWriter` — rejected at `Store::new`.
- `Durability`/`WalWrite` settings with `Persistence::None` or
  `Persistence::smr` — silently ignored; don't rely on them.
- `Serializable` + `SingleWriter` — inert; if you wanted write-skew
  detection, you also want `MultiWriter`.
