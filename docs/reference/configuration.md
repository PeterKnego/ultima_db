# Configuration reference

Every UltimaDB store is constructed from a `StoreConfig`: `Store::new(StoreConfig)`,
or `Store::default()` for all defaults. `StoreConfig::default()` is an in-memory,
single-writer, snapshot-isolation store with no persistence and automatic snapshot GC.

`StoreConfig` is `#[non_exhaustive]`; it is constructed with the builder. Fields
keep their `Default` unless set:

```rust
let store = Store::new(
    StoreConfig::builder()
        .persistence(Persistence::standalone_fast(dir))
        .build(),
)?;
```

## Fields

The fields are `num_snapshots_retained`, `auto_snapshot_gc`, `writer_mode`,
`isolation_level`, `require_explicit_version`, and `persistence`. Per-field
semantics and defaults are documented on the struct itself:
[`StoreConfig` on docs.rs](https://docs.rs/ultima-db/latest/ultima_db/struct.StoreConfig.html).
This page covers what no single field's documentation carries: the behavior
of the enums behind `writer_mode`, `isolation_level`, and `persistence`, and
the legal combinations across fields.

## `WriterMode`

| Variant | Default | Description |
|---|---|---|
| `SingleWriter` | ✓ | At most one active `WriteTx`; a second `begin_write` returns `Error::WriterBusy`. No conflict-tracking overhead. |
| `MultiWriter` | | Multiple concurrent `WriteTx` with key-level optimistic concurrency control. Writers whose modified-key sets overlap on the same table conflict at commit: the later commit returns `Error::WriteConflict`. Writers with disjoint key sets both commit. |

`Store` is `Send + Sync + Clone`; concurrent writers each call `begin_write` on
their own clone of the `Store`.

## `IsolationLevel`

| Variant | Default | Description |
|---|---|---|
| `SnapshotIsolation` | ✓ | Prevents dirty reads, non-repeatable reads, and phantom reads. Does **not** prevent write skew. Reads are untracked (zero overhead). |
| `Serializable` | | SSI: every read made through a `WriteTx` is tracked; commit fails with `Error::SerializationFailure` if a tracked read was invalidated by a concurrent commit since the transaction's base version. Additionally prevents write skew. Point reads are tracked per key; range/scan/index reads are tracked as a coarse per-table flag. |

`Serializable` takes effect only under `WriterMode::MultiWriter`. Under
`SingleWriter` there are no concurrent writers, so `Serializable` is equivalent
to `SnapshotIsolation` and no tracking or validation overhead is paid. `ReadTx`
always has snapshot-isolation guarantees in both modes. See the
[isolation levels reference](isolation-levels.md).

## `Persistence` (requires the `persistence` cargo feature)

`Persistence` is `#[non_exhaustive]`; it is built with its constructors:
`Persistence::standalone(dir, durability, wal_write)`,
`Persistence::standalone_fast(dir)`, `Persistence::smr(dir)`.

| Variant | Default | Description |
|---|---|---|
| `None` | ✓ | In-memory only; no disk I/O. Data is lost when the process exits. |
| `Standalone { dir, durability, wal_write }` | | UltimaDB owns durability: write-ahead log plus checkpoints in `dir`. Tuned by `durability` and `wal_write` below. |
| `Smr { dir }` | | Checkpoint-only; no WAL. For deployments where an external consensus log (Raft/Paxos) provides durability. |

`Persistence::standalone_fast(dir)` is a preset equal to
`Standalone { dir, durability: ConsistentInline, wal_write: CoalescedPrealloc }`.
It inherits `ConsistentInline`'s SingleWriter-only restriction.

### `Durability` — when `commit()` becomes durable (Standalone only)

`Durability` has no default; it must be named inside `Standalone`.

| Variant | Description |
|---|---|
| `Eventual` | `commit()` returns immediately; a background thread fsyncs asynchronously. Commits not yet flushed at the time of a crash are lost. |
| `Consistent` | `commit()` blocks until the entry is fsynced, via the WAL background thread. Acknowledged commits survive a crash. Valid with any `WriterMode`. |
| `ConsistentInline` | Same guarantee as `Consistent`; the committing thread performs the fsync itself, off the store lock, with no cross-thread handoff. **SingleWriter only** — see the compatibility rules below. |

### `WalWrite` — how a committed batch is written (Standalone only)

Orthogonal to `Durability`; any combination is valid within the
`ConsistentInline` restriction.

| Variant | Default | Description |
|---|---|---|
| `PerEntry` | ✓ | One `write` per entry, then `sync_all` per batch. |
| `Coalesced` | | The whole batch is coalesced into one `write`, then `sync_all`. Identical durability; fewer syscalls. |
| `CoalescedPrealloc` | | Positioned writes into a physically pre-zero-filled `wal.bin`; each commit's fsync is a metadata-free `fdatasync`. Recovery uses a tail-tolerant scan. |

WAL and checkpoint integrity checks use hardware-accelerated CRC32
(`crc32fast`) under every `WalWrite` variant.

## Compatibility rules

Enforced (construction fails):

- `Durability::ConsistentInline` requires `WriterMode::SingleWriter`.
  `Store::new` returns `Err(Error::Persistence(..))` for
  `ConsistentInline` + `MultiWriter`.

Inert combinations (no error; the setting has no effect):

- `IsolationLevel::Serializable` under `SingleWriter` behaves as
  `SnapshotIsolation`.
- `durability` and `wal_write` apply only to `Persistence::Standalone`; they
  are ignored by `None` and `Smr` (neither has a WAL).

Freely combinable:

- `wal_write` × `durability` — all combinations inside `Standalone`, subject
  to the `ConsistentInline` rule.
- `num_snapshots_retained` and `auto_snapshot_gc` — independent of everything
  else.
- `writer_mode` × `isolation_level` — any pairing.
- `require_explicit_version` — works in any mode.

### Durability × WriterMode matrix

| | `SingleWriter` | `MultiWriter` |
|---|---|---|
| `Eventual` | ✓ | ✓ |
| `Consistent` | ✓ | ✓ |
| `ConsistentInline` | ✓ | ✗ error at `Store::new` |
