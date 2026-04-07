# Task 13: Persistence

## Goal

Add configurable persistence to UltimaDB with two deployment modes and two
durability levels, feature-gated behind `persistence` to avoid burdening
in-memory-only users.

## Configuration

### Cargo feature

```toml
[features]
default = []
persistence = ["dep:serde", "dep:bincode"]

[dependencies]
serde = { version = "1", features = ["derive"], optional = true }
bincode = { version = "2", features = ["serde"], optional = true }
```

Without the `persistence` feature, the library compiles exactly as before — no
Serde dependency, no persistence code.

### Runtime config

```rust
pub enum Durability {
    /// commit() returns immediately. Background thread fsyncs WAL asynchronously.
    /// Data may be lost on crash (last unflushed entries).
    Eventual,
    /// commit() blocks until WAL entry is written and fsynced. No data loss on crash.
    Consistent,
}

pub enum Persistence {
    /// In-memory only. No disk I/O. Default.
    None,
    /// UltimaDB owns durability. WAL + checkpoints. WAL auto-pruned on checkpoint.
    Standalone { dir: PathBuf, durability: Durability },
    /// Consensus log owns durability. Checkpoints only — no WAL.
    Smr { dir: PathBuf },
}
```

| Mode | WAL | Checkpoint | Use case |
|---|---|---|---|
| `Persistence::None` | No | No | In-memory only (default) |
| `Persistence::Standalone` | Yes | Yes | Single-node durable storage |
| `Persistence::Smr` | No | Yes | State machine replication (consensus log owns durability) |

## Architecture

### Feature gating via Record trait

The `Record` supertrait centralizes the conditional Serde bounds in one place,
avoiding `cfg` proliferation across the codebase. All existing
`R: Send + Sync + 'static` bounds change to `R: Record`.

```rust
#[cfg(feature = "persistence")]
pub trait Record: Send + Sync + Serialize + DeserializeOwned + 'static {}
#[cfg(feature = "persistence")]
impl<T: Send + Sync + Serialize + DeserializeOwned + 'static> Record for T {}

#[cfg(not(feature = "persistence"))]
pub trait Record: Send + Sync + 'static {}
#[cfg(not(feature = "persistence"))]
impl<T: Send + Sync + 'static> Record for T {}
```

### Type registry (`src/registry.rs`)

Tables are `Arc<dyn Any>` in snapshots, so persistence needs type-erased
serialization. `Store::register_table::<R>(name)` captures closures that can
serialize/deserialize records and full tables for type `R`:

```rust
let store = Store::new(config);
store.register_table::<User>("users");
store.register_table::<Order>("orders");
```

Internally, `TableTypeInfo` stores:
- `serialize_record` / `deserialize_record` — single record via Serde
- `serialize_table` / `deserialize_table` — entire `Table<R>` for checkpoints
- `new_empty_table` — create empty `Table<R>` as `Box<dyn Any>`
- `replay_insert` / `replay_update` / `replay_delete` — apply ops during WAL recovery

`open_table::<R>("name")` validates the registered type via `TypeId`.

### WAL (`src/wal.rs`)

Row-level deltas per transaction. Standalone mode only.

```rust
enum WalOp {
    Insert { table: String, id: u64, data: Vec<u8> },
    Update { table: String, id: u64, data: Vec<u8> },
    Delete { table: String, id: u64 },
    CreateTable { name: String },
    DeleteTable { name: String },
}

struct WalEntry {
    version: u64,
    ops: Vec<WalOp>,
}
```

File format: append-only `[entry_len:u32][serialized WalEntry][crc32:u32]`.

`WalHandle` encapsulates the writer: `Consistent` wraps a `File` directly,
`Eventual` sends entries through an `mpsc::channel` to a background thread.
`Store::drop` drops the sender, causing the background thread to drain and exit.

### Commit path

```
WriteTx::commit()
  ├─ OCC validation (existing, unchanged)
  ├─ Build new snapshot (existing, unchanged)
  ├─ if Persistence::Standalone:
  │   ├─ Serialize ops → WalEntry
  │   ├─ Durability::Eventual → push to mpsc::channel, return immediately
  │   └─ Durability::Consistent → write + fsync, return after fsync
  └─ Insert snapshot into StoreInner (existing, unchanged)
```

`TableWriter` methods (insert, update, delete, batch variants) serialize records
and push `WalOp`s when persistence is enabled.

### Checkpoints (`src/checkpoint.rs`)

Full snapshot serialization triggered manually by `store.checkpoint()`. The
checkpoint design is driven by SMR requirements: in SMR deployments, the
consensus log owns durability, but nodes need periodic snapshots to avoid
replaying the entire log on restart or when new replicas join the cluster.
Standalone mode reuses the same checkpoint mechanism for fast recovery.

File format:
```
[magic: 4 bytes "ULDB"]
[format_version: u32]
[snapshot_version: u64]
[num_tables: u32]
for each table:
    [name_len: u32][name: bytes]
    [data_len: u64][serialized Table<R>: bytes]
[crc32: u32]
```

On checkpoint: serialize snapshot (no lock held), write + fsync, prune WAL
(Standalone), delete old checkpoint files. The store continues serving reads
and writes during serialization — only the caller thread blocks.

### Recovery

Three-step API:

```rust
let store = Store::new(config);
store.register_table::<User>("users");
store.recover()?;
```

**Standalone recovery:**
1. Load latest checkpoint → reconstruct `Snapshot`
2. Read WAL, replay entries with `version > checkpoint.version`
3. Replay applies ops directly to table maps via registry functions (bypasses
   `WriteTx` to avoid re-writing ops to WAL)

**SMR recovery:**
1. Load latest checkpoint → reconstruct `Snapshot`
2. Return `store.latest_version()` to application
3. Application replays consensus log from that version forward

**No checkpoint found:**
- Standalone: replay entire WAL from beginning
- SMR: return version 0, application replays entire consensus log

## Files

| File | Purpose |
|---|---|
| `src/persistence.rs` | `Record` trait, `Durability`, `Persistence` enums |
| `src/registry.rs` | Type-erased serialization registry |
| `src/wal.rs` | WAL ops, file I/O, background writer |
| `src/checkpoint.rs` | Checkpoint serialization, file management |
| `src/store.rs` | Integration: WAL handle, commit path, recovery |
| `src/error.rs` | New error variants: `Persistence`, `TableNotRegistered`, `WalCorrupted`, `CheckpointCorrupted` |
| `tests/persistence_integration.rs` | 9 integration tests |

## Test coverage

### Unit tests (21 new)
- Registry: record/table serialize roundtrip, duplicate registration, type validation (7)
- WAL: entry roundtrip, CRC corruption, file I/O, pruning, op capture, handles (9)
- Checkpoint: roundtrip, CRC corruption, file I/O, version selection, cleanup (5)

### Integration tests (9 new)
- `standalone_wal_recovery_consistent`: WAL-only recovery
- `standalone_checkpoint_roundtrip`: Checkpoint-only recovery
- `standalone_checkpoint_plus_wal_recovery`: Checkpoint + WAL replay
- `smr_checkpoint_recovery`: SMR mode (checkpoint only)
- `wal_pruned_after_checkpoint`: WAL pruning verification
- `standalone_eventual_basic`: Eventual durability smoke test
- `persistence_none_unchanged`: No regression for in-memory mode
- `wal_update_and_delete_recovery`: Update/delete ops survive recovery
- `wal_delete_table_recovery`: Table deletion survives recovery

## Decisions

1. **Serialize/deserialize for Arc uniqueness during replay**: When replaying WAL
   on top of a checkpoint, tables loaded from the checkpoint have multiple Arc
   references (from the snapshot stored in `inner.snapshots`). We serialize and
   deserialize each table to create fresh Arcs with refcount 1, enabling
   `Arc::get_mut` during replay.

2. **`recover()` as instance method**: Allows users to register types between
   `Store::new()` and `recover()`, ensuring the registry is populated before
   WAL replay begins. Originally designed as `Store::open()` static constructor,
   but the registry would be empty at that point.

3. **Public `wal` module**: Exposed for integration testing (`read_wal` used to
   verify WAL pruning).

4. **Bincode 2 for serialization**: Compact, fast, already in dev-dependencies.
   Used for WAL entries, checkpoint table data, and record serialization.

## Future optimizations

### Dictionary compression for checkpoints

Inspired by TerarkDB's DictZip approach: sample ~3% of records per table to
build a compression dictionary, then compress all records against it. This
exploits cross-record structural redundancy (records in the same table share
field layout) for much better ratios than generic block compression
(zstd/snappy). Strategy could be automatic by value size — skip compression for
empty/tiny values, use entropy encoding (FSE) for small values, dictionary
compression for larger ones. WAL should stay uncompressed for append performance.
