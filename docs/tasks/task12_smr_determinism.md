# Task 12: SMR Determinism

## Goal

Make UltimaDB deterministic for state machine replication (SMR). Given the
same sequence of commands, every replica must produce identical state.

## Changes

### HashMap/HashSet → BTreeMap/BTreeSet

Replaced all `HashMap` and `HashSet` in library code with their ordered
equivalents:

| Location | Field | Before | After |
|---|---|---|---|
| `Snapshot` | `tables` | `HashMap<String, Arc<dyn Any>>` | `BTreeMap<String, Arc<dyn Any>>` |
| `StoreInner` | `snapshots` | `HashMap<u64, Arc<Snapshot>>` | `BTreeMap<u64, Arc<Snapshot>>` |
| `WriteTx` | `dirty` | `HashMap<String, Box<dyn Any>>` | `BTreeMap<String, Box<dyn Any>>` |
| `WriteTx` | `deleted_tables` | `HashSet<String>` | `BTreeSet<String>` |
| `WriteTx` | `write_set` | `HashMap<String, HashSet<u64>>` | `BTreeMap<String, BTreeSet<u64>>` |
| `WriteTx` | `ever_deleted_tables` | `HashSet<String>` | `BTreeSet<String>` |
| `CommittedWriteSet` | `tables` | `HashMap<String, HashSet<u64>>` | `BTreeMap<String, BTreeSet<u64>>` |
| `CommittedWriteSet` | `deleted_tables` | `HashSet<String>` | `BTreeSet<String>` |
| `Table` | `indexes` | `HashMap<String, Box<dyn IndexMaintainer>>` | `BTreeMap<String, Box<dyn IndexMaintainer>>` |
| `TableSnapshot` | `indexes` | `HashMap<String, Box<dyn IndexMaintainer>>` | `BTreeMap<String, Box<dyn IndexMaintainer>>` |

**Why this matters:** `HashMap` uses randomized hashing, so iteration order
varies between process invocations. Any code path that iterates a HashMap
and makes order-dependent decisions (index maintenance, conflict detection,
snapshot construction) becomes nondeterministic. BTreeMap/BTreeSet iterate
in key order, which is deterministic.

**Performance impact:** Negligible. These collections are keyed by table
names (typically < 100 entries) or snapshot versions. The O(log n) vs O(1)
difference is immaterial at these sizes.

### `require_explicit_version` config option

New `StoreConfig` field:
- `require_explicit_version: bool` (default: `false`)
- When `true`, `begin_write(None)` returns `Error::ExplicitVersionRequired`
- Forces the consensus layer to assign version numbers via `begin_write(Some(log_index))`

**Why this matters:** In SMR, auto-assigned versions would cause replicas to
diverge if they start from different states or process commands in different
batches. The consensus layer must control versioning.

## What this does NOT cover (deferred to durability milestone)

- Byte-level snapshot serialization (requires type registry for `dyn Any`)
- Durable snapshot persistence (writing snapshots to disk)
- Durable log index tracking (persisting last-applied index across restarts)
- Snapshot format versioning for rolling upgrades

## Testing

- All existing tests pass with BTreeMap/BTreeSet (behavioral compatibility).
- New tests verify deterministic ordering of index operations, table iteration,
  and GC behavior.
- New tests verify `require_explicit_version` enforcement.
