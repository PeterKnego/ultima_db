# Task 7: Snapshot Retention

## Problem

`Store::gc()` only distinguishes between "latest + active readers" and "everything else." There's no way to retain a window of recent snapshots for late-arriving readers, and GC must be called manually.

## Design

### StoreConfig

New config struct with `Default` impl:

```rust
pub struct StoreConfig {
    /// How many most-recent snapshots to retain during gc(). Default: 10.
    pub num_snapshots_retained: usize,
    /// Whether gc() runs automatically after each WriteTx::commit(). Default: true.
    pub auto_snapshot_gc: bool,
}
```

`Store::new(config: StoreConfig)` replaces the zero-arg constructor. `Store::default()` uses `StoreConfig::default()`.

### gc() behavior

Retains:
1. The N most recent snapshots (by version number), where N = `num_snapshots_retained`. The latest is always kept regardless of N (even if N = 0).
2. Any older snapshot still referenced by a live `ReadTx` (`Arc::strong_count > 1`).

All other snapshots are dropped.

### Auto GC

When `auto_snapshot_gc` is true, `commit_snapshot()` calls `self.gc()` after inserting the new snapshot. When false, the user calls `gc()` manually.

## Files to modify

- `src/store.rs` — add `StoreConfig`, store it in `Store`, update `gc()`, update `commit_snapshot()`
- `src/lib.rs` — re-export `StoreConfig`
- `tests/store_integration.rs` — update `Store::new()` calls, add retention tests
- `examples/*.rs` — update `Store::new()` calls
- `benches/store_bench.rs` — update `Store::new()` calls
- `CLAUDE.md` — document StoreConfig