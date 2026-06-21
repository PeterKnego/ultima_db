# Inline-fsync WAL Productionization — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the env-gated inline-fsync spike with a real config knob (`Durability::ConsistentInline`) and move the inline fsync off the store lock so readers never block on the flush.

**Architecture:** Add an additive `Durability::ConsistentInline` variant (zero churn to the 36 `Standalone` literals). Task 1 wires it to the existing spike inline backend (lock-held) and removes the env var + validates SingleWriter. Task 2 refactors the inline backend to do `append+fsync` off-lock via a new `SyncWaiter::InlineSync`, reusing the async commit path's lock-drop/re-acquire. Task 3 adds a bench arm + the canonical task doc.

**Tech Stack:** Rust 2024, `std::sync` (Arc/Mutex/Condvar), the `persistence` cargo feature, criterion.

## Global Constraints

- All work on branch `feat/wal-inline-fsync-config`. `main` untouched until merge.
- Tests run with `--features persistence`; `cargo clippy --features persistence -- -D warnings` must pass with zero warnings.
- The default paths (`Durability::Consistent`, `Durability::Eventual`) must stay **byte-for-byte unchanged** in behavior.
- `Durability::ConsistentInline` has the **same durability guarantee** as `Consistent` (commit blocks until the entry is fsynced; no data loss on crash). It differs only in mechanism (committing thread fsyncs itself; no WAL bg thread).
- `ConsistentInline` is **SingleWriter only**: `Store::new` returns `Err(Error::Persistence("Durability::ConsistentInline requires WriterMode::SingleWriter"))` for `ConsistentInline + MultiWriter`.
- The spike's `ULTIMA_WAL_INLINE` env var is **removed** (no env-var gating anywhere).
- Off-lock: inline `write()` must do **no WAL I/O under `store_inner`**; the `append+fsync` happens in `SyncWaiter::wait()` (off-lock).
- The on-disk WAL format is unchanged; recovery (`scan_wal`/`read_wal`) is untouched.

---

### Task 1: `Durability::ConsistentInline` variant + validation + wiring (env var removed)

Adds the public variant, wires it to the **existing** spike inline backend (still lock-held at this point — Task 2 makes it off-lock), removes the env gate, and validates SingleWriter. After this task `ConsistentInline` is reachable and durable end-to-end.

**Files:**
- Modify: `src/persistence.rs` (the `Durability` enum, ~`src/persistence.rs:38-46`)
- Modify: `src/store.rs` (WAL construction `~337-372`, `wal_consistent` derivation `~376-383`)
- Test: `tests/wal_inline_fsync.rs` (create)

**Interfaces:**
- Produces: `Durability::ConsistentInline` (public enum variant).
- Consumes: existing `crate::wal::WalHandle::with_sink_kind_inline(dir, consistent, poison, kind)` (spike) and `with_sink_kind` (async).

- [ ] **Step 1: Write the failing test**

Create `tests/wal_inline_fsync.rs`:

```rust
#![cfg(feature = "persistence")]
use ultima_db::{Durability, Persistence, Store, StoreConfig, WalWrite, WriterMode};
use serde::{Deserialize, Serialize};

// `Record` is blanket-impl'd for any Serialize + DeserializeOwned type — do NOT
// add `impl Record for Row {}` (conflicting impl).
#[derive(Clone, Serialize, Deserialize, PartialEq, Debug)]
struct Row { v: u64 }

fn cfg(dir: &std::path::Path, wm: WriterMode) -> StoreConfig {
    StoreConfig {
        writer_mode: wm,
        persistence: Persistence::Standalone {
            dir: dir.to_path_buf(),
            durability: Durability::ConsistentInline,
            wal_write: WalWrite::PerEntry,
        },
        ..StoreConfig::default()
    }
}

#[test]
fn inline_singlewriter_commits_and_recovers() {
    let dir = tempfile::tempdir().unwrap();
    {
        let store = Store::new(cfg(dir.path(), WriterMode::SingleWriter)).unwrap();
        store.register_table::<Row>("rows").unwrap();
        for v in 1..=20u64 {
            let mut wtx = store.begin_write(None).unwrap();
            { let mut t = wtx.open_table::<Row>("rows").unwrap(); t.insert(Row { v }).unwrap(); }
            wtx.commit().unwrap();
        }
    } // drop = crash
    let store = Store::new(cfg(dir.path(), WriterMode::SingleWriter)).unwrap();
    store.register_table::<Row>("rows").unwrap();
    store.recover().unwrap();
    let rtx = store.begin_read(None).unwrap();
    let t = rtx.open_table::<Row>("rows").unwrap();
    for v in 1..=20u64 {
        assert_eq!(t.get(v).map(|r| r.v), Some(v), "acked inline commit {v} lost");
    }
}

#[test]
fn inline_multiwriter_is_rejected() {
    let dir = tempfile::tempdir().unwrap();
    let err = Store::new(cfg(dir.path(), WriterMode::MultiWriter));
    assert!(err.is_err(), "ConsistentInline + MultiWriter must error");
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `cargo test --features persistence --test wal_inline_fsync 2>&1 | tail -5`
Expected: FAIL to compile — `Durability::ConsistentInline` does not exist.

- [ ] **Step 3: Add the `ConsistentInline` variant**

In `src/persistence.rs`, extend `Durability`:

```rust
    /// Same guarantee as [`Consistent`](Durability::Consistent) (commit blocks
    /// until the entry is fsynced; no data loss on crash). Differs only in
    /// mechanism: the committing thread performs the fsync itself — no WAL
    /// background thread, no cross-thread handoff. **SingleWriter only**
    /// (`Store::new` errors otherwise). Best for serial durable commits on fast
    /// disk, where the bg-thread handoff (~20-35µs) dominates a cheap fsync.
    ConsistentInline,
```

- [ ] **Step 4: Wire validation + handle selection + audit the consistency derivations**

In `src/store.rs`, replace the WAL construction block (the `Persistence::Standalone` arm, ~`337-372`) with:

```rust
            crate::persistence::Persistence::Standalone { dir, durability, wal_write } => {
                use crate::persistence::Durability;
                // ConsistentInline appends inline in lock-acquisition order, which
                // only equals version order under a single writer. MultiWriter
                // relies on the bg-thread/epoch path for that ordering, so reject.
                if matches!(durability, Durability::ConsistentInline)
                    && matches!(config.writer_mode, WriterMode::MultiWriter)
                {
                    return Err(Error::Persistence(
                        "Durability::ConsistentInline requires WriterMode::SingleWriter".into(),
                    ));
                }
                let consistent = matches!(
                    durability,
                    Durability::Consistent | Durability::ConsistentInline
                );
                let kind = match wal_write {
                    crate::persistence::WalWrite::PerEntry => crate::wal::WalSinkKind::FsWrite,
                    crate::persistence::WalWrite::Coalesced => crate::wal::WalSinkKind::Coalesced,
                    crate::persistence::WalWrite::CoalescedPrealloc => {
                        crate::wal::WalSinkKind::CoalescedPrealloc
                    }
                };
                let inline = matches!(durability, Durability::ConsistentInline);
                let handle = if inline {
                    crate::wal::WalHandle::with_sink_kind_inline(
                        dir,
                        consistent,
                        Arc::clone(&wal_poison),
                        kind,
                    )?
                } else {
                    crate::wal::WalHandle::with_sink_kind(
                        dir,
                        consistent,
                        Arc::clone(&wal_poison),
                        kind,
                    )?
                };
                Some(handle)
            }
            _ => None,
```

Then update the `wal_consistent` derivation (`~376-383`) so the three-phase commit treats inline as consistent:

```rust
        #[cfg(feature = "persistence")]
        let wal_consistent = matches!(
            &config.persistence,
            crate::persistence::Persistence::Standalone {
                durability: crate::persistence::Durability::Consistent
                    | crate::persistence::Durability::ConsistentInline,
                ..
            }
        );
```

Confirm `Error` and `WriterMode` are already in scope in `store.rs` (they are — used elsewhere in the file).

- [ ] **Step 5: Run tests to verify they pass**

Run: `cargo test --features persistence --test wal_inline_fsync && cargo test --features persistence --lib`
Expected: both new tests PASS; full lib suite still green (default paths unchanged).

- [ ] **Step 6: Commit**

```bash
git add src/persistence.rs src/store.rs tests/wal_inline_fsync.rs
git commit -m "feat(wal): Durability::ConsistentInline config knob + SingleWriter validation (replaces env var)"
```

---

### Task 2: Off-lock inline fsync (`SyncWaiter::InlineSync`)

Refactor the inline backend so `write()` does **no I/O under the store lock** — it returns a waiter that performs `append+fsync` in the off-lock `wait()` phase. This removes the spike's lock-held-during-fsync (readers no longer block on the flush) while keeping the no-handoff win.

**Files:**
- Modify: `src/wal.rs` (`sync_sink` field, `with_sink_inline`, `SyncWaiter` enum + `wait()`, inline `write()` branch)
- Modify: `src/store.rs` (the two `needs_wal_wait` sites, `~2441-2443` and `~2698-2700`)
- Test: `src/wal.rs` tests module (update the spike's inline durability test for the off-lock waiter)

**Interfaces:**
- Consumes: `WalSink` trait, `WalDurability` (`publish`/`publish_error`), `WalPoison` (`check`/`poison`), `WalEntry`.
- Produces:
  - `WalHandle.sync_sink: Option<Arc<std::sync::Mutex<Box<dyn WalSink>>>>` (was `Option<Mutex<Box<dyn WalSink>>>`).
  - `SyncWaiter::InlineSync { sink: Arc<std::sync::Mutex<Box<dyn WalSink>>>, entry: WalEntry, durability: Arc<WalDurability>, poison: Arc<WalPoison> }`.
  - `write()` on an inline handle returns `SyncWaiter::InlineSync` (not `Done`); `wait()` makes it durable.

- [ ] **Step 1: Update the inline durability unit test for the off-lock waiter**

In `src/wal.rs` tests, replace the body of `inline_write_is_durable_and_recoverable` (added by the spike) with:

```rust
    #[test]
    fn inline_write_is_durable_and_recoverable() {
        // Off-lock inline: write() returns InlineSync (no I/O yet); wait() does
        // the append+fsync and makes it durable.
        let dir = tempfile::tempdir().unwrap();
        let poison = Arc::new(WalPoison::new());
        {
            let wal = WalHandle::with_sink_inline(
                FileSink::open(dir.path()).unwrap(),
                true,
                poison,
            );
            for v in 1..=5u64 {
                let w = wal
                    .write(WalEntry { version: v, ops: vec![WalOp::CreateTable { name: format!("t{v}") }] })
                    .unwrap();
                assert!(matches!(w, SyncWaiter::InlineSync { .. }), "inline write returns InlineSync");
                w.wait().unwrap(); // performs the append+fsync off-lock
            }
            assert_eq!(wal.durable_version(), 5);
        }
        let entries = read_wal(&dir.path().join(WAL_FILENAME)).unwrap();
        assert_eq!(entries.iter().map(|e| e.version).collect::<Vec<_>>(), vec![1, 2, 3, 4, 5]);
    }
```

- [ ] **Step 2: Run to verify it fails**

Run: `cargo test --features persistence --lib wal::tests::inline_write_is_durable_and_recoverable 2>&1 | tail -8`
Expected: FAIL — `SyncWaiter::InlineSync` does not exist (and current spike `write()` returns `Done`).

- [ ] **Step 3: Change `sync_sink` to `Arc<Mutex<…>>`**

In `src/wal.rs`, the `WalHandle` struct field:

```rust
    sync_sink: Option<Arc<std::sync::Mutex<Box<dyn WalSink>>>>,
```

In `with_sink_inline` (the spike constructor), wrap in `Arc`:

```rust
            sync_sink: Some(Arc::new(std::sync::Mutex::new(Box::new(sink)))),
```

(The async `with_sink` keeps `sync_sink: None`.)

- [ ] **Step 4: Add the `InlineSync` waiter variant + handle it in `wait()`**

Extend the `SyncWaiter` enum (`~src/wal.rs:497`):

```rust
    /// Inline-fsync (no bg thread): the committing thread performs the
    /// append+fsync in `wait()`, off the store lock. Durable on `Ok`.
    InlineSync {
        sink: Arc<std::sync::Mutex<Box<dyn WalSink>>>,
        entry: WalEntry,
        durability: Arc<WalDurability>,
        poison: Arc<WalPoison>,
    },
```

Replace `SyncWaiter::wait()` with a `match` that keeps the existing `WaitForEpoch` logic and adds `InlineSync`:

```rust
    pub fn wait(self) -> Result<()> {
        match self {
            SyncWaiter::Done => Ok(()),
            SyncWaiter::WaitForEpoch { epoch, state, poison } => {
                let mut guard = state.mu.lock().unwrap();
                loop {
                    if state.fsynced_epoch.load(std::sync::atomic::Ordering::Acquire) >= epoch {
                        return Ok(());
                    }
                    if poison.is_poisoned() {
                        return Err(poison.error());
                    }
                    guard = state.condvar.wait(guard).unwrap();
                }
            }
            SyncWaiter::InlineSync { sink, entry, durability, poison } => {
                poison.check()?;
                let version = entry.version;
                let mut s = sink.lock().unwrap();
                let res: Result<()> = (|| {
                    s.append(&entry)?;
                    s.sync()
                })();
                match res {
                    Ok(()) => {
                        durability.publish(version);
                        Ok(())
                    }
                    Err(e) => {
                        let msg = format!("WAL durability failure: {e}");
                        poison.poison(msg.clone());
                        durability.publish_error(version, msg);
                        Err(e)
                    }
                }
            }
        }
    }
```

- [ ] **Step 5: Make inline `write()` stage-only (no I/O under lock)**

Replace the spike's inline branch in `write()` (the `if let Some(sink) = &self.sync_sink { … append/sync/Done … }` block) with:

```rust
        // Inline (no bg thread): do NO I/O here (this runs under store_inner).
        // Return a waiter; the committer does append+fsync off-lock in wait().
        if let Some(sink) = &self.sync_sink {
            self.poison.check()?;
            return Ok(SyncWaiter::InlineSync {
                sink: Arc::clone(sink),
                entry,
                durability: Arc::clone(&self.durability),
                poison: Arc::clone(&self.poison),
            });
        }
```

(The inline `request_prune` branch is unchanged — `sink.lock().unwrap().prune(..)` works identically through the `Arc`.)

- [ ] **Step 6: Make `commit()` wait off-lock for `InlineSync`**

In `src/store.rs`, both `needs_wal_wait` sites (`~2443` and `~2700`) — extend the match so inline forces the off-lock wait:

```rust
            let mut w = matches!(
                &waiter,
                Some(crate::wal::SyncWaiter::WaitForEpoch { .. })
                    | Some(crate::wal::SyncWaiter::InlineSync { .. })
            );
```

- [ ] **Step 7: Run tests + clippy**

Run: `cargo test --features persistence --lib wal::tests::inline_write_is_durable_and_recoverable && cargo test --features persistence --test wal_inline_fsync && cargo test --features persistence --lib && cargo clippy --features persistence -- -D warnings`
Expected: all PASS (off-lock waiter test, the Task 1 integration tests still green via the new path, full lib suite, clippy clean).

- [ ] **Step 8: Commit**

```bash
git add src/wal.rs src/store.rs
git commit -m "feat(wal): off-lock inline fsync via SyncWaiter::InlineSync (no reader-blocking)"
```

---

### Task 3: Bench arm + canonical task doc

**Files:**
- Modify: `benches/singlewriter_persistence_bench.rs` (add a `standalone_consistent_inline` arm)
- Create: `docs/tasks/task38_wal_inline_fsync.md`

**Interfaces:**
- Consumes: `Durability::ConsistentInline`.

- [ ] **Step 1: Add the bench arm**

Read the existing `standalone_consistent` arm in `benches/singlewriter_persistence_bench.rs` (~line 155-170). Add a sibling arm built with `durability: ultima_db::Durability::ConsistentInline` and the same `WalWrite::PerEntry`, identical `run_serial_commits` workload, criterion id `standalone_consistent_inline`. Reuse `make_store` and `run_serial_commits` — do not duplicate the workload body. The bench already uses `WriterMode::SingleWriter`, so the variant is valid.

```rust
    // Inline-fsync (off-lock, no bg-thread handoff) — A/B vs standalone_consistent.
    {
        let tmpdir = tempfile::tempdir().unwrap();
        let store = make_store(
            Persistence::Standalone {
                dir: std::path::PathBuf::new(),
                durability: ultima_db::Durability::ConsistentInline,
                wal_write: WalWrite::PerEntry,
            },
            Some(tmpdir.path()),
        );
        group.bench_function("standalone_consistent_inline", |b| {
            b.iter(|| { run_serial_commits(&store); black_box(()); });
        });
    }
```

(Match the exact closure shape of the neighbouring arms; import `black_box` if not already.)

- [ ] **Step 2: Verify the bench compiles**

Run: `cargo bench --bench singlewriter_persistence_bench --features persistence --no-run`
Expected: compiles.

- [ ] **Step 3: Write the canonical task doc**

Create `docs/tasks/task38_wal_inline_fsync.md` covering: the problem (cross-thread handoff ~20-35µs/commit, dominant on fast disk after preallocation), the `Durability::ConsistentInline` knob (additive variant, SingleWriter-only, same guarantee as Consistent), the off-lock mechanism (`SyncWaiter::InlineSync`, append+fsync in `wait()`, reusing the async lock-drop/re-acquire), the MultiWriter rejection rationale, recovery unchanged, and the measured/pending validation (tmpfs handoff ~32µs → ~3µs; projected NVMe ~72µs→~38µs ~1.9×; bench-host A/B still pending). Cross-reference `docs/superpowers/specs/2026-06-21-wal-inline-fsync-config-design.md`, `docs/tasks/task37_wal_preallocation.md`, and `../ultima_cluster/docs/wal-journal-handoff-tax-2026-06-21.md`.

- [ ] **Step 4: Full verification sweep**

Run: `cargo test --features persistence && cargo clippy --features persistence --benches -- -D warnings`
Expected: all tests pass, zero clippy warnings.

- [ ] **Step 5: Commit**

```bash
git add benches/singlewriter_persistence_bench.rs docs/tasks/task38_wal_inline_fsync.md
git commit -m "bench+docs(wal): ConsistentInline bench arm + task38 doc"
```

---

## Self-Review

**Spec coverage:**
- §3 config variant `ConsistentInline` → Task 1 Step 3. ✓
- §4 validation (MultiWriter error) → Task 1 Step 4 + `inline_multiwriter_is_rejected`. ✓
- §4 `matches!(Consistent)` audit (consistent + wal_consistent) → Task 1 Step 4. ✓
- §5 off-lock mechanism (`sync_sink` Arc, `InlineSync`, write() stage-only, needs_wal_wait, wait() append+fsync) → Task 2. ✓
- §5 env var removed → Task 1 Step 4 (gate replaced by the variant match; no `ULTIMA_WAL_INLINE` remains). ✓
- §6 recovery unchanged / prune inline → no change needed (recovery untouched; prune branch carried through Task 2 Step 5 note). ✓
- §7 testing (durability+recovery, validation, off-lock waiter, default unchanged, bench) → Tasks 1-3. ✓
- §9 needs_wal_wait audit (both sites) → Task 2 Step 6. ✓

**Placeholder scan:** No TBD/TODO. Task 3 Step 1/Step 3 instruct mirroring an existing arm / writing prose doc with the exact content enumerated — both name the file/anchor and the required content, not vague placeholders.

**Type consistency:** `sync_sink: Option<Arc<Mutex<Box<dyn WalSink>>>>` used identically in Task 2 (field, with_sink_inline, InlineSync.sink, write()). `SyncWaiter::InlineSync { sink, entry, durability, poison }` field names consistent across the enum def, `wait()`, and `write()`. `Durability::ConsistentInline` consistent across persistence.rs, store.rs (consistent/wal_consistent/inline gate), tests, bench. `WriterMode`/`Error::Persistence` confirmed in scope.
