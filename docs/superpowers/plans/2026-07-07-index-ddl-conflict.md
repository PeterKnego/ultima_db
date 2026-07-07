# MultiWriter Index-DDL Hard Error (task41) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A MultiWriter commit whose transaction defined an index on a table that a concurrent transaction committed to fails loudly with `Error::IndexDdlConflict` instead of silently dropping the index.

**Architecture:** `WriteTx` tracks DDL'd table names in a `RefCell<BTreeSet<String>>` (same interior-mutability pattern as the SSI read set); `TableWriter::define_index`/`define_custom_index` record the table name on success. Commit phase 1 of `commit_multi_writer` already computes a per-dirty-table "has concurrent commit since my base" flag under the read lock — one extra check errors out if any DDL'd table has that flag, before any merge or WAL submission.

**Tech Stack:** Rust, root crate `ultima_db` only. All changes in `src/error.rs` and `src/store.rs`.

**Spec:** `docs/superpowers/specs/2026-07-07-index-ddl-conflict-design.md`

## Global Constraints

- Branch: `fix/index-ddl-conflict` (already created).
- `cargo clippy --workspace --all-targets -- -D warnings` must pass at the end.
- Root `cargo test` covers the root crate; also run `cargo test --features persistence` (WAL paths compile against WriteTx).
- Error message is exactly: `index DDL on table '{table}' conflicts with a concurrent commit; retry the DDL in its own transaction when the table is quiet`.
- Fast path (no concurrent commit on the DDL'd table) and all of SingleWriter mode must be behaviorally unchanged.
- Failed DDL calls (e.g. kind mismatch) must NOT mark the table.

---

### Task 1: `Error::IndexDdlConflict` variant

**Files:**
- Modify: `src/error.rs` (after the `SerializationFailure` variant that follows `WriteConflict`)

**Interfaces:**
- Produces: `Error::IndexDdlConflict { table: String }` — public enum variant used by Tasks 2–3.

- [ ] **Step 1: Add the variant.** In `src/error.rs`, directly after the `SerializationFailure` variant's closing line, add:

```rust
    /// Returned when a MultiWriter commit contained index DDL
    /// (`define_index` / `define_custom_index`) on a table that a concurrent
    /// transaction committed to since this transaction's base snapshot. The
    /// merge slow path replays only write-set keys, so the new index
    /// definition cannot be carried over — the commit is refused instead of
    /// silently dropping the DDL (see task41).
    #[error(
        "index DDL on table '{table}' conflicts with a concurrent commit; \
         retry the DDL in its own transaction when the table is quiet"
    )]
    IndexDdlConflict { table: String },
```

- [ ] **Step 2: Add a Display test.** In the `tests` module of `src/error.rs` (existing Display tests live there, e.g. the `WriteConflict` one at ~line 116), add:

```rust
    #[test]
    fn index_ddl_conflict_display() {
        let e = Error::IndexDdlConflict {
            table: "users".to_string(),
        };
        assert_eq!(
            e.to_string(),
            "index DDL on table 'users' conflicts with a concurrent commit; \
             retry the DDL in its own transaction when the table is quiet"
        );
    }
```

- [ ] **Step 3: Run the test**

Run: `cargo test index_ddl_conflict_display`
Expected: PASS (pure additive variant; nothing else references it yet).

- [ ] **Step 4: Commit**

```bash
git add src/error.rs
git commit -m "feat(error): IndexDdlConflict variant for task41"
```

---

### Task 2: DDL tracking + commit-time check (the core fix)

**Files:**
- Modify: `src/store.rs` — `WriteTx` struct (~line 1652), `WriteTx` construction in `begin_write` (the block with `write_set: BTreeMap::new(),` ~line 531), `TableWriter` struct (~line 1725), `open_table` construction of `TableWriter` (~line 2439), `TableWriter::define_index` (~line 2029), `TableWriter::define_custom_index` (~line 2085), `commit_multi_writer` phase 1 (~line 2606–2638)
- Test: `src/store.rs` tests module

**Interfaces:**
- Consumes: `Error::IndexDdlConflict { table: String }` (Task 1).
- Produces: `WriteTx.ddl_tables: std::cell::RefCell<BTreeSet<String>>` (private field); `TableWriter.ddl_tables: Option<&'tx std::cell::RefCell<BTreeSet<String>>>` (private, `Some` only in MultiWriter). Task 3's tests rely on this exact behavior.

- [ ] **Step 1: Write the two failing tests** — in the `src/store.rs` tests module (near the existing MultiWriter tests, e.g. after `ssi_read_set_records_index_range_as_table_scan`):

```rust
    #[test]
    fn mw_index_ddl_with_concurrent_commit_errors() {
        let store = Store::new(StoreConfig {
            writer_mode: WriterMode::MultiWriter,
            ..StoreConfig::default()
        })
        .unwrap();
        // Seed the table.
        {
            let mut wtx = store.begin_write(None).unwrap();
            let mut t = wtx.open_table::<String>("t").unwrap();
            t.insert("seed".to_string()).unwrap();
            wtx.commit().unwrap();
        }

        let mut ddl_tx = store.begin_write(None).unwrap();
        {
            let mut t = ddl_tx.open_table::<String>("t").unwrap();
            t.define_index("by_val", IndexKind::Unique, |s: &String| s.clone())
                .unwrap();
            t.insert("mine".to_string()).unwrap();
        }

        // Concurrent writer commits a disjoint row to the same table first.
        {
            let mut other = store.begin_write(None).unwrap();
            let mut t = other.open_table::<String>("t").unwrap();
            t.insert("theirs".to_string()).unwrap();
            other.commit().unwrap();
        }

        let err = ddl_tx.commit().unwrap_err();
        assert!(
            matches!(err, Error::IndexDdlConflict { ref table } if table == "t"),
            "expected IndexDdlConflict, got {err:?}"
        );

        // No half-installed index on the latest version, and the concurrent
        // writer's data survived.
        let mut check = store.begin_write(None).unwrap();
        let t = check.open_table::<String>("t").unwrap();
        assert!(matches!(
            t.get_unique("by_val", &"seed".to_string()),
            Err(Error::IndexNotFound(_))
        ));
        assert_eq!(t.len(), 2, "seed + theirs; mine must not be installed");
    }

    #[test]
    fn mw_ddl_only_tx_with_concurrent_commit_errors() {
        // The recommended "DDL in its own transaction" pattern: no row
        // writes at all. Today this lands in the slow path's wholesale-
        // discard branch and the index silently vanishes.
        let store = Store::new(StoreConfig {
            writer_mode: WriterMode::MultiWriter,
            ..StoreConfig::default()
        })
        .unwrap();
        {
            let mut wtx = store.begin_write(None).unwrap();
            let mut t = wtx.open_table::<String>("t").unwrap();
            t.insert("seed".to_string()).unwrap();
            wtx.commit().unwrap();
        }

        let mut ddl_tx = store.begin_write(None).unwrap();
        {
            let mut t = ddl_tx.open_table::<String>("t").unwrap();
            t.define_index("by_val", IndexKind::Unique, |s: &String| s.clone())
                .unwrap();
            // no row writes
        }

        {
            let mut other = store.begin_write(None).unwrap();
            let mut t = other.open_table::<String>("t").unwrap();
            t.insert("theirs".to_string()).unwrap();
            other.commit().unwrap();
        }

        let err = ddl_tx.commit().unwrap_err();
        assert!(
            matches!(err, Error::IndexDdlConflict { ref table } if table == "t"),
            "expected IndexDdlConflict, got {err:?}"
        );
    }
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test mw_index_ddl_with_concurrent_commit_errors mw_ddl_only_tx`
(Actually run each by name: `cargo test --lib mw_index_ddl` and `cargo test --lib mw_ddl_only_tx`.)
Expected: FAIL — both commits currently succeed (`unwrap_err()` panics). This is the silent-drop bug demonstrated.

- [ ] **Step 3: Add the `ddl_tables` field to `WriteTx`.** In the `WriteTx` struct (~line 1652), after the `write_set` field:

```rust
    /// Tables that had index DDL (`define_index` / `define_custom_index`)
    /// in this transaction. The merge slow path cannot carry a new index
    /// definition over (only write-set keys are replayed), so commit fails
    /// with `IndexDdlConflict` if any of these tables saw a concurrent
    /// commit since our base (task41). `RefCell` for the same reason as
    /// `read_set`: recorded through a shared reference held by
    /// `TableWriter`; `WriteTx` is `!Send + !Sync`.
    ddl_tables: std::cell::RefCell<BTreeSet<String>>,
```

In the `WriteTx { ... }` construction inside `begin_write` (the block containing `write_set: BTreeMap::new(),` at ~line 531), add:

```rust
            ddl_tables: std::cell::RefCell::new(BTreeSet::new()),
```

- [ ] **Step 4: Add the reference to `TableWriter`.** In the `TableWriter` struct (~line 1725), after the `read_set` field:

```rust
    /// `Some` in MultiWriter mode: records tables with index DDL into the
    /// parent transaction so commit can refuse the un-mergeable DDL
    /// (task41). `None` in SingleWriter — no concurrent commits exist.
    ddl_tables: Option<&'tx std::cell::RefCell<BTreeSet<String>>>,
```

In `open_table`'s `Ok(TableWriter { ... })` construction (~line 2439), after `read_set: self.read_set.as_ref(),`:

```rust
            ddl_tables: match self.writer_mode {
                WriterMode::MultiWriter => Some(&self.ddl_tables),
                WriterMode::SingleWriter => None,
            },
```

- [ ] **Step 5: Record DDL on success.** Change `TableWriter::define_index` (~line 2029) from:

```rust
        self.metrics.register_index(&self.table_name, name);
        self.table.define_index(name, kind, extractor)
```

to:

```rust
        self.metrics.register_index(&self.table_name, name);
        self.table.define_index(name, kind, extractor)?;
        // Only a *successful* DDL taints the commit (task41) — a rejected
        // define (kind mismatch, name collision) changed nothing.
        if let Some(ddl) = self.ddl_tables {
            ddl.borrow_mut().insert(self.table_name.clone());
        }
        Ok(())
```

Apply the identical transformation to `define_custom_index` (~line 2085): `self.table.define_custom_index(name, index)?;` then the same `if let Some(ddl)` block, then `Ok(())`.

- [ ] **Step 6: Add the commit-time check.** In `commit_multi_writer` phase 1, inside the read-lock scope, directly after the `flags` map is computed (~line 2636, before the `(inner.snapshots[...].tables.clone(), flags)` tuple expression), add:

```rust
            // Index DDL cannot be carried through the merge slow path — the
            // new definition would be silently dropped (only write-set keys
            // are replayed onto the latest table). Fail loudly before any
            // merge or WAL submission instead (task41).
            let ddl = self.ddl_tables.borrow();
            if let Some(table) = ddl
                .iter()
                .find(|t| flags.get(*t).copied().unwrap_or(false))
            {
                let table = table.clone();
                drop(ddl);
                drop(inner);
                self.metrics.add_phase1(t1.elapsed().as_nanos() as u64);
                return Err(Error::IndexDdlConflict { table });
            }
            drop(ddl);
```

(Note: `ddl_tables` ⊆ `dirty` keys by construction — `open_table` inserts into `dirty` before any DDL is possible — so the `flags` map covers every DDL'd table.)

- [ ] **Step 7: Run the two tests to verify they pass**

Run: `cargo test --lib mw_index_ddl && cargo test --lib mw_ddl_only_tx`
Expected: PASS.

- [ ] **Step 8: Run the full store test suite** (regression check on commit paths):

Run: `cargo test --lib store:: && cargo test --features persistence 2>&1 | tail -5`
Expected: all pass.

- [ ] **Step 9: Commit**

```bash
git add src/store.rs
git commit -m "fix(store): refuse MultiWriter commits whose index DDL would be dropped

define_index/define_custom_index in a MultiWriter tx was silently lost
when the commit took the merge slow path (concurrent commit on the same
table) — including the documented 'DDL in its own transaction' pattern,
whose no-row-writes commit discarded the dirty table wholesale. WriteTx
now tracks DDL'd tables; phase 1 returns IndexDdlConflict before WAL
submission when any of them saw a concurrent commit since base."
```

---

### Task 3: Non-conflict behavior stays intact (guard tests)

**Files:**
- Test: `src/store.rs` tests module (after the two Task 2 tests)

**Interfaces:**
- Consumes: `Error::IndexDdlConflict`, the Task 2 behavior. These tests must pass WITHOUT further src changes; a failure means Task 2 over-blocks.

- [ ] **Step 1: Write the four guard tests**:

```rust
    #[test]
    fn mw_ddl_with_concurrent_commit_on_other_table_ok() {
        let store = Store::new(StoreConfig {
            writer_mode: WriterMode::MultiWriter,
            ..StoreConfig::default()
        })
        .unwrap();
        let mut ddl_tx = store.begin_write(None).unwrap();
        {
            let mut t = ddl_tx.open_table::<String>("a").unwrap();
            t.define_index("by_val", IndexKind::Unique, |s: &String| s.clone())
                .unwrap();
            t.insert("alpha".to_string()).unwrap();
        }
        // Concurrent commit on a DIFFERENT table.
        {
            let mut other = store.begin_write(None).unwrap();
            let mut t = other.open_table::<String>("b").unwrap();
            t.insert("beta".to_string()).unwrap();
            other.commit().unwrap();
        }
        ddl_tx.commit().unwrap();

        let mut check = store.begin_write(None).unwrap();
        let t = check.open_table::<String>("a").unwrap();
        let hit = t.get_unique("by_val", &"alpha".to_string()).unwrap();
        assert!(hit.is_some(), "index must be installed and queryable");
    }

    #[test]
    fn mw_ddl_fast_path_installs_index() {
        let store = Store::new(StoreConfig {
            writer_mode: WriterMode::MultiWriter,
            ..StoreConfig::default()
        })
        .unwrap();
        let mut ddl_tx = store.begin_write(None).unwrap();
        {
            let mut t = ddl_tx.open_table::<String>("t").unwrap();
            t.define_index("by_val", IndexKind::Unique, |s: &String| s.clone())
                .unwrap();
            t.insert("alpha".to_string()).unwrap();
        }
        ddl_tx.commit().unwrap();

        let mut check = store.begin_write(None).unwrap();
        let t = check.open_table::<String>("t").unwrap();
        let hit = t.get_unique("by_val", &"alpha".to_string()).unwrap();
        assert!(hit.is_some());
    }

    #[test]
    fn single_writer_ddl_unaffected() {
        let store = Store::new(StoreConfig::default()).unwrap();
        {
            let mut wtx = store.begin_write(None).unwrap();
            let mut t = wtx.open_table::<String>("t").unwrap();
            t.insert("alpha".to_string()).unwrap();
            wtx.commit().unwrap();
        }
        let mut wtx = store.begin_write(None).unwrap();
        {
            let mut t = wtx.open_table::<String>("t").unwrap();
            t.define_index("by_val", IndexKind::Unique, |s: &String| s.clone())
                .unwrap();
        }
        wtx.commit().unwrap();

        let mut check = store.begin_write(None).unwrap();
        let t = check.open_table::<String>("t").unwrap();
        let hit = t.get_unique("by_val", &"alpha".to_string()).unwrap();
        assert!(hit.is_some());
    }

    #[test]
    fn mw_failed_ddl_does_not_taint_commit() {
        let store = Store::new(StoreConfig {
            writer_mode: WriterMode::MultiWriter,
            ..StoreConfig::default()
        })
        .unwrap();
        // Commit an index so a later re-define with a different kind fails.
        {
            let mut wtx = store.begin_write(None).unwrap();
            let mut t = wtx.open_table::<String>("t").unwrap();
            t.define_index("by_val", IndexKind::Unique, |s: &String| s.clone())
                .unwrap();
            t.insert("seed".to_string()).unwrap();
            wtx.commit().unwrap();
        }

        let mut wtx = store.begin_write(None).unwrap();
        {
            let mut t = wtx.open_table::<String>("t").unwrap();
            // Kind mismatch → the DDL fails and must NOT mark the table.
            assert!(
                t.define_index("by_val", IndexKind::NonUnique, |s: &String| s.clone())
                    .is_err()
            );
            t.insert("mine".to_string()).unwrap();
        }
        // Concurrent commit on the same table forces the slow path.
        {
            let mut other = store.begin_write(None).unwrap();
            let mut t = other.open_table::<String>("t").unwrap();
            t.insert("theirs".to_string()).unwrap();
            other.commit().unwrap();
        }
        // Slow-path key merge must succeed — no IndexDdlConflict.
        wtx.commit().unwrap();
    }
```

- [ ] **Step 2: Run them**

Run: `cargo test --lib mw_ddl_with_concurrent_commit_on_other_table_ok mw_ddl_fast_path single_writer_ddl_unaffected mw_failed_ddl` (individually by name substring)
Expected: all PASS with no src changes. If any fails, Task 2's check is wrong — stop and fix before proceeding.

- [ ] **Step 3: Commit**

```bash
git add src/store.rs
git commit -m "test(store): guard tests for index-DDL fast path and non-conflict cases"
```

---

### Task 4: Docs + full verification

**Files:**
- Create: `docs/tasks/task41_index_ddl_conflict.md`
- Modify: `docs/tasks/task21_serializable_isolation.md` ("Index DDL is invisible…" bullet)
- Modify: `CLAUDE.md` (Code Conventions bullet on index DDL)

**Interfaces:**
- Consumes: everything above (documents it).

- [ ] **Step 1: Write the feature doc** — `docs/tasks/task41_index_ddl_conflict.md`:

```markdown
# Task 41: MultiWriter Index-DDL Hard Error

**Origin:** 2026-06 deep-review deferred backlog / task21 "v1 limitations".
`define_index` / `define_custom_index` inside a MultiWriter transaction was
silently dropped when the commit took the merge slow path: the merge clones
the *latest* table (which lacks the new index) and replays only write-set
keys. Worse, the slow path's no-row-writes branch discarded the dirty table
wholesale — so even the recommended "DDL in its own transaction" pattern
silently lost the index when a concurrent writer snuck in.

## What changed

- New `Error::IndexDdlConflict { table }`.
- `WriteTx` tracks DDL'd tables (`ddl_tables: RefCell<BTreeSet<String>>`,
  the SSI read-set pattern); `TableWriter::define_index` /
  `define_custom_index` record the table name **after** the underlying call
  succeeds (a rejected DDL doesn't taint the commit).
- Commit phase 1 (under the same read lock that computes the per-table
  fast/slow flags): any DDL'd table with a concurrent committed write since
  the tx base aborts the commit with `IndexDdlConflict` — before any merge
  or WAL submission, so nothing is half-installed.

## Semantics

- Fast path unchanged: DDL commits fine when no concurrent commit touched
  that table — including all of SingleWriter mode.
- The check covers all slow-path branches, including the concurrent
  table-delete branch: erroring there is a deliberate conservative false
  positive (delete+recreate semantics combined with DDL are murky).
- Re-defining an existing index (idempotent same-kind call) also marks the
  table — conservative, rare, and harmless to retry.
- `IndexDdlConflict` is deliberately distinct from `WriteConflict`:
  existing rebase-retry loops won't auto-retry it. Retry the DDL in its own
  transaction when the table has no concurrent writers.
- Still out of scope (task21 limitations): the DDL backfill read remains
  invisible to SSI, and DDL generates no conflicts for other writers. This
  fix removes only the silent loss.

Design history: `docs/superpowers/specs/2026-07-07-index-ddl-conflict-design.md`,
`docs/superpowers/plans/2026-07-07-index-ddl-conflict.md`.
```

- [ ] **Step 2: Update the task21 bullet.** In `docs/tasks/task21_serializable_isolation.md`, the "**Index DDL is invisible to SSI and OCC.**" bullet: replace its last two sentences ("…the new index definition is silently dropped — only write-set *keys* are replayed onto the latest table. Define indexes in their own transaction, with no concurrent writers on the table.") with:

```markdown
  Since task41 the drop is no longer silent: commit fails with
  `Error::IndexDdlConflict` when a DDL'd table saw a concurrent commit.
  The backfill read is still not SSI-tracked and DDL still generates no
  conflicts for other writers. Define indexes in their own transaction and
  retry on `IndexDdlConflict`.
```

- [ ] **Step 3: Update CLAUDE.md.** In the Code Conventions section, replace the bullet starting "Index DDL (`define_index`/`define_custom_index`) inside a MultiWriter transaction is not conflict-checked and is silently dropped…" with:

```markdown
- Index DDL (`define_index`/`define_custom_index`) inside a MultiWriter transaction is not carried through the merge slow path; since task41 the commit fails with `Error::IndexDdlConflict` (instead of silently dropping the index) when the DDL'd table saw a concurrent commit. Define indexes in their own transaction and retry on that error (see task41, task21 "v1 limitations").
```

- [ ] **Step 4: Full verification**

Run: `cargo test && cargo test --features persistence && cargo test -p ultima-vector && cargo clippy --workspace --all-targets -- -D warnings`
Expected: all pass, zero warnings.

- [ ] **Step 5: Commit**

```bash
git add docs/tasks/task41_index_ddl_conflict.md docs/tasks/task21_serializable_isolation.md CLAUDE.md
git commit -m "docs: task41 index-DDL conflict feature doc; update task21 + CLAUDE.md"
```
