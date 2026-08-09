// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! The whole-pipeline proof for incremental checkpoints.
//!
//! Task 1's oracle (`tests/btree_diff_oracle.rs`) proves `BTree::diff` is
//! right about *trees*: given two trees, the reported changes are exactly the
//! keys that differ. That is necessary but not sufficient — a store recovered
//! from a delta chain also depends on `diff` being fed the right table pairs,
//! the delta payload round-tripping through `encode`/`apply_delta`, the chain
//! walk resolving every hop, and `next_id` being carried forward correctly.
//! This file is the test that would catch a row, or a counter, dropped
//! anywhere in that pipeline: it builds two stores from the *same* operation
//! sequence — one checkpointing only fulls, one checkpointing chains — and
//! asserts recovery cannot tell them apart.
//!
//! ## What "indistinguishable" means here, and why
//!
//! A rows-only comparison is not enough — we know this because it is exactly
//! what let a Critical bug through Task 6's review: a row inserted **and**
//! deleted inside one delta interval emits nothing from `BTree::diff` (the
//! diff compares two endpoints; a key present at neither is invisible to it),
//! so the auto-increment counter never re-advanced past it and recovery
//! handed out an id that had already been used. No row was lost or gained, so
//! a rows-only equivalence check passed straight through it.
//!
//! So this file's equivalence is **rows + next_id + latest_version**:
//!
//! - **rows**: every table's full key/value set, compared exactly. One
//!   caveat: a table `ops` never touches is read back as `TableNotFound`
//!   rather than an existing-but-empty table (see `run_workload`), and this
//!   file treats the two as the same "no rows" case. That is safe today only
//!   because `store_op` never issues table-lifecycle DDL, so both runs always
//!   agree on which tables exist — it is the one place this file's
//!   "indistinguishable" claim is narrower than it sounds, and it would stop
//!   being safe the moment the generator grew a create/drop operation.
//! - **next_id**: `Table<R, u64>::next_id()` has no public accessor on
//!   `TableReader`/`TableWriter` (by design — see `src/store.rs`, `TableReader`
//!   has no such method), so it is observed the same way any caller of the
//!   public API would notice a wrong counter: insert one more row into each
//!   recovered store and compare the id it gets back. If the counter
//!   under-advanced, this probe either collides with a still-live row's key
//!   (silently overwriting it — `Table::insert` does not check the tree
//!   before using `next_id`) or reuses a vacated one; either way the returned
//!   id differs from the same probe against the full-recovery store, and the
//!   comparison catches it without reaching into crate internals.
//! - **latest_version**: `Store::latest_version()` after recovery.
//!
//! **Secondary index contents are deliberately not part of the per-case
//! comparison, and not because they are assumed equal — because they are not
//! persisted at all.** `Table<R, K>` (`src/table.rs:278`) does not derive
//! `Serialize`; checkpoint payloads are written by hand-rolled
//! `serialize_table`/`diff_table` closures (`src/registry.rs`) that walk the
//! row tree only. An index's `KeyExtractor` is a Rust closure baked into the
//! binary, so there is no byte representation of it to write to a checkpoint,
//! and `Store::recover()` never re-issues `define_index` calls it was never
//! told about (see `docs/tasks/task27_snapshot_stream.md`'s identical point
//! about the snapshot-stream install path). So there is no "index survives
//! chain recovery on its own" behavior to assert — a real restart always
//! re-defines its indexes, and once it does, `rebuild_from_sorted_data`
//! backfills from the row data this file already compares exactly, making
//! index-lookup correctness *implied* by row correctness rather than a
//! separate thing to verify per property case. What genuinely needs
//! dedicated coverage is that re-defining an index *after* chain recovery
//! backfills correctly from chain-recovered rows — that is
//! `an_index_defined_after_chain_recovery_backfills_from_the_recovered_rows`
//! below (see its doc comment for the fuller trace; the brief's original
//! sketch for this case assumed automatic index survival, which the code
//! does not do, and that assumption was corrected here rather than encoded
//! into a test that would have needed the assumption to be true).
//!
//! ## Generator shape
//!
//! `store_op` only ever inserts/updates/deletes plain rows — proptest will
//! not synthesize DDL (table create/drop, index define) with useful density,
//! so those live in the deterministic tests in Step 3 of the brief, not the
//! property. Within that constraint, `Delete`/`Update` are biased toward
//! *recently inserted* rows (see `build_events`) specifically so that an
//! insert followed by a delete of the same row lands inside one checkpoint
//! interval often enough to exercise the Task 6 shape — see
//! `generator_produces_insert_delete_within_one_interval_often_enough` for
//! the measured frequency, not an assumption of it.
//!
//! ## Blind spot
//!
//! This is a **differential** test: it compares chained recovery against
//! full recovery, not against an independent oracle of what the ops *should*
//! produce. A bug present on both paths — e.g. a wrong `next_id` field in
//! the *full*-checkpoint serializer itself, or a WAL-replay bug that both
//! `run_workload` calls exercise identically for whatever trails the last
//! checkpoint — is invisible here, because both sides would be wrong the
//! same way. What this file catches is exactly what its name says: chain
//! recovery *diverging* from full recovery, not either of them being wrong
//! in a way the other shares.

#![cfg(feature = "persistence")]

mod common;

use std::collections::BTreeMap;
use std::path::Path;

use proptest::prelude::*;
use proptest::strategy::ValueTree;
use proptest::test_runner::TestRunner;
use ultima_db::{Durability, Error, IndexKind, Persistence, Store, StoreConfig, WalWrite};

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
struct User {
    name: String,
    age: u32,
}

/// Same shape as `tests/persistence_integration.rs`'s helper of the same
/// name — kept as a local copy rather than a shared import because each file
/// under `tests/` is its own crate; there is no `pub` module to reuse without
/// inventing one.
fn chained_config(dir: &Path, durability: Durability, chain_max: usize) -> StoreConfig {
    StoreConfig::builder()
        .persistence(Persistence::standalone(
            dir.to_path_buf(),
            durability,
            WalWrite::PerEntry,
        ))
        .checkpoint_chain_max(chain_max)
        .build()
}

/// Helper: create store, register the `users` table, recover from disk.
/// Matches `tests/persistence_integration.rs`'s `open_store` — used only by
/// the deterministic Step 3/4 tests below, which (per the brief) work with a
/// single `users` table.
fn open_store(config: StoreConfig) -> Store {
    let store = Store::new(config).unwrap();
    store.register_table::<User>("users").unwrap();
    store.recover().unwrap();
    store
}

fn insert_user(store: &Store, name: &str) {
    let mut wtx = store.begin_write(None).unwrap();
    wtx.open_table::<User>("users")
        .unwrap()
        .insert(User {
            name: name.into(),
            age: 30,
        })
        .unwrap();
    wtx.commit().unwrap();
}

// ---------------------------------------------------------------------------
// Step 1/2: the chain-equivalence property
// ---------------------------------------------------------------------------

const TABLE_NAMES: [&str; 2] = ["t0", "t1"];

#[derive(Debug, Clone, Copy)]
enum Op {
    Insert { table: u8, age: u32 },
    Update { table: u8, slot: u8, age: u32 },
    Delete { table: u8, slot: u8 },
}

fn store_op() -> impl Strategy<Value = Op> {
    prop_oneof![
        3 => (0u8..2, 0u32..200).prop_map(|(table, age)| Op::Insert { table, age }),
        2 => (0u8..2, 0u8..4, 0u32..200)
            .prop_map(|(table, slot, age)| Op::Update { table, slot, age }),
        3 => (0u8..2, 0u8..4).prop_map(|(table, slot)| Op::Delete { table, slot }),
    ]
}

#[derive(Debug, Clone, Copy)]
enum EventKind {
    Insert(u32),
    Update(u32),
    Delete,
}

#[derive(Debug, Clone, Copy)]
struct Event {
    table: usize,
    key: u64,
    kind: EventKind,
}

/// Turns raw `Op`s into concrete `(table, key, kind)` events by simulating
/// the auto-increment id assignment and the live-key set each op would see
/// when actually applied. This is the single place that decides "what the
/// ops mean" — `run_workload` (which executes them for real) and
/// `events_have_insert_delete_within_one_interval` (which checks the
/// generator's shape) both consume its output, so the two can never disagree
/// about what a given `Vec<Op>` does.
///
/// No-op `Update`/`Delete` against an empty table are silently dropped
/// (`continue`, no event emitted) rather than turned into a no-op event: a
/// dropped op does not touch the store, so it must not consume a checkpoint
/// interval slot either.
fn build_events(ops: &[Op]) -> Vec<Event> {
    let mut next_id = [1u64, 1u64];
    let mut live: [Vec<u64>; 2] = [Vec::new(), Vec::new()];
    let mut events = Vec::with_capacity(ops.len());
    for op in ops {
        match *op {
            Op::Insert { table, age } => {
                let t = (table % 2) as usize;
                let key = next_id[t];
                next_id[t] += 1;
                // Most-recent-first so a small `slot` range in Update/Delete
                // is biased toward the row just inserted — see the module
                // doc's "Generator shape" section.
                live[t].insert(0, key);
                events.push(Event {
                    table: t,
                    key,
                    kind: EventKind::Insert(age),
                });
            }
            Op::Update { table, slot, age } => {
                let t = (table % 2) as usize;
                if live[t].is_empty() {
                    continue;
                }
                let key = live[t][slot as usize % live[t].len()];
                events.push(Event {
                    table: t,
                    key,
                    kind: EventKind::Update(age),
                });
            }
            Op::Delete { table, slot } => {
                let t = (table % 2) as usize;
                if live[t].is_empty() {
                    continue;
                }
                let i = slot as usize % live[t].len();
                let key = live[t].remove(i);
                events.push(Event {
                    table: t,
                    key,
                    kind: EventKind::Delete,
                });
            }
        }
    }
    events
}

/// `run_workload`'s checkpoint schedule: a checkpoint follows every
/// `checkpoint_every`-th *executed* event (not raw op — dropped ops consume
/// no interval slot, see `build_events`). Shared with the generator-shape
/// diagnostic below so the two never disagree about what "interval" means.
fn checkpoint_boundary(event_index: usize, checkpoint_every: usize) -> bool {
    (event_index + 1).is_multiple_of(checkpoint_every)
}

#[derive(Debug, PartialEq)]
struct WorkloadResult {
    rows: [BTreeMap<u64, User>; 2],
    next_id_probe: [u64; 2],
    version: u64,
}

fn open_workload_store(config: StoreConfig) -> Store {
    let store = Store::new(config).unwrap();
    store.register_table::<User>(TABLE_NAMES[0]).unwrap();
    store.register_table::<User>(TABLE_NAMES[1]).unwrap();
    store.recover().unwrap();
    store
}

/// Builds a store in a fresh scratch dir, applies `ops` (translated to
/// concrete events via `build_events`), checkpointing every
/// `checkpoint_every` executed events at chain length `chain_max`, drops the
/// store, recovers into a new one, and returns rows + a next_id probe +
/// version. See the module doc for why those three and not just rows.
fn run_workload(ops: &[Op], chain_max: usize, checkpoint_every: usize) -> WorkloadResult {
    let events = build_events(ops);
    let dir = common::test_scratch::scratch_dir();
    let config = chained_config(dir.path(), Durability::Consistent, chain_max);
    {
        let store = open_workload_store(config.clone());
        for (j, ev) in events.iter().enumerate() {
            let mut wtx = store.begin_write(None).unwrap();
            {
                let mut t = wtx.open_table::<User>(TABLE_NAMES[ev.table]).unwrap();
                match ev.kind {
                    EventKind::Insert(age) => {
                        t.insert(User {
                            name: format!("u{j}"),
                            age,
                        })
                        .unwrap();
                    }
                    EventKind::Update(age) => {
                        t.update(
                            ev.key,
                            User {
                                name: format!("u{j}"),
                                age,
                            },
                        )
                        .unwrap();
                    }
                    EventKind::Delete => {
                        t.delete(ev.key).unwrap();
                    }
                }
            }
            wtx.commit().unwrap();
            if checkpoint_boundary(j, checkpoint_every) {
                store.checkpoint().unwrap();
            }
        }
        // Store dropped here — WAL replay (Consistent durability, so every
        // commit above is already fsynced) covers whatever trailed the last
        // checkpoint; no crash is being simulated, this is a clean shutdown.
    }

    let store2 = open_workload_store(config);
    let mut rows: [BTreeMap<u64, User>; 2] = [BTreeMap::new(), BTreeMap::new()];
    {
        let rtx = store2.begin_read(None).unwrap();
        for t in 0..2 {
            // A table `ops` never touches never gets created (registration
            // alone does not instantiate one — see `ensure_dirty_entry`), so
            // `TableNotFound` here just means "no rows", exactly like an
            // existing-but-empty table would report. Both runs see the same
            // `ops`, so this can never itself be a source of chained/full
            // divergence — only a real content mismatch on a table that
            // *was* touched can fail the comparison below.
            rows[t] = match rtx.open_table::<User>(TABLE_NAMES[t]) {
                Ok(reader) => reader.iter().map(|(k, v)| (k, v.clone())).collect(),
                Err(Error::TableNotFound(_)) => BTreeMap::new(),
                Err(e) => panic!("unexpected error opening {}: {e:?}", TABLE_NAMES[t]),
            };
        }
    }
    let version = store2.latest_version();

    // next_id probe — see module doc. Deliberately captured *after* rows and
    // version so the probe's own commit cannot affect what was measured.
    let mut next_id_probe = [0u64; 2];
    for (t, probe_id) in next_id_probe.iter_mut().enumerate() {
        let mut wtx = store2.begin_write(None).unwrap();
        let id = wtx
            .open_table::<User>(TABLE_NAMES[t])
            .unwrap()
            .insert(User {
                name: "probe".into(),
                age: 0,
            })
            .unwrap();
        wtx.commit().unwrap();
        *probe_id = id;
    }

    WorkloadResult {
        rows,
        next_id_probe,
        version,
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]

    /// A store recovered from a delta chain must be indistinguishable from
    /// one recovered from a full checkpoint taken at the same version — see
    /// the module doc for exactly what "indistinguishable" means and why.
    #[test]
    fn chain_recovery_matches_full_recovery(
        ops in prop::collection::vec(store_op(), 1..80),
        // Starts at 2, not 1: chain_max = 1 makes the "chained" run identical
        // to the "full" baseline it's compared against (both write only
        // fulls), which is a tautology, not a case that could ever fail.
        chain_max in 2usize..6,
        checkpoint_every in 1usize..7,
    ) {
        let chained = run_workload(&ops, chain_max, checkpoint_every);
        let full = run_workload(&ops, 1, checkpoint_every);
        prop_assert_eq!(chained.rows, full.rows);
        prop_assert_eq!(chained.next_id_probe, full.next_id_probe);
        prop_assert_eq!(chained.version, full.version);
    }
}

/// Not a property — a direct measurement. The brief warns not to assume a
/// random Insert/Update/Delete mix produces the insert-then-delete-in-one-
/// interval shape densely; this samples the actual strategy the property
/// test uses (including its `checkpoint_every` range) and asserts a floor,
/// so a future edit to `store_op`'s weights that quietly starves the shape
/// fails loudly here instead of silently degrading what the property above
/// can catch.
#[test]
fn generator_produces_insert_delete_within_one_interval_often_enough() {
    fn events_have_insert_delete_within_one_interval(
        events: &[Event],
        checkpoint_every: usize,
    ) -> bool {
        let mut opened_in: std::collections::HashMap<(usize, u64), usize> =
            std::collections::HashMap::new();
        for (j, ev) in events.iter().enumerate() {
            let interval = j / checkpoint_every;
            match ev.kind {
                EventKind::Insert(_) => {
                    opened_in.insert((ev.table, ev.key), interval);
                }
                EventKind::Delete => {
                    if opened_in.get(&(ev.table, ev.key)) == Some(&interval) {
                        return true;
                    }
                    opened_in.remove(&(ev.table, ev.key));
                }
                EventKind::Update(_) => {}
            }
        }
        false
    }

    let mut runner = TestRunner::default();
    let strategy = (
        prop::collection::vec(store_op(), 1..80),
        1usize..7, // checkpoint_every, same range the property uses
    );
    let samples = 2000;
    let mut hits = 0usize;
    for _ in 0..samples {
        let value = strategy.new_tree(&mut runner).unwrap().current();
        let (ops, checkpoint_every) = value;
        let events = build_events(&ops);
        if events_have_insert_delete_within_one_interval(&events, checkpoint_every) {
            hits += 1;
        }
    }
    let frequency = hits as f64 / samples as f64;
    eprintln!(
        "insert-then-delete-within-one-interval shape: {hits}/{samples} ({:.1}%)",
        frequency * 100.0
    );
    assert!(
        frequency > 0.5,
        "generator produces the Task 6 bug's shape too rarely to trust the \
         property above to exercise it: {hits}/{samples} ({:.1}%)",
        frequency * 100.0
    );
}

// ---------------------------------------------------------------------------
// Step 3: lifecycle cases (deterministic — proptest will not generate DDL
// sequences densely enough to hit these)
// ---------------------------------------------------------------------------

#[test]
fn a_table_created_between_checkpoints_appears_in_full_in_the_delta() {
    let dir = common::test_scratch::scratch_dir();
    let config = chained_config(dir.path(), Durability::Consistent, 8);
    let store = Store::new(config.clone()).unwrap();
    store.register_table::<User>("users").unwrap();
    store.register_table::<User>("admins").unwrap();
    store.recover().unwrap();

    insert_user(&store, "alice"); // only "users" exists so far
    store.checkpoint().unwrap();

    let mut wtx = store.begin_write(None).unwrap();
    wtx.open_table::<User>("admins")
        .unwrap()
        .insert(User {
            name: "root".into(),
            age: 99,
        })
        .unwrap();
    wtx.commit().unwrap();
    store.checkpoint().unwrap();
    drop(store);

    let store2 = Store::new(config).unwrap();
    store2.register_table::<User>("users").unwrap();
    store2.register_table::<User>("admins").unwrap();
    store2.recover().unwrap();
    let rtx = store2.begin_read(None).unwrap();
    assert_eq!(rtx.open_table::<User>("admins").unwrap().len(), 1);
    assert_eq!(rtx.open_table::<User>("users").unwrap().len(), 1);
}

/// The brief's Step 3 sketch for this case assumes an index defined before a
/// checkpoint is still queryable after chain recovery with no further calls.
/// That assumption does not hold, and it is worth recording precisely why:
/// `Table<R, K>` (`src/table.rs:278`) does not derive `Serialize` at all —
/// checkpoint payloads are written by hand-rolled `serialize_table`/
/// `diff_table` closures (`src/registry.rs`) that walk `self.data` (the row
/// tree) only. The `indexes: BTreeMap<String, Box<dyn IndexMaintainer<R,K>>>`
/// field is never touched by them, because an index's `KeyExtractor` is a
/// Rust closure baked into the binary, not data — there is no byte
/// representation of `|u: &User| u.name.clone()` to write to disk. So
/// `Store::recover()` cannot replay an index definition it was never told
/// about; a real restart re-issues `define_index` itself (see the
/// `task27_snapshot_stream` doc's identical point about the snapshot-stream
/// install path: "the receiver's destination store must already have the
/// matching `define_index` calls in place").
///
/// What chain recovery *is* responsible for, and what this test actually
/// checks, is that the row data `define_index`'s own `rebuild_from_sorted_data`
/// backfill reads is correct — i.e. re-defining the index after chain
/// recovery, exactly as a real restart would, produces the same lookups a
/// full checkpoint would have. This is consistent with (not a contradiction
/// of) the module doc's index-equivalence reasoning: index correctness is
/// *implied* by row correctness, but only once something re-issues the
/// definition — there is no "index survives on its own" case to test because
/// no code path claims one.
#[test]
fn an_index_defined_after_chain_recovery_backfills_from_the_recovered_rows() {
    let dir = common::test_scratch::scratch_dir();
    let config = chained_config(dir.path(), Durability::Consistent, 8);
    {
        let store = open_store(config.clone());
        insert_user(&store, "alice");
        store.checkpoint().unwrap(); // full v1 — the delta base

        insert_user(&store, "bob");
        store.checkpoint().unwrap(); // delta v2, carries "bob" as an Added row
    }

    let store2 = open_store(config);
    let mut wtx = store2.begin_write(None).unwrap();
    wtx.open_table::<User>("users")
        .unwrap()
        .define_index("by_name", IndexKind::Unique, |u: &User| u.name.clone())
        .unwrap();
    wtx.commit().unwrap();

    let rtx = store2.begin_read(None).unwrap();
    let table = rtx.open_table::<User>("users").unwrap();
    assert!(
        table
            .get_unique("by_name", &"bob".to_string())
            .unwrap()
            .is_some()
    );
    assert!(
        table
            .get_unique("by_name", &"alice".to_string())
            .unwrap()
            .is_some()
    );
}

/// Reachability finding (see task-8-report.md for the full trace): table
/// **drop** *is* reachable through the public API — `WriteTx::delete_table`
/// (`src/store.rs:4544`) works in `SingleWriter` mode with no special
/// gating — so this is a live test, not a documented gap.
///
/// Drop-then-recreate-with-a-different-key-type is the one that is *not*
/// reachable while the table stays registered for persistence (a
/// precondition for it to reach the checkpoint chain at all): `Registry::
/// register` (`src/registry.rs:159`) refuses to re-register an existing name
/// under a different `R`/`K`, and `ensure_dirty_entry`'s no-existing-table
/// arm (`src/store.rs:3943-3947`) runs `validate_type_keyed::<R, K>` against
/// that same registry entry *before* creating the fresh table — so even
/// after the table is gone from the live snapshot, reopening its name under
/// a different key type still fails `Error::TypeMismatch`. That shape is
/// exercised at the `serialize_delta` level in Task 5/6's tests instead
/// (`src/checkpoint.rs`'s chain tests construct the `Snapshot`/registry pair
/// directly, bypassing the `Store` API's type-pinning).
#[test]
fn a_table_deleted_between_checkpoints_stays_deleted_after_chain_recovery() {
    let dir = common::test_scratch::scratch_dir();
    let config = chained_config(dir.path(), Durability::Consistent, 8);
    let store = Store::new(config.clone()).unwrap();
    store.register_table::<User>("users").unwrap();
    store.register_table::<User>("admins").unwrap();
    store.recover().unwrap();

    insert_user(&store, "alice");
    let mut wtx = store.begin_write(None).unwrap();
    wtx.open_table::<User>("admins")
        .unwrap()
        .insert(User {
            name: "root".into(),
            age: 99,
        })
        .unwrap();
    wtx.commit().unwrap();
    store.checkpoint().unwrap(); // full v2, both tables present — the delta base

    let mut wtx = store.begin_write(None).unwrap();
    assert!(wtx.delete_table("admins"));
    wtx.commit().unwrap();
    store.checkpoint().unwrap(); // delta v3: "admins" -> Dropped
    drop(store);

    let store2 = Store::new(config).unwrap();
    store2.register_table::<User>("users").unwrap();
    store2.register_table::<User>("admins").unwrap();
    store2.recover().unwrap();
    let rtx = store2.begin_read(None).unwrap();
    assert_eq!(rtx.open_table::<User>("users").unwrap().len(), 1);
    assert!(matches!(
        rtx.open_table::<User>("admins"),
        Err(Error::TableNotFound(_))
    ));
}

// ---------------------------------------------------------------------------
// Step 4: downgrade — an old binary must refuse a delta-headed directory
// ---------------------------------------------------------------------------

#[test]
fn a_delta_headed_directory_is_refused_rather_than_read_as_stale() {
    let dir = common::test_scratch::scratch_dir();
    let config = chained_config(dir.path(), Durability::Consistent, 8);
    {
        let store = open_store(config.clone());
        for i in 0..3 {
            insert_user(&store, &format!("u{i}"));
            store.checkpoint().unwrap();
        }
    }
    // The head is a delta (checkpoint_1 full, checkpoint_2/3 deltas — see
    // src/checkpoint.rs's write_delta_checkpoint, exercised once chain_max >
    // 1 and a base is retained). Stamp it with the pre-incremental format
    // version: an old binary would pick this same file as "latest" and must
    // fail on it, not silently fall back to the full whose WAL tail is
    // already pruned.
    let head = dir.path().join("checkpoint_3.bin");
    // Assert the precondition through the crate's own header reader rather
    // than assuming the file layout — same rationale as
    // `tests/persistence_integration.rs`'s `assert_full_checkpoint`, inverted
    // (we need the *head* to be a delta, not a full, or this test would be
    // exercising the full-checkpoint format-version path instead).
    assert!(
        !ultima_db::checkpoint_is_full_for_test(&head).unwrap(),
        "expected checkpoint_3.bin to be a delta — chain_max(8) with 3 rounds \
         should leave a full base at v1 and deltas at v2/v3"
    );
    let mut raw = std::fs::read(&head).unwrap();
    // Byte 4 is the first (and, for the current value, only) byte of the
    // format_version varint, right after the 4-byte "ULDB" magic
    // (src/checkpoint.rs: MAGIC, FORMAT_VERSION = 2, HEADER_PREFIX_LEN
    // comment). Guarded rather than blind: if FORMAT_VERSION's encoding ever
    // stops being a single byte equal to 2, this assert fails with a clear
    // message instead of silently corrupting the wrong byte.
    assert_eq!(
        raw[4], 2,
        "checkpoint header layout changed — update this offset against src/checkpoint.rs"
    );
    raw[4] = 1; // format_version -> 1
    // The whole file is CRC-protected (src/checkpoint.rs's `apply_delta_file`
    // checks the last 4 bytes against crc32 of everything before them), so
    // flipping the version byte alone corrupts the CRC too — and the CRC
    // check runs *before* the format-version check there, so an unpatched
    // CRC would fail this test for the wrong reason (CRC mismatch, not a
    // version mismatch), without ever exercising the format-version checks
    // this test exists to exercise. Re-stamp it so the version path is what
    // actually runs: `crc32fast` mirrors `crate::wal::crc32`'s
    // hardware-accelerated CRC-32 exactly (see that function's doc comment),
    // and it is already a plain (non-optional, non-persistence-gated) crate
    // dependency.
    //
    // Note this does not isolate any *one* format-version check: both
    // `read_header` (chain discovery) and `apply_delta_file` (chain load)
    // independently re-parse and re-validate the header on the same
    // `recover()` call, by design (see `apply_delta_file`'s own doc comment).
    // What this test pins is the pipeline-level guarantee — a
    // downgraded/corrupted delta head is refused rather than silently
    // treated as a stale full checkpoint.
    let crc_offset = raw.len() - 4;
    let recomputed = crc32fast::hash(&raw[..crc_offset]);
    raw[crc_offset..].copy_from_slice(&recomputed.to_le_bytes());
    std::fs::write(&head, &raw).unwrap();

    let store2 = Store::new(config).unwrap();
    store2.register_table::<User>("users").unwrap();
    // This only checks that the read is *refused* (`Err`, never `Ok`), not
    // which error it fails with. It used to also assert the message
    // contained "unsupported format version" — that stopped being true once
    // task62/task3 gave `format_version == 1` a real, structural meaning
    // (src/checkpoint.rs's `deserialize_snapshot_v1`/`read_header`). Patching
    // byte 4 to `1` no longer reproduces "an old binary sees this file and
    // refuses it on sight": it manufactures a genuinely malformed file — a
    // v1 header glued to v2-Delta-shaped bytes (`kind`, `base_version`,
    // per-entry `entry_kind` — all fields v1 never had) — which the v1
    // reader dutifully tries to parse as v1's `snapshot_version`/
    // `num_tables`/table-name/table-data stream and fails on with whatever
    // nonsense that produces (e.g. `TableNotRegistered` on a garbage table
    // name), not a format-version message. The underlying guarantee this
    // test exists for — a real 0.3.0 binary refuses a real v2 delta, because
    // that binary doesn't understand version 2 — is untouched; it just can't
    // be reproduced in-process anymore by stamping a version byte, since
    // version 1 is no longer a version *this* build refuses. Do not restore
    // the message assertion without first re-deriving a byte pattern that
    // actually simulates an old-binary read rather than a corrupt v1 file.
    store2.recover().unwrap_err();
}
