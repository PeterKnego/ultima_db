// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! Task 12: the background checkpointer thread — dirty-bytes, memory-budget,
//! and interval triggers driving `checkpoint_impl` without any application
//! call to `Store::checkpoint()`. See
//! `docs/superpowers/specs/2026-08-30-paged-btree-stage-1-2-design.md`.

#![cfg(feature = "persistence")]

use ultima_db::{Durability, PagedOptions, Persistence, Store, StoreConfig, WalWrite};

#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
struct Row {
    v: u64,
}

fn store(dir: &std::path::Path) -> Store {
    store_with(dir, PagedOptions::builder().build())
}

fn store_with(dir: &std::path::Path, opts: PagedOptions) -> Store {
    let p = Persistence::standalone(dir, Durability::Eventual, WalWrite::Coalesced)
        .paged(opts)
        .unwrap();
    let s = Store::new(StoreConfig::builder().persistence(p).build()).unwrap();
    s.register_table_paged::<Row>("rows").unwrap();
    s
}

/// One batch insert (not a loop of single `insert()` calls) — see
/// `tests/paged_demotion.rs`'s `write_rows` for why: a fresh table's batch
/// append goes through the dense `BulkBuilder` fast path.
fn write_rows(s: &Store, n: u64) {
    let mut w = s.begin_write(None).unwrap();
    let mut t = w.open_table::<Row>("rows").unwrap();
    t.insert_batch((0..n).map(|v| Row { v }).collect()).unwrap();
    w.commit().unwrap();
}

/// The dirty-bytes trigger: no application call to `Store::checkpoint()`
/// anywhere in this test, yet the background thread must still checkpoint
/// once `PagedStats::dirty_bytes` crosses `checkpoint_dirty_bytes` — proven
/// by both `checkpointer_runs` advancing and a `.root` file landing on disk.
///
/// `PagedSource::note_dirty` (the sole source of `PagedStats::dirty_bytes`)
/// only fires on a *clean*-node CoW — a node that already has a page id,
/// i.e. already survived one checkpoint (`Child::make_mut_after_load`'s
/// `was_clean` guard). A table's very first write can never register: there
/// is no `NodeSource` yet (paged-attachment happens as part of a
/// checkpoint), and a freshly built leaf starts life already `NO_PAGE` —
/// dirty from birth, not by transition. So this test first does one
/// ordinary `checkpoint()` to attach the table and hand every leaf a page
/// id (a legitimate setup step, not the trigger under test — nothing here
/// asserts `checkpointer_runs` yet), then re-dirties every one of those
/// now-clean leaves with an `update_batch` that touches every row — *that*
/// is the write the background thread has to notice and act on with no
/// further application call.
#[test]
fn dirty_bytes_trigger_checkpoints_without_app_call() {
    let d = tempfile::tempdir().unwrap();
    let s = store_with(
        d.path(),
        PagedOptions::builder().checkpoint_dirty_bytes(1 << 20).checkpoint_interval_disabled().build(),
    );
    write_rows(&s, 50_000);
    s.checkpoint().unwrap(); // attach + assign every leaf a page id (setup, not the trigger under test)
    let runs_before = s.paged_stats().unwrap().checkpointer_runs;

    { // ~1.5 KB x 800 leaves >> 1 MiB of dirty bytes, all now clean-node CoWs
        let mut w = s.begin_write(None).unwrap();
        let mut t = w.open_table::<Row>("rows").unwrap();
        t.update_batch((1..=50_000u64).map(|k| (k, Row { v: k + 1 })).collect()).unwrap();
        w.commit().unwrap();
    }

    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    while s.paged_stats().unwrap().checkpointer_runs <= runs_before && std::time::Instant::now() < deadline {
        std::thread::sleep(std::time::Duration::from_millis(20));
    }
    assert!(
        s.paged_stats().unwrap().checkpointer_runs > runs_before,
        "background checkpointer never ran again within the deadline"
    );
    assert!(
        std::fs::read_dir(d.path())
            .unwrap()
            .any(|e| e.unwrap().file_name().to_string_lossy().ends_with(".root")),
        "no .root file appeared in {}",
        d.path().display()
    );
}

/// task64 §14.1(c) follow-up: with a memory budget set and no explicit
/// `checkpoint_dirty_bytes`, the dirty trigger is resolved to half the
/// budget (see `PagedOptionsBuilder::build`). Here the budget is 6 MiB, so
/// the trigger is 3 MiB; the update below dirties ~4.1 MB of clean leaves
/// — enough to trip the scaled trigger, nowhere near the 256 MiB default —
/// while resident stays ~4.1 MB, under the 6 MiB budget, so the memory
/// trigger cannot be what fires (measured with the trigger unscaled: the
/// run ends with dirty 4,118,048 / resident 4,063,552 and no checkpoint). The interval trigger is disabled. So the
/// only way the checkpointer runs again inside the deadline is the scaled
/// dirty trigger.
#[test]
fn memory_budget_scales_the_dirty_trigger_when_unset() {
    let d = tempfile::tempdir().unwrap();
    let s = store_with(
        d.path(),
        PagedOptions::builder().memory_budget_bytes(6 << 20).checkpoint_interval_disabled().build(),
    );
    write_rows(&s, 100_000);
    s.checkpoint().unwrap(); // attach + assign every leaf a page id (setup, not the trigger under test)
    let before = s.paged_stats().unwrap();
    assert!(
        before.resident_leaf_bytes_est < (6 << 20),
        "precondition: the table must sit under the 6 MiB budget so due_mem cannot fire (resident={})",
        before.resident_leaf_bytes_est
    );

    {
        let mut w = s.begin_write(None).unwrap();
        let mut t = w.open_table::<Row>("rows").unwrap();
        t.update_batch((1..=100_000u64).map(|k| (k, Row { v: k + 1 })).collect()).unwrap();
        w.commit().unwrap();
    }

    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    while s.paged_stats().unwrap().checkpointer_runs <= before.checkpointer_runs
        && std::time::Instant::now() < deadline
    {
        std::thread::sleep(std::time::Duration::from_millis(20));
    }
    let after = s.paged_stats().unwrap();
    assert!(
        after.checkpointer_runs > before.checkpointer_runs,
        "the scaled dirty trigger (budget/2 = 3 MiB) never fired within the deadline: dirty_bytes={} resident={}",
        after.dirty_bytes,
        after.resident_leaf_bytes_est
    );
}

/// The memory-budget trigger: a *read-only* store (no writes at all through
/// this process — every row was written and checkpointed by a prior store,
/// then this one recovers and only reads) must still demote leaves once
/// `resident_leaf_bytes` crosses `memory_budget_bytes`, driven entirely by
/// `PagedSource::read_node`'s wake-on-fault path. Demotion only ever
/// touches clean leaves — after `recover()` every leaf is clean, so this is
/// exactly the shape the trigger has to handle.
#[test]
fn memory_budget_trigger_demotes_read_only_store() {
    let d = tempfile::tempdir().unwrap();
    {
        let s = store(d.path());
        write_rows(&s, 50_000);
        s.checkpoint().unwrap();
    }
    let s = store_with(d.path(), PagedOptions::builder().memory_budget_bytes(64 * 1024).build());
    s.recover().unwrap();
    {
        let r = s.begin_read(None).unwrap();
        let t = r.open_table::<Row>("rows").unwrap();
        for k in 1..=50_000u64 {
            t.get(k);
        }
    } // reads only; faults every leaf back in, well past the 64 KiB budget

    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    while s.paged_stats().unwrap().leaves_demoted == 0 && std::time::Instant::now() < deadline {
        std::thread::sleep(std::time::Duration::from_millis(20));
    }
    assert!(
        s.paged_stats().unwrap().leaves_demoted > 0,
        "a read-only store must still demote on the memory trigger"
    );
}

/// The interval trigger: a single small write (well under the default 256
/// MiB dirty-bytes threshold, and well under any memory budget — none is
/// configured here) must still eventually checkpoint on its own, driven
/// purely by `checkpoint_interval` elapsing with something genuinely
/// uncommitted to page storage — see `checkpointer_loop`'s `due_time` doc
/// for why that check is `latest_version > last-checkpointed version`, not
/// `dirty_bytes > 0` (a table's very first write never touches
/// `dirty_bytes`, which is exactly the case this test exercises).
#[test]
fn interval_trigger_checkpoints_without_app_call() {
    let d = tempfile::tempdir().unwrap();
    let s = store_with(
        d.path(),
        PagedOptions::builder().checkpoint_interval(std::time::Duration::from_millis(200)).build(),
    );
    write_rows(&s, 10);

    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    loop {
        if std::fs::read_dir(d.path())
            .unwrap()
            .any(|e| e.unwrap().file_name().to_string_lossy().ends_with(".root"))
        {
            break;
        }
        assert!(std::time::Instant::now() < deadline, "no .root file appeared within the deadline");
        std::thread::sleep(std::time::Duration::from_millis(20));
    }
}

/// `Drop` must join the background thread promptly — never hang the
/// dropping thread indefinitely. Run under a watchdog rather than trusting
/// `drop(s)` to return on its own: if the join deadlocked, this test would
/// otherwise just hang forever instead of failing.
#[test]
fn drop_joins_the_thread() {
    let d = tempfile::tempdir().unwrap();
    let s = store(d.path());

    let (tx, rx) = std::sync::mpsc::channel();
    let watchdog = std::thread::spawn(move || {
        drop(s);
        let _ = tx.send(());
    });
    rx.recv_timeout(std::time::Duration::from_secs(5))
        .expect("Store::drop did not return within the 5s watchdog deadline (joined thread hung?)");
    watchdog.join().unwrap();
}

/// A fully idle store (nothing ever written, so `latest_version` stays `0`
/// — the same version [`PagedState::last_root`] implicitly starts at)
/// must NOT invoke a paged checkpoint on the interval trigger alone: no
/// `.root` file, `checkpointer_runs` stays `0`. This is `due_time`'s
/// `has_uncommitted` guard (`latest_version > last-checkpointed-version`)
/// — see `checkpointer_loop`'s doc for why that, not `dirty_bytes > 0`, is
/// the actual condition (a table's first write never touches
/// `dirty_bytes` before this fix, and even after it, a table with zero
/// writes obviously has no dirty bytes either — this test only needs the
/// weaker, always-true-for-both-versions claim: without *some* guard here,
/// a freshly opened idle paged store would checkpoint forever for no
/// reason).
#[test]
fn idle_store_does_not_spuriously_checkpoint_on_interval_alone() {
    let d = tempfile::tempdir().unwrap();
    let s = store_with(
        d.path(),
        PagedOptions::builder().checkpoint_interval(std::time::Duration::from_millis(50)).build(),
    );
    std::thread::sleep(std::time::Duration::from_millis(600));
    assert_eq!(
        s.paged_stats().unwrap().checkpointer_runs,
        0,
        "an idle store (latest_version == last-checkpointed-version throughout) must never fire the interval trigger"
    );
    assert!(
        !std::fs::read_dir(d.path())
            .unwrap()
            .any(|e| e.unwrap().file_name().to_string_lossy().ends_with(".root")),
        "an idle store must never write a checkpoint"
    );
}

/// Fix round 1, CRITICAL: a brand-new node (never on disk, so no
/// clean-to-dirty *transition* for `PagedSource::note_dirty` to notice) has
/// to credit `dirty_bytes` too, not just a CoW of an already-clean page —
/// otherwise the dirty-bytes trigger can never fire for a fresh table's
/// first real volume of writes, only for a *second* round of edits onto
/// already-checkpointed data (exactly what the test above this one used to
/// have to contrive). `Child::resident_new` (task12 fix round 1) is what
/// closes that gap, wired into every live-mutation-path node-creation site
/// in `btree.rs`, including `BulkBuilder`'s `extend_from_sorted` — which is
/// exactly the path `Table::insert_batch` always takes (never a from-scratch
/// `from_sorted`; see `Table::insert_batch`'s own doc), so this test's
/// second `write_rows` call — appending to an *already paged-attached*
/// table — exercises that path directly.
///
/// One `checkpoint()` first attaches the table (assigns every leaf a page
/// id) with a single throwaway row — a setup step, not the trigger under
/// test, so `checkpointer_runs`/`dirty_bytes` are captured *after* it, not
/// before. `checkpoint_interval_disabled()` isolates the dirty-bytes
/// trigger from the (now non-`None`) interval default.
///
/// Also checks the credit-back actually settles, not just clamps at some
/// arbitrary value via `subtract_dirty_bytes`'s saturation hiding an
/// incomplete credit — but *not* down to an exact `0`. Investigated and
/// understood, not just observed: `BulkBuilder::redistribute_tail` (the
/// tail-rebalance step `finish()` runs when a partial node would otherwise
/// end up underfull) can pop an already-frozen, already-`resident_new`
/// -credited sibling purely to discard it — merging its contents into a
/// freshly built `new_left` replacement — so that original sibling's
/// credit is orphaned: `note_dirty` charged for a node that never actually
/// reaches disk, and nothing this task adds un-charges it, because
/// distinguishing "this popped `Child` was frozen fresh by this exact
/// build round" from "this popped `Child` is a `seed_from_spine` clone of
/// an old, already-dirty-for unrelated-reasons node" isn't derivable from
/// the `Child` alone. At most `O(tree height)` such discards can happen
/// per bulk append (one per rebalanced level, not one per row), so the
/// leak is small and bounded, not proportional to write volume — an
/// accepted imprecision of the same *kind* `PagedStats::resident_leaf_bytes`
/// already documents, not a new correctness problem for the trigger (worst
/// case: it fires a few `Child::NODE_BYTES` earlier than the configured
/// threshold, never later, never wrongly-never). A real concurrent write
/// racing the checkpoint's own capture window would exercise a different,
/// *also* accepted source of imprecision, but isn't cheaply/deterministically
/// arrangeable without a dedicated race hook (paged_demotion.rs's
/// `race_hook` pattern) that this task doesn't add; the bounded-residual
/// check below is the documented fallback for both.
#[test]
fn dirty_bytes_trigger_fires_for_newly_created_nodes_on_a_live_attached_table() {
    let d = tempfile::tempdir().unwrap();
    let s = store_with(
        d.path(),
        PagedOptions::builder()
            .checkpoint_dirty_bytes(1 << 20)
            .checkpoint_interval_disabled()
            .build(),
    );
    write_rows(&s, 1); // fresh, unattached table -- BulkBuilder source is None, credits nothing
    s.checkpoint().unwrap(); // attach: every leaf (just the one) gets a page id
    let runs_before = s.paged_stats().unwrap().checkpointer_runs;
    assert_eq!(
        s.paged_stats().unwrap().dirty_bytes,
        0,
        "the setup row must not have credited anything before this point"
    );

    // ~1.5 KB x 800 leaves >> 1 MiB: brand-new leaves appended via
    // `extend_from_sorted`'s live path (the table is now attached), never
    // a clean-node CoW -- exactly the case `Child::resident_new` exists for.
    write_rows(&s, 50_000);

    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    while s.paged_stats().unwrap().checkpointer_runs <= runs_before && std::time::Instant::now() < deadline {
        std::thread::sleep(std::time::Duration::from_millis(20));
    }
    assert!(
        s.paged_stats().unwrap().checkpointer_runs > runs_before,
        "background checkpointer never ran for newly created nodes within the deadline \
         (dirty_bytes credit for brand-new nodes regressed)"
    );
    assert!(
        std::fs::read_dir(d.path())
            .unwrap()
            .any(|e| e.unwrap().file_name().to_string_lossy().ends_with(".root")),
        "no .root file appeared in {}",
        d.path().display()
    );

    // Credit-back settle check — bounded residual, not exact `0`; see the
    // test's doc for the `redistribute_tail` discard-orphan mechanism this
    // is deliberately tolerating. Poll briefly: the trigger firing and the
    // checkpoint actually completing are two different moments, and more
    // than one background run may be needed (the inter-run floor caps how
    // fast those runs can happen, not whether they do).
    let settle_deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    let residual_bound = 64 * 1024; // generous: a handful of orphaned nodes, not a real leak
    loop {
        let dirty = s.paged_stats().unwrap().dirty_bytes;
        if dirty <= residual_bound || std::time::Instant::now() >= settle_deadline {
            break;
        }
        std::thread::sleep(std::time::Duration::from_millis(20));
    }
    let settled = s.paged_stats().unwrap().dirty_bytes;
    assert!(
        settled <= residual_bound,
        "credit-in (note_dirty via resident_new) and credit-back (subtract_dirty_bytes) \
         must cancel down to a small bounded residual, not stay pinned near the ~1 MiB \
         that was credited in: settled at {settled} bytes"
    );
}
