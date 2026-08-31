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
    s.register_table::<Row>("rows").unwrap();
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
    let s = store_with(d.path(), PagedOptions::builder().checkpoint_dirty_bytes(1 << 20).build());
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
            t.get(&k);
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

/// `checkpointer_runs` advances even across a fully idle store (nothing
/// ever written) once the interval elapses, and — since `dirty_bytes` stays
/// `0` the whole time — must NOT actually invoke a paged checkpoint (no
/// `.root` file). This is `due_time`'s `&& dirty_bytes > 0` guard: without
/// it a freshly opened idle paged store would checkpoint forever for no
/// reason.
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
        "an idle store (dirty_bytes == 0 throughout) must never fire the interval trigger"
    );
    assert!(
        !std::fs::read_dir(d.path())
            .unwrap()
            .any(|e| e.unwrap().file_name().to_string_lossy().ends_with(".root")),
        "an idle store must never write a checkpoint"
    );
}
