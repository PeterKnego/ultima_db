// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! FROZEN conformance floor for the `smr-apply` autobench task.
//! The optimization loop must never edit this file. Pins the SMR contract:
//! version pinning, snapshot/checkpoint round-trips, read isolation, and
//! multi-writer commutative equivalence.
//! Frozen-paths enforcement lives in autobench/program.md (loop discipline).

mod common;

use std::io::Read;

use serde::{Deserialize, Serialize};
use ultima_db::{Persistence, Store, StoreConfig, WriterMode};

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
struct Counter {
    name: u64,
    value: i64,
}

fn smr_store(dir: &std::path::Path, mode: WriterMode) -> Store {
    let store = Store::new(
        StoreConfig::builder()
            .writer_mode(mode)
            .persistence(Persistence::smr(dir.to_path_buf()))
            .build(),
    )
    .unwrap();
    store.register_table::<Counter>("counters").unwrap();
    store.recover().unwrap();
    store
}

/// Apply one log entry at `log_index`: add `delta` to counter row `id`.
fn apply(store: &Store, log_index: u64, id: u64, delta: i64) {
    let mut tx = store.begin_write(Some(log_index)).unwrap();
    {
        let mut t = tx.open_table::<Counter>("counters").unwrap();
        let cur = t.get(id).cloned().expect("row preloaded");
        t.update(
            id,
            Counter {
                value: cur.value + delta,
                ..cur
            },
        )
        .unwrap();
    }
    tx.commit().unwrap();
}

fn preload(store: &Store, rows: u64) {
    let mut tx = store.begin_write(Some(1)).unwrap();
    {
        let mut t = tx.open_table::<Counter>("counters").unwrap();
        for i in 1..=rows {
            t.insert(Counter { name: i, value: 0 }).unwrap();
        }
    }
    tx.commit().unwrap();
}

#[test]
fn pinned_versions_track_log_index() {
    let dir = common::scratch_dir();
    let store = smr_store(dir.path(), WriterMode::SingleWriter);
    preload(&store, 16);
    for idx in 2..=500u64 {
        apply(&store, idx, idx % 16 + 1, 1);
        assert_eq!(
            store.latest_version(),
            idx,
            "smr: version != log index after apply"
        );
    }
}

#[test]
fn snapshot_stream_install_round_trip() {
    let dir = common::scratch_dir();
    let store = smr_store(dir.path(), WriterMode::SingleWriter);
    preload(&store, 100);
    for idx in 2..=200u64 {
        apply(&store, idx, idx % 100 + 1, idx as i64);
    }
    let mut buf = Vec::new();
    store
        .snapshot_stream(None)
        .unwrap()
        .read_to_end(&mut buf)
        .unwrap();

    let dir2 = common::scratch_dir();
    let store2 = smr_store(dir2.path(), WriterMode::SingleWriter);
    // ADJUSTED: `install_snapshot_stream` ignores `InstallOptions::commit_version`
    // in v1 — the installed snapshot always lands at the destination's
    // `base_version + 1` (src/snapshot_stream/install.rs:64-70, :279-282), so a
    // fresh store (version 0) installs at version 1, which does NOT equal the
    // source's pinned version. The frozen contract is row-content equality, not
    // version equality on a fresh install; we assert the documented v1 landing
    // version (1) and full row equality.
    let installed = store2
        .install_snapshot_stream(&buf[..], ultima_db::InstallOptions::default())
        .unwrap();
    assert_eq!(installed, 1, "smr: fresh install must land at base_version + 1");
    assert_eq!(
        store2.latest_version(),
        installed,
        "smr: latest_version must reflect the install return value"
    );
    let r1 = store.begin_read(None).unwrap();
    let r2 = store2.begin_read(None).unwrap();
    let t1 = r1.open_table::<Counter>("counters").unwrap();
    let t2 = r2.open_table::<Counter>("counters").unwrap();
    assert_eq!(t2.len(), t1.len(), "smr: row count differs after install");
    for id in 1..=100u64 {
        assert_eq!(t1.get(id), t2.get(id), "smr: row {id} differs after install");
    }
    drop(t1);
    drop(t2);
    drop(r1);
    drop(r2);
    let mut wtx = store2.begin_write(None).unwrap();
    let new_id = {
        let mut wt = wtx.open_table::<Counter>("counters").unwrap();
        wt.insert(Counter { name: 0, value: 0 }).unwrap()
    };
    wtx.commit().unwrap();
    assert!(
        new_id > 100,
        "smr: next_id after snapshot install collides with existing rows (got {new_id})"
    );
}

#[test]
fn checkpoint_recover_equality() {
    let dir = common::scratch_dir();
    let last;
    {
        let store = smr_store(dir.path(), WriterMode::SingleWriter);
        preload(&store, 50);
        for idx in 2..=300u64 {
            apply(&store, idx, idx % 50 + 1, 1);
        }
        last = store.latest_version();
        store.checkpoint().unwrap();
    }
    let store = smr_store(dir.path(), WriterMode::SingleWriter);
    assert_eq!(
        store.latest_version(),
        last,
        "smr: recover lost the checkpointed version"
    );
    let r = store.begin_read(None).unwrap();
    let t = r.open_table::<Counter>("counters").unwrap();
    let total: i64 = (1..=50u64).map(|id| t.get(id).unwrap().value).sum();
    assert_eq!(total, 299, "smr: counter sum wrong after recover");
    for id in 1..=50u64 {
        let expected = (2u64..=300).filter(|&idx| idx % 50 + 1 == id).count() as i64;
        assert_eq!(
            t.get(id).unwrap().value, expected,
            "smr: row {id} value wrong after checkpoint/recover"
        );
    }
}

/// Stress the SMR checkpoint path while a writer is actively applying log
/// entries, then verify recovery lands on a consistent committed *prefix* —
/// never a torn or half-written snapshot.
///
/// SMR mode has no WAL: durability is owned by the external consensus log, and
/// `recover()` restores only the latest *checkpoint*. So entries applied after
/// the winning checkpoint are *expected* to be absent after recovery — in a
/// real deployment the consensus log replays them. The invariant under test is
/// that whatever version `V` recovery lands on, the state equals exactly
/// "preload + apply entries 2..=V": every counter's value is the number of
/// applies that targeted it through `V`, and the total is `V - 1`. A torn
/// checkpoint (root captured mid-mutation) would violate this.
#[test]
fn checkpoint_concurrent_with_apply_recovers_consistent_prefix() {
    use std::sync::atomic::{AtomicBool, Ordering};

    const ROUNDS: usize = 6;
    const ROWS: u64 = 50;
    const LAST: u64 = 1000; // apply log indices 2..=LAST (version tracks the index)

    for round in 0..ROUNDS {
        let dir = common::scratch_dir();
        {
            let store = smr_store(dir.path(), WriterMode::SingleWriter);
            // preload commits at version 1 *before* the racer spawns, so every
            // checkpoint it takes captures at least version 1 — no empty-store
            // checkpoint can win the race.
            preload(&store, ROWS);

            let done = std::sync::Arc::new(AtomicBool::new(false));
            // SMR applies are pure in-memory (no WAL/fsync), so the writer would
            // otherwise blast through every entry before this thread is even
            // scheduled. The `started` gate makes the checkpointer provably loop
            // before any apply runs, so the apply window genuinely overlaps an
            // active checkpoint loop.
            let started = std::sync::Arc::new(AtomicBool::new(false));
            let ckpt_done = done.clone();
            let ckpt_started = started.clone();
            let ckpt_store = store.clone();
            let checkpointer = std::thread::spawn(move || {
                let mut count = 0usize;
                while !ckpt_done.load(Ordering::Acquire) {
                    // A checkpoint racing live applies must never serialize a
                    // torn snapshot. Transient errors are fine; corruption is not.
                    let _ = ckpt_store.checkpoint();
                    count += 1;
                    ckpt_started.store(true, Ordering::Release);
                }
                count
            });

            while !started.load(Ordering::Acquire) {
                std::hint::spin_loop();
            }

            for idx in 2..=LAST {
                apply(&store, idx, idx % ROWS + 1, 1);
            }

            done.store(true, Ordering::Release);
            let runs = checkpointer.join().unwrap();
            assert!(runs > 0, "round {round}: checkpoint loop never ran");
        }

        // Recover from whichever checkpoint won the race and assert the state is
        // an exact prefix of the apply sequence for the recovered version.
        let store = smr_store(dir.path(), WriterMode::SingleWriter);
        let v = store.latest_version();
        assert!(
            (1..=LAST).contains(&v),
            "round {round}: recovered version {v} outside applied range 1..={LAST}"
        );
        let r = store.begin_read(None).unwrap();
        let t = r.open_table::<Counter>("counters").unwrap();
        let total: i64 = (1..=ROWS).map(|id| t.get(id).unwrap().value).sum();
        assert_eq!(
            total,
            (v - 1) as i64,
            "round {round}: counter sum {total} != applied entries through version {v} (torn checkpoint)"
        );
        for id in 1..=ROWS {
            let expected = (2..=v).filter(|&idx| idx % ROWS + 1 == id).count() as i64;
            assert_eq!(
                t.get(id).unwrap().value,
                expected,
                "round {round}: row {id} value wrong for recovered prefix version {v}"
            );
        }
    }
}

#[test]
fn reads_are_isolated_during_apply() {
    let dir = common::scratch_dir();
    let store = smr_store(dir.path(), WriterMode::SingleWriter);
    preload(&store, 2);
    let stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let writer = {
        let store = store.clone();
        let stop = stop.clone();
        std::thread::spawn(move || -> u64 {
            let mut idx = 2u64;
            while !stop.load(std::sync::atomic::Ordering::Relaxed) {
                // one "entry" = +1 to both rows; readers must never see them differ
                let mut tx = store.begin_write(Some(idx)).unwrap();
                {
                    let mut t = tx.open_table::<Counter>("counters").unwrap();
                    for id in 1..=2u64 {
                        let cur = t.get(id).cloned().unwrap();
                        t.update(
                            id,
                            Counter {
                                value: cur.value + 1,
                                ..cur
                            },
                        )
                        .unwrap();
                    }
                }
                tx.commit().unwrap();
                idx += 1;
            }
            idx
        })
    };
    let t0 = std::time::Instant::now();
    while t0.elapsed().as_millis() < 500 {
        let r = store.begin_read(None).unwrap();
        let t = r.open_table::<Counter>("counters").unwrap();
        let a = t.get(1).unwrap().value;
        let b = t.get(2).unwrap().value;
        assert_eq!(a, b, "smr: snapshot read saw a half-applied entry");
    }
    stop.store(true, std::sync::atomic::Ordering::Relaxed);
    let final_idx = writer.join().unwrap();
    assert!(
        final_idx >= 50,
        "smr: isolation test made too few commits ({final_idx}); scheduler starvation?"
    );
}

#[test]
fn multiwriter_commutative_equivalence() {
    let dir = common::scratch_dir();
    let store = smr_store(dir.path(), WriterMode::MultiWriter);
    preload(&store, 8);
    const WRITERS: usize = 4;
    const TXNS_PER_WRITER: usize = 50;
    let handles: Vec<_> = (0..WRITERS)
        .map(|w| {
            let store = store.clone();
            std::thread::spawn(move || {
                for k in 0..TXNS_PER_WRITER {
                    let id = ((w * TXNS_PER_WRITER + k) % 8 + 1) as u64;
                    loop {
                        let mut tx = store.begin_write(None).unwrap();
                        // ADJUSTED: under MultiWriter the intent conflict surfaces
                        // *inside* the write methods (get/update) as an early-fail
                        // `WriteConflict { wait_for: Some(..) }` from `claim_intent`
                        // (src/store.rs:1658-1684), not only at commit. The loser
                        // must drop the tx and block on the holder's waiter before
                        // retrying — busy-looping just re-collides. Mirrors the
                        // bench retry loop (benches/multiwriter_scaling_bench.rs:233-251).
                        let write_res: Result<(), ultima_db::Error> = {
                            let mut t = tx.open_table::<Counter>("counters").unwrap();
                            match t.get(id).cloned() {
                                Some(cur) => t.update(
                                    id,
                                    Counter {
                                        value: cur.value + 1,
                                        ..cur
                                    },
                                ),
                                None => unreachable!("row {id} preloaded"),
                            }
                        };
                        match write_res {
                            Ok(()) => {}
                            Err(ultima_db::Error::WriteConflict { wait_for, .. }) => {
                                drop(tx);
                                if let Some(waiter) = wait_for {
                                    waiter.wait();
                                }
                                continue;
                            }
                            Err(e) => panic!("smr: unexpected write error: {e}"),
                        }
                        match tx.commit() {
                            Ok(_) => break,
                            Err(ultima_db::Error::WriteConflict { wait_for, .. }) => {
                                if let Some(waiter) = wait_for {
                                    waiter.wait();
                                }
                                continue;
                            }
                            Err(e) => panic!("smr: unexpected: {e}"),
                        }
                    }
                }
            })
        })
        .collect();
    for h in handles {
        h.join().unwrap();
    }
    let r = store.begin_read(None).unwrap();
    let t = r.open_table::<Counter>("counters").unwrap();
    let total: i64 = (1..=8u64).map(|id| t.get(id).unwrap().value).sum();
    assert_eq!(
        total,
        (WRITERS * TXNS_PER_WRITER) as i64,
        "smr: parallel increments lost"
    );
}
