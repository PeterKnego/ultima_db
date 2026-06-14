// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! FROZEN conformance floor for the `multiwriter-commit` autobench task;
//! the optimization loop must never edit this file. Pins the MultiWriter OCC
//! contract: no lost updates under key-level conflict (merge slow path), all
//! disjoint writes land (fast path drops nothing), secondary indexes stay
//! consistent across concurrent merges, high-contention retry terminates
//! without deadlock/livelock, and concurrent readers keep snapshot isolation.
//! Frozen-paths enforcement lives in the task program.md (loop discipline).

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use serde::{Deserialize, Serialize};
use ultima_db::{IndexKind, Persistence, Store, StoreConfig, WriterMode};

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
struct Row {
    key: u64,
    val: u64,
}

fn mw_store() -> Store {
    Store::new(StoreConfig {
        writer_mode: WriterMode::MultiWriter,
        persistence: Persistence::None,
        ..StoreConfig::default()
    })
    .unwrap()
}

fn preload(store: &Store, rows: u64) {
    let mut tx = store.begin_write(None).unwrap();
    {
        let mut t = tx.open_table::<Row>("state").unwrap();
        for i in 1..=rows {
            t.insert(Row { key: i, val: 0 }).unwrap();
        }
    }
    tx.commit().unwrap();
}

/// Run `body` to build a write set and commit it, retrying on conflict at
/// BOTH the write-site and the commit-site (drop tx + block on waiter).
/// `body` receives the open table and returns a write result.
fn commit_retry(
    store: &Store,
    mut body: impl FnMut(&mut ultima_db::TableWriter<'_, Row>) -> Result<(), ultima_db::Error>,
) {
    loop {
        let mut tx = store.begin_write(None).unwrap();
        let write_res = {
            let mut t = tx.open_table::<Row>("state").unwrap();
            body(&mut t)
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
            Err(e) => panic!("mw-torture: write error: {e}"),
        }
        match tx.commit() {
            Ok(_) => return,
            Err(ultima_db::Error::WriteConflict { wait_for, .. }) => {
                if let Some(waiter) = wait_for {
                    waiter.wait();
                }
                continue;
            }
            Err(e) => panic!("mw-torture: commit error: {e}"),
        }
    }
}

#[test]
fn no_lost_updates_under_conflict() {
    let store = mw_store();
    const KEYS: u64 = 3;
    const WRITERS: u64 = 4;
    const N: u64 = 60;
    preload(&store, KEYS);

    let handles: Vec<_> = (0..WRITERS)
        .map(|w| {
            let store = store.clone();
            std::thread::spawn(move || {
                for k in 0..N {
                    let id = (w * N + k) % KEYS + 1;
                    commit_retry(&store, |t| {
                        let cur = t.get(id).cloned().unwrap();
                        t.update(id, Row { val: cur.val + 1, ..cur })
                    });
                }
            })
        })
        .collect();
    for h in handles {
        h.join().unwrap();
    }
    let r = store.begin_read(None).unwrap();
    let t = r.open_table::<Row>("state").unwrap();
    let total: u64 = (1..=KEYS).map(|id| t.get(id).unwrap().val).sum();
    assert_eq!(total, WRITERS * N, "mw: lost update via merge slow path");
}

#[test]
fn disjoint_writers_all_land() {
    let store = mw_store();
    const WRITERS: u64 = 4;
    const RANGE: u64 = 500;
    let rows = WRITERS * RANGE;
    preload(&store, rows);

    let handles: Vec<_> = (0..WRITERS)
        .map(|w| {
            let store = store.clone();
            std::thread::spawn(move || {
                let lo = w * RANGE + 1;
                for off in 0..RANGE {
                    let id = lo + off;
                    commit_retry(&store, |t| t.update(id, Row { key: id, val: id * 7 + 1 }));
                }
            })
        })
        .collect();
    for h in handles {
        h.join().unwrap();
    }
    let r = store.begin_read(None).unwrap();
    let t = r.open_table::<Row>("state").unwrap();
    for id in 1..=rows {
        assert_eq!(
            t.get(id).unwrap().val,
            id * 7 + 1,
            "mw: disjoint write to row {id} was dropped (fast path lost an edit)"
        );
    }
}

#[test]
fn index_consistency_after_concurrent_merge() {
    let store = mw_store();
    const KEYS: u64 = 16;
    const WRITERS: u64 = 4;
    const N: u64 = 80;
    // Preload + define the NonUnique index in its own no-concurrency txns.
    preload(&store, KEYS);
    {
        let mut tx = store.begin_write(None).unwrap();
        {
            let mut t = tx.open_table::<Row>("state").unwrap();
            t.define_index("val_bucket", IndexKind::NonUnique, |r: &Row| r.val % 8)
                .unwrap();
        }
        tx.commit().unwrap();
    }

    let handles: Vec<_> = (0..WRITERS)
        .map(|w| {
            let store = store.clone();
            std::thread::spawn(move || {
                for k in 0..N {
                    let id = (w * N + k) % KEYS + 1;
                    commit_retry(&store, |t| {
                        let cur = t.get(id).cloned().unwrap();
                        t.update(id, Row { val: cur.val + 1, ..cur })
                    });
                }
            })
        })
        .collect();
    for h in handles {
        h.join().unwrap();
    }

    // The index must agree with a full scan: for every bucket, the set of ids
    // the index returns equals the set of ids whose val falls in that bucket.
    let r = store.begin_read(None).unwrap();
    let t = r.open_table::<Row>("state").unwrap();
    for bucket in 0..8u64 {
        let mut from_index: Vec<u64> = t
            .get_by_index::<u64>("val_bucket", &bucket)
            .unwrap()
            .into_iter()
            .map(|(id, _)| id)
            .collect();
        let mut from_scan: Vec<u64> = t
            .iter()
            .filter(|(_, row)| row.val % 8 == bucket)
            .map(|(id, _)| id)
            .collect();
        from_index.sort_unstable();
        from_scan.sort_unstable();
        assert_eq!(
            from_index, from_scan,
            "mw: index bucket {bucket} disagrees with scan after concurrent merge"
        );
    }
}

#[test]
fn retry_eventually_succeeds_no_deadlock() {
    let store = mw_store();
    const KEYS: u64 = 2;
    const WRITERS: u64 = 4;
    const N: u64 = 100;
    preload(&store, KEYS);

    let handles: Vec<_> = (0..WRITERS)
        .map(|w| {
            let store = store.clone();
            std::thread::spawn(move || {
                for k in 0..N {
                    let id = (w + k) % KEYS + 1;
                    commit_retry(&store, |t| {
                        let cur = t.get(id).cloned().unwrap();
                        t.update(id, Row { val: cur.val + 1, ..cur })
                    });
                }
            })
        })
        .collect();
    for h in handles {
        h.join().unwrap(); // termination == no deadlock/livelock
    }
    let r = store.begin_read(None).unwrap();
    let t = r.open_table::<Row>("state").unwrap();
    let total: u64 = (1..=KEYS).map(|id| t.get(id).unwrap().val).sum();
    assert_eq!(
        total,
        WRITERS * N,
        "mw: high-contention run lost commits (expected all to land)"
    );
}

#[test]
fn mixed_disjoint_and_overlap() {
    let store = mw_store();
    // Rows 1 and 2 are kept EQUAL by every writer (overlapping); rows 3.. are
    // each writer's private disjoint lane. A concurrent reader must never see
    // rows 1 and 2 differ (snapshot isolation across the two-row update).
    const WRITERS: u64 = 4;
    const LANE: u64 = 200;
    let rows = 2 + WRITERS * LANE;
    preload(&store, rows);

    let stop = Arc::new(AtomicBool::new(false));
    let reader = {
        let store = store.clone();
        let stop = stop.clone();
        std::thread::spawn(move || {
            while !stop.load(Ordering::Relaxed) {
                let r = store.begin_read(None).unwrap();
                let t = r.open_table::<Row>("state").unwrap();
                let a = t.get(1).unwrap().val;
                let b = t.get(2).unwrap().val;
                assert_eq!(a, b, "mw: reader saw a half-applied overlapping commit");
            }
        })
    };

    let handles: Vec<_> = (0..WRITERS)
        .map(|w| {
            let store = store.clone();
            std::thread::spawn(move || {
                let lane_lo = 2 + w * LANE + 1;
                for off in 0..LANE {
                    // Overlapping commit: bump rows 1 and 2 together (kept equal).
                    commit_retry(&store, |t| {
                        let c1 = t.get(1).cloned().unwrap();
                        t.update(1, Row { val: c1.val + 1, ..c1 })?;
                        let c2 = t.get(2).cloned().unwrap();
                        t.update(2, Row { val: c2.val + 1, ..c2 })
                    });
                    // Disjoint commit in this writer's private lane.
                    let id = lane_lo + off;
                    commit_retry(&store, |t| t.update(id, Row { key: id, val: id }));
                }
            })
        })
        .collect();
    for h in handles {
        h.join().unwrap();
    }
    stop.store(true, Ordering::Relaxed);
    reader.join().unwrap();

    let r = store.begin_read(None).unwrap();
    let t = r.open_table::<Row>("state").unwrap();
    assert_eq!(t.get(1).unwrap().val, t.get(2).unwrap().val);
    assert_eq!(
        t.get(1).unwrap().val,
        WRITERS * LANE,
        "mw: overlapping increments lost"
    );
    for w in 0..WRITERS {
        let lane_lo = 2 + w * LANE + 1;
        for off in 0..LANE {
            let id = lane_lo + off;
            assert_eq!(t.get(id).unwrap().val, id, "mw: disjoint lane row {id} dropped");
        }
    }
}
