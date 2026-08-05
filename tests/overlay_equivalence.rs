// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! Store-level differential and OCC guards for the task58 write overlay.
//!
//! The overlay is a SingleWriter-only optimisation: `Store::new` hard-zeroes
//! the cap under `WriterMode::MultiWriter`, so a MultiWriter store never
//! buffers a write while a SingleWriter store buffers nearly all of them.
//! That makes the two modes a free differential oracle — same public API, same
//! op stream, one side overlaid and one side not — without any test hook or
//! added public surface. Everything here drives `Store` through its ordinary
//! public API.
//!
//! Deliberately *not* ported from the parallel implementation: its bare-`Table`
//! equivalence driver and its `ULTIMA_OVERLAY_CAP` cap-independence binary.
//! The former needs a `flush_overlay_for_test` hook this crate does not have
//! (and `table::tests::overlay_table_is_observationally_identical_to_plain_table`
//! already covers that ground internally, where the hooks are `pub(crate)`);
//! the latter tests an env-var read this crate resolves once at `Store::new`.

use ultima_db::{Error, Store, StoreConfig, WriterMode};

#[derive(Debug, Clone)]
enum Op {
    Put(u64, String),
    Update(u64, String),
    Delete(u64),
    Get(u64),
    Len,
    Iter,
    Range(u64, u64),
}

/// Deterministic xorshift — no dev-dependency, and a failing seed is
/// reproducible from the assertion message alone.
struct Rng(u64);
impl Rng {
    fn next(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.0 = x;
        x
    }
    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }
}

fn gen_ops(seed: u64, count: usize, key_space: u64) -> Vec<Op> {
    let mut rng = Rng(seed | 1);
    (0..count)
        .map(|i| {
            let k = rng.below(key_space);
            match rng.below(7) {
                0 => Op::Put(k, format!("v{i}")),
                1 => Op::Update(k, format!("u{i}")),
                2 => Op::Delete(k),
                3 => Op::Get(k),
                4 => Op::Len,
                5 => Op::Iter,
                _ => {
                    let b = rng.below(key_space);
                    Op::Range(k.min(b), k.max(b))
                }
            }
        })
        .collect()
}

fn store_with(mode: WriterMode) -> Store {
    Store::new(StoreConfig::builder().writer_mode(mode).build()).unwrap()
}

/// Applies `ops` through a store, one transaction per chunk, observing both the
/// in-transaction reads and the committed state after every commit.
fn run_through_store(ops: &[Op], mode: WriterMode) -> Vec<String> {
    let store = store_with(mode);
    let mut obs = Vec::new();
    for chunk in ops.chunks(17) {
        let mut wtx = store.begin_write(None).unwrap();
        {
            let mut t = wtx.open_table_keyed::<String, u64>("t").unwrap();
            for op in chunk {
                match op {
                    Op::Put(k, v) => obs.push(format!("put:{:?}", t.put(*k, v.clone()))),
                    Op::Update(k, v) => {
                        obs.push(format!("update:{}", t.update(*k, v.clone()).is_ok()))
                    }
                    Op::Delete(k) => {
                        let r = t.delete(*k);
                        obs.push(format!("delete:{:?}", r.map(|a| (*a).clone()).ok()));
                    }
                    Op::Get(k) => obs.push(format!("get:{:?}", t.get(*k))),
                    Op::Len => obs.push(format!("len:{}", t.len())),
                    Op::Iter => {
                        let rows: Vec<(u64, String)> =
                            t.iter().map(|(k, v)| (k, v.clone())).collect();
                        obs.push(format!("iter:{rows:?}"));
                    }
                    Op::Range(a, b) => {
                        let rows: Vec<(u64, String)> =
                            t.range(*a..=*b).map(|(k, v)| (k, v.clone())).collect();
                        obs.push(format!("range:{rows:?}"));
                    }
                }
            }
        }
        obs.push(format!("commit:{}", wtx.commit().is_ok()));
        let rtx = store.begin_read(None).unwrap();
        let t = rtx.open_table_keyed::<String, u64>("t").unwrap();
        let rows: Vec<(u64, String)> = t.iter().map(|(k, v)| (k, v.clone())).collect();
        obs.push(format!("committed:{rows:?}"));
    }
    obs
}

/// The differential that matters: a MultiWriter store (overlay off) and a
/// SingleWriter store (overlay on) driven by identical op streams must be
/// observationally identical, in-transaction reads included.
///
/// Small key spaces are the point — that is where tombstones, overlay-only
/// deletes and in-place replacement actually get exercised. 300 ops per run at
/// a cap of 32 flushes the SingleWriter side repeatedly, so the merged view is
/// checked against a tree-only reference on both sides of every flush.
#[test]
fn multi_writer_store_is_observationally_identical_to_single_writer() {
    for seed in 1..=20u64 {
        for key_space in [4u64, 17, 200] {
            let ops = gen_ops(seed, 300, key_space);
            assert_eq!(
                run_through_store(&ops, WriterMode::MultiWriter),
                run_through_store(&ops, WriterMode::SingleWriter),
                "divergence at seed={seed} key_space={key_space}"
            );
        }
    }
}

/// Two writers on the same table with disjoint keys both commit, and the
/// second one's per-key merge preserves the first one's row.
///
/// This is the path the MultiWriter gate exists to protect: the merge runs
/// `merge_keys_from` over a clone of the winner's table, which is the one piece
/// of commit machinery that was never written to reason about an overlay.
#[test]
fn multi_writer_disjoint_keys_both_commit_through_the_merge() {
    let store = store_with(WriterMode::MultiWriter);
    {
        let mut wtx = store.begin_write(None).unwrap();
        {
            let mut t = wtx.open_table_keyed::<String, u64>("t").unwrap();
            for i in 1..=4u64 {
                t.put(i, format!("seed{i}")).unwrap();
            }
        }
        wtx.commit().unwrap();
    }

    // `b` is opened first, so its base is the pre-`a` snapshot — that is what
    // makes `b`'s commit take the merge path rather than the wholesale install.
    let mut b = store.begin_write(None).unwrap();
    let mut a = store.begin_write(None).unwrap();
    a.open_table_keyed::<String, u64>("t")
        .unwrap()
        .put(1, "from_a".to_string())
        .unwrap();
    a.commit().unwrap();

    {
        let mut t = b.open_table_keyed::<String, u64>("t").unwrap();
        t.put(2, "from_b".to_string()).unwrap();
        t.delete(3u64).unwrap();
        t.put(5, "new_from_b".to_string()).unwrap();
    }
    b.commit().expect("disjoint keys must not conflict");

    let rtx = store.begin_read(None).unwrap();
    let t = rtx.open_table_keyed::<String, u64>("t").unwrap();
    assert_eq!(t.get(1u64).map(String::as_str), Some("from_a"), "a's row was lost");
    assert_eq!(t.get(2u64).map(String::as_str), Some("from_b"));
    assert_eq!(t.get(3u64), None, "b's delete was lost");
    assert_eq!(t.get(4u64).map(String::as_str), Some("seed4"));
    assert_eq!(t.get(5u64).map(String::as_str), Some("new_from_b"));
    assert_eq!(t.len(), 4);
}

/// Overlapping keys still conflict at commit, the loser still gets
/// `WriteConflict`, and the retry still rebases onto the winner.
#[test]
fn multi_writer_overlapping_keys_conflict_and_the_retry_rebases() {
    let store = store_with(WriterMode::MultiWriter);
    {
        let mut wtx = store.begin_write(None).unwrap();
        {
            let mut t = wtx.open_table_keyed::<String, u64>("t").unwrap();
            t.put(1, "seed".to_string()).unwrap();
            t.put(2, "seed".to_string()).unwrap();
        }
        wtx.commit().unwrap();
    }

    let mut b = store.begin_write(None).unwrap();
    let mut a = store.begin_write(None).unwrap();
    a.open_table_keyed::<String, u64>("t")
        .unwrap()
        .put(1, "from_a".to_string())
        .unwrap();
    a.commit().unwrap();

    // `a` is no longer active, so `b`'s write claims the intent freely; the
    // conflict is the commit-time OCC check on the overlapping key.
    b.open_table_keyed::<String, u64>("t")
        .unwrap()
        .put(1, "from_b".to_string())
        .unwrap();
    let err = b.commit().unwrap_err();
    assert!(
        matches!(err, Error::WriteConflict { .. }),
        "expected WriteConflict, got {err:?}"
    );

    // The winner's row survives the loser's abort...
    {
        let rtx = store.begin_read(None).unwrap();
        let t = rtx.open_table_keyed::<String, u64>("t").unwrap();
        assert_eq!(t.get(1u64).map(String::as_str), Some("from_a"));
        assert_eq!(t.len(), 2);
    }
    // ...and the retry, rebased onto it, commits.
    let mut retry = store.begin_write(None).unwrap();
    {
        let mut t = retry.open_table_keyed::<String, u64>("t").unwrap();
        let prior = t.get(1u64).cloned().unwrap();
        t.put(1, format!("{prior}+from_b")).unwrap();
    }
    retry.commit().expect("rebased retry must commit");

    let rtx = store.begin_read(None).unwrap();
    let t = rtx.open_table_keyed::<String, u64>("t").unwrap();
    assert_eq!(t.get(1u64).map(String::as_str), Some("from_a+from_b"));
    assert_eq!(t.get(2u64).map(String::as_str), Some("seed"));
    assert_eq!(t.len(), 2);
}
