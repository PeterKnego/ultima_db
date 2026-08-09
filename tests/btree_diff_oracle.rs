// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! `BTree::diff` against a brute-force oracle.
//!
//! The failure mode this guards is a *silently dropped* change: a diff that
//! misses a key loses a row on checkpoint recovery, with no error anywhere.
//! Example tests cannot cover the structural cases (splits, merges, root
//! collapse) that make a key move between nodes, so this compares against a
//! full scan of both trees over randomly generated histories.
//!
//! This is the narrow-key, high-collision half of the oracle: a small key
//! space (`0..200`) so `Add`/`Update`/`Remove` all happen constantly and the
//! tree gets restructured (splits, merges, root collapse) under a shallow
//! (height <= 2 under the default `T=32`) tree. The complementary deep-tree
//! case — wide key space, deterministic seed forcing height >= 3, proving
//! the multi-level subtree-skip and multi-frame pop-chain paths — lives in
//! `src/btree.rs`'s unit tests (`diff_matches_full_scan_oracle_deep_tree`),
//! because confirming it reaches that depth needs `root`/`children`, which
//! this integration test's public-API-only view doesn't have.

use std::collections::BTreeMap;

use proptest::prelude::*;
use ultima_db::{BTree, Change};

#[derive(Debug, Clone)]
enum Op {
    Insert(u64, u64),
    Remove(u64),
}

/// The kind of change `diff` reports, kept alongside the value so the oracle
/// can be checked exactly rather than just on `(key, value)`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Tag {
    Added,
    Updated,
    Removed,
}

fn ops() -> impl Strategy<Value = Vec<Op>> {
    // Keys drawn from a small space so inserts and removes actually collide;
    // a wide key space would make almost every remove a no-op.
    prop::collection::vec(
        prop_oneof![
            (0u64..200, 0u64..1000).prop_map(|(k, v)| Op::Insert(k, v)),
            (0u64..200).prop_map(Op::Remove),
        ],
        0..300,
    )
}

/// `apply` mutates both the real tree and a model that mirrors it — but the
/// model tracks `(value, generation)` per key, not just `value`. `diff`
/// compares `Arc::ptr_eq`, and `BTree::insert` allocates a *fresh* value
/// `Arc` on every call, even when the inserted value is unchanged (e.g. a
/// remove followed by re-inserting the same value: the key's binding is
/// physically a new `Arc`, though `==` can't tell). A model keyed on value
/// equality alone would disagree with `diff` on exactly that case. The
/// generation counter mirrors the Arc-identity axis directly — every insert
/// bumps it — so the oracle built from it agrees with `diff` exactly, with
/// no tolerance needed anywhere in the comparison.
fn apply(
    tree: &BTree<u64, u64>,
    model: &mut BTreeMap<u64, (u64, u64)>,
    generation: &mut u64,
    op: &Op,
) -> BTree<u64, u64> {
    match op {
        Op::Insert(k, v) => {
            *generation += 1;
            model.insert(*k, (*v, *generation));
            tree.insert(*k, *v)
        }
        Op::Remove(k) => {
            model.remove(k);
            tree.remove(k).unwrap_or_else(|_| tree.clone())
        }
    }
}

/// Expected diff, computed the slow way: full scan of both trees, comparing
/// per-key generation (not value) to decide Updated vs. no-change — this is
/// what makes the oracle agree with `diff`'s Arc-identity semantics exactly.
fn oracle(
    new: &BTreeMap<u64, (u64, u64)>,
    base: &BTreeMap<u64, (u64, u64)>,
) -> Vec<(u64, Option<u64>, Tag)> {
    let mut out = Vec::new();
    let mut keys: Vec<u64> = new.keys().chain(base.keys()).copied().collect();
    keys.sort_unstable();
    keys.dedup();
    for k in keys {
        match (new.get(&k), base.get(&k)) {
            (Some((_, ng)), Some((_, bg))) if ng == bg => {}
            (Some((nv, _)), Some(_)) => out.push((k, Some(*nv), Tag::Updated)),
            (Some((nv, _)), None) => out.push((k, Some(*nv), Tag::Added)),
            (None, Some(_)) => out.push((k, None, Tag::Removed)),
            (None, None) => unreachable!("key came from one of the two maps"),
        }
    }
    out
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(256))]

    #[test]
    fn diff_matches_full_scan_oracle_narrow_keys(base_ops in ops(), then_ops in ops()) {
        let mut model: BTreeMap<u64, (u64, u64)> = BTreeMap::new();
        let mut generation: u64 = 0;
        let mut tree = BTree::<u64, u64>::new();
        for op in &base_ops {
            tree = apply(&tree, &mut model, &mut generation, op);
        }
        let base_tree = tree.clone();
        let base_model = model.clone();

        for op in &then_ops {
            tree = apply(&tree, &mut model, &mut generation, op);
        }

        let got: Vec<(u64, Option<u64>, Tag)> = tree
            .diff(&base_tree)
            .map(|c| match c {
                Change::Added(k, v) => (*k, Some(**v), Tag::Added),
                Change::Updated(k, v) => (*k, Some(**v), Tag::Updated),
                Change::Removed(k) => (*k, None, Tag::Removed),
            })
            .collect();

        prop_assert_eq!(got, oracle(&model, &base_model));
    }
}
