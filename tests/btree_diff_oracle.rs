// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! `BTree::diff` against a brute-force oracle.
//!
//! The failure mode this guards is a *silently dropped* change: a diff that
//! misses a key loses a row on checkpoint recovery, with no error anywhere.
//! Example tests cannot cover the structural cases (splits, merges, root
//! collapse) that make a key move between nodes, so this compares against a
//! full scan of both trees over randomly generated histories.

use std::collections::{BTreeMap, BTreeSet};

use proptest::prelude::*;
use ultima_db::{BTree, Change};

#[derive(Debug, Clone)]
enum Op {
    Insert(u64, u64),
    Remove(u64),
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

fn apply(tree: &BTree<u64, u64>, model: &mut BTreeMap<u64, u64>, op: &Op) -> BTree<u64, u64> {
    match op {
        Op::Insert(k, v) => {
            if model.get(k) == Some(v) {
                return tree.clone(); // same value: keep the existing Arc
            }
            model.insert(*k, *v);
            tree.insert(*k, *v)
        }
        Op::Remove(k) => {
            model.remove(k);
            tree.remove(k).unwrap_or_else(|_| tree.clone())
        }
    }
}

/// Expected diff, computed the slow way: full scan of both trees.
fn oracle(
    new: &BTreeMap<u64, u64>,
    base: &BTreeMap<u64, u64>,
) -> Vec<(u64, Option<u64>)> {
    let mut out = Vec::new();
    let mut keys: Vec<u64> = new.keys().chain(base.keys()).copied().collect();
    keys.sort_unstable();
    keys.dedup();
    for k in keys {
        match (new.get(&k), base.get(&k)) {
            (Some(nv), Some(bv)) if nv == bv => {}
            (Some(nv), _) => out.push((k, Some(*nv))),
            (None, Some(_)) => out.push((k, None)),
            (None, None) => unreachable!("key came from one of the two maps"),
        }
    }
    out
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(256))]

    #[test]
    fn diff_matches_full_scan_oracle(base_ops in ops(), then_ops in ops()) {
        let mut model = BTreeMap::new();
        let mut tree = BTree::<u64, u64>::new();
        for op in &base_ops {
            tree = apply(&tree, &mut model, op);
        }
        let base_tree = tree.clone();
        let base_model = model.clone();

        for op in &then_ops {
            tree = apply(&tree, &mut model, op);
        }

        // `Change` carries whether a key was Added/Removed (a presence flip,
        // unambiguous) or merely Updated (a value-Arc flip), so keep that
        // tag around instead of collapsing straight to (k, Option<v>).
        let got: Vec<(u64, Option<u64>, bool)> = tree
            .diff(&base_tree)
            .map(|c| match c {
                Change::Added(k, v) => (*k, Some(**v), false),
                Change::Updated(k, v) => (*k, Some(**v), true),
                Change::Removed(k) => (*k, None, false),
            })
            .collect();

        let expected: BTreeMap<u64, Option<u64>> =
            oracle(&model, &base_model).into_iter().collect();

        // `apply`'s same-binding guard (skip re-insert when the value is
        // already bound) closes the obvious case, but a remove followed by
        // re-inserting the same value slips past it: the model sees no net
        // change (the key ends up bound to the value it had at base), yet
        // the tree genuinely got a fresh Arc for that key in between, so
        // `diff` — which compares Arc identity, not deep value equality —
        // correctly reports Updated. That is documented, intended behavior
        // (see `BTree::diff`'s doc comment), not a bug to paper over: it is
        // conservative (an extra write on checkpoint, never a dropped one).
        // So: every real change must show up (no drops, checked below), and
        // the only kind of surplus tolerated is a same-value Updated.
        let mut got_keys = BTreeSet::new();
        for (k, v, is_updated) in &got {
            got_keys.insert(*k);
            match expected.get(k) {
                Some(ev) => prop_assert_eq!(
                    ev, v,
                    "diff value for key {} does not match the oracle", k
                ),
                None => {
                    prop_assert!(
                        *is_updated,
                        "diff reports {:?} for key {} but the oracle expects no change \
                         at all (only a same-value Updated is tolerable here)",
                        v, k
                    );
                    prop_assert_eq!(
                        model.get(k).copied(), *v,
                        "surplus Updated for key {} does not even match the final value",
                        k
                    );
                }
            }
        }
        for k in expected.keys() {
            prop_assert!(got_keys.contains(k), "diff silently dropped a change for key {}", k);
        }
    }
}
