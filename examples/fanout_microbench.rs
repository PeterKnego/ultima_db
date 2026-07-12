// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! Fanout (`T`) microbench for the **read-vs-write asymmetry** study
//! (`docs/benchmarks/btree-fanout-t-sweep-2026-07-09.md`).
//!
//! `T` is a compile-time const in `src/btree.rs`, so a fanout sweep rebuilds per
//! `T` (see `scripts/fanout_micro_ab.sh`). For each build this binary measures the
//! per-op cost of **get / insert / update / remove** in TWO regimes, because the
//! regime is exactly what the asymmetry hinges on:
//!
//!   * **warm (CoW)** — the op runs on a tree whose root→leaf path is *shared*
//!     with retained clones, so `Arc::make_mut` clones every node on the path.
//!     This is the production MVCC single-op / SMR-apply regime. The formula says
//!     cost ≈ height × width ≈ `T / ln T` (monotonically *increasing* in `T` past
//!     e): bigger `T` is SLOWER.
//!   * **cold (in-place)** — the op runs on a uniquely-owned tree; `make_mut`
//!     mutates in place, no clone. This is the batch / bulk-load regime. Cost ≈
//!     height + O(T) in-node work: a U-shape with an optimum, NOT the warm curve.
//!
//! `get` never clones, so it has one number (no warm/cold split): cost ≈ tree
//! height ≈ `ln N / ln T` — bigger `T` is FASTER. That opposite sign vs warm
//! writes IS the asymmetry.
//!
//! Emits one JSON object per run on stdout with `--json`. Env: `N` (prebuilt
//! keys, default 1_000_000), `M` (timed ops per metric, default 30_000),
//! `FANOUT_QUICK=1` (tiny N/M for a local correctness smoke — NOT for numbers).
//!
//! Run: `cargo run --release --example fanout_microbench -- --json`

use std::hint::black_box;
use std::time::Instant;

use ultima_db::BTree;

/// Deterministic scatter (LCG) — matches the other btree benches so the same key
/// distribution is measured across the sweep. No rng/clock dependency.
fn scatter(n: u64) -> Vec<u64> {
    let mut lcg = 0x2545F4914F6CDD1Du64;
    (0..n)
        .map(|_| {
            lcg = lcg
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            lcg
        })
        .collect()
}

fn build(keys: &[u64]) -> BTree<u64, u64> {
    let mut t = BTree::<u64, u64>::new();
    for &k in keys {
        t.insert_mut(k, k);
    }
    t
}

/// Warm timing: `M` *independent* CoW ops. Pre-clone `base` into `M` copies
/// (untimed — O(1) Arc bumps); every copy shares its whole structure with `base`,
/// so the first mutation on each copy forces `Arc::make_mut` to clone the full
/// root→leaf path (the warm regime). Timing the loop over distinct copies with a
/// single clock read avoids any per-op timer overhead skewing the ratios.
/// `op` performs one mutation on `copies[i]` using `keys[i]`. Returns ns/op.
fn warm_ns<F>(base: &BTree<u64, u64>, keys: &[u64], m: usize, op: F) -> f64
where
    F: Fn(&mut BTree<u64, u64>, u64),
{
    let mut copies: Vec<BTree<u64, u64>> = (0..m).map(|_| base.clone()).collect();
    let t0 = Instant::now();
    for (i, &k) in keys.iter().take(m).enumerate() {
        op(&mut copies[i], k);
    }
    let dt = t0.elapsed();
    black_box(&copies); // keep `base` sharing live through the timed loop
    drop(copies);
    dt.as_nanos() as f64 / m as f64
}

fn main() {
    let json = std::env::args().any(|a| a == "--json");
    let quick = std::env::var("FANOUT_QUICK").as_deref() == Ok("1");
    let n: u64 = std::env::var("N")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(if quick { 20_000 } else { 1_000_000 });
    let m: usize = std::env::var("M")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(if quick { 2_000 } else { 30_000 });
    assert!((m as u64) <= n, "M must be <= N");

    // Keys: first N are inserted ("present"); the next M are fresh ("new").
    let all = scatter(n + m as u64);
    let present = &all[..n as usize];
    let existing_probe = &present[..m]; // present keys, for get / update / remove
    let new_probe = &all[n as usize..]; // absent keys, for insert (grows the tree)

    // One pristine, uniquely-owned base tree. Reused as: (1) read target for get,
    // (2) clone-source for the warm arms (never mutated — clones are dropped), then
    // (3) the uniquely-owned tree for the cold arms (mutated in place, done last).
    let mut base = build(present);

    // -- get (read): traverse only, never clones. Expect ~ 1/ln T.
    let t0 = Instant::now();
    for &k in existing_probe {
        black_box(base.get(&k));
    }
    let get_ns = t0.elapsed().as_nanos() as f64 / m as f64;

    // -- warm writes (CoW-clone regime): expect ~ T/ln T (bigger T slower).
    let insert_warm_ns = warm_ns(&base, new_probe, m, |t, k| t.insert_mut(k, k));
    let update_warm_ns = warm_ns(&base, existing_probe, m, |t, k| t.insert_mut(k, k)); // replace
    let remove_warm_ns = warm_ns(&base, existing_probe, m, |t, k| {
        t.remove_mut(&k);
    });

    // -- cold writes (in-place, uniquely-owned base — all warm clones now dropped,
    // so base is back to strong_count 1). Order chosen to keep every op in place
    // and return the tree to size N: update (size-stable) → insert new (grows by M)
    // → remove the new keys (shrinks back). Expect the U-shaped height+width curve.
    let t0 = Instant::now();
    for &k in existing_probe {
        base.insert_mut(k, k); // replace, in place
    }
    let update_cold_ns = t0.elapsed().as_nanos() as f64 / m as f64;

    let t0 = Instant::now();
    for &k in new_probe {
        base.insert_mut(k, k); // in place, grows the tree
    }
    let insert_cold_ns = t0.elapsed().as_nanos() as f64 / m as f64;

    let t0 = Instant::now();
    for &k in new_probe {
        base.remove_mut(&k); // in place, shrinks back to N
    }
    let remove_cold_ns = t0.elapsed().as_nanos() as f64 / m as f64;

    black_box(&base);

    // T is a compile-time const in btree.rs; surface it via MAX_KEYS so the driver
    // does not have to trust its own sed. BTree exposes neither directly, so we
    // read the env the driver stamps (fallback 0 => driver fills it in).
    let t_const: u64 = std::env::var("FANOUT_T").ok().and_then(|s| s.parse().ok()).unwrap_or(0);

    if json {
        println!(
            "{{\"t\":{t_const},\"n\":{n},\"m\":{m},\
             \"get_ns\":{get_ns:.1},\
             \"insert_warm_ns\":{insert_warm_ns:.1},\"update_warm_ns\":{update_warm_ns:.1},\"remove_warm_ns\":{remove_warm_ns:.1},\
             \"insert_cold_ns\":{insert_cold_ns:.1},\"update_cold_ns\":{update_cold_ns:.1},\"remove_cold_ns\":{remove_cold_ns:.1}}}"
        );
    } else {
        eprintln!("T={t_const} N={n} M={m}");
        eprintln!("  get            {get_ns:8.1} ns   (read; expect faster at big T)");
        eprintln!("  warm insert    {insert_warm_ns:8.1} ns   (CoW; expect slower at big T)");
        eprintln!("  warm update    {update_warm_ns:8.1} ns");
        eprintln!("  warm remove    {remove_warm_ns:8.1} ns");
        eprintln!("  cold insert    {insert_cold_ns:8.1} ns   (in-place; U-shaped)");
        eprintln!("  cold update    {update_cold_ns:8.1} ns");
        eprintln!("  cold remove    {remove_cold_ns:8.1} ns");
    }
}
