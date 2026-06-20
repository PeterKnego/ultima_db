# WAL Preallocation A/B — 2026-06-20

Same-host A/B of the store WAL with preallocation **off** (`WalWrite::Coalesced`)
vs **on** (`WalWrite::CoalescedPrealloc`), using the YCSB bench from
`docs/benchmarks/baseline-2026-06-17-3282af5.md`. Feature implemented on branch
`feat/wal-preallocation`; see `docs/tasks/task37_wal_preallocation.md` and
`docs/superpowers/specs/2026-06-20-wal-preallocation-design.md`.

## ⚠️ Host & noise caveat — read first

Taken on the **noisy virtualized sandbox** (4 cores, kernel 7.0.0-15-generic,
rustc 1.96.0), the same class of host the baseline doc and
`autobench/baselines/*.json` describe as **up to ~2× run-to-run variance**.

- **Absolute numbers are not portable** — do not compare against another machine.
- Only the **same-host off-vs-on delta** is meaningful here.
- The strict-tier win below (~2×) is large enough to clear the noise floor
  decisively, and the read-only + nondurable controls confirm the A/B isolates
  preallocation — but the real bench machine remains the gold standard for the
  portable magnitude. Treat this as a **strong directional confirmation**, not a
  final number.

## Methodology

- **Bench:** `benches/ycsb_bench.rs` (UltimaDB only), criterion burst benches,
  50 samples/bench. `NUM_RECORDS=10_000`, `OPS_PER_ITER=1_000`, one transaction
  per operation.
- **Real disk:** `ULTIMA_BENCH_DIR=$HOME/ultima-benchdisk` on **ext4** (`/dev/sda1`).
  This is essential — on tmpfs there is no ext4 metadata-journal commit to avoid,
  so preallocation would show nothing.
- **Only variable:** `wal_write` (`Coalesced` vs `CoalescedPrealloc`), selected by
  the new `ULTIMA_BENCH_PREALLOC` env toggle. Durability tier
  (`ULTIMA_BENCH_DURABILITY`), workload, preload, and per-op commit granularity
  are identical between the two runs of a tier.
- **Build:** `--features persistence` (required — without it `Persistence::Standalone`
  is a silent in-memory no-op and neither tier exercises fsync).

```bash
export CARGO_TARGET_DIR=/path/to/target
export ULTIMA_BENCH_DIR=$HOME/ultima-benchdisk        # real ext4, NOT tmpfs
for tier in strict nondurable; do
  ULTIMA_BENCH_DURABILITY=$tier \
    cargo bench --bench ycsb_bench --features persistence -- --save-baseline ${tier}_noprealloc
  ULTIMA_BENCH_DURABILITY=$tier ULTIMA_BENCH_PREALLOC=1 \
    cargo bench --bench ycsb_bench --features persistence -- --save-baseline ${tier}_prealloc
done
critcmp strict_noprealloc     strict_prealloc
critcmp nondur_noprealloc     nondur_prealloc
```

## Results

### Strict tier (`Durability::Consistent` — commit blocks until WAL fsync)

| Workload | OFF (`Coalesced`) | ON (`CoalescedPrealloc`) | speedup |
|---|---|---|---|
| A update-heavy (50/50)   | 550.7 ± 76.7 ms · 1815/s   | 240.2 ± 16.2 ms · 4.1 K/s  | **2.29×** |
| B read-mostly (95/5)     | 58.0 ± 7.3 ms · 16.9 K/s   | 25.2 ± 1.3 ms · 38.8 K/s   | **2.30×** |
| C read-only              | 129.9 ± 1.1 µs · 7.3 M/s   | 132.5 ± 1.3 µs · 7.2 M/s   | 1.02× (flat) |
| D read-latest            | 55.6 ± 6.6 ms · 17.6 K/s   | 25.4 ± 1.8 ms · 38.4 K/s   | **2.19×** |
| E short-ranges (scan)    | 52.5 ± 10.5 ms · 18.6 K/s  | 25.8 ± 2.9 ms · 37.9 K/s   | **2.04×** |
| F read-modify-write      | 512.6 ± 69.4 ms · 1950/s   | 229.6 ± 26.1 ms · 4.3 K/s  | **2.23×** |

Every fsync-bound workload (A, B, D, E, F) is **~2.0–2.3× faster**; the speedup
dwarfs the ±15% error bars. Read-only **C is flat** (reads never fsync) — the
sanity check that the A/B isolates preallocation.

### Nondurable tier (`Durability::Eventual` — async fsync) — control

| Workload | OFF | ON | ratio |
|---|---|---|---|
| A update-heavy   | 2.5 ± 0.81 ms   | 2.3 ± 0.82 ms    | 1.08× (noise) |
| B read-mostly    | 328 ± 17 µs     | 585 ± 640 µs     | noise (error > delta) |
| C read-only      | 134.8 ± 0.6 µs  | 128.6 ± 0.8 µs   | ~1.0× |
| D read-latest    | 465.7 ± 30 µs   | 448.1 ± 29 µs    | ~1.0× |
| E short-ranges   | 895.6 ± 202 µs  | 904.0 ± 221 µs   | ~1.0× |
| F read-modify-write | 2.7 ± 0.12 ms | 3.2 ± 0.76 ms    | noise (error ≈ delta) |

**No meaningful change** — as expected. With Eventual durability the commit does
not block on fsync, so removing the per-fsync metadata-journal cost is off the
critical path. The B/F "ratios" have error bars at or above the delta.

## Interpretation

The win is exactly where the design (`§8` barrier discipline) predicted:
fsync-bound commits. Each `Consistent` commit previously paid an ext4
metadata-journal commit (`i_size`/extent-map update) on every size-extending
append + `sync_all`; preallocation overwrites already-written blocks so the
per-commit barrier degenerates to a pure `fdatasync`. This mirrors
`ultima_journal`'s segment-preallocation win (the etcd WAL trick) ported to the
single-file store WAL.

**Competitive implication (directional, pending same-run head-to-head):** the
2026-06-17 baseline put UltimaDB *last* on strict write-heavy A/F (554/512 ms,
behind ReDB's 275/272 ms). Prealloc-on (240/229 ms) would now edge ahead of
those ReDB figures — but the ReDB numbers are from a different capture, so a
same-run head-to-head against the competitor engines is required to claim it.
