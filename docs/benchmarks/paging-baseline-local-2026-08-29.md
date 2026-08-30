# OS paging as a memory tier — local baseline (2026-08-29)

**Status: throwaway numbers, keepable harness.** Everything here ran on the
Claude sandbox (59 GB RAM, 8 GB swap file on a non-rotational LVM volume,
zswap off, cgroup v2 with the memory controller delegated to the user
session). Absolute throughput is meaningless and the same-machine noise
floor is ±2× (see CLAUDE.md, "Benchmarking"). What *is* trustworthy at this
tier is the **shape**: fault counts per operation, which engine degrades
how, and a 50–100× effect does not care about a 2× noise floor. A
publishable version of this matrix needs the `bench-infra/` NVMe host and
a `bench/paging` target (swap file on the ephemeral NVMe, `vm.swappiness`
override — the rig currently sets it to 0).

## Question

Larger-than-memory support was being designed as a staged paged CoW tree
(`docs/superpowers/specs/` — pending). Before building it, the cheapest
possible alternative deserved a number: **keep UltimaDB entirely
in-memory and let the kernel tier it to swap.** How does that compare to
(a) UltimaDB with the dataset fitting in RAM (the ceiling) and (b)
purpose-built disk engines — ReDB, RocksDB, Fjall — given the *same*
memory budget?

Hypotheses written before running:

- **H1** uniform reads at ≥2× data/memory thrash (>1 major fault per op),
  because `Arc<V>` scatters row values across heap pages with no key
  locality.
- **H2** zipfian (θ=0.99) at 2× stays within ~3× of the ceiling (hot set
  fits).
- **H3** dropping a cold tree (gc / shutdown) shows a long tail: freeing
  faults every page in just to run `Arc` destructors.
- **H4** the disk engines degrade smoothly (<10× at 8×).

## Method

Harness: `compare_benches/src/bin/paging_matrix.rs`, driver
`compare_benches/scripts/paging_matrix.sh`, matrix runner
`compare_benches/scripts/paging_matrix_run.sh`.

- **Shrink RAM, not grow data.** Each cell runs in a `systemd-run --user
  --scope` with `memory.max` = budget and `memory.swap.max = max`.
  `memory.max` charges anon memory *and* page cache to the same budget, so
  the disk engines' block/page caches get exactly the budget UltimaDB
  gets. Fair by construction.
- **Load unconstrained, then tighten.** The harness loads, publishes its
  RSS through a file barrier, and blocks; the driver lowers `memory.max`
  on the live scope (the kernel reclaims cold pages to swap once), then
  releases the barrier. The load never pays for swap; the measured phase
  starts from a settled state.
- **Absolute budget from UltimaDB's footprint.** Post-load RSS of the
  disk engines is tiny (their data is on disk), so a self-relative budget
  would be unfair. The runner calibrates once on UltimaDB and applies
  `footprint / ratio` bytes to every engine at that ratio.
- **Dataset:** 5,000,000 rows × 64-byte value (`Row { a, b, pad: [u64; 6]
  }`, no inner heap allocation — "many small rows"). UltimaDB footprint at
  5M rows: **589 MiB** (~124 B/row: 64 B payload + `Arc` header + glibc
  chunk rounding + leaf share). Budgets: ratio 0.5 → 1179 MiB (ceiling),
  2 → 295 MiB, 4 → 147 MiB.
- **Load path:** UltimaDB via `Store::bulk_load` (values allocated in key
  order — the shape a *recovered* store has); ReDB one transaction;
  RocksDB `disable_wal` puts then `flush` + full `compact_range`; Fjall
  sequential inserts.
- **Workloads:** YCSB-C (100% point read), YCSB-A (50% read / 50%
  update), each with scrambled-zipfian θ=0.99 and uniform key
  distributions. Reads *consume* a value field (an `is_some()` check on
  UltimaDB touches only the leaf page and leaves values in swap — the
  first version of the harness had exactly that bug).
- **Per cell:** 2M-op cap or 45 s wall-clock timeout; single thread;
  latencies recorded per op; major/minor faults from `/proc/self/stat`,
  `pswpin`/`pswpout` from `/proc/vmstat`, cgroup `memory.stat` /
  `memory.events` / PSI read before the scope exits; `drop(engine)` timed
  separately with its own fault count.
- **Engines:** `ultima` (glibc, T=32 default), `ultima-mimalloc`
  (`bench-mimalloc`), `ultima-t8` (`fanout-t8`), `redb` 4, `rocksdb` 0.24
  (default block cache), `fjall` 3. UltimaDB runs `Persistence::None` —
  the pure "the OS is the storage engine" case.

## Results

Raw per-cell JSON: `docs/benchmarks/data-paging-baseline-local-2026-08-29.jsonl`
(72 cells + calibration). `†` = hit the 45 s timeout before the 2M-op cap;
throughput is still ops/elapsed. Single-threaded, one cell at a time, so
`pswpin`/`pswpout` are attributable.

**Reading the tables.** `f/op` is UltimaDB's major faults per operation and is
only shown for UltimaDB: its data is anonymous memory, so every miss is a
page fault. The disk engines read through `pread()`; a page-cache miss inside
a syscall is I/O, not a fault, so their fault counts are near zero *and
meaningless* — for them the evidence is throughput and latency. On-disk size
at 5M rows: ReDB 1028 MiB, Fjall 436 MiB, RocksDB 219 MiB (raw payload
≈ 343 MiB). RocksDB's dataset therefore fits the 295 MiB ratio-2 budget
outright, which is why its 2× column equals its ceiling; at 4× (147 MiB) it
is genuinely paging.

**YCSB-C reads, zipfian** — ops/s · p50 · p99 (budget 1179 / 295 / 147 MiB)

| engine | 0.5× (ceiling) | 2× | 4× | 4× vs ceiling |
|---|---|---|---|---|
| ultima | 1928k · 0.3 µs · 1 µs · 0.00 f/op | 64k · 1.3 µs · 106 µs · 0.26 f/op | 31k† · 1.7 µs · 168 µs · 0.58 f/op | 61× slower |
| ultima-mimalloc | 1477k · 0.4 µs · 1 µs · 0.00 f/op | 44k† · 1.2 µs · 135 µs · 0.43 f/op | 25k† · 1.9 µs · 167 µs · 0.77 f/op | 59× slower |
| ultima-t8 | 2194k · 0.3 µs · 1 µs · 0.00 f/op | 62k · 0.8 µs · 111 µs · 0.28 f/op | 25k† · 1.5 µs · 191 µs · 0.69 f/op | 86× slower |
| redb | 478k · 1.0 µs · 50 µs | 44k† · 2.2 µs · 126 µs | 30k† · 2.3 µs · 146 µs | 16× slower |
| rocksdb | 332k · 1.3 µs · 6 µs | 334k · 1.3 µs · 6 µs | 60k · 2.1 µs · 74 µs | 6× slower |
| fjall | 583k · 1.6 µs · 3 µs | 45k · 3.2 µs · 86 µs | 42k† · 3.1 µs · 81 µs | 14× slower |

**YCSB-C reads, uniform**

| engine | 0.5× (ceiling) | 2× | 4× | 4× vs ceiling |
|---|---|---|---|---|
| ultima | 1026k · 0.9 µs · 1 µs · 0.00 f/op | 25k† · 48.8 µs · 175 µs · 0.69 f/op | 14k† · 54.5 µs · 437 µs · 1.28 f/op | 75× slower |
| ultima-mimalloc | 1086k · 0.9 µs · 1 µs · 0.00 f/op | 22k† · 47.8 µs · 387 µs · 0.78 f/op | 13k† · 82.6 µs · 197 µs · 1.49 f/op | 83× slower |
| ultima-t8 | 1541k · 0.6 µs · 1 µs · 0.00 f/op | 24k† · 47.8 µs · 169 µs · 0.73 f/op | 12k† · 61.4 µs · 237 µs · 1.46 f/op | 126× slower |
| redb | 333k · 1.6 µs · 68 µs | 17k† · 56.6 µs · 332 µs | 14k† · 59.2 µs · 501 µs | 24× slower |
| rocksdb | 198k · 5.3 µs · 6 µs | 193k · 5.4 µs · 7 µs | 30k† · 9.3 µs · 85 µs | 7× slower |
| fjall | 441k · 2.2 µs · 3 µs | 21k† · 54.3 µs · 92 µs | 20k† · 54.6 µs · 89 µs | 22× slower |

**YCSB-A 50/50, zipfian**

| engine | 0.5× (ceiling) | 2× | 4× | 4× vs ceiling |
|---|---|---|---|---|
| ultima | 273k · 1.2 µs · 94 µs · 0.00 f/op | 1745† · 3.1 µs · 30.1 ms · 7.72 f/op | 839† · 62.9 µs · 68.7 ms · 12.51 f/op | 326× slower |
| ultima-mimalloc | 312k · 1.2 µs · 73 µs · 0.00 f/op | 1502† · 54.2 µs · 36.5 ms · 11.02 f/op | 957† · 105.2 µs · 59.0 ms · 17.75 f/op | 326× slower |
| ultima-t8 | 558k · 0.8 µs · 41 µs · 0.00 f/op | 11k† · 1.9 µs · 3.0 ms · 1.20 f/op | 3058† · 55.9 µs · 14.7 ms · 5.13 f/op | 183× slower |
| redb | 63k · 19.5 µs · 84 µs | 22k† · 23.8 µs · 262 µs | 15k† · 40.0 µs · 460 µs | 4× slower |
| rocksdb | 433k · 1.8 µs · 8 µs | 288k · 1.8 µs · 62 µs | 123k · 2.1 µs · 70 µs | 4× slower |
| fjall | 555k · 1.5 µs · 5 µs | 58k · 2.7 µs · 77 µs | 45k · 2.7 µs · 148 µs | 12× slower |

**YCSB-A 50/50, uniform**

| engine | 0.5× (ceiling) | 2× | 4× | 4× vs ceiling |
|---|---|---|---|---|
| ultima | 235k · 1.6 µs · 102 µs · 0.00 f/op | 1012† · 59.1 µs · 51.4 ms · 11.77 f/op | 715† · 113.2 µs · 73.5 ms · 18.66 f/op | 328× slower |
| ultima-mimalloc | 302k · 1.5 µs · 72 µs · 0.00 f/op | 983† · 95.2 µs · 51.6 ms · 14.53 f/op | 688† · 120.2 µs · 79.7 ms · 21.90 f/op | 439× slower |
| ultima-t8 | 468k · 1.0 µs · 47 µs · 0.00 f/op | 5824† · 56.2 µs · 5.8 ms · 2.27 f/op | 2114† · 106.4 µs · 21.2 ms · 7.82 f/op | 221× slower |
| redb | 34k† · 21.7 µs · 90 µs | 11k† · 66.9 µs · 713 µs | 6945† · 84.1 µs · 1.1 ms | 5× slower |
| rocksdb | 222k · 3.4 µs · 9 µs | 120k · 3.8 µs · 66 µs | 43k† · 5.2 µs · 84 µs | 5× slower |
| fjall | 341k · 2.8 µs · 6 µs | 30k† · 4.8 µs · 89 µs | 30k† · 5.8 µs · 125 µs | 11× slower |

**Drop (free the tree / close the engine) at 4×** — seconds · major faults

| engine | after C/zipf | after A/uniform |
|---|---|---|
| ultima | 2.8 s · 45,339 | 5.6 s · 68,484 |
| ultima-mimalloc | 2.4 s · 45,872 | 4.9 s · 63,223 |
| ultima-t8 | 3.4 s · 46,022 | 12.1 s · 155,867 |
| redb | 34.3 s · 426,555 | 31.8 s · 384,849 |
| rocksdb | 0.0 s · 504 | 0.7 s · 4,938 |
| fjall | 0.6 s · 6,927 | 2.4 s · 33,883 |

### What the tables say

1. **Reads on swap are survivable, and roughly at parity with ReDB.** At 4×
   UltimaDB does 31k/14k ops/s (zipf/uniform) against ReDB's 30k/14k at the
   same budget, with the same p50 (~55 µs uniform — one swap-in ≈ one
   `pread` on this disk). Per cold read UltimaDB pays ~1.3–1.5 faults:
   leaf page + value page. Zipfian keeps p50 at 1.7 µs (hot set in RAM) and
   moves the cost to p99. The LSMs are 2–4× ahead on reads at 4× because
   their block layout packs ~40–60 rows per 4 KB page where UltimaDB's leaf
   (1.5 KB, `Arc` pointers) plus scattered 96-byte value chunks need two
   pages per row.
2. **Writes on swap collapse, and it is UltimaDB-specific.** At 2× — a
   modest overcommit — YCSB-A drops to 1,745 ops/s zipf / 1,012 uniform
   (**150–230× below ceiling**), p99 30–50 ms, 8–12 faults per op. ReDB, a
   CoW B-tree *on disk*, does 22k/11k at the same budget with p99 under
   1 ms; RocksDB and Fjall 30k–290k. The mechanism is in the next section;
   the short version is that a CoW path update rewrites the refcounts of
   every sibling of the target leaf.
3. **`fanout-t8` helps writes 3–6× under paging and costs nothing on reads.**
   At 2× A/zipf: 11.1k ops/s at 1.2 faults/op, vs 1.7k at 7.7. That is the
   sibling-touch term shrinking with fan-out. It is still 4–20× behind the
   disk engines.
4. **mimalloc does not help.** Same shape, slightly more faults (its
   size-class segregation spreads a row's leaf and value further apart).
5. **Dropping a cold tree is a real cost: 2.4–5.6 s and 45–68k faults** for
   5M rows at 4× (H3 confirmed). ReDB's 30+ s is closing/deleting a 1 GB
   file under a 147 MiB budget, a different problem. `ultima-t8` pays
   double on drop after the write workload because the taller tree has more
   uniquely-owned nodes to free.


## Mechanism: why cold-key *writes* collapse

The write cell was the surprise, and it decomposes cleanly. The harness's
`DIAG` workload measures major faults around each phase of a cold-key
update on a 2M-row store at budget = footprint/4 (n=400 keys, uniform):

| phase | overlay on (default, cap 32) | overlay off (`ULTIMA_OVERLAY_CAP=0`) | hot 1000 keys, overlay off |
|---|---|---|---|
| read | 1.59 faults, 111 µs | 1.57, 116 µs | 0.04 |
| update (`merged_get_arc` + CoW `insert_mut`) | **28.4 faults (max 1,990)**, 1.6 ms | **37.6 (max 178)**, 1.8 ms | 0.52 |
| commit + auto gc | 0.43 | 0.13 | 0.01 |

A cold read costs what paging theory says: ~1 leaf page + ~1 value page.
A cold **update** costs ~25–40 faults, none of them in commit or gc, and
none of them when the key is hot. Not the allocator: mimalloc gives 17
faults/op on the full A/zipf cell vs glibc's 15.

**The CoW step touches the target leaf's whole sibling fan-out.**
`Arc::make_mut` on a path node clones `BTreeNode`, whose `children` is a
`FixedVec<Arc<BTreeNode>, MAX_KEYS + 2>`. Cloning it is up to 64
`Arc::clone`s — 64 refcount *writes* into 64 child nodes. For a level-1
node those children are the target leaf's siblings: 64 × ~1.5 KB ≈ 96 KB
≈ **24 cold pages, faulted in and dirtied** for one row update. Ten
commits later the old level-1 node is dropped and decrements the same 64
counts again. That is also why `pswpout` on the A cells is ~110–140 KB
per op: refcount bumps make clean swapped-in pages dirty, so every
eviction is a swap *write*.

Two independent predictions of that mechanism held:

1. Hot-key-only updates (`--keys=1000`) at the same budget: 1.05M ops/s,
   0.001 faults/op, `pswpout` 0. The write path itself is clean; only the
   coldness of the *siblings* costs.
2. `fanout-t8` (16 children per node): update faults **37.6 → 13.9**,
   1.8 ms → 0.68 ms, reads unchanged (1.24). Not a clean 4× because the
   T=8 tree is two levels taller for 2M rows, so more levels have cold
   siblings.

The write overlay (task58) does not remove the cost, it batches it: mean
faults drop from 38 to 28, but every 32nd write flushes 32 CoW paths at
once — a **1,990-fault, ~60–90 ms** single op. That flush is the p99 on
every constrained A cell.

Not verified with a profiler: `perf_event_paranoid=4` on this host.

### Why this matters for the larger-than-memory design

Intrusive refcounts (`Arc`) put the count *inside the child*, so any CoW
of a parent must touch every child. That is intrinsic to `Arc<BTreeNode>`
children and no allocator or page-size tuning removes it. A paged design
whose child slot is `Child::OnDisk(PageId)` has nothing to bump — the
sibling cost goes to zero by construction. This experiment turns "stage B
is the principled answer" into "stage B is the only one of the options
that fixes the write path", and gives the concrete target its stage-3
eviction work must beat: **faults per cold point read ≈ 1, per cold
update ≈ 2** (leaf in, leaf out), versus ~1.5 and ~30–40 today.

It is also a note for the in-memory engine independent of paging: where
the refcount lives (in the child vs. in the parent's slot) is a real
design lever for write-side cache behaviour, and the T-sweep's
"bigger T slows CoW writes" finding
(`docs/benchmarks/btree-fanout-t-sweep-2026-07-09.md`) has the same
sibling-touch term in it.

## Verdicts on the hypotheses

| | hypothesis | verdict |
|---|---|---|
| H1 | uniform reads at ≥2× thrash (>1 fault/op) | **Partly.** 0.69 faults/op at 2×, 1.28 at 4×. The value-scatter effect is real (two pages per row) but the swap cache and readahead keep it near 1, not the several-per-op that "thrash" implied. Reads degrade to disk speed, not below it. |
| H2 | zipfian at 2× stays within ~3× of ceiling | **Rejected for throughput, confirmed for p50.** 30× below ceiling on reads (1.93M → 64k) because the ~50% of zipf-0.99 accesses that land in the cold tail each cost a swap-in; p50 stays at 1.3 µs. Writes: 156× below. |
| H3 | gc / drop of a cold tree has a long tail | **Confirmed.** 2.4–12 s and 45k–156k faults to free a 5M-row tree at 4×: every `Arc` destructor touches its page. |
| H4 | disk engines degrade smoothly (<10× at 8×) | **Confirmed at 4×** for RocksDB (4–7×) and ReDB on writes (4–5×); Fjall 11–22× and ReDB reads 16–24× on this disk. 8× was not run. |

**Not in the hypotheses, and the headline: cold-key writes cost 25–40
faults each** — an order of magnitude more than the tree walk — because
intrusive `Arc` refcounts make a CoW path touch its whole sibling fan-out.

## Decision

OS paging is **not** a viable larger-than-memory tier for UltimaDB as it
stands: reads are at parity with an on-disk CoW B-tree, but any write to a
cold key is 100–300× slower than the same engine in memory and 10–20×
slower than ReDB/RocksDB at the same budget, with a 30–80 ms p99. The
decision rule set before the run ("within ~2–3× of ReDB at 4× zipf → OS
paging is a legitimate tier-0 answer") fails on writes by an order of
magnitude.

The staged paged-tree design (stage B) stands, with two things this
experiment adds to it:

- The concrete target for its eviction stage: **~1 fault per cold point
  read, ~2 per cold update** (leaf in, leaf out), against ~1.4 and ~30–40
  measured here. A `Child::OnDisk(PageId)` slot has no refcount to bump, so
  the sibling term is gone by construction, not by tuning.
- A cheap interim for anyone who must run overcommitted today: `fanout-t8`
  is 3–6× better on writes under paging and no worse on reads.

Next measurement, when stage B has something to compare: this matrix on
the NVMe rig (`bench-infra/`, new `bench/paging` target) with a
non-compressible payload and the 8× column, so the ReDB/RocksDB/Fjall
columns become the published baseline the paged tree is judged against.


## Reproduce

```bash
cargo build --release -p compare-benches --bin paging_matrix
T="$(cargo metadata --format-version 1 --no-deps | jq -r .target_directory)"
CARGO_TARGET_DIR="$T/mimalloc" cargo build --release -p compare-benches --bin paging_matrix --features bench-mimalloc
CARGO_TARGET_DIR="$T/t8"       cargo build --release -p compare-benches --bin paging_matrix --features ultima-db/fanout-t8
cd compare_benches
ROWS=5000000 RATIOS="0.5 2 4" ENGINES="ultima ultima-mimalloc ultima-t8 redb rocksdb fjall" \
  DISTS="zipf uniform" WORKLOADS="C A" OPS=2000000 TIMEOUT=45 OUT=paging-results.jsonl \
  scripts/paging_matrix_run.sh
# per-phase decomposition of a cold-key update:
ULTIMA_OVERLAY_CAP=0 scripts/paging_matrix.sh RATIO=4 -- "$T/release/paging_matrix" \
  --engine=ultima --rows=2000000 --dist=uniform --workload=DIAG --ops=400
```

Requirements: `systemd --user` with the memory controller delegated
(`cgroup.subtree_control` of `user@$UID.service` lists `memory`) and swap
enabled. The driver lowers `memory.max` on the live scope; on hosts
without delegation it fails at that write rather than silently running
unconstrained.
