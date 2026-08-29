# UltimaDB vs TurboKV — YCSB, local host, 2026-08-29

**Local run — not publishable.** Per `CLAUDE.md`, no perf conclusion is drawn
from absolute numbers here; read this doc for *same-host ratios* and for the
qualitative behaviour observed. A remote NVMe run (`bench-infra/`) has **not**
been done for TurboKV.

Motivation: [TurboKV](https://github.com/kingroryg/turbokv)'s README claims
2.99M keys/s sequential fill (fast), 1.77M (durable) on an Apple M4 vs fjall
and redb. This run puts TurboKV 0.6.0 into this repo's `compare_benches` YCSB
harness against UltimaDB and fjall 3.1.2 on one machine, both durability tiers.
The feature-by-feature inventory lives in
`docs/compared-with-turbokv-2026-08-29.md`.

## Provenance

- Host: AMD Ryzen AI MAX+ 395, 32 threads, ext4 on `/dev/mapper/ubuntu--vg-ubuntu--lv`
  (consumer NVMe, ~2 ms fsync as measured by the strict-tier A cell below),
  Linux 7.0.0-30-generic, rustc/cargo 1.96.0. Single run, sequential arms,
  otherwise idle box.
- UltimaDB working tree at `93eff28` + the harness changes in this commit
  (`compare_benches/benches/ycsb_turbokv_bench.rs`, `Makefile`
  `bench/ycsb/turbokv`). TurboKV 0.6.0 from crates.io, fjall 3.1.2.
- Harness: `bench_workloads::ycsb` — 10,000 records × ~1 KB (10 fields × 100 B,
  bincode), 1,000-op bursts, Zipfian 0.99, criterion 50 samples / 10 s target
  per workload, `ULTIMA_BENCH_DIR=$HOME/bench-disk` (real disk). Every engine
  commits **per operation**. Metric: criterion median per 1,000-op burst.
- Run log and exported criterion baselines (`critcmp --export`) were kept in
  the session scratchpad only; the tables below are the `critcmp` output
  verbatim.

## Tier mapping

| Tier | UltimaDB | fjall | TurboKV |
|---|---|---|---|
| non-durable (log written, no fsync on commit) | `Durability::Eventual` + `Coalesced` | `PersistMode::Buffer` | `DbOptions::durable()` (WAL on, `sync_writes=false`) |
| non-durable, **no log** (TurboKV headline column, no peer) | — | — | `DbOptions::fast()` (`wal_enabled=false`) |
| strict (fsync per commit) | `standalone_fast` (`ConsistentInline` + `CoalescedPrealloc`) | `PersistMode::SyncAll` | `DbOptions::paranoid()` |

TurboKV compression set to `None` (matches its own published protocol and the
other engines, which store bincode bytes verbatim); block cache and 64 MiB
memtable left at preset defaults. Its API is async-only, so the adapter drives
each 1,000-op burst through one `tokio` `Runtime::block_on` (one runtime hop
per burst, not per op; multi-thread runtime so its background flush/compaction
tasks run as they would for an embedder).

## Non-durable tier

`critcmp -g '(.+)/[^/]+' ultima_nondurable fjall_nondurable turbokv_nondurable turbokv_nowal`:

```
group                       fjall_nondurable//burst                  turbokv_nondurable//burst                 turbokv_nowal//burst                      ultima_nondurable//burst
-----                       -----------------------                  -------------------------                 --------------------                      ------------------------
ycsb_a_update_heavy         1.86  1693.5±320.29µs 576.7 KElem/sec    1.34  1220.9±233.45µs 799.9 KElem/sec     1.00   908.9±14.69µs 1074.4 KElem/sec     1.58  1439.7±92.97µs 678.3 KElem/sec
ycsb_b_read_mostly          2.27   546.6±88.05µs 1786.7 KElem/sec    1.59   382.7±87.44µs  2.5 MElem/sec       1.57    376.7±4.49µs  2.5 MElem/sec       1.00    240.3±8.40µs  4.0 MElem/sec
ycsb_c_read_only            4.35   457.7±51.38µs  2.1 MElem/sec      2.58    271.0±8.41µs  3.5 MElem/sec       2.87    301.6±4.58µs  3.2 MElem/sec       1.00    105.1±0.29µs  9.1 MElem/sec
ycsb_d_read_latest          2.98   891.5±85.28µs 1095.5 KElem/sec    22.05     6.6±9.20ms 148.0 KElem/sec      19.23     5.8±5.76ms 169.7 KElem/sec      1.00    299.2±3.61µs  3.2 MElem/sec
ycsb_e_short_ranges         8.47     10.6±0.69ms 92.4 KElem/sec      313.60 391.1±132.14ms  2.5 KElem/sec      296.28 369.5±111.21ms  2.6 KElem/sec      1.00  1247.0±10.98µs 783.1 KElem/sec
ycsb_f_read_modify_write    1.00  1798.3±322.02µs 543.1 KElem/sec    641.36 1153.3±3927.61ms   867 Elem/sec    847.06 1523.2±4377.59ms   656 Elem/sec    1.10  1976.9±19.72µs 494.0 KElem/sec
```

Summary (UltimaDB Eventual vs TurboKV `durable()`, same host):

| Workload | UltimaDB | TurboKV durable | TurboKV fast (no WAL) | Ratio (TurboKV durable / UltimaDB) |
|---|--:|--:|--:|--:|
| A update-heavy | 1.44 ms | **1.22 ms** | 0.91 ms | 0.85× (TurboKV ahead) |
| B read-mostly | **240 µs** | 383 µs | 377 µs | 1.59× |
| C read-only | **105 µs** | 271 µs | 302 µs | 2.58× |
| D read-latest | **299 µs** | 6.6 ms (±9.2) | 5.8 ms (±5.8) | 22× |
| E short-ranges | **1.25 ms** | 391 ms (±132) | 370 ms (±111) | 314× |
| F read-modify-write | **1.98 ms** | 1.15 s (±3.9 s) | 1.52 s (±4.4 s) | 583× |

- TurboKV's write path is genuinely fast: on A it is ~15% ahead of UltimaDB
  Eventual and ~28% ahead of fjall. Dropping the WAL (`fast()`) buys a further
  ~25% on A and **nothing anywhere else** — the WAL is not what limits it.
- Every workload that mixes reads with sustained updates collapses: D 22×, E
  300×, F 600× slower than UltimaDB, with confidence intervals wider than the
  medians. Root cause observed during the run (not inferred): the 10 MB
  dataset's data directory grew to **2.0 GB** — 40 × 64 MiB SSTables plus a
  394 MB WAL segment — and `sstables/L0` reached **1,592 files of 18 KB**
  before a compaction swept it (1,150 → 0 in 30 s). Reads then fan out across
  hundreds of L0 files (Bloom-filter probes per file), scans across all of
  them. This is the standard LSM write-amplification / read-amplification
  trade-off with default `flush_interval` 60 s and `compaction_interval`
  30 s (turbokv `src/storage/engine.rs:188-189`) not keeping up with
  ~500 KB/s of 1 KB overwrites.
- Criterion artefact worth knowing: criterion sizes its sample plan from the
  warm-up, when TurboKV's memtable is empty and fast; the engine then slows
  underneath the plan, so workload A alone ran ~3 min vs ~15 s for the other
  engines. The medians above are therefore medians over a *degrading* run,
  not a steady state — steady state would be worse for TurboKV on D/E/F, not
  better.
- fjall vs UltimaDB reproduces the NVMe-doc pattern
  (`competitor-nvme-2026-08-02-post-task57.md`): UltimaDB ahead on B/C/D/E
  (2.3–8.5× here), fjall ahead on F (1.10×), and A flipped to UltimaDB on this
  host (1.18×; it was 1.29× behind on the NVMe rig — different host, treat as
  noise-band evidence only).

## Strict tier (fsync per commit)

`critcmp -g '(.+)/[^/]+' ultima_strict fjall_strict turbokv_strict`:

```
group                       fjall_strict//burst                     turbokv_strict//burst                  ultima_strict//burst
-----                       -------------------                     ---------------------                  --------------------
ycsb_a_update_heavy         1.36  1365.8±54.83ms   732 Elem/sec     1.40  1410.4±61.06ms   708 Elem/sec    1.00  1006.7±38.19ms   993 Elem/sec
ycsb_b_read_mostly          1.42   141.2±13.07ms  6.9 KElem/sec     1.41   140.2±12.36ms  7.0 KElem/sec    1.00    99.6±10.64ms  9.8 KElem/sec
ycsb_c_read_only            3.39    359.2±2.62µs  2.7 MElem/sec     2.46    261.0±4.55µs  3.7 MElem/sec    1.00   106.0±2.32µs  9.0 MElem/sec
ycsb_d_read_latest          1.39   139.6±15.04ms  7.0 KElem/sec     1.33   133.6±14.46ms  7.3 KElem/sec    1.00   100.2±11.01ms  9.7 KElem/sec
ycsb_e_short_ranges         2.34   237.1±29.18ms  4.1 KElem/sec     4.12   417.3±76.93ms  2.3 KElem/sec    1.00   101.3±9.00ms  9.6 KElem/sec
ycsb_f_read_modify_write    1.37  1388.2±106.82ms   720 Elem/sec    2.24       2.3±2.80s   440 Elem/sec    1.00  1013.5±38.42ms   986 Elem/sec
```

- fsync (~2 ms on this SSD; 500 writes/burst → ~1 s on A/F for every engine)
  dominates, so the write-mix cells converge: UltimaDB `standalone_fast` is
  1.33–1.42× ahead of both fjall and TurboKV on A/B/D and 1.37× / 2.24× on F.
  TurboKV `paranoid()` groups WAL syncs, which is why it is *not* worse than
  fjall on A/B/D despite the LSM churn — but E (4.1×) and F (2.2×, CI
  1.6–3.1 s) still carry the read-amplification cost.
- Strict-tier sweep intact: UltimaDB fastest on all six, same as every NVMe
  run since task38.

## On TurboKV's published claims

Their table measures **ingest-only** workloads (sequential/random fill,
overwrite, batched fill) with a harness that pins `flush_interval` and
`compaction_interval` to **1 hour** and disables the block cache
(`benchmarks/engine.rs` in their repo) — i.e. the memtable + WAL-append path
with background maintenance effectively off. On that path this run agrees with
them directionally: A, the closest YCSB analogue, is TurboKV's best cell. The
claims say nothing about mixed read/write or scan workloads, and on those,
with the engine's *default* maintenance intervals, it is one to three orders
of magnitude behind on this host. Not a like-for-like refutation of their
numbers; a statement that they do not transfer to YCSB.

## Caveats

- Local host, single run, no repeat — noise floor on this machine is
  unmeasured; treat sub-1.3× differences as unresolved.
- TurboKV was run with preset defaults (64 MiB memtable, 64 MiB block cache,
  default flush/compaction intervals). A tuning pass (smaller memtable, faster
  compaction, or their harness's 1 h intervals) might change D/E/F
  materially; not attempted.
- The 60 s flush timer means the non-durable runs span several flush cycles;
  whether a timer flush of a non-full memtable is what produced the 18 KB L0
  files was not verified.
- TurboKV requires `RUSTFLAGS="-C target-feature=+aes,+sse2"` (its `gxhash`
  dep refuses to compile otherwise), so it is behind `--features turbokv` in
  `compare_benches` and built into its own target dir; the other engines were
  built without those flags. Not expected to matter for them.

## Reproduce

```bash
export ULTIMA_BENCH_DIR=$HOME/bench-disk          # real disk, not tmpfs
# non-durable tier
ULTIMA_BENCH_DURABILITY=nondurable make bench/ycsb                              # UltimaDB Eventual
ULTIMA_BENCH_DURABILITY=nondurable make bench/ycsb/fjall
ULTIMA_BENCH_DURABILITY=nondurable make bench/ycsb/turbokv                      # durable()
ULTIMA_BENCH_DURABILITY=nondurable ULTIMA_BENCH_TURBOKV_NOWAL=1 make bench/ycsb/turbokv   # fast()
# strict tier
ULTIMA_BENCH_DURABILITY=strict ULTIMA_BENCH_INLINE=1 ULTIMA_BENCH_PREALLOC=1 make bench/ycsb
ULTIMA_BENCH_DURABILITY=strict make bench/ycsb/fjall
ULTIMA_BENCH_DURABILITY=strict make bench/ycsb/turbokv                          # paranoid()
```

Pass `-- --save-baseline <name>` through `cargo bench` and compare with
`critcmp`; note the TurboKV baselines land under `target/turbokv/criterion`
(`critcmp --target-dir target/turbokv --export <name>`).
