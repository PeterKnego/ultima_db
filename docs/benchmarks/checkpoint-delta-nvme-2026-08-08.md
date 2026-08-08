# Incremental checkpoints — crossover & recovery, bench-host results (2026-08-08)

Authoritative AWS-NVMe results for task61 (incremental/delta checkpoints via
CoW structural diff). See `docs/tasks/task61_incremental_checkpoints.md` §9
for the full write-up, the honesty framing on what these numbers do and do
not justify, and §10 for the resulting recommendation on
`StoreConfig::checkpoint_chain_max`. This doc holds the raw/derived numbers;
the task doc holds the argument.

**Host:** AWS `c6id.2xlarge` (local-NVMe instance store), 8 vCPU, 15701 MB
RAM, kernel `6.17.0-1019-aws`, `rustc 1.97.1 (8bab26f4f 2026-07-14)`.
UltimaDB at git `ffb2c58`. Provisioned + torn down via `bench-infra`
(`make bench-oneshot TARGET=checkpoint-delta`). Raw log:
`bench-infra/bench-out/dist/20260808T194921Z/checkpoint-delta.log`
(`bench-out/` is gitignored, hence this doc — see the fanout-sweep doc for
the same rationale).

Both benches run `Persistence::smr` (checkpoint-only) — there is no
`Durability` tier to sweep here; that knob belongs to `Persistence::standalone`'s
WAL, which `benches/checkpoint_delta.rs` does not exercise. **Same-host
relative comparisons only** — absolute ms figures do not port to another
machine, and no cross-machine ratio should be quoted from this doc.

---

## 1. Crossover: `checkpoint()` cost vs. dirty fraction

100k-row table (`ROWS = 100_000`, 64-byte payload/row), one seed checkpoint
to establish the chain base, then one dirty-batch + `checkpoint()` per
criterion iteration (10 samples/cell). Time is criterion's point estimate;
bracketed triple is `[lower upper]` of its 95%-CI regression fit. `dir_bytes`
is the checkpoint directory's total byte footprint at the end of each cell's
sampling loop — **informational only, not a timing** (at `chain_max=8` it
reflects whatever partial chain state criterion's iteration count happened
to leave on disk, not one file's size).

| dirty % | `chain_max=1` time | `chain_max=1` dir_bytes | `chain_max=8` time | `chain_max=8` dir_bytes | delta vs. full |
|---:|---:|---:|---:|---:|---:|
| 1%   | 32.918 ms `[32.520, 33.541]` | 8,468,870 | 4.8250 ms `[4.7254, 4.9330]` | 8,894,255  | **6.82× cheaper** |
| 10%  | 36.857 ms `[36.774, 36.960]` | 8,468,870 | 7.4969 ms `[7.3924, 7.5995]` | 13,569,310 | **4.92× cheaper** |
| 50%  | 38.272 ms `[38.170, 38.498]` | 8,468,760 | 20.298 ms `[20.097, 20.537]` | 25,468,622 | **1.89× cheaper** |
| 100% | 40.587 ms `[40.356, 40.963]` | 8,468,980 | 45.231 ms `[44.851, 45.648]` | 51,313,825 | **11.4% slower** |

The crossover — where a delta stops being cheaper than a full — sits
**between 50% and 100% dirty**; the bench sampled those two points and
nothing between, so this run bounds the crossover to that interval rather
than pinning an exact percentage. The 100% cell losing is the expected
result: an all-rows-changed delta pays the ordered diff *and* per-key
framing on top of what a full write already pays, with nothing left
unshared to skip.

## 2. Recovery time vs. chain length

20k-row table (`RECOVERY_ROWS = 20_000` — deliberately smaller than the
crossover dataset, since this measures scaling with chain length, not
dataset size), chain built from one full checkpoint plus `chain_len - 1`
deltas, each dirtying ~1% of the rows (~200 rows/delta). `Store::recover()`
wall time, point estimate, 10 samples/cell.

| chain length | `Store::recover()` time | dir_bytes | vs. chain_len=1 |
|---:|---:|---:|---:|
| 1  | 5.6261 ms `[5.6228, 5.6288]` | 1,679,548 | — |
| 2  | 5.7272 ms `[5.7140, 5.7367]` | 1,696,197 | +1.80% |
| 4  | 5.9101 ms `[5.9063, 5.9144]` | 1,729,495 | +5.05% |
| 8  | 6.2749 ms `[6.2712, 6.2778]` | 1,796,091 | +11.53% |
| 16 | 7.0257 ms `[7.0167, 7.0336]` | 1,929,283 | +24.88% |

The percentage growth looks like it accelerates, but that's an artifact of
chain length itself doubling at each row: fitting a line through the two
endpoints (`(7.0257 − 5.6261) / 15 ≈ 0.0933 ms` per extra delta file)
predicts the 2/4/8 cells within a few µs of what was measured
(5.719 / 5.906 / 6.279 ms predicted vs. 5.727 / 5.910 / 6.275 ms measured) —
recovery cost is close to **linear in the number of files replayed** at this
row count and per-delta dirty fraction, not superlinear.

## 3. What this does and does not establish

- **Established:** at ≤50% churn between checkpoints, a delta chain is
  several times cheaper in wall time than always-full, and a chain of 8–16
  costs only a low-double-digit percentage more recovery time. At 100%
  churn, a delta is a net loss.
- **Not established:** the memory cost of holding a chain open.
  `checkpoint()` retains the last-checkpointed snapshot as the diff base for
  the next call (`docs/tasks/task61_incremental_checkpoints.md` §6), which
  keeps that base's unshared B-tree nodes alive for as long as the chain is
  open — a cost this bench does not measure at all (it only times wall
  clock). See the task doc §10 for why that gap means the default stays at
  `checkpoint_chain_max = 1` despite the favorable numbers above.
