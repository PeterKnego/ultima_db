# Where the eventual-tier YCSB update op spends its time — 2026-08-02

Timing decomposition of the one-op-per-txn eventual-durability update (the
regime where UltimaDB trails Fjall 1.38–1.60× on YCSB A/F,
`competitor-nvme-2026-08-02.md`). **All numbers are sandbox (direction-only,
per the local-vs-remote rule); shares were stable across repeated runs even
as absolutes moved with machine load.** Harnesses: `examples/perf_decomp.rs`
(component cells), `examples/wal_spin_ab.rs` (paired A/Bs),
`examples/wal_commit_micro.rs` (BenchWal committer-side isolation).

## Decomposition (update op ≈ 5–7 µs/op total, ultima git f8caa46 + spin knob)

| Component | ns/op (typ.) | Share | How measured |
|---|--:|--:|---|
| WAL committer-side work | ~2000 | ~40% | `store_eventual − store_none`; reproduced in isolation by `wal_commit_micro` |
| MVCC path-clone tax | ~1200–1700 | ~25–33% | `table_warm − table_cold` (O(1) table clone refreshed per op) |
| Record construction (harness) | ~400–900 | ~10–15% | `YcsbRecord::new` alone — all engines pay an equivalent |
| Txn+commit bookkeeping | ~400 | ~8% | empty `begin_write/open_table/commit` |
| Tree op floor (cold, in-place) | ~200–275 | ~4% | raw `Table::update`, uniquely owned tree |
| Store install residual | noisy 75–1000 | — | remainder; within-run noise dominates it |

Sub-decomposition of the WAL bucket:

- bincode serialize of the ~1 KB record: **~250 ns** — small.
- send to a **parked** mpsc receiver: **~0.5–1.5 µs** (run-dependent) — the
  futex wake, the largest single suspect.
- remainder ~0.5 µs: per-op allocs (record `encode_to_vec` Vec, key `Vec`,
  `WalOp.table` String clone, entry/channel node churn).

## Hypotheses tested and killed (locally)

1. **WAL recv-spin** (bg thread busy-polls before parking, so serial commits
   never pay the wake): paired same-process A/B measured **+2% (noise)**.
   *Caveat:* the sandbox had 4 cores at load ≈ 2 — a spinning receiver steals
   CPU from the committer, which can cancel exactly the effect being measured.
   The lever is implemented behind `ULTIMA_WAL_RECV_SPIN_US` (default 0 = old
   behavior) and **needs a quiet 8-vcpu NVMe A/B to be judged**.
2. **`CoalescedPrealloc` under Eventual** (cheaper bg fsync): paired A/B
   **+5.6% (noise)**. Consistent with the committer never waiting on the bg
   thread in Eventual mode — bg-side fsync cost does not backpressure
   the commit path at this rate.

## Conclusions

- The eventual-tier write gap vs Fjall (~2.5 µs/update on the 07/18–08/02
  NVMe cells) is ≈ fully explained by **(a)** the per-commit WAL handoff
  (~2 µs, committer-side) and **(b)** the per-txn CoW path-clone tax
  (~1.2–1.7 µs). Fjall pays neither: its eventual write is a memtable insert
  plus an unsynced log append, with structure maintenance amortized into
  flushes.
- B-tree fanout tuning is confirmed to be the wrong lever for this regime
  (see `btree-fanout-t-sweep-2026-07-09.md`): it changes the *width
  coefficient* of tax (b) but not its per-transaction nature, and moves
  nothing in bucket (a).
- Next levers, in expected-value order:
  1. **NVMe A/B of the recv-spin knob** (this branch; 3 arms on one host:
     main, branch spin=0, branch spin=30). If the wake is ~1 µs there too,
     that alone is ~15–20% of the update op.
  2. **Alloc-slimming the per-op WAL path** (reuse an encode buffer across
     ops; slim `WalOp` churn). Bounded by ~0.5 µs/op; only fleet-measurable.
  3. **A mutable write overlay in front of the B-tree** (mini-memtable):
     the structural fix for tax (b) and the only path that beats — rather
     than approaches — the LSM write cost profile. Design work; snapshots
     hold (frozen overlay, tree root) pairs; merge via the existing
     `BulkBuilder`/`extend_from_sorted` primitives.
- Measured dead end (do not retry locally): judging spin/prealloc effects on
  a loaded ≤4-core sandbox — both A/Bs are core-starved there.
