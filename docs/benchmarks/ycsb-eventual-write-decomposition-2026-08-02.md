# Where the eventual-tier YCSB update op spends its time — 2026-08-02

Timing decomposition of the one-op-per-txn eventual-durability update (the
regime where UltimaDB trails Fjall 1.38–1.60× on YCSB A/F,
`competitor-nvme-2026-08-02.md`), first on the sandbox and then validated
same-day on a bench-infra c6id.2xlarge (local NVMe, 8 vcpu, idle; results
`bench-infra/bench-out/dist/20260802T181146Z-spin-ab/`, ultima git
`78ef9ec`). Harnesses: `examples/perf_decomp.rs` (component cells),
`examples/wal_spin_ab.rs` (paired A/Bs + noise floor),
`examples/wal_commit_micro.rs` (BenchWal committer-side isolation).

## Decomposition — NVMe host (authoritative; sandbox shares agreed)

Total eventual update: **7528 ns/op** (p10 7400, p90 7826 — tight).

| Component | ns/op | Share | How measured |
|---|--:|--:|---|
| WAL committer-side work | **3883** | **52%** | `store_eventual − store_none`; `wal_commit_micro` reproduces ~2.9 µs net of record build |
| MVCC path-clone tax | **1975** | **26%** | `table_warm − table_cold` (O(1) table clone refreshed per op) |
| Record construction (harness) | 731 | 10% | `YcsbRecord::new` alone — all engines pay an equivalent |
| Txn+commit bookkeeping | 529 | 7% | empty `begin_write/open_table/commit` |
| Tree op floor (cold, in-place) | 246 | 3% | raw `Table::update`, uniquely owned tree |
| Store install residual | 164 | 2% | remainder |

Within the WAL bucket: bincode serialize of the ~1 KB record is only
**~560 ns**; the remaining ~2.4–2.9 µs is *not* explained by serialization.

## Hypotheses tested and REFUTED on the NVMe host

1. **WAL recv-spin** (bg thread busy-polls before parking so serial commits
   skip the futex wake — motivated by an isolated mpsc microbench where a
   send to a parked receiver costs 1.5–2.4 µs): paired same-process A/B
   **+0.8/+1.2/+1.3%** across three reps (slightly worse), and end-to-end
   criterion cells (2 interleaved reps): **YCSB A +4–7% worse, F ±2%**.
   The spin was removed. Explanation: in the real store the WAL thread is
   busy in `append+fsync` when the next commit arrives, so sends rarely hit
   a parked receiver — the wake microbench does not model the system.
2. **`CoalescedPrealloc` under Eventual** (cheaper bg fsync):
   **+6.8/−0.9/+3.6%** — null. The committer never waits on the bg thread in
   Eventual mode, and bg-side fsync cost does not backpressure the commit
   path at this rate.
3. (Sandbox-only footnote: both A/Bs were first run on a loaded 4-core
   sandbox and were unresolvable there; the NVMe reps came with a same-run
   noise floor of ~±1–2% and are decisive.)

## Conclusions

- The eventual-tier write gap vs Fjall (~2.5 µs/update on the NVMe cells) is
  ≈ fully explained by **(a)** the per-commit WAL handoff (~3.9 µs on NVMe,
  committer-side) and **(b)** the per-txn CoW path-clone tax (~2 µs). Fjall
  pays neither: its eventual write is a memtable insert plus an unsynced log
  append, with structure maintenance amortized into flushes.
- B-tree fanout tuning is confirmed to be the wrong lever for this regime
  (see `btree-fanout-t-sweep-2026-07-09.md`): the tree op floor is 3% of the
  op; T changes the width coefficient of tax (b), and moves nothing in (a).
- **Open question — the ~2.4 µs unexplained WAL-bucket remainder.** Leading
  hypothesis: cross-thread allocator churn. Every op allocates the payload
  chain on the committer (record `encode_to_vec` Vec ≈1 KB, key `Vec`,
  `WalOp.table` String, `WalEntry.ops` Vec, mpsc node) and the WAL thread
  frees all of it after the write — the same cross-thread free pattern that
  made the snapshot-reclaim thread a 3.1× p99 regression in the fanout
  study. Also plausible: commit-path submission bookkeeping that only runs
  with a WAL configured.
- Measured dead ends (do not retry): recv-spin before park (harmless-looking,
  measurably negative); prealloc-under-Eventual as a committer-side lever;
  judging either on a loaded ≤4-core sandbox.

## Allocator A/B — hypothesis CONFIRMED (same-day follow-up)

Second c6id.2xlarge run (`dist/20260802T192105Z-alloc-ab/`, ultima git
`7d86f56`): identical binaries except the global allocator in the *bench
executables* (`bench-mimalloc` feature — the library never sets one).
Interleaved arms, 2 e2e criterion reps + 3 decomposition reps each.

End-to-end eventual tier: **YCSB A 3.84/3.76 → 2.74/2.80 ms (−27%), YCSB F
3.55/3.55 → 2.52/2.50 ms (−29%)** — criterion's change detection confirms in
both directions across the interleaving, p = 0.00.

Decomposition localizes it exactly where predicted: the WAL bucket falls
**3.4–4.2 µs → 1.2–1.9 µs** (the entire "unexplained remainder" was
cross-thread allocator churn — payload chain allocated on the committer,
freed on the WAL thread, glibc malloc's worst pattern), while the MVCC tax
(~2.0 vs ~2.15 µs) and txn bookkeeping (~520 ns) are allocator-insensitive.
Total update op 7.1–7.8 → 5.3–6.0 µs. (Curiosity, unexplained: the cold-tree
floor cell reads ~250 ns under glibc but ~590 ns under mimalloc — small
either way.)

For context, Fjall's eventual A/F cells sat at 2.7/3.0 ms in
`competitor-nvme-2026-08-02.md` — an ultima-with-mimalloc A cell of ~2.74 ms
suggests the gap ≈ closes, but that claim needs a same-host competitor rerun
(and a decision on whether competitors get the same allocator) before it goes
in a comparison doc.

Follow-ups this opens, in order:
1. **Ship the fix without shipping an allocator**: pool/recycle the WAL
   payload buffers back to the committer (bounded ring of `Vec<u8>`s over a
   return channel), so the allocation/free stays thread-local under glibc
   too. The A/B bounds the prize at ~2–2.5 µs/op.
2. Document the mimalloc option for embedders (application-level
   `#[global_allocator]` — they own `main`, we don't).
3. Re-run the competitor matrix once either lands; then the write overlay
   (mini-memtable) remains the structural attack on the remaining MVCC tax.
