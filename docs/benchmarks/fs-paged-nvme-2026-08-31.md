# fs-paged on the NVMe bench host — matrix + lever validation (2026-08-31)

> **CORRECTION (2026-09-02, task64 §13.4).** `fs_paged_levers.sh` passed
> no `--workload`, so every lever cell below ran `paging_matrix`'s default
> **workload C (read-only zipf)**, not the pressured A this doc's headline
> names (`data-fs-paged-nvme-levers-2026-08-31.jsonl`: `workload: "C"` on
> all four rows). Consequences: (1) "237 -> 13,343 (~56x) from retention
> 10 -> 1" compares A@10 against C@1; the matrix's own C/zipf/eventual
> cell at retention 10 was **13,664** ops/s (0.859 majflt/op) vs the
> lever's 13,343 (0.852) — **retention had no measurable effect**, which
> agrees with the local spike's F5 probe on A. (2) "29.4k, 2.2x ReDB on
> the same cells" compares T=8 on C against ReDB's A cell; ReDB's C cell
> was 28.4k, so T=8 is ~parity on C. (3) What this run *did* validate:
> on pressured read-only C, `fanout-t8` roughly doubles throughput
> (13.3k -> 29.4k, 0.85 -> 0.10 majflt/op), mimalloc is a no-op, and
> reopen is negative. **Nothing in this run measured a write-path
> lever.** Points 1 and 5 under "What the NVMe host changes" are
> retracted; the script now defaults to `--workload=A`.

Provenance: c6id.2xlarge (8 vCPU, 15.7 GB, local instance-store NVMe at
/opt/bench), kernel 7.0.0-1011-aws, rustc 1.98.0, tree b4aa1db, one
`bench-oneshot TARGET=fs-paged` run (n=1 per cell — ordering-grade).
16 G swapfile + vm.swappiness=60 set for this target (host default is
swapless/swappiness=0, which would OOM-kill instead of degrade).
Data: `data-fs-paged-nvme-{matrix,levers}-2026-08-31.jsonl`.

## Matrix (5M rows insert-loaded, zipf, 197 MiB cgroup, both durability tiers)

ultima-paged ran at OUT-OF-BOX defaults (snapshots_retained=10, budget 64 MiB):

| A/eventual | ultima-paged | redb | rocksdb* | fjall |
|---|---|---|---|---|
| ops/s | ~237 | 13.5k | 74.3k | 46.0k |

(C reads: 13.7k / 28.4k / 56.1k / 23.2k. Full tables in the collected md.
*rocksdb's compressed footprint largely fits the budget — RAM-assisted.)

## Lever cells (same host+session, --snapshots-retained=1, --restart)

| cell | run1 ops/s | majflt/op | run2 (after reopen) |
|---|---|---|---|
| glibc T=32 | 13,343 | 0.852 | 8,961 |
| glibc T=8 | **29,432** | 0.097 | 21,615 |
| mimalloc T=32 | 10,988 | 0.969 | 9,297 |
| mimalloc T=8 | 30,525 | 0.117 | 17,256 |

## What the NVMe host changes vs the local spike

1. **Snapshot retention is THE dominant lever here**: default retention 10
   vs 1 is 237 -> 13,343 ops/s (~56x) at identical everything else. The F5
   mechanism (older retained snapshots CoW-pin the pre-demotion tree,
   invisible to the budget) — *refuted as the holder on the local box,
   where heap fragmentation kept the memory live regardless* — dominates on
   this host/kernel. Cross-box mechanism difference (local: fragmentation
   masks retention; NVMe host: retention is the live set) is real and
   unexplained in detail; both point at the same spec requirement.
2. **fanout-t8 compounds everywhere**: +2.2x on NVMe (13.3k -> 29.4k),
   +3.2x locally. The one lever that replicates.
3. **mimalloc: no effect on NVMe** (within noise both fanouts) — its 2.1x
   local win was a box artifact (glibc/kernel swap-path specific). Not a
   general recommendation.
4. **Ingest-then-serve reopen: NEGATIVE on NVMe** (run2 is 20-45% slower
   than run1 — recovery starts fully cold and the 60s window never
   re-warms). The local 3.8-4.3x reopen win does not generalize. Dropped
   from guidance.
5. **Headline**: best validated config (T=8, retention 1) sustains
   29.4k ops/s on pressured zipf A — 2.2x ReDB on the same cells — while
   out-of-box defaults sit at 237. The gap between those two numbers is
   configuration, not code: retention must join the memory-budget story
   (spec input for the stage-3 memory-honesty slice — pinned snapshot
   bytes must either be charged to the budget or adaptively gc'd).
