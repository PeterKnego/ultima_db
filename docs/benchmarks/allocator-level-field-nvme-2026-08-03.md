# Allocator level field: eventual-tier A/F under glibc vs mimalloc-for-everyone — 2026-08-03

Follow-up to task57: does Fjall's eventual-tier write lead survive when every
engine gets the same allocator? Same harness/metric as the competitor docs
(criterion median ms per 1,000-op burst, 10k records, Zipfian, each op its
own transaction), eventual tier only, workloads A and F only (the contested
cells). ReDB skipped — its 15–19 ms A/F cells are structurally out of
contention.

## Method

One binary per engine, built once (glibc); the mimalloc arm injects the
allocator at runtime via `LD_PRELOAD=libmimalloc.so`, which overrides
`malloc` for Rust *and* C++ — the only mechanism that reaches RocksDB's
native side (a Rust `#[global_allocator]` does not). Preload verified via
the mimalloc process-init banner (see `provenance.txt`). Arms interleaved
×2 reps.

## Provenance

- 2026-08-03, bench-infra c6id.2xlarge local NVMe; results
  `bench-infra/bench-out/dist/20260803T060755Z-mi-everyone/`; ultima_db git
  `41bcf2a` (main, post-task57). Cross-host caveat as always: compare
  within-run ratios, not absolutes against other docs.

## Results (median ms, average of 2 interleaved reps)

| Engine | A glibc | A mimalloc | Δ | F glibc | F mimalloc | Δ |
|---|--:|--:|--:|--:|--:|--:|
| **UltimaDB** | 3.45 | 2.88 | **−17%** | 3.40 | 2.38 | **−30%** |
| Fjall | 2.96 | 2.52 | −15% | 3.35 | 2.80 | −16% |
| RocksDB | 3.58 | 3.30 | −8% | 4.05 | 3.64 | −10% |

Standings:

- **glibc (shipped configs)**: A — Fjall 1.17× ahead of UltimaDB; F —
  statistical tie (3.35 vs 3.40; yesterday's host read 1.20×, so the F gap
  is now within cross-host variance of parity). RocksDB last on both.
- **mimalloc for everyone**: A — Fjall 1.14× ahead (2.52 vs 2.88); **F —
  UltimaDB wins outright, 1.18× ahead of Fjall (2.38 vs 2.80)** and 1.53×
  ahead of RocksDB.

## Reading

- Every engine benefits from mimalloc, so the pre-task57 27–29% ultima-only
  delta was *partly* a level-field artifact — but the gains are uneven:
  UltimaDB −17/−30%, Fjall −15/−16%, RocksDB −8/−10%. UltimaDB's F gain is
  ~2× Fjall's, which is what flips that cell.
- Why the asymmetry: post-task57, UltimaDB's residual allocator sensitivity
  is same-thread churn on the RMW-heavy path (per-op `WalOp` chain + record
  clone on read-modify-write); Fjall's LSM path amortizes structure
  maintenance and RocksDB manages much of its memory in internal arenas
  that bypass malloc.
- Fair-comparison stance: the official competitor docs stay on system
  allocators (as-shipped). This doc records that under a level allocator
  field the eventual-tier score is **UltimaDB 5, Fjall 1** (B/C/D/E/F vs A)
  — with A at 1.14×, the last Fjall-held cell, and the write-overlay design
  (the ~2 µs MVCC path-clone tax) the remaining lever aimed at it.

## Reproduce

One-off script (not a bench-infra target): provision via `bench-infra`,
then run `nvme_mi_everyone.sh` — archived alongside the results in the
dist directory.
