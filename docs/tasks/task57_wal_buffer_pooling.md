# Task 57: WAL frame-at-commit + buffer pooling

## Problem

The eventual-tier YCSB decomposition
(`docs/benchmarks/ycsb-eventual-write-decomposition-2026-08-02.md`) showed
the one-op-per-txn eventual update spending **52%** of its 7.5 µs in
committer-side WAL work, of which only ~560 ns was serialization. A
build-level allocator A/B (mimalloc, `bench-mimalloc` feature) recovered
**27–29% of the whole YCSB A/F cells**, localizing the loss: every commit
allocated its payload chain (record bytes ≈1 KB, encoded key, table-name
String, `WalEntry.ops` Vec) on the committing thread and freed it on the WAL
thread — a cross-thread pattern glibc malloc handles poorly. A second cost
hid in the same path: every sink's `append` re-serialized the `WalEntry`
(`frame_entry`) on the WAL thread, a full second encode pass.

## Design

Two changes, wire-format identical (recovery and the v2 entry format are
untouched):

1. **Frame at commit (`FramedEntry`).** `WalHandle::write` serializes the
   entry into its on-disk `[len][payload][crc]` frame *on the committing
   thread* (`frame_entry_into`, one pass, no intermediate payload Vec) and
   sends `FramedEntry { version, bytes }` through the channel. Sinks now
   just write bytes — `FileSink` `write_all`s, the coalescing/prealloc/
   io-uring sinks `extend_from_slice` their batch buffer. The `WalEntry`
   (and its whole ops chain) drops at the end of `write()`, on the
   committer — those frees never cross threads anymore. The WAL thread's
   second serialization pass is gone as a bonus.
2. **`BufPool` — recycle the framed buffer.** The one allocation that still
   crossed threads (the framed bytes) is pooled: the WAL thread returns each
   batch's buffers to a bounded pool (`BUF_POOL_CAP = 64`) after the batch
   is written and before the durability watermark is published (so a
   committer that observes durability also sees the pool refilled —
   deterministic tests). `write()` takes from the pool; `frame_entry_into`
   clears while keeping capacity. Buffers over `BUF_POOL_MAX_RETAIN = 1 MiB`
   are dropped, so a jumbo entry (bulk-load marker, huge record) cannot pin
   memory. The `ConsistentInline` path recycles through the same pool from
   `SyncWaiter::wait()` — there the win is just allocation reuse (its
   alloc/free were already same-thread).

Steady state on the serial-commit path is **zero allocations for WAL bytes**
(the pooled buffer cycles committer → WAL thread → pool → committer), and
the only allocation that crosses threads at all is the mpsc channel node.

## What deliberately did NOT change

- Wire format: `frame_entry_into` produces byte-identical frames (asserted
  by `frame_entry_into_matches_frame_entry_and_reuses_the_buffer`).
- `WalHandle::write(WalEntry)` signature — store.rs commit paths untouched.
- Read/recovery path (`read_wal`, `scan_wal`, `deserialize_entry`).
- Per-op `WalOp` construction in `TableWriter` — those allocs are now freed
  same-thread at commit, which the mimalloc A/B showed is the cheap case;
  slimming them further was measured at <0.5 µs and deferred.

## Tests

- `frame_entry_into_matches_frame_entry_and_reuses_the_buffer` — framing
  equivalence + capacity reuse.
- `eventual_write_recycles_framed_buffers_through_the_pool` — write →
  `durability().wait()` → pool holds 1; second write reuses it (still 1).
- `inline_write_recycles_framed_buffers_through_the_pool` — same via the
  driven `InlineSync` waiter.
- Full suites green in `persistence`, `persistence+bench-internals`, and
  `persistence+bench-internals+wal-iouring` configs (780 tests).

## Validation — fleet A/B 2026-08-02 (MEASURED)

Three arms on one c6id.2xlarge (`bench-infra/bench-out/dist/
20260802T195738Z-pool-ab/`; branch `f5132a5`, main `782efd9`; interleaved
criterion reps ×2, eventual tier; medians):

| Arm | YCSB A | YCSB F |
|---|--:|--:|
| main, glibc | 4.01 / 3.93 ms | 3.77 / 3.60 ms |
| **this branch, glibc** | **3.64 / 3.54 ms (−9%)** | **3.26 / 2.90 ms (−16%)** |
| this branch + mimalloc | 2.93 / 2.94 ms (−26%) | 2.47 / 2.54 ms (−32%) |

The branch decomposition on the same host shows the WAL bucket halved
(3.4–4.2 µs on main-glibc → **1.9–2.0 µs**), total eventual update
7.1–7.8 → **5.5–5.6 µs**, with unprecedentedly tight percentiles.

Reading: pooling recovers the *cross-thread* share under the shipped
allocator (−9%/−16% e2e), but mimalloc still adds ~18% on top — the residual
is generic allocator throughput on the remaining same-thread churn (per-op
`WalOp` chain, record encode, snapshot bookkeeping), not cross-thread frees.
So: this change ships the structural fix; embedders who also swap in
mimalloc get the full −26%/−32% vs pre-task57 main. For context, Fjall's
same-day eventual cells were A 2.7 / F 3.0 ms (different host instance —
ratios only): branch+mimalloc F now reads *ahead* of Fjall's F and A reads
~even; claiming that requires a same-host competitor rerun.
