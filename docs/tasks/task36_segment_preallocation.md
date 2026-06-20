# Task 36 — Journal Segment Preallocation

**Status:** SHIPPED + merged (ultima_db main; `ultima-journal`). Cloud A/B DONE (3 platforms, 2026-06-20). **Enabled by default in `ultima_cluster`** (uc_node `UC_JOURNAL_PREALLOC` defaults on; PR #2). Library `JournalConfig::new()` stays opt-in default-off; the cluster opts in. See §7.

**Design spec:** `docs/superpowers/specs/2026-06-19-segment-preallocation-design.md`
**Implementation plan:** `docs/superpowers/plans/2026-06-19-segment-preallocation.md`

## 1. Problem

The journal's per-commit durability barrier (`Durability::Consistent`) dominates single-commit latency. A microbench on an ext4 dev host decomposed it:

| barrier | p50 | p99 |
|---|---|---|
| `write_only` (page-cache write, no fsync) | ~5–26 µs | ~70–156 µs |
| `fsync_only` (append **extends EOF**, then `fdatasync`) | ~600 µs | ~955 µs |
| `fsync_prealloc` (pre-written full segment, **no EOF move**) | ~210 µs | ~306 µs |
| **delta = ext4 metadata-commit cost** | **−65%** | **−68%** |

Mechanism: a size-extending append forces `fdatasync` to flush the new `i_size`, which on ext4 (data=ordered) triggers a **jbd2 journal-transaction commit** — a second barrier stacked on the raw device flush. Decomposition ≈ ~390 µs jbd2 metadata commit + ~210 µs device flush.

Preallocating a segment (physically writing its blocks, marking extents *written*) before records land lets appends overwrite already-written extents, so `fdatasync` carries no `i_size`/extent-map change and degenerates to a pure data barrier. This is the etcd WAL preallocation technique.

**Scope of the win is latency, not throughput.** Group commit already amortizes `fdatasync` across a batch (~4% of the throughput path; poll-sleep IPC dominates there — see task13/35). This targets single-commit tail latency, which is ~95% `append_consistent` p99 (~1.1 ms).

## 2. Design (etcd `filePipeline` analog)

- **`JournalConfig.preallocate_segments: bool`, default `false`** — opt-in. Flag-off path is byte-identical; existing consumers are unaffected until they opt in.
- **`SegmentPipeline`** (`src/journal/segment_pipeline.rs`) — a background helper thread that keeps **one** preallocated temp segment ready (`seg-prealloc.{n}.tmp`): zero-fill to `segment_size` (real writes, 1 MiB chunks) + `sync_all`, off the commit path. `spawn` pre-warms the first temp; `take_ready` hands one off and triggers the next; `shutdown` (idempotent, also via `Drop`) stops the thread and removes the unconsumed temp.
- **Activation** (`writer.rs` `write_batch` rotation): when the pipeline is active, rotation takes a ready temp, writes the real header (overwriting the already-allocated first block — `sync_data`, no `i_size` change), atomically `rename`s it to `seg-{seq}.log`, fsyncs the dir, and signals the pipeline to prepare the next. The only per-rotation cost on the lock-held commit path is one header write + `sync_data` + rename + dir fsync — one metadata commit per ~64 MB of log, versus the per-commit metadata commit eliminated. Disabled path keeps `SegmentFile::create`.

## 3. The load-bearing recovery change

A preallocated segment is `[header][records][zeros…→segment_size]`. Three coupled changes:

1. **`decode_record`: a zero `u32` length-prefix is a torn tail, not corruption** (`segment.rs`). Previously, a zero `body_len` with trailing bytes returned `Err(Corrupted)` — so a preallocated segment failed recovery. Now it returns `Ok(None)` (end-of-records). **This change is unconditional**, aligning recovery with the wire format's documented `len = 0 → unwritten` semantics that the lock-free live reader already honors (length written atomically after the body). **Trade-off (accepted):** a corruption that zeroes a *valid* record's length prefix mid-log now reads as end-of-log rather than being flagged — but group commit writes records in order (one coalesced `write_all` + one `fdatasync` per batch), so a zeroed-prefix-with-valid-data-after cannot arise from a torn write, and CRC still guards every present record's bytes. A guard test keeps a *non-zero* sub-minimum `body_len` erroring `Corrupted`.

2. **Recovery preserves the preallocated tail** (flag-gated, `mod.rs` `Journal::open`). With the flag on and `had_torn_tail`, recovery calls `SegmentFile::reset_cursor(last_durable_offset)` (rewind the logical cursor, NO file I/O) instead of physically `truncate`-ing — so the writer resumes appending into preallocated space with no truncate-then-refill churn. Flag-off keeps today's physical `truncate`. `truncate_after`'s genuine logical truncation (removing committed records) stays physical `truncate` — only the torn-tail/zero-tail path changed. (The refactor also fixed a pre-existing double-`scan` in the recovery torn-tail path.)

3. **Re-preallocate the active segment at open** (flag-gated) — if the active segment's `physical_len() < segment_size`, zero-fill its tail back up (one-time, open-time). Covers a segment written before the flag was enabled or one a prior recovery physically truncated, restoring steady-state preallocation immediately after restart.

**Orphan cleanup:** `Journal::open` unlinks any `seg-prealloc.*.tmp` (a crash between temp-create and activation rename). The predicate (`seg-prealloc.` prefix + `.tmp` suffix) cannot match real `seg-*.log` segments.

**No truncate-on-close** (deliberate): the active segment stays physically `segment_size` after `close`, so reopen resumes into preallocated space with no re-fill. Idle/closed journals therefore hold `≥ segment_size`; fine for continuously-appended, periodically-purged Raft logs.

## 4. Test coverage

`ultima-journal` unit + integration (`--lib`, 110 tests green; also green under `--features bench-support`):
- `decode_record` zero-prefix → `Ok(None)`; non-zero bad length still `Corrupted`.
- `preallocate_to`/`reset_cursor`/`physical_len`; temp create→activate round-trip (tail preserved, header written, append works).
- `SegmentPipeline` hand-off + shutdown-removes-temp.
- `preallocated_journal_rotates_and_recovers_full_range` (forces ≥2 segments via 512 B × 200 records into 64 KiB segments).
- `open_removes_orphan_prealloc_temps`; `reopen_preallocated_active_segment_keeps_zero_tail` (asserts physical EOF == segment_size after reopen).
- `consistent_durability_survives_reopen_preallocated`; `preallocated_resume_overwrites_zero_tail_correctly` (reopen → append into the former zero tail → spot-read survives a third reopen — exercises the `reset_cursor` resume path).

**Cluster SMR gates (run from `../ultima_cluster` with the journal flag ON via a temporary `log_storage.rs` edit, reverted after):**
- Lincheck capstone `uc_node --test lin_register`: 3/3 (`fault_roundtrip_keeps_serving`, `smoke_3node_submit_read`, `linearizable_under_failover`).
- Hard-crash `uc-crashtest --features hard-crash-tests`: `linearizable_under_hard_crash` + cross-process smoke, both pass.

Preallocation does not perturb SMR correctness under failover or kill-9-mid-load.

## 5. Microbench witness

The `fsync_prealloc_*` autobench variant (`autobench/src/journal_bench.rs`; the bench helper delegates to the production `preallocate_to`, so it exercises the real path) reproduces the win. Re-run during this task on a noisier host: `fsync_only` p50 1094 µs / p99 1461 µs vs `fsync_prealloc` p50 405 µs / p99 698 µs (−63% / −52%). Absolute numbers move with host load; the relative cut is stable.

## 6. Cross-repo wiring (pending, NOT done in this task)

`ultima_cluster` constructs `JournalConfig { … }` as a struct literal at `uc_node/src/raft/log_storage.rs`. DONE: it now sets `preallocate_segments: journal_prealloc_from_env()`, reading `UC_JOURNAL_PREALLOC` — **default ON** post-A/B (set `=0` to roll back). The bench fleet threads it via the `uc_journal_prealloc` ansible group var (default 1). A/B cleanly flips it on the same binary. (Earlier interim state was default-OFF; flipped to default-on when the cloud A/B landed — §7.)

## 7. Cloud A/B — DONE (2026-06-20, 3 platforms) → enabled by default

Ran the interleaved A/B (Consistent, rate 200/s, inflight 8; `UC_JOURNAL_PREALLOC=0` vs `1` on the same binary) on three fleets via the `runtime-stats` instrument (`RAFT_RUNTIME_STATS` scraped from node0). `submitted→persisted` (leader journal append+fsync), prealloc OFF→ON:

| platform | storage | fsync floor (P1, off→on) | P50 | body P1–P10 | P99 | end-to-end |
|---|---|---|---|---|---|---|
| Hetzner ccx13 | local NVMe | ~876µs | 1531→1095 (**−28%**) | −~50% | flat | NULL |
| AWS c7i.4xlarge | **EBS (network)** | 2566→2566µs | 2910→2866 (−1.5%) | ~0% | flat | NULL |
| AWS c6id.4xlarge | **local NVMe** | 155→75µs | 261→194 (**−26%**) | **−43..53%** | flat (~6.5ms) | NULL |

**Mechanism confirmed (not inferred):** on local NVMe the per-commit fsync floor *halves* (155→75µs on c6id) — the ext4 jbd2 metadata commit preallocation removes is ~half a fast-NVMe fsync. On EBS the ~2.5ms network-flush floor swamps it (identical floor on/off → NULL). The win is real and reproducible on two independent local-NVMe platforms (−26..28% P50, −~50% body), NULL on network storage.

**But it does not reach end-to-end** on any platform: commit latency is unchanged (5ms `api_batch_linger` + replication dominate — structurally the same NULL as the fdatasync A/B, task13 §16). The P99 *tail* of the journal stage is also flat (batching/scheduling-bound at this rate, not jbd2-bound).

**Decision: enabled by default** (maintainer call, 2026-06-20). The strict gate (submitted→persisted P99 + an end-to-end effect) was *not* met, but the journal-median win is real, free, correctness-proven (lincheck + hard-crash green with prealloc on), and shows **no regression on any storage** — so default-on with `UC_JOURNAL_PREALLOC=0` rollback. Caveat: on network storage (EBS) the win is nil and the background 64 MB zero-fill is wasted I/O (off the commit path, so harmless to latency); set `UC_JOURNAL_PREALLOC=0` for EBS-backed deployments if the zero-fill I/O is unwanted.

## 8. Future extension (out of scope): O_DIRECT / O_DSYNC

Compatible but marginal. Preallocation (block-allocated written extents) is a clean substrate for O_DIRECT, and O_DSYNC composes with the existing one-`write_all`-per-batch group commit. But preallocation already removes the ~390 µs metadata commit; O_DIRECT only removes the page-cache copy (~5–26 µs) and one syscall, not the ~210 µs device flush. Its real cost is block-alignment of the variable-length record framing — an on-disk-format change independent of preallocation. Would land behind its own flag.
