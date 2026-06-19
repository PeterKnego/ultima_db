# Journal Segment Preallocation — Design

**Date:** 2026-06-19
**Repo of record for code:** `ultima_db/ultima_journal` (consumed by `ultima_cluster` via the `../ultima_db/ultima_journal` path dep)
**Status:** design approved, pending implementation plan

## 1. Problem & Motivation

The journal's per-commit durability barrier (`Durability::Consistent`) is the
dominant cost of single-commit latency. A microbench on this dev VM (ext4 on
`/`, 400 samples) decomposed it:

| barrier | p50 | p99 |
|---|---|---|
| `write_only` (page-cache write, no fsync) | ~5–26 µs | ~156 µs |
| `fsync_only` (append **extends EOF**, then `fdatasync`) | **599.6 µs** | **954.6 µs** |
| `fsync_prealloc` (pre-written full segment, **no EOF move**) | **210.2 µs** | **305.9 µs** |
| **delta = ext4 metadata-commit cost** | **−389 µs (−65%)** | **−649 µs (−68%)** |

Mechanism: a size-extending append forces `fdatasync` to flush the new `i_size`,
which on ext4 (data=ordered) triggers a **jbd2 journal-transaction commit** — a
second barrier stacked on the raw device flush. The decomposition is roughly
~390 µs jbd2 metadata commit + ~210 µs device flush.

If the segment is **preallocated** (its blocks physically written, extents marked
*written*) before records land, appends overwrite already-written extents and
`fdatasync` carries no `i_size`/extent-map change — degenerating to a pure data
barrier. This is the etcd WAL preallocation technique. The microbench above
confirms the ~3× cut in the durability barrier; the barrier is ~95% of
`append_consistent` p99 (~1.1 ms), so this is the largest known lever on
per-commit latency.

**Scope of the win:** latency, not throughput. Group commit already amortizes
`fdatasync` across a batch (~4% of the throughput path; poll-sleep IPC dominates
there). This change targets single-commit tail latency — what `append_consistent`
p99 measures and what a low-batch / interactive Raft commit pays. It will be
validated by an operator cloud A/B on prod NVMe before becoming a default.

## 2. Goals / Non-Goals

**Goals**
- Eliminate the per-commit metadata-commit cost by preallocating segments.
- Add the cost **off the commit path** — no rotation stall (a multi-ms stall can
  trip Raft election timeouts; it would be self-defeating for a latency win).
- Preserve all existing durability and crash-recovery guarantees.
- Opt-in, zero behavior change when disabled.

**Non-Goals (YAGNI)**
- O_DIRECT / O_DSYNC (see §8, recorded as a future extension only).
- Configurable look-ahead depth (>1 segment).
- Per-durability-mode gating.
- Truncate-on-close (see §6).

## 3. Configuration

`JournalConfig.preallocate_segments: bool`, **default `false`**.

- Rollout is safe: existing behavior is byte-identical until a consumer opts in.
- The cloud A/B flips the flag on the same binary (no two-binary build).
- Default flips to `true` only after the A/B confirms the win on prod NVMe.
- Default-off also keeps tests / tmpfs from pointlessly zero-filling.

The preallocation size **is** `segment_size` (preallocate the whole segment, by
definition). No separate knob.

## 4. Architecture — `SegmentPipeline` (etcd `filePipeline` analog)

A background pre-creator owned by the writer, active only when the flag is set.

**`SegmentPipeline`** (new module `journal/segment_pipeline.rs`): a helper thread
that keeps **one** preallocated temp file ready (double-buffering, one segment of
look-ahead). Loop:

1. Create temp `seg-prealloc.<n>.tmp`.
2. Zero-fill to `segment_size` via 1 MiB-chunked `write_all` (a **real write** —
   NOT `fallocate`/`set_len`, which on ext4 leaves *unwritten* extents that
   convert and re-journal on first overwrite, defeating the purpose).
3. `sync_all` (data + metadata durable; this is the only place the metadata
   commit happens, and it is off the commit path).
4. Hand the ready file off over a rendezvous channel; wait for a "consumed"
   signal; prepare the next.

The temp carries **no header** — `base_seq` is unknown until activation.

**Activation** (in `write_batch`, replacing the `SegmentFile::create` call at
`writer.rs:512` when the flag is on):

1. Take the ready temp from the pipeline. Blocks only if not yet ready; the first
   temp is pre-warmed at `Journal::open`, so the steady-state path never blocks.
2. Write the real 32-byte header (`base_seq = req.seq`) at offset 0 — overwrites
   already-allocated blocks, no `i_size` change.
3. `sync_data` the header, then `rename(temp → seg-{req.seq}.log)`, then
   `fsync(dir)` to make the rename durable.
4. Construct the `SegmentFile` with logical `size = SEGMENT_HEADER_SIZE`, physical
   EOF = `segment_size`. Signal the pipeline to prepare the next.

**Cost added to the lock-held commit path per rotation:** header-write +
`sync_data` + `rename` + dir-`fsync` — **one metadata commit per ~64 MB of log**,
versus the per-commit metadata commit being eliminated. Zero-fill is entirely on
the pipeline thread.

**Disabled path** unchanged: `SegmentFile::create` as today (extends EOF, full
`sync_all`).

**Lifecycle:** pipeline thread spawned at `Journal::open` when the flag is on; it
pre-warms the first temp synchronously (open-time, off the hot path). On
`Journal::close`: signal shutdown, join the thread, delete the unused ready temp.

## 5. Recovery Correctness (load-bearing)

A preallocated segment is `[header][records][zeros … → segment_size]`. Three
coupled changes let it recover cleanly and **stay** preallocated.

### 5.1 `decode_record`: zero length-prefix is a torn tail, not corruption

At the first zero byte after the last record, `decode_record` reads
`body_len = 0`. Today, when abundant trailing bytes exist (a preallocated tail
has millions), the `bytes.len() < total` guard is false and it returns
`Err(Corrupted{"body_len 0 < 16"})` — **a preallocated segment fails recovery.**

Change: return `Ok(None)` (torn tail / end-of-records) on a zero length-prefix
regardless of remaining bytes. This aligns recovery with the wire format's
documented `len = 0 → not-yet-written` semantics that the lock-free **live**
reader already honors (length-prefix written atomically after the body;
reader sees 0 → record not yet committed).

**This change is unconditional** (not flag-gated): one scan code path, strictly
more aligned with the format.

**Trade-off (explicit):** a corruption that zeroes a *valid* record's length
prefix mid-log would now read as end-of-log rather than be flagged. Acceptable:
group commit writes records in order (single coalesced `write_all` per batch, one
`fdatasync` barrier), so a zeroed-prefix-with-valid-data-after cannot arise from a
torn write; and CRC still guards the bytes of every present record. A guard test
keeps a *non-zero* bad length with trailing bytes erroring `Corrupted`.

### 5.2 Recovery preserves the preallocated tail

Today: `had_torn_tail → seg.truncate(last_durable_offset)` physically `ftruncate`s
(`set_len` + `sync_all`). With the flag on, recovery instead resets only the
**logical cursor** and leaves the physical zeros in place, so the writer resumes
appending into preallocated space — no truncate-then-re-fill churn, no
post-restart perf cliff.

New `SegmentFile::reset_cursor(offset)` (sets `self.size`, no `set_len`/`sync`)
vs. today's physical `truncate`. Selection is by the config flag. The genuine
logical truncation in `truncate_after` (removing committed records) keeps using
physical `truncate` — it is unrelated to the preallocation tail.

### 5.3 Re-preallocate the recovered active segment at open

After recovery, if the flag is on and the active (last) segment's physical EOF
`< segment_size`, zero-fill its tail back up (one-time, open-time). Covers a
segment written *before* the flag was enabled, or one already physically
truncated — guaranteeing steady-state preallocation immediately after restart
rather than only after the next rotation.

**Net:** a crash on a preallocated segment recovers to the exact logical state,
keeps its zero tail, and overwrites it on resume.

## 6. Disk Hygiene & Edge Cases

**Stray temp cleanup.** A crash between "temp created" and "activation rename"
leaves an orphan `seg-prealloc.*.tmp`. Recovery's dir scan filters `seg-*.log`, so
orphans are ignored for replay but would leak ~64 MB each. At `Journal::open`,
before the segment scan, unlink any `seg-prealloc.*.tmp`. Safe: a `.tmp` is never
referenced until its atomic rename to `seg-{seq}.log`, so it holds zero committed
records.

**Clean-close disk footprint.** With the flag on, the active segment is physically
`segment_size` even when logically near-empty. We **do not** truncate on close —
a reopen resumes into preallocated space with no re-fill. The cost is that
idle/closed journals sit at `≥ segment_size`. For continuously-appended,
periodically-purged Raft logs this is fine; recorded as a deliberate trade rather
than adding a truncate-on-close path that next open would just re-fill.

**`truncate_after` interaction.** `truncate_after` keeps using physical `truncate`
to drop tail records (a real logical truncation). After it lands on the *active*
segment with the flag on, re-preallocate its tail (the §5.3 helper) to return to
preallocated steady state. Purge drops whole non-active segments — untouched.

**Rotation thresholds unchanged.** Rotation keys off the *logical* projected size
(`writer.rs:455/507`), which preallocation deliberately leaves untouched. The
`PayloadTooLargeForSegment` guard (`writer.rs:498`) is unaffected.

## 7. Testing

**Unit (ultima_journal)**
1. `decode_record` returns `Ok(None)` for a zero length-prefix with abundant
   trailing bytes; guard test that a *non-zero* bad length still errors
   `Corrupted`.
2. Preallocated round-trip: write N records into a preallocated segment,
   `sync_data`, reopen → all N replay, `had_torn_tail` true, logical cursor at
   end-of-records, **physical EOF still `segment_size`** (tail preserved).
3. Crash recovery: write into a preallocated segment, drop without clean close,
   reopen → records intact, writer resumes at the right offset and overwrites the
   zero tail.
4. Re-preallocation at open: open a short (non-preallocated) segment with the flag
   on → tail zero-filled to `segment_size`.
5. Orphan temp cleanup: a planted `seg-prealloc.*.tmp` is gone after open; real
   segments untouched.
6. `SegmentPipeline` hand-off: ready temp consumed, next prepared; clean shutdown
   on close joins the thread and removes the unused temp.

**Integration / durability**
7. Extend `consistent_durability_survives_reopen` (the test guarding the
   fdatasync change) to run **with the flag on**.
8. Flag-off regression: full existing suite green; default path byte-identical.

**Cluster gates (ultima_cluster)** — lincheck capstone + hard-crash
linearizability with the journal flag enabled, via the temp `.cargo/config.toml`
`paths` override (the cross-repo worktree trick from the fdatasync work). Proves
preallocation doesn't perturb SMR correctness under crash/churn.

**Bench / validation** — the `fsync_prealloc_*` microbench (already built) is the
in-tree witness. The authoritative signal is the operator cloud A/B
(`submitted→persisted` p50/p99 + throughput) on prod NVMe with the flag flipped —
same harness as the fdatasync A/B (task13 §16) — recorded into a new
`taskXX_segment_preallocation.md`.

## 8. Future Extension: O_DIRECT / O_DSYNC (not in scope)

Recorded for forward-compatibility only. This design is a clean substrate for it,
but it is a **marginal follow-on, not a second big lever.**

- **Composes cleanly.** O_DIRECT wants already-allocated, block-mapped extents to
  write into — exactly what preallocation provides (no allocation during direct
  write, no unwritten-extent conversion). O_DSYNC syncs per `write()`; the writer
  already issues one coalesced `write_all` per group-commit batch, so O_DSYNC =
  one barrier per batch = today's amortization, just dropping the explicit
  `sync_data`. Reads stay buffered on a separate fd, unaffected.
- **Marginal payoff.** Of the ~600 µs barrier, preallocation removes the ~390 µs
  metadata commit. O_DIRECT removes only the page-cache copy (~5–26 µs) and merges
  two syscalls into one; it does **not** touch the ~210 µs device flush, the
  residual floor.
- **Real cost is alignment.** O_DIRECT requires offset, length, and buffer all
  block-aligned (512/4096). Current record framing is variable-length and
  unaligned, so each batch write would need padding to a block boundary — an
  on-disk-format/writer change, independent of preallocation. Nothing here
  forecloses it; nothing here does it.
- Would land behind its own flag.

## 9. Affected Files (orientation, not prescriptive)

- `ultima_journal/src/journal/segment.rs` — `decode_record` zero-prefix change;
  `reset_cursor`; a production preallocation helper (sibling to the bench-only
  `preallocate_zerofill_for_bench`); header-at-activation support.
- `ultima_journal/src/journal/segment_pipeline.rs` — **new**, the pipeline thread.
- `ultima_journal/src/journal/writer.rs` — activation path in `write_batch`;
  flag-gated branch vs. `SegmentFile::create`.
- `ultima_journal/src/journal/mod.rs` — `Journal::open` orphan-temp cleanup,
  flag-gated recovery (preserve tail vs. truncate), re-preallocation at open,
  pipeline spawn/shutdown; `JournalConfig.preallocate_segments`.
- Tests across the above + the cluster gate wiring.
