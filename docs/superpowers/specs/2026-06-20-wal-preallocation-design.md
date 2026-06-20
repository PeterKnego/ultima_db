# WAL Preallocation Design (ultima_db store WAL)

**Date:** 2026-06-20
**Branch:** `feat/wal-preallocation`
**Status:** Design — pending implementation
**Related:** `docs/superpowers/specs/2026-06-19-segment-preallocation-design.md` (the ultima_journal counterpart this borrows the core trick from), `docs/tasks/task15_three_phase_consistent_persistence.md`, `docs/tasks/task31_*` (WAL write modes).

## 1. Motivation

`ultima_journal` added opt-in segment preallocation (`JournalConfig.preallocate_segments`) and the journal microbench shows a strong group-commit / fsync win from it. The mechanism: physically zero-fill the segment ahead of the write cursor (a *real* write, not a sparse `set_len`/`fallocate`), so ext4 marks the extents **written**. Subsequent appends overwrite already-written blocks, so each per-commit `fsync` no longer drags an `i_size`/extent-map change through the filesystem journal — it degenerates to a pure data barrier (the etcd WAL trick).

ultima_db's **store WAL** (`src/wal.rs`) is a separate durability engine with the same pathology: its production `Coalesced` sink (`BufferedFileSink`) appends with `write_all` at EOF and `sync_all`s every batch, so **every `Durability::Consistent` commit pays the ext4 metadata-journal cost** of a size-extending append. The same preallocation trick should remove that.

### Decision: copy, don't share

The preallocation kernel will be **reimplemented inside ultima_db, tuned for the WAL**, *not* extracted into a crate shared with ultima_journal. Rationale:

- ultima_journal has **zero dependency** on ultima_db and is a deliberately runtime-agnostic leaf. It is **slated to move out of this workspace** (into the `ultima_cluster` repo, its only real consumer) at a later date. A shared crate would create cross-repo coupling precisely where we're about to decouple.
- The two engines differ enough architecturally (rotating fixed-size **segments** + background `filePipeline` vs. a **single growing file** + checkpoint-driven prune) that the higher-level logic does not transfer anyway. What's common is a ~12-line zero-fill primitive whose duplication cost is negligible.
- ultima_journal stays **as-is**; this work does not touch it.

## 2. Architecture

The store WAL is a single file `wal.bin` with the on-disk format
`[len: u32 LE][bincode(WalEntry)][crc32: u32 LE]`, repeated. Multiple `WalSink`
backends share the framing helpers (`frame_entry`, `read_wal`, `prune_wal`). Recovery (`read_wal`) **already stops at the first zero-length record** (`src/wal.rs:336`) — a property added for the mmap sink — so a preallocated zero tail is already a recognized, tolerated end-of-log marker. This is what makes the port cheap: the hard "scan must stop at the first unwritten record, not at EOF" problem is already solved.

A new production sink, **`PreallocFileSink`**, is selected when preallocation is enabled. It reuses the existing framing/recovery/prune helpers (byte-identical format) and changes only the **write strategy**: positioned writes into a physically pre-zeroed region instead of `O_APPEND` growth.

### Components

```
struct PreallocFileSink {
    file: File,        // opened read+write, NOT O_APPEND (must overwrite the zero tail)
    path: PathBuf,
    buf: Vec<u8>,      // coalesce one batch (same as BufferedFileSink)
    write_head: u64,   // logical end of live records
    capacity: u64,     // physical, durably zero-filled size on disk
    chunk: u64,        // grow quantum (default 16 MiB)
}
```

Untouched: `FileWalSink` (PerEntry baseline), `MmapSink` (bench-only), `IoUringSink` (feature-gated). The framing helpers are shared as-is.

## 3. Invariants (correctness contract)

1. **`write_head ≤ capacity ≤ physical_len(file)`** at all times. Records live in `[0, write_head)`; `[write_head, capacity)` is durable zeros.
2. **Barrier discipline** (the conversion mechanism):
   - Batch fits in `[write_head, capacity)` → positioned `write_all` at `write_head`, advance `write_head`, then **`sync_data`** (no metadata change → the win).
   - Batch does *not* fit → **extend** `capacity` by `ceil(needed / chunk)` chunks of real zero-fill + **`sync_all`** (size change must be durable before it's used), *then* write the batch + `sync_data`.
3. **`write_head` is reconstructed, never trusted from disk.** On `open`, run the existing `read_wal` scan; set `write_head` = the offset where it stops (first zero len-prefix), and `capacity` = current `physical_len`. There is no persisted head pointer to corrupt.

## 4. Lifecycle (the WAL does not grow unbounded)

The file **sawtooths**, bounded by checkpoint cadence:

- *Between checkpoints:* `write_head` advances through the pre-zeroed region — those blocks are reused continuously as real records overwrite zeros, **no physical growth** until `write_head` hits `capacity`, at which point one chunk is appended (§3 invariant 2, extend case).
- *At each `checkpoint()`:* `prune_wal` drops entries with version ≤ the checkpoint version and **shrinks** the file (the only place it shrinks).

Peak size ≈ (live entries since last checkpoint) + ≤ one chunk of zero tail. Extra disk vs. non-prealloc is bounded by `chunk` (default 16 MiB), transient, and configurable. Preallocation changes the *granularity* of growth (chunks, not per-entry) and removes the per-fsync metadata cost; it does not change the fundamental WAL growth bound, which is and remains the checkpoint interval.

## 5. Extend strategy — **A: inline grow-ahead** (chosen)

When a batch would overrun `capacity`, the extend happens **inline on the commit path** (zero-fill + `sync_all`, then write). The dominant win — eliminating per-fsync metadata commits for the *steady-state* batches — is captured fully. The cost is that one commit per ~chunk-worth of batches absorbs the zero-fill latency (a periodic tail-latency bump).

No background thread, no temp segment files, therefore **no orphan-temp cleanup on open** (which the journal needs and we avoid).

**Rejected for v1:**
- **B — background pre-extender.** The WAL already owns a background writer thread, so a future optimization could fold opportunistic extension into its idle time between batches (no *new* thread) to erase the periodic bump. Deferred until a real-disk A/B shows the inline extend stalls actually matter.
- Per-entry / mmap / io_uring sinks are out of scope; preallocation targets the production `Coalesced` path only.

## 6. Prune strategy — **P2: pre-sized tmp** (chosen)

`prune_wal` rewrites the WAL via write-to-tmp + atomic rename. Under P2 the tmp file is itself **zero-filled to (live_bytes + one chunk) before the live entries are written into it**, so after the rename the new `wal.bin` is *already* preallocated and `write_head`/`capacity` are known without a separate post-prune extend.

- Keeps the existing tmp+rename **crash-atomicity** (a crash leaves either the complete old WAL or the complete new one) — no new failure modes.
- Folds re-preallocation into the prune write; no redundant extend afterward.
- The preallocated zero tail *is* reused continuously between prunes; only at the prune boundary is a fresh tail created (inherent to the crash-safe tmp+rename model on a single file).

**Rejected:**
- **P1 — discard + re-preallocate** (compact tmp + rename, then a separate zero-fill chunk). Correct but does a redundant extend P2 avoids.
- **P3 — in-place compaction** (slide live entries to the front of the existing preallocated file, reuse all zero blocks). Highest raw efficiency but a crash mid-compaction corrupts the *only* WAL copy; requires a generation/double-buffer scheme. The saving (re-zeroing one chunk per checkpoint) is dwarfed by the checkpoint it rides — not worth trading away single-file crash-atomicity.

## 7. Recovery

Free, by construction:

1. `Store::recover()` calls `read_wal`, which scans framed records and stops at the first zero len-prefix (the preallocated tail) — unchanged.
2. `PreallocFileSink::open` reruns that scan to set `write_head` (last durable offset) and reads `physical_len` into `capacity`.
3. First post-recovery append positions at `write_head` and overwrites the zero tail — no gap, no lost records.

A torn final record (partial write before a crash) decodes as "no more records" exactly as today; `write_head` lands at the last *intact* record, and the torn bytes are overwritten on the next append. CRC still guards accidental corruption of intact records.

## 8. Durability semantics

- Steady-state batches use **`sync_data`** (fdatasync). This is safe *because* preallocation holds the file size constant per batch — the metadata fdatasync would otherwise have to flush isn't changing. Today the production `Coalesced` path uses `sync_all`; `sync_data` is currently bench-only (`WalWrite::CoalescedDataSync`). Preallocation makes `sync_data` production-correct, and that switch is the mechanism that realizes the win.
- Extends use **`sync_all`** so the new size is durable before any record is written into the newly allocated region (otherwise a crash could expose a hole).
- `Durability::Eventual` is unaffected in correctness; it benefits from the same reduced per-fsync cost on its async barrier.

## 9. Configuration

Opt-in, default **off**, mirroring `JournalConfig.preallocate_segments`:

- Add a flag on `Persistence::Standalone` — `preallocate: bool` (v1). A future `preallocate_chunk_bytes: Option<u64>` can expose the quantum; v1 hardcodes 16 MiB.
- When `preallocate: true` **and** `wal_write: WalWrite::Coalesced`, the store constructs `PreallocFileSink` instead of `BufferedFileSink`. Any other `wal_write` value ignores the flag (documented; no silent behavior change to PerEntry/mmap/io_uring).
- Default-off means existing deployments are byte-for-byte unchanged until they opt in.

## 10. Out of scope

- ultima_journal changes (it stays as-is; it will relocate to `ultima_cluster` later — tracked separately).
- A shared preallocation crate (explicitly rejected — see §1).
- Background pre-extender (extend strategy B) and in-place compaction (prune strategy P3) — both deferred with documented triggers.
- Preallocation for the PerEntry, mmap, and io_uring sinks.

## 11. Testing strategy

- **Unit (PreallocFileSink):** append-then-read round-trips identical to `BufferedFileSink`; `write_head`/`capacity` invariants hold across extend boundaries; extend triggers exactly when a batch overruns `capacity`.
- **Recovery / crash-resume:** write N batches, simulate crash (drop without clean close) leaving a zero tail and a torn final record; reopen → `read_wal` returns exactly the durable records, `write_head` lands at the last intact record, next append overwrites the tail with no gap.
- **Prune-resume (P2):** checkpoint → prune → verify the rewritten file is preallocated (physical_len = live + chunk), `write_head` correct, recovery after a post-prune crash returns the right records.
- **Durability equivalence:** under `Durability::Consistent`, a kill-after-ack test shows every acked commit survives recovery (same guarantee as the non-prealloc path) — preallocation must not weaken durability.
- **Parity:** a property/fuzz test asserting `read_wal` output is identical whether records were written by `BufferedFileSink` or `PreallocFileSink` for the same input (format byte-identity).

## 12. Validation (perf)

The win must be confirmed on **real disk**, not the noisy virtualized sandbox (per the project's bench-A/B methodology note — sandbox noise floors reach ±2×).

- Add a Coalesced-vs-Prealloc A/B to the existing store-WAL benches (`singlewriter_persistence_bench` / `ycsb_bench` Strict tier), measuring Consistent-commit throughput and p99.
- Run it under the worktree + shared `CARGO_TARGET_DIR` A/B protocol on the bench host; re-record `make perf/baseline` there before trusting `make perf/check`.
- Acceptance: a statistically clear Consistent-commit throughput improvement with no recovery/durability regressions. If the real-disk delta is within noise, the feature stays opt-in/off and the result is recorded (mirroring the journal's pending cloud A/B posture).

## 13. Risks & mitigations

- **`sync_data` correctness assumption.** If the file size *does* change unexpectedly (a bug in the extend gate), `sync_data` could fail to flush the new size. Mitigation: invariant 2 makes size changes go through the `sync_all` extend path exclusively; tests assert no append crosses `capacity` without an extend.
- **Disk overshoot on idle.** A WAL that goes idle with a fresh chunk holds ≤ one chunk of zeros until the next prune. Bounded and configurable; acceptable.
- **Interaction with O_APPEND assumptions elsewhere.** `PreallocFileSink` must not be mixed with append-mode handles to the same file. Mitigation: the sink owns the only writer fd (same contract the mmap sink already relies on); prune reopens through the sink.
```

