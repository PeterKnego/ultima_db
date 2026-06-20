# task37: WAL Preallocation (`WalWrite::CoalescedPrealloc`)

**Status:** Implemented (Tasks 1–7); real-disk A/B validation pending on bench host.
**Related:** `docs/superpowers/specs/2026-06-20-wal-preallocation-design.md` (full design spec),
`docs/tasks/task36_segment_preallocation.md` (the analogous ultima_journal feature),
`docs/tasks/task31_coalesced_wal_write.md` (the Coalesced baseline this builds on),
`docs/tasks/task13_persistence.md`, `docs/tasks/task15_three_phase_consistent_persistence.md`.

---

## 1. Motivation

The store WAL (`src/wal.rs`) in `Durability::Consistent` mode calls `sync_all` on every
committed batch.  On ext4 (and most Linux journalled filesystems), a size-extending `write +
fsync` forces a metadata journal commit — the filesystem must durably record the new `i_size`
and extent map before the data barrier can complete.  Under a serial single-writer workload,
where commits cannot be overlapped, this makes every `Consistent` commit pay the full
metadata-journal round-trip.

`ultima_journal` solved the identical problem for its segment files: by **physically
zero-filling** ("preallocating") space ahead of the write cursor with a real `write_all` (not
`fallocate`/`set_len`, which are sparse), ext4 marks the extents *written*.  Subsequent
appends overwrite already-written blocks, so each `fsync` degenerates to a pure data barrier
(`fdatasync`) with no size change — the etcd WAL trick.

This feature ports the same mechanism to the store WAL as the opt-in `WalWrite::CoalescedPrealloc`.

---

## 2. Copy-not-share decision

The preallocation kernel is **reimplemented inside ultima_db, not extracted into a shared crate
with ultima_journal**.  Reasons:

- `ultima_journal` is a deliberately runtime-agnostic leaf with **zero dependency** on
  `ultima_db`.  It is **slated to relocate into the `ultima_cluster` repo** (its only real
  consumer); a shared crate would create cross-repo coupling exactly where we are about to
  decouple.
- The two engines differ architecturally (rotating fixed-size *segments* + background
  `filePipeline` vs. a *single growing file* + checkpoint-driven prune) — the higher-level
  logic does not transfer.
- What is shared is a ~12-line zero-fill primitive; duplication cost is negligible.

`ultima_journal` stays as-is; this work does not touch it.

---

## 3. `PreallocFileSink` design

A new production sink selected when `WalWrite::CoalescedPrealloc` is configured:

```rust
struct PreallocFileSink {
    file: File,        // opened read+write (NOT O_APPEND — must overwrite the zero tail)
    path: PathBuf,
    buf:  Vec<u8>,     // batch coalescing buffer (same role as BufferedFileSink)
    write_head: u64,   // logical end of live records
    capacity:   u64,   // physical, durably zero-filled file size on disk
    chunk:      u64,   // grow quantum (default 16 MiB)
}
```

On `open`, the existing `read_wal` / `scan_wal` scan determines `write_head` (the offset
of the first zero len-prefix), and `capacity` is set from `physical_len(file)`.

The existing framing helpers (`frame_entry`, `prune_wal`, WAL entry format) are shared
byte-for-byte — `PreallocFileSink` changes only the *write strategy*, not the on-disk format.
Untouched: `FileWalSink` (PerEntry), `MmapSink` (bench-only), `IoUringSink` (feature-gated).

---

## 4. The three invariants

1. **`write_head ≤ capacity ≤ physical_len(file)`** at all times.
   Records live in `[0, write_head)`; `[write_head, capacity)` is durable zeros.

2. **Barrier discipline** (what makes the win):
   - Batch fits in `[write_head, capacity)` → positioned `write_all` at `write_head`,
     advance `write_head`, **`sync_data`** (no size change → no metadata journal cost).
   - Batch does *not* fit → extend `capacity` by `⌈needed / chunk⌉` chunks of zero-fill +
     **`sync_all`** (new size must be durable before use), *then* write the batch +
     `sync_data`.

3. **`write_head` is reconstructed, never trusted from disk.**
   On `open`, run `scan_wal`; set `write_head` to the last durable offset.  There is no
   persisted head pointer to corrupt.

---

## 5. Extend strategy A — inline grow-ahead (chosen)

When a batch overruns `capacity`, the zero-fill + `sync_all` extend happens **inline on the
commit path** before the batch write.  The dominant win — `sync_data` for all steady-state
batches — is fully captured.  The cost is a periodic tail-latency bump (one extend per
~chunk-worth of data).

No background thread; no orphan-temp cleanup on open.

**Rejected (v1):** Strategy B (fold the extend into the background WAL writer thread's idle
time) is deferred until real-disk profiling shows the inline extend bumps matter.

---

## 6. Prune strategy P2 — pre-sized tmp (chosen)

`prune_wal` rewrites the WAL via write-to-tmp + atomic rename.  Under **P2**, the tmp file is
itself zero-filled to `(live_bytes + one chunk)` before the live entries are written into it.
After rename, the new `wal.bin` is already preallocated and `write_head`/`capacity` are known
immediately — no separate post-prune extend.

- Preserves existing tmp+rename **crash-atomicity** (a crash leaves either the complete old
  WAL or the complete new WAL).
- Folds re-preallocation into the prune write; no redundant extend afterward.

**Rejected:** P1 (compact tmp, then a separate zero-fill extend) — correct but redundant.
P3 (in-place compaction) — highest efficiency but risks WAL corruption mid-compaction; not
worth trading away crash-atomicity.

---

## 7. Tail-tolerant recovery — the prealloc subtlety

The clean-tail case is already handled: `read_wal` stops at the first zero len-prefix
(`src/wal.rs:336`, inherited from the mmap sink).

**The new problem:** preallocation means a partially-written record sits in front of durable
zeros.  It *looks* complete (len + data + CRC bytes are all "present," backed by the zero
fill) but its CRC fails.  Under the existing rule (`CRC mismatch → WalCorrupted`) this would
abort recovery for a torn tail that is actually harmless.

**Resolution — `scan_wal(path, tail_tolerant: bool)`:**
- `tail_tolerant = false` (strict): existing behavior, used by every non-prealloc caller.
  `read_wal` is now a thin wrapper around this.
- `tail_tolerant = true`: a CRC mismatch (or any undecodable frame) means **end-of-log** —
  stop at the last good offset, return it, no error.  `Store::recover()` selects this mode
  when `wal_write = WalWrite::CoalescedPrealloc`.

**Trade-off (documented, accepted):** the prealloc path cannot distinguish a torn tail from
tail corruption; it stops at the first bad frame either way.  Mid-WAL corruption *ahead* of
the tail is still caught (intact records before the bad frame are CRC-verified).  This is the
conventional WAL durability posture (etcd, RocksDB) and the necessary price of the zero tail.

---

## 8. Configuration

```rust
Persistence::Standalone {
    dir,
    durability: Durability::Consistent,  // or Eventual
    wal_write:  WalWrite::CoalescedPrealloc,
}
```

**Why a `WalWrite` variant, not a `bool`:**
- Preallocation in v1 targets only the Coalesced write strategy; a `bool` flag would be
  illusory (`Coalesced + true` is the only valid combination).
- A variant slots cleanly into the existing `match wal_write` (`src/store.rs`) and
  `WalSinkKind` enum alongside the existing `Coalesced` arm.
- `Store::recover()` already reads `wal_write`, so tail-tolerant-scan selection is driven by
  the same value with no extra plumbing.
- **No existing `Persistence::Standalone { .. }` literal is broken** — the struct shape is
  unchanged; callers that do not opt in are byte-for-byte identical.

Default remains `WalWrite::PerEntry`.  A `preallocate_chunk_bytes` knob can be added later
if needed; v1 hardcodes the 16 MiB quantum.

---

## 9. Testing

| Test | What it covers |
|---|---|
| `PreallocFileSink` append/read round-trip | Format byte-identity with `BufferedFileSink` |
| `write_head`/`capacity` invariant across extend boundary | Invariant 1 + 2 |
| Extend triggers exactly when batch overruns `capacity` | No premature `sync_all` |
| Crash-resume (drop without close, zero tail + torn tail) | Tolerant scan returns only durable records |
| Torn-tail-looks-complete (hand-crafted CRC-fail record before zeros) | `scan_wal(…, true)` stops cleanly; `scan_wal(…, false)` returns `WalCorrupted` |
| Prune-resume (checkpoint → prune → reopen → append) | P2 pre-sized tmp; correct `write_head` after rename |
| Durability equivalence (kill-after-ack) | Every acked Consistent commit survives recovery |
| Parity fuzz | `read_wal` output identical for `BufferedFileSink` vs `PreallocFileSink` input |

---

## 10. A/B validation

### Sandbox A/B (2026-06-20) — strong directional confirmation

A same-host YCSB A/B on **real ext4** (not tmpfs) shows preallocation makes every
fsync-bound strict-tier workload **~2.0–2.3× faster**, with read-only and the whole
nondurable tier flat (the controls). Full tables, methodology, and caveats:
`docs/benchmarks/wal-preallocation-ab-2026-06-20.md`.

| Strict workload | OFF | ON | speedup |
|---|---|---|---|
| A update-heavy | 550.7 ms | 240.2 ms | 2.29× |
| B read-mostly | 58.0 ms | 25.2 ms | 2.30× |
| D read-latest | 55.6 ms | 25.4 ms | 2.19× |
| E short-ranges | 52.5 ms | 25.8 ms | 2.04× |
| F read-modify-write | 512.6 ms | 229.6 ms | 2.23× |
| C read-only | 129.9 µs | 132.5 µs | flat ✓ |

The A/B is driven by the `ULTIMA_BENCH_PREALLOC` toggle in `benches/ycsb_bench.rs`
(`Coalesced` → `CoalescedPrealloc`); the bench arm
`standalone_consistent_coalesced_prealloc` in `benches/singlewriter_persistence_bench.rs`
is an equivalent pair. The ~2× clears the sandbox noise floor (±15% error bars) decisively.

### Confirmed on real hardware — AWS NVMe (2026-06-20)

Re-run on one node of the AWS bench fleet (`c6id.4xlarge`, local NVMe instance store,
ext4) — full tables in `docs/benchmarks/wal-preallocation-ab-2026-06-20.md`. The YCSB
strict A/B shows **~2.3–2.6× durable-commit throughput, statistically significant
(p<0.05)** on every fsync-bound workload, read-only and Eventual flat (the controls):

| Strict workload | OFF | ON | change |
|---|---|---|---|
| A update-heavy | 89.9 ms | 36.2 ms | −59.7% (p<0.05) |
| B read-mostly | 9.31 ms | 3.91 ms | −57.2% (p<0.05) |
| D read-latest | 9.25 ms | 3.98 ms | −56.6% (p<0.05) |
| E short-ranges | 9.85 ms | 4.32 ms | −55.4% (p<0.05) |
| F read-modify-write | 93.0 ms | 36.3 ms | −61.0% (p<0.05) |
| C read-only | 184 µs | 185 µs | flat ✓ |

The win survives the faster device (NVMe flush is ~6× cheaper than the sandbox disk,
yet removing the per-commit ext4 metadata-journal commit still buys ~2.5×). The
`wal_bench` microbench corroborates (~1.3–1.6×, noisier single-commit measurement).

**Feature stays opt-in/off** (`WalWrite::PerEntry` remains the default) — the validation
confirms the magnitude; flipping the default would be a separate decision.

---

## 11. Files changed

| File | Change |
|---|---|
| `src/wal.rs` | `PreallocFileSink`; `scan_wal(path, tail_tolerant)`; `WalSinkKind::CoalescedPrealloc` |
| `src/store.rs` | `WalWrite::CoalescedPrealloc` → `WalSinkKind::CoalescedPrealloc`; tolerant-scan selection in `recover()` |
| `src/lib.rs` | Re-export `WalWrite::CoalescedPrealloc` |
| `benches/singlewriter_persistence_bench.rs` | `standalone_consistent_coalesced_prealloc` bench arm (A/B pair) |
| `docs/tasks/task37_wal_preallocation.md` | This file |
| `docs/superpowers/specs/2026-06-20-wal-preallocation-design.md` | Retained design history (prior task); not changed by bench task |
