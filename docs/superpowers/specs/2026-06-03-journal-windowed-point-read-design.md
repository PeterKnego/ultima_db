# Design: Sparse-Index Windowed Point Read for `ultima_journal`

**Date:** 2026-06-03
**Status:** Draft for review
**Crate:** `ultima_journal` (`ultima_db/ultima_journal`)
**Relationship:** Read-path counterpart to the write-path coalescing in
[task32](../../tasks/task32_coalesced_journal_write.md). This is "Form 1 (point reads)"
from the read-path brainstorm.

## Problem

`Journal::read(seq)` (`src/journal/mod.rs:223`) selects the segment whose `base_seq ≤ seq`,
then calls `SegmentFile::scan()` — which `read_to_end`s the **entire segment body** (up to
the 64 MiB `segment_size`) and decodes **every** record, only to return one. A point read
is therefore O(segment): to fetch a single record it reads and CRC-checks the whole file.

Meanwhile each `SegmentFile` already maintains a sparse `(seq → byte_offset)` index, one
entry per ~64 KiB (`SPARSE_INDEX_GAP`), but it is **never read** — both `SegmentFile.index`
and `ScanResult.index` are `#[allow(dead_code)]`. This design activates that index to make
a point read touch ≈64 KiB instead of the whole segment.

## Goals

- `Journal::read(seq)` reads **one bounded ≈64 KiB window** (one inter-index gap, plus at
  most one record's overhang) instead of the whole segment.
- **No public API change** — `Journal::read`'s signature and observable results are
  identical; only its internal cost changes.
- **No on-disk format change**, no change to the write path, durability, or recovery.
- Quantify the win with a baseline-first point-read benchmark.

## Non-Goals

- `read_range` / `iter_range` optimization (Form 1's range variant — segment pruning + a
  coalesced span read). Deferred; this slice is point reads only.
- Decoupling reads from the `WriterState` mutex (Form 2 / task15 §12). Not required here:
  reads hold the lock, and the writer thread also takes it, so the writer is blocked during
  a read and `try_clone()`+seek is race-free.
- Caching decoded records / mmap (Form 3 / approach B/C from the brainstorm).
- Whole-segment integrity verification on a point read (see Trade-offs).

## Portability decision

The window read uses the **portable** `try_clone()` + `seek` + `read_exact` pattern (the
same primitives `scan()` already uses). This builds on all platforms, including Windows.
Rejected: Unix-only `pread` (`std::os::unix::fs::FileExt::read_at`) — cleaner (no clone, no
fd-offset disturbance) but ties the read path to Unix. The extra `dup()` per read is
negligible against reading 64 KiB instead of 64 MiB.

## Architecture

The change is confined to the read path. Four small changes:

1. `SegmentFile::read_record` — the windowed point-read primitive (new).
2. `SegmentFile::set_index` — install a precomputed index at open (new, trivial setter).
3. `Journal::read` — swap the full `scan()` for `read_record` (3-line change).
4. `Journal::open` — install each segment's index from the scan it already performs.

### Component 1: `SegmentFile::read_record`

```rust
/// Read a single record by seq using the sparse index to bound the I/O to one
/// ~64 KiB window. Returns `Ok(None)` if `seq` is absent (gap or out of range).
/// Falls back to a full scan if the index is empty.
pub fn read_record(&self, seq: u64) -> Result<Option<(u64, Vec<u8>)>, JournalError>
```

Algorithm:
1. **Empty-index fallback** (defensive): if `self.index.is_empty()`, run `self.scan()` and
   linear-search its records (current behavior). Rare once open populates the index.
2. **Locate the window.** The index is ascending in both seq and offset. Use
   `partition_point(|&(s, _)| s <= seq)` to get `n` = count of entries with seq ≤ target;
   the covering entry is `i = n - 1`. (If `n == 0`, `seq` is below this segment's first
   record — return `Ok(None)`; selection guarantees `seq ≥ base_seq == index[0].0`, so this
   is defensive.)
3. **Window bounds:** `start = index[i].1`; `end = index.get(i + 1).map(|&(_, o)| o)
   .unwrap_or(self.size)`.
4. **Read:** `let mut f = self.file.try_clone()?; f.seek(SeekFrom::Start(start))?;` then
   `read_exact` of `(end - start) as usize` bytes into a buffer.
5. **Decode forward:** walk records in the buffer with `decode_record(&buf[cursor..],
   &segname, start + cursor as u64)`:
   - `Some((rec, n))` with `rec.seq == seq` → return `Ok(Some((rec.meta, rec.payload)))`.
   - `Some((rec, _))` with `rec.seq > seq` → return `Ok(None)` (gap).
   - `Some((_, n))` otherwise → advance `cursor += n`, continue.
   - `None` (torn/short tail within window) → break → `Ok(None)`.
   - On buffer exhaustion without a match → `Ok(None)`.

**Correctness invariant (why one window suffices):** `i` is the largest index entry with
`seq_i ≤ seq`, so `seq_{i+1} > seq`. Every record whose seq lies in `[seq_i, seq]` is at an
offset `< end = off_{i+1}`. Thus the target, if present, is strictly inside `[start, end)`.

### Component 2: index population at open

`open_for_read` initializes `index: Vec::new()`. `scan()` returns its computed index in
`ScanResult` but does not store it back. `Journal::open` already scans every segment
(Phase 1 torn-tail fix; Phase 2 may re-scan). We keep that index instead of discarding it.

Add a setter:
```rust
pub fn set_index(&mut self, index: Vec<(u64, u64)>) { self.index = index; }
```

In `Journal::open`, change the final `segments` construction so each segment receives its
scan's index:
```rust
let segments: Vec<segment::SegmentFile> = segments_with_scan
    .into_iter()
    .map(|(mut seg, scan)| {
        seg.set_index(scan.index);
        seg
    })
    .collect();
```

The active segment keeps extending its index on each append (already done in
`append_records`). Remove the now-obsolete `#[allow(dead_code)]` on `SegmentFile.index`,
`SegmentFile::index_snapshot`, and `ScanResult.index`.

### Component 3: `Journal::read`

```rust
pub fn read(&self, seq: u64) -> Result<Option<(u64, Vec<u8>)>, JournalError> {
    let st = self.state.lock().unwrap();
    let Some(seg) = st.segments.iter().rev().find(|s| s.base_seq() <= seq) else {
        return Ok(None);
    };
    seg.read_record(seq)
}
```

Same lock, same segment selection, same return type and semantics — only the per-segment
work changes from full `scan()` to a windowed `read_record`.

## Data flow

`Journal::read(seq)` → lock `WriterState` → `segments.iter().rev().find(base_seq ≤ seq)` →
`seg.read_record(seq)` → binary-search resident index → one bounded `read_exact` →
decode-to-target → unlock.

## Error handling & edge cases

- **Gap between records** (`seq` exists between two stored seqs) → `r.seq > seq` → `None`.
  Preserves `read_seq_in_gap_returns_none`.
- **Above last record** → final window exhausts with no match → `None`. Preserves
  `read_above_last_seq_returns_none`.
- **Below first / wrong segment** → handled by segment selection; `partition_point == 0`
  defensively returns `None`. Preserves `read_below_first_seq_returns_none`.
- **`seq == base_seq`** → `index[0]` window starts at the header; first record matches.
- **Small segment (< 64 KiB)** → one index entry; window = whole (tiny) segment; no worse
  than today.
- **CRC**: every record decoded within the window is CRC-checked by `decode_record`, exactly
  as `scan` checks those records. A corrupt record in the window returns
  `Err(JournalError::Corrupted{..})`.
- **Concurrency**: the read holds `WriterState`; the writer thread also acquires it, so it
  is blocked during the read — `try_clone()`+seek cannot race the writer's fd offset.

## Trade-offs

- **Point reads no longer verify the whole segment's integrity** — only the records in the
  read window are CRC-checked. This is intentional (the cost reduction *is* reading less)
  and will be documented on `read` / `read_record`. Full-segment verification remains
  available via `scan()` (used by recovery).
- One extra `dup()` (`try_clone`) per point read — negligible versus the I/O saved.
- The window for the last index entry extends to `self.size`; because the writer pushes an
  index entry every ~64 KiB up to near EOF, this tail window is still ~64 KiB-bounded.

## Testing

### Correctness (segment-level unit tests, deterministic)
- `read_record_matches_scan_across_windows`: build a segment with records large enough to
  produce several index entries (cross `SPARSE_INDEX_GAP` multiple times); for every seq,
  assert `read_record(seq)` returns the same `(meta, payload)` a full `scan` + search would.
  Cover first, last, mid-window, and exactly-at-an-index-boundary seqs.
- `read_record_gap_and_out_of_range`: a seq between two records, below first, and above
  last all return `Ok(None)`.
- `read_record_empty_index_falls_back`: a freshly `open_for_read`'d segment (index empty,
  not yet populated) still returns correct records via the scan fallback.

### Integration (journal-level)
- `windowed_read_after_reopen`: append a multi-window journal, drop, reopen, then point-read
  historical seqs across windows and segments → all correct (exercises index-at-open).
- All existing read tests stay green: `read_returns_appended_record`,
  `read_missing_seq_returns_none`, `read_seq_in_gap_returns_none`,
  `read_below_first_seq_returns_none`, `read_above_last_seq_returns_none`,
  `concurrent_reads_during_appends`, and the unchanged `read_range_*` tests.

### Benchmark (baseline-first, mirroring task32)
1. Add `bench_point_read` to `ultima_journal/benches/append_throughput.rs`: fill a single
   large segment (~16 MiB of records), then `b.iter` a point read at a rotating/pseudo-random
   seq. Commit this against the **current full-scan** `read()`.
2. `cargo bench -p ultima-journal --bench append_throughput -- --save-baseline before point_read`.
3. Implement the windowed read.
4. `cargo bench -p ultima-journal --bench append_throughput -- --baseline before point_read`.
5. Record before/after in `docs/tasks/task33_windowed_point_read.md` (latency, µs), with
   machine + filesystem context. Expect a large reduction (≈64 KiB vs whole-segment read).

> Note on bench determinism: `Math.random`-style randomness isn't needed; rotate the target
> seq across the populated range with a counter so the read touches different windows.

## Deliverables

- `src/journal/segment.rs`: `read_record`, `set_index`; remove three `#[allow(dead_code)]`;
  new unit tests.
- `src/journal/mod.rs`: `Journal::read` swap; index install in `Journal::open`; new
  integration test; doc note on point-read window-only CRC scope.
- `benches/append_throughput.rs`: `bench_point_read`.
- `docs/tasks/task33_windowed_point_read.md`: canonical doc + before/after results.
- `cargo clippy -p ultima-journal -- -D warnings` clean; all existing tests green.
