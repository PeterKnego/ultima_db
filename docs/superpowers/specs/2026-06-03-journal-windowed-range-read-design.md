# Design: Sparse-Index Windowed Range Read for `ultima_journal`

**Date:** 2026-06-03
**Status:** Draft for review
**Crate:** `ultima_journal` (`ultima_db/ultima_journal`)
**Relationship:** Range-read counterpart to the point read in
[task33](../../tasks/task33_windowed_point_read.md). "Form 1 (range variant)" from the
read-path exploration: segment pruning + coalesced span read.

## Problem

`Journal::read_range(range)` (`src/journal/mod.rs:242`) computes `(lo, hi)` via
`bounds_to_inclusive`, then **fully `scan()`s every segment from index 0** — including
segments entirely below `lo` — filtering records to `[lo, hi]` with an early return once a
record's `seq > hi`. It therefore pays O(total journal size) for a localized range: a
request for 64 records inside a 64 MiB segment reads and CRC-checks the entire segment, and
a request near the tail of a multi-segment journal still scans every earlier segment.

The sparse `(seq → byte_offset)` index (one entry per ~64 KiB) is now installed into every
segment at open (task33). This design uses it — plus `base_seq` boundaries — to read only
the bytes a range actually needs.

## Goals

- `read_range` skips segments whose seq range does not intersect `[lo, hi]` (pruning, no I/O).
- For each overlapping segment, `read_range` reads only the index-bounded byte span
  `[window(lo), window(hi))` instead of the whole segment (coalesced span read).
- **No public API change** — `read_range`/`iter_range` signatures and observable results are
  identical; only internal I/O cost changes.
- **No on-disk format change**, no write-path change, no durability/recovery change.
- **No regression on full-range reads** (`read_range(..)` reads exactly today's bytes).
- Quantify the win with a baseline-first localized-range benchmark.

## Non-Goals

- Streaming/lazy `iter_range` (yields records without materializing the `Vec`). Decided out
  of scope: a streaming iterator cannot hold the `WriterState` mutex across caller-driven
  iteration, so it requires the deferred lock-decoupling. `iter_range` stays a thin eager
  wrapper over `read_range` and inherits the I/O win.
- Decoupling reads from the `WriterState` mutex (task15 §12).
- Whole-segment integrity verification on a partial range (see Trade-offs).

## Architecture

The change is confined to the read path. Three units:

1. `SegmentFile::read_span` — shared byte-window read (new private helper; `read_record` is
   refactored to use it).
2. `SegmentFile::read_window` — per-segment range primitive (new).
3. `Journal::read_range` — segment pruning + per-segment `read_window` concatenation.

### Component 1: `SegmentFile::read_span`

Both `read_record` (task33) and `read_window` need "read `[start, end)` bytes from this
segment." Extract it once, using the portable `try_clone()` + seek + `read_exact`:

```rust
/// Read the raw bytes in `[start, end)` with one portable `try_clone()` + seek +
/// `read_exact`. Offsets come from the sparse index; caller guarantees
/// `start <= end <= self.size`.
fn read_span(&self, start: u64, end: u64) -> Result<Vec<u8>, JournalError> {
    let mut f = self.file.try_clone()?;
    f.seek(SeekFrom::Start(start))?;
    let mut buf = vec![0u8; (end - start) as usize];
    f.read_exact(&mut buf)?;
    Ok(buf)
}
```

`read_record`'s inline `try_clone`/seek/`read_exact` block is replaced with a `read_span`
call. Its existing tests (`read_record_*`) guard byte-identity. This concentrates the
fd-offset reasoning (safe because reads hold `WriterState`, blocking the writer) in one place.

### Component 2: `SegmentFile::read_window`

```rust
/// Return every record with `lo <= seq <= hi` in this segment, reading only the
/// index-bounded byte span covering `[lo, hi]` rather than the whole segment.
/// Falls back to a full `scan` when the index is empty. Only records in the read
/// span are CRC-verified (same intentional scope as `read_record`).
pub fn read_window(&self, lo: u64, hi: u64) -> Result<Vec<(u64, u64, Vec<u8>)>, JournalError>
```

Algorithm:
1. **Empty-index fallback**: if `self.index.is_empty()`, run `self.scan()`, push records with
   `lo <= seq <= hi` (breaking once `seq > hi`), return.
2. **Span bounds** (`index` ascending in both seq and offset):
   - `let s = self.index.partition_point(|&(sq, _)| sq <= lo);`
     `start = if s == 0 { self.index[0].1 } else { self.index[s - 1].1 }` — the window
     covering `lo`, or the first record's offset if `lo` is below the segment.
   - `let e = self.index.partition_point(|&(sq, _)| sq <= hi);`
     `end = if e < self.index.len() { self.index[e].1 } else { self.size }` — the offset of
     the first record with `seq > hi`, or EOF.
   - `lo <= hi` ⇒ `start <= end` by `partition_point` monotonicity. If `start == end`, the
     read is empty and yields no records (safe).
3. `let buf = self.read_span(start, end)?;` then decode forward via `decode_record`,
   collecting `(seq, meta, payload)` with `lo <= seq <= hi`, advancing the cursor, and
   breaking on `seq > hi` or a torn/short tail (`None`).

**Correctness invariant:** every record with `seq <= hi` is at an offset `< end`, and `start`
never advances past `lo`'s record, so `[start, end)` fully contains all of `[lo, hi]` (plus at
most one index-gap of slop at each end, which the `lo <= seq <= hi` filter discards).

### Component 3: `Journal::read_range`

Same signature, same lock, same `bounds_to_inclusive`. Body changes from "scan every
segment" to prune + per-segment window:

```rust
let st = self.state.lock().unwrap();
let (lo, hi) = bounds_to_inclusive(range, st.first_seq, st.last_seq);
let mut out = Vec::new();
for (i, seg) in st.segments.iter().enumerate() {
    // Prune: segments are seq-ordered; once one starts above hi, all later do.
    if seg.base_seq() > hi {
        break;
    }
    // Prune: skip a segment whose records all fall below lo — true when the next
    // segment's base_seq <= lo (so this segment's max seq < lo).
    if let Some(next) = st.segments.get(i + 1)
        && next.base_seq() <= lo
    {
        continue;
    }
    out.extend(seg.read_window(lo, hi)?);
}
Ok(out)
```

Pruning uses only `base_seq` boundaries (no I/O for skipped segments). Output stays globally
seq-ordered: segments are ordered by `base_seq`, and each `read_window` returns its records
in order.

`iter_range` is unchanged — it still calls `read_range` and returns `v.into_iter().map(Ok)`,
inheriting the win.

## Data flow

`read_range(range)` → lock `WriterState` → `bounds_to_inclusive` → iterate segments with
`base_seq` pruning → `seg.read_window(lo, hi)` (index → `read_span` → decode-filter) per
overlapping segment → concatenate → unlock.

## Error handling & edge cases

- **Unbounded / full range** → every relevant segment's window spans its whole content →
  reads exactly today's bytes, fully CRC-verified → no regression. Preserves
  `read_range_returns_inclusive_records`, `iter_range_streams_records`.
- **Empty / inverted range** (`lo > hi`, e.g. `5..5` → `lo=5, hi=4`) → pruned away or
  `read_window` returns empty (`start >= end`, filter rejects all) → `[]`. Preserves
  `read_range_with_excluded_bounds` semantics.
- **Range below first / above last** → pruned to nothing → `[]`.
- **Multi-segment partial range** → leading/trailing out-of-range segments pruned; boundary
  segments windowed; fully-contained middle segments read whole. Preserves
  `read_range_spans_segments`.
- **Empty-index segment** → falls back to `scan` + filter; correctness preserved.
- **Concurrency** → unchanged: `read_range` holds `WriterState`; the writer thread also
  acquires it, so it is blocked during the read — `try_clone()`+seek cannot race its fd
  offset. Preserves `concurrent_reads_during_appends`.

## Trade-offs

- **A partial range CRC-verifies only the records in its read spans**, not the whole segment
  — the same intentional scope as `read_record` (reading less is the point). A full range
  verifies everything (window = whole segment). Full-segment validation remains in `scan`
  (recovery). Documented on `read_range` and `read_window`.
- One transient `Vec<u8>` per overlapping segment for the span bytes (bounded by the span,
  not the segment). Decoded payloads are still allocated per record, as today.

## Testing

### Segment-level unit tests (`read_window`)
- `read_window_matches_scan_filter`: a multi-window segment (records crossing
  `SPARSE_INDEX_GAP` several times); for several `[lo, hi]` pairs (within one window,
  spanning windows, whole segment, boundary-aligned to an index entry) assert `read_window`
  equals `scan`-then-filter.
- `read_window_empty_and_inverted`: `lo > hi` → `[]`; a range entirely below and entirely
  above the segment → `[]`.
- `read_window_empty_index_falls_back`: a freshly `open_for_read`'d segment (empty index)
  returns correct records via the scan fallback.

### Journal-level
- `read_range_partial_across_segments`: a multi-segment, multi-window journal; a localized
  partial range returns exactly the right records in order, with a leading and a trailing
  segment pruned (asserted via the returned set and `segments.len()`).
- `read_range_after_reopen`: reopen, then a partial range → correct (index installed at open).
- All existing range tests stay green: `read_range_returns_inclusive_records`,
  `read_range_spans_segments`, `read_range_with_excluded_bounds`, `iter_range_streams_records`,
  `concurrent_reads_during_appends`, plus the point-read `read_record_*` tests (guarding the
  `read_span` refactor).

### Benchmark (baseline-first, mirroring task33)
1. Add `bench_range_read` to `ultima_journal/benches/append_throughput.rs`: fill one large
   (~16 MiB) segment, then `read_range` a small localized slice (e.g. 64 consecutive seqs in
   the middle). Commit against the current full-scan `read_range`.
2. `cargo bench -p ultima-journal --bench append_throughput -- --save-baseline before range_read`.
   **Force a rebuild first (`touch` the src files) so the bench relinks against the current
   lib** — task33 hit a stale-binary trap that produced bogus numbers.
3. Implement, then `--baseline before range_read` to get the delta. Read medians from
   `target`'s `criterion/range_read/{before,new}/estimates.json` if needed (note the workspace
   uses a custom `CARGO_TARGET_DIR` at `/home/claude/.cache/cargo-target`).
4. Record before/after in `docs/tasks/task34_windowed_range_read.md` with machine + filesystem
   context, plus a note that a full-range read is unchanged.

## Deliverables

- `src/journal/segment.rs`: `read_span` (private), `read_window` (pub); `read_record`
  refactored to use `read_span`; new unit tests.
- `src/journal/mod.rs`: `read_range` pruning + `read_window`; doc note on partial-range CRC
  scope; new integration tests.
- `benches/append_throughput.rs`: `bench_range_read`.
- `docs/tasks/task34_windowed_range_read.md`: canonical doc + before/after results.
- `cargo clippy -p ultima-journal -- -D warnings` clean; all existing tests green.
