# Sparse-Index Windowed Range Read for `ultima_journal` — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make `Journal::read_range` read only the index-bounded byte span a range needs and skip non-overlapping segments, instead of scanning every segment whole — no public API or on-disk format change.

**Architecture:** Extract a shared `SegmentFile::read_span` byte-window reader (used by the existing `read_record` and the new `read_window`). Add `SegmentFile::read_window(lo, hi)` that uses the sparse index to read only `[window(lo), window(hi))` and decode-filter to `[lo, hi]`, falling back to a full `scan` if the index is empty. Rewrite `Journal::read_range` to prune segments by `base_seq` and concatenate per-segment `read_window` results. `iter_range` stays the eager wrapper and inherits the win.

**Tech Stack:** Rust 2024, `ultima-journal` crate (`ultima_db/ultima_journal`), `criterion` benches, `crc32fast` framing.

**Working directory for all commands:** `/home/claude/ultima/ultima_db`
**Branch:** create `feat/journal-windowed-range` before Task 1 (see Task 0).
**Note:** the workspace uses a custom `CARGO_TARGET_DIR` at `/home/claude/.cache/cargo-target` (criterion data lives there, not under `./target`).

---

## File Structure

- `ultima_journal/benches/append_throughput.rs` — **modify**: add `bench_range_read` (baseline artifact, added first).
- `ultima_journal/src/journal/segment.rs` — **modify**: add `read_span` (private) + `read_window` (pub); refactor `read_record` to use `read_span`; add unit tests.
- `ultima_journal/src/journal/mod.rs` — **modify**: rewrite `read_range` (prune + `read_window`) + doc note; add integration tests.
- `docs/tasks/task34_windowed_range_read.md` — **create**: canonical feature doc + before/after results.

---

## Task 0: Branch

- [ ] **Step 1: Create the feature branch**

Run:
```bash
git checkout -b feat/journal-windowed-range
```
Expected: `Switched to a new branch 'feat/journal-windowed-range'`. Confirm with `git branch --show-current`.

---

## Task 1: Add the range-read baseline bench (BEFORE any read-path change)

**Files:**
- Modify: `ultima_journal/benches/append_throughput.rs`

- [ ] **Step 1: Add the bench function**

Add this function after `bench_point_read` in `ultima_journal/benches/append_throughput.rs`:

```rust
fn bench_range_read(c: &mut Criterion) {
    // Fill one large (~16 MiB) segment, then read a small localized range (64
    // consecutive seqs) from a rotating position. before: full scan of the whole
    // segment; after: bounded windowed span read. Eventual durability + wait-last
    // keeps the fill fast and ensures all records are written before reads.
    let dir = tempfile::tempdir().unwrap();
    let mut cfg = JournalConfig::new(dir.path());
    cfg.durability = Durability::Eventual;
    let j = Journal::open(cfg).unwrap();
    let payload = vec![0xABu8; 1024];
    let n: u64 = 16 * 1024; // ~16 MiB of 1 KiB records, one segment
    let mut last = None;
    for i in 1..=n {
        last = Some(j.append(i, i, &payload).unwrap());
    }
    last.unwrap().wait().unwrap();

    let mut k = 0u64;
    c.bench_function("range_read", |b| {
        b.iter(|| {
            // Rotate the 64-wide slice start deterministically across [1, n-64].
            k = k.wrapping_add(2_654_435_761) % (n - 64) + 1;
            let v = j.read_range(black_box(k)..=black_box(k + 63)).unwrap();
            black_box(v);
        });
    });
}
```

- [ ] **Step 2: Register it in the criterion group**

Change the `criterion_group!` block at the bottom of the file from:

```rust
criterion_group!(
    benches,
    bench_append_consistent,
    bench_append_eventual,
    bench_append_batched,
    bench_point_read,
    bench_iter_range
);
```

to:

```rust
criterion_group!(
    benches,
    bench_append_consistent,
    bench_append_eventual,
    bench_append_batched,
    bench_point_read,
    bench_range_read,
    bench_iter_range
);
```

- [ ] **Step 3: Smoke-run the bench**

Run: `cargo bench -p ultima-journal --bench append_throughput -- --warm-up-time 1 --measurement-time 2 range_read`
Expected: builds and prints a `range_read` time line (no panic).

- [ ] **Step 4: Capture the baseline against the current full-scan read_range**

Run: `cargo bench -p ultima-journal --bench append_throughput -- --save-baseline before range_read`
Expected: completes and saves a baseline named `before`. Record the median time — the comparison point for Task 4.

- [ ] **Step 5: Commit**

```bash
git add ultima_journal/benches/append_throughput.rs
git commit -m "bench(journal): add localized range-read latency bench (task34 baseline)"
```

---

## Task 2: Extract `SegmentFile::read_span` and refactor `read_record` to use it

A behavior-preserving refactor: pull the `try_clone`+seek+`read_exact` out of `read_record` into a shared helper that `read_window` will also use. `read_record`'s existing tests guard byte-identity.

**Files:**
- Modify: `ultima_journal/src/journal/segment.rs`

- [ ] **Step 1: Add the `read_span` helper**

Add this method inside `impl SegmentFile` in `ultima_journal/src/journal/segment.rs`, placed immediately before `read_record`:

```rust
    /// Read the raw bytes in `[start, end)` with one portable `try_clone()` +
    /// seek + `read_exact`. Offsets come from the sparse index; the caller
    /// guarantees `start <= end <= self.size`.
    fn read_span(&self, start: u64, end: u64) -> Result<Vec<u8>, JournalError> {
        let mut f = self.file.try_clone()?;
        f.seek(SeekFrom::Start(start))?;
        let mut buf = vec![0u8; (end - start) as usize];
        f.read_exact(&mut buf)?;
        Ok(buf)
    }
```

- [ ] **Step 2: Refactor `read_record` to call `read_span`**

In `read_record` (in the same file), replace this block:

```rust
        // Read just the bounded window [start, end).
        let mut f = self.file.try_clone()?;
        f.seek(SeekFrom::Start(start))?;
        let mut buf = vec![0u8; (end - start) as usize];
        f.read_exact(&mut buf)?;
```

with:

```rust
        // Read just the bounded window [start, end).
        let buf = self.read_span(start, end)?;
```

- [ ] **Step 3: Run the point-read tests + full suite + lint**

Run: `cargo test -p ultima-journal read_record_ 2>&1 | tail -10 && cargo test -p ultima-journal 2>&1 | tail -8 && cargo clippy -p ultima-journal -- -D warnings 2>&1 | tail -8`
Expected: `read_record_matches_scan_across_windows`, `read_record_gap_and_out_of_range`, `read_record_empty_index_falls_back` all pass (byte-identity preserved); full suite green; zero clippy warnings (`read_span` is reachable via `read_record`).

- [ ] **Step 4: Commit**

```bash
git add ultima_journal/src/journal/segment.rs
git commit -m "refactor(journal): extract SegmentFile::read_span shared by read_record"
```

---

## Task 3: `SegmentFile::read_window` + windowed `Journal::read_range`

Adds the per-segment range primitive and wires it into `read_range` with `base_seq` pruning in the same task, so `read_window` is reachable from non-test code (no transient dead-code).

**Files:**
- Modify: `ultima_journal/src/journal/segment.rs` (add `read_window`; unit tests)
- Modify: `ultima_journal/src/journal/mod.rs` (`read_range` at `mod.rs:242-264`; integration tests)

- [ ] **Step 1: Write the failing unit tests for `read_window`**

Add these three tests to the `#[cfg(test)] mod tests` block in `ultima_journal/src/journal/segment.rs`:

```rust
#[test]
fn read_window_matches_scan_filter() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("seg-00000000000000000001.log");
    let mut seg = SegmentFile::create(&path, 1).unwrap();
    // ~20 KiB payloads → multiple 64 KiB index windows.
    let payload = vec![0x44u8; 20 * 1024];
    let recs: Vec<(u64, u64, &[u8])> =
        (1..=20u64).map(|i| (i, i * 9, payload.as_slice())).collect();
    seg.append_records(&recs).unwrap();
    seg.fsync().unwrap();
    assert!(seg.index_snapshot().len() > 1);

    let all = seg.scan().unwrap().records;
    let expect = |lo: u64, hi: u64| -> Vec<(u64, u64, Vec<u8>)> {
        all.iter()
            .filter(|r| r.seq >= lo && r.seq <= hi)
            .map(|r| (r.seq, r.meta, r.payload.clone()))
            .collect()
    };
    for (lo, hi) in [(1u64, 20u64), (5, 9), (1, 1), (20, 20), (8, 15), (3, 3), (12, 100)] {
        assert_eq!(seg.read_window(lo, hi).unwrap(), expect(lo, hi), "[{lo},{hi}]");
    }
}

#[test]
fn read_window_empty_and_inverted() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("seg-00000000000000000010.log");
    let mut seg = SegmentFile::create(&path, 10).unwrap();
    seg.append_records(&[(10, 0, b"a"), (20, 0, b"b"), (30, 0, b"c")])
        .unwrap();
    seg.fsync().unwrap();

    assert_eq!(seg.read_window(25, 15).unwrap(), Vec::new()); // inverted lo > hi
    assert_eq!(seg.read_window(1, 5).unwrap(), Vec::new()); // entirely below
    assert_eq!(seg.read_window(40, 50).unwrap(), Vec::new()); // entirely above
    assert_eq!(
        seg.read_window(20, 30).unwrap(),
        vec![(20, 0, b"b".to_vec()), (30, 0, b"c".to_vec())]
    );
}

#[test]
fn read_window_empty_index_falls_back() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("seg-00000000000000000001.log");
    {
        let mut seg = SegmentFile::create(&path, 1).unwrap();
        seg.append_records(&[(1, 100, b"x"), (2, 200, b"yy"), (3, 300, b"zzz")])
            .unwrap();
        seg.fsync().unwrap();
    }
    let seg = SegmentFile::open_for_read(&path).unwrap();
    assert!(seg.index_snapshot().is_empty());
    assert_eq!(
        seg.read_window(2, 3).unwrap(),
        vec![(2, 200, b"yy".to_vec()), (3, 300, b"zzz".to_vec())]
    );
    assert_eq!(seg.read_window(5, 9).unwrap(), Vec::new());
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p ultima-journal read_window_ 2>&1 | tail -20`
Expected: compile error — `no method named read_window found for struct SegmentFile`.

- [ ] **Step 3: Implement `read_window`**

Add this method inside `impl SegmentFile` in `ultima_journal/src/journal/segment.rs`, placed immediately after `read_record`:

```rust
    /// Return every record with `lo <= seq <= hi` in this segment, reading only
    /// the index-bounded byte span covering `[lo, hi]` rather than the whole
    /// segment. Falls back to a full `scan` when the index is empty.
    ///
    /// Only records in the read span are CRC-verified; a partial range read does
    /// not validate the whole segment (use `scan` for that).
    pub fn read_window(
        &self,
        lo: u64,
        hi: u64,
    ) -> Result<Vec<(u64, u64, Vec<u8>)>, JournalError> {
        // Defensive fallback: index not yet populated.
        if self.index.is_empty() {
            let scan = self.scan()?;
            let mut out = Vec::new();
            for r in scan.records {
                if r.seq > hi {
                    break;
                }
                if r.seq >= lo {
                    out.push((r.seq, r.meta, r.payload));
                }
            }
            return Ok(out);
        }

        // Byte span covering [lo, hi]: start at the window holding lo (or the
        // first record if lo is below the segment); end at the offset of the
        // first record with seq > hi (or EOF).
        let s = self.index.partition_point(|&(sq, _)| sq <= lo);
        let start = if s == 0 { self.index[0].1 } else { self.index[s - 1].1 };
        let e = self.index.partition_point(|&(sq, _)| sq <= hi);
        let end = if e < self.index.len() {
            self.index[e].1
        } else {
            self.size
        };
        if start >= end {
            return Ok(Vec::new());
        }

        let buf = self.read_span(start, end)?;
        let segname = self.path.file_name().unwrap().to_string_lossy().to_string();
        let mut out = Vec::new();
        let mut cursor = 0usize;
        while cursor < buf.len() {
            let abs_offset = start + cursor as u64;
            match decode_record(&buf[cursor..], &segname, abs_offset)? {
                Some((rec, consumed)) => {
                    if rec.seq > hi {
                        break;
                    }
                    if rec.seq >= lo {
                        out.push((rec.seq, rec.meta, rec.payload));
                    }
                    cursor += consumed;
                }
                None => break,
            }
        }
        Ok(out)
    }
```

- [ ] **Step 4: Run the unit tests to verify they pass**

Run: `cargo test -p ultima-journal read_window_ 2>&1 | tail -20`
Expected: all three `read_window_*` tests PASS.

- [ ] **Step 5: Write the failing integration tests for `read_range`**

Add these two tests to the `#[cfg(test)] mod tests` block in `ultima_journal/src/journal/mod.rs`:

```rust
#[test]
fn read_range_partial_across_segments() {
    let dir = tempfile::tempdir().unwrap();
    let mut cfg = JournalConfig::new(dir.path());
    cfg.segment_size_bytes = 2 * 1024; // small → many segments
    let j = Journal::open(cfg).unwrap();
    let payload = [0x5Au8; 200];
    for i in 1..=50u64 {
        j.append(i, i * 2, &payload).unwrap().wait().unwrap();
    }
    let nseg = j.state.lock().unwrap().segments.len();
    assert!(nseg >= 4, "want several segments to exercise pruning, got {nseg}");

    // Localized partial range in the middle: leading and trailing segments prune.
    let v = j.read_range(20..=29).unwrap();
    let seqs: Vec<u64> = v.iter().map(|(s, _, _)| *s).collect();
    assert_eq!(seqs, (20..=29).collect::<Vec<_>>());
    assert_eq!(v[0], (20, 40, payload.to_vec()));
    assert_eq!(v[9], (29, 58, payload.to_vec()));

    // Full unbounded range is unchanged (no regression).
    assert_eq!(j.read_range(..).unwrap().len(), 50);
}

#[test]
fn read_range_after_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let dir_path = dir.path().to_path_buf();
    let payload = vec![0x6Bu8; 20 * 1024]; // multi-window single segment
    {
        let j = Journal::open(JournalConfig::new(&dir_path)).unwrap();
        for i in 1..=30u64 {
            j.append(i, i * 5, &payload).unwrap().wait().unwrap();
        }
    }
    // Reopen: index installed at open → windowed range path.
    let j2 = Journal::open(JournalConfig::new(&dir_path)).unwrap();
    let v = j2.read_range(10..=15).unwrap();
    let seqs: Vec<u64> = v.iter().map(|(s, _, _)| *s).collect();
    assert_eq!(seqs, vec![10, 11, 12, 13, 14, 15]);
    assert_eq!(v[0].1, 50); // meta = 10 * 5
    assert_eq!(v[5].2, payload);
}
```

- [ ] **Step 6: Run them to verify they pass against the current (full-scan) read_range**

Run: `cargo test -p ultima-journal read_range_partial_across_segments read_range_after_reopen 2>&1 | tail -15`
Expected: BOTH PASS. The current full-scan `read_range` is already correct for these inputs — these are behavioral guards that must STAY green after the rewrite, not red-first.

- [ ] **Step 7: Rewrite `read_range` with pruning + `read_window`**

In `ultima_journal/src/journal/mod.rs`, replace the entire `read_range` method (currently at `mod.rs:242-264`):

```rust
    pub fn read_range(
        &self,
        range: impl RangeBounds<u64>,
    ) -> Result<Vec<(u64, u64, Vec<u8>)>, JournalError> {
        // Compute bounds inside the locked scope so first/last_seq and the
        // scan see the same atomic snapshot of state.
        let st = self.state.lock().unwrap();
        let (lo, hi) = bounds_to_inclusive(range, st.first_seq, st.last_seq);
        let mut out = Vec::new();
        for seg in &st.segments {
            let scan = seg.scan()?;
            for r in scan.records {
                if r.seq < lo {
                    continue;
                }
                if r.seq > hi {
                    return Ok(out);
                }
                out.push((r.seq, r.meta, r.payload));
            }
        }
        Ok(out)
    }
```

with:

```rust
    /// Read all records with `seq` in `range`, in order. Uses each segment's
    /// sparse index to read only the byte span covering the range, and prunes
    /// segments whose seq range does not overlap it. A partial range therefore
    /// CRC-verifies only the records in the spans it reads, not whole segments;
    /// a full/unbounded range reads and verifies everything, as before.
    pub fn read_range(
        &self,
        range: impl RangeBounds<u64>,
    ) -> Result<Vec<(u64, u64, Vec<u8>)>, JournalError> {
        // Bounds + reads under one lock so first/last_seq and the segment state
        // are a consistent snapshot.
        let st = self.state.lock().unwrap();
        let (lo, hi) = bounds_to_inclusive(range, st.first_seq, st.last_seq);
        let mut out = Vec::new();
        for (i, seg) in st.segments.iter().enumerate() {
            // Prune: segments are seq-ordered; once one starts above hi, so do
            // all later ones.
            if seg.base_seq() > hi {
                break;
            }
            // Prune: skip a segment whose records all fall below lo — true when
            // the next segment's base_seq <= lo (this segment's max seq < it).
            if let Some(next) = st.segments.get(i + 1)
                && next.base_seq() <= lo
            {
                continue;
            }
            out.extend(seg.read_window(lo, hi)?);
        }
        Ok(out)
    }
```

- [ ] **Step 8: Run the integration tests, full suite, and lint**

Run: `cargo test -p ultima-journal 2>&1 | tail -25 && cargo clippy -p ultima-journal -- -D warnings 2>&1 | tail -10`
Expected: ALL tests pass, including `read_range_partial_across_segments`, `read_range_after_reopen`, and every existing range test: `read_range_returns_inclusive_records`, `read_range_spans_segments`, `read_range_with_excluded_bounds`, `iter_range_streams_records`, `concurrent_reads_during_appends`, plus the `read_record_*` and `read_window_*` tests. Zero clippy warnings (`read_window` is reachable via `read_range`).

- [ ] **Step 9: Commit**

```bash
git add ultima_journal/src/journal/segment.rs ultima_journal/src/journal/mod.rs
git commit -m "feat(journal): windowed range read via read_window + base_seq pruning"
```

---

## Task 4: Measure the after-result and write the task34 doc

**Files:**
- Create: `docs/tasks/task34_windowed_range_read.md`

- [ ] **Step 1: Force a rebuild, then run the after-benchmark against the saved baseline**

The criterion bench binary must relink against the windowed lib or it reports stale full-scan numbers (this trap bit task33). Touch the sources first:

Run:
```bash
touch ultima_journal/src/journal/mod.rs ultima_journal/src/journal/segment.rs ultima_journal/benches/append_throughput.rs
cargo bench -p ultima-journal --bench append_throughput -- --baseline before range_read
```
Expected: criterion prints `range_read` with a `change: [... %]` line vs `before`. Record the before median, after median, and % change. Medians (ns) are also at `/home/claude/.cache/cargo-target/criterion/range_read/{before,new}/estimates.json` under `median.point_estimate`. Sanity check: the after value should be far smaller than before (reading ~a few windows vs the whole ~16 MiB segment); if it is NOT (within ~noise of before), the bench likely did not relink — re-run after `cargo clean -p ultima-journal`.

- [ ] **Step 2: Capture the machine + filesystem context**

Run: `findmnt -no FSTYPE,TARGET --target "$(git rev-parse --show-toplevel)" && uname -srm`
Expected: prints the filesystem type, mount target, and kernel/arch.

- [ ] **Step 3: Write the task doc**

Create `docs/tasks/task34_windowed_range_read.md` with the content below, replacing every `<...>` placeholder with real values from Steps 1–2. Express before/after in a consistent unit and give the factor (e.g. "−9X%" / "~Nx faster"):

```markdown
# Task 34: Windowed Range Read for the Journal

> Range-read counterpart to the point read in
> [task33](task33_windowed_point_read.md). "Form 1 (range variant)": segment
> pruning + coalesced span read.

## Summary

`Journal::read_range` previously `scan()`-ed every segment whole (up to 64 MiB each) and
filtered to the requested `[lo, hi]`, paying O(total journal size) even for a localized
range. This task makes it (1) prune segments whose `base_seq` range does not overlap
`[lo, hi]` (no I/O) and (2) for each overlapping segment, read only the index-bounded byte
span `[window(lo), window(hi))` via the new `SegmentFile::read_window`. The sparse index is
already installed at open (task33). No public API or on-disk format change.

## Implementation

- `SegmentFile::read_span(start, end)` — shared byte-window reader (`try_clone`+seek+
  `read_exact`), now used by both `read_record` and `read_window`.
- `SegmentFile::read_window(lo, hi)` — `partition_point` finds the span covering `[lo, hi]`;
  reads it via `read_span`, decode-filters to `[lo, hi]`. Falls back to `scan` if the index
  is empty.
- `Journal::read_range` — prunes segments by `base_seq` boundaries, then concatenates each
  overlapping segment's `read_window`. `iter_range` is unchanged (eager wrapper, inherits
  the win).

## Durability, format, concurrency

Unchanged. Reads still hold the `WriterState` lock and the writer also acquires it, so the
writer is blocked during a read — `try_clone()`+seek cannot race its fd offset.

## CRC / integrity scope (intentional)

A *partial* range CRC-verifies only the records in the spans it reads, not whole segments —
the same intentional scope as `read_record` (reading less is the point). A *full/unbounded*
range reads and verifies every segment, exactly as before. Full-segment validation remains
in `scan` (recovery).

## Results

Machine: `<uname -srm output>`; filesystem `<FSTYPE>` at `<TARGET>`.
Benchmark: `cargo bench -p ultima-journal --bench append_throughput -- range_read`.
~16 MiB single segment (16384 × 1 KiB records), `read_range` of a 64-seq localized slice at
a rotating position. Median latency:

| metric | before (full scan) | after (windowed) | change |
|--------|--------------------|------------------|--------|
| range_read | <X> µs | <Y> µs | <-Z% / ~Nx faster> |

A full-range `read_range(..)` is unchanged (each segment's window spans its whole content).

## Analysis

<1–3 sentences: the windowed range read drops localized-range latency from scanning the whole
~16 MiB segment to reading the ~few 64 KiB windows the slice spans, on a warm cache the gain
reflects avoided memcpy + per-record CRC/alloc work (≈ window-bytes / segment-bytes). Note the
win is for partial/localized ranges; full-journal replay is unchanged; and across a
multi-segment journal, pruning additionally skips non-overlapping segments entirely.>
```

- [ ] **Step 4: Verify the doc has no remaining placeholders**

Run: `grep -n "<" docs/tasks/task34_windowed_range_read.md`
Expected: no output except legitimate `<=`/`<` inside prose/code — visually confirm none are unfilled `<...>` template markers.

- [ ] **Step 5: Commit**

```bash
git add docs/tasks/task34_windowed_range_read.md
git commit -m "docs(journal): document windowed range read + before/after results (task34)"
```

---

## Final verification

- [ ] **Full suite + lint + API check**

Run: `cargo test -p ultima-journal 2>&1 | tail -5 && cargo clippy -p ultima-journal -- -D warnings 2>&1 | tail -5`
Expected: all tests pass; zero clippy warnings.

Confirm no public API change: `read_range`/`iter_range` signatures unchanged; the new surface
is `SegmentFile::read_span` (private) / `read_window` (pub on the `pub(crate)` `SegmentFile`).
`JournalConfig`, `Journal`'s public methods, and `lib.rs` re-exports are untouched.

---

## Notes for the implementer

- **Why baseline-first (Task 1 before 2/3):** the only valid before/after is the same bench
  on the same machine across the one code change. Capture `--save-baseline before` on the
  unmodified full-scan `read_range` first.
- **Stale-bench-binary trap (task33 lesson):** always `touch` the src files before the
  after-bench, or criterion reuses a binary linked against the old lib and reports bogus
  (often "no change") numbers. If before ≈ after within noise, suspect this first.
- **Why `read_window` is wired into `read_range` in the same task (Task 3):** a `pub` method
  with no non-test caller trips `dead_code` under plain `clippy`. Wiring it immediately keeps
  every commit lint-clean.
- **No regression on full range:** `read_range(..)` makes each segment's window span the
  whole segment, reading exactly today's bytes (and fully CRC-verifying). The
  `read_range_partial_across_segments` test asserts the unbounded case returns all records.
- **Scope:** `read_range` only. Streaming `iter_range` (lock-decoupling) is a separate
  follow-up; `iter_range` stays the eager wrapper.
- **Feature-doc convention:** per `CLAUDE.md`, the canonical record is `docs/tasks/task34_*.md`.
  The `docs/superpowers/specs` + `plans` artifacts are gitignored working scaffolding.
```
