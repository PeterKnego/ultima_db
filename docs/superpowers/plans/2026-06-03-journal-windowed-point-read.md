# Sparse-Index Windowed Point Read for `ultima_journal` — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make `Journal::read(seq)` read one bounded ~64 KiB window (via the already-maintained sparse index) instead of scanning the whole segment, with no public API or on-disk format change.

**Architecture:** Add `SegmentFile::read_record` that binary-searches the resident sparse `(seq → offset)` index, reads a single `[start, end)` window with the portable `try_clone()`+seek+`read_exact`, and decodes forward to the target. Swap `Journal::read`'s full `scan()` for `read_record` (active segments already carry an index from the append path; reopened segments fall back to a scan until their index is installed). Then populate each segment's index for free at `Journal::open` (the open already scans every segment) so reopened segments also take the fast path. Reads keep the `WriterState` lock (writer blocked during a read), so the window read is race-free.

**Tech Stack:** Rust 2024, `ultima-journal` crate (`ultima_db/ultima_journal`), `criterion` benches, `crc32fast` framing.

**Working directory for all commands:** `/home/claude/ultima/ultima_db`
**Branch:** create `feat/journal-windowed-read` before Task 1 (see Task 0).

---

## File Structure

- `ultima_journal/benches/append_throughput.rs` — **modify**: add `bench_point_read` (baseline artifact, added first).
- `ultima_journal/src/journal/segment.rs` — **modify**: add `read_record` (Task 2) and `set_index` (Task 3); remove two field-level `#[allow(dead_code)]` (Task 3); add unit tests.
- `ultima_journal/src/journal/mod.rs` — **modify**: swap `Journal::read` to `read_record` + doc note (Task 2); install index in `Journal::open` (Task 3); add an integration test (Task 3).
- `docs/tasks/task33_windowed_point_read.md` — **create**: canonical feature doc + before/after results.

---

## Task 0: Branch

- [ ] **Step 1: Create the feature branch**

Run:
```bash
git checkout -b feat/journal-windowed-read
```
Expected: `Switched to a new branch 'feat/journal-windowed-read'`. Confirm with `git branch --show-current` → `feat/journal-windowed-read`.

---

## Task 1: Add the point-read baseline bench (BEFORE any read-path change)

The point-read bench must be added and measured against the current full-scan `read()` so the before/after comparison is valid.

**Files:**
- Modify: `ultima_journal/benches/append_throughput.rs`

- [ ] **Step 1: Add the bench function**

Add this function after `bench_append_batched` in `ultima_journal/benches/append_throughput.rs`:

```rust
fn bench_point_read(c: &mut Criterion) {
    // Fill one large (~16 MiB) segment, then point-read at deterministically
    // rotating seqs. Measures per-read cost: full-segment scan (before) vs the
    // bounded windowed read (after). Eventual durability keeps the fill fast
    // (no per-append fsync); waiting the last notifier ensures every record is
    // written to the file before reads begin.
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
    c.bench_function("point_read", |b| {
        b.iter(|| {
            // Rotate the target deterministically across [1, n] to hit
            // different index windows without RNG.
            k = k.wrapping_add(2_654_435_761) % n + 1;
            let r = j.read(black_box(k)).unwrap();
            black_box(r);
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
    bench_iter_range
);
```

- [ ] **Step 3: Smoke-run the bench**

Run: `cargo bench -p ultima-journal --bench append_throughput -- --warm-up-time 1 --measurement-time 2 point_read`
Expected: builds and prints a `point_read` time line (no panic). This is a smoke check, not the baseline.

- [ ] **Step 4: Capture the baseline against the current full-scan read**

Run: `cargo bench -p ultima-journal --bench append_throughput -- --save-baseline before point_read`
Expected: completes and saves a baseline named `before`. Record the median time (µs) — it is the comparison point for Task 4.

- [ ] **Step 5: Commit**

```bash
git add ultima_journal/benches/append_throughput.rs
git commit -m "bench(journal): add point-read latency bench (task33 baseline)"
```

---

## Task 2: `SegmentFile::read_record` + swap `Journal::read`

Adds the windowed-read primitive and immediately wires it into `Journal::read`, so it is reachable from non-test code (no transient dead-code). Active segments already carry an index from the append path; reopened segments use the scan fallback until Task 3 installs their index.

**Files:**
- Modify: `ultima_journal/src/journal/segment.rs` (add `read_record`; new unit tests)
- Modify: `ultima_journal/src/journal/mod.rs` (`Journal::read` at `mod.rs:223-239`)

- [ ] **Step 1: Write the failing unit tests**

Add these three tests to the `#[cfg(test)] mod tests` block in `ultima_journal/src/journal/segment.rs`:

```rust
#[test]
fn read_record_matches_scan_across_windows() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("seg-00000000000000000001.log");
    let mut seg = SegmentFile::create(&path, 1).unwrap();
    // ~20 KiB payloads → cross the 64 KiB SPARSE_INDEX_GAP several times.
    let payload = vec![0x33u8; 20 * 1024];
    let recs: Vec<(u64, u64, &[u8])> =
        (1..=20u64).map(|i| (i, i * 3, payload.as_slice())).collect();
    seg.append_records(&recs).unwrap();
    seg.fsync().unwrap();

    // The append path populated more than one index window.
    assert!(seg.index_snapshot().len() > 1);

    // read_record matches a full scan for every seq, including boundaries.
    let scan = seg.scan().unwrap();
    for r in &scan.records {
        let got = seg.read_record(r.seq).unwrap();
        assert_eq!(got, Some((r.meta, r.payload.clone())));
    }
}

#[test]
fn read_record_gap_and_out_of_range() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("seg-00000000000000000001.log");
    let mut seg = SegmentFile::create(&path, 10).unwrap();
    // Strictly-increasing seqs with gaps: 10, 20, 30.
    seg.append_records(&[(10, 0, b"a"), (20, 0, b"b"), (30, 0, b"c")])
        .unwrap();
    seg.fsync().unwrap();

    assert_eq!(seg.read_record(10).unwrap(), Some((0u64, b"a".to_vec())));
    assert_eq!(seg.read_record(30).unwrap(), Some((0u64, b"c".to_vec())));
    assert_eq!(seg.read_record(15).unwrap(), None); // gap between records
    assert_eq!(seg.read_record(99).unwrap(), None); // above last
    assert_eq!(seg.read_record(5).unwrap(), None); // below first (partition_point == 0)
}

#[test]
fn read_record_empty_index_falls_back() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("seg-00000000000000000001.log");
    {
        let mut seg = SegmentFile::create(&path, 1).unwrap();
        seg.append_records(&[(1, 100, b"x"), (2, 200, b"yy")]).unwrap();
        seg.fsync().unwrap();
    }
    // Reopen: open_for_read leaves the index empty (not yet populated).
    let seg = SegmentFile::open_for_read(&path).unwrap();
    assert!(seg.index_snapshot().is_empty());
    // read_record still returns correct results via the scan fallback.
    assert_eq!(seg.read_record(2).unwrap(), Some((200u64, b"yy".to_vec())));
    assert_eq!(seg.read_record(9).unwrap(), None);
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p ultima-journal read_record_ 2>&1 | tail -20`
Expected: compile error — `no method named read_record found for struct SegmentFile`.

- [ ] **Step 3: Implement `read_record`**

Add this method inside `impl SegmentFile` in `ultima_journal/src/journal/segment.rs` (place it after the `scan` method). All needed imports (`decode_record`, `SeekFrom`, `Read`, `Seek`) are already in scope in this file:

```rust
    /// Read a single record by `seq`, using the sparse index to bound I/O to one
    /// ~64 KiB window instead of scanning the whole segment. Returns `Ok(None)`
    /// if `seq` is absent (a gap, or out of this segment's range). Falls back to
    /// a full `scan` when the index has not been populated.
    ///
    /// Only records within the read window are CRC-verified; a point read does
    /// not validate the whole segment (use `scan` for that).
    pub fn read_record(&self, seq: u64) -> Result<Option<(u64, Vec<u8>)>, JournalError> {
        // Defensive fallback: index not yet populated (e.g. opened, not installed).
        if self.index.is_empty() {
            let scan = self.scan()?;
            for r in scan.records {
                if r.seq == seq {
                    return Ok(Some((r.meta, r.payload)));
                }
                if r.seq > seq {
                    return Ok(None);
                }
            }
            return Ok(None);
        }

        // Largest index entry whose seq is <= the target. Because we pick the
        // largest seq_i <= seq, the next entry's seq exceeds seq, so the target
        // (if present) lies strictly within [start, end).
        let n = self.index.partition_point(|&(s, _)| s <= seq);
        if n == 0 {
            return Ok(None); // below this segment's first record
        }
        let start = self.index[n - 1].1;
        let end = self.index.get(n).map(|&(_, o)| o).unwrap_or(self.size);

        // Read just the bounded window [start, end).
        let mut f = self.file.try_clone()?;
        f.seek(SeekFrom::Start(start))?;
        let mut buf = vec![0u8; (end - start) as usize];
        f.read_exact(&mut buf)?;

        // Decode forward to the target seq.
        let segname = self.path.file_name().unwrap().to_string_lossy().to_string();
        let mut cursor = 0usize;
        while cursor < buf.len() {
            let abs_offset = start + cursor as u64;
            match decode_record(&buf[cursor..], &segname, abs_offset)? {
                Some((rec, consumed)) => {
                    if rec.seq == seq {
                        return Ok(Some((rec.meta, rec.payload)));
                    }
                    if rec.seq > seq {
                        return Ok(None);
                    }
                    cursor += consumed;
                }
                None => break,
            }
        }
        Ok(None)
    }
```

- [ ] **Step 4: Run the unit tests to verify they pass**

Run: `cargo test -p ultima-journal read_record_ 2>&1 | tail -20`
Expected: all three tests PASS.

- [ ] **Step 5: Swap `Journal::read` to use `read_record`**

In `ultima_journal/src/journal/mod.rs`, replace the entire `read` method (currently at `mod.rs:223-239`):

```rust
    pub fn read(&self, seq: u64) -> Result<Option<(u64, Vec<u8>)>, JournalError> {
        let st = self.state.lock().unwrap();
        let Some(seg) = st.segments.iter().rev().find(|s| s.base_seq() <= seq) else {
            return Ok(None);
        };
        // NOTE: this initial impl scans the whole segment linearly — correctness-first.
        let scan = seg.scan()?;
        for r in &scan.records {
            if r.seq == seq {
                return Ok(Some((r.meta, r.payload.clone())));
            }
            if r.seq > seq {
                return Ok(None);
            }
        }
        Ok(None)
    }
```

with:

```rust
    /// Read a single record by `seq`. Uses the segment's sparse index to read
    /// only a bounded window (~64 KiB) rather than scanning the whole segment;
    /// consequently a point read CRC-verifies only the records in that window,
    /// not the entire segment (recovery's `scan` does full validation).
    pub fn read(&self, seq: u64) -> Result<Option<(u64, Vec<u8>)>, JournalError> {
        let st = self.state.lock().unwrap();
        let Some(seg) = st.segments.iter().rev().find(|s| s.base_seq() <= seq) else {
            return Ok(None);
        };
        seg.read_record(seq)
    }
```

- [ ] **Step 6: Run the full suite + lint**

Run: `cargo test -p ultima-journal 2>&1 | tail -20 && cargo clippy -p ultima-journal -- -D warnings 2>&1 | tail -10`
Expected: ALL tests pass — the three new ones plus every existing read test (`read_returns_appended_record`, `read_missing_seq_returns_none`, `read_seq_in_gap_returns_none`, `read_below_first_seq_returns_none`, `read_above_last_seq_returns_none`, `concurrent_reads_during_appends`). Zero clippy warnings (`read_record` is now reachable from `Journal::read`, and `SegmentFile.index` keeps its existing, now-redundant `#[allow(dead_code)]` — removed in Task 3).

- [ ] **Step 7: Commit**

```bash
git add ultima_journal/src/journal/segment.rs ultima_journal/src/journal/mod.rs
git commit -m "feat(journal): windowed point read via SegmentFile::read_record"
```

---

## Task 3: Populate the index at open + dead-code cleanup

Without this, every reopened-segment point read hits the scan fallback. `Journal::open` already scans each segment — keep that index instead of discarding it.

**Files:**
- Modify: `ultima_journal/src/journal/segment.rs` (add `set_index`; remove two `#[allow(dead_code)]`)
- Modify: `ultima_journal/src/journal/mod.rs` (`Journal::open` index install at `mod.rs:113-114`)
- Test: `ultima_journal/src/journal/mod.rs` (`#[cfg(test)] mod tests`)

- [ ] **Step 1: Write the failing integration test**

Add this test to the `#[cfg(test)] mod tests` block in `ultima_journal/src/journal/mod.rs`:

```rust
#[test]
fn windowed_read_after_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let dir_path = dir.path().to_path_buf();
    // Payloads large enough that the segment spans several 64 KiB index windows.
    let payload = vec![0x7Eu8; 20 * 1024];
    {
        let j = Journal::open(JournalConfig::new(&dir_path)).unwrap();
        for i in 1..=30u64 {
            j.append(i, i * 5, &payload).unwrap().wait().unwrap();
        }
    }
    // Reopen: the index is installed from the open-time scan, so reads take the
    // windowed path (not the empty-index fallback).
    let j2 = Journal::open(JournalConfig::new(&dir_path)).unwrap();
    assert!(j2.state.lock().unwrap().segments[0].index_snapshot().len() > 1);
    for i in [1u64, 7, 15, 23, 30] {
        let (meta, p) = j2.read(i).unwrap().unwrap();
        assert_eq!(meta, i * 5);
        assert_eq!(p, payload);
    }
    assert!(j2.read(1000).unwrap().is_none()); // out of range
}
```

- [ ] **Step 2: Run it to verify it fails**

Run: `cargo test -p ultima-journal windowed_read_after_reopen 2>&1 | tail -20`
Expected: the test COMPILES (both `read` and `index_snapshot` already exist) but FAILS on `assert!(... .index_snapshot().len() > 1)` — after reopen the index is empty because `Journal::open` does not yet install it. (The `read()` calls themselves still return correct data via the scan fallback; only the index assertion fails.)

- [ ] **Step 3: Add `set_index` to `SegmentFile`**

In `ultima_journal/src/journal/segment.rs`, add this method inside `impl SegmentFile` (next to `read_record`):

```rust
    /// Install a precomputed sparse index (built by `scan` at open time) so the
    /// read path can use windowed reads without re-scanning. See `read_record`.
    pub fn set_index(&mut self, index: Vec<(u64, u64)>) {
        self.index = index;
    }
```

- [ ] **Step 4: Install the index in `Journal::open`**

In `ultima_journal/src/journal/mod.rs`, replace the final segments construction (currently at `mod.rs:113-114`):

```rust
        let segments: Vec<segment::SegmentFile> =
            segments_with_scan.into_iter().map(|(seg, _)| seg).collect();
```

with:

```rust
        // Install each segment's sparse index from the scan we already performed
        // above, so point reads can use the windowed `read_record` path.
        let segments: Vec<segment::SegmentFile> = segments_with_scan
            .into_iter()
            .map(|(mut seg, scan)| {
                seg.set_index(scan.index);
                seg
            })
            .collect();
```

- [ ] **Step 5: Remove the now-obsolete dead-code allows**

The read path now consumes these fields in non-test code. In `ultima_journal/src/journal/segment.rs`:

(a) On the `ScanResult.index` field (around `segment.rs:174`), remove the `#[allow(dead_code)]` line and update the comment. It currently reads:
```rust
    /// Sparse seq → byte offset index built during the scan. Currently
    /// unused by the read path (deferred sparse-index-seek optimization).
    #[allow(dead_code)]
    pub index: Vec<(u64, u64)>,
```
Change it to:
```rust
    /// Sparse seq → byte offset index built during the scan. Installed into the
    /// `SegmentFile` at `Journal::open` and used by `read_record`.
    pub index: Vec<(u64, u64)>,
```

(b) On the `SegmentFile.index` field (around `segment.rs:189`), remove the `#[allow(dead_code)]` line and update the comment. It currently reads:
```rust
    /// Sparse seq → byte offset index. Maintained on append; rebuilt on scan.
    /// Currently unused on the read path (see deferred work in `task26_journal.md` —
    /// `Journal::read` linear-scans rather than seeking via this index).
    #[allow(dead_code)]
    index: Vec<(u64, u64)>,
```
Change it to:
```rust
    /// Sparse seq → byte offset index. Maintained on append, installed at open,
    /// and binary-searched by `read_record` to bound point-read I/O to one window.
    index: Vec<(u64, u64)>,
```

(c) Leave the `#[allow(dead_code)]` on `index_snapshot` (around `segment.rs:256`) in place — it is used only in tests, so plain `clippy` (which does not compile the test modules) would still flag it. Update only its comment from `// Used by future sparse-index-seek read path.` to `// Inspection helper; used in tests.`

- [ ] **Step 6: Run the integration test, full suite, and lint**

Run: `cargo test -p ultima-journal 2>&1 | tail -25 && cargo clippy -p ultima-journal -- -D warnings 2>&1 | tail -10`
Expected: ALL tests pass, including `windowed_read_after_reopen` and the unchanged `read_range_*` tests. Zero clippy warnings (both field allows removed; both fields are now read in non-test code; `index_snapshot`'s allow remains, justified).

- [ ] **Step 7: Commit**

```bash
git add ultima_journal/src/journal/segment.rs ultima_journal/src/journal/mod.rs
git commit -m "feat(journal): install sparse index at open so reopened reads use the window"
```

---

## Task 4: Measure the after-result and write the task33 doc

**Files:**
- Create: `docs/tasks/task33_windowed_point_read.md`

- [ ] **Step 1: Run the after-benchmark against the saved baseline**

Run: `cargo bench -p ultima-journal --bench append_throughput -- --baseline before point_read`
Expected: criterion prints `point_read` with a `change: [... %]` line versus the `before` baseline. Record the before median, the after median (both in µs), and the % change. The before median is also at `target/criterion/point_read/before/estimates.json` (`median.point_estimate`, in nanoseconds); the after at `target/criterion/point_read/new/estimates.json`.

- [ ] **Step 2: Capture the machine + filesystem context**

Run: `findmnt -no FSTYPE,TARGET --target "$(git rev-parse --show-toplevel)" && uname -srm`
Expected: prints the filesystem type, mount target, and kernel/arch for the results header.

- [ ] **Step 3: Write the task doc**

Create `docs/tasks/task33_windowed_point_read.md` with the content below, replacing every `<...>` placeholder with the real values from Steps 1–2:

```markdown
# Task 33: Windowed Point Read for the Journal

> Read-path counterpart to the write-path coalescing in
> [task32](task32_coalesced_journal_write.md). "Form 1 (point reads)" from the
> read-path exploration: activate the already-maintained sparse index so a point
> read touches one ~64 KiB window instead of the whole segment.

## Summary

`Journal::read(seq)` previously called `SegmentFile::scan()`, reading and decoding the
entire segment (up to 64 MiB) to return one record. Each `SegmentFile` already maintained
a sparse `(seq → byte_offset)` index (one entry per ~64 KiB) that was never read. This task
adds `SegmentFile::read_record`, which binary-searches that index, reads a single bounded
`[start, end)` window with the portable `try_clone()`+seek+`read_exact`, and decodes forward
to the target. The index is installed into each segment for free at `Journal::open` (the
open already scans every segment). No public API or on-disk format change.

## Implementation

- `SegmentFile::read_record(seq)` — `partition_point` finds the largest index entry with
  `seq_i <= seq`; the window is `[off_i, off_{i+1})` (or `[off_i, size)` for the last entry).
  Because `seq_{i+1} > seq`, the target (if present) lies inside the window. Falls back to a
  full `scan` if the index is empty (defensive).
- `SegmentFile::set_index` — installs the scan-built index at open.
- `Journal::open` — keeps each segment's scan index instead of discarding it.
- `Journal::read` — swaps the full `scan()` for `read_record`; same lock, same selection.

## Durability, format, concurrency

Unchanged. The on-disk format and the write path are untouched. Reads still hold the
`WriterState` lock and the writer also acquires it, so the writer is blocked during a read —
`try_clone()`+seek cannot race its fd offset. Trade-off: a point read CRC-verifies only the
records in its window, not the whole segment (recovery's `scan` does full validation).

## Results

Machine: `<uname -srm output>`; filesystem `<FSTYPE>` at `<TARGET>`.
Benchmark: `cargo bench -p ultima-journal --bench append_throughput -- point_read`.
~16 MiB single segment (16384 × 1 KiB records), point read at rotating seqs. Median latency:

| metric | before (full scan) | after (windowed) | change |
|--------|--------------------|------------------|--------|
| point_read | <X> µs | <Y> µs | <-Z%> |

## Analysis

<1–3 sentences: the windowed read drops point-read latency from scanning ~16 MiB to reading
~64 KiB; state the measured factor and note that the win scales with segment size (a larger
segment widens the gap, since the window stays ~64 KiB). Be honest if the observed factor is
smaller than the naive size ratio — e.g. page-cache effects mean the "before" full scan reads
from RAM, not disk, so the gain reflects decode/CRC work avoided rather than raw I/O.>
```

- [ ] **Step 4: Verify the doc has no remaining placeholders**

Run: `grep -n "<" docs/tasks/task33_windowed_point_read.md`
Expected: no output (every `<...>` replaced).

- [ ] **Step 5: Commit**

```bash
git add docs/tasks/task33_windowed_point_read.md
git commit -m "docs(journal): document windowed point read + before/after results (task33)"
```

---

## Final verification

- [ ] **Full suite + lint + API check**

Run: `cargo test -p ultima-journal 2>&1 | tail -5 && cargo clippy -p ultima-journal -- -D warnings 2>&1 | tail -5`
Expected: all tests pass; zero clippy warnings.

Confirm no public API change: `Journal::read`'s signature is unchanged; the new surface is
`SegmentFile::read_record` / `set_index` on the `pub(crate)` `SegmentFile` (not part of the
journal's public API). `JournalConfig`, `Journal`'s public methods, and `lib.rs` re-exports
are untouched.

---

## Notes for the implementer

- **Why baseline-first (Task 1 before 2/3):** the only valid before/after is the same bench
  on the same machine across the one code change. Capture `--save-baseline before` on the
  unmodified full-scan `read()` first.
- **Why the `Journal::read` swap is in Task 2 (not Task 3):** if `read_record` were added
  without a non-test caller, plain `cargo clippy` (which does not compile test modules) would
  flag it as `dead_code` and fail the lint gate. Wiring `Journal::read` to it in the same
  task keeps every commit lint-clean. Reopened segments use the scan fallback until Task 3
  installs their index — still correct, just not yet fast.
- **Why the index must be installed at open (Task 3):** `open_for_read` leaves `index` empty
  and `scan()` returns its index in `ScanResult` without storing it back. The
  `windowed_read_after_reopen` test guards this (`index_snapshot().len() > 1` after reopen).
- **Window-only CRC is intentional**, not a regression: reading less is the point. Full
  validation remains in `scan` (recovery).
- **Scope:** `Journal::read` only. `read_range`/`iter_range` (segment pruning + coalesced
  span read) are a separate follow-up.
- **Feature-doc convention:** per `CLAUDE.md`, the canonical record is `docs/tasks/task33_*.md`.
  The `docs/superpowers/specs` + `plans` artifacts are gitignored working scaffolding.
```
