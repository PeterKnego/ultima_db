# Coalesced Batch Writes for `ultima_journal` — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Collapse the journal's per-batch write path from N `write_all` (+ N `seek`) syscalls to one `write_all` per segment touched, capturing the task30/31 write-coalescing win (+24–35% Eventual batch throughput) with identical durability and on-disk format.

**Architecture:** Add `SegmentFile::append_records` (one `seek` + one `write_all` for a slice of records, sparse index maintained per record) and make `append_record` a thin wrapper over it. Rewrite `write_batch` to group the batch into per-segment runs (respecting rotation) and flush each run via `append_records`. Everything below stays: the group-commit loop, the single fsync-per-batch, the `SeqWatermark`, and the framed on-disk format.

**Tech Stack:** Rust 2024, `ultima-journal` crate (`ultima_db/ultima_journal`), `criterion` benches, `crc32fast` framing.

**Working directory for all commands:** `/home/claude/ultima/ultima_db`
**Branch:** `feat/journal-coalesced-write` (already created)

---

## File Structure

- `ultima_journal/benches/append_throughput.rs` — **modify**: add `bench_append_batched` (the baseline artifact, added first).
- `ultima_journal/src/journal/segment.rs` — **modify**: add `append_records`; reduce `append_record` to a wrapper; add unit tests.
- `ultima_journal/src/journal/writer.rs` — **modify**: rewrite `write_batch` (`writer.rs:306-347`) for per-segment run grouping; add `flush_run` helper.
- `ultima_journal/src/journal/mod.rs` — **modify**: add one rotation-via-coalescing integration test.
- `docs/tasks/task32_coalesced_journal_write.md` — **create**: canonical feature doc + before/after results table.

---

## Task 1: Add the batched-append baseline bench (BEFORE any write-path change)

The existing benches append one-at-a-time and never form batches. This bench is the baseline artifact; it MUST be committed and measured against the current per-record code first.

**Files:**
- Modify: `ultima_journal/benches/append_throughput.rs`

- [ ] **Step 1: Add the batched bench function**

Add this function after `bench_append_eventual` in `ultima_journal/benches/append_throughput.rs`:

```rust
fn bench_append_batched(c: &mut Criterion) {
    // Eventual mode: the Notifier resolves on the buffered write (fsync is
    // off-clock), so this measures the write path — exactly where coalescing
    // helps. Each iter submits a burst, then waits the final notifier; the
    // writer drains the burst into batches and (after this feature) coalesces
    // each batch into one write_all.
    let mut group = c.benchmark_group("append_batched_eventual");
    const BURST: u64 = 256;
    for &payload_size in &[64usize, 256, 1024, 4096] {
        group.throughput(Throughput::Bytes(payload_size as u64 * BURST));
        group.bench_function(format!("p{payload_size}"), |b| {
            let dir = tempfile::tempdir().unwrap();
            let mut cfg = JournalConfig::new(dir.path());
            cfg.durability = Durability::Eventual;
            let j = Journal::open(cfg).unwrap();
            let payload = vec![0xABu8; payload_size];
            let mut next_seq = 1u64;
            b.iter(|| {
                let mut last = None;
                for _ in 0..BURST {
                    last = Some(j.append(next_seq, 0, &payload).unwrap());
                    next_seq += 1;
                }
                last.unwrap().wait().unwrap();
                black_box(next_seq);
            });
        });
    }
    group.finish();
}
```

- [ ] **Step 2: Register it in the criterion group**

Change the `criterion_group!` block at the bottom of the file from:

```rust
criterion_group!(
    benches,
    bench_append_consistent,
    bench_append_eventual,
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
    bench_iter_range
);
```

- [ ] **Step 3: Verify the bench compiles and runs (quick)**

Run: `cargo bench -p ultima-journal --bench append_throughput -- --warm-up-time 1 --measurement-time 2 append_batched_eventual/p64`
Expected: it builds and prints a throughput line for `append_batched_eventual/p64` (no panic). This is a smoke check, not the baseline.

- [ ] **Step 4: Capture the baseline against the current per-record code**

Run: `cargo bench -p ultima-journal --bench append_throughput -- --save-baseline before append_batched_eventual`
Expected: completes and saves a baseline named `before`. Note the four median throughputs (p64/p256/p1024/p4096) — these are the numbers the final task32 doc compares against.

- [ ] **Step 5: Commit**

```bash
git add ultima_journal/benches/append_throughput.rs
git commit -m "bench(journal): add batched-append throughput bench (task32 baseline)"
```

---

## Task 2: `SegmentFile::append_records` (coalesced write) + `append_record` wrapper

**Files:**
- Modify: `ultima_journal/src/journal/segment.rs` (`append_record` at `segment.rs:256`)
- Test: `ultima_journal/src/journal/segment.rs` (`#[cfg(test)] mod tests`)

- [ ] **Step 1: Write the failing test**

Add to the `mod tests` block in `ultima_journal/src/journal/segment.rs`:

```rust
#[test]
fn append_records_coalesced_matches_per_record() {
    let dir = tempfile::tempdir().unwrap();

    // Reference segment built one record at a time.
    let p1 = dir.path().join("seg-00000000000000000001.log");
    let mut a = SegmentFile::create(&p1, 1).unwrap();
    a.append_record(1, 10, b"alpha").unwrap();
    a.append_record(2, 20, b"beta").unwrap();
    a.append_record(3, 30, b"gamma").unwrap();
    a.fsync().unwrap();

    // Segment built with a single coalesced call.
    let p2 = dir.path().join("seg-00000000000000000010.log");
    let mut b = SegmentFile::create(&p2, 1).unwrap();
    b.append_records(&[(1, 10, b"alpha"), (2, 20, b"beta"), (3, 30, b"gamma")])
        .unwrap();
    b.fsync().unwrap();

    // Identical post-header bytes (headers differ only by created_at).
    let ba = std::fs::read(&p1).unwrap();
    let bb = std::fs::read(&p2).unwrap();
    assert_eq!(a.size().unwrap(), b.size().unwrap());
    assert_eq!(&ba[SEGMENT_HEADER_SIZE..], &bb[SEGMENT_HEADER_SIZE..]);

    // Identical decoded records.
    let sa = SegmentFile::open_for_read(&p1).unwrap().scan().unwrap();
    let sb = SegmentFile::open_for_read(&p2).unwrap().scan().unwrap();
    assert_eq!(sa.records, sb.records);
    assert_eq!(sb.records.len(), 3);
}

#[test]
fn append_records_empty_is_noop() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("seg-00000000000000000001.log");
    let mut seg = SegmentFile::create(&path, 1).unwrap();
    let before = seg.size().unwrap();
    seg.append_records(&[]).unwrap();
    assert_eq!(seg.size().unwrap(), before);
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `cargo test -p ultima-journal append_records_ 2>&1 | tail -20`
Expected: compile error — `no method named append_records found for struct SegmentFile`.

- [ ] **Step 3: Implement `append_records` and reduce `append_record` to a wrapper**

In `ultima_journal/src/journal/segment.rs`, replace the entire `append_record` method (currently `segment.rs:256-276`) with the following two methods:

```rust
    /// Append one record at end-of-file. Thin wrapper over [`append_records`].
    /// Returns the byte offset at which the record was written.
    pub fn append_record(
        &mut self,
        seq: u64,
        meta: u64,
        payload: &[u8],
    ) -> Result<u64, JournalError> {
        let offset = self.size;
        self.append_records(&[(seq, meta, payload)])?;
        Ok(offset)
    }

    /// Append many records with a single `seek` + `write_all`. Caller must
    /// enforce monotonic seq. Coalesces all records into one buffer, writes
    /// once, then advances `size` and maintains the sparse index per record
    /// using the same `SPARSE_INDEX_GAP` rule as a per-record append. Returns
    /// the byte offset of the first record written.
    pub fn append_records(
        &mut self,
        records: &[(u64, u64, &[u8])],
    ) -> Result<u64, JournalError> {
        let start_offset = self.size;
        if records.is_empty() {
            return Ok(start_offset);
        }
        // Coalesce: encode every record into one buffer, tracking each
        // record's absolute byte offset for the sparse index.
        let mut buf = Vec::new();
        let mut offsets: Vec<(u64, u64)> = Vec::with_capacity(records.len());
        let mut running = self.size;
        for (seq, meta, payload) in records {
            let rec = encode_record(*seq, *meta, payload);
            offsets.push((*seq, running));
            running += rec.len() as u64;
            buf.extend_from_slice(&rec);
        }
        // Single seek + single write_all for the whole run.
        self.file.seek(SeekFrom::Start(self.size))?;
        self.file.write_all(&buf)?;
        self.size += buf.len() as u64;
        // Maintain the sparse index per record, identical gap rule.
        for (seq, off) in offsets {
            let want_index = self
                .index
                .last()
                .is_none_or(|(_, prev)| off.saturating_sub(*prev) >= SPARSE_INDEX_GAP);
            if want_index {
                self.index.push((seq, off));
            }
        }
        Ok(start_offset)
    }
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `cargo test -p ultima-journal append_records_ 2>&1 | tail -20`
Expected: `append_records_coalesced_matches_per_record` and `append_records_empty_is_noop` both PASS.

- [ ] **Step 5: Run the full segment + journal test suite (regression)**

Run: `cargo test -p ultima-journal 2>&1 | tail -20`
Expected: all tests pass (the existing `segment_append_and_scan_roundtrip`, `segment_index_snapshot_returns_built_index`, etc. now exercise the wrapper).

- [ ] **Step 6: Commit**

```bash
git add ultima_journal/src/journal/segment.rs
git commit -m "feat(journal): add SegmentFile::append_records (coalesced write)"
```

---

## Task 3: Rewrite `write_batch` to coalesce per segment

**Files:**
- Modify: `ultima_journal/src/journal/writer.rs` (`write_batch` at `writer.rs:306-347`)
- Test: `ultima_journal/src/journal/mod.rs` (`#[cfg(test)] mod tests`)

- [ ] **Step 1: Write the failing test**

Add to the `mod tests` block in `ultima_journal/src/journal/mod.rs`:

```rust
#[test]
fn coalesced_batch_spans_segments() {
    // Submit a burst WITHOUT waiting each append — this lets the bg writer
    // drain multiple records into one batch, exercising the coalesced write
    // path including rotation across the 256-byte segment boundary. Whatever
    // the batch grouping, the result must be correct.
    let dir = tempfile::tempdir().unwrap();
    let mut cfg = JournalConfig::new(dir.path());
    cfg.segment_size_bytes = 256;
    let j = Journal::open(cfg).unwrap();
    let payload = [0xCDu8; 100];
    let mut last = None;
    for i in 1..=12u64 {
        last = Some(j.append(i, i * 7, &payload).unwrap());
    }
    last.unwrap().wait().unwrap();

    assert_eq!(j.last_seq(), Some(12));
    let v = j.read_range(1..=12).unwrap();
    assert_eq!(v.len(), 12);
    assert_eq!(v[0], (1, 7, payload.to_vec()));
    assert_eq!(v[11], (12, 84, payload.to_vec()));
    let segs = j.state.lock().unwrap().segments.len();
    assert!(segs >= 2, "expected rotation across segments, got {segs}");
}
```

- [ ] **Step 2: Run the test against current code (should already pass)**

Run: `cargo test -p ultima-journal coalesced_batch_spans_segments 2>&1 | tail -15`
Expected: PASS (the current per-record `write_batch` is also correct here). This test is a behavioral guard that must STAY green after the rewrite — it is not red-first, because we are refactoring a correct path to a faster one with identical observable behavior.

- [ ] **Step 3: Rewrite `write_batch` and add the `flush_run` helper**

In `ultima_journal/src/journal/writer.rs`, replace the entire `write_batch` function (`writer.rs:306-347`) with:

```rust
fn write_batch(
    state: &Arc<Mutex<WriterState>>,
    batch: &[AppendRequest],
) -> Result<Option<u64>, JournalError> {
    let mut st = state.lock().unwrap();

    // Records accumulated for the current (last) segment, not yet written.
    let mut run: Vec<(u64, u64, &[u8])> = Vec::new();
    // Projected size of the current segment = on-disk size + bytes buffered in
    // `run`. Drives rotation exactly like the old per-record `seg.size()` check.
    let mut projected: u64 = match st.segments.last() {
        Some(seg) => seg.size()?,
        None => 0,
    };
    // Running last seq for monotonic validation across the batch — covers
    // records buffered in `run` that have not yet updated `st.last_seq`.
    let mut prev = st.last_seq;

    for req in batch {
        // Monotonic-seq guard (redundant with append()'s enqueue check, kept
        // as defense). On failure flush the good prefix so earlier records are
        // persisted, matching the old per-record write-then-error behavior.
        if let Some(last) = prev
            && req.seq <= last
        {
            flush_run(&mut st, &mut run)?;
            return Err(JournalError::NonMonotonicSeq {
                expected_gt: last,
                got: req.seq,
            });
        }
        let body_len = 16 + req.payload.len();
        let total = (4 + body_len + 4) as u64;
        if total > st.segment_size {
            flush_run(&mut st, &mut run)?;
            return Err(JournalError::PayloadTooLargeForSegment {
                segment_size: st.segment_size,
                record_size: total,
            });
        }

        // Rotate when there is no segment yet, or the current one is full.
        let need_new = st.segments.is_empty() || projected >= st.segment_size;
        if need_new {
            // Flush whatever was accumulated for the now-full segment first.
            flush_run(&mut st, &mut run)?;
            let path = st.dir.join(format!("seg-{:020}.log", req.seq));
            st.segments.push(SegmentFile::create(&path, req.seq)?);
            projected = st.segments.last().unwrap().size()?;
        }

        run.push((req.seq, req.meta, &req.payload));
        projected += total;
        if st.first_seq.is_none() {
            st.first_seq = Some(req.seq);
        }
        st.last_seq = Some(req.seq);
        prev = Some(req.seq);
    }

    // Flush the final run for the active segment.
    flush_run(&mut st, &mut run)?;
    Ok(st.last_seq)
}

/// Write all accumulated records to the active segment with a single
/// coalesced `write_all`, then clear the run. No-op on an empty run.
fn flush_run(
    st: &mut WriterState,
    run: &mut Vec<(u64, u64, &[u8])>,
) -> Result<(), JournalError> {
    if run.is_empty() {
        return Ok(());
    }
    let seg = st
        .segments
        .last_mut()
        .expect("flush_run called with a non-empty run but no active segment");
    seg.append_records(run.as_slice())?;
    run.clear();
    Ok(())
}
```

Note on the `flush_run(&mut st, ...)` calls: `st` is a `MutexGuard<WriterState>`; deref coercion turns `&mut st` into the `&mut WriterState` the helper expects. If the borrow checker objects, change the calls to `flush_run(&mut *st, &mut run)` (explicit reborrow) — behaviorally identical.

- [ ] **Step 4: Run the rotation test + full suite (regression)**

Run: `cargo test -p ultima-journal 2>&1 | tail -25`
Expected: ALL tests pass, including `coalesced_batch_spans_segments`, `segment_rotates_when_size_exceeded`, `read_range_spans_segments`, `truncate_after_across_segments`, `reopen_sees_appended_records`, and the task28 `durable_seq_*` tests. The coalesced path must be observationally identical to the old one.

- [ ] **Step 5: Lint**

Run: `cargo clippy -p ultima-journal -- -D warnings 2>&1 | tail -15`
Expected: zero warnings.

- [ ] **Step 6: Commit**

```bash
git add ultima_journal/src/journal/writer.rs ultima_journal/src/journal/mod.rs
git commit -m "feat(journal): coalesce batch writes into one write_all per segment"
```

---

## Task 4: Measure the after-result and write the task32 doc

**Files:**
- Create: `docs/tasks/task32_coalesced_journal_write.md`

- [ ] **Step 1: Run the after-benchmark against the saved baseline**

Run: `cargo bench -p ultima-journal --bench append_throughput -- --baseline before append_batched_eventual`
Expected: criterion prints each `append_batched_eventual/pNNN` with a `change: [... %]` line versus the `before` baseline. Record the four medians (before vs after) and the % change. The single-commit guard benches are unaffected; optionally confirm no regression with:
`cargo bench -p ultima-journal --bench append_throughput -- --baseline before append_consistent` (expect changes within noise).

- [ ] **Step 2: Capture the machine + filesystem context**

Run: `findmnt -no FSTYPE,TARGET --target "$(git rev-parse --show-toplevel)" && uname -srm`
Expected: prints the filesystem type, mount target, and kernel/arch. These go in the results header (numbers are disk-dependent).

- [ ] **Step 3: Write the task doc**

Create `docs/tasks/task32_coalesced_journal_write.md`. Fill the RESULTS table with the real numbers from Step 1 and the machine line from Step 2 (replace every `<...>` placeholder):

```markdown
# Task 32: Coalesced Batch Writes for the Journal

> Brings the write-coalescing half of group commit (ultima_db's
> [task30](task30_wal_backends.md)/[task31](task31_coalesced_wal_write.md)) into
> `ultima_journal`. The journal already had one fsync per batch; it lacked the
> single-`write_all`-per-batch that task31 added to the store's WAL.

## Summary

`write_batch` previously issued one `seek` + one `write_all` per record, then a
single fsync for the batch. This task coalesces every record destined for the same
segment into one buffer and issues a single `write_all` per segment (one in the
common single-segment batch), keeping the single fsync, the framed on-disk format,
the `SeqWatermark`, and recovery bit-for-bit identical.

Always-on (no config flag): within-batch coalescing is strictly fewer syscalls with
no durability or format change, and the journal's "batch" is internal — unlike
ultima_db, there was no documented `PerEntry` default to preserve.

## Implementation

- `SegmentFile::append_records(&[(seq, meta, &payload)])` — encodes the run into one
  buffer, one `seek` + one `write_all`, advances `size`, maintains the sparse index
  per record with the same 64 KiB gap rule. `append_record` is now a thin wrapper.
- `write_batch` groups the batch into per-segment runs using a projected-size counter
  that mirrors the old `seg.size() >= segment_size` rotation rule, flushing each run
  via `append_records`. Monotonic-seq and payload-size guards are retained; on failure
  the accumulated good prefix is flushed before returning `Err` (matches prior
  per-record semantics).

## Durability & format

Unchanged. Same framed bytes, same single `sync_all` per batch, same torn-tail and
sentinel recovery. The change is entirely below the fsync boundary.

## Results

Machine: `<uname -srm output>`; filesystem `<FSTYPE>` at `<TARGET>`.
Benchmark: `cargo bench -p ultima-journal --bench append_throughput -- append_batched_eventual`.
Burst = 256 records/iter, Eventual durability. Medians:

### Eventual batch throughput (MiB/s, median) — before vs after

| Payload | before (PerEntry) | after (Coalesced) | change |
|---------|-------------------|-------------------|--------|
| 64 B    | <X> MiB/s         | <Y> MiB/s         | <+Z%>  |
| 256 B   | <X> MiB/s         | <Y> MiB/s         | <+Z%>  |
| 1 KiB   | <X> MiB/s         | <Y> MiB/s         | <+Z%>  |
| 4 KiB   | <X> MiB/s         | <Y> MiB/s         | <+Z%>  |

### Guard: Consistent single-commit latency

No meaningful change (size-1 batches; coalescing is a no-op there), consistent with
task31's finding. Medians within measurement noise of the `before` baseline.

## Analysis

<1–3 sentences: did the measured gain land in task31's +24–35% band? Note any
deviation and the likely cause — e.g. small payloads are more syscall-bound so the
coalescing gain is larger; large payloads are more bandwidth-bound so it is smaller.>
```

- [ ] **Step 4: Verify the doc has no remaining placeholders**

Run: `grep -n "<" docs/tasks/task32_coalesced_journal_write.md`
Expected: no output (every `<...>` replaced with a real value).

- [ ] **Step 5: Commit**

```bash
git add docs/tasks/task32_coalesced_journal_write.md
git commit -m "docs(journal): document coalesced batch writes + before/after results (task32)"
```

---

## Final verification

- [ ] **Full workspace test + lint**

Run: `cargo test -p ultima-journal 2>&1 | tail -5 && cargo clippy -p ultima-journal -- -D warnings 2>&1 | tail -5`
Expected: all tests pass; zero clippy warnings.

- [ ] **Confirm no public API change**

The diff touches only `segment.rs`, `writer.rs`, `mod.rs` (tests), the bench, and the task doc. `JournalConfig`, `Journal`, `Durability`, and `lib.rs` re-exports are untouched — `append_records` is `pub` on the `pub(crate)` `SegmentFile`, not part of the journal's public surface.

---

## Notes for the implementer

- **Why baseline-first (Task 1 before Task 2/3):** the only valid before/after is the same bench on the same machine across the one code change. Capturing `--save-baseline before` on the unmodified write path is mandatory; doing it after implementation loses the comparison.
- **TDD shape:** Task 2 is red→green (new method). Task 3 is a behavior-preserving refactor of a correct path, so its test is green-before-and-after (a regression guard), with the existing rotation/recovery suite as the real safety net.
- **Borrow-checker fallback:** if `flush_run(&mut st, ...)` does not compile, use `flush_run(&mut *st, ...)`.
- **Feature-doc convention:** per `CLAUDE.md`, the canonical record is `docs/tasks/task32_*.md`. The `docs/superpowers/specs` + `plans` artifacts are gitignored working scaffolding and are not committed.
