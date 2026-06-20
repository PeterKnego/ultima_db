# Task 3 Report: `PreallocFileSink` (append/sync with inline grow-ahead)

## Status: DONE

## RED Evidence

Three failing tests added to `src/wal.rs` tests module:
- `prealloc_sink_roundtrips_like_buffered`
- `prealloc_sink_extends_in_chunks_and_holds_invariant`
- `prealloc_sink_steady_state_does_not_grow_physical`

Running `cargo test --features persistence --lib "wal::tests::prealloc_sink"` produced:

```
error[E0433]: cannot find type `PreallocFileSink` in this scope
    --> src/wal.rs:2502:24
error[E0433]: cannot find type `PreallocFileSink` in this scope
    --> src/wal.rs:2516:24
error[E0433]: cannot find type `PreallocFileSink` in this scope
    --> src/wal.rs:2534:24
error: could not compile `ultima-db` (lib test) due to 3 previous errors
```

## GREEN Evidence

After implementing `PreallocFileSink` and its `WalSink` impl:

```
running 3 tests
test wal::tests::prealloc_sink_extends_in_chunks_and_holds_invariant ... ok
test wal::tests::prealloc_sink_steady_state_does_not_grow_physical ... ok
test wal::tests::prealloc_sink_roundtrips_like_buffered ... ok

test result: ok. 3 passed; 0 failed; 0 ignored; 0 measured; 350 filtered out; finished in 0.02s
```

## Full WAL Suite

`cargo test --features persistence --lib "wal::"` — 45 passed, 0 failed, no regressions.

## Clippy

`cargo clippy --features persistence -- -D warnings` — zero warnings, clean.

## Implementation Details

Added to `src/wal.rs` after `BufferedFileSink`:

- `const WAL_PREALLOC_CHUNK: u64 = 16 * 1024 * 1024` — default grow quantum
- `struct PreallocFileSink { file, path, buf, write_head, capacity, chunk }` — production preallocating sink
- `fn open(dir)` — delegates to `open_with_chunk` with default chunk
- `pub(crate) fn open_with_chunk(dir, chunk)` — opens file in read+write mode (NOT O_APPEND), calls `scan_wal(path, true)` to reconstruct `write_head`, reads physical len for `capacity`
- `impl WalSink`: `append` buffers framed bytes; `sync` extends via `preallocate_to` when `write_head + buf.len() > capacity` (using `div_ceil` for chunk alignment), does positioned write at `write_head`, advances `write_head`, clears buf, calls `sync_data` for steady-state
- `prune` not overridden — falls through to `WalSink` default that returns `Err(Persistence("prune not supported"))`

Invariant maintained: `write_head <= capacity <= physical_len(file)`, `capacity % chunk == 0` at all times.
