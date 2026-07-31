// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! Allocation-count guard for the point-read hot path.
//!
//! Timings are meaningless in a noisy environment; allocation counts are
//! deterministic and machine-independent, so that is what this asserts.
//!
//! The regression this exists to catch: `TableWriter`'s read methods record
//! into the Serializable read set, and the read set is keyed by
//! [`PrimaryKey::hash64`]. Computing that digest *eagerly*, as an argument to
//! the recording helper, allocates a `Vec<u8>` (the default `hash64` hashes
//! `encode()`) on every call — including under the default
//! `SnapshotIsolation`, where the read set is `None` and the digest is thrown
//! away. `TableReader` never had the problem, which is what makes it a useful
//! control: the two must agree.
//!
//! The counter is a global allocator flag, so the tests here serialize on a
//! mutex for their whole bodies. That removes cross-attribution between them
//! but not from the libtest harness itself, which prints from another thread
//! while a test runs; under default `--test-threads` that shows up as a
//! handful of stray counts. The assertions are therefore stated as "far below
//! one per operation" rather than exactly zero. The per-call regression they
//! guard is a factor of N, so the margin is never in doubt — and with
//! `--test-threads=1` the observed counts are exactly 0.

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use ultima_db::{Store, StoreConfig, WriterMode};

struct Counting;

static COUNTING: AtomicBool = AtomicBool::new(false);
static ALLOCS: AtomicUsize = AtomicUsize::new(0);

unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        if COUNTING.load(Ordering::Relaxed) {
            ALLOCS.fetch_add(1, Ordering::Relaxed);
        }
        unsafe { System.alloc(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) }
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        if COUNTING.load(Ordering::Relaxed) {
            ALLOCS.fetch_add(1, Ordering::Relaxed);
        }
        unsafe { System.realloc(ptr, layout, new_size) }
    }
}

#[global_allocator]
static ALLOCATOR: Counting = Counting;

/// Serializes the tests in this binary. `COUNTING` is global, so it is not
/// enough to serialize the measured regions: another test's *setup* (building
/// keys, inserting rows) allocates too, and would be attributed to whichever
/// measurement happened to be open. Each test holds this for its whole body.
static MEASURING: std::sync::Mutex<()> = std::sync::Mutex::new(());

fn serialize_measurements() -> std::sync::MutexGuard<'static, ()> {
    MEASURING.lock().unwrap_or_else(|e| e.into_inner())
}

/// Run `body`, returning the number of allocations it made. The caller must
/// hold [`serialize_measurements`].
fn allocations(body: impl FnOnce()) -> usize {
    ALLOCS.store(0, Ordering::Relaxed);
    COUNTING.store(true, Ordering::Relaxed);
    body();
    COUNTING.store(false, Ordering::Relaxed);
    ALLOCS.load(Ordering::Relaxed)
}

const N: u64 = 100;

#[test]
fn point_reads_on_a_u64_table_do_not_allocate() {
    let _serialized = serialize_measurements();
    let store = Store::default();

    let ids: Vec<u64> = {
        let mut wtx = store.begin_write(None).unwrap();
        let mut t = wtx.open_table::<u64>("t").unwrap();
        let ids = (0..N).map(|i| t.insert(i).unwrap()).collect();
        drop(t);
        wtx.commit().unwrap();
        ids
    };

    // --- writer handle ---
    let mut wtx = store.begin_write(None).unwrap();
    let t = wtx.open_table::<u64>("t").unwrap();
    let writer_get = allocations(|| {
        for id in &ids {
            std::hint::black_box(t.get(*id));
        }
    });
    let writer_contains = allocations(|| {
        for id in &ids {
            std::hint::black_box(t.contains(*id));
        }
    });
    drop(t);
    drop(wtx);

    // --- reader handle (the control: never had a hash64 on this path) ---
    let rtx = store.begin_read(None).unwrap();
    let t = rtx.open_table::<u64>("t").unwrap();
    let reader_get = allocations(|| {
        for id in &ids {
            std::hint::black_box(t.get(*id));
        }
    });

    // Measured with N=100, release, --test-threads=1:
    //   pre-fix  reader_get=0 writer_get=100 writer_contains=100
    //   post-fix reader_get=0 writer_get=0   writer_contains=0
    let ceiling = (N / 10) as usize;
    assert!(
        reader_get <= ceiling,
        "TableReader::get x{N} allocated {reader_get} (control)"
    );
    assert!(
        writer_get <= ceiling,
        "TableWriter::get x{N} allocated {writer_get}; expected no per-call \
         allocation (a digest computed eagerly and discarded?)"
    );
    assert!(
        writer_contains <= ceiling,
        "TableWriter::contains x{N} allocated {writer_contains}"
    );
}

/// The MultiWriter mutation path calls `hash64` twice per write — once in
/// `claim_intent`, once in `WriteSetTracker::record` — so the encode-based
/// default showed up there even after the read path was fixed. Bounded rather
/// than zeroed: `update` legitimately allocates (the write set and intent map
/// grow, each `Arc<R>` is a new allocation). What must not happen is a
/// *per-key* digest allocation on top, so the bound is well under `2 x N`.
#[test]
fn multiwriter_updates_do_not_allocate_a_digest_per_key() {
    let _serialized = serialize_measurements();
    let store = Store::new(
        StoreConfig::builder()
            .writer_mode(WriterMode::MultiWriter)
            .build(),
    )
    .unwrap();

    let ids: Vec<u64> = {
        let mut wtx = store.begin_write(None).unwrap();
        let mut t = wtx.open_table::<u64>("t").unwrap();
        let ids = (0..N).map(|i| t.insert(i).unwrap()).collect();
        drop(t);
        wtx.commit().unwrap();
        ids
    };

    let mut wtx = store.begin_write(None).unwrap();
    let mut t = wtx.open_table::<u64>("t").unwrap();
    // Warm the intent map and write set so growth reallocations don't
    // dominate, then measure a second pass over the same keys.
    for id in &ids {
        t.update(*id, 0).unwrap();
    }
    let update_allocs = allocations(|| {
        for id in &ids {
            t.update(*id, 1).unwrap();
        }
    });

    // Two allocations per update are structural and pre-date this task: the
    // `Arc<R>` for the new row, and the `String` key `IntentMap::try_acquire`
    // builds (`src/intents.rs:182`). The two `hash64` calls per update —
    // `claim_intent` and `WriteSetTracker::record` — added two more.
    // Measured with N=100, release, --test-threads=1: 402 pre-fix, 204 post-fix.
    let ceiling = 2 * N as usize + 20;
    assert!(
        update_allocs <= ceiling,
        "MultiWriter update x{N} allocated {update_allocs} (ceiling {ceiling}); \
         expected no per-key digest allocation on top of the structural two"
    );
}

/// Keyed tables get the same treatment: `String` and tuple keys hash their
/// borrowed bytes / compose their elements' digests rather than round-tripping
/// through `encode()`.
#[test]
fn keyed_point_reads_do_not_allocate() {
    let _serialized = serialize_measurements();
    let store = Store::default();
    let keys: Vec<String> = (0..N).map(|i| format!("key-{i:04}")).collect();
    let tuple_keys: Vec<(u32, String)> =
        (0..N).map(|i| (i as u32, format!("c{i:04}"))).collect();

    {
        let mut wtx = store.begin_write(None).unwrap();
        let mut t = wtx.open_table_keyed::<u64, String>("s").unwrap();
        for (i, k) in keys.iter().enumerate() {
            t.put(k.clone(), i as u64).unwrap();
        }
        drop(t);
        let mut t = wtx.open_table_keyed::<u64, (u32, String)>("c").unwrap();
        for (i, k) in tuple_keys.iter().enumerate() {
            t.put(k.clone(), i as u64).unwrap();
        }
        drop(t);
        wtx.commit().unwrap();
    }

    let mut wtx = store.begin_write(None).unwrap();
    let t = wtx.open_table_keyed::<u64, String>("s").unwrap();
    let string_get = allocations(|| {
        for k in &keys {
            std::hint::black_box(t.get(k));
        }
    });
    drop(t);
    let t = wtx.open_table_keyed::<u64, (u32, String)>("c").unwrap();
    let tuple_get = allocations(|| {
        for k in &tuple_keys {
            std::hint::black_box(t.get(k));
        }
    });

    // Measured with N=100, release, --test-threads=1:
    //   pre-fix  string_get=100 tuple_get=400 (the tuple encode allocates per
    //            element plus the output buffer)
    //   post-fix string_get=0   tuple_get=0
    let ceiling = (N / 10) as usize;
    assert!(
        string_get <= ceiling,
        "String-keyed get x{N} allocated {string_get}"
    );
    assert!(
        tuple_get <= ceiling,
        "(u32, String)-keyed get x{N} allocated {tuple_get}"
    );
}
