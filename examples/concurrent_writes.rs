// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! Concurrent writes across multiple threads using `WriterMode::MultiWriter`.
//!
//! `Store` is `Send + Sync`, so each thread holds a `store.clone()` and opens
//! its own `WriteTx` locally. Under table-level optimistic concurrency
//! control, any two writers touching the same table will conflict — the
//! loser retries, rebasing onto the winner's snapshot.
//!
//! Run with: `cargo run --example concurrent_writes`

use std::sync::{Arc, Barrier};
use std::thread;
use ultima_db::{Error, Store, StoreConfig, WriterMode};

const NUM_THREADS: usize = 8;
const WRITES_PER_THREAD: u64 = 50;

fn main() {
    let store = Store::new(StoreConfig {
        writer_mode: WriterMode::MultiWriter,
        ..StoreConfig::default()
    })
    .unwrap();

    // Seed one record per thread so every writer has a key to update.
    {
        let mut wtx = store.begin_write(None).unwrap();
        let mut t = wtx.open_table::<u64>("counters").unwrap();
        for _ in 0..NUM_THREADS {
            t.insert(0).unwrap();
        }
        wtx.commit().unwrap();
    }

    let barrier = Arc::new(Barrier::new(NUM_THREADS));
    let handles: Vec<_> = (0..NUM_THREADS as u64)
        .map(|tid| {
            let store = store.clone();
            let barrier = Arc::clone(&barrier);
            thread::spawn(move || {
                barrier.wait();
                let mut retries = 0u64;
                for n in 0..WRITES_PER_THREAD {
                    loop {
                        let mut wtx = store.begin_write(None).unwrap();
                        {
                            let mut t = wtx.open_table::<u64>("counters").unwrap();
                            // Increment our thread's counter.
                            let cur = t.get(tid + 1).copied().unwrap_or(0);
                            t.update(tid + 1, cur + 1).unwrap();
                        }
                        match wtx.commit() {
                            Ok(_) => break,
                            Err(Error::WriteConflict { .. }) => {
                                retries += 1;
                                continue;
                            }
                            Err(e) => panic!("unexpected: {e}"),
                        }
                    }
                    let _ = n;
                }
                (tid, retries)
            })
        })
        .collect();

    let mut total_retries = 0u64;
    for h in handles {
        let (tid, retries) = h.join().unwrap();
        println!("thread {tid}: {retries} retries");
        total_retries += retries;
    }
    println!(
        "---\n{NUM_THREADS} threads, {WRITES_PER_THREAD} writes each, {total_retries} total retries"
    );

    // Every counter should equal WRITES_PER_THREAD despite concurrent writes.
    let rtx = store.begin_read(None).unwrap();
    let t = rtx.open_table::<u64>("counters").unwrap();
    for tid in 0..NUM_THREADS as u64 {
        let v = t.get(tid + 1).copied().unwrap();
        assert_eq!(v, WRITES_PER_THREAD, "thread {tid} lost writes");
        println!("counter[{}] = {v}", tid + 1);
    }
    println!("all counters correct");
}
