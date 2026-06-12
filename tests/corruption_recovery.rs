// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! Corruption-injection recovery tests: damage the WAL / checkpoint files the
//! way real crashes and bit rot do, and pin what `Store::recover()` does.
//!
//! The contract under test:
//! - **Torn writes** (truncated tail, zero-filled tail, garbage tail) are a
//!   normal crash artifact: recovery silently recovers the durable prefix.
//! - **Mid-file or in-entry corruption** (bit flips) is NOT a crash artifact:
//!   recovery must fail loudly with `WalCorrupted` / `CheckpointCorrupted`
//!   rather than silently dropping committed data.

#![cfg(feature = "persistence")]

use std::fs::OpenOptions;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;

use ultima_db::{Durability, Error, Persistence, Store, StoreConfig, WalWrite};

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
struct User {
    name: String,
    age: u32,
}

fn standalone_config(dir: &Path) -> StoreConfig {
    StoreConfig {
        persistence: Persistence::Standalone {
            dir: dir.to_path_buf(),
            durability: Durability::Eventual,
            wal_write: WalWrite::PerEntry,
        },
        ..StoreConfig::default()
    }
}

/// Create a store, write `n` commits (one user per commit), and drop it —
/// the WAL background thread flushes and fsyncs everything on drop.
fn seed_commits(dir: &Path, n: u64) {
    let store = Store::new(standalone_config(dir)).unwrap();
    store.register_table::<User>("users").unwrap();
    store.recover().unwrap();
    for i in 0..n {
        let mut wtx = store.begin_write(None).unwrap();
        wtx.open_table::<User>("users")
            .unwrap()
            .insert(User {
                name: format!("u{i}"),
                age: i as u32,
            })
            .unwrap();
        wtx.commit().unwrap();
    }
}

/// Open a fresh store over `dir` and run recovery, returning the result and
/// the store (usable only if recovery succeeded).
fn try_recover(dir: &Path) -> (Store, Result<(), Error>) {
    let store = Store::new(standalone_config(dir)).unwrap();
    store.register_table::<User>("users").unwrap();
    let res = store.recover();
    (store, res)
}

fn recovered_user_count(store: &Store) -> usize {
    let rtx = store.begin_read(None).unwrap();
    match rtx.open_table::<User>("users") {
        Ok(t) => t.len(),
        Err(_) => 0,
    }
}

fn wal_path(dir: &Path) -> std::path::PathBuf {
    dir.join("wal.bin")
}

fn file_len(path: &Path) -> u64 {
    std::fs::metadata(path).unwrap().len()
}

fn truncate_file(path: &Path, len: u64) {
    let f = OpenOptions::new().write(true).open(path).unwrap();
    f.set_len(len).unwrap();
    f.sync_all().unwrap();
}

fn flip_byte_at(path: &Path, offset: u64) {
    let mut f = OpenOptions::new().read(true).write(true).open(path).unwrap();
    f.seek(SeekFrom::Start(offset)).unwrap();
    let mut b = [0u8; 1];
    f.read_exact(&mut b).unwrap();
    b[0] ^= 0xFF;
    f.seek(SeekFrom::Start(offset)).unwrap();
    f.write_all(&b).unwrap();
    f.sync_all().unwrap();
}

fn append_bytes(path: &Path, bytes: &[u8]) {
    let mut f = OpenOptions::new().append(true).open(path).unwrap();
    f.write_all(bytes).unwrap();
    f.sync_all().unwrap();
}

// ---------------------------------------------------------------------------
// WAL: torn writes (crash artifacts) → silent prefix recovery
// ---------------------------------------------------------------------------

/// Crash mid-write leaves the last entry truncated: recovery keeps the
/// durable prefix and the store remains fully usable.
#[test]
fn wal_truncated_mid_last_entry_recovers_prefix() {
    let dir = tempfile::tempdir().unwrap();
    seed_commits(dir.path(), 3);

    let wal = wal_path(dir.path());
    // Cut into the last entry (its trailing CRC and some data).
    truncate_file(&wal, file_len(&wal) - 5);

    let (store, res) = try_recover(dir.path());
    res.expect("torn tail is a crash artifact, not corruption");
    assert_eq!(recovered_user_count(&store), 2, "durable prefix expected");

    // The store accepts new writes after prefix recovery.
    let mut wtx = store.begin_write(None).unwrap();
    wtx.open_table::<User>("users")
        .unwrap()
        .insert(User {
            name: "post".into(),
            age: 99,
        })
        .unwrap();
    wtx.commit().unwrap();
    assert_eq!(recovered_user_count(&store), 3);
}

/// Crash after writing only part of the next entry's length prefix.
#[test]
fn wal_truncated_in_length_prefix_recovers_prefix() {
    let dir = tempfile::tempdir().unwrap();
    seed_commits(dir.path(), 3);

    let wal = wal_path(dir.path());
    let full = file_len(&wal);
    // Find the start of the last entry by reading lengths from the front.
    let mut bytes = Vec::new();
    OpenOptions::new()
        .read(true)
        .open(&wal)
        .unwrap()
        .read_to_end(&mut bytes)
        .unwrap();
    let mut offset = 0usize;
    let mut last_entry_start = 0usize;
    while offset + 4 <= bytes.len() {
        let len = u32::from_le_bytes(bytes[offset..offset + 4].try_into().unwrap()) as usize;
        if offset + 4 + len + 4 > bytes.len() {
            break;
        }
        last_entry_start = offset;
        offset += 4 + len + 4;
    }
    // Keep only 2 bytes of the last entry's length prefix.
    assert!(last_entry_start > 0);
    truncate_file(&wal, last_entry_start as u64 + 2);
    assert!(file_len(&wal) < full);

    let (store, res) = try_recover(dir.path());
    res.expect("partial length prefix is a torn write");
    assert_eq!(recovered_user_count(&store), 2);
}

/// A zero-filled tail (pre-sized file, crash before the write reached disk)
/// reads as a clean end-of-log.
#[test]
fn wal_zero_tail_recovers_all_entries() {
    let dir = tempfile::tempdir().unwrap();
    seed_commits(dir.path(), 3);

    append_bytes(&wal_path(dir.path()), &[0u8; 64]);

    let (store, res) = try_recover(dir.path());
    res.expect("zero tail is a clean end-of-log");
    assert_eq!(recovered_user_count(&store), 3);
}

/// A garbage tail whose bytes parse as an absurd length is indistinguishable
/// from a torn write: the prefix is recovered.
#[test]
fn wal_garbage_tail_recovers_prefix() {
    let dir = tempfile::tempdir().unwrap();
    seed_commits(dir.path(), 3);

    // 0xAB×8 → len = 0xABABABAB, far beyond EOF → treated as torn.
    append_bytes(&wal_path(dir.path()), &[0xAB; 8]);

    let (store, res) = try_recover(dir.path());
    res.expect("garbage tail parses as torn write");
    assert_eq!(recovered_user_count(&store), 3);
}

// ---------------------------------------------------------------------------
// WAL: real corruption (bit rot) → loud failure, never silent loss
// ---------------------------------------------------------------------------

/// A flipped byte inside the FIRST entry's payload must fail recovery with
/// `WalCorrupted` — entries after the damage cannot be trusted, and silently
/// recovering a partial prefix would present data loss as success.
#[test]
fn wal_bitflip_in_first_entry_fails_with_wal_corrupted() {
    let dir = tempfile::tempdir().unwrap();
    seed_commits(dir.path(), 3);

    // Byte 4..8 is the first entry's length prefix; byte 8+ is its payload.
    flip_byte_at(&wal_path(dir.path()), 8);

    let (_store, res) = try_recover(dir.path());
    assert!(
        matches!(res, Err(Error::WalCorrupted(_))),
        "mid-file bit flip must fail loudly, got {res:?}"
    );
}

/// A flipped byte inside the LAST entry's payload is bit rot, not a torn
/// write (the frame is complete): recovery must fail with `WalCorrupted`
/// rather than silently dropping a committed entry.
#[test]
fn wal_bitflip_in_last_entry_fails_with_wal_corrupted() {
    let dir = tempfile::tempdir().unwrap();
    seed_commits(dir.path(), 3);

    let wal = wal_path(dir.path());
    // Last 4 bytes are the final entry's CRC; -6 lands in its payload.
    flip_byte_at(&wal, file_len(&wal) - 6);

    let (_store, res) = try_recover(dir.path());
    assert!(
        matches!(res, Err(Error::WalCorrupted(_))),
        "in-entry bit rot must fail loudly, got {res:?}"
    );
}

// ---------------------------------------------------------------------------
// Checkpoint corruption
// ---------------------------------------------------------------------------

fn latest_checkpoint(dir: &Path) -> std::path::PathBuf {
    let mut best: Option<(u64, std::path::PathBuf)> = None;
    for e in std::fs::read_dir(dir).unwrap().filter_map(|e| e.ok()) {
        let name = e.file_name().to_string_lossy().to_string();
        if let Some(v) = name
            .strip_prefix("checkpoint_")
            .and_then(|s| s.strip_suffix(".bin"))
            .and_then(|s| s.parse::<u64>().ok())
            && best.as_ref().is_none_or(|(bv, _)| v > *bv)
        {
            best = Some((v, e.path()));
        }
    }
    best.expect("no checkpoint found").1
}

/// Seed commits and write a checkpoint (which prunes the WAL).
fn seed_with_checkpoint(dir: &Path, n: u64) {
    let store = Store::new(standalone_config(dir)).unwrap();
    store.register_table::<User>("users").unwrap();
    store.recover().unwrap();
    for i in 0..n {
        let mut wtx = store.begin_write(None).unwrap();
        wtx.open_table::<User>("users")
            .unwrap()
            .insert(User {
                name: format!("u{i}"),
                age: i as u32,
            })
            .unwrap();
        wtx.commit().unwrap();
    }
    store.checkpoint().unwrap();
}

/// A flipped byte anywhere in the checkpoint fails its whole-file CRC:
/// recovery must fail with `CheckpointCorrupted`, not load garbage state.
#[test]
fn checkpoint_bitflip_fails_with_checkpoint_corrupted() {
    let dir = tempfile::tempdir().unwrap();
    seed_with_checkpoint(dir.path(), 3);

    let cp = latest_checkpoint(dir.path());
    flip_byte_at(&cp, file_len(&cp) / 2);

    let (_store, res) = try_recover(dir.path());
    assert!(
        matches!(res, Err(Error::CheckpointCorrupted(_))),
        "checkpoint bit flip must fail loudly, got {res:?}"
    );
}

/// A checkpoint truncated mid-file (crash *should* be impossible thanks to
/// tmp+rename, so this is corruption) fails CRC validation.
#[test]
fn checkpoint_truncated_fails_with_checkpoint_corrupted() {
    let dir = tempfile::tempdir().unwrap();
    seed_with_checkpoint(dir.path(), 3);

    let cp = latest_checkpoint(dir.path());
    truncate_file(&cp, file_len(&cp) / 2);

    let (_store, res) = try_recover(dir.path());
    assert!(
        matches!(res, Err(Error::CheckpointCorrupted(_))),
        "truncated checkpoint must fail loudly, got {res:?}"
    );
}

/// A checkpoint reduced to a few bytes fails the minimum-size check.
#[test]
fn checkpoint_too_short_fails_with_checkpoint_corrupted() {
    let dir = tempfile::tempdir().unwrap();
    seed_with_checkpoint(dir.path(), 2);

    let cp = latest_checkpoint(dir.path());
    truncate_file(&cp, 6);

    let (_store, res) = try_recover(dir.path());
    assert!(
        matches!(res, Err(Error::CheckpointCorrupted(_))),
        "tiny checkpoint must fail loudly, got {res:?}"
    );
}

/// A stray `.tmp` file from a crash mid-checkpoint (before the atomic
/// rename) is ignored; recovery uses the last complete checkpoint.
#[test]
fn stray_checkpoint_tmp_is_ignored() {
    let dir = tempfile::tempdir().unwrap();
    seed_with_checkpoint(dir.path(), 3);

    std::fs::write(dir.path().join("checkpoint_99.bin.tmp"), b"partial garbage").unwrap();

    let (store, res) = try_recover(dir.path());
    res.expect("stray .tmp must not affect recovery");
    assert_eq!(recovered_user_count(&store), 3);
}

/// End-to-end: valid checkpoint + WAL with commits on top, where the last
/// WAL entry is torn — recovery is checkpoint + the durable WAL prefix.
#[test]
fn checkpoint_plus_torn_wal_tail_recovers_durable_prefix() {
    let dir = tempfile::tempdir().unwrap();
    seed_with_checkpoint(dir.path(), 2); // checkpoint covers u0, u1

    // Two more commits land in the WAL only.
    {
        let store = Store::new(standalone_config(dir.path())).unwrap();
        store.register_table::<User>("users").unwrap();
        store.recover().unwrap();
        for i in 2..4u64 {
            let mut wtx = store.begin_write(None).unwrap();
            wtx.open_table::<User>("users")
                .unwrap()
                .insert(User {
                    name: format!("u{i}"),
                    age: i as u32,
                })
                .unwrap();
            wtx.commit().unwrap();
        }
    }

    // Tear the last WAL entry.
    let wal = wal_path(dir.path());
    truncate_file(&wal, file_len(&wal) - 5);

    let (store, res) = try_recover(dir.path());
    res.expect("checkpoint + torn WAL tail must recover");
    assert_eq!(
        recovered_user_count(&store),
        3,
        "checkpoint (2) + durable WAL prefix (1) expected"
    );
}
