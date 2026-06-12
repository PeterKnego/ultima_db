// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! FROZEN conformance floor for the `journal-commit` autobench task.
//! The optimization loop must never edit this file. Each test is a
//! behavioral invariant; CRC trailers catch torn/corrupted payloads.
//! Frozen-paths enforcement lives in autobench/program.md (loop discipline); a mechanical hash check is deliberately deferred.

use ultima_journal::{Durability, Journal, JournalConfig};

/// payload = 56 bytes derived from seq + 4-byte crc32 trailer.
fn payload_for(seq: u64) -> Vec<u8> {
    let mut p = Vec::with_capacity(60);
    for i in 0..7u64 {
        p.extend_from_slice(&(seq.wrapping_mul(i + 1)).to_le_bytes());
    }
    let crc = crc32fast::hash(&p);
    p.extend_from_slice(&crc.to_le_bytes());
    p
}

fn verify_payload(seq: u64, payload: &[u8]) {
    let (body, trailer) = payload.split_at(payload.len() - 4);
    let crc = u32::from_le_bytes(trailer.try_into().unwrap());
    assert_eq!(crc, crc32fast::hash(body), "torn/corrupt payload at seq {seq}");
    assert_eq!(payload, payload_for(seq).as_slice(), "wrong payload at seq {seq}");
}

fn open(dir: &std::path::Path, durability: Durability) -> Journal {
    let mut cfg = JournalConfig::new(dir);
    cfg.durability = durability;
    Journal::open(cfg).unwrap()
}

/// Like `open`, but with a small segment so multiple segments roll over —
/// required to exercise `purge_before`, which works at full-segment
/// granularity (see `journal/mod.rs:420` `purge_before`).
fn open_small_segments(dir: &std::path::Path, durability: Durability, seg_bytes: u64) -> Journal {
    let mut cfg = JournalConfig::new(dir);
    cfg.durability = durability;
    cfg.segment_size_bytes = seg_bytes;
    Journal::open(cfg).unwrap()
}

#[test]
fn fifo_round_trip_no_loss_no_torn_reads() {
    let dir = tempfile::tempdir().unwrap();
    let j = open(dir.path(), Durability::Eventual);
    const N: u64 = 10_000;
    let mut last = None;
    for seq in 1..=N {
        last = Some(j.append(seq, seq * 2, &payload_for(seq)).unwrap());
    }
    last.unwrap().wait().unwrap();
    j.wait_durable(N).unwrap();
    // read_range returns Vec<(seq, meta, payload)> (journal/mod.rs:305).
    let recs = j.read_range(1..=N).unwrap();
    assert_eq!(recs.len(), N as usize, "fifo: lost records");
    for (i, (seq_got, meta, payload)) in recs.iter().enumerate() {
        let seq = i as u64 + 1;
        assert_eq!(*seq_got, seq, "fifo: seq mismatch / out of order at index {i}");
        assert_eq!(*meta, seq * 2, "fifo: meta mismatch at {seq}");
        verify_payload(seq, payload);
    }
    j.close().unwrap();

    // Reopen and verify persistence: records must survive across journal close/open.
    let j = open(dir.path(), Durability::Eventual);
    assert_eq!(j.last_seq(), Some(N), "fifo: last_seq wrong after reopen");
    verify_payload(1, &j.read(1).unwrap().unwrap().1);
    verify_payload(N / 2, &j.read(N / 2).unwrap().unwrap().1);
    verify_payload(N, &j.read(N).unwrap().unwrap().1);
}

#[test]
fn truncate_after_drops_tail_and_survives_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let j = open(dir.path(), Durability::Consistent);
    for seq in 1..=100u64 {
        j.append(seq, 0, &payload_for(seq)).unwrap().wait().unwrap();
    }
    // truncate_after(keep_seq) retains seq <= keep_seq (journal/mod.rs:359).
    j.truncate_after(60).unwrap().wait().unwrap();
    assert_eq!(j.last_seq(), Some(60), "truncate: last_seq wrong before reopen");
    assert!(j.read(61).unwrap().is_none(), "truncate: truncated record still readable");
    verify_payload(60, &j.read(60).unwrap().unwrap().1);
    j.close().unwrap();

    let j = open(dir.path(), Durability::Consistent);
    assert_eq!(j.last_seq(), Some(60), "truncate: truncate lost across reopen");
    assert_eq!(j.first_seq(), Some(1), "truncate: head must be preserved by a tail truncation");
    verify_payload(60, &j.read(60).unwrap().unwrap().1);
    // appending after reopen continues cleanly
    j.append(61, 0, &payload_for(61)).unwrap().wait().unwrap();
    assert_eq!(j.last_seq(), Some(61), "truncate: append after reopen failed");
}

#[test]
fn purge_before_drops_head_and_survives_reopen() {
    let dir = tempfile::tempdir().unwrap();
    // `purge_before(seq)` drops full segments whose final record's seq <= `seq`
    // and never drops the active (last) segment (journal/mod.rs:420-463). It is
    // segment-granular, not record-granular, so we use small segments to force
    // several rollovers, then assert at segment granularity: the head advances
    // past seq 1, some sub-threshold records are dropped, and everything above
    // the purge point stays readable and intact.
    let j = open_small_segments(dir.path(), Durability::Consistent, 1024);
    for seq in 1..=200u64 {
        j.append(seq, 0, &payload_for(seq)).unwrap().wait().unwrap();
    }
    assert_eq!(j.first_seq(), Some(1), "purge: initial first_seq should be 1");

    j.purge_before(100).unwrap();
    let first_after = j.first_seq().unwrap();
    // Head advanced: at least the very first record's whole segment was dropped.
    assert!(first_after > 1, "purge: purge dropped nothing (head still at seq 1)");
    // Never drops past the active segment / never removes records above seq.
    assert!(
        first_after <= 100,
        "purge: dropped records at or beyond the threshold: first_seq={first_after}"
    );
    // The dropped head is gone; the surviving head and the tail are intact.
    assert!(j.read(1).unwrap().is_none(), "purge: purged head record still readable");
    verify_payload(first_after, &j.read(first_after).unwrap().unwrap().1);
    verify_payload(200, &j.read(200).unwrap().unwrap().1);
    assert_eq!(j.last_seq(), Some(200), "purge: last_seq wrong before reopen");

    // Verify mid-window records before reopen (catches mid-segment corruption).
    if first_after + 10 <= 200 {
        verify_payload(first_after + 10, &j.read(first_after + 10).unwrap().unwrap().1);
    }
    if first_after <= 150 {
        verify_payload(150, &j.read(150).unwrap().unwrap().1);
    }

    j.close().unwrap();

    let j = open_small_segments(dir.path(), Durability::Consistent, 1024);
    assert_eq!(j.first_seq(), Some(first_after), "purge: purge lost across reopen");
    assert!(j.read(1).unwrap().is_none(), "purge: purge lost across reopen");
    verify_payload(first_after, &j.read(first_after).unwrap().unwrap().1);
    verify_payload(200, &j.read(200).unwrap().unwrap().1);
    assert_eq!(j.last_seq(), Some(200), "purge: last_seq wrong after reopen");

    // Verify mid-window records after reopen (catches mid-segment corruption from rewritten purge path).
    if first_after + 10 <= 200 {
        verify_payload(first_after + 10, &j.read(first_after + 10).unwrap().unwrap().1);
    }
    if first_after <= 150 {
        verify_payload(150, &j.read(150).unwrap().unwrap().1);
    }
}

#[test]
fn eventual_durability_watermark_clamps_and_catches_up() {
    let dir = tempfile::tempdir().unwrap();
    let j = open(dir.path(), Durability::Eventual);
    for seq in 1..=1000u64 {
        j.append(seq, 0, &payload_for(seq)).unwrap();
    }
    // Capture durable_seq immediately after the append loop, before any explicit
    // wait. The upper bound (<=1000) catches watermark over-advancement — a real
    // bug class where the background fsync ticker advances past what was written.
    // We deliberately do NOT assert d0 < 1000 because the background fsync may
    // legitimately have completed already by this point.
    let d0 = j.durable_seq();
    assert!(
        d0 <= 1000,
        "eventual: durable_seq overran appended seqs (watermark advanced past what was written)"
    );
    j.wait_durable(1000).unwrap();
    assert!(j.durable_seq() >= 1000, "eventual: wait_durable returned before durability");
}

#[test]
fn consistent_append_is_immediately_durable() {
    let dir = tempfile::tempdir().unwrap();
    let j = open(dir.path(), Durability::Consistent);
    for seq in 1..=50u64 {
        j.append(seq, 0, &payload_for(seq)).unwrap().wait().unwrap();
        assert!(j.durable_seq() >= seq, "consistent: ack before fsync at {seq}");
    }
}

#[test]
fn seq_monotonicity_reads_match_window() {
    let dir = tempfile::tempdir().unwrap();
    let j = open(dir.path(), Durability::Eventual);
    for seq in 1..=500u64 {
        j.append(seq, 0, &payload_for(seq)).unwrap();
    }
    j.wait_durable(500).unwrap();
    assert_eq!(j.first_seq(), Some(1), "window: first_seq should be 1");
    assert_eq!(j.last_seq(), Some(500), "window: last_seq should be 500");
    let recs = j.read_range(250..=260).unwrap();
    assert_eq!(recs.len(), 11, "window: wrong number of records in range");
    for (offset, (seq_got, _meta, payload)) in recs.iter().enumerate() {
        let seq = 250 + offset as u64;
        assert_eq!(*seq_got, seq, "window: out-of-window or out-of-order seq");
        verify_payload(seq, payload);
    }
}
