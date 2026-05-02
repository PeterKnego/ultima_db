// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

use ultima_journal::{Durability, Journal, JournalConfig};

#[test]
fn end_to_end_append_read_truncate_purge_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let dir_path = dir.path().to_path_buf();
    let mut cfg = JournalConfig::new(&dir_path);
    cfg.segment_size_bytes = 1024;
    cfg.durability = Durability::Consistent;

    {
        let j = Journal::open(cfg.clone()).unwrap();
        for i in 1..=20u64 {
            j.append(i, i * 7, format!("payload-{i}").as_bytes()).unwrap().wait().unwrap();
        }
        assert_eq!(j.last_seq(), Some(20));
        let v = j.read_range(5..=15).unwrap();
        assert_eq!(v.len(), 11);
        assert_eq!(v[0].0, 5);
        assert_eq!(v[10].0, 15);

        j.truncate_after(15).unwrap().wait().unwrap();
        assert_eq!(j.last_seq(), Some(15));

        j.purge_before(10).unwrap();
    }
    let j2 = Journal::open(cfg).unwrap();
    assert!(j2.first_seq().unwrap() >= 1 && j2.first_seq().unwrap() <= 11);
    assert_eq!(j2.last_seq(), Some(15));
    assert_eq!(j2.read(15).unwrap().unwrap().1, b"payload-15".to_vec());
}
