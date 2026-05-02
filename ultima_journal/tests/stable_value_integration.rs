// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

use serde::{Deserialize, Serialize};
use std::fs;
use std::io::{Seek, SeekFrom, Write};
use ultima_journal::{StableValue, StableValueConfig};

#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Clone)]
struct V {
    x: u64,
}

#[test]
fn surviving_slot_chosen_after_truncation_corruption() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("v.sv");
    {
        let sv = StableValue::<V>::open(StableValueConfig::new(&path)).unwrap();
        sv.store(&V { x: 1 }).unwrap().wait().unwrap(); // slot 1, gen=1
        sv.store(&V { x: 2 }).unwrap().wait().unwrap(); // slot 0, gen=2
        sv.store(&V { x: 3 }).unwrap().wait().unwrap(); // slot 1, gen=3
    }

    // Get file metadata
    let file_size = fs::metadata(&path).unwrap().len() as u64;
    let header_size = 32u64;
    let slot_size = (file_size - header_size) / 2;

    // Corrupt slot 1 CRC (which has the highest gen=3)
    // So fallback picks slot 0 (gen=2 → V { x: 2 })
    let mut f = fs::OpenOptions::new().write(true).open(&path).unwrap();
    let slot1_crc_offset = header_size + 2 * slot_size - 4;
    f.seek(SeekFrom::Start(slot1_crc_offset)).unwrap();
    f.write_all(&[0x00, 0x00, 0x00, 0x00]).unwrap();
    drop(f);

    // Reopen — slot 1 corrupt → fall back to slot 0 (gen=2 → V { x: 2 }).
    let sv = StableValue::<V>::open(StableValueConfig::new(&path)).unwrap();
    assert_eq!(sv.load().unwrap(), Some(V { x: 2 }));
}
