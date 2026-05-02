// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

use std::io;

pub struct SnapshotReader { /* TODO */ }

impl io::Read for SnapshotReader {
    fn read(&mut self, _buf: &mut [u8]) -> io::Result<usize> {
        Ok(0)
    }
}
