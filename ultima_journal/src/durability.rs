// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Durability {
    /// Caller blocks (or callback fires) only after fsync completes.
    Consistent,
    /// Caller returns immediately; fsync happens asynchronously.
    Eventual,
}
