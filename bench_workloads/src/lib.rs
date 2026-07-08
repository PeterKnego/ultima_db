// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! Shared workload generators (YCSB, SmallBank) used by the first-party
//! benches, the competitor baselines in `compare_benches`, and the autobench
//! harness. Extracted from the former `benches/{ycsb,smallbank}_common.rs`
//! `#[path]` includes.

pub mod smallbank;
pub mod ycsb;

pub mod bulk_load;
