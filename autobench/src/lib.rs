// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! Library half of the autobench harness. Binaries (`run-iter`,
//! `journal-microbench`, `smr-apply-microbench`) are thin wrappers over
//! these modules so smoke tests can drive the same code with quick configs.

pub mod baseline;
pub mod diskcheck;
pub mod journal_bench;
pub mod sampling;
pub mod smr_bench;
pub mod task_spec;
