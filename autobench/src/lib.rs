// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! Library half of the autobench harness. Binaries (`run-iter`,
//! `smr-apply-microbench`, `mw-commit-microbench`) are thin wrappers over
//! these modules so smoke tests can drive the same code with quick configs.
//!
//! The `journal-commit` task moved to `ultima_cluster/uc_autobench` alongside
//! the `ultima_journal` crate.

pub mod baseline;
pub mod diskcheck;
pub mod mw_commit_bench;
pub mod sampling;
pub mod smr_bench;
pub mod task_spec;
