// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! Shared helpers for integration tests.
//!
//! `test_scratch` is `#[path]`-included from `src/test_scratch.rs` so there is
//! a single source of truth shared with the crate's unit tests — see the module
//! docs there for why durability tests must not run on tmpfs.

#[path = "../../src/test_scratch.rs"]
pub mod test_scratch;
