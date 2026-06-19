// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! `ultima_journal` — segmented append journal + StableValue primitive.

#[cfg(feature = "bench-support")]
#[doc(hidden)]
pub mod bench_support;
pub mod durability;
pub mod error;
pub mod journal;
pub mod notifier;
#[cfg(feature = "stable_value")]
pub mod stable_value;

pub use durability::Durability;
pub use error::JournalError;
#[cfg(feature = "stable_value")]
pub use error::StableValueError;
pub use journal::{Journal, JournalConfig};
pub use notifier::Notifier;
#[cfg(feature = "stable_value")]
pub use stable_value::{StableValue, StableValueConfig};
