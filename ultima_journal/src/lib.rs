// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! `ultima_journal` — segmented append journal + StableValue primitive.

pub mod durability;
pub mod error;
pub mod notifier;
pub mod journal;
#[cfg(feature = "stable_value")]
pub mod stable_value;

pub use durability::Durability;
#[cfg(feature = "stable_value")]
pub use error::StableValueError;
pub use error::JournalError;
pub use journal::{Journal, JournalConfig};
pub use notifier::Notifier;
#[cfg(feature = "stable_value")]
pub use stable_value::{StableValue, StableValueConfig};
