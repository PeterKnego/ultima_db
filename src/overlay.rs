// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! Bounded write overlay: the small sorted write-front that absorbs a
//! table's mutations so each commit's copy-on-write is a bounded memcpy of
//! at most `OVERLAY_CAP` entries instead of a B-tree node-clone chain. See
//! docs/superpowers/specs/2026-08-03-write-overlay-design.md.

use std::sync::Arc;

/// Default capacity. Bench-tunable via `ULTIMA_OVERLAY_CAP` at table-enable
/// time (bench escape hatch, not a public knob — task57 precedent).
#[allow(dead_code)]
pub(crate) const OVERLAY_CAP: usize = 128;

#[allow(dead_code)]
pub(crate) enum OverlayOp<R> {
	/// A live row. `tree_resident` records whether the key also exists in
	/// the backing tree — it decides delete behavior (remove vs tombstone)
	/// and the `len_delta` bookkeeping.
	Put { rec: Arc<R>, tree_resident: bool },
	/// Shadows a tree-resident row. Never written for overlay-born rows,
	/// which is what makes flush's `remove_mut` infallible.
	Tombstone,
}

impl<R> Clone for OverlayOp<R> {
	fn clone(&self) -> Self {
		match self {
			OverlayOp::Put { rec, tree_resident } => OverlayOp::Put {
				rec: Arc::clone(rec),
				tree_resident: *tree_resident,
			},
			OverlayOp::Tombstone => OverlayOp::Tombstone,
		}
	}
}

#[allow(dead_code)]
pub(crate) struct Overlay<R, K> {
	/// Sorted by key. Shared with snapshots via the Arc; a write after a
	/// commit CoWs the whole Vec — bounded by `cap`, that IS the design.
	entries: Arc<Vec<(K, OverlayOp<R>)>>,
	len_delta: i64,
	cap: usize,
}

impl<R, K> Clone for Overlay<R, K> {
	fn clone(&self) -> Self {
		Overlay {
			entries: Arc::clone(&self.entries),
			len_delta: self.len_delta,
			cap: self.cap,
		}
	}
}

#[allow(dead_code)]
impl<R, K: Ord + Clone> Overlay<R, K> {
	pub(crate) fn new(cap: usize) -> Self {
		Overlay { entries: Arc::new(Vec::new()), len_delta: 0, cap }
	}

	pub(crate) fn enabled(&self) -> bool { self.cap > 0 }
	pub(crate) fn is_empty(&self) -> bool { self.entries.is_empty() }
	pub(crate) fn is_full(&self) -> bool { self.cap > 0 && self.entries.len() >= self.cap }
	pub(crate) fn len_delta(&self) -> i64 { self.len_delta }
	pub(crate) fn entries(&self) -> &[(K, OverlayOp<R>)] { &self.entries }

	pub(crate) fn get(&self, key: &K) -> Option<&OverlayOp<R>> {
		self.entries
			.binary_search_by(|(k, _)| k.cmp(key))
			.ok()
			.map(|i| &self.entries[i].1)
	}

	pub(crate) fn set_put(&mut self, key: K, rec: Arc<R>, tree_resident: bool) {
		let entries = Arc::make_mut(&mut self.entries);
		match entries.binary_search_by(|(k, _)| k.cmp(&key)) {
			Ok(i) => {
				if matches!(entries[i].1, OverlayOp::Tombstone) {
					self.len_delta += 1; // Tomb -> Put: row comes back
				}
				// A replaced Put keeps its original residency: the tree's
				// state for this key hasn't changed since the first Put.
				let resident = match entries[i].1 {
					OverlayOp::Put { tree_resident, .. } => tree_resident,
					OverlayOp::Tombstone => true,
				};
				entries[i].1 = OverlayOp::Put { rec, tree_resident: resident };
			}
			Err(i) => {
				if !tree_resident {
					self.len_delta += 1;
				}
				entries.insert(i, (key, OverlayOp::Put { rec, tree_resident }));
			}
		}
	}

	pub(crate) fn set_tombstone(&mut self, key: K) {
		let entries = Arc::make_mut(&mut self.entries);
		match entries.binary_search_by(|(k, _)| k.cmp(&key)) {
			Ok(i) => {
				debug_assert!(
					matches!(entries[i].1, OverlayOp::Put { tree_resident: true, .. }),
					"tombstone over overlay-born Put — caller must remove_entry instead"
				);
				entries[i].1 = OverlayOp::Tombstone;
				self.len_delta -= 1;
			}
			Err(i) => {
				entries.insert(i, (key, OverlayOp::Tombstone));
				self.len_delta -= 1;
			}
		}
	}

	pub(crate) fn remove_entry(&mut self, key: &K) {
		let entries = Arc::make_mut(&mut self.entries);
		if let Ok(i) = entries.binary_search_by(|(k, _)| k.cmp(key)) {
			debug_assert!(
				matches!(entries[i].1, OverlayOp::Put { tree_resident: false, .. }),
				"remove_entry is only for overlay-born rows"
			);
			entries.remove(i);
			self.len_delta -= 1;
		}
	}

	/// Hand the frozen entries to a flush and reset to empty.
	pub(crate) fn take_entries(&mut self) -> Arc<Vec<(K, OverlayOp<R>)>> {
		self.len_delta = 0;
		std::mem::replace(&mut self.entries, Arc::new(Vec::new()))
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use std::sync::Arc;

	fn put(o: &mut Overlay<String, u64>, k: u64, v: &str, resident: bool) {
		o.set_put(k, Arc::new(v.to_string()), resident);
	}

	#[test]
	fn entries_stay_sorted_and_replace_in_place() {
		let mut o: Overlay<String, u64> = Overlay::new(8);
		put(&mut o, 5, "e", false);
		put(&mut o, 1, "a", false);
		put(&mut o, 3, "c", false);
		put(&mut o, 3, "c2", false); // replace, not duplicate
		let keys: Vec<u64> = o.entries().iter().map(|(k, _)| *k).collect();
		assert_eq!(keys, vec![1, 3, 5]);
		assert!(matches!(o.get(&3), Some(OverlayOp::Put { rec, .. }) if rec.as_str() == "c2"));
	}

	#[test]
	fn len_delta_follows_the_transition_table() {
		let mut o: Overlay<String, u64> = Overlay::new(8);
		put(&mut o, 1, "new", false);       // none -> Put(!resident): +1
		assert_eq!(o.len_delta(), 1);
		put(&mut o, 2, "upd", true);        // none -> Put(resident): 0
		assert_eq!(o.len_delta(), 1);
		o.set_tombstone(3);                 // none -> Tomb: -1
		assert_eq!(o.len_delta(), 0);
		put(&mut o, 3, "back", true);       // Tomb -> Put: +1
		assert_eq!(o.len_delta(), 1);
		o.remove_entry(&1);                 // overlay-born delete: -1
		assert_eq!(o.len_delta(), 0);
		o.set_tombstone(2);                 // Put(resident) -> Tomb: -1
		assert_eq!(o.len_delta(), -1);
		put(&mut o, 2, "again", true);      // Put stays Put after Tomb->Put
		assert_eq!(o.len_delta(), 0);
	}

	#[test]
	fn clone_is_cow_and_mutation_after_clone_does_not_leak() {
		let mut o: Overlay<String, u64> = Overlay::new(8);
		put(&mut o, 1, "a", false);
		let frozen = o.clone();
		put(&mut o, 2, "b", false);
		assert_eq!(frozen.entries().len(), 1, "frozen clone must not see later writes");
		assert_eq!(o.entries().len(), 2);
	}

	#[test]
	fn cap_zero_is_disabled_and_never_full() {
		let o: Overlay<String, u64> = Overlay::new(0);
		assert!(!o.enabled());
		assert!(!o.is_full());
		assert!(o.is_empty());
	}

	#[test]
	fn take_entries_resets_state() {
		let mut o: Overlay<String, u64> = Overlay::new(8);
		put(&mut o, 1, "a", false);
		o.set_tombstone(2);
		let drained = o.take_entries();
		assert_eq!(drained.len(), 2);
		assert!(o.is_empty());
		assert_eq!(o.len_delta(), 0);
	}
}
