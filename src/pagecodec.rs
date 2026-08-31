// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! Encode/decode one [`BTreeNode`] to/from a page payload, and
//! [`PagedSource`] — the [`NodeSource`] that reads pages off a [`PageFile`]
//! and decodes them.
//!
//! A table's data tree and each of its indexes all page through the same
//! four-byte-header [`PageFile`], but their *values* are encoded
//! differently: a data leaf's value is a bincode-serialized [`Record`], a
//! unique index's value is a row key (`RK::encode`), and a non-unique
//! index's value is `()` (uniqueness lives in the composite key, not the
//! value). A type can implement both `Record` and `PrimaryKey` (`u64`
//! does), so that choice cannot be a blanket trait impl on `V` — it has to
//! be picked by the caller, once, at construction. [`ValueCodec`] carries
//! that choice as a pair of plain function pointers (no captured state, no
//! `Box<dyn Fn>` allocation) so [`NodeCodec::records`],
//! [`NodeCodec::unique_index`], and [`NodeCodec::non_unique_index`] can each
//! build one without a trait to implement per shape.
//!
//! Payload layout (both leaf and inner; inner appends child ids):
//! `n u16 LE | (key_len u16 LE, key, val_len u32 LE, val)[n] | [child_id u64
//! LE](n+1 if inner)`.

use std::marker::PhantomData;
use std::sync::{Arc, OnceLock};
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};

use crate::btree::BTreeNode;
use crate::child::{Child, NodeSource, PageId};
use crate::pagefile::{PageFile, PageKind};
use crate::persistence::Record;
use crate::primary_key::{PrimaryKey, check_encoded_key_len};
use crate::{Error, Result};

/// How to encode/decode one node's *values* (not its keys — those always go
/// through [`PrimaryKey::encode`]/`decode`). Plain function pointers, not
/// `Box<dyn Fn>`: the three constructors below never capture state, so a
/// pointer is both cheaper and lets `NodeCodec` stay `Copy`-free but
/// allocation-free to build.
pub(crate) struct ValueCodec<V> {
    enc: fn(&V) -> Result<Vec<u8>>,
    dec: fn(&[u8]) -> Result<V>,
}

// Manual `Clone`/`Copy`-free derive would add spurious `V: Clone` bounds on
// the fn-pointer fields (which don't need it); write it by hand instead.
impl<V> Clone for ValueCodec<V> {
    fn clone(&self) -> Self {
        ValueCodec { enc: self.enc, dec: self.dec }
    }
}

/// Encodes/decodes one [`BTreeNode<K, V>`] to/from a page payload, and picks
/// the [`PageKind`] (data vs. index, leaf vs. inner) it belongs under.
///
/// Built via [`Self::records`], [`Self::unique_index`], or
/// [`Self::non_unique_index`] — never by naming the fields directly, since
/// the right value encoding depends on which of those three shapes a table
/// or index needs, not on `V`'s own trait impls (see the module doc).
pub(crate) struct NodeCodec<K, V> {
    value: ValueCodec<V>,
    /// Selects `Index{Leaf,Inner}` over `Data{Leaf,Inner}`.
    index: bool,
    _k: PhantomData<K>,
}

impl<K, V> Clone for NodeCodec<K, V> {
    fn clone(&self) -> Self {
        NodeCodec { value: self.value.clone(), index: self.index, _k: PhantomData }
    }
}

/// Build an `Error::CheckpointCorrupted` naming the byte offset a decode
/// failed at — every truncation/overrun path below goes through this so a
/// malformed payload is always an `Err`, never a panic or an OOB read.
// No production caller yet — these are `NodeCodec::decode`'s internals, and
// that has no production caller yet either (see the note on the impl block
// below). Exercised today by this module's own unit tests.
#[allow(dead_code)]
fn corrupt(offset: usize, what: &str) -> Error {
    Error::CheckpointCorrupted(format!("page payload: {what} truncated at offset {offset}"))
}

/// Read a little-endian `u16` at `*at`, advancing `*at` past it.
// No production caller yet — see `corrupt` above.
#[allow(dead_code)]
fn read_u16(buf: &[u8], at: &mut usize) -> Result<u16> {
    let end = at.checked_add(2).ok_or_else(|| corrupt(*at, "u16 field"))?;
    let bytes: [u8; 2] = buf.get(*at..end).ok_or_else(|| corrupt(*at, "u16 field"))?.try_into().unwrap();
    *at = end;
    Ok(u16::from_le_bytes(bytes))
}

/// Read a little-endian `u32` at `*at`, advancing `*at` past it.
// No production caller yet — see `corrupt` above.
#[allow(dead_code)]
fn read_u32(buf: &[u8], at: &mut usize) -> Result<u32> {
    let end = at.checked_add(4).ok_or_else(|| corrupt(*at, "u32 field"))?;
    let bytes: [u8; 4] = buf.get(*at..end).ok_or_else(|| corrupt(*at, "u32 field"))?.try_into().unwrap();
    *at = end;
    Ok(u32::from_le_bytes(bytes))
}

/// Read a little-endian `u64` at `*at`, advancing `*at` past it.
// No production caller yet — see `corrupt` above.
#[allow(dead_code)]
fn read_u64(buf: &[u8], at: &mut usize) -> Result<u64> {
    let end = at.checked_add(8).ok_or_else(|| corrupt(*at, "u64 field"))?;
    let bytes: [u8; 8] = buf.get(*at..end).ok_or_else(|| corrupt(*at, "u64 field"))?.try_into().unwrap();
    *at = end;
    Ok(u64::from_le_bytes(bytes))
}

/// Read `len` bytes at `*at`, advancing `*at` past them.
// No production caller yet — see `corrupt` above.
#[allow(dead_code)]
fn read_bytes<'a>(buf: &'a [u8], at: &mut usize, len: usize) -> Result<&'a [u8]> {
    let end = at.checked_add(len).ok_or_else(|| corrupt(*at, "byte field"))?;
    let s = buf.get(*at..end).ok_or_else(|| corrupt(*at, "byte field"))?;
    *at = end;
    Ok(s)
}

// No production caller yet — a later task wires `NodeCodec` into a table's
// or index's `BTree::source` for checkpoint-backed pages (see
// `PagedSource`'s note below). Exercised today by this module's own unit
// tests.
#[allow(dead_code)]
impl<K: PrimaryKey, V> NodeCodec<K, V> {
    /// A data table's codec: values are bincode-serialized [`Record`]s,
    /// pages are `Data{Leaf,Inner}`.
    pub(crate) fn records<R: Record>() -> NodeCodec<K, R> {
        NodeCodec {
            value: ValueCodec {
                enc: |v: &R| -> Result<Vec<u8>> {
                    bincode::serde::encode_to_vec(v, bincode::config::standard())
                        .map_err(|e| Error::Persistence(format!("encode page value: {e}")))
                },
                dec: |bytes: &[u8]| -> Result<R> {
                    bincode::serde::decode_from_slice(bytes, bincode::config::standard())
                        .map(|(v, _read): (R, usize)| v)
                        .map_err(|e| Error::Persistence(format!("decode page value: {e}")))
                },
            },
            index: false,
            _k: PhantomData,
        }
    }

    /// A unique index's codec: the value is the row's primary key (encoded
    /// with `RK::encode`/`decode`, not bincode), pages are
    /// `Index{Leaf,Inner}`.
    pub(crate) fn unique_index<IK: PrimaryKey, RK: PrimaryKey>() -> NodeCodec<IK, RK> {
        NodeCodec {
            value: ValueCodec {
                enc: |k: &RK| -> Result<Vec<u8>> { Ok(k.encode()) },
                dec: RK::decode,
            },
            index: true,
            _k: PhantomData,
        }
    }

    /// A non-unique index's codec: uniqueness lives in the composite key
    /// `(IK, RK)`, so the value carries nothing — it encodes to zero bytes
    /// and decodes back to `()`. Pages are `Index{Leaf,Inner}`.
    pub(crate) fn non_unique_index<IK: PrimaryKey, RK: PrimaryKey>() -> NodeCodec<(IK, RK), ()> {
        NodeCodec {
            value: ValueCodec {
                enc: |_: &()| -> Result<Vec<u8>> { Ok(Vec::new()) },
                dec: |_: &[u8]| -> Result<()> { Ok(()) },
            },
            index: true,
            _k: PhantomData,
        }
    }

    /// Encode one node to its page payload and the [`PageKind`] it belongs
    /// under (leaf/inner decided by `node.children.is_empty()`, data/index
    /// by which constructor built `self`).
    ///
    /// Fails if any child slot lacks a page id: an inner node with a dirty
    /// (resident, `NO_PAGE`) child cannot be named by this page — the
    /// caller must checkpoint that child first.
    pub(crate) fn encode(&self, node: &BTreeNode<K, V>) -> Result<(PageKind, Vec<u8>)> {
        let n = node.entries.len();
        // `FixedVec::is_empty` is private to `btree` (only `len` is
        // pub(crate)); clippy can't see that restriction and suggests a
        // method this module cannot call.
        #[allow(clippy::len_zero)]
        let is_inner = node.children.len() != 0;
        let mut buf = Vec::with_capacity(2 + n * 16);
        let n16 = u16::try_from(n)
            .map_err(|_| Error::Persistence(format!("page payload: {n} entries, over u16 range")))?;
        buf.extend_from_slice(&n16.to_le_bytes());
        for i in 0..n {
            let (k, v) = &node.entries[i];
            let kb = k.encode();
            check_encoded_key_len(kb.len(), "page payload")?;
            let klen = u16::try_from(kb.len())
                .map_err(|_| Error::Persistence(format!("page payload: key encodes to {} bytes, over u16 range", kb.len())))?;
            buf.extend_from_slice(&klen.to_le_bytes());
            buf.extend_from_slice(&kb);
            let vb = (self.value.enc)(v)?;
            let vlen = u32::try_from(vb.len())
                .map_err(|_| Error::Persistence(format!("page payload: value encodes to {} bytes, over u32 range", vb.len())))?;
            buf.extend_from_slice(&vlen.to_le_bytes());
            buf.extend_from_slice(&vb);
        }
        if is_inner {
            for i in 0..node.children.len() {
                let id = node.children[i]
                    .page_id()
                    .ok_or_else(|| Error::Persistence(format!("page payload: child slot {i} has no page id (dirty)")))?;
                buf.extend_from_slice(&id.to_le_bytes());
            }
        }
        let kind = match (self.index, is_inner) {
            (false, false) => PageKind::DataLeaf,
            (false, true) => PageKind::DataInner,
            (true, false) => PageKind::IndexLeaf,
            (true, true) => PageKind::IndexInner,
        };
        Ok((kind, buf))
    }

    /// Decode a page payload back into a node. `kind` names the page's
    /// leaf/inner shape (and, redundantly with `self.index`, its data/index
    /// origin — a mismatch there is itself reported as corruption, since it
    /// means this payload was read with the wrong table's codec).
    ///
    /// Never panics on malformed bytes: every length read is bounds-checked
    /// against the remaining payload before being trusted, a short read
    /// anywhere returns `Err(Error::CheckpointCorrupted)` naming the offset,
    /// and trailing bytes left over after a structurally well-formed parse
    /// (garbage appended past a valid payload) are rejected the same way —
    /// a well-formed prefix is not a well-formed payload.
    pub(crate) fn decode(&self, kind: PageKind, payload: &[u8]) -> Result<BTreeNode<K, V>> {
        let is_index_kind = matches!(kind, PageKind::IndexLeaf | PageKind::IndexInner);
        if is_index_kind != self.index {
            return Err(Error::CheckpointCorrupted(format!(
                "page payload: kind {kind:?} does not match codec (data vs. index mismatch)"
            )));
        }
        let is_inner = matches!(kind, PageKind::DataInner | PageKind::IndexInner);
        let mut at = 0usize;
        let n = read_u16(payload, &mut at)? as usize;
        let mut entries = Vec::with_capacity(n);
        for _ in 0..n {
            let key_len = read_u16(payload, &mut at)? as usize;
            let key_bytes = read_bytes(payload, &mut at, key_len)?;
            let key = K::decode(key_bytes)?;
            let val_len = read_u32(payload, &mut at)? as usize;
            let val_bytes = read_bytes(payload, &mut at, val_len)?;
            let val = (self.value.dec)(val_bytes)?;
            entries.push((key, Arc::new(val)));
        }
        let mut children = Vec::new();
        if is_inner {
            for _ in 0..=n {
                let id = read_u64(payload, &mut at)?;
                children.push(Child::on_disk(id));
            }
        }
        if at != payload.len() {
            return Err(Error::CheckpointCorrupted(format!(
                "page payload: {} trailing byte(s) after offset {at}",
                payload.len() - at
            )));
        }
        Ok(BTreeNode { entries: entries.into_iter().collect(), children: children.into_iter().collect() })
    }
}

/// Per-tree paging counters, shared (via `Arc`) between a `PagedSource` and
/// whatever reports them (metrics, tests). All relaxed counters — these are
/// statistics, not synchronization.
#[derive(Default)]
pub(crate) struct PagedStats {
    /// Total pages faulted in by [`PagedSource::read_node`] (data + index).
    // No production caller yet — see `PagedSource`'s note below.
    #[allow(dead_code)]
    pub page_faults: AtomicU64,
    /// Of `page_faults`, how many were `Data{Leaf,Inner}` pages.
    // No production caller yet — Task 13's paging-metrics surface reads
    // this split; added now so that later task needs no schema change.
    #[allow(dead_code)]
    pub data_page_faults: AtomicU64,
    /// Of `page_faults`, how many were `Index{Leaf,Inner}` pages.
    // No production caller yet — see `data_page_faults` above.
    #[allow(dead_code)]
    pub index_page_faults: AtomicU64,
    /// Bytes reported dirty via `note_dirty` (a clean node CoW'd by
    /// `Child::make_mut`).
    // No production caller yet — see `PagedSource`'s note below.
    #[allow(dead_code)]
    pub dirty_bytes: AtomicU64,
    /// Signed running total of resident leaf bytes (grows on fault-in via
    /// [`PagedSource::read_node`], shrinks on demotion via
    /// [`crate::table::MergeableTable::paged_demote`]) — the memory-budget
    /// trigger's input (task12) and the estimate `Store::paged_stats`
    /// reports. An estimate, not an exact count: a leaf built and written
    /// directly (never faulted in through `read_node`, e.g. a freshly
    /// inserted-then-checkpointed row) contributes nothing on the way in
    /// but is still decremented on the way out if later demoted, so this
    /// can and does run negative — `Store::paged_stats` clamps to `0` on
    /// read rather than reporting the raw (nonsensical, wrapped-looking)
    /// negative as a `u64`.
    pub resident_leaf_bytes: AtomicI64,
    /// Pages written by a later task's checkpoint writer.
    // No production caller yet — see `resident_leaf_bytes` above.
    #[allow(dead_code)]
    pub pages_written: AtomicU64,
    /// Leaves demoted back to on-disk by a later task's evictor.
    // No production caller yet — see `resident_leaf_bytes` above.
    #[allow(dead_code)]
    pub leaves_demoted: AtomicU64,
    /// Dead-page byte ranges actually hole-punched (task11) — counted in
    /// ranges, not bytes, matching `PagedRoot::dead_pages`'s own unit. Only
    /// incremented once a range's retention gate clears (the root that
    /// named it as dead is no longer the newest surviving predecessor —
    /// see `Store::checkpoint_impl_paged`'s punch step), never at the point
    /// a range is merely computed and recorded in a root's `dead_pages`.
    pub dead_pages_punched: AtomicU64,
    /// Dead-page ranges dropped before ever reaching `punch` (fix round 1,
    /// I-1) because their claimed `(offset, len)` extent reached past the
    /// page file's current logical end — a defense against a corrupted
    /// length that was already-stored (possibly checkpoints ago) rather
    /// than re-derived at punch time. A range counted here is never
    /// punched and never retried: it is permanently abandoned, leaking
    /// that much disk space rather than risking a destructive
    /// `fallocate` over live data.
    pub dead_pages_dropped: AtomicU64,
    /// Number of times the background checkpointer thread (task12,
    /// `crate::store::Checkpointer`) has actually invoked a checkpoint —
    /// bumped once per `Store::checkpoint_impl` call the thread itself
    /// initiates, whether or not that call succeeds. Counts *attempts* by
    /// the thread, not application-driven `Store::checkpoint()` calls, and
    /// not "work done" the way `pages_written`/`leaves_demoted` do.
    pub checkpointer_runs: AtomicU64,
    /// The background checkpointer's wake signal (task12): `(has_work,
    /// condvar)`. Set exactly once, by `Store::new`'s paged branch,
    /// immediately after the checkpointer thread is spawned — every paged
    /// store starts one (see `crate::store::Checkpointer::start`), so this
    /// is `Some` for the whole lifetime of a `PagedStats` a live
    /// `PagedSource`/commit path can ever observe. [`Self::wake_checkpointer`]
    /// is the only thing that should call through this — everything else
    /// should treat it as an opaque handle.
    pub wake: OnceLock<Arc<(parking_lot::Mutex<bool>, parking_lot::Condvar)>>,
    /// This store's `PagedOptions::memory_budget_bytes`, mirrored here
    /// (task12) so [`PagedSource::read_node`] can wake the checkpointer on
    /// a leaf fault that crosses the budget without needing access to
    /// `PagedOptions` itself (only `Store::new`, which sees both, can set
    /// this). `None` iff no budget is configured — `read_node` then never
    /// calls [`Self::wake_checkpointer`] for that reason, matching
    /// `checkpoint_impl_paged`'s own "no budget, never demote" rule.
    pub mem_budget_bytes: OnceLock<u64>,
}

impl PagedStats {
    /// Notify the background checkpointer thread's condvar that there may
    /// be work to do, if one is wired up ([`Self::wake`] is set — true for
    /// the whole life of any `PagedStats` a caller outside `Store::new` can
    /// reach). A no-op otherwise; harmless (parking_lot's `notify_one` with
    /// no waiter parked is a no-op) if the thread happens to already be
    /// awake or mid-shutdown.
    pub(crate) fn wake_checkpointer(&self) {
        if let Some(w) = self.wake.get() {
            let mut has_work = w.0.lock();
            *has_work = true;
            w.1.notify_one();
        }
    }

    /// Atomically subtract `amount` from `dirty_bytes`, clamping at `0`
    /// instead of wrapping. Used by `checkpoint_impl_paged`'s phase 2 to
    /// undo the `note_dirty` credit for exactly the bytes *this* checkpoint
    /// just wrote — not a blind reset to `0`, since a concurrent writer can
    /// dirty more nodes while phases 1-2 are still running, and those bytes
    /// must not be lost. A plain `fetch_sub` would wrap `AtomicU64` on
    /// underflow (e.g. if the running total briefly reads lower than
    /// `amount` due to relaxed-ordering interleaving) into a huge bogus
    /// value that would then permanently pin the dirty-bytes trigger on;
    /// clamping avoids that.
    pub(crate) fn subtract_dirty_bytes(&self, amount: u64) {
        let mut cur = self.dirty_bytes.load(Ordering::Relaxed);
        loop {
            let new = cur.saturating_sub(amount);
            match self.dirty_bytes.compare_exchange_weak(cur, new, Ordering::Relaxed, Ordering::Relaxed) {
                Ok(_) => return,
                Err(actual) => cur = actual,
            }
        }
    }
}

/// A [`NodeSource`] that reads pages off a [`PageFile`] and decodes them
/// through a [`NodeCodec`], counting faults and dirty bytes into a shared
/// [`PagedStats`].
// No production caller yet — a later task wires this into `BTree::source`
// for a checkpoint-backed table/index. Exercised today by this module's own
// unit tests.
#[allow(dead_code)]
pub(crate) struct PagedSource<K, V> {
    pub(crate) file: Arc<PageFile>,
    pub(crate) codec: NodeCodec<K, V>,
    pub(crate) name: String,
    pub(crate) stats: Arc<PagedStats>,
}

impl<K: PrimaryKey, V: Send + Sync + 'static> NodeSource<K, V> for PagedSource<K, V> {
    fn read_node(&self, id: PageId) -> Result<Arc<BTreeNode<K, V>>> {
        let (kind, bytes) = self.file.read(id)?;
        self.stats.page_faults.fetch_add(1, Ordering::Relaxed);
        match kind {
            PageKind::DataLeaf | PageKind::DataInner => {
                self.stats.data_page_faults.fetch_add(1, Ordering::Relaxed);
            }
            PageKind::IndexLeaf | PageKind::IndexInner => {
                self.stats.index_page_faults.fetch_add(1, Ordering::Relaxed);
            }
        }
        // Only a *data leaf* fault-in grows `resident_leaf_bytes`: that
        // counter tracks the data tree's demotable leaves specifically
        // (`BTree::demote_leaves`/`resident_leaf_estimate` never touch
        // inner nodes or index trees), so counting inner/index fault-ins
        // here would inflate the estimate against a demote pass that can
        // never claim those bytes back.
        if kind == PageKind::DataLeaf {
            let prev = self
                .stats
                .resident_leaf_bytes
                .fetch_add(Child::<K, V>::NODE_BYTES as i64, Ordering::Relaxed);
            // Wake the background checkpointer (task12) the moment this
            // fault-in pushes resident bytes at/over the memory budget —
            // only when a budget is actually configured (`mem_budget_bytes`
            // is set iff `PagedOptions::memory_budget_bytes` is `Some`; see
            // its doc). Cheaper than notifying on every fault: a read-only
            // store that never crosses the budget never wakes the thread
            // early at all, just falls back to the interval poll.
            if let Some(&budget) = self.stats.mem_budget_bytes.get() {
                let resident = (prev + Child::<K, V>::NODE_BYTES as i64).max(0) as u64;
                if resident >= budget {
                    self.stats.wake_checkpointer();
                }
            }
        }
        Ok(Arc::new(self.codec.decode(kind, &bytes)?))
    }

    fn note_dirty(&self, bytes: usize) {
        self.stats.dirty_bytes.fetch_add(bytes as u64, Ordering::Relaxed);
    }

    fn name(&self) -> &str {
        &self.name
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::btree::BTreeNode;
    use crate::child::Child;

    #[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
    struct Row {
        a: u64,
        s: String,
    }

    fn leaf<K: Clone, V>(pairs: Vec<(K, V)>) -> BTreeNode<K, V> {
        BTreeNode { entries: pairs.into_iter().map(|(k, v)| (k, Arc::new(v))).collect(), children: Default::default() }
    }

    #[test]
    fn records_leaf_roundtrip() {
        let c = NodeCodec::<u64, Row>::records::<Row>();
        let n = leaf(vec![(1, Row { a: 1, s: "x".into() }), (2, Row { a: 2, s: "yy".into() })]);
        let (kind, bytes) = c.encode(&n).unwrap();
        assert_eq!(kind, PageKind::DataLeaf);
        let back = c.decode(kind, &bytes).unwrap();
        assert_eq!(back.entries.len(), 2);
        assert_eq!(*back.entries[1].1, Row { a: 2, s: "yy".into() });
    }

    #[test]
    fn inner_node_carries_values_and_child_ids() {
        let c = NodeCodec::<String, Row>::records::<Row>();
        let mut n = leaf(vec![("m".to_string(), Row { a: 9, s: "".into() })]);
        n.children.push(Child::on_disk(100));
        n.children.push(Child::on_disk(200));
        let (kind, bytes) = c.encode(&n).unwrap();
        assert_eq!(kind, PageKind::DataInner);
        let back = c.decode(kind, &bytes).unwrap();
        assert_eq!(back.children.len(), 2);
        assert_eq!(back.children[0].page_id(), Some(100));
        assert_eq!(back.children[1].page_id(), Some(200));
        assert!(!back.children[0].is_loaded());
        assert_eq!(back.entries[0].1.a, 9);
    }

    #[test]
    fn encode_refuses_dirty_child() {
        let c = NodeCodec::<u64, Row>::records::<Row>();
        let mut n = leaf(vec![(1, Row { a: 1, s: "".into() })]);
        n.children.push(Child::resident(Arc::new(leaf(vec![]))));
        n.children.push(Child::on_disk(1));
        assert!(c.encode(&n).is_err(), "a child without a page id cannot be named");
    }

    #[test]
    fn index_kinds_and_every_key_type() {
        let u = NodeCodec::<String, u64>::unique_index::<String, u64>();
        let n = leaf(vec![("a".to_string(), 5u64)]);
        let (k, b) = u.encode(&n).unwrap();
        assert_eq!(k, PageKind::IndexLeaf);
        assert_eq!(*u.decode(k, &b).unwrap().entries[0].1, 5);

        let nu = NodeCodec::<(i32, Vec<u8>), ()>::non_unique_index::<i32, Vec<u8>>();
        let n = leaf(vec![((-3, vec![1, 2]), ())]);
        let (k, b) = nu.encode(&n).unwrap();
        assert_eq!(nu.decode(k, &b).unwrap().entries[0].0, (-3, vec![1, 2]));

        // tuples, u128, i8 through the records codec with unit values
        let t = NodeCodec::<(u128, i8, String), ()>::records::<()>();
        let n = leaf(vec![((u128::MAX, -1, "z".into()), ())]);
        let (k, b) = t.encode(&n).unwrap();
        assert_eq!(t.decode(k, &b).unwrap().entries[0].0.1, -1);
    }

    #[test]
    fn paged_source_reads_and_counts() {
        let d = tempfile::tempdir().unwrap();
        let pf = Arc::new(crate::pagefile::PageFile::open(&crate::pagefile::page_file_path(d.path()), 0, 1 << 16, 4096).unwrap());
        let codec = NodeCodec::<u64, Row>::records::<Row>();
        let n = leaf(vec![(1, Row { a: 1, s: "".into() })]);
        let (k, b) = codec.encode(&n).unwrap();
        let id = pf.append(k, &b).unwrap();
        let stats = Arc::new(PagedStats::default());
        let src = PagedSource { file: pf, codec, name: "t".into(), stats: stats.clone() };
        let back = crate::child::NodeSource::read_node(&src, id).unwrap();
        assert_eq!(back.entries.len(), 1);
        assert_eq!(stats.page_faults.load(std::sync::atomic::Ordering::Relaxed), 1);
        assert_eq!(stats.data_page_faults.load(std::sync::atomic::Ordering::Relaxed), 1);
        assert_eq!(stats.index_page_faults.load(std::sync::atomic::Ordering::Relaxed), 0);
    }

    /// Decode-robustness: truncate a valid payload at every possible length
    /// and feed each prefix to `decode`. None may panic; every one short of
    /// the full length must be an `Err` (a prefix can never happen to
    /// re-parse as a different, still-valid payload here: the full payload
    /// ends with fixed-width child ids, and the leaf case's tail is a
    /// variable-length value with no trailing structure to misparse as
    /// complete).
    #[test]
    fn decode_rejects_every_truncation_without_panicking() {
        let c = NodeCodec::<String, Row>::records::<Row>();
        let mut n = leaf(vec![
            ("a".to_string(), Row { a: 1, s: "hello".into() }),
            ("b".to_string(), Row { a: 2, s: "world!!".into() }),
        ]);
        n.children.push(Child::on_disk(10));
        n.children.push(Child::on_disk(20));
        n.children.push(Child::on_disk(30));
        let (kind, bytes) = c.encode(&n).unwrap();
        assert!(c.decode(kind, &bytes).is_ok(), "sanity: the full payload decodes");
        for len in 0..bytes.len() {
            let prefix = &bytes[..len];
            let result = c.decode(kind, prefix);
            assert!(result.is_err(), "truncation at {len}/{} bytes must be Err, decode of a short payload succeeded", bytes.len());
        }
    }

    /// `decode` must reject trailing garbage appended after a structurally
    /// well-formed payload — a valid prefix is not a valid payload.
    /// Regression for the missing "fully consumed" check: covers both a
    /// leaf (no child ids) and an inner node (child ids present) payload,
    /// with both 1 and 7 extra bytes, and confirms the untouched payload
    /// still decodes fine either way.
    #[test]
    fn decode_rejects_trailing_bytes_leaf_and_inner() {
        let leaf_codec = NodeCodec::<u64, Row>::records::<Row>();
        let leaf_node = leaf(vec![(1, Row { a: 1, s: "x".into() }), (2, Row { a: 2, s: "yy".into() })]);
        let (leaf_kind, leaf_bytes) = leaf_codec.encode(&leaf_node).unwrap();
        assert!(leaf_codec.decode(leaf_kind, &leaf_bytes).is_ok(), "sanity: untouched leaf payload decodes");

        let inner_codec = NodeCodec::<String, Row>::records::<Row>();
        let mut inner_node = leaf(vec![("m".to_string(), Row { a: 9, s: "".into() })]);
        inner_node.children.push(Child::on_disk(100));
        inner_node.children.push(Child::on_disk(200));
        let (inner_kind, inner_bytes) = inner_codec.encode(&inner_node).unwrap();
        assert!(inner_codec.decode(inner_kind, &inner_bytes).is_ok(), "sanity: untouched inner payload decodes");

        for extra in [1usize, 7] {
            let mut with_garbage = leaf_bytes.clone();
            with_garbage.extend(std::iter::repeat_n(0xABu8, extra));
            match leaf_codec.decode(leaf_kind, &with_garbage) {
                Err(Error::CheckpointCorrupted(_)) => {}
                other => panic!("leaf +{extra} trailing byte(s): expected CheckpointCorrupted, got {}", match other {
                    Ok(_) => "Ok".to_string(),
                    Err(e) => format!("{e}"),
                }),
            }

            let mut with_garbage = inner_bytes.clone();
            with_garbage.extend(std::iter::repeat_n(0xCDu8, extra));
            match inner_codec.decode(inner_kind, &with_garbage) {
                Err(Error::CheckpointCorrupted(_)) => {}
                other => panic!("inner +{extra} trailing byte(s): expected CheckpointCorrupted, got {}", match other {
                    Ok(_) => "Ok".to_string(),
                    Err(e) => format!("{e}"),
                }),
            }
        }
    }
}
