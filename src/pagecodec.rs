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
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering};

use crate::btree::{BTreeNode, Value};
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
fn corrupt(offset: usize, what: &str) -> Error {
    Error::CheckpointCorrupted(format!("page payload: {what} truncated at offset {offset}"))
}

/// Read a little-endian `u16` at `*at`, advancing `*at` past it.
fn read_u16(buf: &[u8], at: &mut usize) -> Result<u16> {
    let end = at.checked_add(2).ok_or_else(|| corrupt(*at, "u16 field"))?;
    let bytes: [u8; 2] = buf.get(*at..end).ok_or_else(|| corrupt(*at, "u16 field"))?.try_into().unwrap();
    *at = end;
    Ok(u16::from_le_bytes(bytes))
}

/// Read a little-endian `u32` at `*at`, advancing `*at` past it.
fn read_u32(buf: &[u8], at: &mut usize) -> Result<u32> {
    let end = at.checked_add(4).ok_or_else(|| corrupt(*at, "u32 field"))?;
    let bytes: [u8; 4] = buf.get(*at..end).ok_or_else(|| corrupt(*at, "u32 field"))?.try_into().unwrap();
    *at = end;
    Ok(u32::from_le_bytes(bytes))
}

/// Read a little-endian `u64` at `*at`, advancing `*at` past it.
fn read_u64(buf: &[u8], at: &mut usize) -> Result<u64> {
    let end = at.checked_add(8).ok_or_else(|| corrupt(*at, "u64 field"))?;
    let bytes: [u8; 8] = buf.get(*at..end).ok_or_else(|| corrupt(*at, "u64 field"))?.try_into().unwrap();
    *at = end;
    Ok(u64::from_le_bytes(bytes))
}

/// Read `len` bytes at `*at`, advancing `*at` past them.
fn read_bytes<'a>(buf: &'a [u8], at: &mut usize, len: usize) -> Result<&'a [u8]> {
    let end = at.checked_add(len).ok_or_else(|| corrupt(*at, "byte field"))?;
    let s = buf.get(*at..end).ok_or_else(|| corrupt(*at, "byte field"))?;
    *at = end;
    Ok(s)
}

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
            let k = &node.entries[i].0;
            let kb = k.encode();
            check_encoded_key_len(kb.len(), "page payload")?;
            let klen = u16::try_from(kb.len())
                .map_err(|_| Error::Persistence(format!("page payload: key encodes to {} bytes, over u16 range", kb.len())))?;
            buf.extend_from_slice(&klen.to_le_bytes());
            buf.extend_from_slice(&kb);
            let vb = (self.value.enc)(node.value_at(i))?;
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
    /// trailing bytes left over after a structurally well-formed parse
    /// (garbage appended past a valid payload) are rejected the same way —
    /// a well-formed prefix is not a well-formed payload — and the decoded
    /// entry count `n` is bound-checked against this build's actual node
    /// capacity (`MAX_KEYS + 1`, I-2(a)) *before* anything is pushed into a
    /// `FixedVec`: an oversized `n` (a bit-flipped `u16`, or a `pages.bin`
    /// written under a different `fanout-t8` setting than this build's) is
    /// reported the same way rather than hitting `FixedVec::push`'s release
    /// assert.
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
        // Bound-check the decoded entry count against this build's actual
        // fanout *before* it drives any allocation or `FixedVec` push (I-2(a)):
        // `Entries`'s capacity is `MAX_KEYS + 1` (`Children`'s is `n + 1` of
        // that, `MAX_KEYS + 2`), so an `n` beyond that release-asserts inside
        // `FixedVec::push` — a panic, not the `Err` this function's own doc
        // promises for every malformed payload. Left unchecked, a bit-flipped
        // count (up to 65535, `u16`'s range) panics on the very first
        // overflowing push, and a `fanout-t8` build reading a `pages.bin`
        // written under the default T=32 fanout panics on its first
        // >=16-entry node even with byte-perfect, uncorrupted bytes.
        if n > crate::btree::MAX_KEYS + 1 {
            return Err(Error::CheckpointCorrupted(format!(
                "page payload: entry count {n} exceeds this build's node capacity \
                 ({} entries max — MAX_KEYS+1)",
                crate::btree::MAX_KEYS + 1
            )));
        }
        // Spec §5: only a data leaf (`PageKind::DataLeaf`) decodes into a
        // block leaf — `DataInner` keeps `Value::arc` (inner nodes are
        // always resident and carry Arc values, never blocks — spec §3),
        // and both index kinds are untouched (indexes never carry block
        // leaves; I-B). This function has no way to see whether the tree
        // it's decoding for even *has* a cloning source (`NodeCodec` is
        // generic over `K`/`V` only, not over a `PagedSource`) — that's
        // fine, because building a block here never needs to clone
        // anything: every value below comes straight out of
        // `(self.value.dec)`, already owned. The only place a block leaf's
        // values are ever cloned is a later CoW rebuild
        // (`BTreeNode::clone_with`), which *does* require a source whose
        // `clone_value` is `Some` — guaranteed for every paged data tree
        // reaching this decode path because `register_table_paged`
        // (Task 3) is the only way to attach one, and it requires `R:
        // Clone` up front (I-B holds by construction, not by anything
        // decode itself checks).
        let is_data_leaf = kind == PageKind::DataLeaf;
        let mut entries = Vec::with_capacity(n);
        let mut block: Vec<V> = if is_data_leaf { Vec::with_capacity(n) } else { Vec::new() };
        for _ in 0..n {
            let key_len = read_u16(payload, &mut at)? as usize;
            let key_bytes = read_bytes(payload, &mut at, key_len)?;
            let key = K::decode(key_bytes)?;
            let val_len = read_u32(payload, &mut at)? as usize;
            let val_bytes = read_bytes(payload, &mut at, val_len)?;
            let val = (self.value.dec)(val_bytes)?;
            if is_data_leaf {
                entries.push((key, Value::in_block()));
                block.push(val);
            } else {
                entries.push((key, Value::arc(Arc::new(val))));
            }
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
        let node = BTreeNode {
            entries: entries.into_iter().collect(),
            children: children.into_iter().collect(),
            block: if is_data_leaf { Some(block.into_boxed_slice()) } else { None },
        };
        // I-A, enforced at every block-leaf build site (spec §3): a node's
        // entries are all-Arc or all-in-block, never mixed. Cheap
        // (`entries.len()` bounded by `MAX_KEYS + 1`) and only runs under
        // `debug_assertions`.
        debug_assert!(
            (0..node.entries.len()).all(|i| node.entries[i].1.is_in_block() == is_data_leaf),
            "I-A violated: block leaf must have every entry in-block, non-block leaf none"
        );
        Ok(node)
    }
}

/// Per-tree paging counters, shared (via `Arc`) between a `PagedSource` and
/// whatever reports them (metrics, tests). All relaxed counters — these are
/// statistics, not synchronization.
#[derive(Default)]
pub(crate) struct PagedStats {
    /// Total pages faulted in by [`PagedSource::read_node`] (data + index).
    pub page_faults: AtomicU64,
    /// Of `page_faults`, how many were `Data{Leaf,Inner}` pages.
    pub data_page_faults: AtomicU64,
    /// Of `page_faults`, how many were `Index{Leaf,Inner}` pages.
    pub index_page_faults: AtomicU64,
    /// Bytes reported dirty via `note_dirty` (a clean node CoW'd by
    /// `Child::make_mut`).
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
    /// Pages written by the checkpoint writer.
    pub pages_written: AtomicU64,
    /// Leaves demoted back to on-disk by a demote pass.
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
    /// Set once the background checkpointer thread has caught a panic out of
    /// a `checkpoint_impl` call (final-review wave, I-5) — e.g. a corrupt
    /// on-disk page reached through the LAZY `Child::load`/`load_quiet` fault
    /// path during a dirty-node walk, which still panics rather than
    /// returning `Err` (see `Child::try_load`'s doc — only the EAGER
    /// recovery/attach loads were changed to `Err`). The thread wraps each
    /// iteration's checkpoint call in `std::panic::catch_unwind` specifically
    /// so one bad page cannot silently kill the whole background thread
    /// (leaving a paged store to accumulate dirty bytes/leaves forever with
    /// no checkpoint ever running again); on a caught panic this flag is set
    /// (sticky — never cleared back to `false`, since the underlying corrupt
    /// page does not go away on its own) and the loop continues to its next
    /// tick, where a `checkpoint_impl` that never touches the offending page
    /// again can still succeed. Surfaced to callers via
    /// [`crate::store::PagedStatsSnapshot::checkpointer_panicked`].
    pub checkpointer_panicked: AtomicBool,
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
    /// Edge-trigger gate for [`Self::wake_checkpointer`] (task12 fix round
    /// 1, IMPORTANT): `PagedSource::read_node` is a hot path — every leaf
    /// fault past a configured memory budget calls `wake_checkpointer`, and
    /// under sustained read pressure (a table that stays over budget) that
    /// could mean a mutex lock + `notify_one` on every single fault.
    /// `false -> true` via `compare_exchange` gates the actual mutex+notify
    /// to once per "unacknowledged" wake; the checkpointer thread clears it
    /// back to `false` at the top of every loop iteration (right where it
    /// also clears `wake`'s own `has_work` flag), so a wake that arrives
    /// while the thread is busy running a checkpoint is never lost — the
    /// next `compare_exchange` after the clear succeeds and notifies again.
    /// This only dedupes *redundant* notifies between clears; it never
    /// suppresses one the thread hasn't yet had a chance to observe.
    pub signalled: AtomicBool,
}

impl PagedStats {
    /// Notify the background checkpointer thread's condvar that there may
    /// be work to do, if one is wired up ([`Self::wake`] is set — true for
    /// the whole life of any `PagedStats` a caller outside `Store::new` can
    /// reach). Edge-triggered via [`Self::signalled`] (see its doc): a
    /// no-op both when no thread is wired up and when this call loses the
    /// `compare_exchange` race (someone already signalled since the last
    /// clear) — harmless either way, since `notify_one` with no waiter
    /// parked is itself a no-op, and a still-`true` flag means the
    /// checkpointer thread hasn't consumed the earlier signal yet.
    pub(crate) fn wake_checkpointer(&self) {
        if self.signalled.compare_exchange(false, true, Ordering::AcqRel, Ordering::Relaxed).is_err() {
            return;
        }
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
pub(crate) struct PagedSource<K, V> {
    pub(crate) file: Arc<PageFile>,
    pub(crate) codec: NodeCodec<K, V>,
    pub(crate) name: String,
    pub(crate) stats: Arc<PagedStats>,
    /// The row type's clone fn, captured at `register_table_paged` time
    /// (`TableRegistry::register_paged`, Task 3) and threaded down through
    /// `Table::attach_paged_source`/`from_paged_entry`. `None` for every
    /// index `PagedSource` (indexes never carry block leaves — see
    /// `NodeSource::clone_value`'s doc, I-B) and for a data-tree source
    /// built on a store that was never told `R: Clone` (`register_table`'s
    /// plain path is refused up front by `Error::PagedNeedsClone` on a
    /// paged store, so this only stays `None` off that plain path on a
    /// *non*-paged store, where a table's tree never holds block leaves in
    /// the first place).
    pub(crate) clone: Option<fn(&V) -> V>,
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
        // Decode before crediting: the real-bytes credit below is computed
        // from the decoded node itself (`leaf_bytes`, task 4), so the node
        // has to exist first. Cheap either way — this was always the next
        // line of work regardless of the credit's shape.
        let node = self.codec.decode(kind, &bytes)?;
        // Only a *data leaf* fault-in grows `resident_leaf_bytes`: that
        // counter tracks the data tree's demotable leaves specifically
        // (`BTree::demote_leaves`/`resident_leaf_estimate` never touch
        // inner nodes or index trees), so counting inner/index fault-ins
        // here would inflate the estimate against a demote pass that can
        // never claim those bytes back.
        if kind == PageKind::DataLeaf {
            // Real-bytes credit (task 4, spec §5): `NODE_BYTES + n *
            // size_of::<V>()` via `BTreeNode::leaf_bytes`, not the flat
            // `Child::<K, V>::NODE_BYTES` this used to credit — a block
            // leaf's value bytes are real resident memory the old flat
            // credit ignored entirely. Task 8 makes the debit symmetric
            // (demote still subtracts the flat `NODE_BYTES` until then);
            // the F1 checkpoint-end reconciliation is the safety net for
            // that transient asymmetry in the meantime.
            let credit = node.leaf_bytes() as i64;
            let prev = self.stats.resident_leaf_bytes.fetch_add(credit, Ordering::Relaxed);
            // Wake the background checkpointer (task12) the moment this
            // fault-in pushes resident bytes at/over the memory budget —
            // only when a budget is actually configured (`mem_budget_bytes`
            // is set iff `PagedOptions::memory_budget_bytes` is `Some`; see
            // its doc). Cheaper than notifying on every fault: a read-only
            // store that never crosses the budget never wakes the thread
            // early at all, just falls back to the interval poll.
            if let Some(&budget) = self.stats.mem_budget_bytes.get() {
                let resident = (prev + credit).max(0) as u64;
                if resident >= budget {
                    self.stats.wake_checkpointer();
                }
            }
        }
        Ok(Arc::new(node))
    }

    fn note_dirty(&self, bytes: usize) {
        self.stats.dirty_bytes.fetch_add(bytes as u64, Ordering::Relaxed);
    }

    fn clone_value(&self, v: &V) -> Option<V> {
        self.clone.map(|f| f(v))
    }

    fn name(&self) -> &str {
        &self.name
    }
}

/// Every page id reachable from a page-file root, discovered by walking raw
/// page payloads structurally (child pointers only) — no `K`/`V` type
/// required, unlike [`NodeCodec::decode`]: an entry's key/value bytes are
/// only ever *skipped* by their own length prefixes here, never decoded.
///
/// Built for `Table::paged_changed_pages`'s still-pending persisted-index
/// carry-forward path (Task 13): a pending index's `IK` type is not known
/// statically wherever `Table<R, K>` reasons about its `pending_indexes`
/// (only the persisted `ik_type_id` *code* is), so this is the only way to
/// collect a still-pending index's page ids for the dead-page diff without
/// reinstating a live, typed `BTree` for it just to answer "which pages did
/// this used to reach."
///
/// Two defenses against a corrupt or malformed page graph (fix round 1,
/// controller review — this walk has no `BTree`/`Child` machinery of its
/// own underneath it to lean on for either):
/// - `height` bounds the recursion: a walk that would descend past
///   `height` levels below `root` refuses to continue instead of
///   following a cyclic (or merely very deep, corrupt) child pointer
///   forever. `height` is the caller's own already-trusted
///   `PagedIndexEntry::height` — the same field `BTree::from_root_page`
///   takes on the attach path — never re-derived from the walk itself, so
///   a corrupt page graph has no way to lie its way past a bound it never
///   gets to name.
/// - Every page's `kind` must be `Index{Leaf,Inner}`; anything else
///   (starting with `root` itself) is refused rather than walked. A
///   `Data{Leaf,Inner}` page reachable from what was named as an index
///   root — a corrupt `PagedIndexEntry`, or a stray pointer into the
///   table's own data tree — would otherwise have this function
///   enumerate the *table's own live data pages* as dead, and a caller
///   that then hole-punched them would corrupt the table itself.
pub(crate) fn raw_reachable_page_ids(file: &PageFile, root: PageId, height: usize) -> Result<Vec<PageId>> {
    fn go(file: &PageFile, id: PageId, depth: usize, height: usize, out: &mut Vec<PageId>) -> Result<()> {
        if depth > height {
            return Err(Error::CheckpointCorrupted(format!(
                "raw_reachable_page_ids: page {id} is at depth {depth}, past the tree's own \
                 height ({height}) -- refusing to keep descending (cyclic or corrupt child pointer)"
            )));
        }
        let (kind, payload) = file.read(id)?;
        if !matches!(kind, PageKind::IndexLeaf | PageKind::IndexInner) {
            return Err(Error::CheckpointCorrupted(format!(
                "raw_reachable_page_ids: page {id} has kind {kind:?}, not an index page -- \
                 refusing to walk it as one"
            )));
        }
        out.push(id);
        let is_inner = kind == PageKind::IndexInner;
        let mut at = 0usize;
        let n = read_u16(&payload, &mut at)? as usize;
        for _ in 0..n {
            let key_len = read_u16(&payload, &mut at)? as usize;
            read_bytes(&payload, &mut at, key_len)?;
            let val_len = read_u32(&payload, &mut at)? as usize;
            read_bytes(&payload, &mut at, val_len)?;
        }
        if is_inner {
            let mut children = Vec::with_capacity(n + 1);
            for _ in 0..=n {
                children.push(read_u64(&payload, &mut at)?);
            }
            for child_id in children {
                go(file, child_id, depth + 1, height, out)?;
            }
        }
        Ok(())
    }
    let mut out = Vec::new();
    go(file, root, 0, height, &mut out)?;
    Ok(out)
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
        BTreeNode {
            entries: pairs.into_iter().map(|(k, v)| (k, Value::arc(Arc::new(v)))).collect(),
            children: Default::default(),
            block: None,
        }
    }

    #[test]
    fn records_leaf_roundtrip() {
        let c = NodeCodec::<u64, Row>::records::<Row>();
        let n = leaf(vec![(1, Row { a: 1, s: "x".into() }), (2, Row { a: 2, s: "yy".into() })]);
        let (kind, bytes) = c.encode(&n).unwrap();
        assert_eq!(kind, PageKind::DataLeaf);
        let back = c.decode(kind, &bytes).unwrap();
        assert_eq!(back.entries.len(), 2);
        assert_eq!(*back.value_at(1), Row { a: 2, s: "yy".into() });
    }

    /// Task 4 / spec §3: decoding a `DataLeaf` payload through a records
    /// codec must yield a block leaf — every entry `is_in_block()`, `block`
    /// populated with the same values in the same order. A `DataInner`
    /// payload (encoded from the same node once it has children) must keep
    /// the old all-Arc representation instead: inner nodes are always
    /// resident and never carry blocks (I-B).
    #[test]
    fn data_leaf_decodes_to_block_data_inner_stays_arc() {
        let c = NodeCodec::<u64, Row>::records::<Row>();
        let n = leaf(vec![(1, Row { a: 1, s: "x".into() }), (2, Row { a: 2, s: "yy".into() })]);
        let (kind, bytes) = c.encode(&n).unwrap();
        assert_eq!(kind, PageKind::DataLeaf);
        let back = c.decode(kind, &bytes).unwrap();
        assert!(back.block.is_some(), "DataLeaf decode must build a block");
        for i in 0..back.entries.len() {
            assert!(back.entries[i].1.is_in_block(), "entry {i} must be in-block");
        }
        assert_eq!(back.block.as_ref().unwrap().as_ref(), &[Row { a: 1, s: "x".into() }, Row { a: 2, s: "yy".into() }]);

        let mut inner = leaf(vec![("m".to_string(), Row { a: 9, s: "".into() })]);
        inner.children.push(Child::on_disk(100));
        inner.children.push(Child::on_disk(200));
        let ic = NodeCodec::<String, Row>::records::<Row>();
        let (ikind, ibytes) = ic.encode(&inner).unwrap();
        assert_eq!(ikind, PageKind::DataInner);
        let iback = ic.decode(ikind, &ibytes).unwrap();
        assert!(iback.block.is_none(), "DataInner decode must stay all-Arc");
        assert!(iback.entries[0].1.as_arc().is_some());
    }

    /// Spec §3: "wire format unchanged in both directions" — encode must
    /// serialize identically from either representation. Build the same
    /// logical leaf two ways: all-Arc (the `leaf` test constructor) and
    /// block-backed (decode of the first's own encoding, per the test
    /// above); re-encoding both must produce byte-identical output. This is
    /// the golden-bytes oracle for decode-to-block: the wire format must
    /// not leak which in-memory representation produced it.
    #[test]
    fn encode_bytes_identical_across_representations() {
        let c = NodeCodec::<u64, Row>::records::<Row>();
        let all_arc = leaf(vec![
            (1, Row { a: 1, s: "x".into() }),
            (2, Row { a: 2, s: "yy".into() }),
            (3, Row { a: 3, s: "zzz".into() }),
        ]);
        let (kind, arc_bytes) = c.encode(&all_arc).unwrap();
        assert_eq!(kind, PageKind::DataLeaf);

        // Decode those bytes back through the same records codec — this is
        // now a block leaf (the test above pins that behavior).
        let block_leaf = c.decode(kind, &arc_bytes).unwrap();
        assert!(block_leaf.block.is_some(), "sanity: decode of a DataLeaf payload is block-backed");

        let (kind2, block_bytes) = c.encode(&block_leaf).unwrap();
        assert_eq!(kind2, kind);
        assert_eq!(block_bytes, arc_bytes, "encode output must not depend on the source representation");
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
        assert_eq!(back.value_at(0).a, 9);
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
        assert_eq!(*u.decode(k, &b).unwrap().value_at(0), 5);

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
        let src = PagedSource { file: pf, codec, name: "t".into(), stats: stats.clone(), clone: None };
        let back = crate::child::NodeSource::read_node(&src, id).unwrap();
        assert_eq!(back.entries.len(), 1);
        assert_eq!(stats.page_faults.load(std::sync::atomic::Ordering::Relaxed), 1);
        assert_eq!(stats.data_page_faults.load(std::sync::atomic::Ordering::Relaxed), 1);
        assert_eq!(stats.index_page_faults.load(std::sync::atomic::Ordering::Relaxed), 0);
    }

    /// Task 4 / spec §5: a data-leaf fault-in must credit
    /// `resident_leaf_bytes` with the decoded node's real `leaf_bytes()`
    /// (`NODE_BYTES + n * size_of::<V>()`), not the old flat
    /// `Child::<K, V>::NODE_BYTES` — a leaf with several rows in it must
    /// credit strictly more than a flat, value-blind credit would.
    #[test]
    fn read_node_credits_real_leaf_bytes_not_flat_node_bytes() {
        let d = tempfile::tempdir().unwrap();
        let pf = Arc::new(crate::pagefile::PageFile::open(&crate::pagefile::page_file_path(d.path()), 0, 1 << 16, 4096).unwrap());
        let codec = NodeCodec::<u64, Row>::records::<Row>();
        let n = leaf(vec![
            (1, Row { a: 1, s: "".into() }),
            (2, Row { a: 2, s: "".into() }),
            (3, Row { a: 3, s: "".into() }),
        ]);
        let (k, b) = codec.encode(&n).unwrap();
        assert_eq!(k, PageKind::DataLeaf);
        let id = pf.append(k, &b).unwrap();
        let stats = Arc::new(PagedStats::default());
        let src = PagedSource { file: pf, codec, name: "t".into(), stats: stats.clone(), clone: None };
        let back = crate::child::NodeSource::read_node(&src, id).unwrap();

        let credited = stats.resident_leaf_bytes.load(Ordering::Relaxed);
        assert_eq!(
            credited,
            back.leaf_bytes() as i64,
            "credit must be exactly the decoded node's leaf_bytes()"
        );
        assert!(
            credited > Child::<u64, Row>::NODE_BYTES as i64,
            "a 3-row leaf must credit strictly more than the flat, value-blind NODE_BYTES \
             (credited {credited}, flat {})",
            Child::<u64, Row>::NODE_BYTES
        );
    }

    /// I-2(a): an entry count beyond this build's node capacity (`MAX_KEYS +
    /// 1`) must be rejected as `CheckpointCorrupted` *before* `decode` tries
    /// to push anything into a `FixedVec` — a bare `n = MAX_KEYS + 2` header
    /// (no entries following) is enough to prove the bound-check runs first:
    /// without it, this would have panicked inside `FixedVec::push`'s
    /// release assert on the loop's first iteration instead of returning
    /// `Err`.
    #[test]
    fn decode_rejects_entry_count_over_node_capacity() {
        let c = NodeCodec::<u64, Row>::records::<Row>();
        let n = (crate::btree::MAX_KEYS + 2) as u16;
        let payload = n.to_le_bytes().to_vec();
        match c.decode(PageKind::DataLeaf, &payload) {
            Err(Error::CheckpointCorrupted(msg)) => {
                assert!(msg.contains(&n.to_string()), "error should name the oversized count: {msg}");
            }
            Ok(_) => panic!("expected CheckpointCorrupted, got Ok"),
            Err(e) => panic!("expected CheckpointCorrupted, got {e}"),
        }
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

    /// `raw_reachable_page_ids` walks a two-level tree (root + two leaves)
    /// using only child-pointer structure — never decoding a key or value —
    /// and must report exactly the root and both leaf ids, root first
    /// (Task 13, `Table::paged_changed_pages`'s pending-index diff).
    #[test]
    fn raw_reachable_page_ids_walks_a_two_level_tree() {
        let d = tempfile::tempdir().unwrap();
        let pf = crate::pagefile::PageFile::open(&crate::pagefile::page_file_path(d.path()), 0, 1 << 16, 4096).unwrap();
        let codec = NodeCodec::<u64, u64>::unique_index::<u64, u64>();

        let leaf1 = leaf(vec![(1u64, 100u64), (2u64, 200u64)]);
        let (k1, b1) = codec.encode(&leaf1).unwrap();
        let leaf1_id = pf.append(k1, &b1).unwrap();

        let leaf2 = leaf(vec![(5u64, 500u64)]);
        let (k2, b2) = codec.encode(&leaf2).unwrap();
        let leaf2_id = pf.append(k2, &b2).unwrap();

        let mut root = leaf(vec![(3u64, 300u64)]);
        root.children.push(Child::on_disk(leaf1_id));
        root.children.push(Child::on_disk(leaf2_id));
        let (kr, br) = codec.encode(&root).unwrap();
        let root_id = pf.append(kr, &br).unwrap();

        // height=1: root is an inner level, leaves are one level below it.
        let ids = raw_reachable_page_ids(&pf, root_id, 1).unwrap();
        assert_eq!(ids, vec![root_id, leaf1_id, leaf2_id]);
    }

    /// A single leaf root (no children) reports only itself — the base case
    /// `raw_reachable_page_ids`'s recursion must terminate on. Also covers
    /// `height == 0` (no levels below the root).
    #[test]
    fn raw_reachable_page_ids_single_leaf() {
        let d = tempfile::tempdir().unwrap();
        let pf = crate::pagefile::PageFile::open(&crate::pagefile::page_file_path(d.path()), 0, 1 << 16, 4096).unwrap();
        let codec = NodeCodec::<u64, u64>::unique_index::<u64, u64>();
        let n = leaf(vec![(1u64, 42u64)]);
        let (k, b) = codec.encode(&n).unwrap();
        let id = pf.append(k, &b).unwrap();

        assert_eq!(raw_reachable_page_ids(&pf, id, 0).unwrap(), vec![id]);
    }

    /// A page graph containing a cycle must not recurse forever: the
    /// `height` bound refuses to keep descending once exceeded, rather
    /// than stack-overflowing on a corrupt or adversarial child pointer
    /// (fix round 1, controller review, IMPORTANT #2a).
    ///
    /// Page ids are just byte offsets assigned in append order, and a
    /// page's on-disk length depends only on its payload's *byte length*
    /// — never the numeric value of any child id it names — so both
    /// halves of a two-page cycle can be computed before either page is
    /// actually written: the first append into a fresh file (cursor 0)
    /// always lands at id 0, and the second always lands exactly
    /// `PageFile::page_len(first_payload.len())` bytes after it.
    #[test]
    fn raw_reachable_page_ids_rejects_a_cycle() {
        let d = tempfile::tempdir().unwrap();
        let pf = crate::pagefile::PageFile::open(&crate::pagefile::page_file_path(d.path()), 0, 1 << 16, 4096).unwrap();
        let codec = NodeCodec::<u64, u64>::unique_index::<u64, u64>();

        let mut probe = leaf(vec![]);
        probe.children.push(Child::on_disk(0)); // placeholder: length is unaffected by the value
        let (_, probe_bytes) = codec.encode(&probe).unwrap();
        let id_a = 0u64;
        let id_b = id_a + crate::pagefile::PageFile::page_len(probe_bytes.len());

        let mut a = leaf(vec![]);
        a.children.push(Child::on_disk(id_b));
        let (ka, ba) = codec.encode(&a).unwrap();
        let actual_a = pf.append(ka, &ba).unwrap();
        assert_eq!(actual_a, id_a, "sanity: predicted id_a must match the real append");

        let mut b = leaf(vec![]);
        b.children.push(Child::on_disk(id_a));
        let (kb, bb) = codec.encode(&b).unwrap();
        let actual_b = pf.append(kb, &bb).unwrap();
        assert_eq!(actual_b, id_b, "sanity: predicted id_b must match the real append");

        // A generous height: the cycle must be caught well before any
        // stack limit regardless of how generous the bound is.
        let err = raw_reachable_page_ids(&pf, id_a, 5).unwrap_err();
        assert!(matches!(err, Error::CheckpointCorrupted(_)), "{err:?}");
    }

    /// A root whose page kind is a *data* page (not an index page) is
    /// refused outright: silently walking it as an index tree would
    /// enumerate the table's own live data pages as dead (fix round 1,
    /// controller review, IMPORTANT #2b).
    #[test]
    fn raw_reachable_page_ids_rejects_a_data_kind_root() {
        let d = tempfile::tempdir().unwrap();
        let pf = crate::pagefile::PageFile::open(&crate::pagefile::page_file_path(d.path()), 0, 1 << 16, 4096).unwrap();
        let codec = NodeCodec::<u64, Row>::records::<Row>();
        let n = leaf(vec![(1u64, Row { a: 1, s: "x".into() })]);
        let (k, b) = codec.encode(&n).unwrap();
        assert_eq!(k, PageKind::DataLeaf);
        let id = pf.append(k, &b).unwrap();

        let err = raw_reachable_page_ids(&pf, id, 0).unwrap_err();
        assert!(matches!(err, Error::CheckpointCorrupted(_)), "{err:?}");
    }
}
