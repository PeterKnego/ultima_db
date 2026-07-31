# Task 56: arbitrary primary keys (`Table<R, K = u64>`)

## Motivation

Adoption program §2a. Every table was keyed by an auto-incrementing `u64`, so
a natural key — an email address, a UUID, a `(tenant, id)` pair — had to live
in a unique secondary index with the real row addressed by a surrogate id.
That costs a second B-tree, a second lookup on every read, and index
maintenance on every write, to express something the primary tree could have
stored directly. It also made UltimaDB awkward as a drop-in for stores whose
keys are byte strings.

`Table<R>` becomes `Table<R, K = u64>`. The defaulted type parameter keeps
every existing *type* reference valid, and `BTree<K, V>` was already generic,
so the storage engine itself needed nothing. The work was in the layers that
had hard-coded `u64`: secondary-index storage, the type-erased
`MergeableTable`/registry boundary, the WAL and checkpoint formats, the OCC
write set, and the snapshot wire format.

Auto-increment stays `u64`-only, behind an `AutoKey` bound: the store cannot
invent the next `String`.

## API

```rust
pub trait PrimaryKey: Ord + Clone + Send + Sync + 'static {
    const ENCODED_LEN: Option<usize>;
    fn encode(&self) -> Vec<u8>;
    fn decode(bytes: &[u8]) -> Result<Self> where Self: Sized;
    fn hash64(&self) -> u64;                       // OCC digest; overridable
    fn advance_auto_counter(&self, _c: &mut Option<Self>) where Self: Sized {}
}

pub trait AutoKey: PrimaryKey {   // implemented only for u64
    fn first() -> Self;
    fn next(&self) -> Option<Self> where Self: Sized;
}
```

Implemented for `u8`–`u128`, `i8`–`i128`, `String`, `Vec<u8>`, and 2- and
3-tuples of `PrimaryKey`. Third parties can implement it for their own key
types; the one hard obligation is order preservation (below).

Reaching a non-`u64`-keyed table:

```rust
Store::register_table_keyed::<R, K>(name)          // persistence registration
WriteTx::open_table_keyed::<R, K>(opener)   -> TableWriter<'_, R, K>
ReadTx::open_table_keyed::<R, K>(opener)    -> TableReader<'_, R, K>
Store::bulk_load_keyed::<R, K>(name, input, options)
```

`TableWriter<'_, R, K>` has `put(key, record)` instead of `insert(record)` —
there is no counter for a key the store cannot generate. `insert`,
`insert_batch` and the bulk-append fast path remain available on `K = u64`
via the `AutoKey` bound.

Secondary indexes work over any row key: `ManagedIndex` and `CustomIndex` are
parameterized over the row key `K` independently of the index key `IK`, so a
`String`-keyed table can carry a unique index on a `u32` field, and the
built-in BM25 `FullTextIndex` works on a table with any primary key. See
`examples/string_keyed_table.rs`.

### Passing keys by reference

Reads (`get`, `contains`, `delete`, `get_many`, …) take `impl Borrow<K>`. On a
`String`-keyed table that means **`&str` is not accepted**: the standard
library provides `String: Borrow<str>`, not `str: Borrow<String>`. So
`t.get("alice@example.com")` does not compile; pass a `&String` or an owned
`String`. This is the inverse of `HashMap<String, _>::get` and is the most
common surprise when moving a table off `u64` keys — it is documented on both
`open_table_keyed` methods.

## Design decisions

Four of these were fixed before implementation and three of those four were
recorded incorrectly in the plan or spec; the corrections are the point of
this section.

### 1. Key encoding must be order-preserving — and length prefixes do not do it

`encode(a) < encode(b)` bytewise iff `a < b`. This is load-bearing: WAL
replay, `BTree::from_sorted`, the checkpoint row order, and the snapshot
stream's strict-ascent check all treat encoded byte order as key order. It is
also why `bincode` cannot be used for keys — its varint integer encoding is
not order-preserving.

Integers are big-endian, signed ones with the sign bit flipped
(`(v as uN) ^ (1 << (BITS-1))`) so negatives sort first. `String` and
`Vec<u8>` encode to their raw bytes (UTF-8 byte order equals code-point
order).

**The plan specified 4-byte big-endian length prefixes for every non-final
tuple element. That is wrong, and Task 1's review caught it before it
shipped.** A length prefix puts the *length* ahead of the *content* in the
comparison, so a shorter first element always sorts first regardless of its
value: `("aa", 0)` encodes as `00000002 61 61 …` and `("b", 0)` as
`00000001 62 …`, so `("b", 0) < ("aa", 0)` bytewise while `("aa", 0) <
("b", 0)` under `Ord`. Five such counterexample pairs were reproduced.

The replacement is escape-and-terminate framing, driven by a new associated
const:

- `ENCODED_LEN: Option<usize>` — `Some(n)` for a fixed-width type, `None` for
  a variable-length one. A fixed-width non-final element is self-delimiting
  and gets no framing at all (a `(u32, String)` key costs zero framing bytes).
- A variable-length non-final element has every `0x00` byte escaped to
  `[0x00, 0xFF]` and a terminator `[0x00, 0x01]` appended. The terminator's
  first byte sorts below every literal byte (`>= 0x01`), and against an
  escaped zero the second byte decides (`0x01 < 0xFF`) — so a prefix sorts
  before any extension of itself, which is exactly the property `Ord` on
  tuples needs.

Order preservation was verified by case analysis plus a 4000-value fuzz, and
the five original counterexamples now pass.

### 2. `open_table` could not be widened — Rust has no default type parameters on functions

Default type parameters exist on *types* (`Table<R, K = u64>`) but not on
*functions*. Adding a second parameter to `open_table` would make every
existing `open_table::<User>("users")` a hard `E0107` — partial turbofish is
not allowed, so callers would have to write `open_table::<User, u64>(..)`.

Therefore `open_table`, `register_table`, `open_tables2`/`open_tables3` keep
their exact signatures and stay `u64`-only, and non-`u64` keys go through
additive `_keyed` variants. No existing call site changes. Internally each
`u64` entry point is a one-line delegation to a shared
`*_inner::<R, K>` generic, so there is one implementation, not two.

The keyed multi-table openers (`open_tables2_keyed`, …) were deliberately not
added: the combinatorics are poor and no caller needs them yet.

### 3. `dyn MergeableTable` cannot be parameterized over `K`

A `Snapshot` holds `HashMap<String, Arc<dyn MergeableTable>>`, whose tables
have *heterogeneous* key types. `K` therefore must not appear anywhere in the
trait's signature — a `MergeableTable<K>` would force one key type per
snapshot, which defeats the feature.

The two `u64`-typed methods were reworked rather than parameterized:

- `merge_keys_from(&mut self, source: &dyn MergeableTable, keys: &dyn Any)` —
  the modified-key set crosses the boundary as a `&BTreeSet<K>` erased to
  `&dyn Any`, and the impl (which knows `K`) downcasts it. A failed downcast
  is an internal bug and aborts the commit through `?`; there is no path where
  it silently loses a write.
- `collect_serialized_rows` returns `(encoded key bytes, bincode record
  bytes)` pairs in primary-key order, instead of `(u64, bytes)`.

The same erasure shape appears in `DirtyEntry::modified_keys`
(`Box<dyn Any + Send + Sync>` holding a `BTreeSet<K>`) and in the registry's
type-erased closures, which now take and return encoded key bytes.

Two `TypeId`/name accessors were added for the wire formats:
`key_type_id()` and `key_type_name()`, both read off the **live** table. The
registry knows a key type too, and the two can disagree — a table can be
created by `open_table_keyed` and never registered, and `register_table*`
records whatever the caller asked for. Both ends of the snapshot wire format
and `register_table_keyed`'s guard ask the table, not the registry; an earlier
draft trusted the registry and was shown to destroy an existing table's rows
by reinterpreting its keys.

### 4. The OCC write set stores 64-bit digests, not keys — and the merge needs a second structure

Conflict detection compares modified-key sets across writers that need not
agree on `K` (different tables, different key types), so the write set holds
`PrimaryKey::hash64` digests. A collision produces a *spurious* conflict — a
retry — and never a missed one, so the detector stays sound and
`src/intents.rs`, SSI read-set tracking and the commit path were untouched.

But digests are lossy and the commit *merge* has to replay the writer's exact
writes, so `DirtyEntry` carries a separate type-erased `BTreeSet<K>` alongside
the digest set. Two structures, two jobs: digests for "did we collide", exact
keys for "replay these rows".

`hash64` has a default implementation that hashes `encode()`, which allocates.
Every key type in this crate overrides it with an allocation-free hash of the
value; without that override, `TableWriter::get` — documented hot, HNSW issues
thousands per query — allocated once per call in the *default* config, because
the digest was computed eagerly as an argument and discarded when no read set
was present. Measured 100 allocations per 100 `get`s before the fix, 0 after.

### 5. The magic byte: a bare version byte would have silently misread v1 data

**The plan and spec both specify a one-byte version header on the checkpoint
table payload. Both are wrong**, and the doc you are reading carries the real
layout.

`bincode::config::standard()` is a **varint** encoding: values `0..=250`
encode as a single literal byte. A v1 table payload opened with `next_id`, so
a table that had taken exactly one insert — `next_id == 2` — starts with the
byte `0x02`. A bare version byte of `2` would have matched it, and the reader
would have parsed a v1 payload as v2 and produced garbage rows instead of an
error.

The header is therefore **two** bytes, `[magic 0xFF][version 2]`. `0xFF` is
not a legal bincode varint tag at all (`0..=250` are literals, `251..=253` are
width markers, `254` is `u128`-only, `255` is unused), so no v1 payload can
begin with it and no v2 payload can be mistaken for v1. The WAL entry header
uses the identical trick for the identical reason.

### 6. `BTree::range_prefix` instead of a sentinel row key

The non-unique index stores `BTree<(IK, K), ()>`. Looking up all rows for one
index key is a prefix scan, and a prefix scan cannot be spelled as a
`RangeBounds<(IK, K)>` without inventing minimum and maximum values of `K` —
which do not exist for `String`.

The first implementation invented them with a `RowBound` sentinel wrapper.
That worked but cost **+50% per non-unique index entry on the default `u64`
path** (16 → 24 bytes), a tax on every existing user for a feature they were
not using. It was replaced by `BTree::range_prefix`, an O(log n + k) primitive
that descends on the first component only. A follow-up split its locator into
directional predicates after the merged form was measured to double key
comparisons per yielded item on two-sided ranges; comparison counts are now
identical to the pre-feature tree in all eight measured cells.

## On-disk and wire formats

All three formats broke, and all three refuse pre-0.3.0 data rather than
guessing. There are no compatibility branches.

**Checkpoint table payload — v2:**

```
[magic u8 = 0xFF][version u8 = 2][has_next_id u8]
[next_id_len u32-be, next_id_bytes]?
[num_entries u64-be]
[key_len u32-be, key_bytes, rec_len u32-be, rec_bytes]*
```

All lengths are explicit and big-endian; the v1 assumption of a fixed 8-byte
row id is gone. `has_next_id` is `0` exactly for an explicitly-keyed table
(`next_id_opt() == None`) and `1` for an auto-increment one, which is what
preserves the "explicitly keyed ⟺ no counter" invariant across a checkpoint
round trip. Rejected at `recover()`, with an error naming the table and the
migration path.

**WAL entry payload — v2:** each payload opens with `[magic 0xFF][format 2]`;
`WalOp` carries a length-prefixed encoded key instead of a `u64` id. The WAL
*file* has no header (the preallocating sink reconstructs its write head by
scanning, and prune-by-rewrite depends on the bare `[len][payload][crc]`
concatenation), so the marker lives at the front of each payload.

A v1 WAL is refused **at store open**, in all three write modes, not merely at
`recover()`. This matters: `PreallocFileSink` already refused as a side effect
of its scan, but the two appending sinks did not — so a store pointed at a
pre-0.3.0 directory would construct cleanly, return `Ok` from a
`Durability::Consistent` `commit()` (telling the caller the data was durable),
append v2 records behind the v1 prefix, and then fail `recover()` permanently
with "delete the WAL" as the only remedy — destroying exactly the commits it
had acknowledged. `reject_unreadable_wal` reads the first record's 6-byte
prefix at open; a missing, empty, or sub-record-sized file is accepted.

**Snapshot stream — `FILE_FORMAT_V` 1 → 2:** rows are
`key_len(u32) | key | val_len(u32) | val`, and `TableHeader` gained a public
`key_type` field. This is a **live-replication** break as well as an on-disk
one: a 0.2.0 SMR follower cannot install a 0.3.0 leader's snapshot. It rejects
cleanly in both directions rather than mis-parsing.

`key_type` is *enforced*, unlike the pre-existing best-effort
`record_type_id`, because row keys are opaque bytes that several key types
decode without complaint — the eight bytes of `1u64` are also a valid
NUL-filled `String`. Without the check, a stream aimed at a destination with a
different `K` decodes cleanly, passes strict-ascent and CRC, and installs a
table full of garbage keys with no error anywhere. It is compared as
`std::any::type_name`, which has two accepted limits: not **stable** across
compiler versions (a cross-toolchain stream can be refused when it would have
decoded — safe, loud), and not **injective** (two binaries linking different
*versions* of a key type's crate print the same string — not caught here, and
not catchable on the wire without a discriminant the key type declares
itself). The install path additionally compares `TypeId` between the
destination's registry and its live table, closing the local half. A stable
`PrimaryKey::KEY_TYPE_ID` associated const is the durable fix and is
deliberately out of scope.

**Key length cap.** `MAX_ENCODED_KEY_LEN` (64 KiB) lives in `primary_key.rs`
and is *aliased* by the WAL's `MAX_KEY_LEN`, so the two trust boundaries agree
by construction rather than by coincidence: a key one format accepts and the
other truncates is a row that survives one durability path and is corrupted by
the other. Both read a length off untrusted bytes before allocating, and both
validate before the allocation.

## Migration from 0.2.x

There is no in-place upgrade, and **checkpointing on 0.2.x does not help** —
0.3.0 rejects v1 checkpoints as well as v1 WALs. The path is:

1. With the **0.2.x** binary, `Store::recover()` the existing persistence
   directory.
2. Read the rows out through a `ReadTx`.
3. Load them into a **0.3.0** store with `Store::bulk_load` /
   `Store::bulk_load_batch` (existing `u64`-keyed tables need no key change),
   then `checkpoint()`.

Both rejection messages state this explicitly. An earlier draft of the WAL
message said "export the data", which was wrong — there is no `export` API.

## Testing

Per-layer unit tests in `src/primary_key.rs` (order preservation by case
analysis and fuzz, escape/terminate round trips, the counterexample pairs),
`src/registry.rs` (v1-vs-v2 header collision fixture, exact-consumption,
corrupted-not-truncated), `src/wal.rs` (v1 rejection at open across six
`(Durability, WalWrite)` pairs with the file bytes asserted unchanged,
key-length cap, bincode length-prefix width pinning), `src/bulk_load.rs`
(auto-counter presence read directly via `next_id_opt()` — the public surface
cannot distinguish the two states, so an integration test for that invariant
would be vacuous by construction).

Integration: `tests/persistence_integration.rs` (a `String`-keyed table
through the public API, fsynced, recovered from a fresh store; hand-assembled
v1 WAL fixture), `tests/snapshot_stream.rs` (`KeyTypeMismatch` in both
directions against the *live* table, `KeyTooLong` at both ends),
`tests/bulk_load.rs` (`bulk_load_keyed`, and an OCC test that fires a
concurrent commit from inside a secondary-index extractor — caller code
invoked between the base-version capture and the install with no store lock
held, which makes "a commit landed during the bulk build" deterministic with
no sleeps or threads), `tests/hot_path_allocations.rs` (allocation counts on
`TableWriter::get` for `String` and tuple keys).

Gates: `cargo test`, `cargo test --features persistence`, `cargo test
-p ultima-vector`, `cargo check --lib --no-default-features`, and
`cargo clippy --all-targets --features persistence,fulltext -- -D warnings`.

## Deferred

- `open_tables2_keyed` / `open_tables3_keyed` — `u64`-only until a caller
  needs otherwise.
- A dense-integer-key fast path. Now unblocked (a generic `K` finally gives
  something to specialize on) but it is a performance change; see
  `docs/BACKLOG.md`.
- A migration converter binary — not justified at current adoption.
- `PrimaryKey::KEY_TYPE_ID`, a stable key-type discriminant that would replace
  the `type_name` comparison on the snapshot wire format.

## Design history

- Spec: `docs/superpowers/specs/2026-07-30-adoption-program-design.md` (§2a).
- Plan: `docs/superpowers/plans/2026-07-30-arbitrary-primary-keys.md` — note
  that its tuple-encoding rule (§"Encoding rules") and its one-byte checkpoint
  header are both superseded by this document.
