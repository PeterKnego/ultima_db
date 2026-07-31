# Arbitrary Primary Keys (`Table<R, K = u64>`) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Let a table be keyed by any ordered type — `String`, `u128`, `Vec<u8>`, a tuple — instead of only an auto-incrementing `u64`, without breaking existing code.

**Architecture:** `Table<R>` becomes `Table<R, K = u64>`. The defaulted type parameter keeps every existing *type* reference valid, and `BTree<K, V>` is already generic so the storage engine needs nothing. The work is in the layers that hard-coded `u64`: secondary index storage, the type-erased `MergeableTable`/registry boundary, the WAL and checkpoint formats, and the OCC write-set. Auto-increment remains `u64`-only behind an `AutoKey` bound.

**Tech Stack:** Rust (edition 2024, MSRV 1.88), `bincode` 2 + `serde` for persistence, `criterion` for benches.

## Global Constraints

- **MSRV 1.88, edition 2024.** No features newer than 1.88.
- **Zero-warning lint gate:** `cargo clippy --all-targets --features persistence -- -D warnings` must pass.
- **Do NOT run `cargo fmt`** — repo-wide rustfmt drift, no CI fmt gate. Match surrounding style by hand.
- **Never use `perl -pi` for edits containing non-ASCII.** Use the Edit tool or `python3`.
- **Verification gate for every task:** `cargo test`, `cargo test --features persistence`, `cargo test -p ultima-vector`, and the clippy line above. Note that `cargo test` and `cargo test --features persistence` are the **same run** (the `ultima-bench-workloads` dev-dep unifies `persistence` on); verify the featureless build separately with `cargo check --lib --no-default-features`.
- **`store_integration::concurrent_same_table_overlapping_keys_with_retry` is known-flaky** (~1/12 in full-binary runs, pre-existing). Re-run it alone before treating a failure as a regression.
- **The tree must compile and tests must pass at the end of every task.** The task order is chosen so each layer instantiates the layer below at `K = u64` until it is widened.
- **This is a breaking release (0.3.0).** On-disk format changes are expected and intended; do not add backward-compatibility branches.
- **Target version is 0.3.0**, not 0.2.0 — 0.2.0 is already released.

## Design decisions fixed before implementation

Read these before Task 1; several tasks depend on them and they are not re-derivable from the code.

1. **Functions cannot have default type parameters in Rust.** `open_table::<User>("users")` is a hard error (`E0107`) if `open_table` gains a second parameter — partial turbofish is not allowed. Therefore `open_table<R>`, `register_table<R>`, and `open_tables2/3` **keep their exact current signatures and stay `u64`-only**. Non-`u64` keys go through new `_keyed` variants. This is additive; no existing call site changes.
2. **`dyn MergeableTable` cannot be parameterized over `K`.** A `Snapshot` holds `HashMap<String, Arc<dyn MergeableTable>>` with heterogeneous key types, so `K` must not appear in the trait's signature. Its two `u64`-typed methods are reworked instead (Task 3).
3. **The OCC write-set stores 64-bit key hashes, not keys.** A collision yields a *spurious* conflict, never a missed one, so the detector stays sound and `src/intents.rs`, SSI read-set tracking, and the commit path are untouched. Because hashes are lossy, the *merge* needs exact keys, so `DirtyEntry` carries a separate type-erased `BTreeSet<K>` (Task 6).
4. **Key encoding must be order-preserving.** `encode(a) < encode(b)` bytewise iff `a < b`. WAL replay and `BTree::from_sorted` both depend on encoded order matching in-memory order. Do not use `bincode` for keys — its integer encoding is not order-preserving. Task 1 specifies the encoding.

---

## File Structure

| File | Responsibility | Task |
|---|---|---|
| `src/primary_key.rs` (new) | The `PrimaryKey` trait, `AutoKey` marker, order-preserving encode/decode, blanket impls | 1 |
| `src/index.rs` | Secondary index storage generic over the *row* key type | 2 |
| `src/btree.rs` | `range_prefix` primitive so a prefix scan needs no min/max key values | 2b |
| `src/fulltext.rs` | `CustomIndex` + `FullTextIndex` widened over the row key | 3b |
| `src/table.rs` | `Table<R, K>`; `MergeableTable` reworked for type erasure | 3 |
| `src/registry.rs` | Type-erased closures carry encoded key bytes; table serialization format v2 | 4 |
| `src/wal.rs` | `WalOp` carries key bytes; format version bump; recovery rejects v1 | 5 |
| `src/store.rs` | `open_table_keyed`, `register_table_keyed`; hashed write-set + exact key set | 6 |
| `src/bulk_load.rs`, `src/snapshot_stream/` | Sorted-key paths over encoded keys | 7 |
| `docs/tasks/task56_arbitrary_primary_keys.md`, `README.md`, `CHANGELOG.md` | Feature doc, migration note | 8 |

---

### Task 1: The `PrimaryKey` trait and order-preserving encoding

**Files:**
- Create: `src/primary_key.rs`
- Modify: `src/lib.rs` (add `pub mod primary_key;` and re-export)
- Test: `src/primary_key.rs` (unit tests in-file, matching repo convention)

**Interfaces:**
- Produces:
  - `pub trait PrimaryKey: Ord + Clone + Send + Sync + 'static { fn encode(&self) -> Vec<u8>; fn decode(bytes: &[u8]) -> Result<Self> where Self: Sized; fn hash64(&self) -> u64; }`
  - `pub trait AutoKey: PrimaryKey { fn first() -> Self; fn next(&self) -> Option<Self> where Self: Sized; }` — implemented **only** for `u64`.
  - Blanket impls of `PrimaryKey` for `u8`, `u16`, `u32`, `u64`, `u128`, `i8`, `i16`, `i32`, `i64`, `i128`, `String`, `Vec<u8>`, and 2- and 3-tuples of `PrimaryKey`.
- Consumes: nothing.

**Encoding rules (order-preserving — this is the load-bearing property):**
- Unsigned integers: big-endian fixed-width bytes.
- Signed integers: big-endian with the sign bit flipped (`(v as uN) ^ (1 << (BITS-1))`), so negatives sort before positives.
- `String` / `Vec<u8>`: raw bytes. (UTF-8 byte order equals code-point order, so this is correct for `String`.)
- Tuples: concatenation of each element's encoding, where every element **except the last** is length-prefixed with a 4-byte big-endian length. Fixed-width integer elements need no prefix but get one anyway for decode uniformity — keep it simple and uniform.

> **Superseded during implementation — this tuple rule is NOT order-preserving.** A length prefix puts the length ahead of the content in the comparison, so `("b", 0)` sorts before `("aa", 0)` bytewise while the opposite holds under `Ord`. Replaced by `ENCODED_LEN: Option<usize>` plus escape-and-terminate framing (`0x00 → 0x00,0xFF`; terminator `0x00,0x01`) for variable-length non-final elements, with fixed-width elements left unframed. See `docs/tasks/task56_arbitrary_primary_keys.md`.

- [ ] **Step 1: Write the failing tests**

Create `src/primary_key.rs` with only the tests plus a `use` line that will not yet resolve:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    /// The property everything else depends on: encoded byte order must
    /// match value order, or WAL replay and `BTree::from_sorted` will build
    /// a mis-ordered tree.
    fn assert_order_preserving<K: PrimaryKey>(mut values: Vec<K>) {
        values.sort();
        for pair in values.windows(2) {
            assert!(
                pair[0].encode() < pair[1].encode(),
                "encoding not order-preserving across a sorted pair"
            );
        }
    }

    #[test]
    fn unsigned_ints_encode_in_order() {
        assert_order_preserving(vec![0u64, 1, 255, 256, u64::MAX / 2, u64::MAX]);
        assert_order_preserving(vec![0u32, 7, 65_535, u32::MAX]);
    }

    #[test]
    fn signed_ints_encode_in_order_across_zero() {
        assert_order_preserving(vec![i64::MIN, -1_000, -1, 0, 1, 1_000, i64::MAX]);
    }

    #[test]
    fn strings_encode_in_order() {
        assert_order_preserving(vec![
            String::new(),
            "a".to_string(),
            "ab".to_string(),
            "b".to_string(),
            "\u{1F600}".to_string(),
        ]);
    }

    #[test]
    fn tuples_encode_in_order() {
        assert_order_preserving(vec![
            (1u32, "a".to_string()),
            (1u32, "b".to_string()),
            (2u32, "a".to_string()),
        ]);
    }

    #[test]
    fn roundtrip_decode() {
        assert_eq!(u64::decode(&42u64.encode()).unwrap(), 42u64);
        assert_eq!(i32::decode(&(-7i32).encode()).unwrap(), -7i32);
        let s = "hello".to_string();
        assert_eq!(String::decode(&s.encode()).unwrap(), s);
        let t = (9u32, "x".to_string());
        assert_eq!(<(u32, String)>::decode(&t.encode()).unwrap(), t);
    }

    #[test]
    fn decode_rejects_truncated_input() {
        assert!(u64::decode(&[0, 1, 2]).is_err());
    }

    #[test]
    fn hash64_is_stable_and_differs_across_values() {
        assert_eq!(1u64.hash64(), 1u64.hash64());
        assert_ne!(1u64.hash64(), 2u64.hash64());
    }

    #[test]
    fn auto_key_sequences_from_one() {
        assert_eq!(<u64 as AutoKey>::first(), 1u64);
        assert_eq!(5u64.next(), Some(6u64));
        assert_eq!(u64::MAX.next(), None);
    }
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test --lib primary_key`
Expected: FAIL — the module does not compile, `PrimaryKey` is not defined.

- [ ] **Step 3: Implement the trait and impls**

Add above the test module in `src/primary_key.rs`:

```rust
//! Primary-key contract for [`Table`](crate::table::Table).
//!
//! A table's primary key must be orderable, cheap to clone, and — when the
//! `persistence` feature is on — encodable to bytes whose lexicographic
//! order matches the value order. That last property is load-bearing:
//! WAL replay and [`BTree::from_sorted`](crate::btree::BTree::from_sorted)
//! both assume encoded order equals in-memory order.

use crate::error::{Error, Result};
use std::hash::{Hash, Hasher};

/// A type usable as a table's primary key.
pub trait PrimaryKey: Ord + Clone + Send + Sync + 'static {
    /// Encode to bytes whose lexicographic order matches `Ord`.
    fn encode(&self) -> Vec<u8>;

    /// Decode from bytes produced by [`encode`](PrimaryKey::encode).
    fn decode(bytes: &[u8]) -> Result<Self>
    where
        Self: Sized;

    /// A 64-bit digest used for optimistic-concurrency conflict detection.
    ///
    /// Collisions are permitted: they cause a *spurious* write conflict
    /// (a retry), never a missed one, so the detector stays sound.
    fn hash64(&self) -> u64 {
        let mut h = std::collections::hash_map::DefaultHasher::new();
        self.encode().hash(&mut h);
        h.finish()
    }
}

/// A primary key the table can assign automatically. Implemented only for
/// `u64`; this is what gates [`Table::insert`](crate::table::Table::insert)
/// and the bulk-append fast path to auto-increment tables.
pub trait AutoKey: PrimaryKey {
    /// The first id handed out by a fresh table.
    fn first() -> Self;
    /// The next id after `self`, or `None` on overflow.
    fn next(&self) -> Option<Self>
    where
        Self: Sized;
}

fn truncated(expected: usize, got: usize) -> Error {
    Error::InvalidBulkLoadInput(format!(
        "primary key decode: expected {expected} bytes, got {got}"
    ))
}

macro_rules! impl_unsigned_key {
    ($($t:ty),*) => {$(
        impl PrimaryKey for $t {
            fn encode(&self) -> Vec<u8> {
                self.to_be_bytes().to_vec()
            }
            fn decode(bytes: &[u8]) -> Result<Self> {
                const N: usize = std::mem::size_of::<$t>();
                let arr: [u8; N] = bytes
                    .try_into()
                    .map_err(|_| truncated(N, bytes.len()))?;
                Ok(<$t>::from_be_bytes(arr))
            }
        }
    )*};
}

macro_rules! impl_signed_key {
    ($($t:ty => $u:ty),*) => {$(
        impl PrimaryKey for $t {
            fn encode(&self) -> Vec<u8> {
                // Flip the sign bit so negatives sort before positives.
                let biased = (*self as $u) ^ (1 << (<$u>::BITS - 1));
                biased.to_be_bytes().to_vec()
            }
            fn decode(bytes: &[u8]) -> Result<Self> {
                const N: usize = std::mem::size_of::<$t>();
                let arr: [u8; N] = bytes
                    .try_into()
                    .map_err(|_| truncated(N, bytes.len()))?;
                let biased = <$u>::from_be_bytes(arr);
                Ok((biased ^ (1 << (<$u>::BITS - 1))) as $t)
            }
        }
    )*};
}

impl_unsigned_key!(u8, u16, u32, u64, u128);
impl_signed_key!(i8 => u8, i16 => u16, i32 => u32, i64 => u64, i128 => u128);

impl PrimaryKey for String {
    fn encode(&self) -> Vec<u8> {
        self.as_bytes().to_vec()
    }
    fn decode(bytes: &[u8]) -> Result<Self> {
        String::from_utf8(bytes.to_vec())
            .map_err(|e| Error::InvalidBulkLoadInput(format!("primary key not UTF-8: {e}")))
    }
}

impl PrimaryKey for Vec<u8> {
    fn encode(&self) -> Vec<u8> {
        self.clone()
    }
    fn decode(bytes: &[u8]) -> Result<Self> {
        Ok(bytes.to_vec())
    }
}

/// Length-prefixed concatenation: every element but the last carries a
/// 4-byte big-endian length, so the whole encoding stays order-preserving
/// (a shorter prefix compares less at the length field) and decodable.
fn push_prefixed(out: &mut Vec<u8>, part: &[u8]) {
    out.extend_from_slice(&(part.len() as u32).to_be_bytes());
    out.extend_from_slice(part);
}

fn take_prefixed(bytes: &[u8], at: &mut usize) -> Result<Vec<u8>> {
    if bytes.len() < *at + 4 {
        return Err(truncated(*at + 4, bytes.len()));
    }
    let len = u32::from_be_bytes(bytes[*at..*at + 4].try_into().unwrap()) as usize;
    *at += 4;
    if bytes.len() < *at + len {
        return Err(truncated(*at + len, bytes.len()));
    }
    let part = bytes[*at..*at + len].to_vec();
    *at += len;
    Ok(part)
}

impl<A: PrimaryKey, B: PrimaryKey> PrimaryKey for (A, B) {
    fn encode(&self) -> Vec<u8> {
        let mut out = Vec::new();
        push_prefixed(&mut out, &self.0.encode());
        out.extend_from_slice(&self.1.encode());
        out
    }
    fn decode(bytes: &[u8]) -> Result<Self> {
        let mut at = 0;
        let a = A::decode(&take_prefixed(bytes, &mut at)?)?;
        let b = B::decode(&bytes[at..])?;
        Ok((a, b))
    }
}

impl<A: PrimaryKey, B: PrimaryKey, C: PrimaryKey> PrimaryKey for (A, B, C) {
    fn encode(&self) -> Vec<u8> {
        let mut out = Vec::new();
        push_prefixed(&mut out, &self.0.encode());
        push_prefixed(&mut out, &self.1.encode());
        out.extend_from_slice(&self.2.encode());
        out
    }
    fn decode(bytes: &[u8]) -> Result<Self> {
        let mut at = 0;
        let a = A::decode(&take_prefixed(bytes, &mut at)?)?;
        let b = B::decode(&take_prefixed(bytes, &mut at)?)?;
        let c = C::decode(&bytes[at..])?;
        Ok((a, b, c))
    }
}

impl AutoKey for u64 {
    fn first() -> Self {
        1
    }
    fn next(&self) -> Option<Self> {
        self.checked_add(1)
    }
}
```

- [ ] **Step 4: Wire the module into the crate root**

In `src/lib.rs`, add the module declaration alongside the other `pub mod` lines and a re-export alongside the other `pub use` lines:

```rust
pub mod primary_key;
```

```rust
pub use primary_key::{AutoKey, PrimaryKey};
```

- [ ] **Step 5: Run the tests to verify they pass**

Run: `cargo test --lib primary_key`
Expected: PASS, 8 tests.

- [ ] **Step 6: Verify the full gate**

```bash
cargo test
cargo test --features persistence
cargo check --lib --no-default-features
cargo clippy --all-targets --features persistence -- -D warnings
```

Expected: all pass, zero warnings.

- [ ] **Step 7: Commit**

```bash
git add src/primary_key.rs src/lib.rs
git commit -m "feat(key): PrimaryKey trait with order-preserving encoding

Adds the key contract that Table<R, K> will be generic over. Encoding is
order-preserving (big-endian ints, sign-bit-flipped signed ints, raw string
bytes, length-prefixed tuples) because WAL replay and BTree::from_sorted
both assume encoded order equals in-memory order. AutoKey is u64-only and
gates auto-increment."
```

---

### Task 2: Make secondary index storage generic over the row key

**Files:**
- Modify: `src/index.rs` (`IndexMaintainer`, `UniqueStorage`, `NonUniqueStorage`, and the three `rebuild_from_sorted_data` impls at `:54`, `:173`, `:251`)
- Test: `src/index.rs` (in-file unit tests)

**Interfaces:**
- Consumes: `PrimaryKey` from Task 1.
- Produces:
  - `pub(crate) trait IndexMaintainer<R, K: PrimaryKey>` — every method that mentioned a row id now mentions `K`. `rebuild_from_sorted_data(&mut self, data: &BTree<K, R>) -> Result<()>`.
  - `pub(crate) trait IndexStorage<IK, K>: Send + Sync` — `fn insert(&mut self, key: IK, row_key: K, name: &str) -> Result<()>` and `fn delete(&mut self, key: IK, row_key: K)`. (Currently `IndexStorage<K>` with `id: u64` parameters at `src/index.rs:79-82`.)
  - `pub(crate) struct UniqueStorage<IK: Ord + Clone, K: PrimaryKey> { tree: BTree<IK, K> }` — `get(&self, key: &IK) -> Option<K>`, `range_ids` yielding `(&IK, K)`.
  - `pub(crate) struct NonUniqueStorage<IK: Ord + Clone, K: PrimaryKey> { tree: BTree<(IK, K), ()> }` — `get_ids(&self, key: &IK) -> impl Iterator<Item = K> + '_`, `range_ids` likewise.
  - `ManagedIndex<R, IK, S>` — the index-key parameter is renamed `IK` throughout to avoid colliding with the new row-key `K`; the row key reaches it through the `IndexStorage<IK, K>` bound on `S`.

**Note on naming:** the existing code calls the *index* key `K`. Rename it to `IK` in this file so `K` consistently means the *row/primary* key across the crate. This rename is mechanical but touches most of the file; do it as one pass.

- [ ] **Step 1: Write the failing test**

Add to the existing `#[cfg(test)] mod tests` in `src/index.rs`:

```rust
#[test]
fn unique_index_over_string_primary_key() {
    // Index key is u32 (an age); row key is String (an email).
    let mut storage: UniqueStorage<u32, String> = UniqueStorage::new();
    storage.insert(30u32, "a@x.com".to_string(), "by_age").unwrap();
    storage.insert(40u32, "b@x.com".to_string(), "by_age").unwrap();

    assert_eq!(storage.get(&30), Some("a@x.com".to_string()));
    assert_eq!(storage.get(&40), Some("b@x.com".to_string()));
    assert_eq!(storage.get(&50), None);

    // A second row at the same index key is rejected.
    let err = storage
        .insert(30u32, "c@x.com".to_string(), "by_age")
        .unwrap_err();
    assert!(matches!(err, Error::DuplicateKey(_)), "got {err:?}");
}

#[test]
fn non_unique_index_over_string_primary_key() {
    let mut storage: NonUniqueStorage<u32, String> = NonUniqueStorage::new();
    storage.insert(30u32, "a@x.com".to_string(), "by_age").unwrap();
    storage.insert(30u32, "b@x.com".to_string(), "by_age").unwrap();
    storage.insert(40u32, "c@x.com".to_string(), "by_age").unwrap();

    let mut at_30: Vec<String> = storage.get_ids(&30).collect();
    at_30.sort();
    assert_eq!(at_30, vec!["a@x.com".to_string(), "b@x.com".to_string()]);

    let at_40: Vec<String> = storage.get_ids(&40).collect();
    assert_eq!(at_40, vec!["c@x.com".to_string()]);
}
```

Both tests need `IndexStorage` in scope for `insert` — it is a trait method, not an inherent one. Add `use super::IndexStorage;` inside the test module if it is not already imported.

- [ ] **Step 2: Run the test to verify it fails**

Run: `cargo test --lib index::tests::unique_index_over_string_primary_key`
Expected: FAIL — `UniqueStorage` takes one type parameter, not two.

- [ ] **Step 3: Thread the row-key parameter through `src/index.rs`**

Perform these edits across the file:

1. Rename the existing index-key parameter `K` to `IK` everywhere in the file (trait, structs, impls, method signatures, `where` clauses).
2. Add a new row-key parameter `K: PrimaryKey` to `IndexMaintainer`, `ManagedIndex`, `UniqueStorage`, and `NonUniqueStorage`.
3. Replace every `u64` that denotes a *row id* with `K`:
   - `UniqueStorage.tree: BTree<IK, u64>` → `BTree<IK, K>`
   - `NonUniqueStorage.tree: BTree<(IK, u64), ()>` → `BTree<(IK, K), ()>`
   - `fn rebuild_from_sorted_data(&mut self, data: &BTree<u64, R>)` → `&BTree<K, R>` (all three impls)
   - `let new_tree: BTree<IK, u64> = BTree::from_sorted(...)` → `BTree<IK, K>`
   - `let new_tree: BTree<(IK, u64), ()> = BTree::from_sorted(...)` → `BTree<(IK, K), ()>`
   - every `on_insert` / `on_update` / `on_delete` parameter named `id: u64` → `key: K` (pass by value; `K: Clone`)
4. Add `use crate::primary_key::PrimaryKey;` at the top.

Do **not** change `Table` in this task — it continues to instantiate these types at `K = u64`, which is why the tree still compiles.

- [ ] **Step 4: Point `Table`'s index map at the `u64` instantiation**

In `src/table.rs`, the field becomes:

```rust
    indexes: BTreeMap<String, Box<dyn IndexMaintainer<R, u64>>>,
```

Apply the same change to `TableSnapshot`. This is the only `table.rs` change in this task; it keeps the tree green without widening `Table` yet.

- [ ] **Step 5: Run the tests to verify they pass**

Run: `cargo test --lib index`
Expected: PASS, including the two new tests.

- [ ] **Step 6: Verify the full gate**

```bash
cargo test
cargo test --features persistence
cargo test -p ultima-vector
cargo clippy --all-targets --features persistence -- -D warnings
```

Expected: all pass, zero warnings.

- [ ] **Step 7: Commit**

```bash
git add src/index.rs src/table.rs
git commit -m "refactor(index): parameterize index storage over the row key type

IndexMaintainer/ManagedIndex/UniqueStorage/NonUniqueStorage now carry a
row-key parameter K: PrimaryKey; the index key is renamed K -> IK so K means
the primary key crate-wide. Table still instantiates at K = u64, so behavior
is unchanged."
```

---

### Task 2b: `BTree` prefix range, and removing the `RowBound` sentinel

**Inserted 2026-07-30, after Task 2's review.** Task 2 could not express "every
entry whose first tuple component equals `IK`" as a `RangeBounds<(IK, K)>`,
because that needs concrete endpoint values and `PrimaryKey` guarantees only
`Ord` — `String` has no maximum. It worked around this with a private
`RowBound<K> = NegInf | Row(K) | PosInf` sentinel. That is correct (the review
verified range semantics exhaustively) but it **measurably regresses the
default path**: `size_of::<(u32, u64)>()` is 16 bytes, `size_of::<(u32,
RowBound<u64>)>()` is 24 — +50% per non-unique index entry, ~+33% per B-tree
node, on every existing `u64` user. The cost is specific to niche-less scalar
keys; for `String` row keys the enum packs into the pointer niche and is free.

Peter's ruling: add the primitive and remove the sentinel.

**Files:**
- Modify: `src/btree.rs` (the `range` method at `:536` and the `BTreeRange`
  iterator's bound handling / `descend_left_from` / `descend_right_from`)
- Modify: `src/index.rs` (delete `RowBound`, restore `BTree<(IK, K), ()>`)
- Test: `src/btree.rs` and `src/index.rs` in-file tests

**Interfaces:**
- Consumes: Task 2's generic index storage.
- Produces:
  - `BTree::<(A, B), V>::range_prefix<'a>(&'a self, prefix: &'a A) -> impl Iterator<Item = (&'a (A, B), &'a V)> + 'a` — every entry whose first component equals `prefix`, ascending, in O(log n + k).
  - `NonUniqueStorage<IK, K> { tree: BTree<(IK, K), ()> }` — the bare composite, as the original plan specified. `RowBound` is gone.

**Approach.** Do not add a second iterator type. `BTreeRange` currently stores
`start: Bound<K>` / `end: Bound<K>` and `descend_left_from` / `descend_right_from`
consume them. Generalize that to a *monotone locator* — a `Fn(&K) -> Ordering`
reporting whether a key is before (`Less`), inside (`Equal`), or after
(`Greater`) the range — and have the existing `range()` construct a locator
from its bounds so its behavior is bit-for-bit unchanged. `range_prefix` then
supplies a locator that compares only the tuple's first component. The locator
must be monotone with respect to key order; document that as the safety
contract, since a non-monotone one would silently truncate the scan.

**`src/btree.rs` is the crate's most performance-sensitive file and the one the
Lean model covers.** The formal model covers node invariants (ordering,
fill factor), not range iteration, so this change does not invalidate it — but
keep the edit surgical and do not restructure anything the descent path does
not require.

- [ ] **Step 1: Write the failing tests**

Add to `#[cfg(test)] mod tests` in `src/btree.rs`:

```rust
    #[test]
    fn range_prefix_returns_exactly_the_matching_group() {
        let mut t: BTree<(u32, String), ()> = BTree::new();
        for (a, b) in [
            (1u32, "x"),
            (2, "a"),
            (2, "b"),
            (2, "c"),
            (3, "y"),
        ] {
            t = t.insert((a, b.to_string()), ());
        }

        let got: Vec<(u32, String)> = t.range_prefix(&2).map(|(k, _)| k.clone()).collect();
        assert_eq!(
            got,
            vec![
                (2, "a".to_string()),
                (2, "b".to_string()),
                (2, "c".to_string())
            ]
        );
    }

    #[test]
    fn range_prefix_handles_first_last_absent_and_empty() {
        let mut t: BTree<(u32, String), ()> = BTree::new();
        for (a, b) in [(1u32, "p"), (1, "q"), (5, "r")] {
            t = t.insert((a, b.to_string()), ());
        }

        // First group.
        assert_eq!(t.range_prefix(&1).count(), 2);
        // Last group.
        assert_eq!(t.range_prefix(&5).count(), 1);
        // Absent prefix between two present ones.
        assert_eq!(t.range_prefix(&3).count(), 0);
        // Absent prefix below and above everything.
        assert_eq!(t.range_prefix(&0).count(), 0);
        assert_eq!(t.range_prefix(&9).count(), 0);
        // Empty tree.
        let empty: BTree<(u32, String), ()> = BTree::new();
        assert_eq!(empty.range_prefix(&1).count(), 0);
    }

    /// The scan must not degrade to O(n): a prefix group of 3 in a tree of
    /// 10_000 must not visit the whole tree. Asserted via correctness at
    /// scale plus the group boundary, which a full scan would still pass —
    /// so this test guards correctness, and the O(log n + k) claim rests on
    /// the descent being bound-driven rather than filtered.
    #[test]
    fn range_prefix_is_correct_at_scale() {
        let mut t: BTree<(u32, u32), ()> = BTree::new();
        for i in 0..10_000u32 {
            t = t.insert((i % 1000, i), ());
        }
        let got: Vec<u32> = t.range_prefix(&500).map(|(k, _)| k.1).collect();
        assert_eq!(got, vec![500, 1500, 2500, 3500, 4500, 5500, 6500, 7500, 8500, 9500]);
    }

    /// `range()` must be unchanged by the locator refactor.
    #[test]
    fn range_still_honors_every_bound_combination() {
        use std::ops::Bound;
        let mut t: BTree<u32, ()> = BTree::new();
        for i in [10u32, 20, 30, 40] {
            t = t.insert(i, ());
        }
        let cases: Vec<((Bound<u32>, Bound<u32>), Vec<u32>)> = vec![
            ((Bound::Unbounded, Bound::Unbounded), vec![10, 20, 30, 40]),
            ((Bound::Included(20), Bound::Included(30)), vec![20, 30]),
            ((Bound::Excluded(20), Bound::Included(40)), vec![30, 40]),
            ((Bound::Included(20), Bound::Excluded(40)), vec![20, 30]),
            ((Bound::Excluded(10), Bound::Excluded(40)), vec![20, 30]),
            ((Bound::Included(25), Bound::Unbounded), vec![30, 40]),
            ((Bound::Unbounded, Bound::Excluded(10)), vec![]),
        ];
        for ((s, e), want) in cases {
            let got: Vec<u32> = t.range((s, e)).map(|(k, _)| *k).collect();
            assert_eq!(got, want, "bounds ({s:?}, {e:?})");
        }
    }
```

And add to `#[cfg(test)] mod tests` in `src/index.rs`, closing the review's
Issue 4 (nothing currently guards `range_ids` at a non-`u64` row key):

```rust
    #[test]
    fn non_unique_range_ids_over_string_primary_key() {
        use std::ops::Bound;
        let mut storage: NonUniqueStorage<u32, String> = NonUniqueStorage::new();
        storage.insert(10u32, "a@x.com".to_string(), "by_age").unwrap();
        storage.insert(20u32, "b@x.com".to_string(), "by_age").unwrap();
        storage.insert(20u32, "c@x.com".to_string(), "by_age").unwrap();
        storage.insert(30u32, "d@x.com".to_string(), "by_age").unwrap();

        let mut got: Vec<String> = storage
            .range_ids((Bound::Included(20u32), Bound::Included(30u32)))
            .map(|(_, k)| k)
            .collect();
        got.sort();
        assert_eq!(
            got,
            vec!["b@x.com".to_string(), "c@x.com".to_string(), "d@x.com".to_string()]
        );

        // Excluding the lower bound must drop that whole group, not part of it.
        let mut got: Vec<String> = storage
            .range_ids((Bound::Excluded(20u32), Bound::Unbounded))
            .map(|(_, k)| k)
            .collect();
        got.sort();
        assert_eq!(got, vec!["d@x.com".to_string()]);
    }
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test --lib btree::tests::range_prefix_returns_exactly_the_matching_group`
Expected: FAIL — `range_prefix` does not exist.

- [ ] **Step 3: Generalize `BTreeRange` to a monotone locator and add `range_prefix`**

Read `src/btree.rs`'s `range`, `BTreeRange`, `descend_left_from`, and
`descend_right_from` first. Replace the stored `start`/`end` `Bound<K>` pair
with the locator described in **Approach** above, and introduce one private
constructor both public entry points share:

```rust
    /// Iterate over every entry the monotone `locate` predicate reports as
    /// `Ordering::Equal`, ascending.
    ///
    /// `locate` must be monotone with respect to key order — `Less` for keys
    /// before the range, `Equal` inside, `Greater` after. A non-monotone
    /// predicate silently truncates the scan.
    fn range_by<'a>(
        &'a self,
        locate: impl Fn(&K) -> std::cmp::Ordering + 'a,
    ) -> BTreeRange<'a, K, V>
```

Rebuild the existing `pub fn range` on top of `range_by` by deriving a locator
from its `RangeBounds`, so its observable behavior is identical (the
`range_still_honors_every_bound_combination` test is what holds you to that).
Then add:

```rust
impl<A: Ord + Clone, B: Ord + Clone, V> BTree<(A, B), V> {
    /// Iterate over every entry whose first key component equals `prefix`,
    /// in ascending order, in O(log n + k).
    ///
    /// This exists because a prefix scan cannot be written as a
    /// `RangeBounds<(A, B)>` without inventing minimum and maximum values
    /// for `B`, which do not exist for types like `String`.
    pub fn range_prefix<'a>(
        &'a self,
        prefix: &'a A,
    ) -> impl Iterator<Item = (&'a (A, B), &'a V)> + 'a {
        // locator: compare only the first component
        self.range_by(move |k: &(A, B)| k.0.cmp(prefix))
    }
}
```

- [ ] **Step 4: Delete `RowBound` and restore the bare composite**

In `src/index.rs`: remove the `RowBound` enum and every use of it.
`NonUniqueStorage<IK, K>` goes back to `tree: BTree<(IK, K), ()>`, `get_ids`
becomes a `range_prefix` call, and `range_ids` maps its `IK` bounds onto the
locator path. Remove the now-dead `filter_map` that skipped sentinels.

- [ ] **Step 5: Pin the six drifted tests back to `u64`**

Task 2's review found that after the widening, six tests silently infer
`K = i32` instead of `u64`, removing `u64` coverage from the unique-index
lifecycle. Pin each by making one `on_insert` id argument an explicit `u64`
literal (the same one-token fix already applied to
`clone_box_produces_independent_copy`):

`src/index.rs` tests `unique_index_insert_and_lookup`,
`unique_index_rejects_duplicate`, `unique_index_update_changes_key`,
`unique_index_update_rejects_conflict`, `unique_index_delete`, and
`unique_compound_index`.

- [ ] **Step 6: Confirm the sentinel is gone and sizes are restored**

```bash
grep -rn "RowBound" src/ && echo "FAIL: sentinel still present" || echo "sentinel removed"
```

Expected: "sentinel removed".

- [ ] **Step 7: Run the tests to verify they pass**

Run: `cargo test --lib btree` and `cargo test --lib index`
Expected: PASS, including all five new tests.

- [ ] **Step 8: Verify the full gate**

```bash
cargo test
cargo test --features persistence
cargo test -p ultima-vector
cargo check --lib --no-default-features
cargo clippy --all-targets --features persistence -- -D warnings
```

- [ ] **Step 9: Commit**

```bash
git add src/btree.rs src/index.rs
git commit -m "perf(btree): range_prefix primitive; drop the RowBound sentinel

Task 2 wrapped non-unique index row keys in a NegInf|Row|PosInf sentinel
because a prefix scan could not be expressed as RangeBounds without min/max
values that PrimaryKey does not provide. That cost +50% per non-unique index
entry on the default u64 path. BTreeRange now takes a monotone locator
instead of a Bound pair; range() is rebuilt on it unchanged, and
range_prefix uses it to scan a tuple's first component directly, restoring
the bare (IK, K) composite. Also pins six index tests that had silently
drifted from u64 to i32 after the Task 2 widening."
```

---

### Task 3: `Table<R, K = u64>` and the reworked `MergeableTable`

**Files:**
- Modify: `src/table.rs` (`Table`, `TableSnapshot`, `TableDef`, `MergeableTable` at `:28-60`, and the `impl MergeableTable for Table<R>` block at `:63+`)
- Test: `src/table.rs` (in-file unit tests)

**Interfaces:**
- Consumes: `PrimaryKey`/`AutoKey` (Task 1), the generic index types (Task 2).
- Produces:
  - `pub struct Table<R, K = u64> { data: BTree<K, R>, next_id: K, indexes: BTreeMap<String, Box<dyn IndexMaintainer<R, K>>> }`
  - `pub struct TableDef<R: 'static, K = u64>`
  - `Table<R, K>::put(&mut self, key: K, record: R) -> Result<()>` — available for **all** `K`.
  - `Table<R, u64>::insert(&mut self, record: R) -> Result<u64>` — auto-increment, in an `impl<R: Record, K: AutoKey> Table<R, K>` block so it exists only for `AutoKey` keys.
  - `MergeableTable::merge_keys_from(&mut self, source: &dyn MergeableTable, keys: &dyn Any) -> Result<()>` — `keys` is a `&BTreeSet<K>` erased; the impl downcasts it.
  - `MergeableTable::collect_serialized_rows(...) -> Result<Vec<(Vec<u8>, Vec<u8>)>>` — encoded key bytes, record bytes.

**Why `merge_keys_from` takes `&dyn Any`:** a `Snapshot` holds `HashMap<String, Arc<dyn MergeableTable>>` with heterogeneous key types, so `K` must not appear in the trait signature. The caller (Task 6) holds the concrete `BTreeSet<K>` in the transaction's dirty entry and passes it as `&dyn Any`; the impl, which knows `K`, downcasts. A failed downcast is a `TypeMismatch` bug, not a user error.

- [ ] **Step 1: Write the failing test**

Add to `#[cfg(test)] mod tests` in `src/table.rs`:

```rust
#[test]
fn string_keyed_table_crud() {
    let mut t: Table<String, String> = Table::new();

    t.put("alice@x.com".to_string(), "Alice".to_string()).unwrap();
    t.put("bob@x.com".to_string(), "Bob".to_string()).unwrap();

    assert_eq!(t.get(&"alice@x.com".to_string()), Some(&"Alice".to_string()));
    assert_eq!(t.get(&"nobody@x.com".to_string()), None);
    assert_eq!(t.len(), 2);

    // put on an existing key overwrites.
    t.put("alice@x.com".to_string(), "Alice B".to_string()).unwrap();
    assert_eq!(t.get(&"alice@x.com".to_string()), Some(&"Alice B".to_string()));
    assert_eq!(t.len(), 2);

    t.delete(&"alice@x.com".to_string()).unwrap();
    assert_eq!(t.get(&"alice@x.com".to_string()), None);
    assert_eq!(t.len(), 1);
}

#[test]
fn string_keyed_table_iterates_in_key_order() {
    let mut t: Table<u32, String> = Table::new();
    t.put("c".to_string(), 3).unwrap();
    t.put("a".to_string(), 1).unwrap();
    t.put("b".to_string(), 2).unwrap();

    let keys: Vec<String> = t.iter().map(|(k, _)| k.clone()).collect();
    assert_eq!(keys, vec!["a".to_string(), "b".to_string(), "c".to_string()]);
}

#[test]
fn u64_table_still_auto_increments() {
    let mut t: Table<String> = Table::new();
    let a = t.insert("first".to_string()).unwrap();
    let b = t.insert("second".to_string()).unwrap();
    assert_eq!((a, b), (1, 2));
    assert_eq!(t.get(&1), Some(&"first".to_string()));
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test --lib table::tests::string_keyed_table_crud`
Expected: FAIL — `Table` takes one type parameter.

- [ ] **Step 3: Widen `Table` and `TableDef`**

In `src/table.rs`:

```rust
pub struct Table<R, K = u64> {
    data: BTree<K, R>,
    next_id: K,
    indexes: BTreeMap<String, Box<dyn IndexMaintainer<R, K>>>,
}

struct TableSnapshot<R, K = u64> {
    data: BTree<K, R>,
    next_id: K,
    indexes: BTreeMap<String, Box<dyn IndexMaintainer<R, K>>>,
}

pub struct TableDef<R: 'static, K = u64> {
    name: &'static str,
    _phantom: PhantomData<(R, K)>,
}
```

Split the existing `impl<R: Record> Table<R>` block in two:

- `impl<R: Record, K: PrimaryKey> Table<R, K>` — everything that does not assign ids: `get`, `put` (new; replaces the body of the old `insert_with_id`), `update`, `delete`, `len`, `is_empty`, `contains`, `first`, `last`, `iter`, `range`, `get_many`, `resolve`, all index methods, and the batch operations reworked to take explicit keys.
- `impl<R: Record, K: AutoKey> Table<R, K>` — `new()` (uses `K::first()`), `insert`, `insert_batch`, `next_id`, `set_next_id`, `insert_with_id`.

Every `id: u64` parameter that denotes a row key becomes `key: K` (by value where it is stored, by reference where it is only looked up — `get(&self, key: &K)`).

**`next_id` must become optional.** An explicitly-keyed table has no id counter at all, and there is no sensible `K` to initialize one with. So the field is `Option<K>` — as already written in the struct above, where the doc comment records the invariant:

```rust
pub struct Table<R, K = u64> {
    data: BTree<K, R>,
    /// `Some` only for `AutoKey` tables; `None` for explicitly-keyed ones.
    /// `insert`'s `AutoKey` block may unwrap it: `new()` is the only
    /// constructor reachable under an `AutoKey` bound and it always sets
    /// `Some`.
    next_id: Option<K>,
    indexes: BTreeMap<String, Box<dyn IndexMaintainer<R, K>>>,
}
```

`Table::<R, u64>::new()` (AutoKey block) sets `next_id: Some(K::first())`. Add the constructor for explicitly-keyed tables to the general block:

```rust
impl<R: Record, K: PrimaryKey> Table<R, K> {
    /// Creates an empty table addressed by explicit keys. Unlike
    /// [`Table::new`], there is no id counter — rows are added with
    /// [`Table::put`].
    pub fn new_keyed() -> Self {
        Self {
            data: BTree::new(),
            next_id: None,
            indexes: BTreeMap::new(),
        }
    }
}
```

Also widen the existing `pub(crate) fn from_bulk` (currently at `src/table.rs:221`) in the general block, since Task 4 and Task 7 both build tables through it:

```rust
    pub(crate) fn from_bulk(
        sorted_rows: Vec<(K, Arc<R>)>,
        next_id: Option<K>,
        mut index_defs: Vec<Box<dyn IndexMaintainer<R, K>>>,
    ) -> Result<Self> {
```

Its internal `debug_assert!` on ascending ids stays valid — `K: Ord`.

- [ ] **Step 4: Rework the `MergeableTable` trait**

Change the two `u64`-typed methods in the trait at `src/table.rs:28`:

```rust
    /// For each key in `keys`, take the writer's record at that key from
    /// `source` and apply it to `self`. `keys` is a `&BTreeSet<K>` erased to
    /// `&dyn Any`, because a snapshot holds tables with heterogeneous key
    /// types and `K` cannot appear in this trait's signature. The impl, which
    /// knows `K`, downcasts it; a failed downcast is an internal bug.
    fn merge_keys_from(&mut self, source: &dyn MergeableTable, keys: &dyn Any) -> Result<()>;

    /// Serialize every row to (encoded-key-bytes, record-bytes) pairs, in
    /// primary-key order.
    #[cfg(feature = "persistence")]
    fn collect_serialized_rows(
        &self,
        serialize_record: &(dyn Fn(&dyn Any) -> Result<Vec<u8>> + Send + Sync),
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>>;
```

And the impl header becomes:

```rust
impl<R: Record, K: PrimaryKey> MergeableTable for Table<R, K> {
```

with `merge_keys_from` opening:

```rust
    fn merge_keys_from(&mut self, source: &dyn MergeableTable, keys: &dyn Any) -> Result<()> {
        let keys = keys
            .downcast_ref::<BTreeSet<K>>()
            .ok_or_else(|| Error::TypeMismatch("merge key set".to_string()))?;
        let source = source
            .as_any()
            .downcast_ref::<Table<R, K>>()
            .ok_or_else(|| Error::TypeMismatch("merge source".to_string()))?;
        // ... existing per-key loop, with `id` renamed to `key` and
        // `*id` reads replaced by `key.clone()` where a value is needed.
```

and `collect_serialized_rows` mapping each key through `key.encode()`.

- [ ] **Step 5: Fix the call sites the widening breaks**

`src/store.rs`, `src/bulk_load.rs`, `src/registry.rs`, and `src/snapshot_stream/` reference `Table<R>` — all still valid via the default. The breakages will be at:
- `merge_keys_from` call sites (pass `&my_keys as &dyn Any`)
- `collect_serialized_rows` consumers now receiving `Vec<u8>` keys instead of `u64`

For this task, at each such site do the **minimal** adaptation that preserves current behavior at `K = u64`: at `collect_serialized_rows` consumers, decode with `u64::decode(&key_bytes)?`. Tasks 4 and 7 replace these with the properly generic paths. Leave a `// widened in task 4` / `// widened in task 7` comment at each so the follow-up is findable.

- [ ] **Step 6: Run the tests to verify they pass**

Run: `cargo test --lib table`
Expected: PASS, including the three new tests.

- [ ] **Step 7: Verify the full gate**

```bash
cargo test
cargo test --features persistence
cargo test -p ultima-vector
cargo check --lib --no-default-features
cargo clippy --all-targets --features persistence -- -D warnings
```

Expected: all pass, zero warnings.

- [ ] **Step 8: Commit**

```bash
git add src/table.rs src/store.rs src/bulk_load.rs src/registry.rs src/snapshot_stream
git commit -m "feat(table): Table<R, K = u64> generic over the primary key

The defaulted parameter keeps every existing Table<R>/TableDef<R> reference
valid. Auto-increment (new/insert/insert_batch/next_id) moves behind an
AutoKey bound so it exists only for u64; other keys use put(). MergeableTable
stays object-safe: merge_keys_from takes an erased &BTreeSet<K> and
collect_serialized_rows returns encoded key bytes, because a snapshot holds
heterogeneous key types."
```

---

### Task 3b: Widen `CustomIndex` and `FullTextIndex` over the row key

**Inserted 2026-07-31, after Task 3's review.** `CustomIndex` hard-codes
`id: u64` (`src/index.rs:396-402`), so Task 3 had to pin `define_custom_index`
and `custom_index` to `Table<R, u64>`. The consequence is that a non-`u64`-keyed
table can define **zero** custom indexes — including the built-in BM25
`FullTextIndex`. You could key a table by email but not full-text-search it.
Peter's ruling: widen it, so the feature ships without an asterisk.

**Files:**
- Modify: `src/index.rs` (`CustomIndex` trait at `:394-418`, `CustomIndexAdapter`)
- Modify: `src/fulltext.rs` (`SearchResult`, `FullTextIndex` fields and methods)
- Modify: `src/table.rs` (unpin `define_custom_index`/`custom_index` from the `Table<R, u64>` block)
- Test: `src/fulltext.rs` in-file tests, `tests/fulltext_integration.rs`, `tests/custom_index_api.rs`

**Interfaces:**
- Consumes: `PrimaryKey` (Task 1), `BTree::range_prefix` (Task 2b), `Table<R, K>` (Task 3).
- Produces:
  - `pub trait CustomIndex<R: Record, K: PrimaryKey = u64>: Send + Sync + Clone + 'static` with `on_insert(&mut self, key: K, record: &R)`, `on_update(&mut self, key: K, old: &R, new: &R)`, `on_delete(&mut self, key: K, record: &R)`, and `rebuild<'a>(&mut self, data: impl Iterator<Item = (K, &'a R)>)`.
  - `pub struct SearchResult<K = u64> { pub id: K, pub score: f64 }`
  - `pub struct FullTextIndex<R, K = u64>` with `postings: BTree<(String, K), u32>` and `doc_lengths: BTree<K, u32>`.
  - `Table<R, K>::define_custom_index` / `custom_index` available for **all** `K`.

**The defaulted parameter is what makes this non-breaking:** every existing
downstream `impl CustomIndex<R> for MyIndex` keeps compiling, because `K`
defaults to `u64` in the trait's own parameter list exactly as it does on
`Table`. Verify that claim with the existing `tests/custom_index_api.rs`, which
implements the trait from outside the crate's own modules — it should need no
edits at all. If it does, say so; that would mean the widening is breaking and
the plan owner needs to know.

**Two specifics that are not mechanical:**

1. **`src/fulltext.rs:126` currently reads**
   `postings.range((token.clone(), 0u64)..=(token.clone(), u64::MAX))`.
   That construction has no generic equivalent — it is precisely the
   min/max-value assumption that Task 2b removed from the index layer. Replace
   it with `self.postings.range_prefix(token)`, the primitive Task 2b added.
   This also drops two `String` clones per token from the BM25 scan.
2. **`scores: HashMap<u64, f64>` at `src/fulltext.rs:117` cannot stay a
   `HashMap`** — `PrimaryKey` requires `Ord`, not `Hash`. Use
   `BTreeMap<K, f64>`. Do not add a `Hash` bound to `PrimaryKey` to preserve the
   `HashMap`; that would constrain every key type for one call site's
   convenience.

`total_docs` and `total_doc_length` stay `u64` — they are counts, not keys.
Do not widen them.

- [ ] **Step 1: Write the failing test**

Add to `#[cfg(test)] mod tests` in `src/fulltext.rs`:

```rust
    #[test]
    fn full_text_search_over_a_string_keyed_table() {
        #[derive(Clone, Debug)]
        struct Doc {
            body: String,
        }

        let mut idx: FullTextIndex<Doc, String> =
            FullTextIndex::new(|d: &Doc| d.body.clone());

        idx.on_insert("doc-a".to_string(), &Doc { body: "the quick brown fox".into() })
            .unwrap();
        idx.on_insert("doc-b".to_string(), &Doc { body: "the lazy brown dog".into() })
            .unwrap();
        idx.on_insert("doc-c".to_string(), &Doc { body: "unrelated content".into() })
            .unwrap();

        let hits = idx.search("brown", 10);
        let mut ids: Vec<String> = hits.iter().map(|h| h.id.clone()).collect();
        ids.sort();
        assert_eq!(ids, vec!["doc-a".to_string(), "doc-b".to_string()]);

        // "fox" is unique to doc-a.
        let hits = idx.search("fox", 10);
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].id, "doc-a".to_string());

        // Deleting removes the document from future results.
        idx.on_delete("doc-a".to_string(), &Doc { body: "the quick brown fox".into() });
        assert!(idx.search("fox", 10).is_empty());
        assert_eq!(idx.search("brown", 10).len(), 1);
    }
```

Adjust `FullTextIndex::new`'s constructor call to match its real signature —
read it first; the extractor shape above is illustrative of intent, not
necessarily its exact form.

- [ ] **Step 2: Run the test to verify it fails**

Run: `cargo test --features fulltext --lib fulltext::tests::full_text_search_over_a_string_keyed_table`
Expected: FAIL — `FullTextIndex` takes one type parameter.

- [ ] **Step 3: Widen the `CustomIndex` trait and its adapter**

In `src/index.rs`, add the defaulted `K: PrimaryKey = u64` parameter to
`CustomIndex` and change the four `id: u64` occurrences (three methods plus
`rebuild`'s iterator item) to `key: K`. Widen `CustomIndexAdapter` so it
implements `IndexMaintainer<R, K>` rather than `IndexMaintainer<R, u64>`.

- [ ] **Step 4: Widen `FullTextIndex`**

In `src/fulltext.rs`: add `K = u64` to `SearchResult` and `FullTextIndex`,
change `postings` to `BTree<(String, K), u32>` and `doc_lengths` to
`BTree<K, u32>`, switch `scores` to `BTreeMap<K, f64>`, and replace the
`(token, 0u64)..=(token, u64::MAX)` range with `range_prefix` per the note
above. Leave `total_docs` and `total_doc_length` as `u64`.

- [ ] **Step 5: Unpin `define_custom_index` / `custom_index`**

In `src/table.rs`, move these two methods out of the `Table<R, u64>`-pinned
block back into the general `impl<R: Record, K: PrimaryKey> Table<R, K>` block.
Remove the pinned block if it becomes empty.

- [ ] **Step 6: Confirm the widening is source-compatible for downstream implementors**

Run: `cargo test --features fulltext --test custom_index_api`
Expected: PASS **with no edits to that file.** If it required edits, stop and
report exactly what changed — that would make the widening a breaking change
rather than an additive one, which the plan owner must know before release.

- [ ] **Step 7: Run the tests to verify they pass**

Run: `cargo test --features fulltext` and `cargo test --features persistence,fulltext`
Expected: PASS, including the new String-keyed test.

- [ ] **Step 8: Verify the full gate**

```bash
cargo test
cargo test --features persistence
cargo test --features fulltext
cargo test -p ultima-vector
cargo check --lib --no-default-features
cargo clippy --all-targets --features persistence,fulltext -- -D warnings
```

- [ ] **Step 9: Commit**

```bash
git add src/index.rs src/fulltext.rs src/table.rs tests/
git commit -m "feat(index): widen CustomIndex and FullTextIndex over the row key

CustomIndex hard-coded id: u64, so Task 3 had to pin define_custom_index to
u64-keyed tables — meaning a String-keyed table could define no custom index
and no full-text search. Both now carry a defaulted K = u64, so every
existing downstream impl stays source-compatible.

FullTextIndex's posting scan used (token, 0u64)..=(token, u64::MAX), which
has no generic equivalent; it now uses the range_prefix primitive added in
task 2b, dropping two String clones per token as a side effect."
```

---

### Task 4: Registry — type-erased closures over encoded keys, table format v2

**Files:**
- Modify: `src/registry.rs` (closure type aliases at `:25-45`, `register` at `:87`, `build_table_from_raw` at `:253`, `serialize_table` at `:267`, `deserialize_table` at `:292`)
- Test: `src/registry.rs` (in-file unit tests)

**Interfaces:**
- Consumes: `PrimaryKey` (Task 1), `Table<R, K>` (Task 3).
- Produces:
  - `ReplayInsertFn = Box<dyn Fn(&mut dyn Any, &[u8], &[u8]) -> Result<()> + Send + Sync>` (key bytes, record bytes)
  - `ReplayUpdateFn` — same shape.
  - `ReplayDeleteFn = Box<dyn Fn(&mut dyn Any, &[u8]) -> Result<()> + Send + Sync>`
  - `BuildFromRawRowsFn = Box<dyn Fn(Vec<(Vec<u8>, Vec<u8>)>, Option<&dyn MergeableTable>) -> Result<Box<dyn MergeableTable>> + Send + Sync>`
  - `TableRegistry::register<R: Record, K: PrimaryKey>(&mut self, name: &str) -> Result<()>`
  - Table serialization format **v2**: `[format_version: u8 = 2][has_next_id: u8][next_id: encoded-key-bytes, length-prefixed][num_entries: u64][key_len: u32, key_bytes, record_len: u32, record_bytes]*`

**Why the format changes:** v1 was `[next_id: u64][count: u64][id: u64, record_bytes]*` — every row key was a fixed 8-byte integer. Variable-length keys need explicit lengths, and non-`AutoKey` tables have no `next_id` at all.

- [ ] **Step 1: Write the failing test**

Add to `#[cfg(test)] mod tests` in `src/registry.rs`:

```rust
#[test]
fn roundtrip_string_keyed_table() {
    let mut reg = TableRegistry::new();
    reg.register::<String, String>("emails").unwrap();

    let mut t: Table<String, String> = Table::new_keyed();
    t.put("a@x.com".to_string(), "Alice".to_string()).unwrap();
    t.put("b@x.com".to_string(), "Bob".to_string()).unwrap();

    let info = reg.get("emails").unwrap();
    let bytes = (info.serialize_table)(&t as &dyn std::any::Any).unwrap();
    let restored = (info.deserialize_table)(&bytes).unwrap();
    let restored = restored
        .as_any()
        .downcast_ref::<Table<String, String>>()
        .unwrap();

    assert_eq!(restored.len(), 2);
    assert_eq!(
        restored.get(&"a@x.com".to_string()),
        Some(&"Alice".to_string())
    );
    assert_eq!(restored.get(&"b@x.com".to_string()), Some(&"Bob".to_string()));
}

#[test]
fn roundtrip_u64_keyed_table_preserves_next_id() {
    let mut reg = TableRegistry::new();
    reg.register::<String, u64>("rows").unwrap();

    let mut t: Table<String> = Table::new();
    t.insert("first".to_string()).unwrap();
    t.insert("second".to_string()).unwrap();

    let info = reg.get("rows").unwrap();
    let bytes = (info.serialize_table)(&t as &dyn std::any::Any).unwrap();
    let restored = (info.deserialize_table)(&bytes).unwrap();
    let restored = restored.as_any().downcast_ref::<Table<String>>().unwrap();

    assert_eq!(restored.len(), 2);
    assert_eq!(restored.next_id(), 3);
}

#[test]
fn deserialize_rejects_v1_format() {
    // A v1 payload starts with an 8-byte next_id, so its first byte is 0
    // for any realistic next_id — never the v2 marker (2).
    let mut reg = TableRegistry::new();
    reg.register::<String, u64>("rows").unwrap();
    let info = reg.get("rows").unwrap();

    let v1_bytes = vec![0u8; 24];
    let err = (info.deserialize_table)(&v1_bytes).unwrap_err();
    assert!(
        format!("{err}").contains("format version"),
        "expected a format-version error, got: {err}"
    );
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test --lib registry --features persistence`
Expected: FAIL — `register` takes one type parameter.

- [ ] **Step 3: Add the key parameter and rewrite the serialization**

In `src/registry.rs`:

1. Change the six closure aliases as listed in **Interfaces** above.
2. `pub fn register<R: Record, K: PrimaryKey>(&mut self, name: &str) -> Result<()>` — every closure body it builds now works with `Table<R, K>` and encodes/decodes keys via `K::encode` / `K::decode`.
3. Replace `serialize_table` / `deserialize_table` with the v2 format:

> **Superseded during implementation.** The one-byte header below is wrong and
> was corrected to a two-byte `[magic 0xFF][version 2]`: `bincode`'s standard
> config is a varint encoding, so a v1 payload for a table with `next_id == 2`
> begins with the literal byte `0x02` and a bare version byte would have
> silently misread it as v2. `0xFF` is not a legal varint tag. The real layout
> is in `docs/tasks/task56_arbitrary_primary_keys.md`.

```rust
/// Format v2: `[version: u8 = 2][has_next_id: u8][next_id_len: u32,
/// next_id_bytes]?[num_entries: u64][key_len: u32, key_bytes, rec_len: u32,
/// rec_bytes]*`
///
/// v1 (`[next_id: u64][count: u64][id: u64, rec]*`) is rejected: variable-
/// length keys need explicit lengths, and an explicitly-keyed table has no
/// `next_id` at all.
const TABLE_FORMAT_V2: u8 = 2;

fn serialize_table<R: Record, K: PrimaryKey>(table: &Table<R, K>) -> Result<Vec<u8>> {
    let mut buf = Vec::new();
    buf.push(TABLE_FORMAT_V2);
    match table.next_id_opt() {
        Some(id) => {
            buf.push(1u8);
            let enc = id.encode();
            buf.extend_from_slice(&(enc.len() as u32).to_be_bytes());
            buf.extend_from_slice(&enc);
        }
        None => buf.push(0u8),
    }
    buf.extend_from_slice(&(table.len() as u64).to_be_bytes());
    let config = bincode::config::standard();
    for (key, record) in table.iter() {
        let kb = key.encode();
        buf.extend_from_slice(&(kb.len() as u32).to_be_bytes());
        buf.extend_from_slice(&kb);
        let rb = bincode::serde::encode_to_vec(record, config)
            .map_err(|e| Error::Serialization(e.to_string()))?;
        buf.extend_from_slice(&(rb.len() as u32).to_be_bytes());
        buf.extend_from_slice(&rb);
    }
    Ok(buf)
}
```

Write the matching `deserialize_table<R: Record, K: PrimaryKey>` that rejects any leading byte other than `TABLE_FORMAT_V2` with `Error::Corruption(format!("unsupported table format version {v}; rebuild from a 0.3.0+ checkpoint"))`, reads the optional `next_id`, then the entries, and builds via `Table::from_bulk(sorted_rows, next_id, Vec::new())` — the widened signature from Task 3, taking `Vec<(K, Arc<R>)>` and `Option<K>`. Rows arrive in ascending key order because `serialize_table` iterates the B-tree in key order, which satisfies `from_bulk`'s ascending-order `debug_assert!`.

4. Add `pub(crate) fn next_id_opt(&self) -> Option<K>` to `Table<R, K>` in `src/table.rs` (the general impl block), returning `self.next_id.clone()`.

- [ ] **Step 4: Update `Store::register_table` to pass the key type**

In `src/store.rs:817`, keep the existing signature working and add the keyed form:

```rust
    /// Registers a `u64`-keyed table type for persistence. Unchanged from
    /// 0.2.x.
    pub fn register_table<R: crate::persistence::Record>(&self, name: &str) -> Result<()> {
        self.register_table_keyed::<R, u64>(name)
    }

    /// Registers a table type with an explicit primary-key type.
    pub fn register_table_keyed<R: crate::persistence::Record, K: PrimaryKey>(
        &self,
        name: &str,
    ) -> Result<()> {
        // ... existing body, forwarding K to TableRegistry::register::<R, K>
    }
```

- [ ] **Step 5: Run the tests to verify they pass**

Run: `cargo test --lib registry --features persistence`
Expected: PASS, including the three new tests.

- [ ] **Step 6: Verify the full gate**

```bash
cargo test
cargo test --features persistence
cargo test -p ultima-vector
cargo clippy --all-targets --features persistence -- -D warnings
```

Expected: all pass. Checkpoint tests that hand-build v1 payloads will need their fixtures updated to v2 — update them, do not weaken the assertions.

- [ ] **Step 7: Commit**

```bash
git add src/registry.rs src/store.rs src/table.rs
git commit -m "feat(registry)!: type-erased closures over encoded keys; table format v2

The registry is the type-erasure boundary, so every closure that carried a
u64 row id now carries encoded key bytes. Table serialization moves to v2
(explicit key lengths, optional next_id); v1 payloads are rejected with an
actionable error. register_table stays u64-only; register_table_keyed is new."
```

---

### Task 5: WAL — key bytes, format bump, v1 rejection

**Files:**
- Modify: `src/wal.rs` (`WalOp` at `:81-110`, the encode path, the decode path at `:237` and `:263`, and the format-version constant)
- Test: `src/wal.rs` in-file tests, plus `tests/persistence_integration.rs`

**Interfaces:**
- Consumes: `PrimaryKey` (Task 1), the registry replay closures (Task 4).
- Produces:
  - `WalOp::Insert { table: String, key: Vec<u8>, data: Vec<u8> }`
  - `WalOp::Update { table: String, key: Vec<u8>, data: Vec<u8> }`
  - `WalOp::Delete { table: String, key: Vec<u8> }`
  - WAL header format version incremented; recovery of an older WAL returns `Error::Corruption` naming the version.

- [ ] **Step 1: Write the failing test**

Add to `tests/persistence_integration.rs`:

```rust
#[test]
fn string_keyed_table_survives_wal_recovery() {
    let dir = tempfile::tempdir().unwrap();

    {
        let store = Store::new(
            StoreConfig::builder()
                .persistence(Persistence::standalone(
                    dir.path(),
                    Durability::Consistent,
                    WalWrite::PerEntry,
                ))
                .build(),
        )
        .unwrap();
        store.register_table_keyed::<String, String>("emails").unwrap();

        let mut wtx = store.begin_write(None).unwrap();
        let mut t = wtx.open_table_keyed::<String, String>("emails").unwrap();
        t.put("alice@x.com".to_string(), "Alice".to_string()).unwrap();
        t.put("bob@x.com".to_string(), "Bob".to_string()).unwrap();
        drop(t);
        wtx.commit().unwrap();
    }

    let store = Store::new(
        StoreConfig::builder()
            .persistence(Persistence::standalone(
                dir.path(),
                Durability::Consistent,
                WalWrite::PerEntry,
            ))
            .build(),
    )
    .unwrap();
    store.register_table_keyed::<String, String>("emails").unwrap();
    store.recover().unwrap();

    let rtx = store.begin_read(None).unwrap();
    let t = rtx.open_table_keyed::<String, String>("emails").unwrap();
    assert_eq!(t.get(&"alice@x.com".to_string()), Some(&"Alice".to_string()));
    assert_eq!(t.get(&"bob@x.com".to_string()), Some(&"Bob".to_string()));
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `cargo test --features persistence --test persistence_integration string_keyed_table_survives_wal_recovery`
Expected: FAIL — `register_table_keyed`/`open_table_keyed` for the write path do not exist yet on the tx types (they arrive in Task 6). If the test cannot compile at this point, write it and leave it `#[ignore]`d with a comment naming Task 6, then remove the `#[ignore]` in Task 6's Step 5. Prefer that over weakening the test.

- [ ] **Step 3: Change `WalOp` to carry key bytes**

In `src/wal.rs`, replace `id: u64` with `key: Vec<u8>` in the three variants, update their doc comments to say "encoded primary key", and update every construction site (`src/store.rs`'s `WalOpsWriter` pushes) to pass `key.encode()`.

- [ ] **Step 4: Update the encode/decode paths and bump the format version**

The decode sites at `src/wal.rs:237` and `:263` read `(u64, _)` for the id; change them to read a length-prefixed byte string. Locate the WAL format-version constant and increment it. In the recovery path, a mismatched version must produce:

```rust
return Err(Error::Corruption(format!(
    "WAL format version {found} is not supported by this build (expected {expected}); \
     0.3.0 changed the on-disk key encoding — recover from a 0.2.x build, \
     checkpoint, and re-open with 0.3.0"
)));
```

- [ ] **Step 5: Run the tests to verify they pass**

Run: `cargo test --features persistence wal` and `cargo test --features persistence --test persistence_integration`
Expected: PASS. Existing WAL tests that construct `WalOp` with `id:` need their fixtures updated to `key:`; update them rather than deleting them.

- [ ] **Step 6: Verify the full gate**

```bash
cargo test
cargo test --features persistence
cargo test -p ultima-vector
cargo clippy --all-targets --features persistence -- -D warnings
```

- [ ] **Step 7: Commit**

```bash
git add src/wal.rs src/store.rs tests/persistence_integration.rs
git commit -m "feat(wal)!: WalOp carries encoded key bytes; format version bumped

Insert/Update/Delete now carry Vec<u8> encoded keys instead of u64 ids.
Recovery rejects older WAL files with an actionable error rather than
carrying a v1 compatibility branch."
```

---

### Task 6: Store — `open_table_keyed`, hashed write-set, exact merge keys

**Files:**
- Modify: `src/store.rs` (`DirtyEntry` at `:1931`, `WriteTx.write_set` at `:1961`, `open_table` at `:2806`, `ReadTx::open_table` at `:1883`, the commit merge path around `:3727`)
- Test: `tests/store_integration.rs`

**Interfaces:**
- Consumes: everything from Tasks 1–5.
- Produces:
  - `WriteTx::open_table_keyed<R: Record, K: PrimaryKey>(&mut self, opener: impl TableOpener<R>) -> Result<TableWriter<'_, R, K>>`
  - `ReadTx::open_table_keyed<R: Record, K: PrimaryKey>(&self, opener: impl TableOpener<R>) -> Result<TableReader<'_, R, K>>`
  - `TableWriter<'tx, R, K = u64>` / `TableReader<'tx, R, K = u64>`
  - `DirtyEntry` gains `modified_keys: Box<dyn Any + Send + Sync>` holding a `BTreeSet<K>`.
  - `write_set: BTreeMap<String, BTreeSet<u64>>` — **unchanged shape**, now holding `K::hash64()` digests.

**The two-structure split, and why:** conflict detection compares key sets across writers that may not agree on `K`, so it uses 64-bit hashes — a collision causes a spurious conflict (a retry), never a missed one, so the detector stays sound. The *merge* needs exact keys to replay, so `DirtyEntry` carries the concrete `BTreeSet<K>` type-erased alongside. Both are updated on every mutation.

- [ ] **Step 1: Write the failing tests**

Add to `tests/store_integration.rs`:

```rust
#[test]
fn string_keyed_table_end_to_end() {
    let store = Store::default();

    let mut wtx = store.begin_write(None).unwrap();
    let mut t = wtx.open_table_keyed::<String, String>("emails").unwrap();
    t.put("alice@x.com".to_string(), "Alice".to_string()).unwrap();
    drop(t);
    let v = wtx.commit().unwrap();

    let rtx = store.begin_read(Some(v)).unwrap();
    let t = rtx.open_table_keyed::<String, String>("emails").unwrap();
    assert_eq!(t.get(&"alice@x.com".to_string()), Some(&"Alice".to_string()));
}

#[test]
fn multiwriter_disjoint_string_keys_both_commit() {
    let store = Store::new(
        StoreConfig::builder()
            .writer_mode(WriterMode::MultiWriter)
            .build(),
    )
    .unwrap();

    let mut w1 = store.begin_write(None).unwrap();
    let mut t1 = w1.open_table_keyed::<String, String>("emails").unwrap();
    t1.put("a@x.com".to_string(), "A".to_string()).unwrap();
    drop(t1);

    let mut w2 = store.begin_write(None).unwrap();
    let mut t2 = w2.open_table_keyed::<String, String>("emails").unwrap();
    t2.put("b@x.com".to_string(), "B".to_string()).unwrap();
    drop(t2);

    w1.commit().unwrap();
    let v = w2.commit().expect("disjoint keys must not conflict");

    let rtx = store.begin_read(Some(v)).unwrap();
    let t = rtx.open_table_keyed::<String, String>("emails").unwrap();
    assert_eq!(t.get(&"a@x.com".to_string()), Some(&"A".to_string()));
    assert_eq!(t.get(&"b@x.com".to_string()), Some(&"B".to_string()));
}

#[test]
fn multiwriter_same_string_key_conflicts() {
    let store = Store::new(
        StoreConfig::builder()
            .writer_mode(WriterMode::MultiWriter)
            .build(),
    )
    .unwrap();

    let mut seed = store.begin_write(None).unwrap();
    let mut t = seed.open_table_keyed::<String, String>("emails").unwrap();
    t.put("a@x.com".to_string(), "seed".to_string()).unwrap();
    drop(t);
    seed.commit().unwrap();

    let mut w1 = store.begin_write(None).unwrap();
    let mut t1 = w1.open_table_keyed::<String, String>("emails").unwrap();
    t1.put("a@x.com".to_string(), "one".to_string()).unwrap();
    drop(t1);

    let mut w2 = store.begin_write(None).unwrap();
    let mut t2 = w2.open_table_keyed::<String, String>("emails").unwrap();
    t2.put("a@x.com".to_string(), "two".to_string()).unwrap();
    drop(t2);

    w1.commit().unwrap();
    let err = w2.commit().expect_err("same key must conflict");
    assert!(matches!(err, Error::WriteConflict { .. }), "got {err:?}");
}

/// Proves the hashed write-set is conservative rather than lossy: two keys
/// forced to the same digest must conflict (spuriously), never silently
/// both-commit.
#[test]
fn hash_collision_causes_spurious_conflict_not_a_missed_one() {
    // Uses the documented property that conflict detection is on hash64().
    // Rather than fabricating a collision, assert the direction: a key set
    // that overlaps by hash is treated as overlapping.
    let store = Store::new(
        StoreConfig::builder()
            .writer_mode(WriterMode::MultiWriter)
            .build(),
    )
    .unwrap();
    let mut w = store.begin_write(None).unwrap();
    let mut t = w.open_table_keyed::<String, String>("emails").unwrap();
    t.put("k".to_string(), "v".to_string()).unwrap();
    drop(t);
    // The write set records the digest of "k", not the string.
    assert!(w.write_set_digests("emails").contains(&"k".to_string().hash64()));
}
```

If exposing `write_set_digests` for the last test is unpalatable, make it `#[cfg(test)]`-only rather than public API.

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test --test store_integration string_keyed_table_end_to_end`
Expected: FAIL — `open_table_keyed` does not exist.

- [ ] **Step 3: Widen `TableWriter`/`TableReader` and add the keyed openers**

Add `K = u64` to both handle types, defaulting so existing uses are untouched. Refactor the existing `open_table` body into a private `open_table_inner<R, K>`, then:

```rust
    /// Opens a `u64`-keyed table. Unchanged from 0.2.x.
    pub fn open_table<R: Record>(
        &mut self,
        opener: impl TableOpener<R>,
    ) -> Result<TableWriter<'_, R, u64>> {
        self.open_table_inner::<R, u64>(opener)
    }

    /// Opens a table whose primary key is `K` rather than an auto-increment
    /// `u64`. Rows are addressed with [`TableWriter::put`] and
    /// [`TableWriter::get`]; there is no auto-increment for non-`u64` keys.
    pub fn open_table_keyed<R: Record, K: PrimaryKey>(
        &mut self,
        opener: impl TableOpener<R>,
    ) -> Result<TableWriter<'_, R, K>> {
        self.open_table_inner::<R, K>(opener)
    }
```

Mirror the same pair on `ReadTx`. Leave `open_tables2`/`open_tables3` `u64`-only — no keyed variants (YAGNI; add them when a caller needs one).

- [ ] **Step 4: Add the exact-key set to `DirtyEntry` and record both on mutation**

```rust
struct DirtyEntry {
    table: Box<dyn MergeableTable>,
    table_metrics: Arc<crate::metrics::TableMetrics>,
    name: Arc<str>,
    /// The writer's modified keys as a `BTreeSet<K>`, erased because
    /// `DirtyEntry` is stored in a map alongside entries with different `K`.
    /// Used by the commit merge; the parallel `WriteTx::write_set` holds
    /// `hash64()` digests of the same keys for conflict detection.
    modified_keys: Box<dyn Any + Send + Sync>,
}
```

Every `TableWriter` mutation records `key.hash64()` into `write_set` **and** `key.clone()` into the entry's `BTreeSet<K>`.

- [ ] **Step 5: Update the commit merge path to pass exact keys**

At the merge slow path (around `src/store.rs:3727`), replace the `BTreeSet<u64>` argument to `merge_keys_from` with the entry's erased `BTreeSet<K>`:

```rust
            latest_clone.merge_keys_from(dirty.table.as_ref(), dirty.modified_keys.as_ref())?;
```

Conflict detection above it keeps using `write_set`'s digests — unchanged.

Then remove the `#[ignore]` from Task 5's `string_keyed_table_survives_wal_recovery` if one was added, and confirm it passes.

- [ ] **Step 6: Run the tests to verify they pass**

Run: `cargo test --test store_integration` and `cargo test --features persistence --test persistence_integration`
Expected: PASS.

- [ ] **Step 7: Verify the full gate**

```bash
cargo test
cargo test --features persistence
cargo test -p ultima-vector
cargo check --lib --no-default-features
cargo clippy --all-targets --features persistence -- -D warnings
```

- [ ] **Step 8: Commit**

```bash
git add src/store.rs tests/store_integration.rs tests/persistence_integration.rs
git commit -m "feat(store): open_table_keyed + hashed write-set

open_table/register_table keep their exact u64 signatures (Rust has no
default type params on functions, so widening them would break every
turbofish call site); open_table_keyed is the new entry point. Conflict
detection uses 64-bit key digests — collisions cause spurious conflicts,
never missed ones — while the commit merge uses the exact BTreeSet<K> now
carried, type-erased, on each DirtyEntry."
```

---

### Task 7: Bulk load and snapshot streaming over encoded keys

**Files:**
- Modify: `src/bulk_load.rs`, `src/snapshot_stream/build.rs:157`, `src/snapshot_stream/install.rs:259`
- Test: `tests/bulk_load.rs`, `tests/snapshot_stream.rs`

**Interfaces:**
- Consumes: `collect_serialized_rows -> Vec<(Vec<u8>, Vec<u8>)>` and `BuildFromRawRowsFn` over `Vec<(Vec<u8>, Vec<u8>)>` (Tasks 3–4).
- Produces: bulk load and snapshot install working for any `K`, with rows still required to arrive in ascending encoded-key order.

**The invariant to preserve:** `BTree::from_sorted` requires ascending key order. Encoded order equals `K`'s `Ord` order by Task 1's encoding contract, so sorting by encoded bytes is equivalent to sorting by key — that is what makes the type-erased path correct.

- [ ] **Step 1: Write the failing test**

Add to `tests/bulk_load.rs`:

```rust
#[test]
fn bulk_load_string_keyed_table() {
    let store = Store::default();

    let rows = vec![
        ("a@x.com".to_string(), "Alice".to_string()),
        ("b@x.com".to_string(), "Bob".to_string()),
        ("c@x.com".to_string(), "Carol".to_string()),
    ];

    store
        .bulk_load_keyed::<String, String>("emails", rows, None)
        .unwrap();

    let rtx = store.begin_read(None).unwrap();
    let t = rtx.open_table_keyed::<String, String>("emails").unwrap();
    assert_eq!(t.len(), 3);
    assert_eq!(t.get(&"b@x.com".to_string()), Some(&"Bob".to_string()));
}

#[test]
fn bulk_load_rejects_unsorted_keys() {
    let store = Store::default();
    let rows = vec![
        ("b@x.com".to_string(), "Bob".to_string()),
        ("a@x.com".to_string(), "Alice".to_string()),
    ];
    let err = store
        .bulk_load_keyed::<String, String>("emails", rows, None)
        .unwrap_err();
    assert!(
        matches!(err, Error::InvalidBulkLoadInput(_)),
        "expected InvalidBulkLoadInput, got {err:?}"
    );
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test --test bulk_load bulk_load_string_keyed_table`
Expected: FAIL — `bulk_load_keyed` does not exist.

- [ ] **Step 3: Add the keyed bulk-load entry point**

Mirror the `open_table` pattern: keep `bulk_load` and `bulk_load_batch` at their current `u64` signatures, and add `bulk_load_keyed<R: Record, K: PrimaryKey>`. Reject unsorted input with `Error::InvalidBulkLoadInput("bulk load rows must be in ascending primary-key order")` — check with a single pass comparing adjacent keys, before building the tree.

- [ ] **Step 4: Replace the Task 3 placeholder decodes in snapshot streaming**

Find the `// widened in task 7` comments left by Task 3 in `src/snapshot_stream/build.rs` and `src/snapshot_stream/install.rs` and remove the `u64::decode(...)` adaptations — the rows are now `(Vec<u8>, Vec<u8>)` end to end and the registry's `build_table_from_raw` closure decodes them with the correct `K`. Confirm no `// widened in task` comments remain anywhere:

```bash
grep -rn "widened in task" src/ || echo "(none remaining)"
```

- [ ] **Step 5: Run the tests to verify they pass**

Run: `cargo test --test bulk_load` and `cargo test --features persistence --test snapshot_stream`
Expected: PASS.

- [ ] **Step 6: Verify the full gate**

```bash
cargo test
cargo test --features persistence
cargo test -p ultima-vector
cargo clippy --all-targets --features persistence -- -D warnings
```

- [ ] **Step 7: Commit**

```bash
git add src/bulk_load.rs src/snapshot_stream tests/bulk_load.rs tests/snapshot_stream.rs
git commit -m "feat(bulk): bulk_load_keyed and snapshot streaming over encoded keys

Rows flow as (encoded-key-bytes, record-bytes) end to end. Encoded order
equals K's Ord order by the PrimaryKey contract, so the type-erased sorted
path stays correct. Unsorted input is rejected up front."
```

---

### Task 8: Public surface, docs, and the 0.3.0 release notes

**Files:**
- Create: `docs/tasks/task56_arbitrary_primary_keys.md`
- Create: `examples/string_keyed_table.rs`
- Modify: `README.md`, `CHANGELOG.md`, `CLAUDE.md`, `docs/BACKLOG.md`, `Cargo.toml` (version), `ultima_vector/Cargo.toml` (version + dep)

**Interfaces:**
- Consumes: the finished API from Tasks 1–7.
- Produces: no code changes to the engine; documentation and version metadata only.

- [ ] **Step 1: Write the example**

Create `examples/string_keyed_table.rs` showing a `String`-keyed table end to end: open, `put`, `get`, iterate in key order, and a secondary index over a non-`u64` primary key. It must run to completion, not merely compile — CI builds `examples/`.

- [ ] **Step 2: Verify the example runs**

Run: `cargo run --example string_keyed_table`
Expected: exits 0 with the printed output you expect.

- [ ] **Step 3: Write the feature doc**

Create `docs/tasks/task56_arbitrary_primary_keys.md` following the structure of `docs/tasks/task54_multi_table_writer.md` (context, design, implementation, testing, design history). It must record the four decisions from this plan's "Design decisions fixed before implementation" section — especially why `open_table` could not be widened (no default type params on functions; `E0107` on partial turbofish) and why `dyn MergeableTable` could not take `K`. Link the spec at `docs/superpowers/specs/2026-07-30-adoption-program-design.md`.

- [ ] **Step 4: Update the README and CLAUDE.md**

In `README.md`, add arbitrary primary keys to the Highlights list and show the `open_table_keyed` form in the quick example. In `CLAUDE.md`, update the architecture bullets for `Table<R>` → `Table<R, K = u64>` and the `BTree<u64, R>` reference in the data-structure stack.

- [ ] **Step 5: Bump versions and write the CHANGELOG**

Set `version = "0.3.0"` in `Cargo.toml` and `ultima_vector/Cargo.toml`, and the `ultima-db` dependency requirement in `ultima_vector/Cargo.toml` to `version = "0.3.0"` (for `0.x` crates `^0.2.0` excludes `0.3.0`, so the vector crate must be republished even with no source changes — the same situation as the 0.2.0 release).

Add a `## 0.3.0` CHANGELOG section whose **Breaking** list states plainly that the WAL and checkpoint formats changed, that recovery rejects files written by 0.2.x, and that the migration is: checkpoint on 0.2.x, then re-open with 0.3.0 — or rebuild from source data.

- [ ] **Step 6: Mark the backlog item**

`docs/BACKLOG.md` notes under "apply-path performance" that a dense-integer-key fast path "gets easier after `Table<R, K = u64>` lands". Update that line to say it has landed and what the specialization hook now is.

- [ ] **Step 7: Verify the full gate one last time**

```bash
cargo test
cargo test --features persistence
cargo test -p ultima-vector
cargo check --lib --no-default-features
cargo clippy --all-targets --features persistence -- -D warnings
cargo package --list --allow-dirty | wc -l
cargo publish --dry-run --allow-dirty
```

Expected: all pass; the packaged file list should now include `examples/string_keyed_table.rs`.

- [ ] **Step 8: Commit**

```bash
git add -A
git commit -m "docs+release: arbitrary primary keys (task56), 0.3.0

Feature doc, string-keyed example, README/CLAUDE.md updates, and the 0.3.0
version bump with a CHANGELOG recording the WAL/checkpoint format break and
its migration path."
```

---

## Deferred, deliberately

Not in this plan; record them rather than expanding scope:

- **`open_tables2`/`open_tables3` keyed variants.** They stay `u64`-only. Add them when a caller actually needs a multi-table open with a non-`u64` key.
- **A dense-integer-key fast path.** Now unblocked (a generic `K` gives something to specialize on), but it is a performance change belonging to the backlog's apply-path direction, not to this API change.
- **Migration tooling.** The documented path is "checkpoint on 0.2.x, re-open on 0.3.0". A converter binary is not justified at current adoption.
- **The featureless-config test gap.** `cargo test` cannot exercise the no-`persistence` build because a dev-dependency unifies the feature on. Pre-existing; `cargo check --lib --no-default-features` is the stopgap this plan uses.
