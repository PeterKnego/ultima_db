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
    /// `Some(n)` if this type always encodes to exactly `n` bytes, `None` if
    /// its encoded length varies. Tuple encoding uses this to decide whether
    /// a non-final element needs framing: fixed-width elements are
    /// self-delimiting, variable-length ones are not.
    const ENCODED_LEN: Option<usize>;

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
    ///
    /// **Override this if the type can be hashed without allocating.** The
    /// default hashes `encode()`, which allocates a `Vec<u8>` per call, and
    /// this sits on the point-read and per-mutation hot paths. Every key type
    /// this crate implements overrides it. The only requirements are
    /// determinism within a process and that equal keys agree — digests are
    /// never persisted and never compared across key types (a table has one
    /// `K`), so they need not match `encode()`'s bytes or anything else.
    fn hash64(&self) -> u64 {
        let mut h = std::collections::hash_map::DefaultHasher::new();
        self.encode().hash(&mut h);
        h.finish()
    }

    /// Advance an auto-increment counter past `self`, if this key type has
    /// one. Default: no-op — only [`AutoKey`] types maintain a counter.
    ///
    /// [`Table::put`](crate::table::Table::put) calls this after writing an
    /// explicit key so that a later
    /// [`Table::insert`](crate::table::Table::insert) on the same table
    /// cannot hand out an id that is already occupied.
    fn advance_auto_counter(&self, _counter: &mut Option<Self>)
    where
        Self: Sized,
    {
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

/// The auto-increment counter a fresh table of key type `K` starts with:
/// `Some(1)` for `u64` — the one [`AutoKey`] — and `None` for every
/// explicitly-keyed type.
///
/// A `PrimaryKey`-only bound cannot spell `AutoKey::first()`, and Rust has no
/// specialization, so recover it by type identity instead: `K: 'static`, so
/// this is a plain downcast of a `u64` to `K`, which succeeds exactly when
/// `K == u64`. Keeping the seed here (rather than pushing an `AutoKey` bound
/// out to every caller) is what lets the type-erased registry closures and
/// `WriteTx::open_table_keyed` build an empty table for any key type while
/// preserving the `next_id_opt() == None` ⟺ explicitly-keyed invariant.
pub(crate) fn auto_counter_seed<K: PrimaryKey>() -> Option<K> {
    let first = <u64 as AutoKey>::first();
    (&first as &dyn std::any::Any).downcast_ref::<K>().cloned()
}

/// Hash a value directly, with no intermediate encoding.
///
/// The [`PrimaryKey::hash64`] default hashes `encode()`, which allocates a
/// `Vec<u8>` per call. Every key type with a cheap, allocation-free `Hash`
/// overrides it with this. Digests are process-local — they are only ever
/// compared against other digests from the same run (write sets, read sets,
/// the intent table) and never persisted — so the choice of hash is free, as
/// long as it is deterministic within the process and equal keys agree, which
/// `Hash`/`Eq` guarantees.
fn hash_of<T: Hash + ?Sized>(value: &T) -> u64 {
    let mut h = std::collections::hash_map::DefaultHasher::new();
    value.hash(&mut h);
    h.finish()
}

fn truncated(expected: usize, got: usize) -> Error {
    Error::InvalidBulkLoadInput(format!(
        "primary key decode: expected {expected} bytes, got {got}"
    ))
}

fn malformed_escape() -> Error {
    Error::InvalidBulkLoadInput("primary key decode: malformed escape sequence".to_string())
}

/// Upper bound on one [`PrimaryKey::encode`] output, enforced by every wire
/// format that carries a key as a length-prefixed byte string.
///
/// Both trust boundaries read a length off untrusted bytes before allocating,
/// so both need a cap, and they need the *same* cap: a key the WAL accepts but
/// the snapshot stream truncates (or vice versa) is a row that survives one
/// durability path and is corrupted by the other. Defining it once is what
/// makes them agree by construction rather than by coincidence.
///
/// 64 KiB is orders of magnitude above any sane key (a `u64` encodes to 8
/// bytes; the largest plausible tuple-of-strings key is a few hundred) while
/// keeping a corrupt length from driving a large allocation.
#[cfg_attr(not(feature = "persistence"), allow(dead_code))]
pub(crate) const MAX_ENCODED_KEY_LEN: usize = 64 * 1024;

/// The `ENCODED_LEN`/`encode`/`decode` trio shared by every unsigned integer
/// key. Split out of `impl_unsigned_key!` so `u64` — the one `AutoKey` type —
/// can spell its own impl block and add `advance_auto_counter` to it.
macro_rules! unsigned_key_body {
    ($t:ty) => {
        const ENCODED_LEN: Option<usize> = Some(std::mem::size_of::<$t>());
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
        fn hash64(&self) -> u64 {
            hash_of(self)
        }
    };
}

macro_rules! impl_unsigned_key {
    ($($t:ty),*) => {$(
        impl PrimaryKey for $t {
            unsigned_key_body!($t);
        }
    )*};
}

macro_rules! impl_signed_key {
    ($($t:ty => $u:ty),*) => {$(
        impl PrimaryKey for $t {
            const ENCODED_LEN: Option<usize> = Some(std::mem::size_of::<$t>());
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
            fn hash64(&self) -> u64 {
                hash_of(self)
            }
        }
    )*};
}

impl_unsigned_key!(u8, u16, u32, u128);

impl PrimaryKey for u64 {
    unsigned_key_body!(u64);

    /// `u64` is the only [`AutoKey`], so it is the only key type that carries
    /// a counter to advance. Writing an explicit key at or past the counter
    /// pushes it one beyond, which is what keeps `put`-then-`insert` from
    /// colliding. A key of `u64::MAX` clears the counter (there is no next
    /// id); the `AutoKey` methods panic on a cleared counter rather than
    /// reissue an occupied id.
    fn advance_auto_counter(&self, counter: &mut Option<Self>) {
        if counter.as_ref().is_none_or(|n| self >= n) {
            *counter = self.checked_add(1);
        }
    }
}
impl_signed_key!(i8 => u8, i16 => u16, i32 => u32, i64 => u64, i128 => u128);

impl PrimaryKey for String {
    const ENCODED_LEN: Option<usize> = None;
    fn encode(&self) -> Vec<u8> {
        self.as_bytes().to_vec()
    }
    fn decode(bytes: &[u8]) -> Result<Self> {
        String::from_utf8(bytes.to_vec())
            .map_err(|e| Error::InvalidBulkLoadInput(format!("primary key not UTF-8: {e}")))
    }
    fn hash64(&self) -> u64 {
        hash_of(self.as_bytes())
    }
}

impl PrimaryKey for Vec<u8> {
    const ENCODED_LEN: Option<usize> = None;
    fn encode(&self) -> Vec<u8> {
        self.clone()
    }
    fn decode(bytes: &[u8]) -> Result<Self> {
        Ok(bytes.to_vec())
    }
    fn hash64(&self) -> u64 {
        hash_of(self.as_slice())
    }
}

/// Escape-and-terminate framing for variable-length non-final elements.
/// Every 0x00 byte in the encoding is escaped as [0x00, 0xFF], then
/// a terminator [0x00, 0x01] is appended. This ensures order-preservation:
/// the terminator's first byte (0x00) sorts before any literal byte (>= 0x01),
/// and against an escaped zero the second byte decides (0x01 < 0xFF).
fn escape_and_terminate(bytes: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(bytes.len() + 2);
    for &b in bytes {
        if b == 0x00 {
            out.push(0x00);
            out.push(0xFF);
        } else {
            out.push(b);
        }
    }
    out.push(0x00);
    out.push(0x01);
    out
}

/// Unescape-and-scan to extract a variable-length element terminated by
/// [0x00, 0x01]. Returns the unescaped data and the position after the
/// terminator, or an error if truncated or malformed.
fn unescape_until_terminator(bytes: &[u8], at: &mut usize) -> Result<Vec<u8>> {
    let mut out = Vec::new();
    while *at < bytes.len() {
        let b = bytes[*at];
        if b == 0x00 {
            *at += 1;
            if *at >= bytes.len() {
                return Err(malformed_escape());
            }
            let next = bytes[*at];
            *at += 1;
            if next == 0x01 {
                return Ok(out);
            } else if next == 0xFF {
                out.push(0x00);
            } else {
                return Err(malformed_escape());
            }
        } else {
            out.push(b);
            *at += 1;
        }
    }
    Err(malformed_escape())
}

impl<A: PrimaryKey, B: PrimaryKey> PrimaryKey for (A, B) {
    const ENCODED_LEN: Option<usize> = None;
    fn encode(&self) -> Vec<u8> {
        let mut out = Vec::new();
        let a_enc = self.0.encode();
        if let Some(_len) = A::ENCODED_LEN {
            // Fixed-width: raw bytes, no framing.
            out.extend_from_slice(&a_enc);
        } else {
            // Variable-length: escape and terminate.
            out.extend_from_slice(&escape_and_terminate(&a_enc));
        }
        out.extend_from_slice(&self.1.encode());
        out
    }
    fn decode(bytes: &[u8]) -> Result<Self> {
        let mut at = 0;
        let a = if let Some(len) = A::ENCODED_LEN {
            // Fixed-width: consume exactly `len` bytes.
            if bytes.len() < at + len {
                return Err(truncated(at + len, bytes.len()));
            }
            let a = A::decode(&bytes[at..at + len])?;
            at += len;
            a
        } else {
            // Variable-length: scan for terminator.
            let a_enc = unescape_until_terminator(bytes, &mut at)?;
            A::decode(&a_enc)?
        };
        let b = B::decode(&bytes[at..])?;
        Ok((a, b))
    }
    /// Compose the elements' digests rather than hashing the composite
    /// encoding: encoding a tuple allocates once per element plus the output
    /// buffer, and every element type that can hash without allocating
    /// already does.
    fn hash64(&self) -> u64 {
        hash_of(&(self.0.hash64(), self.1.hash64()))
    }
}

impl<A: PrimaryKey, B: PrimaryKey, C: PrimaryKey> PrimaryKey for (A, B, C) {
    const ENCODED_LEN: Option<usize> = None;
    fn encode(&self) -> Vec<u8> {
        let mut out = Vec::new();
        let a_enc = self.0.encode();
        if let Some(_len) = A::ENCODED_LEN {
            out.extend_from_slice(&a_enc);
        } else {
            out.extend_from_slice(&escape_and_terminate(&a_enc));
        }
        let b_enc = self.1.encode();
        if let Some(_len) = B::ENCODED_LEN {
            out.extend_from_slice(&b_enc);
        } else {
            out.extend_from_slice(&escape_and_terminate(&b_enc));
        }
        out.extend_from_slice(&self.2.encode());
        out
    }
    fn decode(bytes: &[u8]) -> Result<Self> {
        let mut at = 0;
        let a = if let Some(len) = A::ENCODED_LEN {
            if bytes.len() < at + len {
                return Err(truncated(at + len, bytes.len()));
            }
            let a = A::decode(&bytes[at..at + len])?;
            at += len;
            a
        } else {
            let a_enc = unescape_until_terminator(bytes, &mut at)?;
            A::decode(&a_enc)?
        };
        let b = if let Some(len) = B::ENCODED_LEN {
            if bytes.len() < at + len {
                return Err(truncated(at + len, bytes.len()));
            }
            let b = B::decode(&bytes[at..at + len])?;
            at += len;
            b
        } else {
            let b_enc = unescape_until_terminator(bytes, &mut at)?;
            B::decode(&b_enc)?
        };
        let c = C::decode(&bytes[at..])?;
        Ok((a, b, c))
    }
    /// See the two-element impl.
    fn hash64(&self) -> u64 {
        hash_of(&(self.0.hash64(), self.1.hash64(), self.2.hash64()))
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
    fn tuples_with_variable_length_leading_elements_encode_in_order() {
        // The case the length-prefix scheme got wrong: a longer leading
        // element that sorts EARLIER than a shorter one.
        assert_order_preserving(vec![
            ("aa".to_string(), 0u8),
            ("b".to_string(), 0u8),
        ]);
        assert_order_preserving(vec![
            (String::new(), 1u8),
            ("a".to_string(), 0u8),
            ("aa".to_string(), 0u8),
            ("b".to_string(), 0u8),
        ]);
        // Embedded NULs must not be confused with the terminator.
        assert_order_preserving(vec![
            (vec![0u8], 0u8),
            (vec![0u8, 0u8], 0u8),
            (vec![0u8, 1u8], 0u8),
            (vec![1u8], 0u8),
        ]);
        // Nested tuples are variable-length too.
        assert_order_preserving(vec![
            (("aa".to_string(), 0u8), 0u8),
            (("b".to_string(), 0u8), 0u8),
        ]);
        // 3-tuple with two variable-length leading elements.
        assert_order_preserving(vec![
            ("a".to_string(), "x".to_string(), 0u8),
            ("a".to_string(), "y".to_string(), 0u8),
            ("b".to_string(), "a".to_string(), 0u8),
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
    fn roundtrip_decode_with_embedded_nuls_and_escapes() {
        let t = (vec![0u8, 0xFF, 0u8], "x".to_string());
        assert_eq!(<(Vec<u8>, String)>::decode(&t.encode()).unwrap(), t);
        let n = (("a\u{0}b".to_string(), 7u16), 9u8);
        assert_eq!(<((String, u16), u8)>::decode(&n.encode()).unwrap(), n);
    }

    #[test]
    fn decode_rejects_truncated_input() {
        assert!(u64::decode(&[0, 1, 2]).is_err());
    }

    #[test]
    fn decode_rejects_truncated_escape() {
        // A tuple with a String: truncate mid-escape.
        let (s, _): (String, u8) = ("a".to_string(), 5);
        let mut enc = s.encode();
        enc.push(0x00); // Start escape but truncate.
        assert!(<(String, u8)>::decode(&enc).is_err());
    }

    #[test]
    fn hash64_is_stable_and_differs_across_values() {
        assert_eq!(1u64.hash64(), 1u64.hash64());
        assert_ne!(1u64.hash64(), 2u64.hash64());
    }

    #[test]
    fn advance_auto_counter_moves_only_for_u64() {
        // `u64` is the one `AutoKey`: an explicitly written key installs the
        // counter and pushes it one past the key.
        let mut c: Option<u64> = None;
        7u64.advance_auto_counter(&mut c);
        assert_eq!(c, Some(8));
        // A key below the counter leaves it alone.
        3u64.advance_auto_counter(&mut c);
        assert_eq!(c, Some(8));
        // A key *at* the counter moves it past — this is the collision the
        // guard exists for.
        8u64.advance_auto_counter(&mut c);
        assert_eq!(c, Some(9));
        // No next id at the top of the range: the counter is cleared, and the
        // `AutoKey` methods panic rather than reissue an occupied id.
        u64::MAX.advance_auto_counter(&mut c);
        assert_eq!(c, None);

        // No other key type maintains a counter — the default is a no-op, so
        // an explicitly-keyed table never grows one.
        let mut c32: Option<u32> = None;
        7u32.advance_auto_counter(&mut c32);
        assert_eq!(c32, None);
        let mut cs: Option<String> = None;
        "k".to_string().advance_auto_counter(&mut cs);
        assert_eq!(cs, None);
    }

    #[test]
    fn auto_key_sequences_from_one() {
        assert_eq!(<u64 as AutoKey>::first(), 1u64);
        assert_eq!(5u64.next(), Some(6u64));
        assert_eq!(u64::MAX.next(), None);
    }
}
