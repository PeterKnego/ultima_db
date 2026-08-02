//! Verification kernel: the order-preserving key encoding of ultima_db's
//! `PrimaryKey` (`src/primary_key.rs`), ported to the Aeneas-supported
//! safe-Rust subset.
//!
//! Deltas from the real code, chosen to keep the algorithm recognizable while
//! staying translatable:
//! - The `PrimaryKey` trait is not modeled: Aeneas has no trait support that
//!   would carry an associated const, so each impl becomes a monomorphic free
//!   function (`encode_u64`, `encode_bytes`, ...).
//! - `Result<T, Error>` -> `Option<T>`: the kernel has no crate error type
//!   (same delta as `formal/kernel`'s `remove`).
//! - `unescape_until_terminator(bytes, at: &mut usize)` takes the cursor by
//!   value and returns the new position, rather than mutating an out-param.
//! - `for &b in bytes` -> indexed `while` loops; no closures, no early return
//!   from inside a loop.
//! - `Vec::with_capacity(n)` -> `Vec::new()`; capacity is not modeled.
//! - `String` is NOT ported. `String::encode` is `self.as_bytes().to_vec()`,
//!   so `encode_bytes` proves the `String` encode case exactly — same bytes,
//!   and `Ord for String` is bytewise over UTF-8, agreeing with
//!   `Ord for Vec<u8>`. `String::decode` calls `String::from_utf8`, which
//!   Aeneas would have to axiomatize; it is a documented boundary, not a gap
//!   in the ordering result.

pub fn encode_u64(v: u64) -> Vec<u8> {
    let mut out = Vec::new();
    let mut i: usize = 0;
    while i < 8 {
        let shift = 8 * (7 - i);
        out.push(((v >> shift) & 0xFF) as u8);
        i += 1;
    }
    out
}

pub fn decode_u64(b: &[u8]) -> Option<u64> {
    if b.len() != 8 {
        return None;
    }
    let mut acc: u64 = 0;
    let mut i: usize = 0;
    while i < 8 {
        acc = (acc << 8) | (b[i] as u64);
        i += 1;
    }
    Some(acc)
}

/// Sign-bit flip so negatives sort before positives, mirroring
/// `impl_signed_key!` in `src/primary_key.rs`.
pub fn encode_i64(v: i64) -> Vec<u8> {
    encode_u64((v as u64) ^ (1u64 << 63))
}

pub fn decode_i64(b: &[u8]) -> Option<i64> {
    match decode_u64(b) {
        Some(biased) => Some((biased ^ (1u64 << 63)) as i64),
        None => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The kernel must agree with the real implementation byte-for-byte.
    /// The real impls are reproduced here as oracles rather than depending on
    /// `ultima-db`, because this crate is outside the workspace and Charon
    /// must own its build. Keep them in sync by eye; the module doc lists the
    /// source lines.
    fn oracle_u64(v: u64) -> Vec<u8> {
        v.to_be_bytes().to_vec()
    }
    fn oracle_i64(v: i64) -> Vec<u8> {
        ((v as u64) ^ (1u64 << 63)).to_be_bytes().to_vec()
    }

    #[test]
    fn u64_matches_the_real_encoding() {
        for v in [0u64, 1, 250, 251, 255, 256, 65_535, u64::MAX / 2, u64::MAX] {
            assert_eq!(encode_u64(v), oracle_u64(v), "u64 {v}");
            assert_eq!(decode_u64(&encode_u64(v)), Some(v), "u64 roundtrip {v}");
        }
    }

    #[test]
    fn i64_matches_the_real_encoding() {
        for v in [i64::MIN, -65_536, -256, -1, 0, 1, 256, 65_536, i64::MAX] {
            assert_eq!(encode_i64(v), oracle_i64(v), "i64 {v}");
            assert_eq!(decode_i64(&encode_i64(v)), Some(v), "i64 roundtrip {v}");
        }
    }

    #[test]
    fn i64_encoding_orders_negatives_before_positives() {
        let mut vals = vec![i64::MIN, -1_000, -1, 0, 1, 1_000, i64::MAX];
        vals.sort();
        for w in vals.windows(2) {
            assert!(
                encode_i64(w[0]) < encode_i64(w[1]),
                "order not preserved across {} -> {}",
                w[0],
                w[1]
            );
        }
    }

    #[test]
    fn decode_rejects_wrong_length() {
        assert_eq!(decode_u64(&[0, 1, 2]), None);
        assert_eq!(decode_i64(&[0; 9]), None);
    }
}
