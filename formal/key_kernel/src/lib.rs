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

pub const ESCAPE: u8 = 0xFF;
pub const TERMINATOR: u8 = 0x01;

/// Escape every 0x00 as [0x00, ESCAPE], then append [0x00, TERMINATOR].
pub fn escape_and_terminate(bytes: &[u8]) -> Vec<u8> {
    let mut out = Vec::new();
    let mut i: usize = 0;
    while i < bytes.len() {
        let b = bytes[i];
        if b == 0x00 {
            out.push(0x00);
            out.push(ESCAPE);
        } else {
            out.push(b);
        }
        i += 1;
    }
    out.push(0x00);
    out.push(TERMINATOR);
    out
}

/// Scan from `at` to the first unescaped [0x00, TERMINATOR], unescaping as we
/// go. Returns the element and the position just past the terminator.
pub fn unescape_until_terminator(bytes: &[u8], at: usize) -> Option<(Vec<u8>, usize)> {
    let mut out = Vec::new();
    let mut i: usize = at;
    let mut done = false;
    let mut bad = false;
    while i < bytes.len() && !done && !bad {
        let b = bytes[i];
        if b == 0x00 {
            if i + 1 >= bytes.len() {
                bad = true;
            } else {
                let next = bytes[i + 1];
                if next == TERMINATOR {
                    i += 2;
                    done = true;
                } else if next == ESCAPE {
                    out.push(0x00);
                    i += 2;
                } else {
                    bad = true;
                }
            }
        } else {
            out.push(b);
            i += 1;
        }
    }
    if done { Some((out, i)) } else { None }
}

pub fn encode_bytes(b: &[u8]) -> Vec<u8> {
    b.to_vec()
}

pub fn decode_bytes(b: &[u8]) -> Option<Vec<u8>> {
    Some(b.to_vec())
}

/// A variable-length leading element followed by a fixed-width one — the
/// tuple shape the original length-prefixed design encoded out of order.
pub fn encode_pair_bytes_u64(a: &[u8], b: u64) -> Vec<u8> {
    let mut out = escape_and_terminate(a);
    let tail = encode_u64(b);
    let mut i: usize = 0;
    while i < tail.len() {
        out.push(tail[i]);
        i += 1;
    }
    out
}

pub fn decode_pair_bytes_u64(bytes: &[u8]) -> Option<(Vec<u8>, u64)> {
    match unescape_until_terminator(bytes, 0) {
        None => None,
        Some((head, at)) => {
            let mut tail = Vec::new();
            let mut i: usize = at;
            while i < bytes.len() {
                tail.push(bytes[i]);
                i += 1;
            }
            match decode_u64(&tail) {
                None => None,
                Some(v) => Some((head, v)),
            }
        }
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

    fn oracle_escape(bytes: &[u8]) -> Vec<u8> {
        let mut out = Vec::new();
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

    #[test]
    fn escaping_matches_the_real_framing() {
        for case in [
            vec![],
            vec![0x00],
            vec![0x00, 0x00],
            vec![0x01],
            vec![0xFF],
            vec![0x00, 0xFF, 0x00],
            b"alice@example.com".to_vec(),
        ] {
            assert_eq!(escape_and_terminate(&case), oracle_escape(&case), "{case:?}");
        }
    }

    #[test]
    fn unescape_inverts_escape() {
        for case in [
            vec![],
            vec![0x00],
            vec![0x00, 0x01, 0x00, 0xFF],
            b"bob".to_vec(),
        ] {
            let framed = escape_and_terminate(&case);
            let got = unescape_until_terminator(&framed, 0);
            assert_eq!(got, Some((case.clone(), framed.len())), "{case:?}");
        }
    }

    /// The bug the original length-prefix design had: a longer leading element
    /// that sorts EARLIER must still encode earlier.
    #[test]
    fn pair_encoding_preserves_order_for_variable_length_leads() {
        let mut cases = vec![
            (b"aa".to_vec(), 0u64),
            (b"b".to_vec(), 0u64),
            (b"".to_vec(), 1u64),
            (b"a".to_vec(), 0u64),
            (vec![0x00], 0u64),
            (vec![0x00, 0x00], 0u64),
            (vec![0x00, 0x01], 0u64),
            (vec![0x01], 0u64),
        ];
        cases.sort();
        for w in cases.windows(2) {
            let (lo, hi) = (&w[0], &w[1]);
            assert!(
                encode_pair_bytes_u64(&lo.0, lo.1) < encode_pair_bytes_u64(&hi.0, hi.1),
                "order not preserved: {lo:?} -> {hi:?}"
            );
        }
    }

    #[test]
    fn pair_roundtrips_including_embedded_nuls() {
        for case in [
            (vec![], 0u64),
            (vec![0x00, 0xFF, 0x00], 7u64),
            (b"alice".to_vec(), u64::MAX),
        ] {
            let enc = encode_pair_bytes_u64(&case.0, case.1);
            assert_eq!(decode_pair_bytes_u64(&enc), Some(case.clone()), "{case:?}");
        }
    }

    #[test]
    fn unescape_rejects_malformed_input() {
        // No terminator at all.
        assert_eq!(unescape_until_terminator(&[0x01, 0x02], 0), None);
        // Truncated mid-escape: a trailing lone 0x00.
        assert_eq!(unescape_until_terminator(&[0x01, 0x00], 0), None);
        // 0x00 followed by neither 0x01 nor 0xFF.
        assert_eq!(unescape_until_terminator(&[0x00, 0x07], 0), None);
    }
}
