# F-DB-1: Formal Verification of the `PrimaryKey` Order-Preserving Encoding

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Machine-check, in Lean 4 over an Aeneas translation of the real Rust, that `PrimaryKey::encode` is strictly order-preserving and that `decode ∘ encode = id`.

**Architecture:** Mirror the existing `formal/` B-tree arc exactly — a second Aeneas kernel crate (`formal/key_kernel/`) holding a translatable, monomorphised port of `src/primary_key.rs`, anchored to the real implementation by a differential test, mechanically translated to Lean by Charon + Aeneas, with the proofs living beside the B-tree proofs in `formal/proofs/`.

**Tech Stack:** Rust (Aeneas-supported safe subset), Charon `nightly-2026.07.02`, Aeneas `nightly-2026.07.06-45061fa`, Lean 4 `v4.30.0-rc2` + Mathlib.

## Why this is worth proving

`encode`'s bytewise order must equal `Ord` order. Three consumers corrupt silently if it does not: WAL replay ordering, `BTree::from_sorted` (assumes input sorted by `Ord` == encoded order), and secondary-index range scans via `BTree::range_prefix` over `(IK, K)` composite keys.

This is not hypothetical. The original design of this encoding used **length prefixes** and was not order-preserving — `("aa", 0) < ("b", 0)` in value order encoded the other way round, because the length byte was compared before any content. It was caught in review, and replaced with escape-and-terminate. Property tests over ~3000 keys now sample the space; a proof covers it. Escape-sequence comparators are a classic off-by-one-byte bug class, and the tuple/escaping interaction — does an escaped `0x00` inside a leading element compare correctly against a terminator in a shorter key? — is exactly the corner sampling misses.

## Global Constraints

- **Toolchain pins are exact** (`formal/README.md`): Aeneas `nightly-2026.07.06-45061fa`, Charon `nightly-2026.07.02` (= the commit in that Aeneas's `charon-pin`), Rust `nightly-2026-06-01`, Lean `leanprover/lean4:v4.30.0-rc2`. Charon and Aeneas versions must match exactly — LLBC is a versioned format and same-day nightlies mismatch. All are already fetched under `formal/.toolchain/`; verified working on 2026-08-02 (regenerating `BtreeKernel.lean` reproduces it byte-for-byte).
- **Generated Lean is never hand-edited.** `formal/proofs/KeyKernel.lean` is Aeneas output, exactly like `BtreeKernel.lean`.
- **Zero `axiom`s in the generated Lean.** An axiom means an unmodeled std function leaked in. Check with `grep -c '^axiom'` after every regeneration; it must print `0`.
- **No `sorry` may be committed.** Verify with `#print axioms` on each top-level theorem — only `propext, Classical.choice, Quot.sound` are acceptable.
- **The kernel is a port, not a rewrite.** Every deviation from `src/primary_key.rs` goes in the module-level "Deltas from the real code" doc comment, following `formal/kernel/src/lib.rs`'s pattern. A reader must be able to check the port by eye.
- Repo-wide: MSRV 1.88 for the main crate; do NOT run `cargo fmt`; never `perl -pi` on non-ASCII.
- The main crate's gate is `.github/workflows/ci.yml` — `make lint`, `cargo clippy -p ultima-vector -- -D warnings`, `cargo test --features persistence,fulltext,metrics`, `cargo test --lib`, `cargo test -p ultima-vector --features persistence`. Run all five before pushing.

## Aeneas-supported subset — the constraints that shape the port

Learned from `formal/kernel/src/lib.rs`'s delta list. Violating these produces either a translation failure or a leaked axiom:

- **No generics, no traits.** `PrimaryKey` is a trait with an associated const and ~15 impls; the kernel exports monomorphic free functions instead (`encode_u64`, `encode_i64`, …). The trait itself is not modeled.
- **No closures and no early `return` from inside a loop.** `for &b in bytes` becomes an indexed `while` loop; `?` inside a loop becomes an explicit accumulator plus a post-loop check.
- **No `&mut` out-parameters where a return will do.** The real `unescape_until_terminator(bytes: &[u8], at: &mut usize)` becomes `unescape_until_terminator(bytes: &[u8], at: usize) -> Option<(Vec<u8>, usize)>`.
- **No crate `Error` type.** The kernel returns `Option`, mirroring how `formal/kernel`'s `remove` collapses `Err(KeyNotFound)` to `None`.
- **`Vec::with_capacity` → `Vec::new()`.** Capacity is not modeled and buys nothing here.
- **`String` is not modeled.** This is load-bearing and must be documented: `String::encode` is literally `self.as_bytes().to_vec()`, so **proving the `Vec<u8>` case proves the `String` encode case** — same bytes, same order, and Rust's `Ord for String` is bytewise on UTF-8, which agrees with `Ord for Vec<u8>`. The **decode** side differs: `String::decode` calls `String::from_utf8`, a std function Aeneas will not model. Do not attempt to port it; that is precisely where an axiom would leak in. Record it as a documented boundary in the README table.

## File Structure

| File | Responsibility | Task |
|---|---|---|
| `formal/key_kernel/Cargo.toml` (new) | Standalone crate, `[workspace]` empty so the parent workspace excludes it | 1 |
| `formal/key_kernel/src/lib.rs` (new) | The translatable port + differential tests | 1, 2 |
| `formal/proofs/KeyKernel.lean` (new, generated) | Aeneas output — never hand-edited | 3 |
| `formal/proofs/KeyRoundTrip.lean` (new) | `decode_encode` for every ported impl | 4 |
| `formal/proofs/KeyMonoFixed.lean` (new) | `encode_strict_mono` for `u64`/`i64` | 5 |
| `formal/proofs/KeyFraming.lean` (new) | The framing lemma + variable-length and tuple monotonicity | 6 |
| `formal/proofs/lakefile.lean` | `lean_lib` entries for the four new modules | 3, 4, 5, 6 |
| `formal/scripts/check-drift.sh` | Watch `src/primary_key.rs` as well as `src/btree.rs` | 7 |
| `formal/README.md`, `Makefile` | Theorem table, regen commands, `test/formal-key-kernel` target | 1, 7 |

---

### Task 1: Kernel crate + fixed-width encodings

**Files:**
- Create: `formal/key_kernel/Cargo.toml`, `formal/key_kernel/src/lib.rs`
- Modify: `Makefile` (add `test/formal-key-kernel`)

**Interfaces:**
- Produces: `encode_u64(v: u64) -> Vec<u8>`, `decode_u64(b: &[u8]) -> Option<u64>`, `encode_i64(v: i64) -> Vec<u8>`, `decode_i64(b: &[u8]) -> Option<i64>`.
- Consumes: nothing.

**The property that matters here** is the signed sign-bit flip. `src/primary_key.rs` encodes `i64` as `(v as u64) ^ (1 << 63)`, big-endian, so that negatives sort before positives. Get that wrong and the ordering theorem in Task 5 will not close — which is the point.

- [ ] **Step 1: Create the crate manifest**

`formal/key_kernel/Cargo.toml` — the empty `[workspace]` table is what excludes it from the parent workspace, exactly as `formal/kernel/Cargo.toml` does:

```toml
[workspace]

[package]
name = "key_kernel"
version = "0.1.0"
edition = "2021"

[lib]
```

- [ ] **Step 2: Write the failing differential test**

Create `formal/key_kernel/src/lib.rs` containing only this test module:

```rust
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
```

- [ ] **Step 3: Run it to verify it fails**

Run: `cargo test --manifest-path formal/key_kernel/Cargo.toml`
Expected: FAIL — `encode_u64` is not defined.

- [ ] **Step 4: Write the port**

Prepend to `formal/key_kernel/src/lib.rs`:

```rust
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
```

- [ ] **Step 5: Run the tests to verify they pass**

Run: `cargo test --manifest-path formal/key_kernel/Cargo.toml`
Expected: PASS, 4 tests.

- [ ] **Step 6: Add the Makefile target**

In `Makefile`, beside `test/formal-kernel`:

```make
# Key-encoding kernel port (formal/key_kernel). Lean proofs: see formal/README.md.
test/formal-key-kernel:
	cargo test --manifest-path formal/key_kernel/Cargo.toml
```

Add `test/formal-key-kernel` to the `.PHONY` list on line 1.

- [ ] **Step 7: Confirm the parent workspace is unaffected**

Run: `cargo test --lib`
Expected: PASS — the new crate must not be pulled into the workspace. If cargo complains about `formal/key_kernel`, the empty `[workspace]` table is missing from its manifest.

- [ ] **Step 8: Commit**

```bash
git add formal/key_kernel Makefile
git commit -m "formal(key): kernel crate + fixed-width key encodings

Aeneas-translatable port of PrimaryKey's u64/i64 encode/decode, anchored to
the real implementation by a differential test. The sign-bit flip is the
load-bearing part: negatives must sort before positives."
```

---

### Task 2: Escape/terminate framing, byte-string and tuple encodings

**Files:**
- Modify: `formal/key_kernel/src/lib.rs`

**Interfaces:**
- Consumes: `encode_u64`/`decode_u64` from Task 1.
- Produces: `escape_and_terminate(bytes: &[u8]) -> Vec<u8>`, `unescape_until_terminator(bytes: &[u8], at: usize) -> Option<(Vec<u8>, usize)>`, `encode_bytes(b: &[u8]) -> Vec<u8>`, `decode_bytes(b: &[u8]) -> Option<Vec<u8>>`, `encode_pair_bytes_u64(a: &[u8], b: u64) -> Vec<u8>`, `decode_pair_bytes_u64(b: &[u8]) -> Option<(Vec<u8>, u64)>`.

**The framing rule**, from `src/primary_key.rs:362-378`: every `0x00` in a non-final element's encoding is escaped to `[0x00, 0xFF]`, then the terminator `[0x00, 0x01]` is appended. The final element is emitted raw. Order is preserved because at the position where one key ends and another continues, the terminator's `0x00` sorts below any literal byte (all real zeros having been escaped), and against an escaped zero the second byte decides — `0x01 < 0xFF`.

`encode_pair_bytes_u64` is the representative tuple: a **variable-length leading element** followed by a fixed-width one. That is the shape the original length-prefix design got wrong, so it is the one worth proving.

- [ ] **Step 1: Write the failing tests**

Add to the `tests` module in `formal/key_kernel/src/lib.rs`:

```rust
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
```

- [ ] **Step 2: Run to verify they fail**

Run: `cargo test --manifest-path formal/key_kernel/Cargo.toml`
Expected: FAIL — `escape_and_terminate` is not defined.

- [ ] **Step 3: Write the port**

Add to `formal/key_kernel/src/lib.rs`, above the test module:

```rust
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
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cargo test --manifest-path formal/key_kernel/Cargo.toml`
Expected: PASS, 9 tests.

- [ ] **Step 5: Commit**

```bash
git add formal/key_kernel/src/lib.rs
git commit -m "formal(key): escape/terminate framing and the pair encoding

Ports the variable-length framing and the (bytes, u64) tuple — the shape the
original length-prefixed design encoded out of order. Differential tests pin
the embedded-NUL and longer-sorts-earlier cases."
```

---

### Task 3: Translate to Lean and wire the build

**Files:**
- Create: `formal/proofs/KeyKernel.lean` (generated)
- Modify: `formal/proofs/lakefile.lean`, `formal/README.md`

**Interfaces:**
- Consumes: the finished kernel from Tasks 1–2.
- Produces: Lean definitions `key_kernel.encode_u64`, `key_kernel.decode_u64`, `key_kernel.encode_i64`, `key_kernel.decode_i64`, `key_kernel.escape_and_terminate`, `key_kernel.unescape_until_terminator`, `key_kernel.encode_bytes`, `key_kernel.decode_bytes`, `key_kernel.encode_pair_bytes_u64`, `key_kernel.decode_pair_bytes_u64`. The exact Lean names are whatever Aeneas emits — read them out of the generated file and record them in the report, because Tasks 4–6 refer to them.

**This task is a gate, not a deliverable.** If the translation does not come out axiom-free, stop and report which function leaked — that identifies an unmodeled std call to remove from the port, and it is better to find it now than inside a proof.

- [ ] **Step 1: Run the differential test as a precondition**

Run: `make test/formal-key-kernel`
Expected: PASS. The regen procedure in `formal/README.md` requires this first — the translation is only meaningful if the port matches the real code.

- [ ] **Step 2: Translate**

```bash
cd formal/key_kernel
PATH=$PWD/../.toolchain/charon-bin:$PATH charon cargo --preset=aeneas
../.toolchain/aeneas -backend lean key_kernel.llbc
```

Expected: `Generated: ./KeyKernel.lean`.

- [ ] **Step 3: Verify zero axioms — the gate**

```bash
grep -c '^axiom' formal/key_kernel/KeyKernel.lean
```

Expected: `0`. If it is not zero, run `grep -n '^axiom' KeyKernel.lean`, identify the unmodeled function, remove it from the port (replace with an explicit implementation), re-run Task 2's tests, and re-translate. Report what leaked and how you removed it.

- [ ] **Step 4: Install it beside the B-tree translation**

```bash
cp formal/key_kernel/KeyKernel.lean formal/proofs/
```

- [ ] **Step 5: Register the library**

In `formal/proofs/lakefile.lean`, beside the existing `lean_lib «BtreeKernel»` entry:

```lean
lean_lib «KeyKernel»
```

- [ ] **Step 6: Build**

Run: `cd formal/proofs && lake build KeyKernel`
Expected: success. First run in a fresh checkout fetches the Mathlib binary cache (~10 min); subsequent runs are fast.

- [ ] **Step 7: Record the emitted names**

Read `formal/proofs/KeyKernel.lean` and write the exact Lean name and type of each of the ten functions into your report. Tasks 4–6 depend on these, and Aeneas's naming (namespacing, `Result`/`Option` wrapping, `Std.Slice` vs `List`) is not guessable from the Rust.

- [ ] **Step 8: Add the regen procedure to the README**

In `formal/README.md`, under "Regenerating the translation after changing the kernel", add the key-kernel variant:

```bash
cd formal/key_kernel
cargo test                                            # differential test must pass
PATH=$PWD/../.toolchain/charon-bin:$PATH charon cargo --preset=aeneas
../.toolchain/aeneas -backend lean key_kernel.llbc    # writes KeyKernel.lean
cp KeyKernel.lean ../proofs/                          # then: cd ../proofs && lake build
```

- [ ] **Step 10: Commit**

```bash
git add formal/key_kernel/KeyKernel.lean formal/key_kernel/key_kernel.llbc \
        formal/proofs/KeyKernel.lean formal/proofs/lakefile.lean formal/README.md
git commit -m "formal(key): Aeneas translation of the key-encoding kernel

Generated, axiom-free. Never hand-edit KeyKernel.lean; regenerate via the
procedure in formal/README.md."
```

---

### Task 4: `decode_encode` — the round-trip theorem

**Files:**
- Create: `formal/proofs/KeyRoundTrip.lean`
- Modify: `formal/proofs/lakefile.lean`

**Interfaces:**
- Consumes: the generated definitions from Task 3.
- Produces: `key.decode_encode_u64`, `key.decode_encode_i64`, `key.unescape_escape`, `key.decode_encode_pair`.

**Statements to prove** (adapt the exact Lean spelling to Aeneas's emitted types — the *content* is fixed):

- `decode_encode_u64 : ∀ v, decode_u64 (encode_u64 v) = ok (some v)`
- `decode_encode_i64 : ∀ v, decode_i64 (encode_i64 v) = ok (some v)`
- `unescape_escape : ∀ b, unescape_until_terminator (escape_and_terminate b) 0 = ok (some (b, (escape_and_terminate b).length))`
- `decode_encode_pair : ∀ a v, decode_pair_bytes_u64 (encode_pair_bytes_u64 a v) = ok (some (a, v))`

**Proof strategy.** Do these in the order listed; each feeds the next.

1. `decode_encode_u64` is arithmetic: `decode` folds `acc := acc <<< 8 ||| b[i]` over the eight bytes `encode` produced by shifting. The lemma you need is that `(v >>> (8*(7-i))) &&& 0xFF` reassembles to `v` — `omega` after `simp` on the loop unrolling, or `bv_decide` if the bitvector view is cleaner. Eight iterations, so unrolling is tractable.
2. `decode_encode_i64` follows immediately from (1) plus involutivity of `xor` with a constant: `(x ^^^ k) ^^^ k = x`.
3. `unescape_escape` is the real work and needs an induction, so state and prove a generalized helper first — the naive statement is not an induction hypothesis, because a scan that stops at the terminator must be shown to ignore whatever follows it:

   `escape_prefix_scan : ∀ b suffix, unescape_until_terminator (escape_and_terminate b ++ suffix) 0 = ok (some (b, (escape_and_terminate b).length))`

   Induct on `b`. The two cases are `b[0] = 0x00` (the scan consumes two bytes and emits one) and `b[0] ≠ 0x00` (consumes one, emits one). The `suffix` generalization is what makes the lemma reusable for the tuple cases, where a fixed-width tail follows the terminator; `unescape_escape` is then `escape_prefix_scan` with `suffix := []`.
4. `decode_encode_pair` composes (3) with (1): `unescape_escape` gives back the head and a position, and the remaining bytes are exactly `encode_u64 v`.

- [ ] **Step 1: Create the file with the statements and `sorry`**

Write `formal/proofs/KeyRoundTrip.lean` with all four theorem statements, each proved by `sorry`, plus the `escape_prefix_scan` helper. Add `lean_lib «KeyRoundTrip»` to `formal/proofs/lakefile.lean`.

- [ ] **Step 2: Confirm the statements typecheck**

Run: `cd formal/proofs && lake build KeyRoundTrip`
Expected: builds with `sorry` warnings only. **If a statement does not typecheck, fix the statement now** — a theorem that does not even elaborate against the generated definitions is the most common way this kind of task stalls, and it is cheap to fix before any proof effort.

- [ ] **Step 3: Discharge the two fixed-width round-trips**

Replace the `sorry` in `decode_encode_u64` and `decode_encode_i64` per strategy items 1–2.

- [ ] **Step 4: Verify**

Run: `cd formal/proofs && lake build KeyRoundTrip`
Expected: two fewer `sorry` warnings; no errors.

- [ ] **Step 5: Discharge `escape_prefix_scan` and `unescape_escape`**

Per strategy item 3. Prove the generalized helper first, then derive `unescape_escape` with `suffix := []`.

- [ ] **Step 6: Discharge `decode_encode_pair`**

Per strategy item 4.

- [ ] **Step 7: Verify sorry-free and axiom-clean**

Add at the end of the file:

```lean
#print axioms key.decode_encode_u64
#print axioms key.decode_encode_i64
#print axioms key.unescape_escape
#print axioms key.decode_encode_pair
```

Run: `cd formal/proofs && lake build KeyRoundTrip`
Expected: zero `sorry` warnings; each `#print axioms` reports only `propext, Classical.choice, Quot.sound`.

- [ ] **Step 8: Commit**

```bash
git add formal/proofs/KeyRoundTrip.lean formal/proofs/lakefile.lean
git commit -m "formal(key): decode_encode round-trip theorems

u64/i64 by bit arithmetic; the escape framing via a suffix-generalized scan
lemma, which is what makes the induction go through and what the pair case
reuses."
```

---

### Task 5: `encode_strict_mono` for the fixed-width encodings

**Files:**
- Create: `formal/proofs/KeyMonoFixed.lean`
- Modify: `formal/proofs/lakefile.lean`

**Interfaces:**
- Consumes: the generated definitions from Task 3.
- Produces: `key.lex_lt` (the comparison relation), `key.encode_mono_u64`, `key.encode_mono_i64`.

**Statements:**

- `encode_mono_u64 : ∀ a b, a < b ↔ lex_lt (encode_u64 a) (encode_u64 b)`
- `encode_mono_i64 : ∀ a b, a < b ↔ lex_lt (encode_i64 a) (encode_i64 b)`

**The `↔` is deliberate and must not be weakened to `→`.** Both directions together give antisymmetry, and antisymmetry gives injectivity for free — which is the property `BTree::from_sorted` actually relies on when it assumes distinct keys stay distinct.

**Proof strategy.**

1. Define `lex_lt : List U8 → List U8 → Prop` yourself — do not reach for a Mathlib lexicographic order and then fight its `DecidableEq`/`WellFounded` instances. The direct recursive definition (`[] < (_::_)`; `(x::xs) < (y::ys)` iff `x < y ∨ (x = y ∧ xs < ys)`) is what the byte comparison in the consumers actually does, and it is easier to reason about.
2. For `u64`: both encodings are exactly eight bytes, so `lex_lt` reduces to "first differing byte decides". Prove a helper `be_bytes_lex : ∀ a b, a < b ↔ lex_lt (encode_u64 a) (encode_u64 b)` by finding the most significant differing byte. `bv_decide` may discharge the byte-level comparison directly; if it times out, unroll the eight positions.
3. For `i64`: rewrite through `encode_i64 v = encode_u64 ((v as u64) ^^^ 2^63)` and reduce to (2). The remaining obligation is that `x ↦ x ^^^ 2^63` is an order-isomorphism from `i64`'s signed order to `u64`'s unsigned order — the mathematical heart of the sign-bit flip, and worth stating as its own lemma `sign_flip_order_iso` because it is the part a reader will want to check.

- [ ] **Step 1: Create the file with statements and `sorry`; register the lib**

Write `formal/proofs/KeyMonoFixed.lean` with `lex_lt`, `sign_flip_order_iso`, and the two theorems, all `sorry`. Add `lean_lib «KeyMonoFixed»` to the lakefile.

- [ ] **Step 2: Confirm it typechecks**

Run: `cd formal/proofs && lake build KeyMonoFixed`
Expected: builds with `sorry` warnings only.

- [ ] **Step 3: Discharge `encode_mono_u64`**

Per strategy item 2.

- [ ] **Step 4: Verify**

Run: `cd formal/proofs && lake build KeyMonoFixed`
Expected: one fewer `sorry`; no errors.

- [ ] **Step 5: Discharge `sign_flip_order_iso` and `encode_mono_i64`**

Per strategy item 3.

- [ ] **Step 6: Verify sorry-free and axiom-clean**

Append `#print axioms key.encode_mono_u64` and `#print axioms key.encode_mono_i64`.

Run: `cd formal/proofs && lake build KeyMonoFixed`
Expected: zero `sorry` warnings; standard trio only.

- [ ] **Step 7: Commit**

```bash
git add formal/proofs/KeyMonoFixed.lean formal/proofs/lakefile.lean
git commit -m "formal(key): strict monotonicity for u64/i64 encodings

Stated as an iff, so antisymmetry — and hence injectivity, which from_sorted
relies on — falls out. The sign-bit flip is isolated as an order-isomorphism
lemma, the part worth checking by eye."
```

---

### Task 6: The framing lemma and monotonicity for variable-length and tuple keys

**Files:**
- Create: `formal/proofs/KeyFraming.lean`
- Modify: `formal/proofs/lakefile.lean`

**Interfaces:**
- Consumes: `lex_lt` and `encode_mono_u64` from Task 5; `escape_prefix_scan` from Task 4.
- Produces: `key.framing_no_confusion`, `key.encode_mono_bytes`, `key.encode_mono_pair`.

**This is the task the whole plan exists for.** Tasks 1–5 are infrastructure and warm-up; the length-prefix bug lived exactly here.

**Statements:**

- `encode_mono_bytes : ∀ a b, lex_lt a b ↔ lex_lt (encode_bytes a) (encode_bytes b)` (trivial — `encode_bytes` is the identity — but state it so the README table is honest about what is and is not proved).
- `framing_no_confusion` — the load-bearing lemma. For any two byte strings `a ≠ b`, comparing `escape_and_terminate a ++ sa` against `escape_and_terminate b ++ sb` is decided **inside the framed region**, never by the suffixes:

  `∀ a b sa sb, a ≠ b → (lex_lt (escape_and_terminate a ++ sa) (escape_and_terminate b ++ sb) ↔ lex_lt a b)`

- `encode_mono_pair : ∀ a x b y, (a, x) < (b, y) ↔ lex_lt (encode_pair_bytes_u64 a x) (encode_pair_bytes_u64 b y)`, where `(a, x) < (b, y)` is the lexicographic product order — `lex_lt a b ∨ (a = b ∧ x < y)`.

**Proof strategy for `framing_no_confusion`.** Induct on `a` and `b` together. The interesting case is where one runs out first — say `a` is a proper prefix of `b`. Then `escape_and_terminate a` continues with `0x00, TERM = 0x00, 0x01`, while `escape_and_terminate b` continues with the escaping of `b`'s next byte, which is either:

- a literal byte `≥ 0x01` (all real zeros having been escaped), so `0x00 < that` and the shorter key sorts first; or
- an escaped zero `0x00, 0xFF`, so the first bytes tie at `0x00` and the second decides: `0x01 < 0xFF`, and again the shorter key sorts first.

Both branches agree with `lex_lt a b`, which is what "a proper prefix sorts first" means. **That two-case split is the entire correctness argument for the encoding** — the length-prefix design failed precisely because it decided this comparison on a length byte before reaching either case. Make those two cases explicit named sub-lemmas rather than burying them in a tactic block; a reader should be able to find them.

Then `encode_mono_pair` follows: `framing_no_confusion` handles `a ≠ b`, and the `a = b` case reduces to `encode_mono_u64` on the tails.

- [ ] **Step 1: Create the file with statements and `sorry`; register the lib**

Write `formal/proofs/KeyFraming.lean` with the three theorems and the two named sub-lemmas for the prefix case, all `sorry`. Add `lean_lib «KeyFraming»` to the lakefile.

- [ ] **Step 2: Confirm it typechecks**

Run: `cd formal/proofs && lake build KeyFraming`
Expected: builds with `sorry` warnings only.

- [ ] **Step 3: Discharge `encode_mono_bytes`**

It is `Iff.rfl` or close to it, since `encode_bytes` is the identity.

- [ ] **Step 4: Discharge the two prefix sub-lemmas**

The literal-byte case and the escaped-zero case, per the strategy above.

- [ ] **Step 5: Discharge `framing_no_confusion`**

The induction, using the two sub-lemmas for the prefix case.

- [ ] **Step 6: Discharge `encode_mono_pair`**

Compose `framing_no_confusion` with `encode_mono_u64`.

- [ ] **Step 7: Add the 3-tuple and discharge it**

The brief names the 2- *and* 3-tuple combinators, and the 3-tuple is where two framed elements sit in sequence — a shape the 2-tuple does not exercise, since it has only one framed element. Add to `formal/key_kernel/src/lib.rs`:

```rust
/// Two variable-length leading elements followed by a fixed-width one.
/// Both leading elements are framed; only the last is raw.
pub fn encode_triple_bytes_bytes_u64(a: &[u8], b: &[u8], c: u64) -> Vec<u8> {
    let mut out = escape_and_terminate(a);
    let mid = escape_and_terminate(b);
    let mut i: usize = 0;
    while i < mid.len() {
        out.push(mid[i]);
        i += 1;
    }
    let tail = encode_u64(c);
    let mut j: usize = 0;
    while j < tail.len() {
        out.push(tail[j]);
        j += 1;
    }
    out
}
```

Add a differential test mirroring `pair_encoding_preserves_order_for_variable_length_leads` but varying the *second* element while the first ties — that is the case only the 3-tuple reaches:

```rust
    #[test]
    fn triple_encoding_preserves_order_when_the_first_element_ties() {
        let mut cases = vec![
            (b"t".to_vec(), b"aa".to_vec(), 0u64),
            (b"t".to_vec(), b"b".to_vec(), 0u64),
            (b"t".to_vec(), vec![0x00], 0u64),
            (b"t".to_vec(), vec![0x00, 0x00], 0u64),
            (b"s".to_vec(), b"zzz".to_vec(), 0u64),
        ];
        cases.sort();
        for w in cases.windows(2) {
            let (lo, hi) = (&w[0], &w[1]);
            assert!(
                encode_triple_bytes_bytes_u64(&lo.0, &lo.1, lo.2)
                    < encode_triple_bytes_bytes_u64(&hi.0, &hi.1, hi.2),
                "order not preserved: {lo:?} -> {hi:?}"
            );
        }
    }
```

Re-run `make test/formal-key-kernel`, re-translate per Task 3's procedure (the kernel changed, so `KeyKernel.lean` must be regenerated and the zero-axiom check re-run), then prove:

`encode_mono_triple : ∀ a b x c d y, (a, b, x) < (c, d, y) ↔ lex_lt (encode_triple_bytes_bytes_u64 a b x) (encode_triple_bytes_bytes_u64 c d y)`

by applying `framing_no_confusion` twice — once at the first boundary, and once at the second under the hypothesis that the first elements are equal.

- [ ] **Step 8: Verify sorry-free and axiom-clean**

Append `#print axioms` for all three theorems.

Run: `cd formal/proofs && lake build`
Expected: the whole proof library builds; zero `sorry` warnings; standard trio only.

- [ ] **Step 9: Cross-check the proof against the differential tests**

Run: `make test/formal-key-kernel`
Expected: PASS. The proofs and the tests must agree; if the proof of `framing_no_confusion` went through but `pair_encoding_preserves_order_for_variable_length_leads` fails, the port has drifted from what was translated — re-translate before believing the proof.

- [ ] **Step 10: Commit**

```bash
git add formal/proofs/KeyFraming.lean formal/proofs/lakefile.lean
git commit -m "formal(key): framing lemma + tuple monotonicity

framing_no_confusion is the theorem this whole arc exists for: comparing two
framed elements is decided inside the framed region, never by what follows.
The two prefix sub-lemmas (literal byte >= 0x01 vs escaped zero 0x00,0xFF)
are the entire correctness argument for choosing escape-and-terminate over
length prefixes, which encoded ('aa', 0) after ('b', 0)."
```

---

### Task 7: Drift guard, README, and CI

**Files:**
- Modify: `formal/scripts/check-drift.sh`, `formal/README.md`, `.github/workflows/formal.yml`

**Interfaces:**
- Consumes: everything above.
- Produces: a drift guard that fires on `src/primary_key.rs` as well as `src/btree.rs`.

- [ ] **Step 1: Generalize the drift guard to multiple watched files**

`formal/scripts/check-drift.sh` currently has `WATCHED="src/btree.rs"` and a single string comparison. Change it to a list and test membership:

```bash
WATCHED="src/btree.rs src/primary_key.rs"
```

and in the loop, replace `[ "$f" = "$WATCHED" ] && touched_watched=true` with:

```bash
  for w in $WATCHED; do
    [ "$f" = "$w" ] && touched_watched=true
  done
```

Update the messages: the failure text names the B-tree insert/remove paths specifically, so extend it to say which surface each watched file maps to — `src/btree.rs` → `formal/kernel/`, `src/primary_key.rs` → `formal/key_kernel/`.

- [ ] **Step 2: Verify the guard fires**

```bash
touch src/primary_key.rs && git add -A && formal/scripts/check-drift.sh HEAD; echo "EXIT=$?"
```
Expected: FAIL with a non-zero exit, naming `src/primary_key.rs`. Then `git reset` and confirm a `formal/`-touching change makes it pass.

- [ ] **Step 3: Verify the existing guard still works**

```bash
touch src/btree.rs && git add -A && formal/scripts/check-drift.sh HEAD; echo "EXIT=$?"
```
Expected: still FAIL — the generalization must not have broken the original case. `git reset` afterwards.

- [ ] **Step 4: Update the README theorem table**

In `formal/README.md`, add rows to the "What is proved" table for `encode_mono_u64`, `encode_mono_i64`, `encode_mono_bytes`, `encode_mono_pair`, `framing_no_confusion`, and the four `decode_encode` theorems, each with its file and a one-line meaning.

Then update the "Not yet covered" paragraph with the two honest boundaries: **`String::decode`** (calls `String::from_utf8`; the encode side is covered by `encode_bytes` since `String::encode` is `as_bytes().to_vec()`), and **the remaining `PrimaryKey` impls** (`u8`/`u16`/`u32`/`u128` and their signed counterparts, and the 3-tuple) — the same argument applies to each by the same shape, but only the listed representatives are machine-checked.

Also add the key-kernel entries to the "Ground rules" section: `proofs/KeyKernel.lean` is generated and never hand-edited; `#print axioms` must be checked on the new theorems too.

- [ ] **Step 5: Add the key kernel to the weekly Lean CI job**

In `.github/workflows/formal.yml`, the `lean` job rebuilds the proofs and checks axioms. `lake build` already covers the new libs once they are in the lakefile, but the differential test needs a step of its own — add `make test/formal-key-kernel` alongside `make test/formal-kernel`.

- [ ] **Step 6: Run the whole gate**

```bash
make test/formal-kernel
make test/formal-key-kernel
formal/scripts/check-drift.sh HEAD
cd formal/proofs && lake build
```
Expected: all pass; `lake build` sorry-free.

- [ ] **Step 7: Run the main-crate CI matrix**

```bash
make lint
cargo clippy -p ultima-vector -- -D warnings
cargo test --features persistence,fulltext,metrics
cargo test --lib
cargo test -p ultima-vector --features persistence
```
Expected: all pass. The new crate is outside the workspace, so this should be unaffected — confirm rather than assume.

- [ ] **Step 8: Commit**

```bash
git add formal/scripts/check-drift.sh formal/README.md .github/workflows/formal.yml
git commit -m "formal(key): extend the drift guard to src/primary_key.rs

The guard now watches both modeled surfaces. README records the new theorems
and the two honest boundaries: String::decode (from_utf8 is unmodeled; the
encode side is covered because String::encode is as_bytes().to_vec()), and
the un-instantiated width/arity variants."
```

---

## Risks, and what to do about them

- **A proof does not close.** Unlike ordinary implementation, proof effort is not reliably estimable; the roadmap's "~1 session" is a hope, not a measurement. If `framing_no_confusion` resists, the fallback that preserves most of the value is to prove it for a bounded byte alphabet or bounded length and record the restriction explicitly in the README — a bounded theorem honestly labelled beats an unbounded `sorry`. Do not commit a `sorry`.
- **Aeneas rejects a construct.** Most likely candidates are the `while` loops with multiple exit flags in `unescape_until_terminator`, and slice indexing. If translation fails, simplify toward the shape `formal/kernel/src/lib.rs` already uses successfully — it is the proof that these constructs translate.
- **The port drifts from `src/primary_key.rs`.** Task 7's guard is the systematic answer; the differential tests are the immediate one. Both must be in place before this is worth trusting.

## Non-goals

- **`hash64`.** It is an OCC digest; collisions are tolerated by design and cause a spurious retry, never a missed conflict. There is nothing to prove.
- **Porting every `PrimaryKey` impl.** `u64`/`i64` represent the fixed-width family, `Vec<u8>` the variable-length one, and `(bytes, u64)` the tuple combinator. The remaining widths and the 3-tuple follow the same shape; Task 7 records that they are not separately machine-checked.
- **`String::decode`.** `String::from_utf8` is unmodeled and would leak an axiom.
