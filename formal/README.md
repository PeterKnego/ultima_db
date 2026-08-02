# Formal verification of the B-tree insert/delete paths and the primary-key encoding

Machine-checked (Lean 4) proofs about UltimaDB's copy-on-write B-tree
insertion and deletion algorithms, and about the order-preserving primary-key
encoding (`src/primary_key.rs`), verified against a mechanical Rust→Lean
translation produced by [Aeneas](https://github.com/AeneasVerif/aeneas).

## What is proved: B-tree insert/delete

Over the translated kernel (`proofs/BtreeKernel.lean`, generated — never edit),
all sorry-free, `#print axioms` = `propext, Classical.choice, Quot.sound` only:

| Theorem (file) | Meaning |
|---|---|
| `BTree.insert_inv` (`BtreeInsertInv.lean`) | insert preserves the full B-tree ordering invariant (sorted entries, key-interval-aligned children, arity ≤ MAX_KEYS), incl. median splits |
| `BTree.insert_get` (`BtreeInsertGet.lean`) | `get k` after `insert k v` returns `some v` |
| `BTree.insert_frame` (`BtreeInsertFrame.lean`) | `get k'` is unchanged for every `k' ≠ k` |
| `BTree.remove_inv` (`RemoveInv.lean`) | remove preserves the ordering invariant **and** height-uniformity (`HeightInv`), through rotate/merge rebalancing and root collapse |
| `BTree.remove_get` (`RemoveGet.lean`) | `get k` after `remove k` returns `none` |
| `BTree.remove_frame` (`RemoveFrame.lean`) | `get k'` is unchanged for every `k' ≠ k` |
| `BTree.remove_total` (`RemoveTotal.lean`) | `remove` never fails on a well-formed nonempty tree (`∃ r, remove k = ok r`) |
| `BTree.remove_spec` (`RemoveTotal.lean`) | unconditional: `remove k` reports the key absent, or returns a valid balanced tree with `k` gone |
| `BTree.remove_minkeys` (`MinKeysPreserve.lean`) | `remove` **preserves** the MIN_KEYS balance invariant: the rotate/merge rebalancers restore every non-root node to ≥ `T−1 = 31` entries after a delete drops one to 30 |
| `BTree.remove_balanced_spec` (`MinKeysPreserve.lean`) | capstone: absent, or a valid tree that is well-formed, height-uniform, **and MIN_KEYS-balanced**, with `k` gone |

Together: **insert behaves exactly as a map update, and remove exactly as a map
deletion.** Because the Aeneas translation is total-with-explicit-effects, the
theorems also rule out panics, overflow, and out-of-bounds indexing on the
insert/get and (via `remove_total`) remove/rebalance paths for any tree
satisfying the invariant.

Two notes on the remove proofs:
- The invariant is strengthened with a height-uniformity predicate (`HeightInv`,
  `BalancedInvariant.lean`). Bare `NodeInv`/`Aligned` permit a parent whose
  children have mixed leaf/internal status, on which `merge` is malformed — so
  height-uniformity is *required* to state remove-preserves-the-invariant truly.
- `remove_inv`/`remove_get`/`remove_frame` are stated conditional on the kernel
  returning `ok`, because `NodeInv ∧ HeightInv` still admit pathological 0-entry
  internal nodes on which `delete` legitimately fails. `remove_total`
  (`MinKeysInvariant.lean`) closes that gap: under the MIN_KEYS balance invariant
  (every non-root node ≥ 31 entries), `remove` provably returns `ok`, and
  `remove_spec` packages this with the properties into an unconditional statement.
- `remove_total` shows the balance invariant is *strong enough* for `remove` to
  succeed; `remove_minkeys` (`MinKeysPreserve.lean`) closes the other direction —
  the invariant is *preserved*, so the balanced class is closed under `remove`.
  The proof carries an "almost-balanced" post-condition through the recursion
  (`AlmostMinArity`: the returned subtree root may be underfull by one, but every
  proper descendant is ≥ 31) and shows each rebalancer restores ≥ 31; this is the
  arithmetic-heavy direction. `remove_balanced_spec` bundles it with `remove_spec`.

The remove-preserves-lookups proofs go through an in-order `flatten`
characterization (`RemoveFlatten.lean`): `get` equals a lookup in the flattened
key list, and every rebalancer (rotate/merge/fix) is flatten-invariant, so
delete's only effect on lookups is dropping the deleted key.

Not yet covered: range iterators; anything concurrent (store/OCC/WAL — out of
Aeneas scope; needs hand-written protocol models).

## What is proved: primary-key encoding

Over the translated kernel (`proofs/KeyKernel.lean`, generated — never edit),
all in `namespace key_kernel`, all sorry-free, `#print axioms` =
`propext, Classical.choice, Quot.sound` only:

| Theorem (file) | Meaning |
|---|---|
| `decode_encode_u64` (`KeyRoundTrip.lean`) | `decode_u64 (encode_u64 v) = v` — the fixed-width unsigned encoding round-trips |
| `decode_encode_i64` (`KeyRoundTrip.lean`) | `decode_i64 (encode_i64 v) = v` — ditto for the sign-flipped signed encoding |
| `escape_prefix_scan` (`KeyRoundTrip.lean`) | scanning an escaped-and-terminated buffer for the terminator recovers exactly the escaped span that was written |
| `unescape_escape` (`KeyRoundTrip.lean`) | unescaping the output of the escaper recovers the original bytes |
| `decode_encode_pair` (`KeyRoundTrip.lean`) | round-trip for the variable-length-lead `(Vec<u8>, u64)` pair encoding |
| `encode_mono_u64` (`KeyMonoFixed.lean`) | `a < b ↔ encode_u64 a` is lexicographically less than `encode_u64 b` — order-preserving |
| `encode_mono_i64` (`KeyMonoFixed.lean`) | same order-preservation for the signed encoding, through the sign-flip |
| `sign_flip_order_iso` (`KeyMonoFixed.lean`) | the sign-flip transform used by `encode_i64` is an order isomorphism on `i64` |
| `encode_inj_u64` (`KeyMonoFixed.lean`) | `encode_u64` is injective (distinct inputs give distinct encodings) |
| `encode_inj_i64` (`KeyMonoFixed.lean`) | `encode_i64` is injective |
| `encode_mono_bytes` (`KeyFraming.lean`) | the escape-and-terminate framing of `Vec<u8>` is order-preserving over lexicographic byte order |
| `framed_prefix_lt_literal` (`KeyFraming.lean`) | a framed prefix that ends (terminator) sorts before any continuation that instead has a literal non-zero byte at that position |
| `framed_prefix_lt_escaped_zero` (`KeyFraming.lean`) | a framed prefix that ends sorts before a continuation that instead has an escaped `0x00` at that position |
| `escaped_byte_order` (`KeyFraming.lean`) | escaped bytes preserve the order of the underlying byte at a divergence point |
| `framing_no_confusion` (`KeyFraming.lean`) | headline lemma: for any two distinct byte strings, comparing their framed encodings followed by arbitrary suffixes gives exactly the order of the byte strings themselves — the framing never confuses "ended" with "continues" |
| `framing_no_confusion_kernel` (`KeyFraming.lean`) | the same no-confusion property restated directly over the kernel's `escape_and_terminate` |
| `framed_element_lex` (`KeyFraming.lean`) | the reusable composition primitive: comparing two framed elements followed by arbitrary suffixes reduces to comparing the framed elements alone |
| `encode_mono_pair` (`KeyFraming.lean`) | the `(Vec<u8>, u64)` pair encoding is order-preserving (lexicographic on the pair) |
| `encode_mono_triple` (`KeyFraming.lean`) | the `(Vec<u8>, Vec<u8>, u64)` triple encoding is order-preserving |

Together: **the modeled encoders are injective and order-preserving**, and the
escape/terminate framing used to make variable-length elements self-delimiting
inside a composite key has no confusion cases — a framed prefix that has ended
never silently compares equal-or-wrong against a continuation.

### Not yet covered (primary-key encoding)

These boundaries were each found during the arc and are recorded here rather
than glossed over:

- **`String::decode`** is not covered — it calls `String::from_utf8`, which
  Aeneas would have to axiomatize (an unmodeled std function). The *encode*
  side of `String` IS covered: `String::encode` is `as_bytes().to_vec()`, and
  `Ord for String` is bytewise over UTF-8, agreeing with `Ord for Vec<u8>`, so
  `encode_mono_bytes` applies transitively.
- **Fixed-width *non-final* tuple elements are unmodeled.** The real `(A, B)` /
  `(A, B, C)` impls (`src/primary_key.rs:443-505`) skip the escape/terminate
  framing for any non-final element whose `ENCODED_LEN == Some(_)` (fixed-width
  elements are already self-delimiting by their constant length) — this isn't
  limited to a leading `A`: in the 3-tuple, `B` is a middle element and gets
  the same treatment (`src/primary_key.rs:496-500`). The kernel only models
  the variable-length branch, so that half of the tuple encoder — "a
  fixed-width non-final element needs no framing" — has no theorem.
- **The rejection path is uncharacterized.** The proofs only talk about
  outputs of `escape_and_terminate`; inside the kernel's decoder the `bad`
  branch was proved *unreachable* on those outputs, not characterized in
  general. Nothing here says the decoder is safe (or what it does) on
  arbitrary malformed byte strings that didn't come from the encoder.
- **Un-instantiated widths and arities.** `u8`/`u16`/`u32`/`u128` and their
  signed counterparts, and tuples beyond the modeled 2-/3-element shapes,
  follow the same argument by the same shape, but only `u64`/`i64`,
  `Vec<u8>`, and the pair/triple combinators are machine-checked.
- **`lex_lt` ↔ Rust's `Ord`** remains prose, not machine-checked. `lex_lt`
  was mechanically proved interchangeable with Lean core's
  `List.Lex (· < ·)` (`lex_lt_iff_lexNat` and friends, `KeyMonoFixed.lean`),
  but the claim that Rust's std `Ord` for `Vec<u8>` *is* that lexicographic
  order is outside the Aeneas translation and is asserted, not proved.
- **No general composition theorem** for arbitrary element types — there is
  no "for any two order-preserving encoders, their framed concatenation is
  order-preserving" theorem. `framed_element_lex` is the reusable piece that
  a future such theorem would be built from, but composition itself is only
  instantiated for the specific pair/triple shapes above.

## Layout

- `kernel/` — the verification kernel: the insert/get core of `src/btree.rs`
  ported to the Aeneas-supported safe-Rust subset. Deltas from the real code
  are documented at the top of `kernel/src/lib.rs`; behavioral equivalence is
  anchored by a differential test against `std::collections::BTreeMap`
  (`cargo test --manifest-path formal/kernel/Cargo.toml`, or
  `make test/formal-kernel`). Excluded from the cargo workspace so Charon
  owns its build.

  **Node-storage abstraction (task52):** production nodes store entries and
  children in `FixedVec` inline slot arrays; the kernel keeps `Vec`. This is
  deliberate: the kernel is a *behavioral* model of the node's sequence
  semantics, which `FixedVec` implements Vec-compatibly for the initialized
  prefix. Representation-level risks the model therefore does not see (slot
  bookkeeping, the `u8` length) are covered by a compile-time capacity guard
  (`T ≤ 127`) and the `FixedVec` unit tests in `src/btree.rs`.

  **Fanout coverage:** this instantiation proves the default `T = 32`. The
  opt-in `fanout-t8` configuration (T = 8) is the same algorithm with the
  same by-role constants and is *not* separately instantiated; closing that
  gap properly means making the development T-parametric (`2 ≤ T ≤ 127`) —
  tracked as a follow-up in `docs/tasks/task52_btree_fixedvec_fanout.md`.
- `key_kernel/` — a second verification kernel: the order-preserving key
  encoding of `src/primary_key.rs` (`encode_u64`/`decode_u64`,
  `encode_i64`/`decode_i64`, the `escape_and_terminate` framing, and the
  variable-length-lead pair encoding), ported to the same safe-Rust subset.
  Deltas from the real code are documented at the top of
  `key_kernel/src/lib.rs`; equivalence is anchored by a differential test
  against reproduced copies of the real impls
  (`make test/formal-key-kernel`). Translated to `proofs/KeyKernel.lean`;
  the ordering/roundtrip theorems over it live in `KeyRoundTrip.lean`,
  `KeyMonoFixed.lean`, and `KeyFraming.lean` (see "What is proved:
  primary-key encoding" above).
- `proofs/` — the lake package: `BtreeKernel.lean` (generated),
  `KeyKernel.lean` (generated),
  `BtreeInvariant.lean` + `BalancedInvariant.lean` + `MinKeysInvariant.lean`
  (invariant definitions), helper-lemma modules (`FindPosSpec`, `EntrySpecs`,
  `ChildrenSpecs`, `ListLemmas`, `AlignedLemmas`, `TransportLemmas`,
  `RemoveSpecs`), the rebalance surgery (`RemoveRebalance.lean`), the length-only
  rebalancer-totality lemmas (`RemoveTotalCore.lean`), the in-order flatten
  foundation (`RemoveFlatten.lean`), the theorem files
  (`BtreeInsert{Inv,Get,Frame}`, `Remove{Inv,Get,Frame}`, `RemoveTotal`), and the
  MIN_KEYS-preservation layer (`MinKeysPreserve.lean`) for the B-tree; and the
  key-encoding theorem files `KeyRoundTrip.lean` (round-trip),
  `KeyMonoFixed.lean` (fixed-width monotonicity/injectivity), and
  `KeyFraming.lean` (framing no-confusion and composite-key monotonicity).
- `WRITEUP.md` — the full narrative (methodology, Leanstral evaluation,
  Lean-engineering findings).

## Toolchain pins

| Component | Version |
|---|---|
| Aeneas | `nightly-2026.07.06-45061fa` (prebuilt release) |
| Charon | `nightly-2026.07.02` (= the commit in that Aeneas's `charon-pin`) |
| Rust (for Charon) | `nightly-2026-06-01` + rustc-dev, llvm-tools-preview, rust-src |
| Lean | `leanprover/lean4:v4.30.0-rc2` (via elan, see `proofs/lean-toolchain`) |

Charon and Aeneas versions must match exactly (LLBC format is versioned);
same-day nightlies can mismatch — always take Charon from the Aeneas
`charon-pin`.

**`bv_decide` is unusable in this repo.** The key-encoding proofs lean on
bitvector reasoning (byte extraction, sign flips), which makes `bv_decide` a
tempting shortcut — do not use it. In Lean `v4.30.0-rc2` (the pin above) it
discharges via native evaluation and mints a `…_native.bv_decide.ax_N` axiom
that no config flag disables, so any theorem that uses it fails the
"only `propext`/`Classical.choice`/`Quot.sound`" axiom check. The same applies
to `native_decide` and anything else routing through `ofReduceBool`.

## Building the proofs

```bash
./formal/scripts/fetch-toolchain.sh   # once: prebuilt toolchain → formal/.toolchain
cd formal/proofs
lake build                            # first run: fetches Mathlib binary cache (~10 min)
```

## Regenerating the translation after changing the kernel

B-tree kernel:

```bash
cd formal/kernel
cargo test                                            # differential test must pass
PATH=$PWD/../.toolchain/charon-bin:$PATH charon cargo --preset=aeneas
../.toolchain/aeneas -backend lean btree_kernel.llbc  # writes BtreeKernel.lean
cp BtreeKernel.lean ../proofs/                        # then: cd ../proofs && lake build
```

Key-encoding kernel:

```bash
cd formal/key_kernel
cargo test                                            # differential test must pass
PATH=$PWD/../.toolchain/charon-bin:$PATH charon cargo --preset=aeneas
../.toolchain/aeneas -backend lean key_kernel.llbc    # writes KeyKernel.lean
cp KeyKernel.lean ../proofs/                          # then: cd ../proofs && lake build
```

The generated `BtreeKernel.lean` / `KeyKernel.lean` must contain **zero
`axiom`s** (an axiom means an unmodeled std function slipped in) — `grep -c
'^axiom'` after regenerating. The `.llbc` and the in-kernel-directory `.lean`
are build intermediates and are gitignored; only the `proofs/` copy is
committed.

## Ground rules

- `proofs/BtreeKernel.lean` and `proofs/KeyKernel.lean` are generated; never
  hand-edit.
- Any change to the insert or delete path of `src/btree.rs` must be mirrored in
  `formal/kernel/src/lib.rs` (or explicitly noted as unverified drift). Any
  change to the order-preserving encoding in `src/primary_key.rs` must be
  mirrored in `formal/key_kernel/src/lib.rs` likewise. Both are enforced by
  the same guard: `make formal/drift-check` (run in CI on every PR, see
  `.github/workflows/formal.yml`) fails if `src/btree.rs` or
  `src/primary_key.rs` changed without a matching `formal/` change. For a
  change outside the verified surface (e.g. range iterators, comments, an
  unrelated method or `PrimaryKey` impl), acknowledge it with
  `ACK_NO_FORMAL=1` locally, or `[skip-formal-drift]` in the PR title.
- No `sorry` may be committed; verify with `#print axioms` on the top-level
  theorems `btree_kernel.BTree.{insert,remove}_{inv,get,frame}` plus
  `BTree.remove_total` / `BTree.remove_spec` / `BTree.remove_minkeys` /
  `BTree.remove_balanced_spec` — and, for the key encoding, the theorems
  listed in "What is proved: primary-key encoding" above (all in
  `namespace key_kernel`) — only the three standard Lean axioms are
  acceptable. The scheduled `lean` CI job (weekly + `workflow_dispatch`)
  rebuilds the proofs and re-runs both differential tests (`make
  test/formal-kernel` / `make test/formal-key-kernel` — each checked against a
  reproduced oracle: `std::BTreeMap` for the B-tree kernel, reproduced copies
  of the real impls for the key kernel), plus the actual axiom check: the
  `#print axioms` step in `.github/workflows/formal.yml`.
