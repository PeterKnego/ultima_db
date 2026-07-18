# Formal verification of the B-tree insert and delete paths

Machine-checked (Lean 4) proofs about UltimaDB's copy-on-write B-tree
insertion and deletion algorithms, verified against a mechanical Rust→Lean
translation produced by [Aeneas](https://github.com/AeneasVerif/aeneas).

## What is proved

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
- `proofs/` — the lake package: `BtreeKernel.lean` (generated),
  `BtreeInvariant.lean` + `BalancedInvariant.lean` + `MinKeysInvariant.lean`
  (invariant definitions), helper-lemma modules (`FindPosSpec`, `EntrySpecs`,
  `ChildrenSpecs`, `ListLemmas`, `AlignedLemmas`, `TransportLemmas`,
  `RemoveSpecs`), the rebalance surgery (`RemoveRebalance.lean`), the length-only
  rebalancer-totality lemmas (`RemoveTotalCore.lean`), the in-order flatten
  foundation (`RemoveFlatten.lean`), the theorem files
  (`BtreeInsert{Inv,Get,Frame}`, `Remove{Inv,Get,Frame}`, `RemoveTotal`), and the
  MIN_KEYS-preservation layer (`MinKeysPreserve.lean`).
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

## Building the proofs

```bash
./formal/scripts/fetch-toolchain.sh   # once: prebuilt toolchain → formal/.toolchain
cd formal/proofs
lake build                            # first run: fetches Mathlib binary cache (~10 min)
```

## Regenerating the translation after changing the kernel

```bash
cd formal/kernel
cargo test                                            # differential test must pass
PATH=$PWD/../.toolchain/charon-bin:$PATH charon cargo --preset=aeneas
../.toolchain/aeneas -backend lean btree_kernel.llbc  # writes BtreeKernel.lean
cp BtreeKernel.lean ../proofs/                        # then: cd ../proofs && lake build
```

The generated `BtreeKernel.lean` must contain **zero `axiom`s** (an axiom
means an unmodeled std function slipped in) — `grep -c axiom` after
regenerating.

## Ground rules

- `proofs/BtreeKernel.lean` is generated; never hand-edit.
- Any change to the insert or delete path of `src/btree.rs` must be mirrored in
  `formal/kernel/src/lib.rs` (or explicitly noted as unverified drift). This is
  enforced automatically: `make formal/drift-check` (run in CI on every PR, see
  `.github/workflows/formal.yml`) fails if `src/btree.rs` changed without a
  matching `formal/` change. For a change outside the verified surface (e.g.
  range iterators, comments, an unrelated method), acknowledge it with
  `ACK_NO_FORMAL=1` locally, or `[skip-formal-drift]` in the PR title.
- No `sorry` may be committed; verify with `#print axioms` on the top-level
  theorems `btree_kernel.BTree.{insert,remove}_{inv,get,frame}` plus
  `BTree.remove_total` / `BTree.remove_spec` / `BTree.remove_minkeys` /
  `BTree.remove_balanced_spec` — only the three standard Lean
  axioms are acceptable. The scheduled `lean` CI job (weekly + `workflow_dispatch`)
  rebuilds the proofs and re-runs this check.
