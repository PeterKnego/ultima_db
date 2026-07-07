# Formal verification of the B-tree insert path

Machine-checked (Lean 4) proofs about UltimaDB's copy-on-write B-tree
insertion algorithm, verified against a mechanical Rust→Lean translation
produced by [Aeneas](https://github.com/AeneasVerif/aeneas).

## What is proved

Over the translated kernel (`proofs/BtreeKernel.lean`, generated — never edit),
all sorry-free, `#print axioms` = `propext, Classical.choice, Quot.sound` only:

| Theorem (file) | Meaning |
|---|---|
| `BTree.insert_inv` (`BtreeInsertInv.lean`) | insert preserves the full B-tree ordering invariant (sorted entries, key-interval-aligned children, arity ≤ MAX_KEYS), incl. median splits |
| `BTree.insert_get` (`BtreeInsertGet.lean`) | `get k` after `insert k v` returns `some v` |
| `BTree.insert_frame` (`BtreeInsertFrame.lean`) | `get k'` is unchanged for every `k' ≠ k` |

Together: **insertion behaves exactly as a map update**. Because the Aeneas
translation is total-with-explicit-effects, the same theorems also rule out
panics, arithmetic overflow, and out-of-bounds indexing on the insert/get
paths for any tree satisfying the invariant.

Not yet covered: `remove`/rebalancing, range iterators, anything concurrent
(store/OCC/WAL — out of Aeneas scope; needs hand-written protocol models).

## Layout

- `kernel/` — the verification kernel: the insert/get core of `src/btree.rs`
  ported to the Aeneas-supported safe-Rust subset. Deltas from the real code
  are documented at the top of `kernel/src/lib.rs`; behavioral equivalence is
  anchored by a differential test against `std::collections::BTreeMap`
  (`cargo test --manifest-path formal/kernel/Cargo.toml`, or
  `make test/formal-kernel`). Excluded from the cargo workspace so Charon
  owns its build.
- `proofs/` — the lake package: `BtreeKernel.lean` (generated),
  `BtreeInvariant.lean` (invariant definitions), helper-lemma modules
  (`FindPosSpec`, `EntrySpecs`, `ChildrenSpecs`, `ListLemmas`,
  `AlignedLemmas`, `TransportLemmas`), and the theorem files.
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
- Any change to the insert path of `src/btree.rs` must be mirrored in
  `formal/kernel/src/lib.rs` (or explicitly noted as unverified drift). This is
  enforced automatically: `make formal/drift-check` (run in CI on every PR, see
  `.github/workflows/formal.yml`) fails if `src/btree.rs` changed without a
  matching `formal/` change. For a change outside the verified insert/get path
  (e.g. `remove`/rebalance, comments), acknowledge it with `ACK_NO_FORMAL=1`
  locally, or `[skip-formal-drift]` in the PR title.
- No `sorry` may be committed; verify with
  `#print axioms btree_kernel.BTree.insert_frame` (and `_inv`, `_get`) —
  only the three standard Lean axioms are acceptable. The scheduled `lean` CI
  job (weekly + `workflow_dispatch`) rebuilds the proofs and re-runs this check.
