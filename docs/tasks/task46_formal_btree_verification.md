# Task 46: Formal verification of the B-tree insert path (Aeneas + Lean 4)

> Renumbered from task43 (numbering collided with the Unicode tokenizer, which
> is the canonical task43); formal verification is task46.


## Summary

Machine-checked proofs that UltimaDB's B-tree insertion is functionally
correct, verified with the Lean 4 kernel against a mechanical Rust→Lean
translation (Aeneas) of the insert/get core of `src/btree.rs`. Lives under
`formal/`; see `formal/README.md` for build/regeneration and
`formal/WRITEUP.md` for the full narrative.

## Theorems (all sorry-free; axioms = propext/Classical.choice/Quot.sound)

1. **`BTree.insert_inv`** — insert preserves the B-tree invariant: per-node
   strictly-sorted entries, children positionally aligned to key intervals
   (child i strictly between entry i−1 and entry i), arity ≤ MAX_KEYS = 63.
   Covers the 64-entry median split and root splits.
2. **`BTree.insert_get`** — `get k` after `insert k v` = `ok (some v)`,
   including the case where the inserted pair becomes a promoted median.
3. **`BTree.insert_frame`** — `get k'` unchanged for all `k' ≠ k`.

Together these say insert behaves exactly as a map update. The Aeneas
`Result` monad makes totality part of the statement: no panic, overflow, or
out-of-bounds indexing on these paths for invariant-satisfying trees.

## Architectural decisions

- **Verify a port, anchor it empirically.** Aeneas handles a safe-Rust
  subset, so `formal/kernel` ports the algorithm with documented deltas
  (Arc→Box since the persistent structure never mutates through the Arc;
  children as a cons-list since recursive types through `Vec` fail Lean's
  strict positivity; hand-written mutually-recursive clones since Charon
  rejects mixed recursive trait-impl groups; monomorphized `u64` keys
  matching `Table`'s `BTree<u64, R>`). A differential test vs
  `std::collections::BTreeMap` (2000 randomized inserts with splits) anchors
  behavioral equivalence — `make test/formal-kernel`.
- **Invariant as mutual inductive** (`NodeInv`/`Aligned` in
  `BtreeInvariant.lean`), with alignment stated over `List Node` (via a
  `clist` view) so all reassembly proofs are pure list surgery.
- **Proof stack**: helper spec modules (`FindPosSpec` = binary-search
  correctness, `EntrySpecs`/`ChildrenSpecs` = loop/recursion specs for every
  hand-rolled helper, `ListLemmas`/`AlignedLemmas`/`TransportLemmas` = pure
  lemmas) feed three fuel-induction theorems. Custom specs are
  `@[step]`-tagged so Aeneas's `step*` composes monadic chains automatically.
- **Kernel excluded from the cargo workspace** so Charon owns its build and
  the pinned nightly never interacts with workspace deps.

## Toolchain pins & gotchas

Aeneas `nightly-2026.07.06-45061fa`, Charon `nightly-2026.07.02` (must be the
commit in Aeneas's `charon-pin`; LLBC format is versioned and same-day
nightlies can mismatch), Rust `nightly-2026-06-01` for Charon, Lean
`v4.30.0-rc2`. All prebuilt via `formal/scripts/fetch-toolchain.sh` — no
OCaml build. Watch for `axiom`s in regenerated `BtreeKernel.lean` (unmodeled
std functions, e.g. `Vec::is_empty` — use `len() == 0`).

## Limitations / next steps

- `remove`/rebalancing unported and unverified (hairiest remaining path).
- Range iterators unverified.
- Concurrency (MultiWriter OCC merge, promotion ordering, WAL recovery) is
  out of Aeneas scope — needs hand-written protocol models.
- Drift discipline: changes to `src/btree.rs` insert logic must be mirrored
  in the kernel (see formal/README.md ground rules). Enforced by
  `make formal/drift-check` (`formal/scripts/check-drift.sh`), run on every PR
  via `.github/workflows/formal.yml`; the same workflow re-verifies the proofs
  (build + axiom check) weekly.
