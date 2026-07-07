# Task 46: Formal verification of the B-tree insert and delete paths (Aeneas + Lean 4)

> Renumbered from task43 (numbering collided with the Unicode tokenizer, which
> is the canonical task43); formal verification is task46.


## Summary

Machine-checked proofs that UltimaDB's B-tree insertion **and deletion** are
functionally correct, verified with the Lean 4 kernel against a mechanical
Rust→Lean translation (Aeneas) of the insert/get **and remove/rebalance** core
of `src/btree.rs`. Lives under `formal/`; see `formal/README.md` for
build/regeneration and `formal/WRITEUP.md` for the full narrative.

## Theorems (all sorry-free; axioms = propext/Classical.choice/Quot.sound)

1. **`BTree.insert_inv`** — insert preserves the B-tree invariant: per-node
   strictly-sorted entries, children positionally aligned to key intervals
   (child i strictly between entry i−1 and entry i), arity ≤ MAX_KEYS = 63.
   Covers the 64-entry median split and root splits.
2. **`BTree.insert_get`** — `get k` after `insert k v` = `ok (some v)`,
   including the case where the inserted pair becomes a promoted median.
3. **`BTree.insert_frame`** — `get k'` unchanged for all `k' ≠ k`.
4. **`BTree.remove_inv`** — remove preserves the ordering invariant **and**
   height-uniformity (`HeightInv`), through borrow/rotate, merge, and
   root-collapse rebalancing.
5. **`BTree.remove_get`** — `get k` after `remove k` = `ok none`.
6. **`BTree.remove_frame`** — `get k'` unchanged for all `k' ≠ k`.

Together (1–3) say insert behaves exactly as a map update, and (4–6) say remove
behaves exactly as a map deletion. The insert theorems are total (the Aeneas
`Result` monad makes no-panic/overflow/OOB part of the statement for
invariant-satisfying trees); the remove theorems are conditional on the kernel
returning `ok` (see "Remove-specific decisions").

## Remove-specific decisions

- **A height invariant is required, not optional.** Bare `NodeInv`/`Aligned`
  (sortedness + child-interval alignment + arity ≤ 63) permit a parent whose
  children have mixed leaf/internal status. `merge` on such siblings is
  malformed, so `remove_inv` over the bare invariant is *false*. `HeightInv`
  (`BalancedInvariant.lean`, a mutual inductive with `ChildrenHeight`) is
  carried alongside `NodeInv` through every remove proof; it is additive and
  does not touch the insert proofs.
- **Conditional on success, not total.** `NodeInv ∧ HeightInv` still admit
  pathological 0-entry internal nodes on which `fix_underfull_child` indexes an
  empty entry vector and `delete` legitimately fails. The theorems therefore
  read "if the kernel returns `ok`, the result is well-formed / behaves as a
  deletion." Totality would additionally need the MIN_KEYS lower-bound invariant
  (future work). The nonempty precondition of the rebalance lemmas is *derived
  from* that success (`fix_underfull_child_ok_pos`).
- **Lookups via an in-order `flatten`.** Rather than track `get` through each
  rebalancer, `RemoveFlatten.lean` proves `get_in_node` equals a lookup in the
  in-order flattened key list and that every rebalancer (rotate/merge/fix) is
  *flatten-invariant*. Then `remove_get` and `remove_frame` reduce to "delete
  drops exactly the removed key from the flattened list."

## Architectural decisions

- **Verify a port, anchor it empirically.** Aeneas handles a safe-Rust
  subset, so `formal/kernel` ports the algorithm with documented deltas
  (Arc→Box since the persistent structure never mutates through the Arc;
  children as a cons-list since recursive types through `Vec` fail Lean's
  strict positivity; hand-written mutually-recursive clones since Charon
  rejects mixed recursive trait-impl groups; monomorphized `u64` keys
  matching `Table`'s `BTree<u64, R>`). A differential test vs
  `std::collections::BTreeMap` (2000 randomized inserts with splits) anchors
  behavioral equivalence — `make test/formal-kernel`. The delete port adds a
  second differential test (3000 inserts + 8000 interleaved insert/remove ops +
  full drain to empty). The in-place `&mut` rebalancers are rendered as pure
  functions returning the repaired `(entries, children)` — observationally
  identical for the persistent structure.
- **Invariant as mutual inductive** (`NodeInv`/`Aligned` in
  `BtreeInvariant.lean`, `HeightInv`/`ChildrenHeight` in
  `BalancedInvariant.lean`), with alignment stated over `List Node` (via a
  `clist` view) so all reassembly proofs are pure list surgery.
- **Proof stack**: helper spec modules (`FindPosSpec` = binary-search
  correctness, `EntrySpecs`/`ChildrenSpecs`/`RemoveSpecs` = loop/recursion specs
  for every hand-rolled helper, `ListLemmas`/`AlignedLemmas`/`TransportLemmas` =
  pure lemmas) feed the fuel-induction theorems. Custom specs are
  `@[step]`-tagged so Aeneas's `step*` composes monadic chains automatically.
  The delete side adds `RemoveRebalance.lean` (rotate/merge surgery preserving
  `NodeInv ∧ HeightInv`, with `Aligned.append` — the merge/concatenation inverse
  of `Aligned.split`) and `RemoveFlatten.lean` (the in-order lookup foundation).
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

- Totality of `remove`: the theorems are conditional on the kernel returning
  `ok`. Proving `remove` never fails on a well-formed tree needs the MIN_KEYS
  lower-bound invariant (every non-root node ≥ T−1 entries) threaded through the
  rebalancers — the natural next strengthening.
- Range iterators unverified.
- Concurrency (MultiWriter OCC merge, promotion ordering, WAL recovery) is
  out of Aeneas scope — needs hand-written protocol models.
- Drift discipline: changes to `src/btree.rs` insert logic must be mirrored
  in the kernel (see formal/README.md ground rules). Enforced by
  `make formal/drift-check` (`formal/scripts/check-drift.sh`), run on every PR
  via `.github/workflows/formal.yml`; the same workflow re-verifies the proofs
  (build + axiom check) weekly.
