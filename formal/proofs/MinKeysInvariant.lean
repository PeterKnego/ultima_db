/- Minimum-arity (balance) invariant for the B-tree, for proving `remove` TOTAL.

   `NodeInv`/`HeightInv` capture sortedness, alignment and height-uniformity but
   place NO lower bound on a node's entry count, so they admit a 0-entry internal
   node — on which `fix_underfull_child` indexes an empty entry vector and the
   kernel legitimately fails. That is exactly why the `remove_*` theorems in
   `RemoveInv`/`RemoveGet`/`RemoveFrame` are stated conditional on `= ok`.

   A real B-tree keeps every non-root node at `MIN_KEYS = 31` entries or more.
   Totality does not actually need the full `31` lower bound: `fix_underfull_child`
   returns `ok` given only that the node it repairs is nonempty and its children
   are correctly aligned (rotation only fires against a sibling with
   `> MIN_KEYS ≥ 1` entries; a merge only reads the separator). So we split the
   invariant in two:

   * `NE`  — every node in the subtree has `≥ 1` entry (the *consumed* fact);
   * `MinArity` — every node has `≥ MIN_KEYS = 31` entries (the honest B-tree
     invariant, used as the public hypothesis; `MinArity → NE`).

   Both are relaxed at the root (a root may legitimately hold fewer entries, even
   zero when the whole tree is one leaf): `NERoot` / `BalancedRoot`. -/
import BtreeKernel
import BtreeInvariant
import ChildrenSpecs

open Aeneas Aeneas.Std Result

namespace btree_kernel

/-! ## `NE` — every node in the subtree is nonempty (≥ 1 entry) -/

mutual
/-- Every node in the subtree rooted here has at least one entry. -/
inductive NE : Node → Prop where
  | mk : ∀ (entries : alloc.vec.Vec (Std.U64 × Std.U64)) (cs : Children),
      0 < entries.val.length → ChildrenNE cs → NE (Node.mk entries cs)

/-- Every node in the children cons-list satisfies `NE`. -/
inductive ChildrenNE : Children → Prop where
  | nil : ChildrenNE Children.Nil
  | cons : ∀ (c : Node) (cs : Children),
      NE c → ChildrenNE cs → ChildrenNE (Children.Cons c cs)
end

/-- List-model form of `ChildrenNE`. -/
def AllNE (ns : List Node) : Prop := ∀ n ∈ ns, NE n

/-! ### Destructors -/

theorem NE.entries_pos {n : Node} (h : NE n) : 0 < n.entries.val.length := by
  cases h with | mk entries cs hpos _ => exact hpos

theorem NE.children {n : Node} (h : NE n) : ChildrenNE n.children := by
  cases h with | mk entries cs _ hcs => exact hcs

/-! ### `ChildrenNE` ↔ `AllNE (clist ·)` bridge (mirrors `ChildrenHeight`) -/

theorem ChildrenNE.allNE {cs : Children} (h : ChildrenNE cs) : AllNE (clist cs) := by
  induction cs using children_ind with
  | nil => intro n hn; simp [clist] at hn
  | cons c rest ih =>
    cases h with
    | cons _ _ hc hrest =>
      intro n hn
      simp only [clist, List.mem_cons] at hn
      rcases hn with rfl | hn
      · exact hc
      · exact ih hrest n hn

theorem ChildrenNE.of_allNE {cs : Children} (hall : AllNE (clist cs)) :
    ChildrenNE cs := by
  induction cs using children_ind with
  | nil => exact ChildrenNE.nil
  | cons c rest ih =>
    refine ChildrenNE.cons c rest (hall c (by simp [clist])) (ih ?_)
    intro n hn
    exact hall n (by simp [clist]; right; exact hn)

theorem ChildrenNE_iff_allNE {cs : Children} :
    ChildrenNE cs ↔ AllNE (clist cs) :=
  ⟨ChildrenNE.allNE, ChildrenNE.of_allNE⟩

/-- The child at a valid index of a `ChildrenNE` list is itself `NE`. -/
theorem ChildrenNE.getElem {cs : Children} (h : ChildrenNE cs)
    (i : Nat) (hi : i < (clist cs).length) : NE ((clist cs)[i]'hi) :=
  h.allNE _ (List.getElem_mem hi)

/-! ## `NERoot` — the root-relaxed form -/

/-- A tree root is well-formed for totality if it is a leaf (possibly empty), or
    an internal node with ≥ 1 entry whose children are all `NE`. -/
def NERoot (n : Node) : Prop :=
  clist n.children = [] ∨ (0 < n.entries.val.length ∧ ChildrenNE n.children)

/-- `NE` (a non-root subtree) is in particular `NERoot`-well-formed, so a
    recursive call may be handed a child directly. -/
theorem NE.toNERoot {n : Node} (h : NE n) : NERoot n := by
  cases h with
  | mk entries cs hpos hcs =>
    exact Or.inr ⟨hpos, hcs⟩

/-- An internal `NERoot` node has ≥ 1 entry (the leaf disjunct is excluded). -/
theorem NERoot.entries_pos_of_internal {n : Node}
    (h : NERoot n) (hne : clist n.children ≠ []) : 0 < n.entries.val.length := by
  rcases h with hnil | ⟨hpos, _⟩
  · exact absurd hnil hne
  · exact hpos

/-- The children of an internal `NERoot` node are `NE`. -/
theorem NERoot.children_ne_of_internal {n : Node}
    (h : NERoot n) (hne : clist n.children ≠ []) : ChildrenNE n.children := by
  rcases h with hnil | ⟨_, hcs⟩
  · exact absurd hnil hne
  · exact hcs

/-! ## `MinArity` — the honest B-tree balance invariant (≥ MIN_KEYS = 31) -/

mutual
/-- Every node in the subtree has at least `MIN_KEYS = 31` entries. -/
inductive MinArity : Node → Prop where
  | mk : ∀ (entries : alloc.vec.Vec (Std.U64 × Std.U64)) (cs : Children),
      31 ≤ entries.val.length → ChildrenMinArity cs → MinArity (Node.mk entries cs)

/-- Every node in the children cons-list satisfies `MinArity`. -/
inductive ChildrenMinArity : Children → Prop where
  | nil : ChildrenMinArity Children.Nil
  | cons : ∀ (c : Node) (cs : Children),
      MinArity c → ChildrenMinArity cs → ChildrenMinArity (Children.Cons c cs)
end

/-- List-model form of `ChildrenMinArity`. -/
def AllMinArity (ns : List Node) : Prop := ∀ n ∈ ns, MinArity n

theorem ChildrenMinArity.allMinArity {cs : Children} (h : ChildrenMinArity cs) :
    AllMinArity (clist cs) := by
  induction cs using children_ind with
  | nil => intro n hn; simp [clist] at hn
  | cons c rest ih =>
    cases h with
    | cons _ _ hc hrest =>
      intro n hn
      simp only [clist, List.mem_cons] at hn
      rcases hn with rfl | hn
      · exact hc
      · exact ih hrest n hn

/-- `MinArity` (≥ 31) implies `NE` (≥ 1). Proved by induction on subtree size,
    since the mutual eliminator gives no induction hypothesis for a child. -/
private theorem MinArity_toNE_fuel (fuel : Nat) : ∀ (n : Node),
    Node.size n ≤ fuel → MinArity n → NE n := by
  induction fuel with
  | zero => intro n hsz _; exfalso; have := Node.one_le_size n; omega
  | succ fuel ih =>
    intro n hsz h
    cases h with
    | mk entries cs hlen hcs =>
      refine NE.mk entries cs (by omega) (ChildrenNE.of_allNE ?_)
      intro m hm
      have hszm : Node.size m ≤ Children.size cs := size_mem_clist cs m hm
      have hnsz : Node.size (Node.mk entries cs) = 1 + Children.size cs := by
        simp [Node.size]
      exact ih m (by omega) (hcs.allMinArity m hm)

theorem MinArity.toNE {n : Node} (h : MinArity n) : NE n :=
  MinArity_toNE_fuel (Node.size n) n (Nat.le_refl _) h

/-- The honest B-tree balance invariant, relaxed at the root. -/
def BalancedRoot (n : Node) : Prop :=
  clist n.children = [] ∨ (0 < n.entries.val.length ∧ ChildrenMinArity n.children)

theorem ChildrenMinArity.toChildrenNE {cs : Children}
    (h : ChildrenMinArity cs) : ChildrenNE cs := by
  induction cs using children_ind with
  | nil => exact ChildrenNE.nil
  | cons c rest ih =>
    cases h with
    | cons _ _ hc hrest => exact ChildrenNE.cons c rest hc.toNE (ih hrest)

theorem BalancedRoot.toNERoot {n : Node} (h : BalancedRoot n) : NERoot n := by
  rcases h with hnil | ⟨hpos, hcs⟩
  · exact Or.inl hnil
  · exact Or.inr ⟨hpos, hcs.toChildrenNE⟩

/-- Top-level minimum-arity invariant of a tree. -/
def MinKeysInv (t : BTree) : Prop := BalancedRoot t.root

end btree_kernel
