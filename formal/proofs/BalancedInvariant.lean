/- Height-uniformity invariant for the B-tree, complementing `NodeInv`.

   `NodeInv`/`Aligned` capture sortedness and child-interval alignment but do
   NOT force all leaves to sit at the same depth, so they would permit a parent
   with mixed leaf/internal children — on which `merge` produces a malformed
   node. `HeightInv h n` says the subtree `n` is perfectly height-balanced with
   height `h` (leaves at height 0); `ChildrenHeight h cs` says every child in
   the cons-list `cs` satisfies `HeightInv h`.

   The delete-path rebalancers (RemoveRebalance.lean) carry `HeightInv`
   alongside `NodeInv`. -/
import BtreeKernel
import BtreeInvariant
import ChildrenSpecs

open Aeneas Aeneas.Std Result

namespace btree_kernel

mutual
/-- The subtree rooted at the node is height-balanced with the given height.
    Leaves have height 0; an internal node has height `h + 1` where all its
    children have height `h`. -/
inductive HeightInv : Nat → Node → Prop where
  | leaf : ∀ (entries : alloc.vec.Vec (Std.U64 × Std.U64)),
      HeightInv 0 (Node.mk entries Children.Nil)
  | internal : ∀ {h} (entries : alloc.vec.Vec (Std.U64 × Std.U64))
      (c : Node) (cs : Children),
      ChildrenHeight h (Children.Cons c cs) →
      HeightInv (h + 1) (Node.mk entries (Children.Cons c cs))

/-- Every node in the children cons-list is balanced with height `h`. -/
inductive ChildrenHeight : Nat → Children → Prop where
  | nil : ∀ {h}, ChildrenHeight h Children.Nil
  | cons : ∀ {h} (c : Node) (cs : Children),
      HeightInv h c → ChildrenHeight h cs → ChildrenHeight h (Children.Cons c cs)
end

/-- List-model form of `ChildrenHeight`: all nodes in a plain list have
    height `h`. -/
def AllH (h : Nat) (ns : List Node) : Prop := ∀ n ∈ ns, HeightInv h n

/-! ### `clist` bridges -/

theorem clist_eq_nil_iff (c : Children) : clist c = [] ↔ c = Children.Nil := by
  cases c <;> simp [clist]

theorem ChildrenHeight.allH {h : Nat} {cs : Children}
    (hch : ChildrenHeight h cs) : AllH h (clist cs) := by
  induction cs using children_ind with
  | nil => intro n hn; simp [clist] at hn
  | cons c rest ih =>
    cases hch with
    | cons _ _ hc hrest =>
      intro n hn
      simp only [clist, List.mem_cons] at hn
      rcases hn with rfl | hn
      · exact hc
      · exact ih hrest n hn

theorem ChildrenHeight.of_allH {h : Nat} {cs : Children}
    (hall : AllH h (clist cs)) : ChildrenHeight h cs := by
  induction cs using children_ind with
  | nil => exact ChildrenHeight.nil
  | cons c rest ih =>
    refine ChildrenHeight.cons c rest (hall c (by simp [clist])) (ih ?_)
    intro n hn
    exact hall n (by simp [clist]; right; exact hn)

/-- The requested bridge: `ChildrenHeight` over the cons-list is exactly
    `HeightInv` for every member of the plain-list view. -/
theorem ChildrenHeight_iff_allH {h : Nat} {cs : Children} :
    ChildrenHeight h cs ↔ AllH h (clist cs) :=
  ⟨ChildrenHeight.allH, ChildrenHeight.of_allH⟩

/-! ### Inversion / introduction for `HeightInv` -/

theorem HeightInv.zero_nil {entries : alloc.vec.Vec (Std.U64 × Std.U64)}
    {cs : Children} (hh : HeightInv 0 (Node.mk entries cs)) :
    cs = Children.Nil := by
  cases hh; rfl

theorem HeightInv.children_ne_nil {h : Nat}
    {entries : alloc.vec.Vec (Std.U64 × Std.U64)} {cs : Children}
    (hh : HeightInv (h + 1) (Node.mk entries cs)) : clist cs ≠ [] := by
  cases hh with
  | internal _ c cs' _ => simp [clist]

theorem HeightInv.children_allH {h : Nat}
    {entries : alloc.vec.Vec (Std.U64 × Std.U64)} {cs : Children}
    (hh : HeightInv (h + 1) (Node.mk entries cs)) : AllH h (clist cs) := by
  cases hh with
  | internal _ c cs' hch => exact hch.allH

theorem HeightInv.mk_internal {h : Nat}
    (entries : alloc.vec.Vec (Std.U64 × Std.U64)) (cs : Children)
    (hne : clist cs ≠ []) (hall : AllH h (clist cs)) :
    HeightInv (h + 1) (Node.mk entries cs) := by
  cases cs with
  | Nil => simp [clist] at hne
  | Cons c cs' =>
    exact HeightInv.internal entries c cs' (ChildrenHeight.of_allH hall)

/-! ### `AllH` list surgery lemmas (++ / set / eraseIdx / take / drop) -/

theorem AllH.cons {h : Nat} {n : Node} {l : List Node}
    (hn : HeightInv h n) (hl : AllH h l) : AllH h (n :: l) := by
  intro m hm
  rcases List.mem_cons.mp hm with rfl | hm
  · exact hn
  · exact hl m hm

theorem AllH.append {h : Nat} {a b : List Node}
    (ha : AllH h a) (hb : AllH h b) : AllH h (a ++ b) := by
  intro n hn
  rcases List.mem_append.mp hn with hn | hn
  · exact ha n hn
  · exact hb n hn

theorem AllH.set {h : Nat} {l : List Node} (i : Nat) {n : Node}
    (hl : AllH h l) (hn : HeightInv h n) : AllH h (l.set i n) := by
  intro m hm
  rcases List.mem_or_eq_of_mem_set hm with hm | rfl
  · exact hl m hm
  · exact hn

theorem AllH.take {h : Nat} {l : List Node} (i : Nat)
    (hl : AllH h l) : AllH h (l.take i) :=
  fun n hn => hl n (List.mem_of_mem_take hn)

theorem AllH.drop {h : Nat} {l : List Node} (i : Nat)
    (hl : AllH h l) : AllH h (l.drop i) :=
  fun n hn => hl n (List.mem_of_mem_drop hn)

theorem AllH.eraseIdx {h : Nat} {l : List Node} (i : Nat)
    (hl : AllH h l) : AllH h (l.eraseIdx i) :=
  fun n hn => hl n ((l.eraseIdx_sublist i).mem hn)

theorem AllH.getElem {h : Nat} {l : List Node} (hl : AllH h l)
    (i : Nat) (hi : i < l.length) : HeightInv h (l[i]'hi) :=
  hl _ (List.getElem_mem hi)

end btree_kernel
