/- PRESERVATION of the MIN_KEYS balance invariant under `remove`.

   `MinKeysInvariant.lean` proved `remove` TOTAL under `MinKeysInv` (every
   non-root node ≥ 31 entries). This file proves the complementary, arithmetic
   direction: `remove` PRESERVES `MinKeysInv` — the result is again balanced.

   The classic B-tree deletion argument. A delete drops one node to exactly
   `MIN_KEYS - 1 = 30`; the rotate/merge rebalancers restore it to ≥ 31. So the
   right post-condition of the recursion is the "almost-balanced" predicate

     `AlmostMinArity n := 30 ≤ #entries ∧ ChildrenMinArity n.children`

   — the returned subtree root may be underfull by one, but every *proper*
   descendant is ≥ 31, and the `underfull` flag exactly tracks whether the root
   itself dropped below 31 (`RemoveInv`/`RemoveTotal` already prove that half).

   Layering (each reuses the structural surgery in `RemoveRebalance.lean` and
   the helper specs, so nothing is reproved):
   * L0 — `MinArity` destructors, the `ChildrenMinArity ↔ AllMinArity` reverse
     bridge, and `AllMinArity` closure under the list surgeries.
   * L1 — `fix_underfull_child` / `maybe_fix` restore `ChildrenMinArity` and drop
     the parent's entry count by at most one.
   * L2 — `remove_leftmost` / `delete_from_node` return `AlmostMinArity`.
   * L3 — `BTree.remove` preserves `MinKeysInv` (root-relaxed + collapse). -/
import MinKeysInvariant
import RemoveRebalance
import RemoveTotalCore
import RemoveInv
import RemoveTotal

open Aeneas Aeneas.Std Result

namespace btree_kernel

/-! ## L0 — `MinArity` destructors + `AllMinArity` algebra -/

theorem MinArity.entries_ge {n : Node} (h : MinArity n) :
    31 ≤ n.entries.val.length := by
  cases h with | mk entries cs hlen _ => exact hlen

theorem MinArity.children {n : Node} (h : MinArity n) :
    ChildrenMinArity n.children := by
  cases h with | mk entries cs _ hcs => exact hcs

/-- The reverse of `ChildrenMinArity.allMinArity` (mirrors `ChildrenNE.of_allNE`). -/
theorem ChildrenMinArity.of_allMinArity {cs : Children} (hall : AllMinArity (clist cs)) :
    ChildrenMinArity cs := by
  induction cs using children_ind with
  | nil => exact ChildrenMinArity.nil
  | cons c rest ih =>
    refine ChildrenMinArity.cons c rest (hall c (by simp [clist])) (ih ?_)
    intro n hn
    exact hall n (by simp [clist]; right; exact hn)

theorem ChildrenMinArity_iff_allMinArity {cs : Children} :
    ChildrenMinArity cs ↔ AllMinArity (clist cs) :=
  ⟨ChildrenMinArity.allMinArity, ChildrenMinArity.of_allMinArity⟩

/-- `MinArity` reconstructor from the two component facts. -/
theorem MinArity.mk' {n : Node} (hlen : 31 ≤ n.entries.val.length)
    (hcs : ChildrenMinArity n.children) : MinArity n := by
  obtain ⟨entries, cs⟩ := n
  exact MinArity.mk entries cs hlen hcs

/-! ### `AllMinArity` closure under the list surgeries -/

theorem AllMinArity.mem {l : List Node} (h : AllMinArity l) {n : Node} (hn : n ∈ l) :
    MinArity n := h n hn

theorem AllMinArity.getElem? {l : List Node} (h : AllMinArity l) {i : Nat} {n : Node}
    (hn : l[i]? = some n) : MinArity n := h n (List.mem_of_getElem? hn)

theorem AllMinArity.take {l : List Node} (h : AllMinArity l) (k : Nat) :
    AllMinArity (l.take k) := fun n hn => h n (List.mem_of_mem_take hn)

theorem AllMinArity.drop {l : List Node} (h : AllMinArity l) (k : Nat) :
    AllMinArity (l.drop k) := fun n hn => h n (List.mem_of_mem_drop hn)

theorem AllMinArity.append {l₁ l₂ : List Node}
    (h₁ : AllMinArity l₁) (h₂ : AllMinArity l₂) : AllMinArity (l₁ ++ l₂) := by
  intro n hn
  rcases List.mem_append.mp hn with h | h
  · exact h₁ n h
  · exact h₂ n h

theorem AllMinArity.cons {a : Node} {l : List Node}
    (ha : MinArity a) (hl : AllMinArity l) : AllMinArity (a :: l) := by
  intro n hn
  rcases List.mem_cons.mp hn with rfl | h
  · exact ha
  · exact hl n h

theorem AllMinArity.set {l : List Node} (h : AllMinArity l) (i : Nat) {x : Node}
    (hx : MinArity x) : AllMinArity (l.set i x) := by
  intro n hn
  rcases List.mem_or_eq_of_mem_set hn with h' | rfl
  · exact h n h'
  · exact hx

theorem AllMinArity.eraseIdx {l : List Node} (h : AllMinArity l) (i : Nat) :
    AllMinArity (l.eraseIdx i) := by
  intro n hn
  exact h n (List.mem_of_mem_eraseIdx hn)

/-- Pointwise characterization: useful when the base list is *not* uniformly
    `MinArity` (the delete site has one underfull child) so the closure lemmas
    above don't apply, and we must reason position-by-position through the
    `set`/`eraseIdx` surgery. -/
theorem getElem?_some_lt {α : Type _} {l : List α} {j : Nat} {a : α}
    (h : l[j]? = some a) : j < l.length := by
  rcases Nat.lt_or_ge j l.length with hlt | hge
  · exact hlt
  · rw [List.getElem?_eq_none hge] at h; exact absurd h (by simp)

/-- Local copies of two `private` helpers from `RemoveInv.lean` (unpeel a
    monadic bind, discharge a WP triple against a known `= ok`). -/
theorem bind_ok_inv {α β : Type} {m : Result α} {k : α → Result β}
    {out : β} (h : (do let a ← m; k a) = ok out) :
    ∃ a, m = ok a ∧ k a = ok out := by
  cases m with
  | ok a => exact ⟨a, rfl, by rwa [bind_tc_ok] at h⟩
  | fail e => rw [bind_tc_fail] at h; exact absurd h (by simp)
  | div => rw [bind_tc_div] at h; exact absurd h (by simp)

theorem spec_elim {α : Type} {m : Result α} {p : α → Prop}
    (hspec : WP.spec m p) {v : α} (heq : m = ok v) : p v := by
  obtain ⟨w, hw, hp⟩ := WP.spec_imp_exists hspec
  rw [heq] at hw; cases hw; exact hp

theorem allMinArity_iff_getElem? {l : List Node} :
    AllMinArity l ↔ ∀ (j : Nat) (n : Node), l[j]? = some n → MinArity n := by
  constructor
  · intro h j n hjn; exact h n (List.mem_of_getElem? hjn)
  · intro h n hn
    obtain ⟨j, hj⟩ := List.mem_iff_getElem?.mp hn
    exact h j n hj

/-- Setting two positions of a list to `MinArity` nodes yields an `AllMinArity`
    list, provided every *other* position was already `MinArity`. (rotate: the
    two donor/receiver slots are overwritten; the underfull slot is one of them.) -/
theorem allMinArity_set2 {l : List Node} {p q : Nat} {x y : Node}
    (hpl : p < l.length) (hql : q < l.length)
    (hx : MinArity x) (hy : MinArity y)
    (hoth : ∀ (j : Nat) (n : Node), j ≠ p → j ≠ q → l[j]? = some n → MinArity n) :
    AllMinArity ((l.set p x).set q y) := by
  rw [allMinArity_iff_getElem?]
  intro j n hjn
  by_cases hjq : j = q
  · subst hjq
    rw [List.getElem?_set_self (by rw [List.length_set]; exact hql), Option.some.injEq] at hjn
    subst hjn; exact hy
  · rw [List.getElem?_set_ne (Ne.symm hjq)] at hjn
    by_cases hjp : j = p
    · subst hjp
      rw [List.getElem?_set_self hpl, Option.some.injEq] at hjn
      subst hjn; exact hx
    · rw [List.getElem?_set_ne (Ne.symm hjp)] at hjn
      exact hoth j n hjp hjq hjn

/-- Setting position `p` to a `MinArity` node then erasing position `q ≠ p`
    yields `AllMinArity`, provided every position other than `q` was already
    `MinArity`. (merge: slot `p` becomes the merged node, the underfull slot `q`
    is removed.) -/
theorem allMinArity_set_eraseIdx {l : List Node} {p q : Nat} {x : Node}
    (hpl : p < l.length)
    (hx : MinArity x)
    (hoth : ∀ (j : Nat) (n : Node), j ≠ p → j ≠ q → l[j]? = some n → MinArity n) :
    AllMinArity ((l.set p x).eraseIdx q) := by
  rw [allMinArity_iff_getElem?]
  intro j n hjn
  rw [List.getElem?_eraseIdx] at hjn
  by_cases hjq : j < q
  · rw [if_pos hjq] at hjn
    by_cases hjp : j = p
    · subst hjp
      rw [List.getElem?_set_self hpl, Option.some.injEq] at hjn
      subst hjn; exact hx
    · rw [List.getElem?_set_ne (Ne.symm hjp)] at hjn
      exact hoth j n hjp (by omega) hjn
  · rw [if_neg hjq] at hjn
    by_cases hjp : j + 1 = p
    · subst hjp
      rw [List.getElem?_set_self hpl, Option.some.injEq] at hjn
      subst hjn; exact hx
    · rw [List.getElem?_set_ne (Ne.symm hjp)] at hjn
      exact hoth (j + 1) n hjp (by omega) hjn

/-! ## `AlmostMinArity` — the delete recursion's post-condition -/

/-- The returned subtree root may be underfull by one (`≥ 30`), but every proper
    descendant is `MinArity` (`≥ 31`). -/
def AlmostMinArity (n : Node) : Prop :=
  30 ≤ n.entries.val.length ∧ ChildrenMinArity n.children

theorem AlmostMinArity.of_minArity {n : Node} (h : MinArity n) : AlmostMinArity n :=
  ⟨by have := h.entries_ge; omega, h.children⟩

/-- Not-underfull (`≥ 31`) + `AlmostMinArity` upgrades to `MinArity`. -/
theorem AlmostMinArity.toMinArity {n : Node} (h : AlmostMinArity n)
    (hge : 31 ≤ n.entries.val.length) : MinArity n :=
  MinArity.mk' hge h.2

/-- A `MinArity` node is `BalancedRoot`-well-formed (either a leaf, or an
    internal node with ≥ 1 entry and `MinArity` children). -/
theorem MinArity.toBalancedRoot {n : Node} (h : MinArity n) : BalancedRoot n := by
  by_cases hc : clist n.children = []
  · exact Or.inl hc
  · exact Or.inr ⟨by have := h.entries_ge; omega, h.children⟩

/-! ## L1 — the rebalancers restore `ChildrenMinArity` -/

/-- `rotate_right`: the donor (child `idx-1`, `≥ 32` entries) lends one to the
    receiver (child `idx`, `≥ 30`); both come out `≥ 31` and all other children
    are untouched. The parent's entry count is preserved (a `replace_entry`).
    Capacity caps mirror `rotate_right_total`. -/
theorem rotate_right_minarity
    (entries : alloc.vec.Vec (Std.U64 × Std.U64)) (children : Children) (idx : Std.Usize)
    (halign : (clist children).length = entries.val.length + 1)
    (hidx0 : 0 < idx.val) (hidx : idx.val ≤ entries.val.length)
    (hins : ∀ n, (clist children)[idx.val]? = some n → n.entries.val.length < Std.Usize.max)
    (hgcl : ∀ n, (clist children)[idx.val - 1]? = some n →
      (clist n.children).length ≤ Std.Usize.max)
    (hdon : ∀ n, (clist children)[idx.val - 1]? = some n →
      32 ≤ n.entries.val.length ∧ ChildrenMinArity n.children)
    (hrec : ∀ n, (clist children)[idx.val]? = some n → AlmostMinArity n)
    (hoth : ∀ (j : Nat) (n : Node), j ≠ idx.val - 1 → j ≠ idx.val →
      (clist children)[j]? = some n → MinArity n) :
    rotate_right entries children idx ⦃ out =>
      out.1.val.length = entries.val.length ∧ ChildrenMinArity out.2 ⦄ := by
  have hchlt : idx.val < (clist children).length := by omega
  have hchlt' : idx.val - 1 < (clist children).length := by omega
  rw [rotate_right]
  obtain ⟨i, hisub, hivp⟩ := WP.spec_imp_exists
    (Usize.sub_spec (x := idx) (y := 1#usize) (by scalar_tac))
  have hiv : i.val = idx.val - 1 := by scalar_tac
  obtain ⟨left, hleft, hleftv⟩ := WP.spec_imp_exists
    (child_at_spec children i (by omega))
  simp only [hiv] at hleftv
  obtain ⟨right, hright, hrightv⟩ := WP.spec_imp_exists
    (child_at_spec children idx hchlt)
  -- MinArity facts on the two involved children
  have hleft? : (clist children)[idx.val - 1]? = some left := by
    rw [List.getElem?_eq_getElem hchlt', hleftv]
  have hright? : (clist children)[idx.val]? = some right := by
    rw [List.getElem?_eq_getElem hchlt, hrightv]
  obtain ⟨hdlen, hdch⟩ := hdon left hleft?
  obtain ⟨hrlen, hrch⟩ := hrec right hright?
  have hll0 : 0 < left.entries.val.length := by omega
  have hrcap : right.entries.val.length < Std.Usize.max := hins right hright?
  have hgccap : (clist left.children).length ≤ Std.Usize.max := hgcl left hleft?
  obtain ⟨i1, hi1, hi1p⟩ := WP.spec_imp_exists
    (Usize.sub_spec (x := alloc.vec.Vec.len left.entries) (y := 1#usize)
      (by have := alloc.vec.Vec.len_val left.entries; scalar_tac))
  have hi1v : i1.val = left.entries.val.length - 1 := by
    have := alloc.vec.Vec.len_val left.entries; scalar_tac
  obtain ⟨stolen, hstol, hstolv⟩ := WP.spec_imp_exists
    (alloc.vec.Vec.index_usize_spec left.entries i1 (by scalar_tac))
  obtain ⟨separator, hsep, hsepv⟩ := WP.spec_imp_exists
    (alloc.vec.Vec.index_usize_spec entries i (by scalar_tac))
  obtain ⟨new_entries, hne', hnev⟩ := WP.spec_imp_exists
    (replace_entry_spec entries i stolen)
  simp only [hiv] at hnev
  obtain ⟨nle, hnle', hnlev⟩ := WP.spec_imp_exists
    (entries_prefix_spec left.entries i1 (by scalar_tac))
  simp only [hi1v] at hnlev
  obtain ⟨nre, hnre', hnrev⟩ := WP.spec_imp_exists
    (insert_entry_at_spec right.entries 0#usize separator (by scalar_tac) (by scalar_tac))
  have hnrev' : nre.val = separator :: right.entries.val := by simpa using hnrev
  obtain ⟨nlc, hnlc', hnlcv⟩ := WP.spec_imp_exists
    (drop_last_child_spec left.children hgccap)
  obtain ⟨moved, hmv', hmvv⟩ := WP.spec_imp_exists
    (last_child_singleton_spec left.children hgccap)
  obtain ⟨nrc, hnrc', hnrcv⟩ := WP.spec_imp_exists
    (concat_children_spec moved right.children)
  rw [hmvv] at hnrcv
  obtain ⟨children1, hch1, hch1v⟩ := WP.spec_imp_exists
    (replace_child_spec children i (Node.mk nle nlc))
  simp only [hiv] at hch1v
  obtain ⟨children2, hch2, hch2v⟩ := WP.spec_imp_exists
    (replace_child_spec children1 idx (Node.mk nre nrc))
  rw [hch1v] at hch2v
  simp only [alloc.vec.Vec.index_slice_index, hisub, hleft, hright, hi1, hstol,
    hsep, hne', hnle', hnre', hnlc', hmv', hnrc', hch1, hch2, bind_tc_ok, WP.spec_ok]
  -- the two rebuilt siblings are MinArity
  have hNL : MinArity (Node.mk nle nlc) := by
    refine MinArity.mk' ?_ ?_
    · simp only [Node.entries._simpLemma_, hnlev, List.length_take]; omega
    · refine ChildrenMinArity.of_allMinArity ?_
      simp only [Node.children._simpLemma_, hnlcv]
      exact (hdch.allMinArity).take _
  have hNR : MinArity (Node.mk nre nrc) := by
    refine MinArity.mk' ?_ ?_
    · simp only [Node.entries._simpLemma_, hnrev', List.length_cons]; omega
    · refine ChildrenMinArity.of_allMinArity ?_
      simp only [Node.children._simpLemma_, hnrcv]
      exact (hdch.allMinArity.drop _).append hrch.allMinArity
  refine ⟨?_, ?_⟩
  · simp only [hnev, List.length_set]
  · refine ChildrenMinArity.of_allMinArity ?_
    rw [hch2v]
    refine allMinArity_set2 (by omega) (by omega) hNL hNR ?_
    intro j n hjp hjq
    have hj' : j ≠ idx.val - 1 := by simpa [hiv] using hjp
    exact hoth j n hj' hjq

/-- `rotate_left`: the donor (child `idx+1`, `≥ 32`) lends one to the receiver
    (child `idx`, `≥ 30`). Parent entry count preserved. -/
theorem rotate_left_minarity
    (entries : alloc.vec.Vec (Std.U64 × Std.U64)) (children : Children) (idx : Std.Usize)
    (halign : (clist children).length = entries.val.length + 1)
    (hidx : idx.val < entries.val.length)
    (hins : ∀ n, (clist children)[idx.val]? = some n → n.entries.val.length < Std.Usize.max)
    (hdon : ∀ n, (clist children)[idx.val + 1]? = some n →
      32 ≤ n.entries.val.length ∧ ChildrenMinArity n.children)
    (hrec : ∀ n, (clist children)[idx.val]? = some n → AlmostMinArity n)
    (hoth : ∀ (j : Nat) (n : Node), j ≠ idx.val → j ≠ idx.val + 1 →
      (clist children)[j]? = some n → MinArity n) :
    rotate_left entries children idx ⦃ out =>
      out.1.val.length = entries.val.length ∧ ChildrenMinArity out.2 ⦄ := by
  have hchlt : idx.val < (clist children).length := by omega
  have hchlt1 : idx.val + 1 < (clist children).length := by omega
  rw [rotate_left]
  obtain ⟨left, hleft, hleftv⟩ := WP.spec_imp_exists
    (child_at_spec children idx hchlt)
  obtain ⟨i, hiadd, hivp⟩ := WP.spec_imp_exists
    (Usize.add_spec (x := idx) (y := 1#usize) (by scalar_tac))
  have hiv : i.val = idx.val + 1 := by scalar_tac
  obtain ⟨right, hright, hrightv⟩ := WP.spec_imp_exists
    (child_at_spec children i (by omega))
  simp only [hiv] at hrightv
  have hleft? : (clist children)[idx.val]? = some left := by
    rw [List.getElem?_eq_getElem hchlt, hleftv]
  have hright? : (clist children)[idx.val + 1]? = some right := by
    rw [List.getElem?_eq_getElem hchlt1, hrightv]
  obtain ⟨hdlen, hdch⟩ := hdon right hright?
  obtain ⟨hrlen, hrch⟩ := hrec left hleft?
  have hlcap : left.entries.val.length < Std.Usize.max := hins left hleft?
  obtain ⟨stolen, hstol, hstolv⟩ := WP.spec_imp_exists
    (alloc.vec.Vec.index_usize_spec right.entries 0#usize (by scalar_tac))
  obtain ⟨separator, hsep, hsepv⟩ := WP.spec_imp_exists
    (alloc.vec.Vec.index_usize_spec entries idx (by scalar_tac))
  obtain ⟨new_entries, hnee, hnev⟩ := WP.spec_imp_exists
    (replace_entry_spec entries idx stolen)
  obtain ⟨nre, hnree, hnrev⟩ := WP.spec_imp_exists
    (entries_suffix_spec right.entries 1#usize)
  obtain ⟨nle, hnlee, hnlev⟩ := WP.spec_imp_exists
    (insert_entry_at_spec left.entries (alloc.vec.Vec.len left.entries) separator
      (by have := alloc.vec.Vec.len_val left.entries; scalar_tac)
      (by have := alloc.vec.Vec.len_val left.entries; scalar_tac))
  obtain ⟨moved, hmve, hmvv⟩ := WP.spec_imp_exists
    (children_prefix_spec right.children 1#usize)
  obtain ⟨nlc, hnlce, hnlcv⟩ := WP.spec_imp_exists
    (concat_children_spec left.children moved)
  rw [hmvv] at hnlcv
  obtain ⟨nrc, hnrce, hnrcv⟩ := WP.spec_imp_exists
    (children_suffix_spec right.children 1#usize)
  obtain ⟨children1, hch1, hch1v⟩ := WP.spec_imp_exists
    (replace_child_spec children idx (Node.mk nle nlc))
  obtain ⟨children2, hch2, hch2v⟩ := WP.spec_imp_exists
    (replace_child_spec children1 i (Node.mk nre nrc))
  rw [hch1v] at hch2v
  simp only [hiv] at hch2v
  simp only [alloc.vec.Vec.index_slice_index, hleft, hiadd, hright, hstol,
    hsep, hnee, hnree, hnlee, hmve, hnlce, hnrce, hch1, hch2, bind_tc_ok, WP.spec_ok]
  have hlenv : (alloc.vec.Vec.len left.entries).val = left.entries.val.length :=
    alloc.vec.Vec.len_val left.entries
  have hNL : MinArity (Node.mk nle nlc) := by
    refine MinArity.mk' ?_ ?_
    · simp only [Node.entries._simpLemma_, hnlev, hlenv, List.take_length, List.drop_length,
        List.append_nil, List.length_append, List.length_cons, List.length_nil]; omega
    · refine ChildrenMinArity.of_allMinArity ?_
      simp only [Node.children._simpLemma_, hnlcv]
      exact hrch.allMinArity.append (hdch.allMinArity.take _)
  have hNR : MinArity (Node.mk nre nrc) := by
    refine MinArity.mk' ?_ ?_
    · simp only [Node.entries._simpLemma_, hnrev, List.length_drop]; omega
    · refine ChildrenMinArity.of_allMinArity ?_
      simp only [Node.children._simpLemma_, hnrcv]
      exact hdch.allMinArity.drop _
  refine ⟨?_, ?_⟩
  · simp only [hnev, List.length_set]
  · refine ChildrenMinArity.of_allMinArity ?_
    rw [hch2v]
    exact allMinArity_set2 (by omega) (by omega) hNL hNR hoth

/-- `merge_with_left`: fuse child `idx` (`≥ 30`) into its left sibling (child
    `idx-1`, `≥ 31`) with the separator; result `≥ 62`. Parent loses one entry. -/
theorem merge_with_left_minarity
    (entries : alloc.vec.Vec (Std.U64 × Std.U64)) (children : Children) (idx : Std.Usize)
    (halign : (clist children).length = entries.val.length + 1)
    (hidx0 : 0 < idx.val) (hidx : idx.val ≤ entries.val.length)
    (hml : ∀ nl nr, (clist children)[idx.val - 1]? = some nl →
      (clist children)[idx.val]? = some nr →
      nl.entries.val.length + 1 + nr.entries.val.length ≤ Std.Usize.max)
    (hsib : ∀ n, (clist children)[idx.val - 1]? = some n →
      31 ≤ n.entries.val.length ∧ ChildrenMinArity n.children)
    (hrec : ∀ n, (clist children)[idx.val]? = some n → AlmostMinArity n)
    (hoth : ∀ (j : Nat) (n : Node), j ≠ idx.val - 1 → j ≠ idx.val →
      (clist children)[j]? = some n → MinArity n) :
    merge_with_left entries children idx ⦃ out =>
      out.1.val.length = entries.val.length - 1 ∧ ChildrenMinArity out.2 ⦄ := by
  have hchlt : idx.val < (clist children).length := by omega
  have hchlt' : idx.val - 1 < (clist children).length := by omega
  rw [merge_with_left]
  obtain ⟨i, hisub, hivp⟩ := WP.spec_imp_exists
    (Usize.sub_spec (x := idx) (y := 1#usize) (by scalar_tac))
  have hiv : i.val = idx.val - 1 := by scalar_tac
  obtain ⟨separator, hsep, hsepv⟩ := WP.spec_imp_exists
    (alloc.vec.Vec.index_usize_spec entries i (by scalar_tac))
  obtain ⟨left, hleft, hleftv⟩ := WP.spec_imp_exists
    (child_at_spec children i (by omega))
  simp only [hiv] at hleftv
  obtain ⟨right, hright, hrightv⟩ := WP.spec_imp_exists
    (child_at_spec children idx hchlt)
  have hleft? : (clist children)[idx.val - 1]? = some left := by
    rw [List.getElem?_eq_getElem hchlt', hleftv]
  have hright? : (clist children)[idx.val]? = some right := by
    rw [List.getElem?_eq_getElem hchlt, hrightv]
  obtain ⟨hllen, hlch⟩ := hsib left hleft?
  obtain ⟨hrlen, hrch⟩ := hrec right hright?
  have hcap : left.entries.val.length + 1 + right.entries.val.length ≤ Std.Usize.max :=
    hml left right hleft? hright?
  obtain ⟨me, hmee, hmev⟩ := WP.spec_imp_exists
    (merge_entries_spec left.entries separator right.entries hcap)
  obtain ⟨mc, hmce, hmcv⟩ := WP.spec_imp_exists
    (concat_children_spec left.children right.children)
  obtain ⟨new_entries, hnee, hnev⟩ := WP.spec_imp_exists
    (remove_entry_at_spec entries i)
  obtain ⟨children1, hch1, hch1v⟩ := WP.spec_imp_exists
    (replace_child_spec children i (Node.mk me mc))
  simp only [hiv] at hch1v
  obtain ⟨children2, hch2, hch2v⟩ := WP.spec_imp_exists
    (remove_child_at_spec children1 idx)
  rw [hch1v] at hch2v
  simp only [alloc.vec.Vec.index_slice_index, hisub, hsep, hleft, hright, hmee,
    hmce, hnee, hch1, hch2, bind_tc_ok, WP.spec_ok]
  have hME : MinArity (Node.mk me mc) := by
    refine MinArity.mk' ?_ ?_
    · simp only [Node.entries._simpLemma_, hmev, List.length_append, List.length_cons]; omega
    · refine ChildrenMinArity.of_allMinArity ?_
      simp only [Node.children._simpLemma_, hmcv]
      exact hlch.allMinArity.append hrch.allMinArity
  refine ⟨?_, ?_⟩
  · rw [hnev, List.length_eraseIdx]
    have hi_lt : i.val < entries.val.length := by rw [hiv]; omega
    simp [hi_lt]
  · refine ChildrenMinArity.of_allMinArity ?_
    rw [hch2v]
    refine allMinArity_set_eraseIdx (by omega) hME ?_
    intro j n hjp hjq
    have hj' : j ≠ idx.val - 1 := by simpa [hiv] using hjp
    exact hoth j n hj' hjq

/-- `merge_with_right`: fuse child `idx` (`≥ 30`) into its right sibling (child
    `idx+1`, `≥ 31`) with the separator. Parent loses one entry. -/
theorem merge_with_right_minarity
    (entries : alloc.vec.Vec (Std.U64 × Std.U64)) (children : Children) (idx : Std.Usize)
    (halign : (clist children).length = entries.val.length + 1)
    (hidx : idx.val < entries.val.length)
    (hmr : ∀ nl nr, (clist children)[idx.val]? = some nl →
      (clist children)[idx.val + 1]? = some nr →
      nl.entries.val.length + 1 + nr.entries.val.length ≤ Std.Usize.max)
    (hrec : ∀ n, (clist children)[idx.val]? = some n → AlmostMinArity n)
    (hsib : ∀ n, (clist children)[idx.val + 1]? = some n →
      31 ≤ n.entries.val.length ∧ ChildrenMinArity n.children)
    (hoth : ∀ (j : Nat) (n : Node), j ≠ idx.val → j ≠ idx.val + 1 →
      (clist children)[j]? = some n → MinArity n) :
    merge_with_right entries children idx ⦃ out =>
      out.1.val.length = entries.val.length - 1 ∧ ChildrenMinArity out.2 ⦄ := by
  have hchlt : idx.val < (clist children).length := by omega
  have hchlt1 : idx.val + 1 < (clist children).length := by omega
  rw [merge_with_right]
  obtain ⟨separator, hsep, hsepv⟩ := WP.spec_imp_exists
    (alloc.vec.Vec.index_usize_spec entries idx (by scalar_tac))
  obtain ⟨left, hleft, hleftv⟩ := WP.spec_imp_exists
    (child_at_spec children idx hchlt)
  obtain ⟨i, hiadd, hivp⟩ := WP.spec_imp_exists
    (Usize.add_spec (x := idx) (y := 1#usize) (by scalar_tac))
  have hiv : i.val = idx.val + 1 := by scalar_tac
  obtain ⟨right, hright, hrightv⟩ := WP.spec_imp_exists
    (child_at_spec children i (by omega))
  simp only [hiv] at hrightv
  have hleft? : (clist children)[idx.val]? = some left := by
    rw [List.getElem?_eq_getElem hchlt, hleftv]
  have hright? : (clist children)[idx.val + 1]? = some right := by
    rw [List.getElem?_eq_getElem hchlt1, hrightv]
  obtain ⟨hllen, hlch⟩ := hrec left hleft?
  obtain ⟨hrlen, hrch⟩ := hsib right hright?
  have hcap : left.entries.val.length + 1 + right.entries.val.length ≤ Std.Usize.max :=
    hmr left right hleft? hright?
  obtain ⟨me, hmee, hmev⟩ := WP.spec_imp_exists
    (merge_entries_spec left.entries separator right.entries hcap)
  obtain ⟨mc, hmce, hmcv⟩ := WP.spec_imp_exists
    (concat_children_spec left.children right.children)
  obtain ⟨new_entries, hnee, hnev⟩ := WP.spec_imp_exists
    (remove_entry_at_spec entries idx)
  obtain ⟨children1, hch1, hch1v⟩ := WP.spec_imp_exists
    (replace_child_spec children idx (Node.mk me mc))
  obtain ⟨children2, hch2, hch2v⟩ := WP.spec_imp_exists
    (remove_child_at_spec children1 i)
  rw [hch1v] at hch2v
  simp only [hiv] at hch2v
  simp only [alloc.vec.Vec.index_slice_index, hsep, hleft, hiadd, hright, hmee,
    hmce, hnee, hch1, hch2, bind_tc_ok, WP.spec_ok]
  have hME : MinArity (Node.mk me mc) := by
    refine MinArity.mk' ?_ ?_
    · simp only [Node.entries._simpLemma_, hmev, List.length_append, List.length_cons]; omega
    · refine ChildrenMinArity.of_allMinArity ?_
      simp only [Node.children._simpLemma_, hmcv]
      exact hlch.allMinArity.append hrch.allMinArity
  refine ⟨?_, ?_⟩
  · rw [hnev, List.length_eraseIdx]; simp [hidx]
  · refine ChildrenMinArity.of_allMinArity ?_
    rw [hch2v]
    exact allMinArity_set_eraseIdx (by omega) hME hoth

/-- `fix_underfull_child` restores `ChildrenMinArity` (all children back to
    `≥ 31`) and drops the parent's entry count by at most one — the delete-site
    dispatcher. Every non-`idx` child is `MinArity` (`hother`); child `idx` is
    the recursively-deleted, possibly-underfull one (`hidxc`, `≥ 30`). The
    branch structure and capacity discharges mirror `fix_underfull_child_total`;
    each leaf calls the matching `*_minarity` lemma. -/
theorem fix_underfull_child_minarity
    (entries : alloc.vec.Vec (Std.U64 × Std.U64)) (children : Children) (idx : Std.Usize)
    (halign : (clist children).length = entries.val.length + 1)
    (hpar63 : entries.val.length ≤ 63)
    (hnz : 0 < entries.val.length)
    (hidx : idx.val ≤ entries.val.length)
    (hcap63 : ∀ (j : Nat) (n : Node), (clist children)[j]? = some n →
      n.entries.val.length ≤ 63)
    (hcapC : ∀ (j : Nat) (n : Node), (clist children)[j]? = some n →
      (clist n.children).length ≤ 64)
    (hother : ∀ (j : Nat) (n : Node), j ≠ idx.val →
      (clist children)[j]? = some n → MinArity n)
    (hidxc : ∀ n, (clist children)[idx.val]? = some n → AlmostMinArity n) :
    fix_underfull_child entries children idx ⦃ out =>
      entries.val.length - 1 ≤ out.1.val.length ∧ ChildrenMinArity out.2 ⦄ := by
  have hclen := halign
  have hchE : ∀ (j : Nat), j ≤ entries.val.length → ∀ m,
      (clist children)[j]? = some m → m.entries.val.length ≤ 63 :=
    fun j _ m hm => hcap63 j m hm
  have hchC : ∀ (j : Nat), j ≤ entries.val.length → ∀ m,
      (clist children)[j]? = some m → (clist m.children).length ≤ 64 :=
    fun j _ m hm => hcapC j m hm
  -- `hother` restated to exclude a second slot (for the rebalancer `hoth`)
  have hoth2r : ∀ (j : Nat) (n : Node), j ≠ idx.val - 1 → j ≠ idx.val →
      (clist children)[j]? = some n → MinArity n := fun j n _ hj2 hn => hother j n hj2 hn
  have hoth2l : ∀ (j : Nat) (n : Node), j ≠ idx.val → j ≠ idx.val + 1 →
      (clist children)[j]? = some n → MinArity n := fun j n hj1 _ hn => hother j n hj1 hn
  rw [fix_underfull_child]
  obtain ⟨n, hne, hnv⟩ := WP.spec_imp_exists (children_len_spec children (by rw [hclen]; scalar_tac))
  simp only [hne, bind_tc_ok]
  split
  · rename_i hpos
    have hidx0 : 0 < idx.val := by scalar_tac
    obtain ⟨i, hisub, hivp⟩ := WP.spec_imp_exists
      (Usize.sub_spec (x := idx) (y := 1#usize) (by scalar_tac))
    have hiv : i.val = idx.val - 1 := by scalar_tac
    obtain ⟨n1, hn1e, hn1v⟩ := WP.spec_imp_exists (child_at_spec children i (by omega))
    simp only [hiv] at hn1v
    obtain ⟨i2, hmk, hmkv⟩ := WP.spec_imp_exists MIN_KEYS_spec
    simp only [hisub, hn1e, hmk, bind_tc_ok]
    split
    · rename_i hgt
      have hn1len : 31 < n1.entries.val.length := by
        have := alloc.vec.Vec.len_val n1.entries; scalar_tac
      have hdon : ∀ m, (clist children)[idx.val - 1]? = some m →
          32 ≤ m.entries.val.length ∧ ChildrenMinArity m.children := by
        intro m hm
        have hmn : m = n1 := by
          rw [List.getElem?_eq_getElem (show idx.val - 1 < (clist children).length by omega)] at hm
          rw [hn1v]; exact (Option.some.inj hm).symm
        exact ⟨by rw [hmn]; omega, (hother (idx.val - 1) m (by omega) hm).children⟩
      apply WP.spec_mono (rotate_right_minarity entries children idx hclen hidx0 hidx
        (fun m hm => by have := hchE idx.val hidx m hm; scalar_tac)
        (fun m hm => by have := hchC (idx.val - 1) (by omega) m hm; scalar_tac)
        hdon hidxc hoth2r)
      rintro out ⟨h1, h2⟩; exact ⟨by omega, h2⟩
    · rename_i hngt
      obtain ⟨i3, hi3a, hi3p⟩ := WP.spec_imp_exists
        (Usize.add_spec (x := idx) (y := 1#usize) (by scalar_tac))
      have hi3v : i3.val = idx.val + 1 := by scalar_tac
      simp only [hi3a, bind_tc_ok]
      split
      · rename_i hlt3
        have hidxlt : idx.val < entries.val.length := by scalar_tac
        obtain ⟨n2, hn2e, hn2v⟩ := WP.spec_imp_exists (child_at_spec children i3 (by omega))
        simp only [hi3v] at hn2v
        simp only [hn2e, bind_tc_ok]
        split
        · rename_i hgt4
          have hn2len : 31 < n2.entries.val.length := by
            have := alloc.vec.Vec.len_val n2.entries; scalar_tac
          have hdon : ∀ m, (clist children)[idx.val + 1]? = some m →
              32 ≤ m.entries.val.length ∧ ChildrenMinArity m.children := by
            intro m hm
            have hmn : m = n2 := by
              rw [List.getElem?_eq_getElem (show idx.val + 1 < (clist children).length by omega)] at hm
              rw [hn2v]; exact (Option.some.inj hm).symm
            exact ⟨by rw [hmn]; omega, (hother (idx.val + 1) m (by omega) hm).children⟩
          apply WP.spec_mono (rotate_left_minarity entries children idx hclen hidxlt
            (fun m hm => by have := hchE idx.val hidx m hm; scalar_tac)
            hdon hidxc hoth2l)
          rintro out ⟨h1, h2⟩; exact ⟨by omega, h2⟩
        · rename_i hngt4
          have hsib : ∀ m, (clist children)[idx.val - 1]? = some m →
              31 ≤ m.entries.val.length ∧ ChildrenMinArity m.children := by
            intro m hm
            have := hother (idx.val - 1) m (by omega) hm
            exact ⟨this.entries_ge, this.children⟩
          apply WP.spec_mono (merge_with_left_minarity entries children idx hclen hidx0 hidx
            (fun nl nr hnl hnr => by
              have := hchE (idx.val - 1) (by omega) nl hnl
              have := hchE idx.val hidx nr hnr; scalar_tac)
            hsib hidxc hoth2r)
          rintro out ⟨h1, h2⟩; exact ⟨by omega, h2⟩
      · rename_i hge3
        have hsib : ∀ m, (clist children)[idx.val - 1]? = some m →
            31 ≤ m.entries.val.length ∧ ChildrenMinArity m.children := by
          intro m hm
          have := hother (idx.val - 1) m (by omega) hm
          exact ⟨this.entries_ge, this.children⟩
        apply WP.spec_mono (merge_with_left_minarity entries children idx hclen hidx0 hidx
          (fun nl nr hnl hnr => by
            have := hchE (idx.val - 1) (by omega) nl hnl
            have := hchE idx.val hidx nr hnr; scalar_tac)
          hsib hidxc hoth2r)
        rintro out ⟨h1, h2⟩; exact ⟨by omega, h2⟩
  · rename_i hnpos
    have hidx0 : idx.val = 0 := by scalar_tac
    obtain ⟨i, hiadd, hivp⟩ := WP.spec_imp_exists
      (Usize.add_spec (x := idx) (y := 1#usize) (by scalar_tac))
    have hiv : i.val = idx.val + 1 := by scalar_tac
    simp only [hiadd, bind_tc_ok]
    split
    · rename_i hlt
      have hidxlt : idx.val < entries.val.length := by omega
      obtain ⟨n1, hn1e, hn1v⟩ := WP.spec_imp_exists (child_at_spec children i (by omega))
      simp only [hiv] at hn1v
      obtain ⟨i2, hmk, hmkv⟩ := WP.spec_imp_exists MIN_KEYS_spec
      simp only [hn1e, hmk, bind_tc_ok]
      split
      · rename_i hgt
        have hn1len : 31 < n1.entries.val.length := by
          have := alloc.vec.Vec.len_val n1.entries; scalar_tac
        have hdon : ∀ m, (clist children)[idx.val + 1]? = some m →
            32 ≤ m.entries.val.length ∧ ChildrenMinArity m.children := by
          intro m hm
          have hmn : m = n1 := by
            rw [List.getElem?_eq_getElem (show idx.val + 1 < (clist children).length by omega)] at hm
            rw [hn1v]; exact (Option.some.inj hm).symm
          exact ⟨by rw [hmn]; omega, (hother (idx.val + 1) m (by omega) hm).children⟩
        apply WP.spec_mono (rotate_left_minarity entries children idx hclen hidxlt
          (fun m hm => by have := hchE idx.val hidx m hm; scalar_tac)
          hdon hidxc hoth2l)
        rintro out ⟨h1, h2⟩; exact ⟨by omega, h2⟩
      · rename_i hngt
        have hsib : ∀ m, (clist children)[idx.val + 1]? = some m →
            31 ≤ m.entries.val.length ∧ ChildrenMinArity m.children := by
          intro m hm
          have := hother (idx.val + 1) m (by omega) hm
          exact ⟨this.entries_ge, this.children⟩
        apply WP.spec_mono (merge_with_right_minarity entries children idx hclen hidxlt
          (fun nl nr hnl hnr => by
            have := hchE idx.val hidx nl hnl
            have := hchE (idx.val + 1) (by omega) nr hnr; scalar_tac)
          hidxc hsib hoth2l)
        rintro out ⟨h1, h2⟩; exact ⟨by omega, h2⟩
    · rename_i hge
      exfalso; scalar_tac

/-- `maybe_fix`: identity when the child was not underfull (then *all* children
    are `MinArity`), otherwise `fix_underfull_child_minarity`. -/
theorem maybe_fix_minarity
    (entries : alloc.vec.Vec (Std.U64 × Std.U64)) (children : Children)
    (idx : Std.Usize) (underfull : Bool)
    (halign : (clist children).length = entries.val.length + 1)
    (hpar63 : entries.val.length ≤ 63)
    (hnz : underfull = true → 0 < entries.val.length)
    (hidx : idx.val ≤ entries.val.length)
    (hcap63 : ∀ (j : Nat) (n : Node), (clist children)[j]? = some n →
      n.entries.val.length ≤ 63)
    (hcapC : ∀ (j : Nat) (n : Node), (clist children)[j]? = some n →
      (clist n.children).length ≤ 64)
    (hother : ∀ (j : Nat) (n : Node), j ≠ idx.val →
      (clist children)[j]? = some n → MinArity n)
    (hidxc : ∀ n, (clist children)[idx.val]? = some n → AlmostMinArity n)
    (hfull : underfull = false → ∀ n, (clist children)[idx.val]? = some n →
      31 ≤ n.entries.val.length) :
    maybe_fix entries children idx underfull ⦃ out =>
      entries.val.length - 1 ≤ out.1.val.length ∧ ChildrenMinArity out.2 ⦄ := by
  rw [maybe_fix]
  split
  · rename_i hu
    exact fix_underfull_child_minarity entries children idx halign hpar63 (hnz hu) hidx
      hcap63 hcapC hother hidxc
  · rename_i hu
    refine (WP.spec_ok _).mpr ⟨Nat.sub_le _ _, ?_⟩
    refine ChildrenMinArity.of_allMinArity ?_
    rw [allMinArity_iff_getElem?]
    intro j n hjn
    by_cases hj : j = idx.val
    · subst hj; exact (hidxc n hjn).toMinArity (hfull (by simpa using hu) n hjn)
    · exact hother j n hj hjn

/-! ## L2 — `remove_leftmost` / `delete_from_node` return `AlmostMinArity` -/

/-- Every child of a `MinArity` node is `MinArity`, addressed by `getElem?`. -/
private theorem child_minArity_of_getElem? {node : Node} (hma : MinArity node)
    {j : Nat} {n : Node} (hn : (clist node.children)[j]? = some n) : MinArity n :=
  hma.children.allMinArity n (List.mem_of_getElem? hn)

/-- `remove_leftmost` on a `MinArity` subtree returns an `AlmostMinArity` one,
    and the underfull flag is exact enough to recover `≥ 31` when it is `false`. -/
theorem remove_leftmost_minarity (fuel : Nat) {lo hi : Option Nat} {h : Nat}
    (node : Node) (e : Std.U64 × Std.U64) (n' : Node) (uf : Bool)
    (hfuel : Node.size node ≤ fuel)
    (hinv : NodeInv lo hi node) (hh : HeightInv h node) (hma : MinArity node)
    (hok : remove_leftmost node = ok (e, n', uf)) :
    AlmostMinArity n' ∧ (uf = false → 31 ≤ n'.entries.val.length) := by
  induction fuel generalizing lo hi h node e n' uf with
  | zero => exfalso; have := Node.one_le_size node; omega
  | succ fuel ih =>
    cases hinv with
    | leaf entries hs hb hlen =>
      have hmalen : 31 ≤ entries.val.length := by simpa using hma.entries_ge
      rw [remove_leftmost] at hok
      simp only [Node.entries._simpLemma_, Node.children._simpLemma_] at hok
      obtain ⟨i, hi', hok⟩ := bind_ok_inv hok
      have hilen : i.val = (clist Children.Nil).length :=
        spec_elim (children_len_spec Children.Nil (by simp [clist])) hi'
      simp only [clist, List.length_nil] at hilen
      rw [if_pos (by scalar_tac : i = 0#usize)] at hok
      simp only [alloc.vec.Vec.index_slice_index] at hok
      obtain ⟨first, hfirst, hok⟩ := bind_ok_inv hok
      obtain ⟨ne0, hne0, hok⟩ := bind_ok_inv hok
      have hne0v : ne0.val = entries.val.eraseIdx (0#usize).val :=
        spec_elim (remove_entry_at_spec entries 0#usize) hne0
      obtain ⟨i2, hmk, hok⟩ := bind_ok_inv hok
      have hi2 : i2.val = 31 := spec_elim MIN_KEYS_spec hmk
      simp only [ok.injEq, Prod.mk.injEq] at hok
      obtain ⟨-, hnn, hufe⟩ := hok
      subst hnn
      have hlenne0 : ne0.val.length = entries.val.length - 1 := by
        rw [hne0v, List.length_eraseIdx, if_pos (show (0#usize).val < entries.val.length by scalar_tac)]
      refine ⟨⟨by simp only [Node.entries._simpLemma_]; omega, ChildrenMinArity.nil⟩, ?_⟩
      intro huff
      simp only [Node.entries._simpLemma_]
      have hlv := alloc.vec.Vec.len_val ne0
      rw [huff] at hufe
      simp only [decide_eq_false_iff_not, not_lt] at hufe
      scalar_tac
    | internal entries c cs hs hb hlen hal =>
      cases hh with
      | @internal h0 e2 c2 cs2 hch =>
        have hma_cs : ChildrenMinArity (Children.Cons c cs) := hma.children
        have hma_c : MinArity c := by cases hma_cs with | cons _ _ hc _ => exact hc
        rw [remove_leftmost] at hok
        simp only [Node.entries._simpLemma_, Node.children._simpLemma_] at hok
        have hclen : (clist (Children.Cons c cs)).length = entries.val.length + 1 := by
          have := hal.length_eq; simpa [clist] using this
        obtain ⟨i, hi', hok⟩ := bind_ok_inv hok
        have hilen : i.val = (clist (Children.Cons c cs)).length :=
          spec_elim (children_len_spec _ (by rw [hclen]; scalar_tac)) hi'
        rw [hclen] at hilen
        rw [if_neg (by scalar_tac : ¬ (i = 0#usize))] at hok
        obtain ⟨n, hn, hok⟩ := bind_ok_inv hok
        have hn_eq : n = (clist (Children.Cons c cs))[(0#usize).val]'(by rw [hclen]; scalar_tac) :=
          spec_elim (child_at_spec _ 0#usize (by rw [hclen]; scalar_tac)) hn
        have hnc : n = c := by rw [hn_eq]; simp [clist]
        obtain ⟨hc0lt, hc0inv⟩ := hal.child 0 (Nat.zero_le _)
        simp only [lbnd, List.getElem_cons_zero, ↓reduceIte] at hc0inv
        have hc_h : HeightInv h0 c := (hch.allH) c (by simp [clist])
        have hsz : Node.size c ≤ fuel := by
          have hmem : c ∈ clist (Children.Cons c cs) := by simp [clist]
          have h1 := size_mem_clist _ _ hmem
          have h2 : Node.size (Node.mk entries (Children.Cons c cs)) =
              1 + Children.size (Children.Cons c cs) := by simp [Node.size]
          simp only [Node.size] at hfuel; omega
        obtain ⟨⟨entry, new_first_child, child_underfull⟩, hrl, hok⟩ := bind_ok_inv hok
        have hrl' : remove_leftmost c = ok (entry, new_first_child, child_underfull) := by
          rw [← hnc]; exact hrl
        obtain ⟨-, hNI_nfc, -, huf_nfc⟩ :=
          remove_leftmost_spec fuel c entry new_first_child child_underfull hsz hc0inv hc_h hrl'
        obtain ⟨hma_nfc, hfull_nfc⟩ :=
          ih c entry new_first_child child_underfull hsz hc0inv hc_h hma_c hrl'
        obtain ⟨entries0, hcl0, hok⟩ := bind_ok_inv hok
        have he0 : entries0 = entries := spec_elim (cloneVec_builtin_spec entries) hcl0
        obtain ⟨children0, hrc0, hok⟩ := bind_ok_inv hok
        have hc0 : clist children0 = (clist (Children.Cons c cs)).set (0#usize).val new_first_child :=
          spec_elim (replace_child_spec (Children.Cons c cs) 0#usize new_first_child) hrc0
        obtain ⟨⟨entries1, children1⟩, hmf, hok⟩ := bind_ok_inv hok
        obtain ⟨i2, hmk, hok⟩ := bind_ok_inv hok
        have hi2 : i2.val = 31 := spec_elim MIN_KEYS_spec hmk
        simp only [ok.injEq, Prod.mk.injEq] at hok
        obtain ⟨-, hnn, hufe⟩ := hok
        -- feed maybe_fix_minarity at (entries0, children0), idx = 0
        have halign0 : (clist children0).length = entries0.val.length + 1 := by
          rw [hc0, List.length_set, hclen, he0]
        have hpar0 : entries0.val.length ≤ 63 := by rw [he0]; exact hlen
        have hchildB : ∀ (j : Nat) (m : Node), (clist children0)[j]? = some m →
            m.entries.val.length ≤ 63 ∧ (clist m.children).length ≤ 64 := by
          intro j m hm
          have hjle : j ≤ entries.val.length := by
            have hlt := getElem?_some_lt hm; rw [halign0, he0] at hlt; omega
          rw [hc0] at hm
          by_cases hj0 : j = (0#usize).val
          · subst hj0
            rw [List.getElem?_set_self (by rw [hclen]; scalar_tac), Option.some.injEq] at hm
            subst hm; exact ⟨hNI_nfc.entries_len_le, hNI_nfc.children_len_le⟩
          · rw [List.getElem?_set_ne (Ne.symm hj0)] at hm
            simp only [clist] at hm
            obtain ⟨hc, hNI⟩ := hal.child j hjle
            have hmeq : m = (c :: clist cs)[j]'hc := by
              rw [List.getElem?_eq_getElem hc] at hm; exact (Option.some.inj hm).symm
            rw [hmeq]; exact ⟨hNI.entries_len_le, hNI.children_len_le⟩
        have hcap63 : ∀ (j : Nat) (m : Node), (clist children0)[j]? = some m →
            m.entries.val.length ≤ 63 := fun j m hm => (hchildB j m hm).1
        have hcapC : ∀ (j : Nat) (m : Node), (clist children0)[j]? = some m →
            (clist m.children).length ≤ 64 := fun j m hm => (hchildB j m hm).2
        have hother0 : ∀ (j : Nat) (m : Node), j ≠ (0#usize).val →
            (clist children0)[j]? = some m → MinArity m := by
          intro j m hj0 hm
          rw [hc0, List.getElem?_set_ne (Ne.symm hj0)] at hm
          exact hma_cs.allMinArity m (List.mem_of_getElem? hm)
        have hidxc0 : ∀ m, (clist children0)[(0#usize).val]? = some m → AlmostMinArity m := by
          intro m hm
          rw [hc0, List.getElem?_set_self (by rw [hclen]; scalar_tac), Option.some.injEq] at hm
          subst hm; exact hma_nfc
        have hfull0 : child_underfull = false → ∀ m, (clist children0)[(0#usize).val]? = some m →
            31 ≤ m.entries.val.length := by
          intro hcuf m hm
          rw [hc0, List.getElem?_set_self (by rw [hclen]; scalar_tac), Option.some.injEq] at hm
          subst hm; exact hfull_nfc hcuf
        have hme31 : 31 ≤ entries.val.length := by simpa using hma.entries_ge
        have hmf_spec := maybe_fix_minarity entries0 children0 0#usize child_underfull
          halign0 hpar0 (fun _ => by rw [he0]; omega) (by scalar_tac)
          hcap63 hcapC hother0 hidxc0 hfull0
        obtain ⟨hlen1, hcm1⟩ := spec_elim hmf_spec hmf
        have hlen1' : entries0.val.length - 1 ≤ entries1.val.length := hlen1
        have hcm1' : ChildrenMinArity children1 := hcm1
        subst hnn
        have hentry0 : 31 ≤ entries0.val.length := by rw [he0]; exact hma.entries_ge
        refine ⟨⟨by simp only [Node.entries._simpLemma_]; omega, hcm1'⟩, ?_⟩
        intro huff
        simp only [Node.entries._simpLemma_]
        have hlv := alloc.vec.Vec.len_val entries1
        rw [huff] at hufe
        simp only [decide_eq_false_iff_not, not_lt] at hufe
        scalar_tac

/-- `delete_from_node` on a `MinArity` subtree returns an `AlmostMinArity` one,
    with the underfull flag exact enough to recover `≥ 31` when `false`. -/
theorem delete_from_node_minarity (fuel : Nat) {lo hi : Option Nat} {h : Nat}
    (node : Node) (key : Std.U64) (n' : Node) (uf : Bool)
    (hfuel : Node.size node ≤ fuel)
    (hinv : NodeInv lo hi node) (hh : HeightInv h node) (hma : MinArity node)
    (hok : delete_from_node node key = ok (DeleteResult.Removed n' uf)) :
    AlmostMinArity n' ∧ (uf = false → 31 ≤ n'.entries.val.length) := by
  induction fuel generalizing lo hi h node n' uf with
  | zero => exfalso; have := Node.one_le_size node; omega
  | succ fuel ih =>
    cases hinv with
    | leaf entries hs hb hlen =>
      have hme31 : 31 ≤ entries.val.length := by simpa using hma.entries_ge
      cases hh with
      | leaf _ =>
        rw [delete_from_node] at hok
        simp only [Node.entries._simpLemma_, Node.children._simpLemma_] at hok
        obtain ⟨⟨hit, pos⟩, hfp, hok⟩ := bind_ok_inv hok
        obtain ⟨hple, hhit, hmiss⟩ := spec_elim (find_pos_spec entries key hs) hfp
        obtain ⟨i, hi', hok⟩ := bind_ok_inv hok
        have hilen : i.val = (clist Children.Nil).length :=
          spec_elim (children_len_spec Children.Nil (by simp [clist])) hi'
        simp only [clist, List.length_nil] at hilen
        rw [if_pos (by scalar_tac : i = 0#usize)] at hok
        split at hok
        · rename_i hcond
          obtain ⟨hplt, -⟩ := hhit (by simpa using hcond)
          obtain ⟨new_entries, hre, hok⟩ := bind_ok_inv hok
          have hrev : new_entries.val = entries.val.eraseIdx pos.val :=
            spec_elim (remove_entry_at_spec entries pos) hre
          obtain ⟨i2, hmk, hok⟩ := bind_ok_inv hok
          have hi2 : i2.val = 31 := spec_elim MIN_KEYS_spec hmk
          simp only [ok.injEq, DeleteResult.Removed.injEq] at hok
          obtain ⟨hnn, hufe⟩ := hok
          subst hnn
          have hlenne : new_entries.val.length = entries.val.length - 1 := by
            rw [hrev, List.length_eraseIdx, if_pos hplt]
          refine ⟨⟨by simp only [Node.entries._simpLemma_]; omega, ChildrenMinArity.nil⟩, ?_⟩
          intro huff
          simp only [Node.entries._simpLemma_]
          have hlv := alloc.vec.Vec.len_val new_entries
          rw [huff] at hufe
          simp only [decide_eq_false_iff_not, not_lt] at hufe
          scalar_tac
        · rename_i hcond; simp at hok
    | internal entries c cs hs hb hlen hal =>
      cases hh with
      | @internal h0 e2 c2 cs2 hch =>
        have hma_cs : ChildrenMinArity (Children.Cons c cs) := hma.children
        have hme31 : 31 ≤ entries.val.length := by simpa using hma.entries_ge
        rw [delete_from_node] at hok
        simp only [Node.entries._simpLemma_, Node.children._simpLemma_] at hok
        obtain ⟨⟨hit, pos⟩, hfp, hok⟩ := bind_ok_inv hok
        obtain ⟨hple, hhit, hmiss⟩ := spec_elim (find_pos_spec entries key hs) hfp
        dsimp only at hple hhit hmiss
        have hclen : (clist (Children.Cons c cs)).length = entries.val.length + 1 := by
          have := hal.length_eq; simpa [clist] using this
        obtain ⟨i, hi', hok⟩ := bind_ok_inv hok
        have hilen : i.val = (clist (Children.Cons c cs)).length :=
          spec_elim (children_len_spec _ (by rw [hclen]; scalar_tac)) hi'
        rw [hclen] at hilen
        rw [if_neg (by scalar_tac : ¬ (i = 0#usize))] at hok
        split at hok
        · -- internal hit: successor from child pos+1
          rename_i hcond
          obtain ⟨hplt, -⟩ := hhit hcond
          obtain ⟨i1, hi1, hok⟩ := bind_ok_inv hok
          have hi1v : i1.val = pos.val + 1 := by
            have := spec_elim (Usize.add_spec (x := pos) (y := 1#usize) (by scalar_tac)) hi1
            scalar_tac
          have hi1lt : i1.val < (clist (Children.Cons c cs)).length := by rw [hclen, hi1v]; omega
          obtain ⟨n, hn, hok⟩ := bind_ok_inv hok
          have hn_eq : n = (clist (Children.Cons c cs))[i1.val]'hi1lt :=
            spec_elim (child_at_spec _ i1 hi1lt) hn
          obtain ⟨hcslt, hcinv⟩ := hal.child i1.val (by rw [hi1v]; omega)
          have hcn : NodeInv (lbnd lo entries.val i1.val) (rbnd hi entries.val i1.val) n := by
            rw [hn_eq]
            have heq : (clist (Children.Cons c cs))[i1.val]'hi1lt = (c :: clist cs)[i1.val]'hcslt := by
              simp [clist]
            rw [heq]; exact hcinv
          have hn_h : HeightInv h0 n := by rw [hn_eq]; exact hch.allH.getElem i1.val hi1lt
          have hn_ma : MinArity n := by
            rw [hn_eq]; exact hma_cs.allMinArity _ (List.getElem_mem hi1lt)
          obtain ⟨⟨entry, new_right, right_underfull⟩, hrl, hok⟩ := bind_ok_inv hok
          obtain ⟨-, hNI_nr, -, huf_nr⟩ :=
            remove_leftmost_spec (Node.size n) n entry new_right right_underfull
              (le_refl _) hcn hn_h hrl
          obtain ⟨hma_nr, hfull_nr⟩ :=
            remove_leftmost_minarity (Node.size n) n entry new_right right_underfull
              (le_refl _) hcn hn_h hn_ma hrl
          obtain ⟨entries0, hre0, hok⟩ := bind_ok_inv hok
          have he0v : entries0.val = entries.val.set pos.val entry :=
            spec_elim (replace_entry_spec entries pos entry) hre0
          have he0len : entries0.val.length = entries.val.length := by rw [he0v, List.length_set]
          obtain ⟨children0, hrc0, hok⟩ := bind_ok_inv hok
          have hc0v : clist children0 = (clist (Children.Cons c cs)).set i1.val new_right :=
            spec_elim (replace_child_spec (Children.Cons c cs) i1 new_right) hrc0
          obtain ⟨⟨entries1, children1⟩, hmf, hok⟩ := bind_ok_inv hok
          obtain ⟨i3, hmk, hok⟩ := bind_ok_inv hok
          have hi3 : i3.val = 31 := spec_elim MIN_KEYS_spec hmk
          simp only [ok.injEq, DeleteResult.Removed.injEq] at hok
          obtain ⟨hnn, hufe⟩ := hok
          have halign0 : (clist children0).length = entries0.val.length + 1 := by
            rw [hc0v, List.length_set, hclen, he0len]
          have hpar0 : entries0.val.length ≤ 63 := by rw [he0len]; exact hlen
          have hchildB : ∀ (j : Nat) (m : Node), (clist children0)[j]? = some m →
              m.entries.val.length ≤ 63 ∧ (clist m.children).length ≤ 64 := by
            intro j m hm
            have hjle : j ≤ entries.val.length := by
              have hlt := getElem?_some_lt hm; rw [halign0, he0len] at hlt; omega
            rw [hc0v] at hm
            by_cases hj0 : j = i1.val
            · subst hj0
              rw [List.getElem?_set_self hi1lt, Option.some.injEq] at hm
              subst hm; exact ⟨hNI_nr.entries_len_le, hNI_nr.children_len_le⟩
            · rw [List.getElem?_set_ne (Ne.symm hj0)] at hm
              simp only [clist] at hm
              obtain ⟨hc, hNI⟩ := hal.child j hjle
              have hmeq : m = (c :: clist cs)[j]'hc := by
                rw [List.getElem?_eq_getElem hc] at hm; exact (Option.some.inj hm).symm
              rw [hmeq]; exact ⟨hNI.entries_len_le, hNI.children_len_le⟩
          have hother0 : ∀ (j : Nat) (m : Node), j ≠ i1.val →
              (clist children0)[j]? = some m → MinArity m := by
            intro j m hj0 hm
            rw [hc0v, List.getElem?_set_ne (Ne.symm hj0)] at hm
            exact hma_cs.allMinArity m (List.mem_of_getElem? hm)
          have hidxc0 : ∀ m, (clist children0)[i1.val]? = some m → AlmostMinArity m := by
            intro m hm
            rw [hc0v, List.getElem?_set_self hi1lt, Option.some.injEq] at hm
            subst hm; exact hma_nr
          have hfull0 : right_underfull = false → ∀ m, (clist children0)[i1.val]? = some m →
              31 ≤ m.entries.val.length := by
            intro hcuf m hm
            rw [hc0v, List.getElem?_set_self hi1lt, Option.some.injEq] at hm
            subst hm; exact hfull_nr hcuf
          have hmf_spec := maybe_fix_minarity entries0 children0 i1 right_underfull
            halign0 hpar0 (fun _ => by rw [he0len]; omega) (by rw [he0len, hi1v]; omega)
            (fun j m hm => (hchildB j m hm).1) (fun j m hm => (hchildB j m hm).2)
            hother0 hidxc0 hfull0
          obtain ⟨hlen1, hcm1⟩ := spec_elim hmf_spec hmf
          have hlen1' : entries0.val.length - 1 ≤ entries1.val.length := hlen1
          have hcm1' : ChildrenMinArity children1 := hcm1
          have hentry0 : 31 ≤ entries0.val.length := by rw [he0len]; exact hme31
          subst hnn
          refine ⟨⟨by simp only [Node.entries._simpLemma_]; omega, hcm1'⟩, ?_⟩
          intro huff
          simp only [Node.entries._simpLemma_]
          have hlv := alloc.vec.Vec.len_val entries1
          rw [huff] at hufe
          simp only [decide_eq_false_iff_not, not_lt] at hufe
          scalar_tac
        · -- internal miss: delete recurses into child pos
          rename_i hcond
          have hitf : hit = false := by simpa using hcond
          have hposlt : pos.val < (clist (Children.Cons c cs)).length := by rw [hclen]; scalar_tac
          obtain ⟨n, hn, hok⟩ := bind_ok_inv hok
          have hn_eq : n = (clist (Children.Cons c cs))[pos.val]'hposlt :=
            spec_elim (child_at_spec _ pos hposlt) hn
          obtain ⟨dr, hdr, hok⟩ := bind_ok_inv hok
          obtain ⟨hcslt, hcinv⟩ := hal.child pos.val hple
          have hcn : NodeInv (lbnd lo entries.val pos.val) (rbnd hi entries.val pos.val) n := by
            rw [hn_eq]
            have heq : (clist (Children.Cons c cs))[pos.val]'hposlt = (c :: clist cs)[pos.val]'hcslt := by
              simp [clist]
            rw [heq]; exact hcinv
          have hn_h : HeightInv h0 n := by rw [hn_eq]; exact hch.allH.getElem pos.val hposlt
          have hn_ma : MinArity n := by
            rw [hn_eq]; exact hma_cs.allMinArity _ (List.getElem_mem hposlt)
          have hsz : Node.size n ≤ fuel := by
            have hmem : n ∈ clist (Children.Cons c cs) := by rw [hn_eq]; exact List.getElem_mem hposlt
            have h1 := size_mem_clist _ _ hmem
            simp only [Node.size] at hfuel; omega
          cases dr with
          | NotFound => simp at hok
          | Removed new_child underfull =>
            dsimp only at hok
            obtain ⟨hNI_nc, -, huf_nc⟩ :=
              delete_from_node_inv (Node.size n) n key new_child underfull (le_refl _) hcn hn_h hdr
            obtain ⟨hma_nc, hfull_nc⟩ := ih n new_child underfull hsz hcn hn_h hn_ma hdr
            obtain ⟨entries0, hcl0, hok⟩ := bind_ok_inv hok
            have he0 : entries0 = entries := spec_elim (cloneVec_builtin_spec entries) hcl0
            obtain ⟨children0, hrc0, hok⟩ := bind_ok_inv hok
            have hc0v : clist children0 = (clist (Children.Cons c cs)).set pos.val new_child :=
              spec_elim (replace_child_spec (Children.Cons c cs) pos new_child) hrc0
            obtain ⟨⟨entries1, children1⟩, hmf, hok⟩ := bind_ok_inv hok
            obtain ⟨i2, hmk, hok⟩ := bind_ok_inv hok
            have hi2 : i2.val = 31 := spec_elim MIN_KEYS_spec hmk
            simp only [ok.injEq, DeleteResult.Removed.injEq] at hok
            obtain ⟨hnn, hufe⟩ := hok
            have halign0 : (clist children0).length = entries0.val.length + 1 := by
              rw [hc0v, List.length_set, hclen, he0]
            have hpar0 : entries0.val.length ≤ 63 := by rw [he0]; exact hlen
            have hchildB : ∀ (j : Nat) (m : Node), (clist children0)[j]? = some m →
                m.entries.val.length ≤ 63 ∧ (clist m.children).length ≤ 64 := by
              intro j m hm
              have hjle : j ≤ entries.val.length := by
                have hlt := getElem?_some_lt hm; rw [halign0, he0] at hlt; omega
              rw [hc0v] at hm
              by_cases hj0 : j = pos.val
              · subst hj0
                rw [List.getElem?_set_self hposlt, Option.some.injEq] at hm
                subst hm; exact ⟨hNI_nc.entries_len_le, hNI_nc.children_len_le⟩
              · rw [List.getElem?_set_ne (Ne.symm hj0)] at hm
                simp only [clist] at hm
                obtain ⟨hc, hNI⟩ := hal.child j hjle
                have hmeq : m = (c :: clist cs)[j]'hc := by
                  rw [List.getElem?_eq_getElem hc] at hm; exact (Option.some.inj hm).symm
                rw [hmeq]; exact ⟨hNI.entries_len_le, hNI.children_len_le⟩
            have hother0 : ∀ (j : Nat) (m : Node), j ≠ pos.val →
                (clist children0)[j]? = some m → MinArity m := by
              intro j m hj0 hm
              rw [hc0v, List.getElem?_set_ne (Ne.symm hj0)] at hm
              exact hma_cs.allMinArity m (List.mem_of_getElem? hm)
            have hidxc0 : ∀ m, (clist children0)[pos.val]? = some m → AlmostMinArity m := by
              intro m hm
              rw [hc0v, List.getElem?_set_self hposlt, Option.some.injEq] at hm
              subst hm; exact hma_nc
            have hfull0 : underfull = false → ∀ m, (clist children0)[pos.val]? = some m →
                31 ≤ m.entries.val.length := by
              intro hcuf m hm
              rw [hc0v, List.getElem?_set_self hposlt, Option.some.injEq] at hm
              subst hm; exact hfull_nc hcuf
            have hmf_spec := maybe_fix_minarity entries0 children0 pos underfull
              halign0 hpar0 (fun _ => by rw [he0]; omega) (by rw [he0]; exact hple)
              (fun j m hm => (hchildB j m hm).1) (fun j m hm => (hchildB j m hm).2)
              hother0 hidxc0 hfull0
            obtain ⟨hlen1, hcm1⟩ := spec_elim hmf_spec hmf
            have hlen1' : entries0.val.length - 1 ≤ entries1.val.length := hlen1
            have hcm1' : ChildrenMinArity children1 := hcm1
            have hentry0 : 31 ≤ entries0.val.length := by rw [he0]; exact hme31
            subst hnn
            refine ⟨⟨by simp only [Node.entries._simpLemma_]; omega, hcm1'⟩, ?_⟩
            intro huff
            simp only [Node.entries._simpLemma_]
            have hlv := alloc.vec.Vec.len_val entries1
            rw [huff] at hufe
            simp only [decide_eq_false_iff_not, not_lt] at hufe
            scalar_tac

/-! ## L3 — `BTree.remove` preserves `MinKeysInv` -/

/-- Root-level: `delete_from_node` on a `BalancedRoot` node returns a node whose
    children are all `MinArity`. This is the *only* fact `BTree.remove` needs
    (it discharges every `BalancedRoot` case, including root-collapse). The root
    itself is not `MinArity` (it may hold < 31 entries), so this cannot use
    `delete_from_node_minarity` directly — but the recursion descends into
    children, which *are* `MinArity`, so it delegates to the L2 lemmas. -/
theorem delete_from_node_root_cma {lo hi : Option Nat} {h : Nat}
    (node : Node) (key : Std.U64) (n' : Node) (uf : Bool)
    (hinv : NodeInv lo hi node) (hh : HeightInv h node) (hbr : BalancedRoot node)
    (hok : delete_from_node node key = ok (DeleteResult.Removed n' uf)) :
    ChildrenMinArity n'.children := by
  cases hinv with
  | leaf entries hs hb hlen =>
    -- a leaf delete returns a leaf: no children
    cases hh with
    | leaf _ =>
      rw [delete_from_node] at hok
      simp only [Node.entries._simpLemma_, Node.children._simpLemma_] at hok
      obtain ⟨⟨hit, pos⟩, hfp, hok⟩ := bind_ok_inv hok
      obtain ⟨i, hi', hok⟩ := bind_ok_inv hok
      have hilen : i.val = (clist Children.Nil).length :=
        spec_elim (children_len_spec Children.Nil (by simp [clist])) hi'
      simp only [clist, List.length_nil] at hilen
      rw [if_pos (by scalar_tac : i = 0#usize)] at hok
      split at hok
      · rename_i hcond
        obtain ⟨new_entries, hre, hok⟩ := bind_ok_inv hok
        obtain ⟨i2, hmk, hok⟩ := bind_ok_inv hok
        simp only [ok.injEq, DeleteResult.Removed.injEq] at hok
        obtain ⟨hnn, -⟩ := hok
        subst hnn; exact ChildrenMinArity.nil
      · rename_i hcond; simp at hok
  | internal entries c cs hs hb hlen hal =>
    cases hh with
    | @internal h0 e2 c2 cs2 hch =>
      have hcm_cs : ChildrenMinArity (Children.Cons c cs) := by
        rcases hbr with hnil | ⟨-, hcm⟩
        · simp [clist] at hnil
        · exact hcm
      have hpos : 0 < entries.val.length := by
        rcases hbr with hnil | ⟨hp, -⟩
        · simp [clist] at hnil
        · exact hp
      rw [delete_from_node] at hok
      simp only [Node.entries._simpLemma_, Node.children._simpLemma_] at hok
      obtain ⟨⟨hit, pos⟩, hfp, hok⟩ := bind_ok_inv hok
      obtain ⟨hple, hhit, hmiss⟩ := spec_elim (find_pos_spec entries key hs) hfp
      dsimp only at hple hhit hmiss
      have hclen : (clist (Children.Cons c cs)).length = entries.val.length + 1 := by
        have := hal.length_eq; simpa [clist] using this
      obtain ⟨i, hi', hok⟩ := bind_ok_inv hok
      have hilen : i.val = (clist (Children.Cons c cs)).length :=
        spec_elim (children_len_spec _ (by rw [hclen]; scalar_tac)) hi'
      rw [hclen] at hilen
      rw [if_neg (by scalar_tac : ¬ (i = 0#usize))] at hok
      -- shared machinery: build the maybe_fix hypotheses from a modified child `X`
      split at hok
      · -- internal hit
        rename_i hcond
        obtain ⟨hplt, -⟩ := hhit hcond
        obtain ⟨i1, hi1, hok⟩ := bind_ok_inv hok
        have hi1v : i1.val = pos.val + 1 := by
          have := spec_elim (Usize.add_spec (x := pos) (y := 1#usize) (by scalar_tac)) hi1
          scalar_tac
        have hi1lt : i1.val < (clist (Children.Cons c cs)).length := by rw [hclen, hi1v]; omega
        obtain ⟨n, hn, hok⟩ := bind_ok_inv hok
        have hn_eq : n = (clist (Children.Cons c cs))[i1.val]'hi1lt :=
          spec_elim (child_at_spec _ i1 hi1lt) hn
        obtain ⟨hcslt, hcinv⟩ := hal.child i1.val (by rw [hi1v]; omega)
        have hcn : NodeInv (lbnd lo entries.val i1.val) (rbnd hi entries.val i1.val) n := by
          rw [hn_eq]
          have heq : (clist (Children.Cons c cs))[i1.val]'hi1lt = (c :: clist cs)[i1.val]'hcslt := by
            simp [clist]
          rw [heq]; exact hcinv
        have hn_h : HeightInv h0 n := by rw [hn_eq]; exact hch.allH.getElem i1.val hi1lt
        have hn_ma : MinArity n := by
          rw [hn_eq]; exact hcm_cs.allMinArity _ (List.getElem_mem hi1lt)
        obtain ⟨⟨entry, new_right, right_underfull⟩, hrl, hok⟩ := bind_ok_inv hok
        obtain ⟨-, hNI_nr, -, -⟩ :=
          remove_leftmost_spec (Node.size n) n entry new_right right_underfull
            (le_refl _) hcn hn_h hrl
        obtain ⟨hma_nr, hfull_nr⟩ :=
          remove_leftmost_minarity (Node.size n) n entry new_right right_underfull
            (le_refl _) hcn hn_h hn_ma hrl
        obtain ⟨entries0, hre0, hok⟩ := bind_ok_inv hok
        have he0len : entries0.val.length = entries.val.length := by
          rw [spec_elim (replace_entry_spec entries pos entry) hre0, List.length_set]
        obtain ⟨children0, hrc0, hok⟩ := bind_ok_inv hok
        have hc0v : clist children0 = (clist (Children.Cons c cs)).set i1.val new_right :=
          spec_elim (replace_child_spec (Children.Cons c cs) i1 new_right) hrc0
        obtain ⟨⟨entries1, children1⟩, hmf, hok⟩ := bind_ok_inv hok
        obtain ⟨i3, hmk, hok⟩ := bind_ok_inv hok
        simp only [ok.injEq, DeleteResult.Removed.injEq] at hok
        obtain ⟨hnn, -⟩ := hok
        have halign0 : (clist children0).length = entries0.val.length + 1 := by
          rw [hc0v, List.length_set, hclen, he0len]
        have hchildB : ∀ (j : Nat) (m : Node), (clist children0)[j]? = some m →
            m.entries.val.length ≤ 63 ∧ (clist m.children).length ≤ 64 := by
          intro j m hm
          have hjle : j ≤ entries.val.length := by
            have hlt := getElem?_some_lt hm; rw [halign0, he0len] at hlt; omega
          rw [hc0v] at hm
          by_cases hj0 : j = i1.val
          · subst hj0
            rw [List.getElem?_set_self hi1lt, Option.some.injEq] at hm
            subst hm; exact ⟨hNI_nr.entries_len_le, hNI_nr.children_len_le⟩
          · rw [List.getElem?_set_ne (Ne.symm hj0)] at hm
            simp only [clist] at hm
            obtain ⟨hc, hNI⟩ := hal.child j hjle
            have hmeq : m = (c :: clist cs)[j]'hc := by
              rw [List.getElem?_eq_getElem hc] at hm; exact (Option.some.inj hm).symm
            rw [hmeq]; exact ⟨hNI.entries_len_le, hNI.children_len_le⟩
        have hother0 : ∀ (j : Nat) (m : Node), j ≠ i1.val →
            (clist children0)[j]? = some m → MinArity m := by
          intro j m hj0 hm
          rw [hc0v, List.getElem?_set_ne (Ne.symm hj0)] at hm
          exact hcm_cs.allMinArity m (List.mem_of_getElem? hm)
        have hidxc0 : ∀ m, (clist children0)[i1.val]? = some m → AlmostMinArity m := by
          intro m hm
          rw [hc0v, List.getElem?_set_self hi1lt, Option.some.injEq] at hm
          subst hm; exact hma_nr
        have hfull0 : right_underfull = false → ∀ m, (clist children0)[i1.val]? = some m →
            31 ≤ m.entries.val.length := by
          intro hcuf m hm
          rw [hc0v, List.getElem?_set_self hi1lt, Option.some.injEq] at hm
          subst hm; exact hfull_nr hcuf
        have hmf_spec := maybe_fix_minarity entries0 children0 i1 right_underfull
          halign0 (by rw [he0len]; exact hlen) (fun _ => by rw [he0len]; exact hpos)
          (by rw [he0len, hi1v]; omega)
          (fun j m hm => (hchildB j m hm).1) (fun j m hm => (hchildB j m hm).2)
          hother0 hidxc0 hfull0
        obtain ⟨-, hcm1⟩ := spec_elim hmf_spec hmf
        subst hnn; exact hcm1
      · -- internal miss
        rename_i hcond
        have hposlt : pos.val < (clist (Children.Cons c cs)).length := by rw [hclen]; scalar_tac
        obtain ⟨n, hn, hok⟩ := bind_ok_inv hok
        have hn_eq : n = (clist (Children.Cons c cs))[pos.val]'hposlt :=
          spec_elim (child_at_spec _ pos hposlt) hn
        obtain ⟨dr, hdr, hok⟩ := bind_ok_inv hok
        obtain ⟨hcslt, hcinv⟩ := hal.child pos.val hple
        have hcn : NodeInv (lbnd lo entries.val pos.val) (rbnd hi entries.val pos.val) n := by
          rw [hn_eq]
          have heq : (clist (Children.Cons c cs))[pos.val]'hposlt = (c :: clist cs)[pos.val]'hcslt := by
            simp [clist]
          rw [heq]; exact hcinv
        have hn_h : HeightInv h0 n := by rw [hn_eq]; exact hch.allH.getElem pos.val hposlt
        have hn_ma : MinArity n := by
          rw [hn_eq]; exact hcm_cs.allMinArity _ (List.getElem_mem hposlt)
        cases dr with
        | NotFound => simp at hok
        | Removed new_child underfull =>
          dsimp only at hok
          obtain ⟨hNI_nc, -, -⟩ :=
            delete_from_node_inv (Node.size n) n key new_child underfull (le_refl _) hcn hn_h hdr
          obtain ⟨hma_nc, hfull_nc⟩ :=
            delete_from_node_minarity (Node.size n) n key new_child underfull (le_refl _) hcn hn_h hn_ma hdr
          obtain ⟨entries0, hcl0, hok⟩ := bind_ok_inv hok
          have he0 : entries0 = entries := spec_elim (cloneVec_builtin_spec entries) hcl0
          obtain ⟨children0, hrc0, hok⟩ := bind_ok_inv hok
          have hc0v : clist children0 = (clist (Children.Cons c cs)).set pos.val new_child :=
            spec_elim (replace_child_spec (Children.Cons c cs) pos new_child) hrc0
          obtain ⟨⟨entries1, children1⟩, hmf, hok⟩ := bind_ok_inv hok
          obtain ⟨i2, hmk, hok⟩ := bind_ok_inv hok
          simp only [ok.injEq, DeleteResult.Removed.injEq] at hok
          obtain ⟨hnn, -⟩ := hok
          have halign0 : (clist children0).length = entries0.val.length + 1 := by
            rw [hc0v, List.length_set, hclen, he0]
          have hchildB : ∀ (j : Nat) (m : Node), (clist children0)[j]? = some m →
              m.entries.val.length ≤ 63 ∧ (clist m.children).length ≤ 64 := by
            intro j m hm
            have hjle : j ≤ entries.val.length := by
              have hlt := getElem?_some_lt hm; rw [halign0, he0] at hlt; omega
            rw [hc0v] at hm
            by_cases hj0 : j = pos.val
            · subst hj0
              rw [List.getElem?_set_self hposlt, Option.some.injEq] at hm
              subst hm; exact ⟨hNI_nc.entries_len_le, hNI_nc.children_len_le⟩
            · rw [List.getElem?_set_ne (Ne.symm hj0)] at hm
              simp only [clist] at hm
              obtain ⟨hc, hNI⟩ := hal.child j hjle
              have hmeq : m = (c :: clist cs)[j]'hc := by
                rw [List.getElem?_eq_getElem hc] at hm; exact (Option.some.inj hm).symm
              rw [hmeq]; exact ⟨hNI.entries_len_le, hNI.children_len_le⟩
          have hother0 : ∀ (j : Nat) (m : Node), j ≠ pos.val →
              (clist children0)[j]? = some m → MinArity m := by
            intro j m hj0 hm
            rw [hc0v, List.getElem?_set_ne (Ne.symm hj0)] at hm
            exact hcm_cs.allMinArity m (List.mem_of_getElem? hm)
          have hidxc0 : ∀ m, (clist children0)[pos.val]? = some m → AlmostMinArity m := by
            intro m hm
            rw [hc0v, List.getElem?_set_self hposlt, Option.some.injEq] at hm
            subst hm; exact hma_nc
          have hfull0 : underfull = false → ∀ m, (clist children0)[pos.val]? = some m →
              31 ≤ m.entries.val.length := by
            intro hcuf m hm
            rw [hc0v, List.getElem?_set_self hposlt, Option.some.injEq] at hm
            subst hm; exact hfull_nc hcuf
          have hmf_spec := maybe_fix_minarity entries0 children0 pos underfull
            halign0 (by rw [he0]; exact hlen) (fun _ => by rw [he0]; exact hpos)
            (by rw [he0]; exact hple)
            (fun j m hm => (hchildB j m hm).1) (fun j m hm => (hchildB j m hm).2)
            hother0 hidxc0 hfull0
          obtain ⟨-, hcm1⟩ := spec_elim hmf_spec hmf
          subst hnn; exact hcm1

/-- **`remove` preserves the MIN_KEYS balance invariant.** Whenever
    `BTree.remove` succeeds on a balanced tree, the result is balanced. -/
theorem BTree.remove_minkeys (t : BTree) (key : Std.U64) (t' : BTree)
    (hinv : BTreeInv t) (hbal : BTreeBalanced t) (hmin : MinKeysInv t)
    (hok : BTree.remove t key = ok (some t')) :
    MinKeysInv t' := by
  obtain ⟨h, hh⟩ := hbal
  rw [BTree.remove] at hok
  obtain ⟨dr, hdr, hok⟩ := bind_ok_inv hok
  cases dr with
  | NotFound => simp at hok
  | Removed new_root ufflag =>
    obtain ⟨nre, nrc⟩ := new_root
    dsimp only at hok
    have hcma : ChildrenMinArity nrc := by
      have := delete_from_node_root_cma t.root key (Node.mk nre nrc) ufflag hinv hh hmin hdr
      simpa using this
    obtain ⟨hNI_nr, -, -⟩ :=
      delete_from_node_inv (Node.size t.root) t.root key (Node.mk nre nrc) ufflag
        (le_refl _) hinv hh hdr
    obtain ⟨actual_root, hact, hok⟩ := bind_ok_inv hok
    obtain ⟨i1sub, hsub, hok⟩ := bind_ok_inv hok
    simp only [ok.injEq, Option.some.injEq] at hok
    subst hok
    simp only [Node.entries._simpLemma_, Node.children._simpLemma_] at hact
    show BalancedRoot actual_root
    split at hact
    · -- root became entryless
      rename_i hi0
      have hentnil : nre.val = [] := by
        have hlv := alloc.vec.Vec.len_val nre
        exact List.eq_nil_of_length_eq_zero (by scalar_tac)
      obtain ⟨i1, hcl, hact⟩ := bind_ok_inv hact
      have hi1len : i1.val = (clist nrc).length := by
        refine spec_elim (children_len_spec _ ?_) hcl
        have := NodeInv.children_len_le hNI_nr
        simp only [Node.children._simpLemma_] at this; scalar_tac
      split at hact
      · -- children ≠ [] → collapse to child 0 (which is `MinArity`)
        rename_i hi1ne
        have hchne : clist nrc ≠ [] :=
          List.ne_nil_of_length_pos (by rw [← hi1len]; scalar_tac)
        obtain ⟨nchild, hca, hact⟩ := bind_ok_inv hact
        have hclt : (0#usize).val < (clist nrc).length :=
          List.length_pos_iff.mpr hchne
        have hnc? : (clist nrc)[(0#usize).val]? = some nchild :=
          spec_elim (child_at_spec' _ 0#usize hclt) hca
        have hact' : actual_root = nchild := spec_elim (clone_node_spec nchild) hact
        rw [hact']
        exact (hcma.allMinArity nchild (List.mem_of_getElem? hnc?)).toBalancedRoot
      · -- children = [] → no collapse; root is an (empty-child) leaf
        rename_i hi1eq
        simp only [ok.injEq] at hact
        subst hact
        have : (clist nrc).length = 0 := by rw [← hi1len]; simpa using hi1eq
        exact Or.inl (List.eq_nil_of_length_eq_zero this)
    · -- root kept ≥ 1 entry → no collapse
      rename_i hine
      simp only [ok.injEq] at hact
      subst hact
      refine Or.inr ⟨?_, hcma⟩
      show 0 < nre.val.length
      have hlv := alloc.vec.Vec.len_val nre
      scalar_tac

/-- Capstone: the fully-balanced, unconditional deletion spec. `remove` either
    reports the key absent, or returns a tree that is well-formed
    (`BTreeInv`), height-uniform (`BTreeBalanced`), **MIN_KEYS-balanced**
    (`MinKeysInv`), and no longer contains the key. Combines `remove_spec`
    (RemoveTotal) with `remove_minkeys`. -/
theorem BTree.remove_balanced_spec (t : BTree) (key : Std.U64)
    (hinv : BTreeInv t) (hbal : BTreeBalanced t) (hmin : MinKeysInv t)
    (hlen : 0 < t.len.val) :
    BTree.remove t key = ok none ∨
    ∃ t', BTree.remove t key = ok (some t') ∧
      BTreeInv t' ∧ BTreeBalanced t' ∧ MinKeysInv t' ∧ t'.get key = ok none := by
  rcases BTree.remove_spec t key hinv hbal hmin hlen with hnone | ⟨t', hrm, hI, hB, hG⟩
  · exact Or.inl hnone
  · exact Or.inr ⟨t', hrm, hI, hB, BTree.remove_minkeys t key t' hinv hbal hmin hrm, hG⟩

end btree_kernel
