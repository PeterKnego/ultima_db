/- Delete-path rebalancers preserve the B-tree invariant.

   Proves that `rotate_right`, `rotate_left`, `merge_with_left`,
   `merge_with_right`, `fix_underfull_child` and `maybe_fix` preserve BOTH
   `NodeInv lo hi` (sortedness + alignment + arity, BtreeInvariant.lean) and
   `HeightInv (h+1)` (height uniformity, BalancedInvariant.lean) of the parent
   node they operate on.

   Layering:
   1. pure list surgery helpers (set/eraseIdx/take/drop identities);
   2. pure `Aligned` surgery: `Aligned.append` (the merge/concatenation lemma,
      inverse of `Aligned.split`), `Aligned.set_last`, `Aligned.set_head`, and
      the derived `Aligned.replace_pair` (rotate shape) and
      `Aligned.merge_children` (merge shape);
   3. pure node-level lemmas building the rebalanced siblings' invariants;
   4. the monadic surgery lemmas connecting the generated kernel code to the
      pure layer via the RemoveSpecs/ChildrenSpecs/EntrySpecs specs. -/
import BtreeKernel
import BtreeInvariant
import ListLemmas
import AlignedLemmas
import ChildrenSpecs
import EntrySpecs
import RemoveSpecs
import BtreeInsertInv
import BalancedInvariant

open Aeneas Aeneas.Std Result

namespace btree_kernel

/-! ## `lbnd` / `rbnd` equations (private copies; the AlignedLemmas ones are
    private to that file) -/

private theorem lbnd_zero' (lo : Option Nat) (es : List (Std.U64 × Std.U64)) :
    lbnd lo es 0 = lo := by
  simp [lbnd]

private theorem lbnd_cons_succ' (lo : Option Nat) (e : Std.U64 × Std.U64)
    (es : List (Std.U64 × Std.U64)) (pos : Nat) :
    lbnd lo (e :: es) (pos + 1) = lbnd (some e.1.val) es pos := by
  cases pos with
  | zero => simp [lbnd]
  | succ q =>
    simp only [lbnd]
    rw [if_neg (by omega), if_neg (by omega)]
    simp only [Nat.add_sub_cancel, List.length_cons]
    by_cases h : q < es.length
    · rw [dif_pos (by omega : q + 1 < es.length + 1), dif_pos h]
      simp
    · rw [dif_neg (by omega), dif_neg h]

private theorem rbnd_cons_zero' (hi : Option Nat) (e : Std.U64 × Std.U64)
    (es : List (Std.U64 × Std.U64)) :
    rbnd hi (e :: es) 0 = some e.1.val := by
  simp [rbnd]

private theorem rbnd_nil' (hi : Option Nat) (pos : Nat) :
    rbnd hi ([] : List (Std.U64 × Std.U64)) pos = hi := by
  simp [rbnd]

/-- `rbnd` at an in-range position is the entry key at that position. -/
theorem rbnd_eq_some (hi : Option Nat) (es : List (Std.U64 × Std.U64))
    (pos : Nat) (h : pos < es.length) :
    rbnd hi es pos = some ((es[pos]'h).1.val) := by
  simp [rbnd, h]

/-- `rbnd` past the end is the outer bound. -/
theorem rbnd_eq_hi (hi : Option Nat) (es : List (Std.U64 × Std.U64))
    (pos : Nat) (h : es.length ≤ pos) :
    rbnd hi es pos = hi := by
  simp only [rbnd]
  rw [dif_neg (by omega)]

/-- `lbnd` at a positive in-range position is the previous entry key. -/
theorem lbnd_eq_some (lo : Option Nat) (es : List (Std.U64 × Std.U64))
    (pos : Nat) (h0 : pos ≠ 0) (h : pos - 1 < es.length) :
    lbnd lo es pos = some ((es[pos - 1]'h).1.val) := by
  simp only [lbnd]
  rw [if_neg h0, dif_pos h]

/-- Taking the first `j` entries does not change the lower bound of slot `j`. -/
theorem lbnd_take (lo : Option Nat) (es : List (Std.U64 × Std.U64)) (j : Nat)
    (hj : j ≤ es.length) :
    lbnd lo (es.take j) j = lbnd lo es j := by
  by_cases h0 : j = 0
  · simp [lbnd, h0]
  · have h1 : j - 1 < (es.take j).length := by
      simp only [List.length_take]; omega
    have h2 : j - 1 < es.length := by omega
    rw [lbnd_eq_some lo (es.take j) j h0 h1, lbnd_eq_some lo es j h0 h2]
    simp

/-- Dropping the first `j + 1` entries turns slot `j + 1`'s upper bound into
    slot `0`'s. -/
theorem rbnd_drop (hi : Option Nat) (es : List (Std.U64 × Std.U64)) (j : Nat) :
    rbnd hi (es.drop (j + 1)) 0 = rbnd hi es (j + 1) := by
  by_cases h : j + 1 < es.length
  · have h0 : 0 < (es.drop (j + 1)).length := by
      simp only [List.length_drop]; omega
    rw [rbnd_eq_some hi (es.drop (j + 1)) 0 h0, rbnd_eq_some hi es (j + 1) h]
    simp
  · rw [rbnd_eq_hi hi (es.drop (j + 1)) 0 (by simp only [List.length_drop]; omega),
      rbnd_eq_hi hi es (j + 1) (by omega)]

/-! ## Pure list surgery helpers -/

private theorem set_eq_take_cons_drop {α : Type} (l : List α) (j : Nat) (a : α)
    (h : j < l.length) :
    l.set j a = l.take j ++ a :: l.drop (j + 1) := by
  induction l generalizing j with
  | nil => simp at h
  | cons x t ih =>
    cases j with
    | zero => simp
    | succ k =>
      simp only [List.set_cons_succ, List.take_succ_cons, List.drop_succ_cons,
        List.cons_append]
      rw [ih k (by simp only [List.length_cons] at h; omega)]

private theorem eraseIdx_eq_take_drop {α : Type} (l : List α) (j : Nat) :
    l.eraseIdx j = l.take j ++ l.drop (j + 1) := by
  induction l generalizing j with
  | nil => simp
  | cons x t ih =>
    cases j with
    | zero => simp
    | succ k =>
      simp only [List.eraseIdx_cons_succ, List.take_succ_cons,
        List.drop_succ_cons, List.cons_append]
      rw [ih k]

/-- Setting index `j` inside `l.take (j+1)` replaces the last kept element. -/
private theorem take_set_last {α : Type} (l : List α) (j : Nat) (a : α)
    (h : j < l.length) :
    (l.take (j + 1)).set j a = l.take j ++ [a] := by
  induction l generalizing j with
  | nil => simp at h
  | cons x t ih =>
    cases j with
    | zero => simp
    | succ k =>
      simp only [List.take_succ_cons, List.set_cons_succ, List.cons_append]
      rw [ih k (by simp only [List.length_cons] at h; omega)]

/-- Two adjacent `set`s expressed as take/cons/cons/drop. -/
private theorem set_set_adjacent {α : Type} (l : List α) (j : Nat) (a b : α)
    (h : j + 1 < l.length) :
    (l.set j a).set (j + 1) b = l.take j ++ a :: b :: l.drop (j + 2) := by
  induction l generalizing j with
  | nil => simp at h
  | cons x t ih =>
    cases j with
    | zero =>
      match t, h with
      | y :: t', _ => simp
    | succ k =>
      simp only [List.set_cons_succ, List.take_succ_cons, List.cons_append]
      have ht : (x :: t).drop (k + 1 + 2) = t.drop (k + 2) := by
        simp [List.drop_succ_cons]
      rw [ht, ih k (by simp only [List.length_cons] at h; omega)]

/-- A `set` followed by erasing the next index, as take/cons/drop. -/
private theorem set_eraseIdx_adjacent {α : Type} (l : List α) (j : Nat) (a : α)
    (h : j + 1 < l.length) :
    (l.set j a).eraseIdx (j + 1) = l.take j ++ a :: l.drop (j + 2) := by
  induction l generalizing j with
  | nil => simp at h
  | cons x t ih =>
    cases j with
    | zero =>
      match t, h with
      | y :: t', _ => simp
    | succ k =>
      simp only [List.set_cons_succ, List.eraseIdx_cons_succ,
        List.take_succ_cons, List.cons_append]
      have ht : (x :: t).drop (k + 1 + 2) = t.drop (k + 2) := by
        simp [List.drop_succ_cons]
      rw [ht, ih k (by simp only [List.length_cons] at h; omega)]

/-! ## Pure `Aligned` surgery -/

/-- MERGE / concatenation: the inverse of `Aligned.split`. Two aligned
    families joined by a separator entry form one aligned family. -/
theorem Aligned.append {lo hi : Option Nat} {es1 es2 : List (Std.U64 × Std.U64)}
    {cs1 cs2 : List Node} (e : Std.U64 × Std.U64)
    (h1 : Aligned lo (some e.1.val) es1 cs1)
    (h2 : Aligned (some e.1.val) hi es2 cs2) :
    Aligned lo hi (es1 ++ e :: es2) (cs1 ++ cs2) := by
  induction es1 generalizing lo cs1 with
  | nil =>
    cases h1 with
    | last c hc => simpa using Aligned.step e es2 c cs2 hc h2
  | cons e1 es1 ih =>
    cases h1 with
    | step _ _ c cs hc htail =>
      simpa using Aligned.step e1 (es1 ++ e :: es2) c (cs ++ cs2) hc (ih htail)

/-- Replace the LAST child of an aligned family, retargeting the family's
    upper bound to `hi'` (the last child is the only place `hi` occurs). -/
theorem Aligned.set_last {lo hi hi' : Option Nat}
    {es : List (Std.U64 × Std.U64)} {cs : List Node}
    (h : Aligned lo hi es cs) (c' : Node)
    (hc' : NodeInv (lbnd lo es es.length) hi' c') :
    Aligned lo hi' es (cs.set es.length c') := by
  induction es generalizing lo cs with
  | nil =>
    cases h with
    | last c hc =>
      rw [List.length_nil, lbnd_zero'] at hc'
      simpa using Aligned.last c' hc'
  | cons e es ih =>
    cases h with
    | step _ _ c cs hc htail =>
      rw [List.length_cons, lbnd_cons_succ'] at hc'
      simpa using Aligned.step e es c (cs.set es.length c') hc (ih htail hc')

/-- Replace the FIRST child of an aligned family, retargeting the family's
    lower bound to `lo'` (the first child is the only place `lo` occurs). -/
theorem Aligned.set_head {lo lo' hi : Option Nat}
    {es : List (Std.U64 × Std.U64)} {c : Node} {cs : List Node}
    (h : Aligned lo hi es (c :: cs)) (c' : Node)
    (hc' : NodeInv lo' (rbnd hi es 0) c') :
    Aligned lo' hi es (c' :: cs) := by
  cases h with
  | last _ hc =>
    rw [rbnd_nil'] at hc'
    exact Aligned.last c' hc'
  | step e es _ cs hc htail =>
    rw [rbnd_cons_zero'] at hc'
    exact Aligned.step e es c' cs hc' htail

/-- ROTATE shape: replace separator `j` by a new key `x` and the two children
    flanking it by `l`, `r` (bounded by `x` in the middle). -/
theorem Aligned.replace_pair {lo hi : Option Nat}
    {es : List (Std.U64 × Std.U64)} {cs : List Node}
    (hal : Aligned lo hi es cs) (j : Nat) (hj : j < es.length)
    (x : Std.U64 × Std.U64) (l r : Node)
    (hl : NodeInv (lbnd lo es j) (some x.1.val) l)
    (hr : NodeInv (some x.1.val) (rbnd hi es (j + 1)) r) :
    Aligned lo hi (es.set j x) (cs.take j ++ l :: r :: cs.drop (j + 2)) := by
  have hlen := hal.length_eq
  obtain ⟨A1, A2⟩ := hal.split j hj
  have htl : (es.take j).length = j := by
    simp only [List.length_take]; omega
  have hl' : NodeInv (lbnd lo (es.take j) (es.take j).length) (some x.1.val) l := by
    rw [htl, lbnd_take lo es j (by omega)]; exact hl
  have A1' := A1.set_last l hl'
  rw [htl, take_set_last cs j l (by omega)] at A1'
  have hcsd : cs.drop (j + 1) = (cs[j + 1]'(by omega)) :: cs.drop (j + 2) :=
    List.drop_eq_getElem_cons (by omega)
  rw [hcsd] at A2
  have hr' : NodeInv (some x.1.val) (rbnd hi (es.drop (j + 1)) 0) r := by
    rw [rbnd_drop]; exact hr
  have A2' := A2.set_head r hr'
  have hres := Aligned.append x A1' A2'
  rw [set_eq_take_cons_drop es j x hj]
  simpa using hres

/-- MERGE shape: erase separator `j` and replace the two children flanking it
    by the single merged node `m` spanning both slots. -/
theorem Aligned.merge_children {lo hi : Option Nat}
    {es : List (Std.U64 × Std.U64)} {cs : List Node}
    (hal : Aligned lo hi es cs) (j : Nat) (hj : j < es.length)
    (m : Node)
    (hm : NodeInv (lbnd lo es j) (rbnd hi es (j + 1)) m) :
    Aligned lo hi (es.eraseIdx j) (cs.take j ++ m :: cs.drop (j + 2)) := by
  have hlen := hal.length_eq
  obtain ⟨A1, A2⟩ := hal.split j hj
  have htl : (es.take j).length = j := by
    simp only [List.length_take]; omega
  have hm' : NodeInv (lbnd lo (es.take j) (es.take j).length)
      (rbnd hi es (j + 1)) m := by
    rw [htl, lbnd_take lo es j (by omega)]; exact hm
  have A1' := A1.set_last m hm'
  rw [htl, take_set_last cs j m (by omega)] at A1'
  by_cases hj1 : j + 1 < es.length
  · have hesd : es.drop (j + 1) = (es[j + 1]'hj1) :: es.drop (j + 2) :=
      List.drop_eq_getElem_cons hj1
    have hcsd : cs.drop (j + 1) = (cs[j + 1]'(by omega)) :: cs.drop (j + 2) :=
      List.drop_eq_getElem_cons (by omega)
    rw [hesd, hcsd] at A2
    cases A2 with
    | step _ _ c0 cs0 hc0 htail =>
      rw [rbnd_eq_some hi es (j + 1) hj1] at A1'
      have hres := Aligned.append (es[j + 1]'hj1) A1' htail
      rw [eraseIdx_eq_take_drop es j, hesd]
      simpa using hres
  · have hesd : es.drop (j + 1) = [] := List.drop_eq_nil_of_le (by omega)
    rw [rbnd_eq_hi hi es (j + 1) (by omega)] at A1'
    have h1 : es.eraseIdx j = es.take j := by
      rw [eraseIdx_eq_take_drop es j, hesd, List.append_nil]
    have h2 : cs.drop (j + 2) = [] := List.drop_eq_nil_of_le (by omega)
    rw [h1, h2]
    simpa using A1'

/-! ## `InB` widening and entry-list facts -/

/-- Widen an upper bound `some m` to any `hi` that lies above `m`. -/
theorem InB.widen_hi {lo hi : Option Nat} {m n : Nat}
    (h : InB lo (some m) n) (hm : ∀ u ∈ hi, m < u) : InB lo hi n := by
  obtain ⟨h1, h2⟩ := h
  refine ⟨h1, fun u hu => ?_⟩
  have := h2 m (by simp)
  have := hm u hu
  omega

/-- Widen a lower bound `some m` to any `lo` that lies below `m`. -/
theorem InB.widen_lo {lo hi : Option Nat} {m n : Nat}
    (h : InB (some m) hi n) (hm : ∀ l ∈ lo, l < m) : InB lo hi n := by
  obtain ⟨h1, h2⟩ := h
  refine ⟨fun l hl => ?_, h2⟩
  have := h1 m (by simp)
  have := hm l hl
  omega

/-- The separator entry at `j` lies strictly between slot `j`'s lower bound
    and slot `j + 1`'s upper bound. -/
theorem sep_InB {lo hi : Option Nat} (es : List (Std.U64 × Std.U64)) (j : Nat)
    (hj : j < es.length) (hs : SortedE es)
    (hb : ∀ e ∈ es, InB lo hi e.1.val) :
    InB (lbnd lo es j) (rbnd hi es (j + 1)) ((es[j]'hj).1.val) := by
  have hs' := List.pairwise_iff_getElem.mp hs
  constructor
  · intro l hl
    simp only [lbnd] at hl
    split at hl
    · exact (hb _ (es.getElem_mem hj)).1 l hl
    · rename_i h0
      rw [dif_pos (show j - 1 < es.length by omega)] at hl
      simp only [Option.mem_def, Option.some.injEq] at hl
      subst hl
      exact hs' (j - 1) j (by omega) hj (by omega)
  · intro u hu
    simp only [rbnd] at hu
    split at hu
    · rename_i hlt
      simp only [Option.mem_def, Option.some.injEq] at hu
      subst hu
      exact hs' j (j + 1) hj hlt (by omega)
    · exact (hb _ (es.getElem_mem hj)).2 u hu

/-- Replacing entry `j` by an entry whose key still separates the neighbors
    preserves sortedness (generalizes `SortedE_set`, which needs equal keys). -/
theorem SortedE_set_key (es : List (Std.U64 × Std.U64)) (j : Nat)
    (x : Std.U64 × Std.U64) (hs : SortedE es) (hj : j < es.length)
    (hlt : ∀ i, (h : i < es.length) → i < j → (es[i]'h).1.val < x.1.val)
    (hgt : ∀ i, (h : i < es.length) → j < i → x.1.val < (es[i]'h).1.val) :
    SortedE (es.set j x) := by
  have hs' := List.pairwise_iff_getElem.mp hs
  unfold SortedE
  rw [List.pairwise_iff_getElem]
  intro a b ha hb hab
  have ha' : a < es.length := by simpa using ha
  have hb' : b < es.length := by simpa using hb
  rw [List.getElem_set, List.getElem_set]
  by_cases h1 : j = a
  · subst h1
    rw [if_pos rfl, if_neg (by omega)]
    exact hgt b hb' (by omega)
  · rw [if_neg h1]
    by_cases h2 : j = b
    · subst h2
      rw [if_pos rfl]
      exact hlt a ha' (by omega)
    · rw [if_neg h2]
      exact hs' a b ha' hb' hab

/-- Sortedness and bounds of a merged entry list `le ++ S :: re`. -/
private theorem merged_entries_facts {L R : Option Nat}
    (le re : List (Std.U64 × Std.U64)) (S : Std.U64 × Std.U64)
    (hsl : SortedE le) (hsr : SortedE re)
    (hbl : ∀ e ∈ le, InB L (some S.1.val) e.1.val)
    (hbr : ∀ e ∈ re, InB (some S.1.val) R e.1.val)
    (hS : InB L R S.1.val) :
    SortedE (le ++ S :: re) ∧ ∀ e ∈ le ++ S :: re, InB L R e.1.val := by
  constructor
  · unfold SortedE
    rw [List.pairwise_append]
    refine ⟨hsl, ?_, ?_⟩
    · rw [List.pairwise_cons]
      exact ⟨fun e he => (hbr e he).1 S.1.val (by simp), hsr⟩
    · intro a ha b hb
      have haS : a.1.val < S.1.val := (hbl a ha).2 S.1.val (by simp)
      rcases List.mem_cons.mp hb with rfl | hb'
      · exact haS
      · have := (hbr b hb').1 S.1.val (by simp)
        omega
  · intro e he
    rcases List.mem_append.mp he with h1 | h2
    · exact (hbl e h1).widen_hi hS.2
    · rcases List.mem_cons.mp h2 with rfl | h3
      · exact hS
      · exact (hbr e h3).widen_lo hS.1

/-! ## `NodeInv` accessors / smart constructor -/

theorem NodeInv.entries_sorted {lo hi : Option Nat} {n : Node}
    (h : NodeInv lo hi n) : SortedE n.entries.val := by
  cases h with
  | leaf entries hs hb hlen => simpa using hs
  | internal entries c cs hs hb hlen hal => simpa using hs

theorem NodeInv.entries_bounds {lo hi : Option Nat} {n : Node}
    (h : NodeInv lo hi n) : ∀ e ∈ n.entries.val, InB lo hi e.1.val := by
  cases h with
  | leaf entries hs hb hlen => simpa using hb
  | internal entries c cs hs hb hlen hal => simpa using hb

theorem NodeInv.entries_len_le {lo hi : Option Nat} {n : Node}
    (h : NodeInv lo hi n) : n.entries.val.length ≤ 63 := by
  cases h with
  | leaf entries hs hb hlen => simpa using hlen
  | internal entries c cs hs hb hlen hal => simpa using hlen

theorem NodeInv.aligned_of_ne_nil {lo hi : Option Nat} {n : Node}
    (h : NodeInv lo hi n) (hne : clist n.children ≠ []) :
    Aligned lo hi n.entries.val (clist n.children) := by
  cases h with
  | leaf entries hs hb hlen => simp [clist] at hne
  | internal entries c cs hs hb hlen hal => simpa [clist] using hal

/-- Build `NodeInv` from list-level facts; the children are given by their
    `clist` view (empty = leaf, otherwise the alignment must hold). -/
theorem NodeInv.mk_node {lo hi : Option Nat}
    (entries : alloc.vec.Vec (Std.U64 × Std.U64)) (children : Children)
    (hs : SortedE entries.val)
    (hb : ∀ e ∈ entries.val, InB lo hi e.1.val)
    (hlen : entries.val.length ≤ 63)
    (hal : clist children = [] ∨ Aligned lo hi entries.val (clist children)) :
    NodeInv lo hi (Node.mk entries children) := by
  cases children with
  | Nil => exact NodeInv.leaf entries hs hb hlen
  | Cons c cs =>
    rcases hal with h | h
    · simp [clist] at h
    · exact NodeInv.internal entries c cs hs hb hlen (by simpa [clist] using h)

/-- Inversion for an aligned family with no entries: exactly one child,
    carrying the full interval. -/
theorem Aligned.nil_inv {lo hi : Option Nat} {cs : List Node}
    (h : Aligned lo hi [] cs) : ∃ c, cs = [c] ∧ NodeInv lo hi c := by
  cases h with
  | last c hc => exact ⟨c, rfl, hc⟩

/-! ## Node-level rebalancing lemmas (pure) -/

/-- Merging two adjacent same-height siblings through their separator yields
    a node satisfying the invariant on the combined interval, at the same
    height. -/
theorem merged_node_inv {L R : Option Nat} {h : Nat} {left right : Node}
    {S : Std.U64 × Std.U64}
    (hlinv : NodeInv L (some S.1.val) left)
    (hrinv : NodeInv (some S.1.val) R right)
    (hlh : HeightInv h left) (hrh : HeightInv h right)
    (hS : InB L R S.1.val)
    (hcap : left.entries.val.length + 1 + right.entries.val.length ≤ 63)
    (me : alloc.vec.Vec (Std.U64 × Std.U64)) (mc : Children)
    (hme : me.val = left.entries.val ++ S :: right.entries.val)
    (hmc : clist mc = clist left.children ++ clist right.children) :
    NodeInv L R (Node.mk me mc) ∧ HeightInv h (Node.mk me mc) := by
  obtain ⟨le, lc⟩ := left
  obtain ⟨re, rc⟩ := right
  simp only [Node.entries._simpLemma_, Node.children._simpLemma_]
    at hme hmc hcap
  obtain ⟨hsm, hbm⟩ := merged_entries_facts le.val re.val S
    (hlinv.entries_sorted) (hrinv.entries_sorted)
    (by simpa using hlinv.entries_bounds) (by simpa using hrinv.entries_bounds)
    hS
  have hlm : me.val.length ≤ 63 := by
    rw [hme]; simp only [List.length_append, List.length_cons]; omega
  cases hlh with
  | leaf _ =>
    cases hrh with
    | leaf _ =>
      have hmcnil : clist mc = [] := by simpa [clist] using hmc
      obtain rfl := (clist_eq_nil_iff mc).mp hmcnil
      exact ⟨NodeInv.leaf me (hme ▸ hsm) (hme ▸ hbm) hlm, HeightInv.leaf me⟩
  | internal _ c0 cs0 hch =>
    cases hrh with
    | internal _ c1 cs1 hch' =>
      have hall := hlinv.aligned_of_ne_nil (by simp [clist])
      have halr := hrinv.aligned_of_ne_nil (by simp [clist])
      simp only [Node.entries._simpLemma_, Node.children._simpLemma_]
        at hall halr
      have halm : Aligned L R me.val (clist mc) := by
        rw [hme, hmc]
        exact Aligned.append S hall halr
      refine ⟨NodeInv.mk_node me mc (hme ▸ hsm) (hme ▸ hbm) hlm (Or.inr halm), ?_⟩
      refine HeightInv.mk_internal me mc ?_ ?_
      · rw [hmc]; simp [clist]
      · rw [hmc]
        exact hch.allH.append hch'.allH

/-- Rotate-right at the node level: the left sibling donates its last entry
    (which becomes the new separator `stolen`) and its last child to the
    right sibling; both rebuilt nodes satisfy the invariant on the two
    sub-intervals split at `stolen`, at the same height. -/
theorem rotate_right_nodes {L R : Option Nat} {h : Nat} {left right : Node}
    {S : Std.U64 × Std.U64}
    (hlinv : NodeInv L (some S.1.val) left)
    (hrinv : NodeInv (some S.1.val) R right)
    (hlh : HeightInv h left) (hrh : HeightInv h right)
    (hS : InB L R S.1.val)
    (hll : 0 < left.entries.val.length)
    (hrl : right.entries.val.length < 63)
    (stolen : Std.U64 × Std.U64)
    (hstolen : left.entries.val[left.entries.val.length - 1]? = some stolen)
    (nle nre : alloc.vec.Vec (Std.U64 × Std.U64)) (nlc nrc : Children)
    (hnle : nle.val = left.entries.val.take (left.entries.val.length - 1))
    (hnre : nre.val = S :: right.entries.val)
    (hnlc : clist nlc =
      (clist left.children).take ((clist left.children).length - 1))
    (hnrc : clist nrc =
      (clist left.children).drop ((clist left.children).length - 1)
        ++ clist right.children) :
    NodeInv L (some stolen.1.val) (Node.mk nle nlc)
    ∧ NodeInv (some stolen.1.val) R (Node.mk nre nrc)
    ∧ HeightInv h (Node.mk nle nlc) ∧ HeightInv h (Node.mk nre nrc)
    ∧ InB L (some S.1.val) stolen.1.val := by
  obtain ⟨le, lc⟩ := left
  obtain ⟨re, rc⟩ := right
  simp only [Node.entries._simpLemma_, Node.children._simpLemma_]
    at hll hrl hstolen hnle hnre hnlc hnrc
  have hsl := hlinv.entries_sorted
  have hbl := hlinv.entries_bounds
  have hsr := hrinv.entries_sorted
  have hbr := hrinv.entries_bounds
  simp only [Node.entries._simpLemma_] at hsl hbl hsr hbr
  have hlt1 : le.val.length - 1 < le.val.length := by omega
  have hst : le.val[le.val.length - 1]'hlt1 = stolen := by
    rw [List.getElem?_eq_getElem hlt1] at hstolen
    simpa using hstolen
  have hstInB : InB L (some S.1.val) stolen.1.val := by
    rw [← hst]; exact hbl _ (le.val.getElem_mem hlt1)
  -- new left entry facts
  have hsnl : SortedE nle.val := by rw [hnle]; exact SortedE_take _ _ hsl
  have hbnl : ∀ e ∈ nle.val, InB L (some stolen.1.val) e.1.val := by
    rw [hnle]
    intro e he
    refine (hbl e (List.mem_of_mem_take he)).tighten_hi ?_
    have := mem_take_lt_median le.val (le.val.length - 1) hlt1 hsl e he
    rwa [hst] at this
  have hlnl : nle.val.length ≤ 63 := by
    rw [hnle]; simp only [List.length_take]
    have := hlinv.entries_len_le
    simp only [Node.entries._simpLemma_] at this
    omega
  -- new right entry facts
  have hsnr : SortedE nre.val := by
    rw [hnre]
    unfold SortedE
    rw [List.pairwise_cons]
    exact ⟨fun e he => (hbr e he).1 S.1.val (by simp), hsr⟩
  have hstoltS : stolen.1.val < S.1.val := hstInB.2 S.1.val (by simp)
  have hSin : InB (some stolen.1.val) R S.1.val :=
    ⟨fun l hl => by
      simp only [Option.mem_def, Option.some.injEq] at hl
      subst hl
      exact hstoltS, hS.2⟩
  have hbnr : ∀ e ∈ nre.val, InB (some stolen.1.val) R e.1.val := by
    rw [hnre]
    intro e he
    rcases List.mem_cons.mp he with heq | he'
    · rw [heq]; exact hSin
    · exact (hbr e he').widen_lo
        (fun l hl => by
          simp only [Option.mem_def, Option.some.injEq] at hl
          subst hl
          exact hstoltS)
  have hlnr : nre.val.length ≤ 63 := by
    rw [hnre]; simp only [List.length_cons]; omega
  cases hlh with
  | leaf _ =>
    cases hrh with
    | leaf _ =>
      have hnlcnil : clist nlc = [] := by simpa [clist] using hnlc
      have hnrcnil : clist nrc = [] := by simpa [clist] using hnrc
      obtain rfl := (clist_eq_nil_iff nlc).mp hnlcnil
      obtain rfl := (clist_eq_nil_iff nrc).mp hnrcnil
      exact ⟨NodeInv.leaf nle hsnl hbnl hlnl,
             NodeInv.leaf nre hsnr hbnr hlnr,
             HeightInv.leaf nle, HeightInv.leaf nre, hstInB⟩
  | @internal h0 _ c0 cs0 hch =>
    cases hrh with
    | internal _ c1 cs1 hch' =>
      have hall := hlinv.aligned_of_ne_nil (by simp [clist])
      have halr := hrinv.aligned_of_ne_nil (by simp [clist])
      simp only [Node.entries._simpLemma_, Node.children._simpLemma_]
        at hall halr
      have hclen : (clist (Children.Cons c0 cs0)).length = le.val.length + 1 :=
        hall.length_eq
      obtain ⟨A1, A2⟩ := hall.split (le.val.length - 1) hlt1
      rw [hst] at A1 A2
      have hll1 : le.val.length - 1 + 1 = le.val.length := by omega
      rw [hll1] at A1 A2
      have hdrople : le.val.drop le.val.length = [] := by simp
      rw [hdrople] at A2
      obtain ⟨cl, hcleq, hclinv⟩ := Aligned.nil_inv A2
      -- children lists in terms of the split
      have hlcs1 : (clist (Children.Cons c0 cs0)).length - 1 = le.val.length := by
        omega
      rw [hlcs1] at hnlc hnrc
      rw [hcleq] at hnrc
      -- new left
      have hnl_inv : NodeInv L (some stolen.1.val) (Node.mk nle nlc) :=
        NodeInv.mk_node nle nlc hsnl hbnl hlnl
          (Or.inr (by rw [hnlc, hnle]; exact A1))
      -- new right
      have hnr_al : Aligned (some stolen.1.val) R nre.val (clist nrc) := by
        rw [hnre, hnrc]
        exact Aligned.step S re.val cl _ hclinv halr
      have hnr_inv : NodeInv (some stolen.1.val) R (Node.mk nre nrc) :=
        NodeInv.mk_node nre nrc hsnr hbnr hlnr (Or.inr hnr_al)
      -- heights
      have hclmem : cl ∈ clist (Children.Cons c0 cs0) :=
        List.mem_of_mem_drop (hcleq ▸ List.mem_singleton.mpr rfl)
      have hnl_h : HeightInv (h0 + 1) (Node.mk nle nlc) := by
        refine HeightInv.mk_internal nle nlc ?_ ?_
        · apply List.ne_nil_of_length_pos
          rw [hnlc]
          simp only [List.length_take]
          omega
        · rw [hnlc]
          exact hch.allH.take _
      have hnr_h : HeightInv (h0 + 1) (Node.mk nre nrc) := by
        refine HeightInv.mk_internal nre nrc ?_ ?_
        · rw [hnrc]; simp
        · rw [hnrc]
          exact AllH.cons (hch.allH cl hclmem) hch'.allH
      exact ⟨hnl_inv, hnr_inv, hnl_h, hnr_h, hstInB⟩

/-- Rotate-left at the node level: the right sibling donates its first entry
    (which becomes the new separator `stolen`) and its first child to the
    left sibling; both rebuilt nodes satisfy the invariant on the two
    sub-intervals split at `stolen`, at the same height. -/
theorem rotate_left_nodes {L R : Option Nat} {h : Nat} {left right : Node}
    {S : Std.U64 × Std.U64}
    (hlinv : NodeInv L (some S.1.val) left)
    (hrinv : NodeInv (some S.1.val) R right)
    (hlh : HeightInv h left) (hrh : HeightInv h right)
    (hS : InB L R S.1.val)
    (hrl0 : 0 < right.entries.val.length)
    (hll : left.entries.val.length < 63)
    (stolen : Std.U64 × Std.U64)
    (hstolen : right.entries.val[0]? = some stolen)
    (nle nre : alloc.vec.Vec (Std.U64 × Std.U64)) (nlc nrc : Children)
    (hnle : nle.val = left.entries.val ++ [S])
    (hnre : nre.val = right.entries.val.drop 1)
    (hnlc : clist nlc = clist left.children ++ (clist right.children).take 1)
    (hnrc : clist nrc = (clist right.children).drop 1) :
    NodeInv L (some stolen.1.val) (Node.mk nle nlc)
    ∧ NodeInv (some stolen.1.val) R (Node.mk nre nrc)
    ∧ HeightInv h (Node.mk nle nlc) ∧ HeightInv h (Node.mk nre nrc)
    ∧ InB (some S.1.val) R stolen.1.val := by
  obtain ⟨le, lc⟩ := left
  obtain ⟨re, rc⟩ := right
  simp only [Node.entries._simpLemma_, Node.children._simpLemma_]
    at hrl0 hll hstolen hnle hnre hnlc hnrc
  have hsl := hlinv.entries_sorted
  have hbl := hlinv.entries_bounds
  have hsr := hrinv.entries_sorted
  have hbr := hrinv.entries_bounds
  simp only [Node.entries._simpLemma_] at hsl hbl hsr hbr
  have hst : re.val[0]'hrl0 = stolen := by
    rw [List.getElem?_eq_getElem hrl0] at hstolen
    simpa using hstolen
  have hstInB : InB (some S.1.val) R stolen.1.val := by
    rw [← hst]; exact hbr _ (re.val.getElem_mem hrl0)
  have hSltst : S.1.val < stolen.1.val := hstInB.1 S.1.val (by simp)
  -- new left entry facts
  have hsnl : SortedE nle.val := by
    rw [hnle]
    unfold SortedE
    rw [List.pairwise_append]
    refine ⟨hsl, by simp [SortedE], ?_⟩
    intro a ha b hb
    rw [List.mem_singleton.mp hb]
    exact (hbl a ha).2 S.1.val (by simp)
  have hbnl : ∀ e ∈ nle.val, InB L (some stolen.1.val) e.1.val := by
    rw [hnle]
    intro e he
    rcases List.mem_append.mp he with h1 | h2
    · exact (hbl e h1).widen_hi
        (fun u hu => by
          simp only [Option.mem_def, Option.some.injEq] at hu
          subst hu
          exact hSltst)
    · rw [List.mem_singleton.mp h2]
      exact ⟨hS.1, fun u hu => by
        simp only [Option.mem_def, Option.some.injEq] at hu
        subst hu
        exact hSltst⟩
  have hlnl : nle.val.length ≤ 63 := by
    rw [hnle]; simp only [List.length_append, List.length_cons,
      List.length_nil]; omega
  -- new right entry facts
  have hsnr : SortedE nre.val := by rw [hnre]; exact SortedE_drop _ _ hsr
  have hbnr : ∀ e ∈ nre.val, InB (some stolen.1.val) R e.1.val := by
    rw [hnre]
    intro e he
    refine ⟨fun l hl => ?_, ((hbr e (List.mem_of_mem_drop he)).2)⟩
    simp only [Option.mem_def, Option.some.injEq] at hl
    subst hl
    have := mem_drop_gt_median re.val 0 hrl0 hsr e (by simpa using he)
    rwa [hst] at this
  have hlnr : nre.val.length ≤ 63 := by
    rw [hnre]; simp only [List.length_drop]
    have := hrinv.entries_len_le
    simp only [Node.entries._simpLemma_] at this
    omega
  cases hlh with
  | leaf _ =>
    cases hrh with
    | leaf _ =>
      have hnlcnil : clist nlc = [] := by simpa [clist] using hnlc
      have hnrcnil : clist nrc = [] := by simpa [clist] using hnrc
      obtain rfl := (clist_eq_nil_iff nlc).mp hnlcnil
      obtain rfl := (clist_eq_nil_iff nrc).mp hnrcnil
      exact ⟨NodeInv.leaf nle hsnl hbnl hlnl,
             NodeInv.leaf nre hsnr hbnr hlnr,
             HeightInv.leaf nle, HeightInv.leaf nre, hstInB⟩
  | @internal h0 _ c0 cs0 hch =>
    cases hrh with
    | internal _ c1 cs1 hch' =>
      have hall := hlinv.aligned_of_ne_nil (by simp [clist])
      have halr := hrinv.aligned_of_ne_nil (by simp [clist])
      simp only [Node.entries._simpLemma_, Node.children._simpLemma_]
        at hall halr
      have hrclen : (clist (Children.Cons c1 cs1)).length = re.val.length + 1 :=
        halr.length_eq
      -- the donated child: first child of the right sibling
      obtain ⟨hc0lt, hrc0inv⟩ := halr.child 0 (by omega)
      rw [lbnd_zero', rbnd_eq_some R re.val 0 hrl0, hst] at hrc0inv
      have hget0 : (clist (Children.Cons c1 cs1))[0]'hc0lt = c1 := by
        simp [clist]
      rw [hget0] at hrc0inv
      -- hrc0inv : NodeInv (some S.1.val) (some stolen.1.val) c1
      have htake1 : (clist (Children.Cons c1 cs1)).take 1 = [c1] := by
        simp [clist]
      rw [htake1] at hnlc
      -- new left
      have hnl_al : Aligned L (some stolen.1.val) nle.val (clist nlc) := by
        rw [hnle, hnlc]
        exact Aligned.append S hall (Aligned.last c1 hrc0inv)
      have hnl_inv : NodeInv L (some stolen.1.val) (Node.mk nle nlc) :=
        NodeInv.mk_node nle nlc hsnl hbnl hlnl (Or.inr hnl_al)
      -- new right
      obtain ⟨-, A2⟩ := halr.split 0 hrl0
      rw [hst] at A2
      have hnr_inv : NodeInv (some stolen.1.val) R (Node.mk nre nrc) :=
        NodeInv.mk_node nre nrc hsnr hbnr hlnr
          (Or.inr (by rw [hnre, hnrc]; simpa using A2))
      -- heights
      have hc1h : HeightInv h0 c1 := hch'.allH c1 (by simp [clist])
      have hnl_h : HeightInv (h0 + 1) (Node.mk nle nlc) := by
        refine HeightInv.mk_internal nle nlc ?_ ?_
        · rw [hnlc]; simp [clist]
        · rw [hnlc]
          exact hch.allH.append
            (fun n hn => by rw [List.mem_singleton.mp hn]; exact hc1h)
      have hnr_h : HeightInv (h0 + 1) (Node.mk nre nrc) := by
        refine HeightInv.mk_internal nre nrc ?_ ?_
        · rw [hnrc]
          apply List.ne_nil_of_length_pos
          simp only [List.length_drop]
          omega
        · rw [hnrc]
          exact hch'.allH.drop 1
      exact ⟨hnl_inv, hnr_inv, hnl_h, hnr_h, hstInB⟩

/-! ## Monadic surgery lemmas -/

theorem NodeInv.children_len_le {lo hi : Option Nat} {n : Node}
    (h : NodeInv lo hi n) : (clist n.children).length ≤ 64 := by
  cases h with
  | leaf entries hs hb hlen => simp [clist]
  | internal entries c cs hs hb hlen hal =>
    have hle := hal.length_eq
    simp only [Node.children._simpLemma_, clist]
    simp only [List.length_cons] at hle ⊢
    omega

unseal MIN_KEYS T in
@[step] theorem MIN_KEYS_spec : MIN_KEYS ⦃ m => m.val = 31 ⦄ := by
  simp only [MIN_KEYS, T]
  step*

/-- Destructure a parent satisfying both invariants: sortedness, bounds,
    arity, full child alignment, uniform child height, and the length link. -/
theorem parent_facts {lo hi : Option Nat} {h : Nat}
    (entries : alloc.vec.Vec (Std.U64 × Std.U64)) (children : Children)
    (hinv : NodeInv lo hi (Node.mk entries children))
    (hh : HeightInv (h + 1) (Node.mk entries children)) :
    SortedE entries.val ∧ (∀ e ∈ entries.val, InB lo hi e.1.val)
    ∧ entries.val.length ≤ 63
    ∧ Aligned lo hi entries.val (clist children)
    ∧ AllH h (clist children)
    ∧ (clist children).length = entries.val.length + 1 := by
  have hne := hh.children_ne_nil
  have hal := hinv.aligned_of_ne_nil (by simpa using hne)
  simp only [Node.entries._simpLemma_, Node.children._simpLemma_] at hal
  refine ⟨?_, ?_, ?_, hal, hh.children_allH, hal.length_eq⟩
  · simpa using hinv.entries_sorted
  · simpa using hinv.entries_bounds
  · simpa using hinv.entries_len_le

/-- Reassemble a parent from list-level facts about the rebalanced family. -/
theorem parent_assemble {lo hi : Option Nat} {h : Nat}
    (entries : alloc.vec.Vec (Std.U64 × Std.U64)) (children : Children)
    (hs : SortedE entries.val) (hb : ∀ e ∈ entries.val, InB lo hi e.1.val)
    (hlen : entries.val.length ≤ 63)
    (hal : Aligned lo hi entries.val (clist children))
    (hne : clist children ≠ []) (hall : AllH h (clist children)) :
    NodeInv lo hi (Node.mk entries children) ∧
    HeightInv (h + 1) (Node.mk entries children) :=
  ⟨NodeInv.mk_node entries children hs hb hlen (Or.inr hal),
   HeightInv.mk_internal entries children hne hall⟩

/-- `rotate_right entries children idx` (steal from the left sibling of child
    `idx`) preserves `NodeInv` and `HeightInv` of the parent family.
    Side conditions: `idx` is a non-leftmost valid child slot, the left
    sibling is non-empty (it donates an entry), and child `idx` is not full
    (it receives one). -/
theorem rotate_right_inv {lo hi : Option Nat} {h : Nat}
    (entries : alloc.vec.Vec (Std.U64 × Std.U64)) (children : Children)
    (idx : Std.Usize)
    (hinv : NodeInv lo hi (Node.mk entries children))
    (hh : HeightInv (h + 1) (Node.mk entries children))
    (hidx0 : 0 < idx.val) (hidx : idx.val ≤ entries.val.length)
    (hlsz : ∀ n, (clist children)[idx.val - 1]? = some n →
      0 < n.entries.val.length)
    (hrsz : ∀ n, (clist children)[idx.val]? = some n →
      n.entries.val.length < 63) :
    rotate_right entries children idx ⦃ out =>
      NodeInv lo hi (Node.mk out.1 out.2) ∧
      HeightInv (h + 1) (Node.mk out.1 out.2) ⦄ := by
  obtain ⟨hs, hb, hlen, hal, hall, hclen⟩ := parent_facts entries children hinv hh
  have hj : idx.val - 1 < entries.val.length := by omega
  have hj1 : idx.val - 1 + 1 = idx.val := by omega
  have hchlt : idx.val < (clist children).length := by omega
  have hchlt' : idx.val - 1 < (clist children).length := by omega
  rw [rotate_right]
  -- i ← idx - 1
  obtain ⟨i, hisub, hivp⟩ := WP.spec_imp_exists
    (Usize.sub_spec (x := idx) (y := 1#usize) (by scalar_tac))
  have hiv : i.val = idx.val - 1 := by scalar_tac
  -- left ← child_at children i
  obtain ⟨left, hleft, hleftv⟩ := WP.spec_imp_exists
    (child_at_spec children i (by omega))
  simp only [hiv] at hleftv
  -- right ← child_at children idx
  obtain ⟨right, hright, hrightv⟩ := WP.spec_imp_exists
    (child_at_spec children idx hchlt)
  -- sibling invariants at their slots
  obtain ⟨hlt0, hlinv0⟩ := hal.child (idx.val - 1) (by omega)
  obtain ⟨hrt0, hrinv0⟩ := hal.child idx.val hidx
  rw [rbnd_eq_some hi entries.val (idx.val - 1) hj] at hlinv0
  rw [lbnd_eq_some lo entries.val idx.val (by omega) hj] at hrinv0
  rw [← hleftv] at hlinv0
  rw [← hrightv] at hrinv0
  have hlh : HeightInv h left := by
    rw [hleftv]; exact hall.getElem _ hchlt'
  have hrh : HeightInv h right := by
    rw [hrightv]; exact hall.getElem _ hchlt
  -- sizes of the siblings
  have hll0 : 0 < left.entries.val.length := by
    apply hlsz left
    rw [List.getElem?_eq_getElem hchlt', hleftv]
  have hrl : right.entries.val.length < 63 := by
    apply hrsz right
    rw [List.getElem?_eq_getElem hchlt, hrightv]
  have hlle := hlinv0.entries_len_le
  have hlcl := hlinv0.children_len_le
  -- ll := Vec.len left.entries; i1 ← ll - 1
  obtain ⟨i1, hi1, hi1p⟩ := WP.spec_imp_exists
    (Usize.sub_spec (x := alloc.vec.Vec.len left.entries) (y := 1#usize)
      (by have := alloc.vec.Vec.len_val left.entries; scalar_tac))
  have hi1v : i1.val = left.entries.val.length - 1 := by
    have := alloc.vec.Vec.len_val left.entries
    scalar_tac
  -- stolen ← left.entries[i1]
  obtain ⟨stolen, hstol, hstolv⟩ := WP.spec_imp_exists
    (alloc.vec.Vec.index_usize_spec left.entries i1 (by scalar_tac))
  -- separator ← entries[i]
  obtain ⟨separator, hsep, hsepv⟩ := WP.spec_imp_exists
    (alloc.vec.Vec.index_usize_spec entries i (by scalar_tac))
  simp only [hiv] at hsepv
  rw [← hsepv] at hlinv0 hrinv0
  -- new_entries ← replace_entry entries i stolen
  obtain ⟨new_entries, hne', hnev⟩ := WP.spec_imp_exists
    (replace_entry_spec entries i stolen)
  simp only [hiv] at hnev
  -- new_left_entries ← entries_prefix left.entries i1
  obtain ⟨nle, hnle', hnlev⟩ := WP.spec_imp_exists
    (entries_prefix_spec left.entries i1 (by scalar_tac))
  simp only [hi1v] at hnlev
  -- new_right_entries ← insert_entry_at right.entries 0 separator
  obtain ⟨nre, hnre', hnrev⟩ := WP.spec_imp_exists
    (insert_entry_at_spec right.entries 0#usize separator (by scalar_tac)
      (by scalar_tac))
  have hnrev' : nre.val = separator :: right.entries.val := by
    simpa using hnrev
  -- new_left_children ← drop_last_child left.children
  obtain ⟨nlc, hnlc', hnlcv⟩ := WP.spec_imp_exists
    (drop_last_child_spec left.children (by scalar_tac))
  -- moved ← last_child_singleton left.children
  obtain ⟨moved, hmv', hmvv⟩ := WP.spec_imp_exists
    (last_child_singleton_spec left.children (by scalar_tac))
  -- new_right_children ← concat_children moved right.children
  obtain ⟨nrc, hnrc', hnrcv⟩ := WP.spec_imp_exists
    (concat_children_spec moved right.children)
  rw [hmvv] at hnrcv
  -- children1 ← replace_child children i (mk nle nlc)
  obtain ⟨children1, hch1, hch1v⟩ := WP.spec_imp_exists
    (replace_child_spec children i (Node.mk nle nlc))
  simp only [hiv] at hch1v
  -- children2 ← replace_child children1 idx (mk nre nrc)
  obtain ⟨children2, hch2, hch2v⟩ := WP.spec_imp_exists
    (replace_child_spec children1 idx (Node.mk nre nrc))
  rw [hch1v] at hch2v
  -- reduce the do-block
  simp only [alloc.vec.Vec.index_slice_index, hisub, hleft, hright, hi1, hstol,
    hsep, hne', hnle', hnre', hnlc', hmv', hnrc', hch1, hch2, bind_tc_ok,
    WP.spec_ok]
  -- pure surgery: the two rebuilt siblings
  have hstolen? : left.entries.val[left.entries.val.length - 1]? = some stolen := by
    rw [← hi1v, List.getElem?_eq_getElem (by scalar_tac), hstolv]
  obtain ⟨hnlinv, hnrinv, hnlh, hnrh, hstInB⟩ :=
    rotate_right_nodes (S := separator) hlinv0 hrinv0 hlh hrh
      (by
        have := sep_InB entries.val (idx.val - 1) hj hs hb
        rw [hj1, ← hsepv] at this
        exact this)
      hll0 hrl stolen hstolen? nle nre nlc nrc
      (by rw [hnlev]) hnrev' (by rw [hnlcv]) hnrcv
  -- separator key bracket for the new parent entries
  have hsepInB : InB (lbnd lo entries.val (idx.val - 1))
      (rbnd hi entries.val (idx.val - 1)) stolen.1.val := by
    rw [rbnd_eq_some hi entries.val (idx.val - 1) hj, ← hsepv]
    exact hstInB
  obtain ⟨hltst, hgtst⟩ := bracket_of_InB_bnd entries.val (idx.val - 1)
    stolen.1.val hs (by omega) hsepInB
  -- assemble the parent
  have hnev' : new_entries.val = entries.val.set (idx.val - 1) stolen := hnev
  have hch2v' : clist children2 = (clist children).take (idx.val - 1)
      ++ Node.mk nle nlc :: Node.mk nre nrc :: (clist children).drop (idx.val - 1 + 2) := by
    rw [hch2v, ← hj1]
    exact set_set_adjacent (clist children) (idx.val - 1) _ _ (by omega)
  refine parent_assemble new_entries children2 ?_ ?_ ?_ ?_ ?_ ?_
  · rw [hnev']
    exact SortedE_set_key entries.val (idx.val - 1) stolen hs hj
      hltst (fun k hk hgt => hgtst k hk (by omega))
  · rw [hnev']
    exact InB_set entries.val (idx.val - 1) stolen lo hi hb
      (InB_outer_of_bnd entries.val (idx.val - 1) stolen.1.val (by omega)
        hb hsepInB)
  · rw [hnev']
    simpa using hlen
  · rw [hnev', hch2v']
    refine hal.replace_pair (idx.val - 1) hj stolen _ _ hnlinv ?_
    rw [hj1]
    exact hnrinv
  · rw [hch2v']
    simp
  · rw [hch2v']
    exact ((hall.take _).append
      (AllH.cons hnlh (AllH.cons hnrh (hall.drop _))))
/-- `rotate_left entries children idx` (steal from the right sibling of child
    `idx`) preserves `NodeInv` and `HeightInv` of the parent family.
    Side conditions: slot `idx + 1` exists, the right sibling is non-empty
    (it donates an entry), and child `idx` is not full (it receives one). -/
theorem rotate_left_inv {lo hi : Option Nat} {h : Nat}
    (entries : alloc.vec.Vec (Std.U64 × Std.U64)) (children : Children)
    (idx : Std.Usize)
    (hinv : NodeInv lo hi (Node.mk entries children))
    (hh : HeightInv (h + 1) (Node.mk entries children))
    (hidx : idx.val < entries.val.length)
    (hrsz : ∀ n, (clist children)[idx.val + 1]? = some n →
      0 < n.entries.val.length)
    (hlsz : ∀ n, (clist children)[idx.val]? = some n →
      n.entries.val.length < 63) :
    rotate_left entries children idx ⦃ out =>
      NodeInv lo hi (Node.mk out.1 out.2) ∧
      HeightInv (h + 1) (Node.mk out.1 out.2) ⦄ := by
  obtain ⟨hs, hb, hlen, hal, hall, hclen⟩ := parent_facts entries children hinv hh
  have hchlt : idx.val < (clist children).length := by omega
  have hchlt1 : idx.val + 1 < (clist children).length := by omega
  rw [rotate_left]
  -- left ← child_at children idx
  obtain ⟨left, hleft, hleftv⟩ := WP.spec_imp_exists
    (child_at_spec children idx hchlt)
  -- i ← idx + 1
  obtain ⟨i, hiadd, hivp⟩ := WP.spec_imp_exists
    (Usize.add_spec (x := idx) (y := 1#usize) (by scalar_tac))
  have hiv : i.val = idx.val + 1 := by scalar_tac
  -- right ← child_at children i
  obtain ⟨right, hright, hrightv⟩ := WP.spec_imp_exists
    (child_at_spec children i (by omega))
  simp only [hiv] at hrightv
  -- sibling invariants at their slots
  obtain ⟨hlt0, hlinv0⟩ := hal.child idx.val (by omega)
  obtain ⟨hrt0, hrinv0⟩ := hal.child (idx.val + 1) (by omega)
  rw [rbnd_eq_some hi entries.val idx.val hidx] at hlinv0
  rw [lbnd_eq_some lo entries.val (idx.val + 1) (by omega)
    (by simpa using hidx)] at hrinv0
  simp only [Nat.add_sub_cancel] at hrinv0
  rw [← hleftv] at hlinv0
  rw [← hrightv] at hrinv0
  have hlh : HeightInv h left := by
    rw [hleftv]; exact hall.getElem _ hchlt
  have hrh : HeightInv h right := by
    rw [hrightv]; exact hall.getElem _ hchlt1
  -- sizes of the siblings
  have hrl0 : 0 < right.entries.val.length := by
    apply hrsz right
    rw [List.getElem?_eq_getElem hchlt1, hrightv]
  have hll : left.entries.val.length < 63 := by
    apply hlsz left
    rw [List.getElem?_eq_getElem hchlt, hleftv]
  -- stolen ← right.entries[0]
  obtain ⟨stolen, hstol, hstolv⟩ := WP.spec_imp_exists
    (alloc.vec.Vec.index_usize_spec right.entries 0#usize (by scalar_tac))
  -- separator ← entries[idx]
  obtain ⟨separator, hsep, hsepv⟩ := WP.spec_imp_exists
    (alloc.vec.Vec.index_usize_spec entries idx (by scalar_tac))
  rw [← hsepv] at hlinv0 hrinv0
  -- new_entries ← replace_entry entries idx stolen
  obtain ⟨new_entries, hnee, hnev⟩ := WP.spec_imp_exists
    (replace_entry_spec entries idx stolen)
  -- new_right_entries ← entries_suffix right.entries 1
  obtain ⟨nre, hnree, hnrev⟩ := WP.spec_imp_exists
    (entries_suffix_spec right.entries 1#usize)
  have hnrev' : nre.val = right.entries.val.drop 1 := by simpa using hnrev
  -- new_left_entries ← insert_entry_at left.entries (len left.entries) separator
  obtain ⟨nle, hnlee, hnlev⟩ := WP.spec_imp_exists
    (insert_entry_at_spec left.entries (alloc.vec.Vec.len left.entries)
      separator
      (by have := alloc.vec.Vec.len_val left.entries; scalar_tac)
      (by scalar_tac))
  have hnlev' : nle.val = left.entries.val ++ [separator] := by
    rw [hnlev]
    simp [alloc.vec.Vec.len_val]
  -- moved ← children_prefix right.children 1
  obtain ⟨moved, hmve, hmvv⟩ := WP.spec_imp_exists
    (children_prefix_spec right.children 1#usize)
  have hmvv' : clist moved = (clist right.children).take 1 := by simpa using hmvv
  -- new_left_children ← concat_children left.children moved
  obtain ⟨nlc, hnlce, hnlcv⟩ := WP.spec_imp_exists
    (concat_children_spec left.children moved)
  rw [hmvv'] at hnlcv
  -- new_right_children ← children_suffix right.children 1
  obtain ⟨nrc, hnrce, hnrcv⟩ := WP.spec_imp_exists
    (children_suffix_spec right.children 1#usize)
  have hnrcv' : clist nrc = (clist right.children).drop 1 := by simpa using hnrcv
  -- children1 ← replace_child children idx (mk nle nlc)
  obtain ⟨children1, hch1, hch1v⟩ := WP.spec_imp_exists
    (replace_child_spec children idx (Node.mk nle nlc))
  -- children2 ← replace_child children1 i (mk nre nrc)
  obtain ⟨children2, hch2, hch2v⟩ := WP.spec_imp_exists
    (replace_child_spec children1 i (Node.mk nre nrc))
  simp only [hiv] at hch2v
  rw [hch1v] at hch2v
  -- reduce the do-block
  simp only [alloc.vec.Vec.index_slice_index, hleft, hiadd, hright, hstol,
    hsep, hnee, hnree, hnlee, hmve, hnlce, hnrce, hch1, hch2, bind_tc_ok,
    WP.spec_ok]
  -- pure surgery: the two rebuilt siblings
  have hstolen? : right.entries.val[0]? = some stolen := by
    rw [List.getElem?_eq_getElem hrl0, hstolv]
    simp
  obtain ⟨hnlinv, hnrinv, hnlh, hnrh, hstInB⟩ :=
    rotate_left_nodes (S := separator) hlinv0 hrinv0 hlh hrh
      (by
        have := sep_InB entries.val idx.val hidx hs hb
        rw [← hsepv] at this
        exact this)
      hrl0 hll stolen hstolen? nle nre nlc nrc
      hnlev' hnrev' hnlcv hnrcv'
  -- stolen key bracket at pos idx+1
  have hstb : InB (lbnd lo entries.val (idx.val + 1))
      (rbnd hi entries.val (idx.val + 1)) stolen.1.val := by
    rw [lbnd_eq_some lo entries.val (idx.val + 1) (by omega)
      (by simpa using hidx)]
    simp only [Nat.add_sub_cancel]
    rw [← hsepv]
    exact hstInB
  obtain ⟨hltst, hgtst⟩ := bracket_of_InB_bnd entries.val (idx.val + 1)
    stolen.1.val hs (by omega) hstb
  have hch2v' : clist children2 = (clist children).take idx.val
      ++ Node.mk nle nlc :: Node.mk nre nrc
        :: (clist children).drop (idx.val + 2) := by
    rw [hch2v]
    exact set_set_adjacent (clist children) idx.val _ _ (by omega)
  refine parent_assemble new_entries children2 ?_ ?_ ?_ ?_ ?_ ?_
  · rw [hnev]
    exact SortedE_set_key entries.val idx.val stolen hs hidx
      (fun k hk hlt => hltst k hk (by omega))
      (fun k hk hgt => hgtst k hk (by omega))
  · rw [hnev]
    exact InB_set entries.val idx.val stolen lo hi hb
      (InB_outer_of_bnd entries.val (idx.val + 1) stolen.1.val (by omega)
        hb hstb)
  · rw [hnev]; simpa using hlen
  · rw [hnev, hch2v']
    exact hal.replace_pair idx.val hidx stolen _ _ hnlinv hnrinv
  · rw [hch2v']; simp
  · rw [hch2v']
    exact ((hall.take _).append
      (AllH.cons hnlh (AllH.cons hnrh (hall.drop _))))

/-- `merge_with_right entries children idx` (merge child `idx` with its right
    sibling through separator `idx`) preserves `NodeInv` and `HeightInv` of
    the parent family. Side conditions: separator `idx` exists (hence both
    child slots do), and the two children's entries fit in one node. -/
theorem merge_with_right_inv {lo hi : Option Nat} {h : Nat}
    (entries : alloc.vec.Vec (Std.U64 × Std.U64)) (children : Children)
    (idx : Std.Usize)
    (hinv : NodeInv lo hi (Node.mk entries children))
    (hh : HeightInv (h + 1) (Node.mk entries children))
    (hidx : idx.val < entries.val.length)
    (hsz : ∀ nl nr, (clist children)[idx.val]? = some nl →
      (clist children)[idx.val + 1]? = some nr →
      nl.entries.val.length + nr.entries.val.length ≤ 62) :
    merge_with_right entries children idx ⦃ out =>
      NodeInv lo hi (Node.mk out.1 out.2) ∧
      HeightInv (h + 1) (Node.mk out.1 out.2) ⦄ := by
  obtain ⟨hs, hb, hlen, hal, hall, hclen⟩ := parent_facts entries children hinv hh
  have hchlt : idx.val < (clist children).length := by omega
  have hchlt1 : idx.val + 1 < (clist children).length := by omega
  rw [merge_with_right]
  -- separator ← entries[idx]
  obtain ⟨separator, hsep, hsepv⟩ := WP.spec_imp_exists
    (alloc.vec.Vec.index_usize_spec entries idx (by scalar_tac))
  -- left ← child_at children idx
  obtain ⟨left, hleft, hleftv⟩ := WP.spec_imp_exists
    (child_at_spec children idx hchlt)
  -- i ← idx + 1
  obtain ⟨i, hiadd, hivp⟩ := WP.spec_imp_exists
    (Usize.add_spec (x := idx) (y := 1#usize) (by scalar_tac))
  have hiv : i.val = idx.val + 1 := by scalar_tac
  -- right ← child_at children i
  obtain ⟨right, hright, hrightv⟩ := WP.spec_imp_exists
    (child_at_spec children i (by omega))
  simp only [hiv] at hrightv
  -- sibling invariants
  obtain ⟨hlt0, hlinv0⟩ := hal.child idx.val (by omega)
  obtain ⟨hrt0, hrinv0⟩ := hal.child (idx.val + 1) (by omega)
  rw [rbnd_eq_some hi entries.val idx.val hidx] at hlinv0
  rw [lbnd_eq_some lo entries.val (idx.val + 1) (by omega)
    (by simpa using hidx)] at hrinv0
  simp only [Nat.add_sub_cancel] at hrinv0
  rw [← hsepv] at hlinv0 hrinv0
  rw [← hleftv] at hlinv0
  rw [← hrightv] at hrinv0
  have hlh : HeightInv h left := by
    rw [hleftv]; exact hall.getElem _ hchlt
  have hrh : HeightInv h right := by
    rw [hrightv]; exact hall.getElem _ hchlt1
  -- combined size of the two siblings
  have hszv : left.entries.val.length + right.entries.val.length ≤ 62 := by
    apply hsz left right
    · rw [List.getElem?_eq_getElem hchlt, hleftv]
    · rw [List.getElem?_eq_getElem hchlt1, hrightv]
  -- merged_entries ← merge_entries left.entries separator right.entries
  obtain ⟨me, hmee, hmev⟩ := WP.spec_imp_exists
    (merge_entries_spec left.entries separator right.entries (by scalar_tac))
  -- merged_children ← concat_children left.children right.children
  obtain ⟨mc, hmce, hmcv⟩ := WP.spec_imp_exists
    (concat_children_spec left.children right.children)
  -- new_entries ← remove_entry_at entries idx
  obtain ⟨new_entries, hnee, hnev⟩ := WP.spec_imp_exists
    (remove_entry_at_spec entries idx)
  -- children1 ← replace_child children idx (mk me mc)
  obtain ⟨children1, hch1, hch1v⟩ := WP.spec_imp_exists
    (replace_child_spec children idx (Node.mk me mc))
  -- children2 ← remove_child_at children1 i
  obtain ⟨children2, hch2, hch2v⟩ := WP.spec_imp_exists
    (remove_child_at_spec children1 i)
  simp only [hiv] at hch2v
  rw [hch1v] at hch2v
  -- reduce the do-block
  simp only [alloc.vec.Vec.index_slice_index, hsep, hleft, hiadd, hright,
    hmee, hmce, hnee, hch1, hch2, bind_tc_ok, WP.spec_ok]
  -- the merged node
  obtain ⟨hminv, hmh⟩ := merged_node_inv (S := separator) hlinv0 hrinv0 hlh hrh
    (by
      have := sep_InB entries.val idx.val hidx hs hb
      rw [← hsepv] at this
      exact this)
    (by omega) me mc hmev hmcv
  -- assemble the parent
  have hch2v' : clist children2 = (clist children).take idx.val
      ++ Node.mk me mc :: (clist children).drop (idx.val + 2) := by
    rw [hch2v]
    exact set_eraseIdx_adjacent (clist children) idx.val _ (by omega)
  refine parent_assemble new_entries children2 ?_ ?_ ?_ ?_ ?_ ?_
  · rw [hnev]
    exact List.Pairwise.sublist (entries.val.eraseIdx_sublist idx.val) hs
  · rw [hnev]
    exact fun e he => hb e ((entries.val.eraseIdx_sublist idx.val).mem he)
  · rw [hnev]
    have := eraseIdx_length_le entries.val idx.val
    omega
  · rw [hnev, hch2v']
    exact hal.merge_children idx.val hidx _ hminv
  · rw [hch2v']; simp
  · rw [hch2v']
    exact ((hall.take _).append (AllH.cons hmh (hall.drop _)))

/-- `merge_with_left entries children idx` (merge child `idx` with its left
    sibling through separator `idx - 1`) preserves `NodeInv` and `HeightInv`
    of the parent family. Side conditions: `idx` is a non-leftmost valid child
    slot, and the two children's entries fit in one node. -/
theorem merge_with_left_inv {lo hi : Option Nat} {h : Nat}
    (entries : alloc.vec.Vec (Std.U64 × Std.U64)) (children : Children)
    (idx : Std.Usize)
    (hinv : NodeInv lo hi (Node.mk entries children))
    (hh : HeightInv (h + 1) (Node.mk entries children))
    (hidx0 : 0 < idx.val) (hidx : idx.val ≤ entries.val.length)
    (hsz : ∀ nl nr, (clist children)[idx.val - 1]? = some nl →
      (clist children)[idx.val]? = some nr →
      nl.entries.val.length + nr.entries.val.length ≤ 62) :
    merge_with_left entries children idx ⦃ out =>
      NodeInv lo hi (Node.mk out.1 out.2) ∧
      HeightInv (h + 1) (Node.mk out.1 out.2) ⦄ := by
  obtain ⟨hs, hb, hlen, hal, hall, hclen⟩ := parent_facts entries children hinv hh
  have hj : idx.val - 1 < entries.val.length := by omega
  have hj1 : idx.val - 1 + 1 = idx.val := by omega
  have hchlt : idx.val < (clist children).length := by omega
  have hchlt' : idx.val - 1 < (clist children).length := by omega
  rw [merge_with_left]
  -- i ← idx - 1
  obtain ⟨i, hisub, hivp⟩ := WP.spec_imp_exists
    (Usize.sub_spec (x := idx) (y := 1#usize) (by scalar_tac))
  have hiv : i.val = idx.val - 1 := by scalar_tac
  -- separator ← entries[i]
  obtain ⟨separator, hsep, hsepv⟩ := WP.spec_imp_exists
    (alloc.vec.Vec.index_usize_spec entries i (by scalar_tac))
  simp only [hiv] at hsepv
  -- left ← child_at children i
  obtain ⟨left, hleft, hleftv⟩ := WP.spec_imp_exists
    (child_at_spec children i (by omega))
  simp only [hiv] at hleftv
  -- right ← child_at children idx
  obtain ⟨right, hright, hrightv⟩ := WP.spec_imp_exists
    (child_at_spec children idx hchlt)
  -- sibling invariants
  obtain ⟨hlt0, hlinv0⟩ := hal.child (idx.val - 1) (by omega)
  obtain ⟨hrt0, hrinv0⟩ := hal.child idx.val hidx
  rw [rbnd_eq_some hi entries.val (idx.val - 1) hj] at hlinv0
  rw [lbnd_eq_some lo entries.val idx.val (by omega) hj] at hrinv0
  rw [← hsepv] at hlinv0 hrinv0
  rw [← hleftv] at hlinv0
  rw [← hrightv] at hrinv0
  have hlh : HeightInv h left := by
    rw [hleftv]; exact hall.getElem _ hchlt'
  have hrh : HeightInv h right := by
    rw [hrightv]; exact hall.getElem _ hchlt
  -- combined size of the two siblings
  have hszv : left.entries.val.length + right.entries.val.length ≤ 62 := by
    apply hsz left right
    · rw [List.getElem?_eq_getElem hchlt', hleftv]
    · rw [List.getElem?_eq_getElem hchlt, hrightv]
  -- merged_entries ← merge_entries left.entries separator right.entries
  obtain ⟨me, hmee, hmev⟩ := WP.spec_imp_exists
    (merge_entries_spec left.entries separator right.entries (by scalar_tac))
  -- merged_children ← concat_children left.children right.children
  obtain ⟨mc, hmce, hmcv⟩ := WP.spec_imp_exists
    (concat_children_spec left.children right.children)
  -- new_entries ← remove_entry_at entries i
  obtain ⟨new_entries, hnee, hnev⟩ := WP.spec_imp_exists
    (remove_entry_at_spec entries i)
  simp only [hiv] at hnev
  -- children1 ← replace_child children i (mk me mc)
  obtain ⟨children1, hch1, hch1v⟩ := WP.spec_imp_exists
    (replace_child_spec children i (Node.mk me mc))
  simp only [hiv] at hch1v
  -- children2 ← remove_child_at children1 idx
  obtain ⟨children2, hch2, hch2v⟩ := WP.spec_imp_exists
    (remove_child_at_spec children1 idx)
  rw [hch1v] at hch2v
  -- reduce the do-block
  simp only [alloc.vec.Vec.index_slice_index, hisub, hsep, hleft, hright,
    hmee, hmce, hnee, hch1, hch2, bind_tc_ok, WP.spec_ok]
  -- the merged node
  obtain ⟨hminv, hmh⟩ := merged_node_inv (S := separator) hlinv0 hrinv0 hlh hrh
    (by
      have := sep_InB entries.val (idx.val - 1) hj hs hb
      rw [hj1, ← hsepv] at this
      exact this)
    (by omega) me mc hmev hmcv
  -- assemble the parent
  have hch2v' : clist children2 = (clist children).take (idx.val - 1)
      ++ Node.mk me mc :: (clist children).drop (idx.val - 1 + 2) := by
    rw [hch2v, ← hj1]
    exact set_eraseIdx_adjacent (clist children) (idx.val - 1) _ (by omega)
  refine parent_assemble new_entries children2 ?_ ?_ ?_ ?_ ?_ ?_
  · rw [hnev]
    exact List.Pairwise.sublist (entries.val.eraseIdx_sublist (idx.val - 1)) hs
  · rw [hnev]
    exact fun e he => hb e ((entries.val.eraseIdx_sublist (idx.val - 1)).mem he)
  · rw [hnev]
    have := eraseIdx_length_le entries.val (idx.val - 1)
    omega
  · rw [hnev, hch2v']
    refine hal.merge_children (idx.val - 1) hj _ ?_
    rw [hj1]
    exact hminv
  · rw [hch2v']; simp
  · rw [hch2v']
    exact ((hall.take _).append (AllH.cons hmh (hall.drop _)))

/-- `fix_underfull_child entries children idx` preserves `NodeInv` and
    `HeightInv` of the parent family. Side conditions: the parent has at
    least one entry (so child `idx` has a sibling — with a single child the
    kernel function would fail), `idx` is a valid child slot, and child `idx`
    is underfull (`< MIN_KEYS = 31` entries; needed so a merge fits in one
    node). The sibling-size tests only pick the branch. -/
theorem fix_underfull_child_inv {lo hi : Option Nat} {h : Nat}
    (entries : alloc.vec.Vec (Std.U64 × Std.U64)) (children : Children)
    (idx : Std.Usize)
    (hinv : NodeInv lo hi (Node.mk entries children))
    (hh : HeightInv (h + 1) (Node.mk entries children))
    (hnz : 0 < entries.val.length)
    (hidx : idx.val ≤ entries.val.length)
    (hund : ∀ n, (clist children)[idx.val]? = some n →
      n.entries.val.length < 31) :
    fix_underfull_child entries children idx ⦃ out =>
      NodeInv lo hi (Node.mk out.1 out.2) ∧
      HeightInv (h + 1) (Node.mk out.1 out.2) ⦄ := by
  obtain ⟨hs, hb, hlen, hal, hall, hclen⟩ := parent_facts entries children hinv hh
  have hchlt : idx.val < (clist children).length := by omega
  rw [fix_underfull_child]
  obtain ⟨n, hne, hnv⟩ := WP.spec_imp_exists
    (children_len_spec children (by scalar_tac))
  simp only [hne, bind_tc_ok]
  split
  · rename_i hpos
    have hidx0 : 0 < idx.val := by scalar_tac
    obtain ⟨i, hisub, hivp⟩ := WP.spec_imp_exists
      (Usize.sub_spec (x := idx) (y := 1#usize) (by scalar_tac))
    have hiv : i.val = idx.val - 1 := by scalar_tac
    obtain ⟨n1, hn1e, hn1v⟩ := WP.spec_imp_exists
      (child_at_spec children i (by omega))
    simp only [hiv] at hn1v
    obtain ⟨i2, hmk, hmkv⟩ := WP.spec_imp_exists MIN_KEYS_spec
    simp only [hisub, hn1e, hmk, bind_tc_ok]
    split
    · rename_i hgt
      -- left sibling can donate: rotate_right
      have hn1len : 31 < n1.entries.val.length := by
        have := alloc.vec.Vec.len_val n1.entries
        scalar_tac
      exact rotate_right_inv entries children idx hinv hh hidx0 hidx
        (fun m hm => by
          rw [List.getElem?_eq_getElem
            (show idx.val - 1 < (clist children).length by omega)] at hm
          have hmn : m = n1 := by rw [hn1v]; exact (Option.some.inj hm).symm
          rw [hmn]; omega)
        (fun m hm => by have := hund m hm; omega)
    · rename_i hngt
      have hn1len : n1.entries.val.length ≤ 31 := by
        have := alloc.vec.Vec.len_val n1.entries
        scalar_tac
      obtain ⟨i3, hi3a, hi3p⟩ := WP.spec_imp_exists
        (Usize.add_spec (x := idx) (y := 1#usize) (by scalar_tac))
      have hi3v : i3.val = idx.val + 1 := by scalar_tac
      simp only [hi3a, bind_tc_ok]
      split
      · rename_i hlt3
        -- a right sibling exists too
        have hidxlt : idx.val < entries.val.length := by scalar_tac
        obtain ⟨n2, hn2e, hn2v⟩ := WP.spec_imp_exists
          (child_at_spec children i3 (by omega))
        simp only [hi3v] at hn2v
        simp only [hn2e, bind_tc_ok]
        split
        · rename_i hgt4
          -- right sibling can donate: rotate_left
          have hn2len : 31 < n2.entries.val.length := by
            have := alloc.vec.Vec.len_val n2.entries
            scalar_tac
          exact rotate_left_inv entries children idx hinv hh hidxlt
            (fun m hm => by
              rw [List.getElem?_eq_getElem
                (show idx.val + 1 < (clist children).length by omega)] at hm
              have hmn : m = n2 := by rw [hn2v]; exact (Option.some.inj hm).symm
              rw [hmn]; omega)
            (fun m hm => by have := hund m hm; omega)
        · rename_i hngt4
          exact merge_with_left_inv entries children idx hinv hh hidx0 hidx
            (fun ml mr hml hmr => by
              rw [List.getElem?_eq_getElem
                (show idx.val - 1 < (clist children).length by omega)] at hml
              have hmln : ml = n1 := by
                rw [hn1v]; exact (Option.some.inj hml).symm
              have := hund mr hmr
              rw [hmln]; omega)
      · rename_i hge3
        exact merge_with_left_inv entries children idx hinv hh hidx0 hidx
          (fun ml mr hml hmr => by
            rw [List.getElem?_eq_getElem
              (show idx.val - 1 < (clist children).length by omega)] at hml
            have hmln : ml = n1 := by
              rw [hn1v]; exact (Option.some.inj hml).symm
            have := hund mr hmr
            rw [hmln]; omega)
  · rename_i hnpos
    have hidx0 : idx.val = 0 := by scalar_tac
    obtain ⟨i, hiadd, hivp⟩ := WP.spec_imp_exists
      (Usize.add_spec (x := idx) (y := 1#usize) (by scalar_tac))
    have hiv : i.val = idx.val + 1 := by scalar_tac
    simp only [hiadd, bind_tc_ok]
    split
    · rename_i hlt
      have hidxlt : idx.val < entries.val.length := by omega
      obtain ⟨n1, hn1e, hn1v⟩ := WP.spec_imp_exists
        (child_at_spec children i (by omega))
      simp only [hiv] at hn1v
      obtain ⟨i2, hmk, hmkv⟩ := WP.spec_imp_exists MIN_KEYS_spec
      simp only [hn1e, hmk, bind_tc_ok]
      split
      · rename_i hgt
        -- right sibling can donate: rotate_left
        have hn1len : 31 < n1.entries.val.length := by
          have := alloc.vec.Vec.len_val n1.entries
          scalar_tac
        exact rotate_left_inv entries children idx hinv hh hidxlt
          (fun m hm => by
            rw [List.getElem?_eq_getElem
              (show idx.val + 1 < (clist children).length by omega)] at hm
            have hmn : m = n1 := by rw [hn1v]; exact (Option.some.inj hm).symm
            rw [hmn]; omega)
          (fun m hm => by have := hund m hm; omega)
      · rename_i hngt
        have hn1len : n1.entries.val.length ≤ 31 := by
          have := alloc.vec.Vec.len_val n1.entries
          scalar_tac
        exact merge_with_right_inv entries children idx hinv hh hidxlt
          (fun ml mr hml hmr => by
            have := hund ml hml
            rw [List.getElem?_eq_getElem
              (show idx.val + 1 < (clist children).length by omega)] at hmr
            have hmrn : mr = n1 := by
              rw [hn1v]; exact (Option.some.inj hmr).symm
            rw [hmrn]; omega)
    · rename_i hge
      -- impossible: ≥ 1 entry means ≥ 2 children, so slot 1 exists
      exfalso
      scalar_tac

/-- `maybe_fix entries children idx underfull` preserves `NodeInv` and
    `HeightInv`: identity when `underfull = false`, otherwise
    `fix_underfull_child_inv` (whose side conditions are only required when
    the flag is set). -/
theorem maybe_fix_inv {lo hi : Option Nat} {h : Nat}
    (entries : alloc.vec.Vec (Std.U64 × Std.U64)) (children : Children)
    (idx : Std.Usize) (underfull : Bool)
    (hinv : NodeInv lo hi (Node.mk entries children))
    (hh : HeightInv (h + 1) (Node.mk entries children))
    (hnz : underfull = true → 0 < entries.val.length)
    (hidx : underfull = true → idx.val ≤ entries.val.length)
    (hund : underfull = true → ∀ n, (clist children)[idx.val]? = some n →
      n.entries.val.length < 31) :
    maybe_fix entries children idx underfull ⦃ out =>
      NodeInv lo hi (Node.mk out.1 out.2) ∧
      HeightInv (h + 1) (Node.mk out.1 out.2) ⦄ := by
  rw [maybe_fix]
  split
  · rename_i hu
    exact fix_underfull_child_inv entries children idx hinv hh
      (hnz hu) (hidx hu) (hund hu)
  · exact (WP.spec_ok _).mpr ⟨hinv, hh⟩

end btree_kernel
