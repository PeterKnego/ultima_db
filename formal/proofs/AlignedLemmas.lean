/- Pure lemmas about the `Aligned` inductive predicate, supporting the
   B-tree verification: length, child-slot bounds, child replacement,
   split-insertion, and node splitting. -/
import BtreeKernel
import BtreeInvariant

open Aeneas Aeneas.Std

namespace btree_kernel

/-! ### Bound equations for `lbnd` / `rbnd` -/

private theorem lbnd_zero (lo : Option Nat) (es : List (Std.U64 × Std.U64)) :
    lbnd lo es 0 = lo := by
  simp [lbnd]

private theorem rbnd_nil (hi : Option Nat) (pos : Nat) :
    rbnd hi ([] : List (Std.U64 × Std.U64)) pos = hi := by
  simp [rbnd]

private theorem rbnd_cons_zero (hi : Option Nat) (e : Std.U64 × Std.U64)
    (es : List (Std.U64 × Std.U64)) :
    rbnd hi (e :: es) 0 = some e.1.val := by
  simp [rbnd]

private theorem lbnd_cons_succ (lo : Option Nat) (e : Std.U64 × Std.U64)
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

private theorem rbnd_cons_succ (hi : Option Nat) (e : Std.U64 × Std.U64)
    (es : List (Std.U64 × Std.U64)) (pos : Nat) :
    rbnd hi (e :: es) (pos + 1) = rbnd hi es pos := by
  simp only [rbnd, List.length_cons]
  by_cases h : pos < es.length
  · rw [dif_pos (by omega : pos + 1 < es.length + 1), dif_pos h]
    simp
  · rw [dif_neg (by omega), dif_neg h]

/-! ### Main lemmas -/

theorem Aligned.length_eq {lo hi : Option Nat} {es : List (Std.U64 × Std.U64)}
    {cs : List Node} (h : Aligned lo hi es cs) : cs.length = es.length + 1 := by
  induction es generalizing lo cs with
  | nil => cases h with | last c hc => simp
  | cons e es ih =>
    cases h with
    | step _ _ c cs hc htail => simp [ih htail]

theorem InB_bnd_of_bracket {lo hi : Option Nat} (es : List (Std.U64 × Std.U64))
    (pos : Nat) (k : Nat) (hpos : pos ≤ es.length) (hk : InB lo hi k)
    (hlt : ∀ i, (h : i < es.length) → i < pos → (es[i]'h).1.val < k)
    (hgt : ∀ i, (h : i < es.length) → pos ≤ i → k < (es[i]'h).1.val) :
    InB (lbnd lo es pos) (rbnd hi es pos) k := by
  obtain ⟨hk1, hk2⟩ := hk
  refine ⟨fun l hl => ?_, fun x hx => ?_⟩
  · simp only [lbnd] at hl
    split at hl
    · exact hk1 l hl
    · rename_i h0
      rw [dif_pos (show pos - 1 < es.length by omega)] at hl
      simp only [Option.mem_def, Option.some.injEq] at hl
      subst hl
      exact hlt (pos - 1) (by omega) (by omega)
  · simp only [rbnd] at hx
    split at hx
    · rename_i hlen
      simp only [Option.mem_def, Option.some.injEq] at hx
      subst hx
      exact hgt pos hlen (Nat.le_refl pos)
    · exact hk2 x hx

theorem Aligned.child {lo hi : Option Nat} {es : List (Std.U64 × Std.U64)}
    {cs : List Node} (h : Aligned lo hi es cs) (pos : Nat) (hpos : pos ≤ es.length) :
    ∃ hc : pos < cs.length, NodeInv (lbnd lo es pos) (rbnd hi es pos) (cs[pos]'hc) := by
  induction pos generalizing lo es cs with
  | zero =>
    cases h with
    | last c hc =>
      exact ⟨by simp, by simpa [lbnd, rbnd] using hc⟩
    | step e es c cs hc htail =>
      exact ⟨by simp, by simpa [lbnd, rbnd] using hc⟩
  | succ pos ih =>
    cases h with
    | last c hc => simp at hpos
    | step e es c cs hc htail =>
      have hpos' : pos ≤ es.length := by simp at hpos; omega
      obtain ⟨hlt, hinv⟩ := ih htail hpos'
      refine ⟨by simp; omega, ?_⟩
      rw [lbnd_cons_succ, rbnd_cons_succ]
      simpa using hinv

theorem Aligned.set_child {lo hi : Option Nat} {es : List (Std.U64 × Std.U64)}
    {cs : List Node} (h : Aligned lo hi es cs) (pos : Nat) (hpos : pos ≤ es.length)
    (n : Node) (hn : NodeInv (lbnd lo es pos) (rbnd hi es pos) n) :
    Aligned lo hi es (cs.set pos n) := by
  induction pos generalizing lo es cs with
  | zero =>
    cases h with
    | last c hc =>
      simp only [lbnd_zero, rbnd_nil] at hn
      simpa using Aligned.last n hn
    | step e es c cs hc htail =>
      simp only [lbnd_zero, rbnd_cons_zero] at hn
      simpa using Aligned.step e es n cs hn htail
  | succ pos ih =>
    cases h with
    | last c hc => simp at hpos
    | step e es c cs hc htail =>
      simp only [lbnd_cons_succ, rbnd_cons_succ] at hn
      have hpos' : pos ≤ es.length := by simp at hpos; omega
      simpa using Aligned.step e es c (cs.set pos n) hc (ih htail hpos' hn)

theorem Aligned.insert_split {lo hi : Option Nat} {es : List (Std.U64 × Std.U64)}
    {cs : List Node} (h : Aligned lo hi es cs) (pos : Nat) (hpos : pos ≤ es.length)
    (m : Std.U64 × Std.U64) (l r : Node)
    (hm : InB (lbnd lo es pos) (rbnd hi es pos) m.1.val)
    (hl : NodeInv (lbnd lo es pos) (some m.1.val) l)
    (hr : NodeInv (some m.1.val) (rbnd hi es pos) r) :
    Aligned lo hi (es.take pos ++ m :: es.drop pos)
      (cs.take pos ++ l :: r :: cs.drop (pos + 1)) := by
  induction pos generalizing lo es cs with
  | zero =>
    cases h with
    | last c hc =>
      simp only [lbnd_zero, rbnd_nil] at hl hr
      simpa using Aligned.step m [] l [r] hl (Aligned.last r hr)
    | step e es c cs hc htail =>
      simp only [lbnd_zero, rbnd_cons_zero] at hl hr
      simpa using Aligned.step m (e :: es) l (r :: cs) hl
        (Aligned.step e es r cs hr htail)
  | succ pos ih =>
    cases h with
    | last c hc => simp at hpos
    | step e es c cs hc htail =>
      simp only [lbnd_cons_succ, rbnd_cons_succ] at hm hl hr
      have hpos' : pos ≤ es.length := by simp at hpos; omega
      have tail := ih htail hpos' hm hl hr
      simpa using Aligned.step e (es.take pos ++ m :: es.drop pos) c
        (cs.take pos ++ l :: r :: cs.drop (pos + 1)) hc tail

theorem Aligned.split {lo hi : Option Nat} {es : List (Std.U64 × Std.U64)}
    {cs : List Node} (h : Aligned lo hi es cs) (m : Nat) (hm : m < es.length) :
    Aligned lo (some ((es[m]'hm).1.val)) (es.take m) (cs.take (m + 1)) ∧
    Aligned (some ((es[m]'hm).1.val)) hi (es.drop (m + 1)) (cs.drop (m + 1)) := by
  induction m generalizing lo es cs with
  | zero =>
    cases h with
    | last c hc => simp at hm
    | step e es c cs hc htail =>
      constructor
      · simpa using Aligned.last c hc
      · simpa using htail
  | succ m ih =>
    cases h with
    | last c hc => simp at hm
    | step e es c cs hc htail =>
      have hm' : m < es.length := by simp at hm; omega
      obtain ⟨hleft, hright⟩ := ih htail hm'
      constructor
      · simpa using Aligned.step e (es.take m) c (cs.take (m + 1)) hc hleft
      · simpa using hright

end btree_kernel
