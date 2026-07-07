/- Pure list lemmas about SortedE / InB supporting the B-tree verification. -/
import BtreeKernel
import BtreeInvariant

open Aeneas Aeneas.Std

namespace btree_kernel

theorem SortedE_take (l : List (Std.U64 × Std.U64)) (n : Nat) (h : SortedE l) :
    SortedE (l.take n) :=
  List.Pairwise.sublist (List.take_sublist n l) h

theorem SortedE_drop (l : List (Std.U64 × Std.U64)) (n : Nat) (h : SortedE l) :
    SortedE (l.drop n) :=
  List.Pairwise.sublist (List.drop_sublist n l) h

theorem SortedE_insertAt (l : List (Std.U64 × Std.U64)) (pos : Nat)
    (x : Std.U64 × Std.U64) (hs : SortedE l) (hpos : pos ≤ l.length)
    (hlt : ∀ i, (h : i < l.length) → i < pos → (l[i]'h).1.val < x.1.val)
    (hgt : ∀ i, (h : i < l.length) → pos ≤ i → x.1.val < (l[i]'h).1.val) :
    SortedE (l.take pos ++ x :: l.drop pos) := by
  have hs' := List.pairwise_iff_getElem.mp hs
  unfold SortedE
  rw [List.pairwise_append]
  refine ⟨SortedE_take l pos hs, ?_, ?_⟩
  · rw [List.pairwise_cons]
    refine ⟨?_, SortedE_drop l pos hs⟩
    intro b hb
    obtain ⟨j, hj, rfl⟩ := List.mem_iff_getElem.mp hb
    have hjl : j < l.length - pos := by
      have := hj; simp only [List.length_drop] at this; omega
    rw [List.getElem_drop]
    exact hgt (pos + j) (by omega) (by omega)
  · intro a ha b hb
    obtain ⟨i, hi, rfl⟩ := List.mem_iff_getElem.mp ha
    have hil : i < pos := by
      have := hi; simp only [List.length_take] at this; omega
    rw [List.getElem_take]
    rcases List.mem_cons.mp hb with rfl | hb'
    · exact hlt i (by omega) hil
    · obtain ⟨j, hj, rfl⟩ := List.mem_iff_getElem.mp hb'
      have hjl : j < l.length - pos := by
        have := hj; simp only [List.length_drop] at this; omega
      rw [List.getElem_drop]
      exact hs' i (pos + j) (by omega) (by omega) (by omega)

theorem SortedE_set (l : List (Std.U64 × Std.U64)) (pos : Nat)
    (x : Std.U64 × Std.U64) (hs : SortedE l) (hpos : pos < l.length)
    (hkey : (l[pos]'hpos).1.val = x.1.val) :
    SortedE (l.set pos x) := by
  have hs' := List.pairwise_iff_getElem.mp hs
  unfold SortedE
  rw [List.pairwise_iff_getElem]
  intro i j hi hj hij
  have hi' : i < l.length := by simpa using hi
  have hj' : j < l.length := by simpa using hj
  rw [List.getElem_set, List.getElem_set]
  by_cases h1 : pos = i
  · subst h1
    by_cases h2 : pos = j
    · omega
    · rw [if_pos rfl, if_neg h2, ← hkey]
      exact hs' pos j hpos hj' hij
  · by_cases h2 : pos = j
    · subst h2
      rw [if_neg h1, if_pos rfl, ← hkey]
      exact hs' i pos hi' hpos hij
    · rw [if_neg h1, if_neg h2]
      exact hs' i j hi' hj' hij

theorem mem_take_lt_median (l : List (Std.U64 × Std.U64)) (m : Nat)
    (hm : m < l.length) (hs : SortedE l) :
    ∀ e ∈ l.take m, e.1.val < (l[m]'hm).1.val := by
  have hs' := List.pairwise_iff_getElem.mp hs
  intro e he
  obtain ⟨i, hi, rfl⟩ := List.mem_iff_getElem.mp he
  have hil : i < m := by
    have := hi; simp only [List.length_take] at this; omega
  rw [List.getElem_take]
  exact hs' i m (by omega) hm hil

theorem mem_drop_gt_median (l : List (Std.U64 × Std.U64)) (m : Nat)
    (hm : m < l.length) (hs : SortedE l) :
    ∀ e ∈ l.drop (m + 1), (l[m]'hm).1.val < e.1.val := by
  have hs' := List.pairwise_iff_getElem.mp hs
  intro e he
  obtain ⟨j, hj, rfl⟩ := List.mem_iff_getElem.mp he
  have hjl : j < l.length - (m + 1) := by
    have := hj; simp only [List.length_drop] at this; omega
  rw [List.getElem_drop]
  exact hs' m (m + 1 + j) hm (by omega) (by omega)

theorem InB_insertAt (l : List (Std.U64 × Std.U64)) (pos : Nat)
    (x : Std.U64 × Std.U64) (lo hi : Option Nat)
    (hb : ∀ e ∈ l, InB lo hi e.1.val) (hx : InB lo hi x.1.val) :
    ∀ e ∈ (l.take pos ++ x :: l.drop pos), InB lo hi e.1.val := by
  intro e he
  rcases List.mem_append.mp he with h1 | h2
  · exact hb e (List.mem_of_mem_take h1)
  · rcases List.mem_cons.mp h2 with rfl | h3
    · exact hx
    · exact hb e (List.mem_of_mem_drop h3)

theorem InB_set (l : List (Std.U64 × Std.U64)) (pos : Nat)
    (x : Std.U64 × Std.U64) (lo hi : Option Nat)
    (hb : ∀ e ∈ l, InB lo hi e.1.val) (hx : InB lo hi x.1.val) :
    ∀ e ∈ l.set pos x, InB lo hi e.1.val := by
  intro e he
  rcases List.mem_or_eq_of_mem_set he with h1 | rfl
  · exact hb e h1
  · exact hx

theorem InB.tighten_hi {lo hi : Option Nat} {n m : Nat}
    (h : InB lo hi n) (hm : n < m) : InB lo (some m) n := by
  obtain ⟨h1, _⟩ := h
  refine ⟨h1, fun k hk => ?_⟩
  simp only [Option.mem_def, Option.some.injEq] at hk
  omega

theorem InB.tighten_lo {lo hi : Option Nat} {n m : Nat}
    (h : InB lo hi n) (hm : m < n) : InB (some m) hi n := by
  obtain ⟨_, h2⟩ := h
  refine ⟨fun k hk => ?_, h2⟩
  simp only [Option.mem_def, Option.some.injEq] at hk
  omega

theorem InB.of_take {lo hi : Option Nat} (l : List (Std.U64 × Std.U64)) (m : Nat)
    (hm : m < l.length) (hs : SortedE l)
    (hb : ∀ e ∈ l, InB lo hi e.1.val) :
    ∀ e ∈ l.take m, InB lo (some ((l[m]'hm).1.val)) e.1.val := fun e he =>
  (hb e (List.mem_of_mem_take he)).tighten_hi (mem_take_lt_median l m hm hs e he)

theorem InB.of_drop {lo hi : Option Nat} (l : List (Std.U64 × Std.U64)) (m : Nat)
    (hm : m < l.length) (hs : SortedE l)
    (hb : ∀ e ∈ l, InB lo hi e.1.val) :
    ∀ e ∈ l.drop (m + 1), InB (some ((l[m]'hm).1.val)) hi e.1.val := fun e he =>
  (hb e (List.mem_of_mem_drop he)).tighten_lo (mem_drop_gt_median l m hm hs e he)

end btree_kernel
