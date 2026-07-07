/- Transport lemmas for `insertAt` (`l.take pos ++ x :: l.drop pos`):
   how getElem and key brackets move across an insertion. -/
import BtreeKernel
import BtreeInvariant
import BtreeInsertGet

open Aeneas Aeneas.Std

namespace btree_kernel

theorem insertAt_getElem_lo (l : List (Std.U64 × Std.U64)) (pos : Nat)
    (x : Std.U64 × Std.U64) (hpos : pos ≤ l.length)
    (j : Nat) (hj : j < l.length) (hjp : j < pos) :
    (l.take pos ++ x :: l.drop pos)[j]'(by simp; omega) = l[j]'hj := by
  apply getElem_of_getElem?
  rw [List.getElem?_append_left (by simp only [List.length_take]; omega),
    List.getElem?_take_of_lt hjp]
  exact List.getElem?_eq_getElem hj

theorem insertAt_getElem_hi (l : List (Std.U64 × Std.U64)) (pos : Nat)
    (x : Std.U64 × Std.U64) (hpos : pos ≤ l.length)
    (j : Nat) (hj : j < l.length) (hjp : pos ≤ j) :
    (l.take pos ++ x :: l.drop pos)[j + 1]'(by simp; omega) = l[j]'hj := by
  apply getElem_of_getElem?
  rw [List.getElem?_append_right (by simp only [List.length_take]; omega)]
  simp only [List.length_take]
  rw [show j + 1 - min pos l.length = (j - pos) + 1 from by omega,
    List.getElem?_cons_succ, List.getElem?_drop,
    show pos + (j - pos) = j from by omega]
  exact List.getElem?_eq_getElem hj

/-- `insertAt_getElem_hi` restated at index `i` with `pos < i`,
    with the bound proof taken as given (convenient at use sites). -/
theorem insertAt_getElem_hi' (l : List (Std.U64 × Std.U64)) (pos : Nat)
    (x : Std.U64 × Std.U64) (hpos : pos ≤ l.length)
    (i : Nat) (h : i < (l.take pos ++ x :: l.drop pos).length)
    (hip : pos < i) (hil : i - 1 < l.length) :
    (l.take pos ++ x :: l.drop pos)[i]'h = l[i - 1]'hil := by
  apply getElem_of_getElem?
  rw [List.getElem?_append_right (by simp only [List.length_take]; omega)]
  simp only [List.length_take]
  rw [show i - min pos l.length = (i - pos - 1) + 1 from by omega,
    List.getElem?_cons_succ, List.getElem?_drop,
    show pos + (i - pos - 1) = i - 1 from by omega]
  exact List.getElem?_eq_getElem hil

theorem bracket_lt_le (l : List (Std.U64 × Std.U64)) {q pos : Nat} {a b : Nat}
    (hq : q ≤ l.length) (hpos : pos ≤ l.length) (hab : a < b)
    (hlta : ∀ i, (h : i < l.length) → i < q → (l[i]'h).1.val < a)
    (hgtb : ∀ i, (h : i < l.length) → pos ≤ i → b < (l[i]'h).1.val) :
    q ≤ pos := by
  by_contra hc
  have hc : pos < q := by omega
  have h1 := hlta pos (by omega) hc
  have h2 := hgtb pos (by omega) (Nat.le_refl _)
  omega

theorem bracket_insertAt_lo (l : List (Std.U64 × Std.U64)) (pos : Nat)
    (x : Std.U64 × Std.U64) (hpos : pos ≤ l.length)
    (q : Nat) (hq : q ≤ l.length) (hqp : q ≤ pos)
    (k' : Nat) (hkx : k' < x.1.val)
    (hlt : ∀ i, (h : i < l.length) → i < q → (l[i]'h).1.val < k')
    (hgt : ∀ i, (h : i < l.length) → q ≤ i → k' < (l[i]'h).1.val) :
    (∀ i, (h : i < (l.take pos ++ x :: l.drop pos).length) → i < q →
      ((l.take pos ++ x :: l.drop pos)[i]'h).1.val < k') ∧
    (∀ i, (h : i < (l.take pos ++ x :: l.drop pos).length) → q ≤ i →
      k' < ((l.take pos ++ x :: l.drop pos)[i]'h).1.val) := by
  constructor
  · intro i h hi
    have hil : i < l.length := by omega
    have heq : (l.take pos ++ x :: l.drop pos)[i]'h = l[i]'hil :=
      insertAt_getElem_lo l pos x hpos i hil (by omega)
    rw [heq]
    exact hlt i hil hi
  · intro i h hi
    rcases Nat.lt_trichotomy i pos with hip | hip | hip
    · have hil : i < l.length := by omega
      have heq : (l.take pos ++ x :: l.drop pos)[i]'h = l[i]'hil :=
        insertAt_getElem_lo l pos x hpos i hil hip
      rw [heq]
      exact hgt i hil hi
    · have heq : (l.take pos ++ x :: l.drop pos)[i]'h = x := by
        subst hip
        exact getElem_insertAt_self _ _ _ (by omega)
      rw [heq]
      exact hkx
    · have hil : i - 1 < l.length := by
        have h' := h
        simp only [List.length_append, List.length_take, List.length_cons,
          List.length_drop] at h'
        omega
      have heq : (l.take pos ++ x :: l.drop pos)[i]'h = l[i - 1]'hil :=
        insertAt_getElem_hi' l pos x hpos i h hip hil
      rw [heq]
      exact hgt (i - 1) hil (by omega)

theorem bracket_insertAt_hi (l : List (Std.U64 × Std.U64)) (pos : Nat)
    (x : Std.U64 × Std.U64) (hpos : pos ≤ l.length)
    (q : Nat) (hq : q ≤ l.length) (hqp : pos ≤ q)
    (k' : Nat) (hkx : x.1.val < k')
    (hlt : ∀ i, (h : i < l.length) → i < q → (l[i]'h).1.val < k')
    (hgt : ∀ i, (h : i < l.length) → q ≤ i → k' < (l[i]'h).1.val) :
    (∀ i, (h : i < (l.take pos ++ x :: l.drop pos).length) → i < q + 1 →
      ((l.take pos ++ x :: l.drop pos)[i]'h).1.val < k') ∧
    (∀ i, (h : i < (l.take pos ++ x :: l.drop pos).length) → q + 1 ≤ i →
      k' < ((l.take pos ++ x :: l.drop pos)[i]'h).1.val) := by
  constructor
  · intro i h hi
    rcases Nat.lt_trichotomy i pos with hip | hip | hip
    · have hil : i < l.length := by omega
      have heq : (l.take pos ++ x :: l.drop pos)[i]'h = l[i]'hil :=
        insertAt_getElem_lo l pos x hpos i hil hip
      rw [heq]
      exact hlt i hil (by omega)
    · have heq : (l.take pos ++ x :: l.drop pos)[i]'h = x := by
        subst hip
        exact getElem_insertAt_self _ _ _ (by omega)
      rw [heq]
      exact hkx
    · have hil : i - 1 < l.length := by omega
      have heq : (l.take pos ++ x :: l.drop pos)[i]'h = l[i - 1]'hil :=
        insertAt_getElem_hi' l pos x hpos i h hip hil
      rw [heq]
      exact hlt (i - 1) hil (by omega)
  · intro i h hi
    have hip : pos < i := by omega
    have hil : i - 1 < l.length := by
      have h' := h
      simp only [List.length_append, List.length_take, List.length_cons,
        List.length_drop] at h'
      omega
    have heq : (l.take pos ++ x :: l.drop pos)[i]'h = l[i - 1]'hil :=
      insertAt_getElem_hi' l pos x hpos i h hip hil
    rw [heq]
    exact hgt (i - 1) hil (by omega)

end btree_kernel
