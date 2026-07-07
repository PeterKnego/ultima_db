import BtreeKernel
import BtreeInvariant

open Aeneas Aeneas.Std Result

namespace btree_kernel

theorem find_pos_spec (entries : alloc.vec.Vec (Std.U64 × Std.U64)) (k : Std.U64)
    (hs : SortedE entries.val) :
    find_pos entries k ⦃ r =>
      r.2.val ≤ entries.val.length ∧
      (r.1 = true → ∃ h : r.2.val < entries.val.length,
        (entries.val[r.2.val]'h).1 = k) ∧
      (r.1 = false →
        (∀ i, (h : i < entries.val.length) → i < r.2.val →
          (entries.val[i]'h).1.val < k.val) ∧
        (∀ i, (h : i < entries.val.length) → r.2.val ≤ i →
          k.val < (entries.val[i]'h).1.val)) ⦄ := by
  rename' k => key
  have hsorted : ∀ i j (hi' : i < entries.val.length) (hj : j < entries.val.length),
      i < j → (entries.val[i]'hi').1.val < (entries.val[j]'hj).1.val := by
    rw [SortedE, List.pairwise_iff_getElem] at hs
    exact hs
  simp only [find_pos, find_pos_loop]
  have h := loop.spec_decr_nat
      (fun (x : Std.Usize × Std.Usize × Bool × Std.Usize) =>
        (if x.2.2.1 then 0 else 1) + (x.2.1.val - x.1.val))
      (fun (x : Std.Usize × Std.Usize × Bool × Std.Usize) =>
        x.1.val ≤ x.2.1.val ∧ x.2.1.val ≤ entries.val.length ∧
        (∀ i, (h : i < entries.val.length) → i < x.1.val →
          (entries.val[i]'h).1.val < key.val) ∧
        (∀ i, (h : i < entries.val.length) → x.2.1.val ≤ i →
          key.val < (entries.val[i]'h).1.val) ∧
        (x.2.2.1 = true → ∃ h : x.2.2.2.val < entries.val.length,
          (entries.val[x.2.2.2.val]'h).1 = key))
      (fun (r : Std.Usize × Bool × Std.Usize) =>
        r.1.val ≤ entries.val.length ∧
        (r.2.1 = true → ∃ h : r.2.2.val < entries.val.length,
          (entries.val[r.2.2.val]'h).1 = key) ∧
        (r.2.1 = false →
          (∀ i, (h : i < entries.val.length) → i < r.1.val →
            (entries.val[i]'h).1.val < key.val) ∧
          (∀ i, (h : i < entries.val.length) → r.1.val ≤ i →
            key.val < (entries.val[i]'h).1.val)))
      (fun (lo1, hi1, found1, pos1) => find_pos_loop.body entries key lo1 hi1 found1 pos1)
      (0#usize, alloc.vec.Vec.len entries, false, 0#usize)
      ?hbody ?hinv
  case hinv =>
    dsimp only
    refine ⟨by scalar_tac, by scalar_tac, ?_, ?_, by simp⟩
    · intro i h hi0; scalar_tac
    · intro i h hge; scalar_tac
  case hbody =>
    rintro ⟨lo, hi, found, pos⟩ ⟨h1, h2, hlt, hgt, hfound⟩
    have h1' : lo.val ≤ hi.val := h1
    have h2' : hi.val ≤ entries.val.length := h2
    have hlt' : ∀ i, (h : i < entries.val.length) → i < lo.val →
        (entries.val[i]'h).1.val < key.val := hlt
    have hgt' : ∀ i, (h : i < entries.val.length) → hi.val ≤ i →
        key.val < (entries.val[i]'h).1.val := hgt
    have hfound' : found = true → ∃ h : pos.val < entries.val.length,
        (entries.val[pos.val]'h).1 = key := hfound
    simp only [find_pos_loop.body]
    split
    · rename_i hlohi
      split
      · rename_i hf
        simp only [Aeneas.Std.WP.spec_ok]
        exact ⟨by scalar_tac, fun _ => hfound' hf, by simp⟩
      · rename_i hf
        step*
        · -- k < key branch: cont (mid+1, hi, false, pos)
          have hmidlen : mid.val < entries.val.length := by scalar_tac
          have hkeq : (entries.val[mid.val]'hmidlen).1 = k :=
            (congrArg Prod.fst k_post).symm
          have hklt : (entries.val[mid.val]'hmidlen).1.val < key.val := by
            rw [hkeq]; scalar_tac
          refine ⟨by scalar_tac, h2', ?_, hgt', by simp, by scalar_tac⟩
          intro j hj hjlt
          rcases Nat.lt_or_ge j lo.val with hcase | hcase
          · exact hlt' j hj hcase
          · rcases Nat.lt_or_ge j mid.val with hcase2 | hcase2
            · exact Nat.lt_trans (hsorted j mid.val hj hmidlen hcase2) hklt
            · have hjeq : j = mid.val := by scalar_tac
              subst hjeq
              exact hklt
        · -- key < k branch: cont (lo, mid, false, pos)
          have hmidlen : mid.val < entries.val.length := by scalar_tac
          have hkeq : (entries.val[mid.val]'hmidlen).1 = k :=
            (congrArg Prod.fst k_post).symm
          have hkgt : key.val < (entries.val[mid.val]'hmidlen).1.val := by
            rw [hkeq]; scalar_tac
          refine ⟨by scalar_tac, by scalar_tac, hlt', ?_, by simp, by scalar_tac⟩
          intro j hj hge
          rcases Nat.lt_or_ge mid.val j with hcase | hcase
          · exact Nat.lt_trans hkgt (hsorted mid.val j hmidlen hj hcase)
          · have hjeq : j = mid.val := by scalar_tac
            subst hjeq
            exact hkgt
    · rename_i hlohi
      simp only [Aeneas.Std.WP.spec_ok]
      have hle : hi.val ≤ lo.val := by scalar_tac
      exact ⟨by scalar_tac, hfound',
        fun _ => ⟨hlt', fun j hj hge => hgt' j hj (by omega)⟩⟩
  obtain ⟨v, hloop, hpost⟩ := Aeneas.Std.WP.spec_imp_exists h
  rw [hloop]
  obtain ⟨lo, found, pos⟩ := v
  obtain ⟨hp1, hp2, hp3⟩ := hpost
  have hp1' : lo.val ≤ entries.val.length := hp1
  have hp2' : found = true → ∃ h : pos.val < entries.val.length,
      (entries.val[pos.val]'h).1 = key := hp2
  have hp3' : found = false →
      (∀ i, (h : i < entries.val.length) → i < lo.val →
        (entries.val[i]'h).1.val < key.val) ∧
      (∀ i, (h : i < entries.val.length) → lo.val ≤ i →
        key.val < (entries.val[i]'h).1.val) := hp3
  cases found
  · simp only [bind_tc_ok]
    exact (Aeneas.Std.WP.spec_ok _).mpr ⟨hp1', by simp, fun _ => hp3' rfl⟩
  · simp only [bind_tc_ok]
    obtain ⟨hplt, hpeq⟩ := hp2' rfl
    exact (Aeneas.Std.WP.spec_ok _).mpr
      ⟨Nat.le_of_lt hplt, fun _ => ⟨hplt, hpeq⟩, by simp⟩

end btree_kernel
