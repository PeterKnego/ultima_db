/- First theorems about the Aeneas-translated UltimaDB B-tree kernel.

   `BtreeKernel.lean` is generated from Rust by Charon+Aeneas — do not edit it.
   Everything here is proved against the generated monadic definitions.
-/
import BtreeKernel

open Aeneas Aeneas.Std Result

namespace btree_kernel

/-- Looking up any key in a freshly created tree returns `none`
    (and in particular does not fail). -/
theorem new_get_none (k : Std.U64) :
    ∃ t, BTree.new = ok t ∧ t.get k = ok none := by
  refine ⟨_, rfl, ?_⟩
  simp only [BTree.get]
  rw [get_in_node]
  simp [find_pos, find_pos_loop, loop, find_pos_loop.body, children_len]

/-- `find_pos` never fails: binary search over any entries vector returns
    `ok` — no overflow, no out-of-bounds index, and the loop terminates.
    Invariant: `lo ≤ hi ∧ hi ≤ entries.len`; termination measure:
    `(if found then 0 else 1) + (hi - lo)` (strictly decreases when `found`
    flips, covering the one extra iteration that exits the loop). -/
theorem find_pos_ok (entries : alloc.vec.Vec (Std.U64 × Std.U64)) (k : Std.U64) :
    ∃ r, find_pos entries k = ok r := by
  simp only [find_pos, find_pos_loop]
  have h := loop.spec_decr_nat
      (fun (x : Std.Usize × Std.Usize × Bool × Std.Usize) =>
        (if x.2.2.1 then 0 else 1) + (x.2.1.val - x.1.val))
      (fun x => x.1.val ≤ x.2.1.val ∧ x.2.1.val ≤ (alloc.vec.Vec.len entries).val)
      (fun _ => True)
      (fun (lo1, hi1, found1, pos1) => find_pos_loop.body entries k lo1 hi1 found1 pos1)
      (0#usize, alloc.vec.Vec.len entries, false, 0#usize)
      ?hbody ?hinv
  case hinv => simp
  case hbody =>
    rintro ⟨lo, hi, found, pos⟩ ⟨h1, h2⟩
    simp only [find_pos_loop.body]
    split
    · split
      · simp [Aeneas.Std.WP.spec_ok]
      · step* <;> scalar_tac
    · simp [Aeneas.Std.WP.spec_ok]
  obtain ⟨v, hloop, -⟩ := Aeneas.Std.WP.spec_imp_exists h
  rw [hloop]
  obtain ⟨lo, found, pos⟩ := v
  cases found <;> simp

end btree_kernel
