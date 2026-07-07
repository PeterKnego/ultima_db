/- Specs for the delete-path helpers of the Aeneas-translated B-tree kernel:
   `remove_entry_at`, `merge_entries`, `remove_child_at`, `concat_children`,
   `last_child_singleton`, `drop_last_child`.

   The loop-based Vec specs mirror EntrySpecs.lean (`loop.spec_decr_nat`); the
   recursive Children specs mirror ChildrenSpecs.lean (`children_ind`). All
   primary specs are tagged `[step]` at the bottom of the file. -/
import BtreeKernel
import BtreeInvariant
import ChildrenSpecs

open Aeneas Aeneas.Std Result

namespace btree_kernel

/-! ## Pure list helper lemmas (eraseIdx / last-element surgery) -/

theorem eraseIdx_length_le {α : Type} (l : List α) (i : Nat) :
    (l.eraseIdx i).length ≤ l.length := by
  induction l generalizing i with
  | nil => simp
  | cons a t ih =>
    cases i with
    | zero => simp only [List.eraseIdx_cons_zero, List.length_cons]; omega
    | succ j =>
      simp only [List.eraseIdx_cons_succ, List.length_cons]
      exact Nat.succ_le_succ (ih j)

theorem eraseIdx_of_len_le {α : Type} (l : List α) (i : Nat) (h : l.length ≤ i) :
    l.eraseIdx i = l := by
  induction l generalizing i with
  | nil => simp
  | cons a t ih =>
    cases i with
    | zero => simp at h
    | succ j =>
      simp only [List.eraseIdx_cons_succ]
      rw [ih j (by simp only [List.length_cons] at h; omega)]

theorem eraseIdx_append_singleton_lt {α : Type} (l : List α) (a : α) (i : Nat)
    (h : i < l.length) :
    (l ++ [a]).eraseIdx i = l.eraseIdx i ++ [a] := by
  induction l generalizing i with
  | nil => simp at h
  | cons b t ih =>
    cases i with
    | zero => simp
    | succ j =>
      simp only [List.cons_append, List.eraseIdx_cons_succ]
      rw [ih j (by simp only [List.length_cons] at h; omega)]

theorem eraseIdx_append_singleton_eq {α : Type} (l : List α) (a : α) :
    (l ++ [a]).eraseIdx l.length = l := by
  induction l with
  | nil => simp
  | cons b t ih =>
    simp only [List.cons_append, List.length_cons, List.eraseIdx_cons_succ]
    rw [ih]

/-- Erasing a snoc'd list at an index `≠` the length of the prefix commutes
    with the snoc (either the erase hits inside `l`, or it is out of range on
    both sides). -/
theorem eraseIdx_snoc_ne {α : Type} (l : List α) (a : α) (i pos : Nat)
    (hlen : l.length = i) (hne : i ≠ pos) :
    (l ++ [a]).eraseIdx pos = l.eraseIdx pos ++ [a] := by
  rcases Nat.lt_or_ge pos i with hlt | hge
  · exact eraseIdx_append_singleton_lt l a pos (by omega)
  · have hgt : i < pos := by omega
    rw [eraseIdx_of_len_le l pos (by omega),
      eraseIdx_of_len_le (l ++ [a]) pos (by simp only [List.length_append,
        List.length_cons, List.length_nil]; omega)]

/-- Erasing a snoc'd list exactly at the prefix length drops the snoc'd
    element (and the erase is a no-op on the prefix alone). -/
theorem eraseIdx_snoc_eq {α : Type} (l : List α) (a : α) (pos : Nat)
    (hlen : l.length = pos) :
    (l ++ [a]).eraseIdx pos = l.eraseIdx pos := by
  rw [eraseIdx_of_len_le l pos (by omega)]
  subst hlen
  exact eraseIdx_append_singleton_eq l a

theorem cons_take_len {α : Type} (a : α) (L : List α) (hL : L ≠ []) :
    (a :: L).take L.length = a :: L.take (L.length - 1) := by
  obtain ⟨b, M, rfl⟩ := List.exists_cons_of_ne_nil hL
  simp only [List.length_cons, List.take_succ_cons, Nat.add_sub_cancel]

theorem cons_drop_len {α : Type} (a : α) (L : List α) (hL : L ≠ []) :
    (a :: L).drop L.length = L.drop (L.length - 1) := by
  obtain ⟨b, M, rfl⟩ := List.exists_cons_of_ne_nil hL
  simp only [List.length_cons, List.drop_succ_cons, Nat.add_sub_cancel]

/-! ## `remove_entry_at` -/

theorem remove_entry_at_spec (v : alloc.vec.Vec (Std.U64 × Std.U64))
    (pos : Std.Usize) :
    remove_entry_at v pos ⦃ out => out.val = v.val.eraseIdx pos.val ⦄ := by
  simp only [remove_entry_at, remove_entry_at_loop]
  have h := loop.spec_decr_nat
      (fun (y : (alloc.vec.Vec (Std.U64 × Std.U64)) × Std.Usize) => v.val.length - y.2.val)
      (fun y => y.1.val = (v.val.take y.2.val).eraseIdx pos.val)
      (fun out => out.val = v.val.eraseIdx pos.val)
      (fun (out1, i1) => remove_entry_at_loop.body v pos out1 i1)
      (alloc.vec.Vec.new (Std.U64 × Std.U64), 0#usize)
      ?hbody ?hinv
  case hinv => simp
  case hbody =>
    rintro ⟨out, i⟩ hinv0
    have hout : out.val = (v.val.take i.val).eraseIdx pos.val := hinv0
    have houtlen : out.val.length ≤ i.val := by
      have h1 := eraseIdx_length_le (v.val.take i.val) pos.val
      have h2 : (v.val.take i.val).length ≤ i.val := by
        rw [List.length_take]; omega
      rw [hout]; omega
    simp only [remove_entry_at_loop.body]
    split
    · -- i < v.len
      have hiltv : i.val < v.val.length := by scalar_tac
      have hpush : (if i != pos
            then
              do
              let p ←
                alloc.vec.Vec.index (core.slice.index.SliceIndexUsizeSlice (Std.U64
                  × Std.U64)) v i
              alloc.vec.Vec.push out p
            else ok out) ⦃ out1 =>
            out1.val = (v.val.take (i.val + 1)).eraseIdx pos.val ⦄ := by
        split
        · rename_i hne
          have hnev : i.val ≠ pos.val := by simpa using hne
          simp only [alloc.vec.Vec.index_slice_index]
          apply WP.spec_bind (alloc.vec.Vec.index_usize_spec v i (by scalar_tac))
          intro p hp
          apply WP.spec_mono (alloc.vec.Vec.push_spec out p (by scalar_tac))
          intro out1 hout1
          rw [hout1, hp, hout, List.take_add_one,
            List.getElem?_eq_getElem (by scalar_tac)]
          simp only [Option.toList_some]
          rw [eraseIdx_snoc_ne (v.val.take i.val) _ i.val pos.val
            (by rw [List.length_take]; omega) hnev]
        · rename_i heq
          have hiv : i.val = pos.val := by simpa using heq
          simp only [WP.spec_ok]
          rw [hout, List.take_add_one, List.getElem?_eq_getElem (by scalar_tac)]
          simp only [Option.toList_some]
          rw [eraseIdx_snoc_eq (v.val.take i.val) _ pos.val
            (by rw [List.length_take]; omega)]
      apply WP.spec_bind hpush
      intro out1 hout1
      apply WP.spec_bind (Usize.add_spec (by scalar_tac))
      intro i2 hi2
      have hi2' : i2.val = i.val + 1 := by scalar_tac
      simp only [WP.spec_ok]
      refine ⟨?_, by scalar_tac⟩
      show out1.val = (v.val.take i2.val).eraseIdx pos.val
      rw [hi2']
      exact hout1
    · -- exit: i ≥ v.len
      simp only [WP.spec_ok]
      show out.val = v.val.eraseIdx pos.val
      rw [hout, List.take_of_length_le (by scalar_tac)]
  exact h

/-! ## `merge_entries` -/

/-- The first `merge_entries` loop appends `left[i0..]` to the accumulator. -/
theorem merge_entries_loop0_spec (left out0 : alloc.vec.Vec (Std.U64 × Std.U64))
    (i0 : Std.Usize)
    (hcap : out0.val.length + (left.val.length - i0.val) ≤ Std.Usize.max) :
    merge_entries_loop0 left out0 i0 ⦃ out =>
      out.val = out0.val ++ left.val.drop i0.val ⦄ := by
  simp only [merge_entries_loop0]
  have h := loop.spec_decr_nat
      (fun (x : (alloc.vec.Vec (Std.U64 × Std.U64)) × Std.Usize) => left.val.length - x.2.val)
      (fun x => x.1.val ++ left.val.drop x.2.val = out0.val ++ left.val.drop i0.val)
      (fun out => out.val = out0.val ++ left.val.drop i0.val)
      (fun (out1, i1) => merge_entries_loop0.body left out1 i1)
      (out0, i0)
      ?hbody ?hinv
  case hinv => simp
  case hbody =>
    rintro ⟨out, i⟩ hinv0
    have hinv : out.val ++ left.val.drop i.val = out0.val ++ left.val.drop i0.val := hinv0
    have hlen := congrArg List.length hinv
    simp at hlen
    simp only [merge_entries_loop0.body]
    split
    · -- i < left.len
      simp only [alloc.vec.Vec.index_slice_index]
      apply WP.spec_bind (alloc.vec.Vec.index_usize_spec left i (by scalar_tac))
      intro p hp
      apply WP.spec_bind (alloc.vec.Vec.push_spec out p (by scalar_tac))
      intro out1 hout1
      apply WP.spec_bind (Usize.add_spec (by scalar_tac))
      intro i2 hi2
      have hi2' : i2.val = i.val + 1 := by scalar_tac
      simp only [WP.spec_ok]
      refine ⟨?_, by scalar_tac⟩
      show out1.val ++ left.val.drop i2.val = out0.val ++ left.val.drop i0.val
      rw [hout1, hp, hi2', List.append_assoc, List.singleton_append,
        ← List.drop_eq_getElem_cons (by scalar_tac)]
      exact hinv
    · -- exit
      simp only [WP.spec_ok]
      show out.val = out0.val ++ left.val.drop i0.val
      have hd : left.val.drop i.val = [] := by
        apply List.drop_eq_nil_of_le
        scalar_tac
      rw [← hinv, hd, List.append_nil]
  exact h

/-- The second `merge_entries` loop appends `right[j0..]` to the accumulator. -/
theorem merge_entries_loop1_spec (right out0 : alloc.vec.Vec (Std.U64 × Std.U64))
    (j0 : Std.Usize)
    (hcap : out0.val.length + (right.val.length - j0.val) ≤ Std.Usize.max) :
    merge_entries_loop1 right out0 j0 ⦃ out =>
      out.val = out0.val ++ right.val.drop j0.val ⦄ := by
  simp only [merge_entries_loop1]
  have h := loop.spec_decr_nat
      (fun (x : (alloc.vec.Vec (Std.U64 × Std.U64)) × Std.Usize) => right.val.length - x.2.val)
      (fun x => x.1.val ++ right.val.drop x.2.val = out0.val ++ right.val.drop j0.val)
      (fun out => out.val = out0.val ++ right.val.drop j0.val)
      (fun (out1, j1) => merge_entries_loop1.body right out1 j1)
      (out0, j0)
      ?hbody ?hinv
  case hinv => simp
  case hbody =>
    rintro ⟨out, j⟩ hinv0
    have hinv : out.val ++ right.val.drop j.val = out0.val ++ right.val.drop j0.val := hinv0
    have hlen := congrArg List.length hinv
    simp at hlen
    simp only [merge_entries_loop1.body]
    split
    · -- j < right.len
      simp only [alloc.vec.Vec.index_slice_index]
      apply WP.spec_bind (alloc.vec.Vec.index_usize_spec right j (by scalar_tac))
      intro p hp
      apply WP.spec_bind (alloc.vec.Vec.push_spec out p (by scalar_tac))
      intro out1 hout1
      apply WP.spec_bind (Usize.add_spec (by scalar_tac))
      intro j2 hj2
      have hj2' : j2.val = j.val + 1 := by scalar_tac
      simp only [WP.spec_ok]
      refine ⟨?_, by scalar_tac⟩
      show out1.val ++ right.val.drop j2.val = out0.val ++ right.val.drop j0.val
      rw [hout1, hp, hj2', List.append_assoc, List.singleton_append,
        ← List.drop_eq_getElem_cons (by scalar_tac)]
      exact hinv
    · -- exit
      simp only [WP.spec_ok]
      show out.val = out0.val ++ right.val.drop j0.val
      have hd : right.val.drop j.val = [] := by
        apply List.drop_eq_nil_of_le
        scalar_tac
      rw [← hinv, hd, List.append_nil]
  exact h

theorem merge_entries_spec (left : alloc.vec.Vec (Std.U64 × Std.U64))
    (sep : Std.U64 × Std.U64) (right : alloc.vec.Vec (Std.U64 × Std.U64))
    (hcap : left.val.length + 1 + right.val.length ≤ Std.Usize.max) :
    merge_entries left sep right ⦃ out =>
      out.val = left.val ++ sep :: right.val ⦄ := by
  simp only [merge_entries]
  apply WP.spec_bind (merge_entries_loop0_spec left
    (alloc.vec.Vec.new (Std.U64 × Std.U64)) 0#usize (by scalar_tac))
  intro out0 hout0
  have hout0' : out0.val = left.val := by simpa using hout0
  have h0len : out0.val.length = left.val.length := by rw [hout0']
  apply WP.spec_bind (alloc.vec.Vec.push_spec out0 sep (by scalar_tac))
  intro out1 hout1
  have h1len : out1.val.length = left.val.length + 1 := by
    rw [hout1]
    simp [h0len]
  apply WP.spec_mono (merge_entries_loop1_spec right out1 0#usize (by scalar_tac))
  intro out hout
  rw [hout, hout1, hout0']
  simp

/-! ## `remove_child_at` -/

theorem remove_child_at_spec (c : Children) (pos : Std.Usize) :
    remove_child_at c pos ⦃ out => clist out = (clist c).eraseIdx pos.val ⦄ := by
  induction c using children_ind generalizing pos with
  | nil => rw [remove_child_at]; simp [clist]
  | cons n rest ih =>
    rw [remove_child_at]
    split
    · rename_i hp0
      obtain ⟨c1, hc, hcv⟩ := WP.spec_imp_exists (clone_children_spec rest)
      have hpv : pos.val = 0 := by scalar_tac
      simp only [hc, WP.spec_ok]
      simp [clist, hpv, hcv]
    · rename_i hp0
      have h1 : (1#usize).val ≤ pos.val := by scalar_tac
      obtain ⟨n1, hn, hnv⟩ := WP.spec_imp_exists (clone_node_spec n)
      obtain ⟨i, hi, hiv⟩ := WP.spec_imp_exists (Usize.sub_spec h1)
      obtain ⟨c1, hc, hcv⟩ := WP.spec_imp_exists (ih i)
      simp only [hn, hi, hc, bind_tc_ok, WP.spec_ok]
      have hpv : pos.val = i.val + 1 := by scalar_tac
      simp [clist, hpv, hnv, hcv, List.eraseIdx_cons_succ]

/-! ## `concat_children` -/

theorem concat_children_spec (a b : Children) :
    concat_children a b ⦃ out => clist out = clist a ++ clist b ⦄ := by
  induction a using children_ind with
  | nil =>
    rw [concat_children]
    obtain ⟨d, hd, hdv⟩ := WP.spec_imp_exists (clone_children_spec b)
    simp only [hd, WP.spec_ok]
    simp [clist, hdv]
  | cons n rest ih =>
    rw [concat_children]
    obtain ⟨n1, hn, hnv⟩ := WP.spec_imp_exists (clone_node_spec n)
    obtain ⟨c1, hc, hcv⟩ := WP.spec_imp_exists ih
    simp only [hn, hc, bind_tc_ok, WP.spec_ok]
    simp [clist, hnv, hcv]

/-! ## `last_child_singleton` -/

theorem last_child_singleton_spec (c : Children)
    (hlen : (clist c).length ≤ Std.Usize.max) :
    last_child_singleton c ⦃ out =>
      clist out = (clist c).drop ((clist c).length - 1) ⦄ := by
  induction c using children_ind with
  | nil => rw [last_child_singleton]; simp [clist]
  | cons n rest ih =>
    rw [last_child_singleton]
    have hlen' : (clist rest).length ≤ Std.Usize.max := by
      simp [clist] at hlen; omega
    obtain ⟨i, hi, hiv⟩ := WP.spec_imp_exists (children_len_spec rest hlen')
    simp only [hi, bind_tc_ok]
    split
    · rename_i hi0
      have h0 : (clist rest).length = 0 := by scalar_tac
      have hnil : clist rest = [] := List.eq_nil_of_length_eq_zero h0
      obtain ⟨n1, hn, hnv⟩ := WP.spec_imp_exists (clone_node_spec n)
      simp only [hn, bind_tc_ok, WP.spec_ok]
      simp [clist, hnil, hnv]
    · rename_i hi0
      have hpos : 1 ≤ (clist rest).length := by scalar_tac
      have hne : clist rest ≠ [] := by
        intro hcontra
        rw [hcontra] at hpos
        simp at hpos
      apply WP.spec_mono (ih hlen')
      intro out hout
      rw [hout]
      simp only [clist, List.length_cons, Nat.add_sub_cancel]
      rw [cons_drop_len n (clist rest) hne]

/-! ## `drop_last_child` -/

theorem drop_last_child_spec (c : Children)
    (hlen : (clist c).length ≤ Std.Usize.max) :
    drop_last_child c ⦃ out =>
      clist out = (clist c).take ((clist c).length - 1) ⦄ := by
  induction c using children_ind with
  | nil => rw [drop_last_child]; simp [clist]
  | cons n rest ih =>
    rw [drop_last_child]
    have hlen' : (clist rest).length ≤ Std.Usize.max := by
      simp [clist] at hlen; omega
    obtain ⟨i, hi, hiv⟩ := WP.spec_imp_exists (children_len_spec rest hlen')
    simp only [hi, bind_tc_ok]
    split
    · rename_i hi0
      have h0 : (clist rest).length = 0 := by scalar_tac
      have hnil : clist rest = [] := List.eq_nil_of_length_eq_zero h0
      simp only [WP.spec_ok]
      simp [clist, hnil]
    · rename_i hi0
      have hpos : 1 ≤ (clist rest).length := by scalar_tac
      have hne : clist rest ≠ [] := by
        intro hcontra
        rw [hcontra] at hpos
        simp at hpos
      obtain ⟨n1, hn, hnv⟩ := WP.spec_imp_exists (clone_node_spec n)
      obtain ⟨c1, hc, hcv⟩ := WP.spec_imp_exists (ih hlen')
      simp only [hn, hc, bind_tc_ok, WP.spec_ok]
      simp only [clist, List.length_cons, Nat.add_sub_cancel]
      rw [cons_take_len n (clist rest) hne, hnv, hcv]

/-! ## Convenience snoc-forms for the last-child helpers -/

/-- If the children list is known as `init ++ [lst]`, `last_child_singleton`
    returns exactly `[lst]`. -/
theorem last_child_singleton_spec_snoc (c : Children) (init : List Node)
    (lst : Node) (hsplit : clist c = init ++ [lst])
    (hlen : (clist c).length ≤ Std.Usize.max) :
    last_child_singleton c ⦃ out => clist out = [lst] ⦄ := by
  apply WP.spec_mono (last_child_singleton_spec c hlen)
  intro out hout
  rw [hout, hsplit]
  simp

/-- If the children list is known as `init ++ [lst]`, `drop_last_child`
    returns exactly `init`. -/
theorem drop_last_child_spec_snoc (c : Children) (init : List Node)
    (lst : Node) (hsplit : clist c = init ++ [lst])
    (hlen : (clist c).length ≤ Std.Usize.max) :
    drop_last_child c ⦃ out => clist out = init ⦄ := by
  apply WP.spec_mono (drop_last_child_spec c hlen)
  intro out hout
  rw [hout, hsplit]
  simp

attribute [step] remove_entry_at_spec merge_entries_spec remove_child_at_spec
  concat_children_spec last_child_singleton_spec drop_last_child_spec

end btree_kernel
