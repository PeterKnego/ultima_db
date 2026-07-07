/- Specs for the entry-vector helpers of the Aeneas-translated B-tree kernel:
   `entries_suffix`, `entries_prefix`, `replace_entry`, `insert_entry_at`. -/
import BtreeKernel
import BtreeInvariant

open Aeneas Aeneas.Std Result

namespace btree_kernel

/-- General spec for the suffix-copying loop: starting from accumulator `out0`
    at index `i0`, the loop appends `v[i0..]` to `out0`. -/
theorem entries_suffix_loop_spec (v out0 : alloc.vec.Vec (Std.U64 × Std.U64))
    (i0 : Std.Usize)
    (hcap : out0.val.length + (v.val.length - i0.val) ≤ Std.Usize.max) :
    entries_suffix_loop v out0 i0 ⦃ out => out.val = out0.val ++ v.val.drop i0.val ⦄ := by
  simp only [entries_suffix_loop]
  have h := loop.spec_decr_nat
      (fun (x : (alloc.vec.Vec (Std.U64 × Std.U64)) × Std.Usize) => v.val.length - x.2.val)
      (fun x => x.1.val ++ v.val.drop x.2.val = out0.val ++ v.val.drop i0.val)
      (fun out => out.val = out0.val ++ v.val.drop i0.val)
      (fun (out1, i1) => entries_suffix_loop.body v out1 i1)
      (out0, i0)
      ?hbody ?hinv
  case hinv => simp
  case hbody =>
    rintro ⟨out, i⟩ hinv0
    have hinv : out.val ++ v.val.drop i.val = out0.val ++ v.val.drop i0.val := hinv0
    have hlen := congrArg List.length hinv
    simp at hlen
    simp only [entries_suffix_loop.body]
    split
    · -- i < v.len
      simp only [alloc.vec.Vec.index_slice_index]
      apply WP.spec_bind (alloc.vec.Vec.index_usize_spec v i (by scalar_tac))
      intro p hp
      apply WP.spec_bind (alloc.vec.Vec.push_spec out p (by scalar_tac))
      intro out1 hout1
      apply WP.spec_bind (Usize.add_spec (by scalar_tac))
      intro i2 hi2
      have hi2' : i2.val = i.val + 1 := by scalar_tac
      simp only [WP.spec_ok]
      refine ⟨?_, by scalar_tac⟩
      show out1.val ++ v.val.drop i2.val = out0.val ++ v.val.drop i0.val
      rw [hout1, hp, hi2', List.append_assoc, List.singleton_append,
        ← List.drop_eq_getElem_cons (by scalar_tac)]
      exact hinv
    · -- exit
      simp only [WP.spec_ok]
      show out.val = out0.val ++ v.val.drop i0.val
      have hd : v.val.drop i.val = [] := by
        apply List.drop_eq_nil_of_le
        scalar_tac
      rw [← hinv, hd, List.append_nil]
  exact h

theorem entries_suffix_spec (v : alloc.vec.Vec (Std.U64 × Std.U64)) (n : Std.Usize) :
    entries_suffix v n ⦃ out => out.val = v.val.drop n.val ⦄ := by
  have h := entries_suffix_loop_spec v (alloc.vec.Vec.new (Std.U64 × Std.U64)) n
    (by scalar_tac)
  apply WP.spec_mono h
  intro out hout
  simpa using hout

theorem entries_prefix_spec (v : alloc.vec.Vec (Std.U64 × Std.U64)) (n : Std.Usize)
    (hn : n.val ≤ v.val.length) :
    entries_prefix v n ⦃ out => out.val = v.val.take n.val ⦄ := by
  simp only [entries_prefix, entries_prefix_loop]
  have h := loop.spec_decr_nat
      (fun (x : (alloc.vec.Vec (Std.U64 × Std.U64)) × Std.Usize) => n.val - x.2.val)
      (fun x => x.1.val = v.val.take x.2.val ∧ x.2.val ≤ n.val)
      (fun out => out.val = v.val.take n.val)
      (fun (out1, i1) => entries_prefix_loop.body v n out1 i1)
      (alloc.vec.Vec.new (Std.U64 × Std.U64), 0#usize)
      ?hbody ?hinv
  case hinv => simp
  case hbody =>
    rintro ⟨out, i⟩ hinv0
    have hout : out.val = v.val.take i.val := hinv0.1
    have hile : i.val ≤ n.val := hinv0.2
    have hlen := congrArg List.length hout
    simp at hlen
    simp only [entries_prefix_loop.body]
    split
    · -- i < n
      simp only [alloc.vec.Vec.index_slice_index]
      apply WP.spec_bind (alloc.vec.Vec.index_usize_spec v i (by scalar_tac))
      intro p hp
      apply WP.spec_bind (alloc.vec.Vec.push_spec out p (by scalar_tac))
      intro out1 hout1
      apply WP.spec_bind (Usize.add_spec (by scalar_tac))
      intro i2 hi2
      have hi2' : i2.val = i.val + 1 := by scalar_tac
      simp only [WP.spec_ok]
      refine ⟨⟨?_, by scalar_tac⟩, by scalar_tac⟩
      show out1.val = v.val.take i2.val
      rw [hout1, hp, hout, hi2', List.take_add_one,
        List.getElem?_eq_getElem (by scalar_tac)]
      simp
    · -- exit: i = n
      simp only [WP.spec_ok]
      show out.val = v.val.take n.val
      have hi : i.val = n.val := by scalar_tac
      rw [hout, hi]
  exact h

theorem replace_entry_spec (v : alloc.vec.Vec (Std.U64 × Std.U64)) (pos : Std.Usize)
    (x : Std.U64 × Std.U64) :
    replace_entry v pos x ⦃ out => out.val = v.val.set pos.val x ⦄ := by
  simp only [replace_entry, replace_entry_loop]
  have h := loop.spec_decr_nat
      (fun (y : (alloc.vec.Vec (Std.U64 × Std.U64)) × Std.Usize) => v.val.length - y.2.val)
      (fun y => y.1.val = (v.val.set pos.val x).take y.2.val)
      (fun out => out.val = v.val.set pos.val x)
      (fun (out1, i1) => replace_entry_loop.body v pos x out1 i1)
      (alloc.vec.Vec.new (Std.U64 × Std.U64), 0#usize)
      ?hbody ?hinv
  case hinv => simp
  case hbody =>
    rintro ⟨out, i⟩ hinv0
    have hout : out.val = (v.val.set pos.val x).take i.val := hinv0
    have hset_len : ((v.val).set pos.val x).length = v.val.length := by simp
    have hlen := congrArg List.length hout
    simp at hlen
    simp only [replace_entry_loop.body]
    split
    · -- i < v.len
      rename_i hilt
      -- The inner if produces the pushed accumulator `out1`.
      have hpush : (if i = pos then alloc.vec.Vec.push out x
            else do
              let p ← alloc.vec.Vec.index
                (core.slice.index.SliceIndexUsizeSlice (Std.U64 × Std.U64)) v i
              alloc.vec.Vec.push out p) ⦃ out1 =>
            out1.val = out.val ++ [(v.val.set pos.val x)[i.val]'(by scalar_tac)] ⦄ := by
        split
        · rename_i hipos
          apply WP.spec_mono (alloc.vec.Vec.push_spec out x (by scalar_tac))
          intro out1 hout1
          have hip : i.val = pos.val := by scalar_tac
          simp only [hout1]
          simp [hip]
        · rename_i hipos
          simp only [alloc.vec.Vec.index_slice_index]
          apply WP.spec_bind (alloc.vec.Vec.index_usize_spec v i (by scalar_tac))
          intro p hp
          apply WP.spec_mono (alloc.vec.Vec.push_spec out p (by scalar_tac))
          intro out1 hout1
          rw [hout1, hp, List.getElem_set_ne (by scalar_tac)]
      apply WP.spec_bind hpush
      intro out1 hout1
      apply WP.spec_bind (Usize.add_spec (by scalar_tac))
      intro i2 hi2
      have hi2' : i2.val = i.val + 1 := by scalar_tac
      simp only [WP.spec_ok]
      refine ⟨?_, by scalar_tac⟩
      show out1.val = (v.val.set pos.val x).take i2.val
      rw [hout1, hout, hi2', List.take_add_one,
        List.getElem?_eq_getElem (by scalar_tac)]
      simp
    · -- exit
      simp only [WP.spec_ok]
      show out.val = v.val.set pos.val x
      rw [hout]
      apply List.take_of_length_le
      scalar_tac
  exact h

/-- The first `insert_entry_at` loop copies `v[0..pos]` and stops at `pos`. -/
theorem insert_entry_at_loop0_spec (v : alloc.vec.Vec (Std.U64 × Std.U64))
    (pos : Std.Usize) (hpos : pos.val ≤ v.val.length) :
    insert_entry_at_loop0 v pos (alloc.vec.Vec.new (Std.U64 × Std.U64)) 0#usize
      ⦃ r => r.1.val = v.val.take pos.val ∧ r.2.val = pos.val ⦄ := by
  simp only [insert_entry_at_loop0]
  have h := loop.spec_decr_nat
      (fun (y : (alloc.vec.Vec (Std.U64 × Std.U64)) × Std.Usize) => pos.val - y.2.val)
      (fun y => y.1.val = v.val.take y.2.val ∧ y.2.val ≤ pos.val)
      (fun (r : (alloc.vec.Vec (Std.U64 × Std.U64)) × Std.Usize) =>
        r.1.val = v.val.take pos.val ∧ r.2.val = pos.val)
      (fun (out1, i1) => insert_entry_at_loop0.body v pos out1 i1)
      (alloc.vec.Vec.new (Std.U64 × Std.U64), 0#usize)
      ?hbody ?hinv
  case hinv => simp
  case hbody =>
    rintro ⟨out, i⟩ hinv0
    have hout : out.val = v.val.take i.val := hinv0.1
    have hile : i.val ≤ pos.val := hinv0.2
    have hlen := congrArg List.length hout
    simp at hlen
    simp only [insert_entry_at_loop0.body]
    split
    · -- i < pos
      simp only [alloc.vec.Vec.index_slice_index]
      apply WP.spec_bind (alloc.vec.Vec.index_usize_spec v i (by scalar_tac))
      intro p hp
      apply WP.spec_bind (alloc.vec.Vec.push_spec out p (by scalar_tac))
      intro out1 hout1
      apply WP.spec_bind (Usize.add_spec (by scalar_tac))
      intro i2 hi2
      have hi2' : i2.val = i.val + 1 := by scalar_tac
      simp only [WP.spec_ok]
      refine ⟨⟨?_, by scalar_tac⟩, by scalar_tac⟩
      show out1.val = v.val.take i2.val
      rw [hout1, hp, hout, hi2', List.take_add_one,
        List.getElem?_eq_getElem (by scalar_tac)]
      simp
    · -- exit: i = pos
      simp only [WP.spec_ok]
      have hi : i.val = pos.val := by scalar_tac
      show out.val = v.val.take pos.val ∧ i.val = pos.val
      rw [hout, hi]
      simp
  exact h

/-- The second `insert_entry_at` loop appends `v[i0..]` to the accumulator. -/
theorem insert_entry_at_loop1_spec (v out0 : alloc.vec.Vec (Std.U64 × Std.U64))
    (i0 : Std.Usize)
    (hcap : out0.val.length + (v.val.length - i0.val) ≤ Std.Usize.max) :
    insert_entry_at_loop1 v out0 i0 ⦃ out => out.val = out0.val ++ v.val.drop i0.val ⦄ := by
  simp only [insert_entry_at_loop1]
  have h := loop.spec_decr_nat
      (fun (x : (alloc.vec.Vec (Std.U64 × Std.U64)) × Std.Usize) => v.val.length - x.2.val)
      (fun x => x.1.val ++ v.val.drop x.2.val = out0.val ++ v.val.drop i0.val)
      (fun out => out.val = out0.val ++ v.val.drop i0.val)
      (fun (out1, i1) => insert_entry_at_loop1.body v out1 i1)
      (out0, i0)
      ?hbody ?hinv
  case hinv => simp
  case hbody =>
    rintro ⟨out, i⟩ hinv0
    have hinv : out.val ++ v.val.drop i.val = out0.val ++ v.val.drop i0.val := hinv0
    have hlen := congrArg List.length hinv
    simp at hlen
    simp only [insert_entry_at_loop1.body]
    split
    · -- i < v.len
      simp only [alloc.vec.Vec.index_slice_index]
      apply WP.spec_bind (alloc.vec.Vec.index_usize_spec v i (by scalar_tac))
      intro p hp
      apply WP.spec_bind (alloc.vec.Vec.push_spec out p (by scalar_tac))
      intro out1 hout1
      apply WP.spec_bind (Usize.add_spec (by scalar_tac))
      intro i2 hi2
      have hi2' : i2.val = i.val + 1 := by scalar_tac
      simp only [WP.spec_ok]
      refine ⟨?_, by scalar_tac⟩
      show out1.val ++ v.val.drop i2.val = out0.val ++ v.val.drop i0.val
      rw [hout1, hp, hi2', List.append_assoc, List.singleton_append,
        ← List.drop_eq_getElem_cons (by scalar_tac)]
      exact hinv
    · -- exit
      simp only [WP.spec_ok]
      show out.val = out0.val ++ v.val.drop i0.val
      have hd : v.val.drop i.val = [] := by
        apply List.drop_eq_nil_of_le
        scalar_tac
      rw [← hinv, hd, List.append_nil]
  exact h

theorem insert_entry_at_spec (v : alloc.vec.Vec (Std.U64 × Std.U64)) (pos : Std.Usize)
    (x : Std.U64 × Std.U64) (hpos : pos.val ≤ v.val.length)
    (hcap : v.val.length < Std.Usize.max) :
    insert_entry_at v pos x ⦃ out =>
      out.val = v.val.take pos.val ++ x :: v.val.drop pos.val ⦄ := by
  simp only [insert_entry_at]
  apply WP.spec_bind (insert_entry_at_loop0_spec v pos hpos)
  rintro ⟨out0, i0⟩ hr
  have hout0 : out0.val = v.val.take pos.val := hr.1
  have hi0 : i0.val = pos.val := hr.2
  have h0len : out0.val.length = pos.val := by
    rw [hout0]
    simp only [List.length_take]
    scalar_tac
  apply WP.spec_bind (alloc.vec.Vec.push_spec out0 x (by scalar_tac))
  intro out1 hout1
  have h1len : out1.val.length = pos.val + 1 := by
    rw [hout1]
    simp [h0len]
  apply WP.spec_mono (insert_entry_at_loop1_spec v out1 i0 (by scalar_tac))
  intro out hout
  rw [hout, hout1, hout0, hi0, List.append_assoc, List.singleton_append]

end btree_kernel
