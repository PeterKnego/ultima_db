/- Length-only (+ machine-capacity) TOTALITY of the delete-path rebalancers.

   Proves that `rotate_right`, `rotate_left`, `merge_with_left`,
   `merge_with_right`, `fix_underfull_child` and `maybe_fix` return `ok`
   (`∃ out, f … = ok out`) — i.e. never fail — from length facts alone.

   NOTE ON SIGNATURES (deviation from the brief):
   The brief's literal signatures (halign + index bounds only) are *false* at
   the `Usize.max` boundary and hence unprovable sorry-free: an Aeneas
   `alloc.vec.Vec` may have length exactly `Usize.max` (its invariant is
   `l.length ≤ Usize.max`), so `Vec.push`/`Vec.insert` (used by
   `insert_entry_at`, `merge_entries`) and the `children_len` /
   `last_child_singleton` / `drop_last_child` counters genuinely overflow and
   `fail`. `scalar_tac` cannot (and must not) close `v.val.length < Usize.max`
   or `(clist children).length ≤ Usize.max` from `halign` alone. In the
   corresponding `_inv` proofs (RemoveRebalance.lean) these caps are discharged
   by the arity invariant (entries ≤ 63, children ≤ 64). Since this file
   deliberately drops arity, each theorem below carries the *minimal* extra
   capacity hypotheses (in the same `(clist children)[i]? = some n → …` style as
   the brief's `hlnz`/`hrnz`) that its reachable branches actually require.
   All original hypotheses and theorem names are preserved. -/
import BtreeKernel
import BtreeInvariant
import EntrySpecs
import ChildrenSpecs
import RemoveSpecs

open Aeneas Aeneas.Std Result

namespace btree_kernel

set_option maxRecDepth 4096

-- Local copy of `RemoveRebalance.MIN_KEYS_spec` (kept here to avoid importing
-- the whole invariant-preservation file just for one 3-line lemma).
unseal MIN_KEYS T in
@[local step] theorem MIN_KEYS_spec_local : MIN_KEYS ⦃ m => m.val = 31 ⦄ := by
  simp only [MIN_KEYS, T]
  step*

/-- `rotate_right` never fails.
    Added caps: child `idx`'s entries fit (`insert_entry_at`), and child
    `idx-1`'s children count fits (`drop_last_child`/`last_child_singleton`). -/
theorem rotate_right_total
    (entries : alloc.vec.Vec (Std.U64 × Std.U64)) (children : Children) (idx : Std.Usize)
    (halign : (clist children).length = entries.val.length + 1)
    (hidx0 : 0 < idx.val) (hidx : idx.val ≤ entries.val.length)
    (hlnz : ∀ n, (clist children)[idx.val - 1]? = some n → 0 < n.entries.val.length)
    (hins : ∀ n, (clist children)[idx.val]? = some n → n.entries.val.length < Std.Usize.max)
    (hgcl : ∀ n, (clist children)[idx.val - 1]? = some n →
      (clist n.children).length ≤ Std.Usize.max) :
    ∃ out, rotate_right entries children idx = ok out := by
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
  have hll0 : 0 < left.entries.val.length := by
    apply hlnz left
    rw [List.getElem?_eq_getElem hchlt', hleftv]
  have hrcap : right.entries.val.length < Std.Usize.max := by
    apply hins right
    rw [List.getElem?_eq_getElem hchlt, hrightv]
  have hgccap : (clist left.children).length ≤ Std.Usize.max := by
    apply hgcl left
    rw [List.getElem?_eq_getElem hchlt', hleftv]
  obtain ⟨i1, hi1, hi1p⟩ := WP.spec_imp_exists
    (Usize.sub_spec (x := alloc.vec.Vec.len left.entries) (y := 1#usize)
      (by have := alloc.vec.Vec.len_val left.entries; scalar_tac))
  have hi1v : i1.val = left.entries.val.length - 1 := by
    have := alloc.vec.Vec.len_val left.entries
    scalar_tac
  obtain ⟨stolen, hstol, hstolv⟩ := WP.spec_imp_exists
    (alloc.vec.Vec.index_usize_spec left.entries i1 (by scalar_tac))
  obtain ⟨separator, hsep, hsepv⟩ := WP.spec_imp_exists
    (alloc.vec.Vec.index_usize_spec entries i (by scalar_tac))
  obtain ⟨new_entries, hne', hnev⟩ := WP.spec_imp_exists
    (replace_entry_spec entries i stolen)
  obtain ⟨nle, hnle', hnlev⟩ := WP.spec_imp_exists
    (entries_prefix_spec left.entries i1 (by scalar_tac))
  obtain ⟨nre, hnre', hnrev⟩ := WP.spec_imp_exists
    (insert_entry_at_spec right.entries 0#usize separator (by scalar_tac) (by scalar_tac))
  obtain ⟨nlc, hnlc', hnlcv⟩ := WP.spec_imp_exists
    (drop_last_child_spec left.children hgccap)
  obtain ⟨moved, hmv', hmvv⟩ := WP.spec_imp_exists
    (last_child_singleton_spec left.children hgccap)
  obtain ⟨nrc, hnrc', hnrcv⟩ := WP.spec_imp_exists
    (concat_children_spec moved right.children)
  obtain ⟨children1, hch1, hch1v⟩ := WP.spec_imp_exists
    (replace_child_spec children i (Node.mk nle nlc))
  obtain ⟨children2, hch2, hch2v⟩ := WP.spec_imp_exists
    (replace_child_spec children1 idx (Node.mk nre nrc))
  simp only [alloc.vec.Vec.index_slice_index, hisub, hleft, hright, hi1, hstol,
    hsep, hne', hnle', hnre', hnlc', hmv', hnrc', hch1, hch2, bind_tc_ok]
  exact ⟨_, rfl⟩

/-- `rotate_left` never fails.
    Added cap: child `idx`'s entries fit (`insert_entry_at`). -/
theorem rotate_left_total
    (entries : alloc.vec.Vec (Std.U64 × Std.U64)) (children : Children) (idx : Std.Usize)
    (halign : (clist children).length = entries.val.length + 1)
    (hidx : idx.val < entries.val.length)
    (hrnz : ∀ n, (clist children)[idx.val + 1]? = some n → 0 < n.entries.val.length)
    (hins : ∀ n, (clist children)[idx.val]? = some n → n.entries.val.length < Std.Usize.max) :
    ∃ out, rotate_left entries children idx = ok out := by
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
  have hrl0 : 0 < right.entries.val.length := by
    apply hrnz right
    rw [List.getElem?_eq_getElem hchlt1, hrightv]
  have hlcap : left.entries.val.length < Std.Usize.max := by
    apply hins left
    rw [List.getElem?_eq_getElem hchlt, hleftv]
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
  obtain ⟨nrc, hnrce, hnrcv⟩ := WP.spec_imp_exists
    (children_suffix_spec right.children 1#usize)
  obtain ⟨children1, hch1, hch1v⟩ := WP.spec_imp_exists
    (replace_child_spec children idx (Node.mk nle nlc))
  obtain ⟨children2, hch2, hch2v⟩ := WP.spec_imp_exists
    (replace_child_spec children1 i (Node.mk nre nrc))
  simp only [alloc.vec.Vec.index_slice_index, hleft, hiadd, hright, hstol,
    hsep, hnee, hnree, hnlee, hmve, hnlce, hnrce, hch1, hch2, bind_tc_ok]
  exact ⟨_, rfl⟩

/-- `merge_with_right` never fails.
    Added cap: children `idx` and `idx+1`'s entries jointly fit (`merge_entries`). -/
theorem merge_with_right_total
    (entries : alloc.vec.Vec (Std.U64 × Std.U64)) (children : Children) (idx : Std.Usize)
    (halign : (clist children).length = entries.val.length + 1)
    (hidx : idx.val < entries.val.length)
    (hmr : ∀ nl nr, (clist children)[idx.val]? = some nl →
      (clist children)[idx.val + 1]? = some nr →
      nl.entries.val.length + 1 + nr.entries.val.length ≤ Std.Usize.max) :
    ∃ out, merge_with_right entries children idx = ok out := by
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
  have hcap : left.entries.val.length + 1 + right.entries.val.length ≤ Std.Usize.max := by
    apply hmr left right
    · rw [List.getElem?_eq_getElem hchlt, hleftv]
    · rw [List.getElem?_eq_getElem hchlt1, hrightv]
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
  simp only [alloc.vec.Vec.index_slice_index, hsep, hleft, hiadd, hright, hmee,
    hmce, hnee, hch1, hch2, bind_tc_ok]
  exact ⟨_, rfl⟩

/-- `merge_with_left` never fails.
    Added cap: children `idx-1` and `idx`'s entries jointly fit (`merge_entries`). -/
theorem merge_with_left_total
    (entries : alloc.vec.Vec (Std.U64 × Std.U64)) (children : Children) (idx : Std.Usize)
    (halign : (clist children).length = entries.val.length + 1)
    (hidx0 : 0 < idx.val) (hidx : idx.val ≤ entries.val.length)
    (hml : ∀ nl nr, (clist children)[idx.val - 1]? = some nl →
      (clist children)[idx.val]? = some nr →
      nl.entries.val.length + 1 + nr.entries.val.length ≤ Std.Usize.max) :
    ∃ out, merge_with_left entries children idx = ok out := by
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
  have hcap : left.entries.val.length + 1 + right.entries.val.length ≤ Std.Usize.max := by
    apply hml left right
    · rw [List.getElem?_eq_getElem hchlt', hleftv]
    · rw [List.getElem?_eq_getElem hchlt, hrightv]
  obtain ⟨me, hmee, hmev⟩ := WP.spec_imp_exists
    (merge_entries_spec left.entries separator right.entries hcap)
  obtain ⟨mc, hmce, hmcv⟩ := WP.spec_imp_exists
    (concat_children_spec left.children right.children)
  obtain ⟨new_entries, hnee, hnev⟩ := WP.spec_imp_exists
    (remove_entry_at_spec entries i)
  obtain ⟨children1, hch1, hch1v⟩ := WP.spec_imp_exists
    (replace_child_spec children i (Node.mk me mc))
  obtain ⟨children2, hch2, hch2v⟩ := WP.spec_imp_exists
    (remove_child_at_spec children1 idx)
  simp only [alloc.vec.Vec.index_slice_index, hisub, hsep, hleft, hright, hmee,
    hmce, hnee, hch1, hch2, bind_tc_ok]
  exact ⟨_, rfl⟩

/-- `fix_underfull_child` never fails.
    Added caps: `children_len` fits (`hcl`); the branch it dispatches to gets
    the caps its sub-rebalancer needs (`hins` for the rotates' `insert_entry_at`,
    `hgcl` for `rotate_right`'s grandchild counters, `hml`/`hmr` for the merges).
    The sibling-nonemptiness side conditions are *derived* from the runtime
    `> MIN_KEYS` branch tests, exactly as in `fix_underfull_child_inv`. -/
theorem fix_underfull_child_total
    (entries : alloc.vec.Vec (Std.U64 × Std.U64)) (children : Children) (idx : Std.Usize)
    (halign : (clist children).length = entries.val.length + 1)
    (hnz : 0 < entries.val.length) (hidx : idx.val ≤ entries.val.length)
    (hcl : (clist children).length ≤ Std.Usize.max)
    (hins : ∀ n, (clist children)[idx.val]? = some n → n.entries.val.length < Std.Usize.max)
    (hgcl : ∀ n, (clist children)[idx.val - 1]? = some n →
      (clist n.children).length ≤ Std.Usize.max)
    (hml : ∀ nl nr, (clist children)[idx.val - 1]? = some nl →
      (clist children)[idx.val]? = some nr →
      nl.entries.val.length + 1 + nr.entries.val.length ≤ Std.Usize.max)
    (hmr : ∀ nl nr, (clist children)[idx.val]? = some nl →
      (clist children)[idx.val + 1]? = some nr →
      nl.entries.val.length + 1 + nr.entries.val.length ≤ Std.Usize.max) :
    ∃ out, fix_underfull_child entries children idx = ok out := by
  rw [fix_underfull_child]
  obtain ⟨n, hne, hnv⟩ := WP.spec_imp_exists (children_len_spec children hcl)
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
    obtain ⟨i2, hmk, hmkv⟩ := WP.spec_imp_exists MIN_KEYS_spec_local
    simp only [hisub, hn1e, hmk, bind_tc_ok]
    split
    · rename_i hgt
      have hn1len : 31 < n1.entries.val.length := by
        have := alloc.vec.Vec.len_val n1.entries
        scalar_tac
      exact rotate_right_total entries children idx halign hidx0 hidx
        (fun m hm => by
          rw [List.getElem?_eq_getElem
            (show idx.val - 1 < (clist children).length by omega)] at hm
          have hmn : m = n1 := by rw [hn1v]; exact (Option.some.inj hm).symm
          rw [hmn]; omega)
        hins hgcl
    · rename_i hngt
      obtain ⟨i3, hi3a, hi3p⟩ := WP.spec_imp_exists
        (Usize.add_spec (x := idx) (y := 1#usize) (by scalar_tac))
      have hi3v : i3.val = idx.val + 1 := by scalar_tac
      simp only [hi3a, bind_tc_ok]
      split
      · rename_i hlt3
        have hidxlt : idx.val < entries.val.length := by scalar_tac
        obtain ⟨n2, hn2e, hn2v⟩ := WP.spec_imp_exists
          (child_at_spec children i3 (by omega))
        simp only [hi3v] at hn2v
        simp only [hn2e, bind_tc_ok]
        split
        · rename_i hgt4
          have hn2len : 31 < n2.entries.val.length := by
            have := alloc.vec.Vec.len_val n2.entries
            scalar_tac
          exact rotate_left_total entries children idx halign hidxlt
            (fun m hm => by
              rw [List.getElem?_eq_getElem
                (show idx.val + 1 < (clist children).length by omega)] at hm
              have hmn : m = n2 := by rw [hn2v]; exact (Option.some.inj hm).symm
              rw [hmn]; omega)
            hins
        · rename_i hngt4
          exact merge_with_left_total entries children idx halign hidx0 hidx hml
      · rename_i hge3
        exact merge_with_left_total entries children idx halign hidx0 hidx hml
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
      obtain ⟨i2, hmk, hmkv⟩ := WP.spec_imp_exists MIN_KEYS_spec_local
      simp only [hn1e, hmk, bind_tc_ok]
      split
      · rename_i hgt
        have hn1len : 31 < n1.entries.val.length := by
          have := alloc.vec.Vec.len_val n1.entries
          scalar_tac
        exact rotate_left_total entries children idx halign hidxlt
          (fun m hm => by
            rw [List.getElem?_eq_getElem
              (show idx.val + 1 < (clist children).length by omega)] at hm
            have hmn : m = n1 := by rw [hn1v]; exact (Option.some.inj hm).symm
            rw [hmn]; omega)
          hins
      · rename_i hngt
        exact merge_with_right_total entries children idx halign hidxlt hmr
    · rename_i hge
      exfalso
      scalar_tac

/-- `maybe_fix` never fails: identity when `underfull = false`, otherwise
    delegates to `fix_underfull_child_total`. Carries the same capacity caps,
    which are only consumed on the `underfull = true` branch. -/
theorem maybe_fix_total
    (entries : alloc.vec.Vec (Std.U64 × Std.U64)) (children : Children)
    (idx : Std.Usize) (underfull : Bool)
    (halign : (clist children).length = entries.val.length + 1)
    (hnz : underfull = true → 0 < entries.val.length)
    (hidx : underfull = true → idx.val ≤ entries.val.length)
    (hcl : (clist children).length ≤ Std.Usize.max)
    (hins : ∀ n, (clist children)[idx.val]? = some n → n.entries.val.length < Std.Usize.max)
    (hgcl : ∀ n, (clist children)[idx.val - 1]? = some n →
      (clist n.children).length ≤ Std.Usize.max)
    (hml : ∀ nl nr, (clist children)[idx.val - 1]? = some nl →
      (clist children)[idx.val]? = some nr →
      nl.entries.val.length + 1 + nr.entries.val.length ≤ Std.Usize.max)
    (hmr : ∀ nl nr, (clist children)[idx.val]? = some nl →
      (clist children)[idx.val + 1]? = some nr →
      nl.entries.val.length + 1 + nr.entries.val.length ≤ Std.Usize.max) :
    ∃ out, maybe_fix entries children idx underfull = ok out := by
  rw [maybe_fix]
  cases underfull with
  | true =>
    exact fix_underfull_child_total entries children idx halign (hnz rfl) (hidx rfl)
      hcl hins hgcl hml hmr
  | false => exact ⟨(entries, children), rfl⟩

end btree_kernel
