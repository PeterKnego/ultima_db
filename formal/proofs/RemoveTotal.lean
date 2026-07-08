/- Totality of `remove`: on a well-formed B-tree, `BTree.remove` never fails.

   The `remove_*` theorems in RemoveInv/RemoveGet/RemoveFrame are conditional on
   `= ok` because `NodeInv`/`HeightInv` admit a 0-entry internal node on which
   `fix_underfull_child` fails. `MinKeysInv` (`MinKeysInvariant.lean`) rules those
   out. Totality is essentially a *length* property (`RemoveTotalCore.lean` proves
   each rebalancer returns `ok` from alignment + nonemptiness + machine-capacity
   caps); here we thread `NodeInv` (alignment + the ≤ 63 arity that discharges the
   caps), `HeightInv` (to read off `NodeInv` of a recursive result via the F2/F3
   lemmas), and the nonempty invariant (`NE`/`NERoot`) down the delete recursion,
   so every call returns `ok`.

   `BTree.remove_spec` combines totality with the existing conditional results. -/
import BtreeKernel
import BtreeInvariant
import AlignedLemmas
import BalancedInvariant
import ChildrenSpecs
import EntrySpecs
import FindPosSpec
import MinKeysInvariant
import RemoveTotalCore
import RemoveInv
import RemoveGet

open Aeneas Aeneas.Std Result

namespace btree_kernel

set_option maxRecDepth 4096

/-- `MIN_KEYS` evaluates (it is a checked `32 - 1`). -/
private theorem min_keys_ok : ∃ m : Std.Usize, MIN_KEYS = ok m := by
  obtain ⟨m, hm, -⟩ := WP.spec_imp_exists MIN_KEYS_spec
  exact ⟨m, hm⟩

/-- A valid node has ≤ 64 children (leaf: 0; internal: entries + 1 ≤ 64). -/
theorem NodeInv.children_le64 {lo hi : Option Nat} {n : Node} (h : NodeInv lo hi n) :
    (clist n.children).length ≤ 64 := by
  cases h with
  | leaf entries _ _ _ => simp [clist]
  | internal entries c cs _ _ hlen hal =>
    have hh := hal.length_eq
    simp only [List.length_cons] at hh
    simp only [Node.children._simpLemma_, clist, List.length_cons]; omega

/-- Every node in an aligned family is a valid node: ≤ 63 entries and ≤ 64
    children (used to discharge the machine-capacity caps of the rebalancers). -/
theorem Aligned.mem_bounds {lo hi : Option Nat} {es : List (Std.U64 × Std.U64)}
    {cs : List Node} (h : Aligned lo hi es cs) :
    ∀ m ∈ cs, m.entries.val.length ≤ 63 ∧ (clist m.children).length ≤ 64 := by
  induction es generalizing lo cs with
  | nil =>
    cases h with
    | last c hc =>
      intro m hm
      simp only [List.mem_singleton] at hm; subst hm
      exact ⟨hc.entries_len_le, hc.children_le64⟩
  | cons e es ih =>
    cases h with
    | step _ _ c cs hc htail =>
      intro m hm
      rcases List.mem_cons.mp hm with rfl | hm
      · exact ⟨hc.entries_len_le, hc.children_le64⟩
      · exact ih htail m hm

/-- Wrapper over `maybe_fix_total`: the five machine-capacity caps all follow from
    uniform "every child ≤ 63 entries / ≤ 64 children" bounds plus the parent's
    own ≤ 63 arity. -/
theorem maybe_fix_total' (entries : alloc.vec.Vec (Std.U64 × Std.U64))
    (children : Children) (idx : Std.Usize) (underfull : Bool)
    (halign : (clist children).length = entries.val.length + 1)
    (hent : entries.val.length ≤ 63)
    (hnz : underfull = true → 0 < entries.val.length)
    (hidx : underfull = true → idx.val ≤ entries.val.length)
    (hb63 : ∀ m ∈ clist children, m.entries.val.length ≤ 63)
    (hbc : ∀ m ∈ clist children, (clist m.children).length ≤ 64) :
    ∃ out, maybe_fix entries children idx underfull = ok out := by
  have hmem : ∀ {i : Nat} {n : Node}, (clist children)[i]? = some n → n ∈ clist children :=
    fun h => List.mem_of_getElem? h
  refine maybe_fix_total entries children idx underfull halign hnz hidx ?_ ?_ ?_ ?_ ?_
  · -- hcl
    rw [halign]; scalar_tac
  · -- hins
    intro n hn; have h1 := hb63 n (hmem hn); scalar_tac
  · -- hgcl
    intro n hn; have h1 := hbc n (hmem hn); scalar_tac
  · -- hml
    intro nl nr hnl hnr
    have h1 := hb63 nl (hmem hnl); have h2 := hb63 nr (hmem hnr); scalar_tac
  · -- hmr
    intro nl nr hnl hnr
    have h1 := hb63 nl (hmem hnl); have h2 := hb63 nr (hmem hnr); scalar_tac

/-- `∀ m ∈ (l.set i n'), P m` from `P n'` and `∀ m ∈ l, P m`. -/
private theorem forall_mem_set {α : Type} {l : List α} {i : Nat} {x : α}
    {P : α → Prop} (hx : P x) (hl : ∀ m ∈ l, P m) : ∀ m ∈ l.set i x, P m := by
  intro m hm
  rcases List.mem_or_eq_of_mem_set hm with hm | rfl
  · exact hl m hm
  · exact hx

/-! ## `remove_leftmost` is total on a nonempty subtree -/

theorem remove_leftmost_total (fuel : Nat) {lo hi : Option Nat} {h : Nat} (node : Node)
    (hfuel : Node.size node ≤ fuel)
    (hinv : NodeInv lo hi node) (hh : HeightInv h node) (hne : NE node) :
    ∃ e n' uf, remove_leftmost node = ok (e, n', uf) := by
  induction fuel generalizing lo hi h node with
  | zero => exfalso; have := Node.one_le_size node; omega
  | succ fuel ih =>
    rw [remove_leftmost]
    cases hinv with
    | leaf entries hs hb hlen =>
      obtain ⟨cl, hcl, hclv⟩ :=
        WP.spec_imp_exists (children_len_spec Children.Nil (by simp [clist]))
      simp only [Node.entries._simpLemma_, Node.children._simpLemma_] at *
      simp only [hcl, bind_tc_ok]
      have hcl0 : cl = 0#usize := by
        have : cl.val = 0 := by simpa [clist] using hclv
        scalar_tac
      rw [if_pos hcl0]
      have hpos : 0 < entries.val.length := hne.entries_pos
      obtain ⟨first, hfirst, -⟩ :=
        WP.spec_imp_exists (alloc.vec.Vec.index_usize_spec entries 0#usize (by scalar_tac))
      obtain ⟨ne0, hne0, -⟩ :=
        WP.spec_imp_exists (remove_entry_at_spec entries 0#usize)
      obtain ⟨m, hm⟩ := min_keys_ok
      simp only [alloc.vec.Vec.index_slice_index, hfirst, hne0, hm, bind_tc_ok]
      exact ⟨_, _, _, rfl⟩
    | internal entries c cs hs hb hlen hal =>
      obtain ⟨cl, hcl, hclv⟩ :=
        WP.spec_imp_exists (children_len_spec (Children.Cons c cs) (by
          have := hal.length_eq
          simp only [clist, List.length_cons]
          scalar_tac))
      simp only [Node.entries._simpLemma_, Node.children._simpLemma_] at *
      simp only [hcl, bind_tc_ok]
      have halen : (clist (Children.Cons c cs)).length = entries.val.length + 1 := by
        simpa [clist] using hal.length_eq
      have hclne : cl ≠ 0#usize := by
        have : cl.val = (clist (Children.Cons c cs)).length := by simpa using hclv
        rw [halen] at this; scalar_tac
      rw [if_neg hclne]
      -- child 0 = c
      obtain ⟨n0, hn0, hn0v⟩ :=
        WP.spec_imp_exists (child_at_spec (Children.Cons c cs) 0#usize (by simp [clist]))
      simp only [clist, List.getElem_cons_zero] at hn0v
      obtain ⟨hlt, hNI_c⟩ := hal.child 0 (by simp)
      have hNIn0 : NodeInv (lbnd lo entries.val 0) (rbnd hi entries.val 0) n0 := by
        rw [hn0v]; simpa using hNI_c
      -- HeightInv of child 0
      obtain ⟨h', hHeq, hHc⟩ : ∃ h', h = h' + 1 ∧ HeightInv h' n0 := by
        cases hh with
        | internal _ c' cs' hch =>
          refine ⟨_, rfl, ?_⟩
          rw [hn0v]; exact (hch.allH) c (by simp [clist])
      have hNEn0 : NE n0 := by
        rw [hn0v]
        cases hne.children with | cons _ _ hc _ => exact hc
      have hszc : Node.size n0 ≤ fuel := by
        have hmem : n0 ∈ clist (Children.Cons c cs) := by rw [hn0v]; simp [clist]
        have h1 := size_mem_clist (Children.Cons c cs) n0 hmem
        have h2 : Node.size (Node.mk entries (Children.Cons c cs)) =
            1 + Children.size (Children.Cons c cs) := by simp [Node.size]
        simp only [Node.size] at hfuel ⊢
        omega
      obtain ⟨e', n', uf', hrl⟩ := ih n0 hszc hNIn0 hHc hNEn0
      simp only [hn0, hrl, bind_tc_ok]
      show ∃ e nn, ∃ (uf : Bool), (do
          let entries0 ← alloc.vec.CloneVec.clone (BuiltinClone (Std.U64 × Std.U64)) entries
          let children0 ← replace_child (Children.Cons c cs) 0#usize n'
          let (entries1, children1) ← maybe_fix entries0 children0 0#usize uf'
          let i2 ← MIN_KEYS
          ok (e', Node.mk entries1 children1, decide (alloc.vec.Vec.len entries1 < i2))) = ok (e, nn, uf)
      simp only [cloneVec_builtin_eq, bind_tc_ok]
      obtain ⟨children0, hrc, hrcv⟩ :=
        WP.spec_imp_exists (replace_child_spec (Children.Cons c cs) 0#usize n')
      simp only [hrc, bind_tc_ok]
      -- NodeInv of n' (for its ≤ 63 / ≤ 64 bounds) via F3
      obtain ⟨_, hNI_n', _, _⟩ :=
        remove_leftmost_spec (Node.size n0) n0 e' n' uf' (le_refl _) hNIn0 hHc hrl
      have halign0 : (clist children0).length = entries.val.length + 1 := by
        rw [hrcv]; simp only [List.length_set]; exact halen
      -- uniform child bounds for children0 = (clist (Cons c cs)).set 0 n'
      have hbounds := hal.mem_bounds
      have hb63 : ∀ m ∈ clist children0, m.entries.val.length ≤ 63 := by
        rw [hrcv]
        exact forall_mem_set hNI_n'.entries_len_le (fun m hm => (hbounds m hm).1)
      have hbc : ∀ m ∈ clist children0, (clist m.children).length ≤ 64 := by
        rw [hrcv]
        refine forall_mem_set hNI_n'.children_le64 (fun m hm => (hbounds m hm).2)
      obtain ⟨out, hmf⟩ :=
        maybe_fix_total' entries children0 0#usize uf' halign0 hlen
          (fun _ => hne.entries_pos) (fun _ => by scalar_tac) hb63 hbc
      obtain ⟨m, hm⟩ := min_keys_ok
      simp only [hmf, hm, bind_tc_ok]
      exact ⟨_, _, _, rfl⟩

/-! ## `delete_from_node` is total on a well-formed subtree -/

theorem delete_from_node_total (fuel : Nat) {lo hi : Option Nat} {h : Nat} (node : Node)
    (key : Std.U64) (hfuel : Node.size node ≤ fuel)
    (hinv : NodeInv lo hi node) (hh : HeightInv h node) (hne : NERoot node) :
    ∃ r, delete_from_node node key = ok r := by
  induction fuel generalizing lo hi h node with
  | zero => exfalso; have := Node.one_le_size node; omega
  | succ fuel ih =>
    rw [delete_from_node]
    cases hinv with
    | leaf entries hs hb hlen =>
      obtain ⟨⟨hit, pos⟩, hfp, hfpv⟩ :=
        WP.spec_imp_exists (find_pos_spec entries key hs)
      simp only [Node.entries._simpLemma_, Node.children._simpLemma_] at *
      simp only [hfp, bind_tc_ok]
      obtain ⟨cl, hcl, hclv⟩ :=
        WP.spec_imp_exists (children_len_spec Children.Nil (by simp [clist]))
      simp only [hcl, bind_tc_ok]
      have hcl0 : cl = 0#usize := by
        have : cl.val = 0 := by simpa [clist] using hclv
        scalar_tac
      show ∃ r, (if cl = 0#usize then
          if hit = true then do
            let entries ← remove_entry_at entries pos
            let i2 ← MIN_KEYS
            ok (DeleteResult.Removed (Node.mk entries Children.Nil) (decide (entries.len < i2)))
          else ok DeleteResult.NotFound
        else
          if hit = true then do
            let i1 ← pos + 1#usize
            let n ← child_at Children.Nil i1
            let (succ, new_right, right_underfull) ← remove_leftmost n
            let entries0 ← replace_entry entries pos succ
            let children0 ← replace_child Children.Nil i1 new_right
            let (entries1, children1) ← maybe_fix entries0 children0 i1 right_underfull
            let i3 ← MIN_KEYS
            ok (DeleteResult.Removed (Node.mk entries1 children1) (decide (entries1.len < i3)))
          else do
            let n ← child_at Children.Nil pos
            let dr ← delete_from_node n key
            match dr with
              | DeleteResult.NotFound => ok DeleteResult.NotFound
              | DeleteResult.Removed new_child underfull => do
                let entries0 ← alloc.vec.CloneVec.clone (BuiltinClone (Std.U64 × Std.U64)) entries
                let children0 ← replace_child Children.Nil pos new_child
                let (entries1, children1) ← maybe_fix entries0 children0 pos underfull
                let i3 ← MIN_KEYS
                ok (DeleteResult.Removed (Node.mk entries1 children1) (decide (entries1.len < i3)))) = ok r
      rw [if_pos hcl0]
      cases hit with
      | true =>
        obtain ⟨ne0, hne0, -⟩ :=
          WP.spec_imp_exists (remove_entry_at_spec entries pos)
        obtain ⟨m, hm⟩ := min_keys_ok
        simp only [hne0, hm, bind_tc_ok]
        exact ⟨_, rfl⟩
      | false => exact ⟨_, rfl⟩
    | internal entries c cs hs hb hlen hal =>
      obtain ⟨⟨hit, pos⟩, hfp, hfpv⟩ :=
        WP.spec_imp_exists (find_pos_spec entries key hs)
      obtain ⟨hple, hhit_prop, _⟩ := hfpv
      simp only [Node.entries._simpLemma_, Node.children._simpLemma_] at *
      simp only [hfp, bind_tc_ok]
      have halen : (clist (Children.Cons c cs)).length = entries.val.length + 1 := by
        simpa [clist] using hal.length_eq
      obtain ⟨cl, hcl, hclv⟩ :=
        WP.spec_imp_exists (children_len_spec (Children.Cons c cs) (by
          rw [halen]; scalar_tac))
      simp only [hcl, bind_tc_ok]
      have hclne : cl ≠ 0#usize := by
        have : cl.val = (clist (Children.Cons c cs)).length := by simpa using hclv
        rw [halen] at this; scalar_tac
      show ∃ r, (if cl = 0#usize then
          if hit = true then do
            let entries ← remove_entry_at entries pos
            let i2 ← MIN_KEYS
            ok (DeleteResult.Removed (Node.mk entries Children.Nil) (decide (entries.len < i2)))
          else ok DeleteResult.NotFound
        else
          if hit = true then do
            let i1 ← pos + 1#usize
            let n ← child_at (Children.Cons c cs) i1
            let (succ, new_right, right_underfull) ← remove_leftmost n
            let entries0 ← replace_entry entries pos succ
            let children0 ← replace_child (Children.Cons c cs) i1 new_right
            let (entries1, children1) ← maybe_fix entries0 children0 i1 right_underfull
            let i3 ← MIN_KEYS
            ok (DeleteResult.Removed (Node.mk entries1 children1) (decide (entries1.len < i3)))
          else do
            let n ← child_at (Children.Cons c cs) pos
            let dr ← delete_from_node n key
            match dr with
              | DeleteResult.NotFound => ok DeleteResult.NotFound
              | DeleteResult.Removed new_child underfull => do
                let entries0 ← alloc.vec.CloneVec.clone (BuiltinClone (Std.U64 × Std.U64)) entries
                let children0 ← replace_child (Children.Cons c cs) pos new_child
                let (entries1, children1) ← maybe_fix entries0 children0 pos underfull
                let i3 ← MIN_KEYS
                ok (DeleteResult.Removed (Node.mk entries1 children1) (decide (entries1.len < i3)))) = ok r
      rw [if_neg hclne]
      have hnz : 0 < entries.val.length :=
        hne.entries_pos_of_internal (by simp [clist])
      have hcne : ChildrenNE (Children.Cons c cs) :=
        hne.children_ne_of_internal (by simp [clist])
      have hbounds := hal.mem_bounds
      -- HeightInv of children (height h - 1)
      obtain ⟨h', hHeq, hHch⟩ : ∃ h', h = h' + 1 ∧ ChildrenHeight h' (Children.Cons c cs) := by
        cases hh with | internal _ c' cs' hch => exact ⟨_, rfl, hch⟩
      have hallH : AllH h' (clist (Children.Cons c cs)) := hHch.allH
      cases hit with
      | true =>
        have hposlt : pos.val < entries.val.length := by
          obtain ⟨h0, _⟩ := hhit_prop rfl; exact h0
        obtain ⟨i1, hi1, hi1p⟩ :=
          WP.spec_imp_exists (Usize.add_spec (x := pos) (y := 1#usize) (by scalar_tac))
        have hi1v : i1.val = pos.val + 1 := by scalar_tac
        simp only [hi1, bind_tc_ok]
        have hi1lt : i1.val < (clist (Children.Cons c cs)).length := by
          rw [halen]; omega
        obtain ⟨n1, hn1, hn1v⟩ :=
          WP.spec_imp_exists (child_at_spec (Children.Cons c cs) i1 hi1lt)
        have hi1le : i1.val ≤ entries.val.length := by omega
        obtain ⟨hlt, hNI_ch⟩ := hal.child i1.val (by rw [halen] at hi1lt; omega)
        have hNIn1 : NodeInv (lbnd lo entries.val i1.val) (rbnd hi entries.val i1.val) n1 := by
          rw [hn1v]; simpa [clist] using hNI_ch
        have hHn1 : HeightInv h' n1 := by rw [hn1v]; exact hallH _ (List.getElem_mem _)
        have hNEn1 : NE n1 := by
          rw [hn1v]; exact hcne.getElem i1.val (by rw [halen]; omega)
        obtain ⟨e', nr, ruf, hrl⟩ :=
          remove_leftmost_total (Node.size n1) n1 (le_refl _) hNIn1 hHn1 hNEn1
        simp only [hn1, hrl, bind_tc_ok]
        show ∃ r, (do
            let entries0 ← replace_entry entries pos e'
            let children0 ← replace_child (Children.Cons c cs) i1 nr
            let (entries1, children1) ← maybe_fix entries0 children0 i1 ruf
            let i3 ← MIN_KEYS
            ok (DeleteResult.Removed (Node.mk entries1 children1) (decide (entries1.len < i3)))) = ok r
        obtain ⟨e0, he0, he0v⟩ :=
          WP.spec_imp_exists (replace_entry_spec entries pos e')
        simp only [he0, bind_tc_ok]
        obtain ⟨children0, hrc, hrcv⟩ :=
          WP.spec_imp_exists (replace_child_spec (Children.Cons c cs) i1 nr)
        simp only [hrc, bind_tc_ok]
        obtain ⟨_, hNI_nr, _, _⟩ :=
          remove_leftmost_spec (Node.size n1) n1 e' nr ruf (le_refl _) hNIn1 hHn1 hrl
        have he0len : e0.val.length = entries.val.length := by
          rw [he0v]; simp [List.length_set]
        have halign0 : (clist children0).length = e0.val.length + 1 := by
          rw [hrcv]; simp only [List.length_set]; rw [halen, he0len]
        have hb63 : ∀ m ∈ clist children0, m.entries.val.length ≤ 63 := by
          rw [hrcv]
          exact forall_mem_set hNI_nr.entries_len_le (fun m hm => (hbounds m hm).1)
        have hbc : ∀ m ∈ clist children0, (clist m.children).length ≤ 64 := by
          rw [hrcv]
          refine forall_mem_set hNI_nr.children_le64 (fun m hm => (hbounds m hm).2)
        obtain ⟨out, hmf⟩ :=
          maybe_fix_total' e0 children0 i1 ruf halign0 (by rw [he0len]; exact hlen)
            (fun _ => by rw [he0len]; exact hnz) (fun _ => by rw [he0len]; omega) hb63 hbc
        obtain ⟨m, hm⟩ := min_keys_ok
        simp only [hmf, hm, bind_tc_ok]
        exact ⟨_, rfl⟩
      | false =>
        have hposle : pos.val ≤ entries.val.length := hple
        obtain ⟨n1, hn1, hn1v⟩ :=
          WP.spec_imp_exists (child_at_spec (Children.Cons c cs) pos (by rw [halen]; omega))
        obtain ⟨hlt, hNI_ch⟩ := hal.child pos.val hposle
        have hNIn1 : NodeInv (lbnd lo entries.val pos.val) (rbnd hi entries.val pos.val) n1 := by
          rw [hn1v]; simpa [clist] using hNI_ch
        have hHn1 : HeightInv h' n1 := by rw [hn1v]; exact hallH _ (List.getElem_mem _)
        have hNEn1 : NE n1 := by
          rw [hn1v]; exact hcne.getElem pos.val (by rw [halen]; omega)
        have hszc : Node.size n1 ≤ fuel := by
          have hmem : n1 ∈ clist (Children.Cons c cs) := by rw [hn1v]; exact List.getElem_mem _
          have h1 := size_mem_clist (Children.Cons c cs) n1 hmem
          have h2 : Node.size (Node.mk entries (Children.Cons c cs)) =
              1 + Children.size (Children.Cons c cs) := by simp [Node.size]
          simp only [Node.size] at hfuel ⊢
          omega
        obtain ⟨dr, hdr⟩ := ih n1 hszc hNIn1 hHn1 hNEn1.toNERoot
        simp only [hn1, hdr, bind_tc_ok]
        cases dr with
        | NotFound => exact ⟨_, rfl⟩
        | Removed new_child uf =>
          simp only [cloneVec_builtin_eq, bind_tc_ok]
          obtain ⟨children0, hrc, hrcv⟩ :=
            WP.spec_imp_exists (replace_child_spec (Children.Cons c cs) pos new_child)
          simp only [hrc, bind_tc_ok]
          obtain ⟨hNI_nc, _, _⟩ :=
            delete_from_node_inv (Node.size n1) n1 key new_child uf (le_refl _) hNIn1 hHn1 hdr
          have halign0 : (clist children0).length = entries.val.length + 1 := by
            rw [hrcv]; simp only [List.length_set]; exact halen
          have hb63 : ∀ m ∈ clist children0, m.entries.val.length ≤ 63 := by
            rw [hrcv]
            exact forall_mem_set hNI_nc.entries_len_le (fun m hm => (hbounds m hm).1)
          have hbc : ∀ m ∈ clist children0, (clist m.children).length ≤ 64 := by
            rw [hrcv]
            refine forall_mem_set hNI_nc.children_le64 (fun m hm => (hbounds m hm).2)
          obtain ⟨out, hmf⟩ :=
            maybe_fix_total' entries children0 pos uf halign0 hlen
              (fun _ => hnz) (fun _ => hposle) hb63 hbc
          obtain ⟨m, hm⟩ := min_keys_ok
          simp only [hmf, hm, bind_tc_ok]
          exact ⟨_, rfl⟩

/-! ## `BTree.remove` is total; the unconditional spec -/

/-- On a well-formed nonempty B-tree, `remove` never fails. -/
theorem BTree.remove_total (t : BTree) (key : Std.U64)
    (hinv : BTreeInv t) (hbal : BTreeBalanced t) (hmin : MinKeysInv t)
    (hlen : 0 < t.len.val) :
    ∃ r, BTree.remove t key = ok r := by
  obtain ⟨h, hh⟩ := hbal
  rw [BTree.remove]
  obtain ⟨dr, hdr⟩ :=
    delete_from_node_total (Node.size t.root) t.root key (le_refl _) hinv hh hmin.toNERoot
  simp only [hdr, bind_tc_ok]
  cases dr with
  | NotFound => exact ⟨_, rfl⟩
  | Removed new_root ufflag =>
    obtain ⟨hNI_nr, _, _⟩ :=
      delete_from_node_inv (Node.size t.root) t.root key new_root ufflag
        (le_refl _) hinv hh hdr
    have hnrbound : (clist new_root.children).length ≤ Std.Usize.max := by
      have h1 := hNI_nr.children_le64
      scalar_tac
    have hact : ∃ ar, (if alloc.vec.Vec.len new_root.entries = 0#usize then
          (do let i1 ← children_len new_root.children
              if i1 != 0#usize then
                (do let n ← child_at new_root.children 0#usize; clone_node n)
              else ok new_root)
        else ok new_root) = ok ar := by
      by_cases hi0 : alloc.vec.Vec.len new_root.entries = 0#usize
      · rw [if_pos hi0]
        obtain ⟨cl, hcl, hclv⟩ :=
          WP.spec_imp_exists (children_len_spec new_root.children hnrbound)
        simp only [hcl, bind_tc_ok]
        by_cases hclz : cl = 0#usize
        · rw [hclz]; simp only [bne_self_eq_false, if_false]; exact ⟨_, rfl⟩
        · rw [if_pos (by simpa using hclz)]
          have hlt0 : (0#usize).val < (clist new_root.children).length := by
            have : cl.val = (clist new_root.children).length := by simpa using hclv
            scalar_tac
          obtain ⟨n0, hn0, -⟩ :=
            WP.spec_imp_exists (child_at_spec new_root.children 0#usize hlt0)
          obtain ⟨cn, hcn, -⟩ := WP.spec_imp_exists (clone_node_spec n0)
          simp only [hn0, hcn, bind_tc_ok]; exact ⟨_, rfl⟩
      · rw [if_neg hi0]; exact ⟨_, rfl⟩
    obtain ⟨ar, har⟩ := hact
    obtain ⟨i1, hsub, -⟩ :=
      WP.spec_imp_exists (Usize.sub_spec (x := t.len) (y := 1#usize) (by scalar_tac))
    simp only [har, hsub, bind_tc_ok]
    exact ⟨_, rfl⟩

/-- Unconditional specification of `remove` on a well-formed nonempty tree:
    either the key was absent, or the result is a valid balanced tree with the
    key gone. Combines totality (`remove_total`) with the conditional results. -/
theorem BTree.remove_spec (t : BTree) (key : Std.U64)
    (hinv : BTreeInv t) (hbal : BTreeBalanced t) (hmin : MinKeysInv t)
    (hlen : 0 < t.len.val) :
    BTree.remove t key = ok none ∨
    ∃ t', BTree.remove t key = ok (some t') ∧
      BTreeInv t' ∧ BTreeBalanced t' ∧ t'.get key = ok none := by
  obtain ⟨r, hr⟩ := BTree.remove_total t key hinv hbal hmin hlen
  cases r with
  | none => exact Or.inl hr
  | some t' =>
    refine Or.inr ⟨t', hr, ?_, ?_, ?_⟩
    · exact (BTree.remove_inv t key t' hinv hbal hr).1
    · exact (BTree.remove_inv t key t' hinv hbal hr).2
    · exact BTree.remove_get t t' key hinv hbal hr

end btree_kernel
