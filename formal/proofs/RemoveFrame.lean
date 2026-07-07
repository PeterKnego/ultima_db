/- Functional correctness of `BTree.remove`, part 2: the frame property.

   `BTree.remove_frame`: removing `key` leaves `get k'` unchanged for every
   `k' ≠ key`.  Built on the in-order `flatten` foundation (RemoveFlatten.lean):
   `get_flatten` characterises `get` as a lookup in the flattened list, and the
   new `delete_from_node_flatten` shows delete's exact effect on the flattened
   list is `filter (·.1 ≠ key)`.  Filtering out `key` entries does not change the
   lookup of any other key, so `get k'` is preserved.

   Verify axioms with `#print axioms BTree.remove_frame`: only
   `propext`/`Classical.choice`/`Quot.sound`. -/
import RemoveFlatten
import RemoveInv

open Aeneas Aeneas.Std Result

namespace btree_kernel

set_option maxRecDepth 4096

/-! ## Private monadic deconstruction helpers (local copies) -/

private theorem bind_ok_inv {α β : Type} {m : Result α} {k : α → Result β}
    {out : β} (h : (do let a ← m; k a) = ok out) :
    ∃ a, m = ok a ∧ k a = ok out := by
  cases m with
  | ok a => exact ⟨a, rfl, by rwa [bind_tc_ok] at h⟩
  | fail e => rw [bind_tc_fail] at h; exact absurd h (by simp)
  | div => rw [bind_tc_div] at h; exact absurd h (by simp)

private theorem spec_elim {α : Type} {m : Result α} {p : α → Prop}
    (hspec : WP.spec m p) {v : α} (heq : m = ok v) : p v := by
  obtain ⟨w, hw, hp⟩ := WP.spec_imp_exists hspec
  rw [heq] at hw; cases hw; exact hp

private theorem set_succ_eq_take_getElem_cons_drop {α : Type} (l : List α)
    (j : Nat) (a : α) (h : j + 1 < l.length) :
    l.set (j + 1) a = l.take j ++ (l[j]'(by omega)) :: a :: l.drop (j + 2) := by
  induction l generalizing j with
  | nil => simp at h
  | cons x t ih =>
    cases j with
    | zero => match t, h with | y :: t', _ => simp
    | succ k =>
      simp only [List.set_cons_succ, List.take_succ_cons,
        List.getElem_cons_succ, List.cons_append]
      have ht : (x :: t).drop (k + 1 + 2) = t.drop (k + 2) := by
        simp [List.drop_succ_cons]
      rw [ht, ih k (by simp only [List.length_cons] at h; omega)]

/-! ## The delete filter predicate -/

/-- Bool predicate: this entry's key is *not* `key` (delete keeps everything
    else). -/
def keyNe (key : Std.U64) : Std.U64 × Std.U64 → Bool := fun p => p.1.val != key.val

@[simp] theorem keyNe_eq_true (key : Std.U64) (p : Std.U64 × Std.U64) :
    keyNe key p = true ↔ p.1.val ≠ key.val := by
  simp [keyNe, bne_iff_ne]

@[simp] theorem keyNe_eq_false (key : Std.U64) (p : Std.U64 × Std.U64) :
    keyNe key p = false ↔ p.1.val = key.val := by
  simp [keyNe]

/-! ## Pure list / `lookupKey` lemmas -/

/-- Filtering out `key` entries does not change the lookup of a different key. -/
theorem lookupKey_filter (k' key : Std.U64) (l : List (Std.U64 × Std.U64))
    (hne : k'.val ≠ key.val) :
    lookupKey k' (l.filter (keyNe key)) = lookupKey k' l := by
  induction l with
  | nil => rfl
  | cons p rest ih =>
    rw [List.filter_cons]
    by_cases hp : keyNe key p = true
    · rw [if_pos hp, lookupKey_cons, lookupKey_cons, ih]
    · simp only [Bool.not_eq_true] at hp
      rw [if_neg (by simp [hp]), lookupKey_cons, if_neg, ih]
      -- p.1 = key ≠ k', so the key test fails
      rw [keyNe_eq_false] at hp
      omega

/-! ## `flattenFam` filter is identity when nothing matches `key` -/

/-- If every entry and every child key survives the filter, filtering the family
    is the identity. -/
theorem flattenFam_filter_id (key : Std.U64) :
    ∀ (es : List (Std.U64 × Std.U64)) (cs : List Node),
      (∀ e ∈ es, keyNe key e = true) →
      (∀ c ∈ cs, ∀ q ∈ flatten c, keyNe key q = true) →
      (flattenFam es cs).filter (keyNe key) = flattenFam es cs := by
  intro es cs
  induction cs generalizing es with
  | nil =>
    intro hes _
    simp only [flattenFam_nil_children]
    exact List.filter_eq_self.mpr hes
  | cons c cs' ih =>
    intro hes hcs
    have hc : ∀ q ∈ flatten c, keyNe key q = true := hcs c (by simp)
    have hc' : ∀ x ∈ cs', ∀ q ∈ flatten x, keyNe key q = true :=
      fun x hx => hcs x (by simp [hx])
    cases es with
    | nil =>
      rw [flattenFam_nil_cons, List.filter_append, List.filter_eq_self.mpr hc,
        ih [] (by simp) hc']
    | cons e es' =>
      have hes' : ∀ f ∈ es', keyNe key f = true := fun f hf => hes f (by simp [hf])
      rw [flattenFam_cons_cons, List.filter_append, List.filter_cons,
        if_pos (hes e (by simp)), List.filter_eq_self.mpr hc, ih es' hes' hc']

/-! ## Leaf case: erasing the unique `key` index = filtering it out -/

/-- On a strictly-sorted list, filtering out the (unique) `key` entry equals
    erasing it by index. -/
theorem filter_keyNe_eq_eraseIdx (l : List (Std.U64 × Std.U64)) (pos : Nat)
    (key : Std.U64) (hs : SortedE l) (hp : pos < l.length)
    (hkey : (l[pos]'hp).1.val = key.val) :
    l.filter (keyNe key) = l.eraseIdx pos := by
  have hpair := List.pairwise_iff_getElem.mp hs
  have hsplit : l = l.take pos ++ (l[pos]'hp) :: l.drop (pos + 1) := by
    conv_lhs => rw [← List.take_append_drop pos l]
    rw [List.drop_eq_getElem_cons hp]
  have htake : (l.take pos).filter (keyNe key) = l.take pos := by
    apply List.filter_eq_self.mpr
    intro x hx
    obtain ⟨j, hj, rfl⟩ := List.mem_iff_getElem.mp hx
    rw [List.length_take] at hj
    have hjlen : j < l.length := by omega
    rw [List.getElem_take, keyNe_eq_true]
    have := hpair j pos hjlen hp (by omega)
    rw [hkey] at this; omega
  have hdrop : (l.drop (pos + 1)).filter (keyNe key) = l.drop (pos + 1) := by
    apply List.filter_eq_self.mpr
    intro x hx
    obtain ⟨j, hj, rfl⟩ := List.mem_iff_getElem.mp hx
    rw [List.length_drop] at hj
    have hjlen : pos + 1 + j < l.length := by omega
    rw [List.getElem_drop, keyNe_eq_true]
    have := hpair pos (pos + 1 + j) hp hjlen (by omega)
    rw [hkey] at this; omega
  have hf : keyNe key (l[pos]'hp) = false := by rw [keyNe_eq_false]; exact hkey
  conv_lhs => rw [hsplit]
  rw [List.filter_append, List.filter_cons, hf]
  simp only [Bool.false_eq_true, if_false]
  rw [htake, hdrop, List.eraseIdx_eq_take_drop_succ]

/-! ## Family-level filter localization -/

/-- Filtering a family where only the *head* child changes (its flatten filters
    to the new child, everything else survives). -/
theorem flattenFam_filter_headChild (key : Std.U64)
    (E : List (Std.U64 × Std.U64)) (oldc newc : Node) (C2' : List Node)
    (hesurv : ∀ e ∈ E, keyNe key e = true)
    (hrest : ∀ c ∈ C2', ∀ q ∈ flatten c, keyNe key q = true)
    (hnew : flatten newc = (flatten oldc).filter (keyNe key)) :
    (flattenFam E (oldc :: C2')).filter (keyNe key) = flattenFam E (newc :: C2') := by
  cases E with
  | nil =>
    rw [flattenFam_nil_cons, flattenFam_nil_cons, List.filter_append, ← hnew,
      flattenFam_filter_id key [] C2' (by simp) hrest]
  | cons e E' =>
    rw [flattenFam_cons_cons, flattenFam_cons_cons, List.filter_append,
      List.filter_cons, if_pos (hesurv e (by simp)), ← hnew,
      flattenFam_filter_id key E' C2' (fun f hf => hesurv f (by simp [hf])) hrest]

/-- Filtering a family where only child `pos` changes. -/
theorem flattenFam_filter_child (key : Std.U64)
    (E : List (Std.U64 × Std.U64)) (CS : List Node) (pos : Nat) (newc : Node)
    (hpos : pos < CS.length) (hlen : CS.length = E.length + 1)
    (hesurv : ∀ e ∈ E, keyNe key e = true)
    (hother : ∀ j (hj : j < CS.length), j ≠ pos →
        ∀ q ∈ flatten (CS[j]'hj), keyNe key q = true)
    (hnew : flatten newc = (flatten (CS[pos]'hpos)).filter (keyNe key)) :
    (flattenFam E CS).filter (keyNe key) = flattenFam E (CS.set pos newc) := by
  have hposE : pos ≤ E.length := by omega
  have hE : E = E.take pos ++ E.drop pos := (List.take_append_drop pos E).symm
  have hCS : CS = CS.take pos ++ (CS[pos]'hpos) :: CS.drop (pos + 1) := by
    conv_lhs => rw [← List.take_append_drop pos CS]
    rw [List.drop_eq_getElem_cons hpos]
  have hset : CS.set pos newc = CS.take pos ++ newc :: CS.drop (pos + 1) :=
    list_set_eq_take_cons_drop CS pos newc hpos
  have htlen : (CS.take pos).length = (E.take pos).length := by
    rw [List.length_take, List.length_take]; omega
  -- prefix children survive
  have hC1 : ∀ c ∈ CS.take pos, ∀ q ∈ flatten c, keyNe key q = true := by
    intro x hx q hq
    obtain ⟨j, hj, rfl⟩ := List.mem_iff_getElem.mp hx
    rw [List.length_take] at hj
    have hjlen : j < CS.length := by omega
    rw [List.getElem_take] at hq
    exact hother j hjlen (by omega) q hq
  -- suffix children survive
  have hC2 : ∀ c ∈ CS.drop (pos + 1), ∀ q ∈ flatten c, keyNe key q = true := by
    intro x hx q hq
    obtain ⟨j, hj, rfl⟩ := List.mem_iff_getElem.mp hx
    rw [List.length_drop] at hj
    have hjlen : pos + 1 + j < CS.length := by omega
    rw [List.getElem_drop] at hq
    exact hother (pos + 1 + j) hjlen (by omega) q hq
  have hpref : (flattenFam (E.take pos) (CS.take pos)).filter (keyNe key)
      = flattenFam (E.take pos) (CS.take pos) :=
    flattenFam_filter_id key _ _
      (fun e he => hesurv e (List.mem_of_mem_take he)) hC1
  have hhead : (flattenFam (E.drop pos) ((CS[pos]'hpos) :: CS.drop (pos + 1))).filter (keyNe key)
      = flattenFam (E.drop pos) (newc :: CS.drop (pos + 1)) :=
    flattenFam_filter_headChild key _ _ _ _
      (fun e he => hesurv e (List.mem_of_mem_drop he)) hC2 hnew
  calc (flattenFam E CS).filter (keyNe key)
      = (flattenFam (E.take pos ++ E.drop pos)
          (CS.take pos ++ (CS[pos]'hpos) :: CS.drop (pos + 1))).filter (keyNe key) := by
        conv_lhs => rw [hE, hCS]
    _ = (flattenFam (E.take pos) (CS.take pos)
          ++ flattenFam (E.drop pos) ((CS[pos]'hpos) :: CS.drop (pos + 1))).filter (keyNe key) := by
        rw [flattenFam_split_eq _ _ _ _ htlen]
    _ = flattenFam (E.take pos) (CS.take pos)
          ++ flattenFam (E.drop pos) (newc :: CS.drop (pos + 1)) := by
        rw [List.filter_append, hpref, hhead]
    _ = flattenFam (E.take pos ++ E.drop pos)
          (CS.take pos ++ newc :: CS.drop (pos + 1)) := by
        rw [flattenFam_split_eq _ _ _ _ htlen]
    _ = flattenFam E (CS.set pos newc) := by rw [← hE, ← hset]

/-- Filtering a family for the *hit* case: separator entry `pos` equals `key`,
    child `pos+1` loses its leftmost (`succ`).  Net effect = drop the key entry
    while `succ` (which stays) is promoted to the separator slot. -/
theorem flattenFam_filter_hit (key : Std.U64)
    (E : List (Std.U64 × Std.U64)) (CS : List Node) (pos : Nat)
    (succ : Std.U64 × Std.U64) (new_right : Node)
    (hpE : pos < E.length) (hpCS : pos + 1 < CS.length)
    (hlen : CS.length = E.length + 1)
    (hkey : (E[pos]'hpE).1.val = key.val)
    (hsucc : keyNe key succ = true)
    (heother : ∀ j (hj : j < E.length), j ≠ pos → keyNe key (E[j]'hj) = true)
    (hcother : ∀ j (hj : j < CS.length), j ≠ pos + 1 →
        ∀ q ∈ flatten (CS[j]'hj), keyNe key q = true)
    (hrl : flatten (CS[pos + 1]'hpCS) = succ :: flatten new_right)
    (hnrsurv : ∀ q ∈ flatten new_right, keyNe key q = true) :
    (flattenFam E CS).filter (keyNe key)
      = flattenFam (E.set pos succ) (CS.set (pos + 1) new_right) := by
  -- decompositions
  have hE : E = E.take pos ++ (E[pos]'hpE) :: E.drop (pos + 1) := by
    conv_lhs => rw [← List.take_append_drop pos E]
    rw [List.drop_eq_getElem_cons hpE]
  have hCS : CS = CS.take (pos + 1) ++ (CS[pos + 1]'hpCS) :: CS.drop (pos + 2) := by
    conv_lhs => rw [← List.take_append_drop (pos + 1) CS]
    rw [List.drop_eq_getElem_cons hpCS]
  have hEset : E.set pos succ = E.take pos ++ succ :: E.drop (pos + 1) :=
    list_set_eq_take_cons_drop E pos succ hpE
  have hCSset : CS.set (pos + 1) new_right
      = CS.take (pos + 1) ++ new_right :: CS.drop (pos + 2) :=
    list_set_eq_take_cons_drop CS (pos + 1) new_right hpCS
  have htlen : (CS.take (pos + 1)).length = (E.take pos).length + 1 := by
    rw [List.length_take, List.length_take]; omega
  -- prefix children (indices 0..pos, all ≠ pos+1) survive
  have hC1 : ∀ c ∈ CS.take (pos + 1), ∀ q ∈ flatten c, keyNe key q = true := by
    intro x hx q hq
    obtain ⟨j, hj, rfl⟩ := List.mem_iff_getElem.mp hx
    rw [List.length_take] at hj
    have hjlen : j < CS.length := by omega
    rw [List.getElem_take] at hq
    exact hcother j hjlen (by omega) q hq
  -- suffix children (indices ≥ pos+2, all ≠ pos+1) survive
  have hC2 : ∀ c ∈ CS.drop (pos + 2), ∀ q ∈ flatten c, keyNe key q = true := by
    intro x hx q hq
    obtain ⟨j, hj, rfl⟩ := List.mem_iff_getElem.mp hx
    rw [List.length_drop] at hj
    have hjlen : pos + 2 + j < CS.length := by omega
    rw [List.getElem_drop] at hq
    exact hcother (pos + 2 + j) hjlen (by omega) q hq
  -- prefix entries (indices 0..pos-1, all ≠ pos) survive
  have hE1surv : ∀ e ∈ E.take pos, keyNe key e = true := by
    intro x hx
    obtain ⟨j, hj, rfl⟩ := List.mem_iff_getElem.mp hx
    rw [List.length_take] at hj
    have hjlen : j < E.length := by omega
    rw [List.getElem_take]
    exact heother j hjlen (by omega)
  -- suffix entries (indices ≥ pos+1, all ≠ pos) survive
  have hE2surv : ∀ e ∈ E.drop (pos + 1), keyNe key e = true := by
    intro x hx
    obtain ⟨j, hj, rfl⟩ := List.mem_iff_getElem.mp hx
    rw [List.length_drop] at hj
    have hjlen : pos + 1 + j < E.length := by omega
    rw [List.getElem_drop]
    exact heother (pos + 1 + j) hjlen (by omega)
  -- the tail region `R` survives filtering (entries E2, children new_right :: C2')
  have hR : (flattenFam (E.drop (pos + 1)) (new_right :: CS.drop (pos + 2))).filter (keyNe key)
      = flattenFam (E.drop (pos + 1)) (new_right :: CS.drop (pos + 2)) :=
    flattenFam_filter_id key _ _ hE2surv (by
      intro c hc
      simp only [List.mem_cons] at hc
      rcases hc with rfl | hc
      · exact hnrsurv
      · exact hC2 c hc)
  have hpref : (flattenFam (E.take pos) (CS.take (pos + 1))).filter (keyNe key)
      = flattenFam (E.take pos) (CS.take (pos + 1)) :=
    flattenFam_filter_id key _ _ hE1surv hC1
  have hkeyf : keyNe key (E[pos]'hpE) = false := by rw [keyNe_eq_false]; exact hkey
  calc (flattenFam E CS).filter (keyNe key)
      = (flattenFam (E.take pos ++ (E[pos]'hpE) :: E.drop (pos + 1))
          (CS.take (pos + 1) ++ (CS[pos + 1]'hpCS) :: CS.drop (pos + 2))).filter (keyNe key) := by
        conv_lhs => rw [hE, hCS]
    _ = (flattenFam (E.take pos) (CS.take (pos + 1))
          ++ (E[pos]'hpE) :: flattenFam (E.drop (pos + 1))
              ((CS[pos + 1]'hpCS) :: CS.drop (pos + 2))).filter (keyNe key) := by
        rw [flattenFam_split _ _ _ _ _ htlen]
    _ = (flattenFam (E.take pos) (CS.take (pos + 1))
          ++ (E[pos]'hpE) :: succ :: flattenFam (E.drop (pos + 1))
              (new_right :: CS.drop (pos + 2))).filter (keyNe key) := by
        rw [flattenFam_head_cons (E.drop (pos + 1)) (CS[pos + 1]'hpCS) new_right
          (CS.drop (pos + 2)) succ hrl]
    _ = flattenFam (E.take pos) (CS.take (pos + 1))
          ++ succ :: flattenFam (E.drop (pos + 1)) (new_right :: CS.drop (pos + 2)) := by
        rw [List.filter_append, hpref, List.filter_cons, hkeyf]
        simp only [Bool.false_eq_true, if_false]
        rw [List.filter_cons, if_pos hsucc, hR]
    _ = flattenFam (E.take pos ++ succ :: E.drop (pos + 1))
          (CS.take (pos + 1) ++ new_right :: CS.drop (pos + 2)) := by
        rw [flattenFam_split _ _ _ _ _ htlen]
    _ = flattenFam (E.set pos succ) (CS.set (pos + 1) new_right) := by
        rw [← hEset, ← hCSset]

/-! ## `delete_from_node` filters `key` out of the flattened list -/

/-- **Exact form** of delete's effect on the flattened list: the result is the
    input with the (unique) `key` entry filtered out.  Conditional on success. -/
theorem delete_from_node_flatten (fuel : Nat) {lo hi : Option Nat} {h : Nat}
    (node : Node) (key : Std.U64) (n' : Node) (uf : Bool)
    (hfuel : Node.size node ≤ fuel)
    (hinv : NodeInv lo hi node) (hh : HeightInv h node)
    (hok : delete_from_node node key = ok (DeleteResult.Removed n' uf)) :
    flatten n' = (flatten node).filter (keyNe key) := by
  induction fuel generalizing lo hi h node n' uf with
  | zero => exfalso; have := Node.one_le_size node; omega
  | succ fuel ih =>
    cases hinv with
    | leaf entries hs hb hlen =>
      cases hh with
      | leaf _ =>
        rw [delete_from_node] at hok
        simp only [Node.entries._simpLemma_, Node.children._simpLemma_] at hok
        obtain ⟨⟨hit, pos⟩, hfp, hok⟩ := bind_ok_inv hok
        obtain ⟨hple, hhit, hmiss⟩ := spec_elim (find_pos_spec entries key hs) hfp
        obtain ⟨i, hi', hok⟩ := bind_ok_inv hok
        have hilen : i.val = (clist Children.Nil).length :=
          spec_elim (children_len_spec Children.Nil (by simp [clist])) hi'
        simp only [clist, List.length_nil] at hilen
        rw [if_pos (by scalar_tac : i = 0#usize)] at hok
        split at hok
        · rename_i hcond
          obtain ⟨hplt, hpkey⟩ := hhit hcond
          have hkey : (entries.val[pos.val]'hplt).1.val = key.val := by rw [hpkey]
          obtain ⟨new_entries, hre, hok⟩ := bind_ok_inv hok
          have hrev : new_entries.val = entries.val.eraseIdx pos.val :=
            spec_elim (remove_entry_at_spec entries pos) hre
          obtain ⟨i2, hmk, hok⟩ := bind_ok_inv hok
          simp only [ok.injEq, DeleteResult.Removed.injEq] at hok
          obtain ⟨hnn, -⟩ := hok
          subst hnn
          rw [flatten_leaf, flatten_leaf, hrev]
          exact (filter_keyNe_eq_eraseIdx entries.val pos.val key hs hplt hkey).symm
        · rename_i hcond; simp at hok
    | internal entries c cs hs hb hlen hal =>
      cases hh with
      | @internal h0 e2 c2 cs2 hch =>
        have hpair := List.pairwise_iff_getElem.mp hs
        rw [delete_from_node] at hok
        simp only [Node.entries._simpLemma_, Node.children._simpLemma_] at hok
        obtain ⟨⟨hit, pos⟩, hfp, hok⟩ := bind_ok_inv hok
        obtain ⟨hple, hhit, hmiss⟩ := spec_elim (find_pos_spec entries key hs) hfp
        dsimp only at hple hhit hmiss
        have hclen : (clist (Children.Cons c cs)).length = entries.val.length + 1 := by
          have := hal.length_eq; simpa [clist] using this
        obtain ⟨i, hi', hok⟩ := bind_ok_inv hok
        have hilen : i.val = (clist (Children.Cons c cs)).length :=
          spec_elim (children_len_spec _ (by rw [hclen]; scalar_tac)) hi'
        rw [hclen] at hilen
        rw [if_neg (by scalar_tac : ¬ (i = 0#usize))] at hok
        split at hok
        · -- internal hit
          rename_i hcond
          obtain ⟨hplt, hkeyeq⟩ := hhit hcond
          have hkey : (entries.val[pos.val]'hplt).1.val = key.val := by rw [hkeyeq]
          obtain ⟨i1, hi1, hok⟩ := bind_ok_inv hok
          have hi1v : i1.val = pos.val + 1 := by
            have := spec_elim (Usize.add_spec (x := pos) (y := 1#usize) (by scalar_tac)) hi1
            scalar_tac
          have hi1lt : i1.val < (clist (Children.Cons c cs)).length := by
            rw [hclen, hi1v]; omega
          obtain ⟨n, hn, hok⟩ := bind_ok_inv hok
          have hn_eq : n = (clist (Children.Cons c cs))[i1.val]'hi1lt :=
            spec_elim (child_at_spec _ i1 hi1lt) hn
          obtain ⟨hcslt, hcinv⟩ := hal.child i1.val (by rw [hi1v]; omega)
          have hcn : NodeInv (lbnd lo entries.val i1.val) (rbnd hi entries.val i1.val) n := by
            rw [hn_eq]
            have heq : (clist (Children.Cons c cs))[i1.val]'hi1lt
                = (c :: clist cs)[i1.val]'hcslt := by simp [clist]
            rw [heq]; exact hcinv
          have hn_h : HeightInv h0 n := by
            rw [hn_eq]; exact hch.allH.getElem i1.val hi1lt
          obtain ⟨⟨succ, new_right, right_underfull⟩, hrl, hok⟩ := bind_ok_inv hok
          obtain ⟨hsucc_InB, hNI_nr, hHI_nr, -⟩ :=
            remove_leftmost_spec (Node.size n) n succ new_right right_underfull
              (le_refl _) hcn hn_h hrl
          rw [hi1v] at hsucc_InB hNI_nr
          have hsucc_gt : key.val < succ.1.val := by
            have hlb := hsucc_InB.1
            rw [lbnd_eq_some lo entries.val (pos.val + 1) (by omega) (by omega)] at hlb
            simp only [Nat.add_sub_cancel] at hlb
            have := hlb (entries.val[pos.val]'hplt).1.val (by simp)
            omega
          -- flatten of the right child peels off succ
          have hrlflat : flatten n = succ :: flatten new_right :=
            remove_leftmost_flatten (Node.size n) n succ new_right right_underfull
              (le_refl _) hcn hn_h hrl
          obtain ⟨entries0, hre0, hok⟩ := bind_ok_inv hok
          have he0v : entries0.val = entries.val.set pos.val succ :=
            spec_elim (replace_entry_spec entries pos succ) hre0
          obtain ⟨children0, hrc0, hok⟩ := bind_ok_inv hok
          have hc0v : clist children0 = (clist (Children.Cons c cs)).set i1.val new_right :=
            spec_elim (replace_child_spec (Children.Cons c cs) i1 new_right) hrc0
          obtain ⟨⟨entries1, children1⟩, hmf, hok⟩ := bind_ok_inv hok
          obtain ⟨i3, hmk, hok⟩ := bind_ok_inv hok
          simp only [ok.injEq, DeleteResult.Removed.injEq] at hok
          obtain ⟨hnn, -⟩ := hok
          subst hnn
          obtain ⟨hbk1, hbk2⟩ := bracket_of_InB_bnd entries.val (pos.val + 1) succ.1.val hs
            (by omega) hsucc_InB
          obtain ⟨hplt2, hlinv⟩ := hal.child pos.val (le_of_lt hplt)
          rw [rbnd_eq_some hi entries.val pos.val hplt] at hlinv
          have hl_relaxed : NodeInv (lbnd lo entries.val pos.val) (some succ.1.val)
              ((c :: clist cs)[pos.val]'hplt2) :=
            NodeInv.relax_hi hlinv (fun u hu => by
              simp only [Option.mem_def, Option.some.injEq] at hu; subst hu
              exact le_of_lt (hbk1 pos.val hplt (by omega)))
          have hsucclt : pos.val + 1 < (c :: clist cs).length := by
            rw [hal.length_eq]; omega
          have hNI0 : NodeInv lo hi (Node.mk entries0 children0) := by
            refine NodeInv.mk_node entries0 children0 ?_ ?_ ?_ (Or.inr ?_)
            · rw [he0v]
              exact SortedE_set_key entries.val pos.val succ hs hplt
                (fun i hi hip => hbk1 i hi (by omega))
                (fun i hi hip => hbk2 i hi (by omega))
            · rw [he0v]
              exact InB_set entries.val pos.val succ lo hi hb
                (InB_outer_of_bnd entries.val (pos.val + 1) succ.1.val (by omega) hb hsucc_InB)
            · rw [he0v]; simpa using hlen
            · rw [he0v, hc0v]; simp only [clist]
              rw [hi1v, set_succ_eq_take_getElem_cons_drop (c :: clist cs) pos.val new_right hsucclt]
              exact Aligned.replace_pair hal pos.val hplt succ
                ((c :: clist cs)[pos.val]'hplt2) new_right hl_relaxed hNI_nr
          have hHI0 : HeightInv (h0 + 1) (Node.mk entries0 children0) := by
            refine HeightInv.mk_internal entries0 children0 ?_ ?_
            · rw [hc0v]; apply List.ne_nil_of_length_pos
              rw [List.length_set, hclen]; omega
            · rw [hc0v]; exact AllH.set _ hch.allH hHI_nr
          have hmf_flat := maybe_fix_flatten entries0 children0 i1 right_underfull hNI0 hHI0
            (fun hu => maybe_fix_ok_pos (hu ▸ hmf))
            (fun _ => by rw [he0v, List.length_set]; omega)
          have hmf_eq : flattenFam entries1.val (clist children1)
              = flattenFam entries0.val (clist children0) := spec_elim hmf_flat hmf
          -- endgame: use flattenFam_filter_hit
          have hpCS : pos.val + 1 < (clist (Children.Cons c cs)).length := by
            rw [← hi1v]; exact hi1lt
          have hrl2 : flatten ((clist (Children.Cons c cs))[pos.val + 1]'hpCS)
              = succ :: flatten new_right := by
            have heqn : (clist (Children.Cons c cs))[pos.val + 1]'hpCS = n := by
              rw [hn_eq]; congr 1; exact hi1v.symm
            rw [heqn]; exact hrlflat
          have hsucc : keyNe key succ = true := by rw [keyNe_eq_true]; omega
          have heother : ∀ j (hj : j < entries.val.length), j ≠ pos.val →
              keyNe key (entries.val[j]'hj) = true := by
            intro j hj hjne
            rw [keyNe_eq_true]
            rcases Nat.lt_or_gt_of_ne hjne with hjl | hjg
            · have := hpair j pos.val hj hplt hjl; rw [hkey] at this; omega
            · have := hpair pos.val j hplt hj hjg; rw [hkey] at this; omega
          have hcother : ∀ j (hj : j < (clist (Children.Cons c cs)).length), j ≠ pos.val + 1 →
              ∀ q ∈ flatten ((clist (Children.Cons c cs))[j]'hj), keyNe key q = true := by
            intro j hj hjne q hq
            rw [keyNe_eq_true]
            have hjle : j ≤ entries.val.length := by rw [hclen] at hj; omega
            obtain ⟨hcltj, hcinvj⟩ := hal.child j hjle
            have hqb := flatten_bounds hcinvj q hq
            rcases Nat.lt_or_ge j (pos.val + 1) with hjl | hjg
            · have hjlt2 : j < entries.val.length := by omega
              rw [rbnd_eq_some hi entries.val j hjlt2] at hqb
              have hq_lt := hqb.2 (entries.val[j]'hjlt2).1.val (by simp)
              have hjle_pos : entries.val[j]'hjlt2 |>.1.val ≤ key.val := by
                rcases Nat.lt_or_eq_of_le (show j ≤ pos.val from by omega) with hh1 | hh1
                · have := hpair j pos.val hjlt2 hplt hh1; rw [hkey] at this; omega
                · subst hh1; rw [hkey]
              omega
            · have hj2 : pos.val + 1 < j := by omega
              have hj1lt : j - 1 < entries.val.length := by omega
              rw [lbnd_eq_some lo entries.val j (by omega) hj1lt] at hqb
              have hq_gt := hqb.1 (entries.val[j-1]'hj1lt).1.val (by simp)
              have hgt_key : key.val < (entries.val[j-1]'hj1lt).1.val := by
                rw [← hkey]; exact hpair pos.val (j-1) hplt hj1lt (by omega)
              omega
          have hnrsurv : ∀ q ∈ flatten new_right, keyNe key q = true := by
            intro q hq
            rw [keyNe_eq_true]
            have := (flatten_bounds hNI_nr q hq).1 succ.1.val (by simp)
            omega
          conv_lhs => rw [flatten_eq_fam, hmf_eq, he0v, hc0v, hi1v]
          conv_rhs => rw [flatten_eq_fam]
          exact (flattenFam_filter_hit key entries.val (clist (Children.Cons c cs)) pos.val
            succ new_right hplt hpCS hclen hkey hsucc heother hcother hrl2 hnrsurv).symm
        · -- internal miss
          rename_i hcond
          have hitf : hit = false := by simpa using hcond
          obtain ⟨hlt', hgt'⟩ := hmiss hitf
          have hposlt : pos.val < (clist (Children.Cons c cs)).length := by
            rw [hclen]; scalar_tac
          obtain ⟨n, hn, hok⟩ := bind_ok_inv hok
          have hn_eq : n = (clist (Children.Cons c cs))[pos.val]'hposlt :=
            spec_elim (child_at_spec _ pos hposlt) hn
          obtain ⟨dr, hdr, hok⟩ := bind_ok_inv hok
          obtain ⟨hcslt, hcinv⟩ := hal.child pos.val hple
          have hcn : NodeInv (lbnd lo entries.val pos.val) (rbnd hi entries.val pos.val) n := by
            rw [hn_eq]
            have heq : (clist (Children.Cons c cs))[pos.val]'hposlt
                = (c :: clist cs)[pos.val]'hcslt := by simp [clist]
            rw [heq]; exact hcinv
          have hn_h : HeightInv h0 n := by rw [hn_eq]; exact hch.allH.getElem pos.val hposlt
          have hsz : Node.size n ≤ fuel := by
            have hmem : n ∈ clist (Children.Cons c cs) := by
              rw [hn_eq]; exact List.getElem_mem hposlt
            have h1 := size_mem_clist _ _ hmem
            simp only [Node.size] at hfuel; omega
          cases dr with
          | NotFound => simp at hok
          | Removed new_child underfull =>
            dsimp only at hok
            have hIH : flatten new_child = (flatten n).filter (keyNe key) :=
              ih n new_child underfull hsz hcn hn_h hdr
            obtain ⟨entries0, hcl0, hok⟩ := bind_ok_inv hok
            have he0 : entries0 = entries := spec_elim (cloneVec_builtin_spec entries) hcl0
            obtain ⟨children0, hrc0, hok⟩ := bind_ok_inv hok
            have hc0v : clist children0 = (clist (Children.Cons c cs)).set pos.val new_child :=
              spec_elim (replace_child_spec (Children.Cons c cs) pos new_child) hrc0
            obtain ⟨⟨entries1, children1⟩, hmf, hok⟩ := bind_ok_inv hok
            obtain ⟨i2, hmk, hok⟩ := bind_ok_inv hok
            simp only [ok.injEq, DeleteResult.Removed.injEq] at hok
            obtain ⟨hnn, -⟩ := hok
            subst hnn
            have hal' : Aligned lo hi entries.val (clist children0) := by
              rw [hc0v]; simp only [clist]
              exact hal.set_child pos.val hple new_child (by
                obtain ⟨hNI_nc, -, -⟩ :=
                  delete_from_node_inv (Node.size n) n key new_child underfull
                    (le_refl _) hcn hn_h hdr
                exact hNI_nc)
            have hNI0 : NodeInv lo hi (Node.mk entries0 children0) := by
              rw [he0]; exact NodeInv.mk_node entries children0 hs hb hlen (Or.inr hal')
            have hHI0 : HeightInv (h0 + 1) (Node.mk entries0 children0) := by
              rw [he0]
              refine HeightInv.mk_internal entries children0 ?_ ?_
              · rw [hc0v]; apply List.ne_nil_of_length_pos
                rw [List.length_set, hclen]; omega
              · rw [hc0v]
                obtain ⟨-, hHI_nc, -⟩ :=
                  delete_from_node_inv (Node.size n) n key new_child underfull
                    (le_refl _) hcn hn_h hdr
                exact AllH.set _ hch.allH hHI_nc
            have hmf_flat := maybe_fix_flatten entries0 children0 pos underfull hNI0 hHI0
              (fun hu => maybe_fix_ok_pos (hu ▸ hmf))
              (fun _ => by rw [he0]; exact hple)
            have hmf_eq : flattenFam entries1.val (clist children1)
                = flattenFam entries0.val (clist children0) := spec_elim hmf_flat hmf
            -- endgame: use flattenFam_filter_child
            have hesurv : ∀ e ∈ entries.val, keyNe key e = true := by
              intro x hx
              obtain ⟨j, hj, rfl⟩ := List.mem_iff_getElem.mp hx
              rw [keyNe_eq_true]
              rcases Nat.lt_or_ge j pos.val with hjl | hjg
              · have := hlt' j hj hjl; omega
              · have := hgt' j hj hjg; omega
            have hother : ∀ j (hj : j < (clist (Children.Cons c cs)).length), j ≠ pos.val →
                ∀ q ∈ flatten ((clist (Children.Cons c cs))[j]'hj), keyNe key q = true := by
              intro j hj hjne q hq
              rw [keyNe_eq_true]
              have hjle : j ≤ entries.val.length := by rw [hclen] at hj; omega
              obtain ⟨hcltj, hcinvj⟩ := hal.child j hjle
              have hqb := flatten_bounds hcinvj q hq
              rcases Nat.lt_or_ge j pos.val with hjl | hjg
              · have hjlt2 : j < entries.val.length := by omega
                rw [rbnd_eq_some hi entries.val j hjlt2] at hqb
                have hq_lt := hqb.2 (entries.val[j]'hjlt2).1.val (by simp)
                have := hlt' j hjlt2 hjl
                omega
              · have hjg' : pos.val < j := by omega
                have hj1lt : j - 1 < entries.val.length := by omega
                rw [lbnd_eq_some lo entries.val j (by omega) hj1lt] at hqb
                have hq_gt := hqb.1 (entries.val[j-1]'hj1lt).1.val (by simp)
                have := hgt' (j-1) hj1lt (by omega)
                omega
            have hnew : flatten new_child
                = (flatten ((clist (Children.Cons c cs))[pos.val]'hposlt)).filter (keyNe key) := by
              rw [hIH, ← hn_eq]
            conv_lhs => rw [flatten_eq_fam, hmf_eq, he0, hc0v]
            conv_rhs => rw [flatten_eq_fam]
            exact (flattenFam_filter_child key entries.val (clist (Children.Cons c cs)) pos.val
              new_child hposlt hclen hesurv hother hnew).symm

/-! ## The frame property -/

/-- **Frame property of `remove`.** Removing `key` leaves the lookup of every
    other key `k'` unchanged. -/
theorem BTree.remove_frame (t t' : BTree) (key k' : Std.U64) (hne : k'.val ≠ key.val)
    (hinv : BTreeInv t) (hbal : BTreeBalanced t)
    (hok : BTree.remove t key = ok (some t')) :
    t'.get k' = t.get k' := by
  obtain ⟨hinv', _⟩ := BTree.remove_inv t key t' hinv hbal hok
  rw [BTree.get, BTree.get,
    get_flatten (Node.size t'.root) t'.root k' (le_refl _) hinv',
    get_flatten (Node.size t.root) t.root k' (le_refl _) hinv]
  suffices hfl : flatten t'.root = (flatten t.root).filter (keyNe key) by
    rw [hfl, lookupKey_filter k' key (flatten t.root) hne]
  obtain ⟨h, hh⟩ := hbal
  rw [BTree.remove] at hok
  obtain ⟨dr, hdr, hok⟩ := bind_ok_inv hok
  cases dr with
  | NotFound => simp at hok
  | Removed new_root ufflag =>
    obtain ⟨nre, nrc⟩ := new_root
    dsimp only at hok
    have hflat : flatten (Node.mk nre nrc) = (flatten t.root).filter (keyNe key) :=
      delete_from_node_flatten (Node.size t.root) t.root key (Node.mk nre nrc) ufflag
        (le_refl _) hinv hh hdr
    obtain ⟨hNI_nr, hHI_nr, -⟩ :=
      delete_from_node_inv (Node.size t.root) t.root key (Node.mk nre nrc) ufflag
        (le_refl _) hinv hh hdr
    obtain ⟨actual_root, hact, hok⟩ := bind_ok_inv hok
    obtain ⟨i1sub, hsub, hok⟩ := bind_ok_inv hok
    simp only [ok.injEq, Option.some.injEq] at hok
    subst hok
    show flatten actual_root = (flatten t.root).filter (keyNe key)
    rw [← hflat]
    simp only [Node.entries._simpLemma_, Node.children._simpLemma_] at hact
    split at hact
    · rename_i hi0
      have hentnil : nre.val = [] := by
        have hlv := alloc.vec.Vec.len_val nre
        have hz : nre.val.length = 0 := by scalar_tac
        exact List.eq_nil_of_length_eq_zero hz
      obtain ⟨i1, hcl, hact⟩ := bind_ok_inv hact
      have hi1len : i1.val = (clist nrc).length := by
        have hle : (clist nrc).length ≤ Std.Usize.max := by
          have := NodeInv.children_len_le hNI_nr
          simp only [Node.children._simpLemma_] at this; scalar_tac
        exact spec_elim (children_len_spec _ hle) hcl
      split at hact
      · rename_i hi1ne
        have hchne : clist nrc ≠ [] :=
          List.ne_nil_of_length_pos (by rw [← hi1len]; scalar_tac)
        have hal := hNI_nr.aligned_of_ne_nil (by
          simp only [Node.children._simpLemma_]; exact hchne)
        simp only [Node.entries._simpLemma_, Node.children._simpLemma_] at hal
        rw [hentnil] at hal
        obtain ⟨c0, hc0eq, hc0inv⟩ := hal.nil_inv
        obtain ⟨nchild, hca, hact⟩ := bind_ok_inv hact
        have hclt : (0#usize).val < (clist nrc).length := by rw [hc0eq]; simp
        have hnc? : (clist nrc)[(0#usize).val]? = some nchild :=
          spec_elim (child_at_spec' _ 0#usize hclt) hca
        have h0v : (0#usize).val = 0 := rfl
        rw [h0v, hc0eq] at hnc?
        simp only [List.getElem?_cons_zero, Option.some.injEq] at hnc?
        have hact' : actual_root = nchild := spec_elim (clone_node_spec nchild) hact
        have hac_c0 : actual_root = c0 := by rw [hact']; exact hnc?.symm
        rw [hac_c0, flatten_eq_fam, hentnil, hc0eq, flattenFam_nil_cons,
          flattenFam_nil_children]
        simp
      · rename_i hi1eq
        simp only [ok.injEq] at hact; subst hact; rfl
    · rename_i hine
      simp only [ok.injEq] at hact; subst hact; rfl

#print axioms BTree.remove_frame

end btree_kernel
