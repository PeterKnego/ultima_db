/- Main theorems: `insert_into_node` and `BTree.insert` preserve the B-tree
   sortedness invariant (`NodeInv`/`BTreeInv` in BtreeInvariant.lean).

   Helper spec stack: AgentFindPos (binary search correctness), AgentEntryHelpers
   (entry-vector specs), AgentListLemmas (pure sortedness/bounds lemmas),
   AgentChildrenHelpers (children cons-list specs), AgentAligned (alignment
   surgery lemmas). Everything is sorry-free; `#print axioms` on the main
   theorems shows only propext, Classical.choice, Quot.sound.
-/
import BtreeKernel
import BtreeInvariant
import ListLemmas
import AlignedLemmas
import ChildrenSpecs
import FindPosSpec
import EntrySpecs

open Aeneas Aeneas.Std Result

namespace btree_kernel

attribute [step] clone_children_spec children_len_spec child_at_spec
  replace_child_spec replace_and_insert_child_spec children_prefix_spec
  children_suffix_spec find_pos_spec entries_suffix_spec entries_prefix_spec
  replace_entry_spec insert_entry_at_spec

/-! ## Small glue lemmas (proved here) -/

/-- `Aligned` only looks at entry keys, so replacing an entry with one of
    equal key preserves it. -/
theorem Aligned.congr_keys {lo hi : Option Nat} {es es' : List (Std.U64 × Std.U64)}
    {cs : List Node} (h : Aligned lo hi es cs)
    (hlen : es'.length = es.length)
    (hkeys : ∀ i, (h1 : i < es.length) → (h2 : i < es'.length) →
      (es'[i]'h2).1.val = (es[i]'h1).1.val) :
    Aligned lo hi es' cs := by
  induction es generalizing lo es' cs with
  | nil =>
    cases h with
    | last c hc =>
      have : es' = [] := List.eq_nil_of_length_eq_zero hlen
      subst this
      exact Aligned.last c hc
  | cons e es ih =>
    cases h with
    | step _ _ c cs hc htail =>
      match es', hlen with
      | e' :: es', hlen =>
        have hkey0 : e'.1.val = e.1.val := hkeys 0 (by simp) (by simp)
        have htail' : ∀ i, (h1 : i < es.length) → (h2 : i < es'.length) →
            (es'[i]'h2).1.val = (es[i]'h1).1.val := by
          intro i h1 h2
          have := hkeys (i + 1) (by simpa using Nat.succ_lt_succ h1)
            (by simpa using Nat.succ_lt_succ h2)
          simpa using this
        have hc' : NodeInv lo (some e'.1.val) c := by rw [hkey0]; exact hc
        have htail2 : Aligned (some e'.1.val) hi es cs := by
          rw [hkey0]; exact htail
        exact Aligned.step e' es' c _ hc'
          (ih htail2 (by simpa using hlen) htail')

/-- Adjacent slot bounds imply full bracketing, by sortedness. -/
theorem bracket_of_InB_bnd {lo hi : Option Nat} (es : List (Std.U64 × Std.U64))
    (pos : Nat) (m : Nat) (hs : SortedE es) (hpos : pos ≤ es.length)
    (h : InB (lbnd lo es pos) (rbnd hi es pos) m) :
    (∀ i, (h1 : i < es.length) → i < pos → (es[i]'h1).1.val < m) ∧
    (∀ i, (h1 : i < es.length) → pos ≤ i → m < (es[i]'h1).1.val) := by
  have hs' := List.pairwise_iff_getElem.mp hs
  obtain ⟨hlo, hhi⟩ := h
  constructor
  · intro i h1 hip
    have hp0 : pos ≠ 0 := by omega
    have hp1 : pos - 1 < es.length := by omega
    have hlast : (es[pos - 1]'hp1).1.val < m := by
      apply hlo
      simp [lbnd, hp0, hp1]
    rcases Nat.lt_or_ge i (pos - 1) with hlt | hge
    · exact lt_trans (hs' i (pos - 1) h1 hp1 hlt) hlast
    · have : i = pos - 1 := by omega
      subst this; exact hlast
  · intro i h1 hpi
    have hfirst : pos < es.length := by omega
    have hm : m < (es[pos]'hfirst).1.val := by
      apply hhi
      simp [rbnd, hfirst]
    rcases Nat.lt_or_ge pos i with hlt | hge
    · exact lt_trans hm (hs' pos i hfirst h1 hlt)
    · have : i = pos := by omega
      subst this; exact hm

/-- A key inside a slot's bounds is inside the outer bounds, given the
    entries themselves are. -/
theorem InB_outer_of_bnd {lo hi : Option Nat} (es : List (Std.U64 × Std.U64))
    (pos : Nat) (m : Nat) (hpos : pos ≤ es.length)
    (hb : ∀ e ∈ es, InB lo hi e.1.val)
    (h : InB (lbnd lo es pos) (rbnd hi es pos) m) : InB lo hi m := by
  obtain ⟨hlo, hhi⟩ := h
  constructor
  · intro l hl
    by_cases hp0 : pos = 0
    · exact hlo l (by simpa [lbnd, hp0] using hl)
    · have hp1 : pos - 1 < es.length := by omega
      have h1 : (es[pos - 1]'hp1).1.val < m := by
        apply hlo; simp [lbnd, hp0, hp1]
      have h2 := (hb _ (es.getElem_mem hp1)).1 l hl
      omega
  · intro u hu
    by_cases hlt : pos < es.length
    · have h1 : m < (es[pos]'hlt).1.val := by
        apply hhi; simp [rbnd, hlt]
      have h2 := (hb _ (es.getElem_mem hlt)).2 u hu
      omega
    · exact hhi u (by simpa [rbnd, hlt] using hu)

theorem exists_cons_of_clist_ne_nil (c : Children) (h : clist c ≠ []) :
    ∃ n cs, c = Children.Cons n cs := by
  cases c with
  | Nil => simp [clist] at h
  | Cons n cs => exact ⟨n, cs, rfl⟩

@[step] theorem cloneVec_builtin_spec (v : alloc.vec.Vec (Std.U64 × Std.U64)) :
    alloc.vec.CloneVec.clone (BuiltinClone (Std.U64 × Std.U64)) v ⦃ w => w = v ⦄ := by
  simp only [alloc.vec.CloneVec.clone]
  have h : ∀ x ∈ v.val, (BuiltinClone (Std.U64 × Std.U64)).clone x = ok x := by
    intro x _; rfl
  obtain ⟨w, heq, hw⟩ := Aeneas.Std.WP.spec_imp_exists (Slice.clone_spec h)
  rw [heq]
  subst hw
  exact (Aeneas.Std.WP.spec_ok _ (p := fun w => w = v)).mpr rfl

unseal MAX_KEYS T in
@[step] theorem MAX_KEYS_spec : MAX_KEYS ⦃ m => m.val = 63 ⦄ := by
  simp only [MAX_KEYS, T]
  step*

/-! ## Post-condition of insert and the split lemma -/

/-- What `insert_into_node` guarantees: a `Fit` keeps the invariant at the
    same bounds; a `Split` yields two invariant halves separated by the
    median, whose key is inside the outer bounds. -/
def InsPost (lo hi : Option Nat) : InsertResult → Prop
  | .Fit n _ => NodeInv lo hi n
  | .Split l m r _ =>
      NodeInv lo (some m.1.val) l ∧ NodeInv (some m.1.val) hi r ∧ InB lo hi m.1.val

theorem maybe_split_spec {lo hi : Option Nat}
    (entries : alloc.vec.Vec (Std.U64 × Std.U64)) (children : Children)
    (replaced : Bool)
    (hs : SortedE entries.val)
    (hb : ∀ e ∈ entries.val, InB lo hi e.1.val)
    (hlen : entries.val.length ≤ 64)
    (hch : children = Children.Nil ∨
      ∃ c cs, children = Children.Cons c cs ∧
        Aligned lo hi entries.val (c :: clist cs)) :
    maybe_split entries children replaced ⦃ r => InsPost lo hi r ⦄ := by
  rw [maybe_split]
  step*
  · -- Fit: entries still fit in one node
    have hlen63 : entries.val.length ≤ 63 := by scalar_tac
    rcases hch with rfl | ⟨c, cs, rfl, hal⟩
    · exact NodeInv.leaf entries hs hb hlen63
    · exact NodeInv.internal entries c cs hs hb hlen63 hal
  · -- side condition: children_len can't overflow
    rcases hch with rfl | ⟨c, cs, rfl, hal⟩
    · simp [clist]
    · have hl := hal.length_eq
      simp [clist] at hl ⊢
      scalar_tac
  · -- Split: 64 entries, split at the median
    have h64 : entries.val.length = 64 := by scalar_tac
    have hmid : mid.val = 32 := by scalar_tac
    have hi3 : i3.val = 33 := by scalar_tac
    have hmidlt : mid.val < entries.val.length := by scalar_tac
    have hmedian : median = entries.val[mid.val]'hmidlt := by
      rw [median_post]
    have hsl : SortedE left_entries.val := by
      rw [left_entries_post]; exact SortedE_take _ _ hs
    have hsr : SortedE right_entries.val := by
      rw [right_entries_post]; exact SortedE_drop _ _ hs
    have hbl : ∀ e ∈ left_entries.val, InB lo (some median.1.val) e.1.val := by
      rw [left_entries_post, hmedian]
      exact InB.of_take entries.val mid.val hmidlt hs hb
    have hbr : ∀ e ∈ right_entries.val, InB (some median.1.val) hi e.1.val := by
      rw [right_entries_post, hmedian]
      have h31 : i3.val = mid.val + 1 := by scalar_tac
      rw [h31]
      exact InB.of_drop entries.val mid.val hmidlt hs hb
    have hll : left_entries.val.length ≤ 63 := by
      rw [left_entries_post]; simp; scalar_tac
    have hrl : right_entries.val.length ≤ 63 := by
      rw [right_entries_post]; simp; scalar_tac
    have hmb : InB lo hi median.1.val := by
      rw [hmedian]; exact hb _ (entries.val.getElem_mem hmidlt)
    split
    · -- children empty: two leaves
      rename_i hi40
      rcases hch with rfl | ⟨c, cs, rfl, hal⟩
      · step*
        exact ⟨NodeInv.leaf left_entries hsl hbl hll,
               NodeInv.leaf right_entries hsr hbr hrl, hmb⟩
      · exfalso
        have hl := hal.length_eq
        simp [clist] at i4_post hl
        scalar_tac
    · -- children aligned: split them alongside
      rename_i hi40
      rcases hch with rfl | ⟨c, cs, rfl, hal⟩
      · exfalso; simp [clist] at i4_post; scalar_tac
      · have hcl : clist (Children.Cons c cs) = c :: clist cs := by simp [clist]
        have hlen65 : (c :: clist cs).length = (entries.val).length + 1 :=
          hal.length_eq
        obtain ⟨lc, hlc, hlcv⟩ :=
          Aeneas.Std.WP.spec_imp_exists (children_prefix_spec (Children.Cons c cs) i3)
        obtain ⟨rc, hrc, hrcv⟩ :=
          Aeneas.Std.WP.spec_imp_exists (children_suffix_spec (Children.Cons c cs) i3)
        rw [hlc, hrc]
        simp only [bind_tc_ok, Aeneas.Std.WP.spec_ok]
        have hmid1 : mid.val + 1 = i3.val := by scalar_tac
        obtain ⟨halL, halR⟩ := hal.split mid.val hmidlt
        -- left children: take i3 of a 65-element list is a nonempty cons
        have hlcl : (clist lc).length = 33 := by
          rw [hlcv, hcl]
          simp only [List.length_take]
          omega
        have hlcne : clist lc ≠ [] := by
          intro h
          rw [h] at hlcl
          simp at hlcl
        obtain ⟨c1, cs1, rfl⟩ := exists_cons_of_clist_ne_nil lc hlcne
        have hc1 : c1 :: clist cs1 = (c :: clist cs).take (mid.val + 1) := by
          have : clist (Children.Cons c1 cs1) = c1 :: clist cs1 := by simp [clist]
          rw [← this, hlcv, hcl, hmid1]
        -- right children
        have hrcl : (clist rc).length = 32 := by
          rw [hrcv, hcl]
          simp only [List.length_drop]
          omega
        have hrcne : clist rc ≠ [] := by
          intro h
          rw [h] at hrcl
          simp at hrcl
        obtain ⟨c2, cs2, rfl⟩ := exists_cons_of_clist_ne_nil rc hrcne
        have hc2 : c2 :: clist cs2 = (c :: clist cs).drop (mid.val + 1) := by
          have : clist (Children.Cons c2 cs2) = c2 :: clist cs2 := by simp [clist]
          rw [← this, hrcv, hcl, hmid1]
        refine ⟨?_, ?_, hmb⟩
        · refine NodeInv.internal left_entries c1 cs1 hsl hbl hll ?_
          rw [hc1, left_entries_post, hmedian]
          exact halL
        · refine NodeInv.internal right_entries c2 cs2 hsr hbr hrl ?_
          rw [hc2, right_entries_post, hmedian, ← hmid1]
          exact halR

theorem insert_into_node_inv (fuel : Nat) {lo hi : Option Nat}
    (node : Node) (k v : Std.U64)
    (hfuel : Node.size node ≤ fuel)
    (hinv : NodeInv lo hi node) (hk : InB lo hi k.val) :
    insert_into_node node k v ⦃ r => InsPost lo hi r ⦄ := by
  induction fuel generalizing lo hi node with
  | zero =>
    exfalso
    cases node with
    | mk e c => simp [Node.size] at hfuel
  | succ fuel ih =>
    cases hinv with
    | leaf entries hs hb hlen =>
      rw [insert_into_node]
      simp only [Node.entries._simpLemma_, Node.children._simpLemma_]
      obtain ⟨⟨hit, pos⟩, hfp, hple, hhit, hmiss⟩ :=
        Aeneas.Std.WP.spec_imp_exists (find_pos_spec entries k hs)
      rw [hfp]
      simp only [bind_tc_ok]
      dsimp only at hple hhit hmiss
      cases hit with
      | true =>
        show (do
            let entries1 ← replace_entry entries pos (k, v)
            let children ← clone_children Children.Nil
            ok (InsertResult.Fit (Node.mk entries1 children) true)) ⦃ r => InsPost lo hi r ⦄
        obtain ⟨hplt, hkey⟩ := hhit rfl
        obtain ⟨out, hre, hrev⟩ :=
          Aeneas.Std.WP.spec_imp_exists (replace_entry_spec entries pos (k, v))
        obtain ⟨d, hcc, hccv⟩ :=
          Aeneas.Std.WP.spec_imp_exists (clone_children_spec Children.Nil)
        rw [hre, hcc]
        simp only [bind_tc_ok, Aeneas.Std.WP.spec_ok]
        subst hccv
        have hkv : (entries.val[pos.val]'hplt).1.val = (k, v).1.val := by
          rw [hkey]
        refine NodeInv.leaf out ?_ ?_ ?_
        · rw [hrev]; exact SortedE_set _ _ _ hs hplt hkv
        · rw [hrev]; exact InB_set _ _ _ _ _ hb hk
        · rw [hrev]; simpa using hlen
      | false =>
        show (do
            let i ← children_len Children.Nil
            if i = 0#usize then do
              let entries1 ← insert_entry_at entries pos (k, v)
              maybe_split entries1 Children.Nil false
            else do
              let n ← child_at Children.Nil pos
              let ir ← insert_into_node n k v
              match ir with
              | InsertResult.Fit new_child replaced => do
                let children ← replace_child Children.Nil pos new_child
                let entries1 ← alloc.vec.CloneVec.clone (BuiltinClone (Std.U64 × Std.U64)) entries
                ok (InsertResult.Fit (Node.mk entries1 children) replaced)
              | InsertResult.Split left median right replaced => do
                let entries1 ← insert_entry_at entries pos median
                let children ← replace_and_insert_child Children.Nil pos left right
                maybe_split entries1 children replaced) ⦃ r => InsPost lo hi r ⦄
        obtain ⟨hlt', hgt'⟩ := hmiss rfl
        obtain ⟨i, hcl, hclv⟩ := Aeneas.Std.WP.spec_imp_exists
          (children_len_spec Children.Nil (by simp [clist]))
        rw [hcl]
        simp only [bind_tc_ok]
        rw [if_pos (by simp [clist] at hclv; scalar_tac)]
        obtain ⟨out, hia, hiav⟩ := Aeneas.Std.WP.spec_imp_exists
          (insert_entry_at_spec entries pos (k, v) hple (by scalar_tac))
        rw [hia]
        simp only [bind_tc_ok]
        apply maybe_split_spec
        · rw [hiav]
          exact SortedE_insertAt _ _ _ hs hple hlt' hgt'
        · rw [hiav]
          exact InB_insertAt _ _ _ _ _ hb hk
        · rw [hiav]
          simp only [List.length_append, List.length_take, List.length_cons,
            List.length_drop]
          omega
        · exact Or.inl rfl
    | internal entries c cs hs hb hlen hal =>
      rw [insert_into_node]
      simp only [Node.entries._simpLemma_, Node.children._simpLemma_]
      obtain ⟨⟨hit, pos⟩, hfp, hple, hhit, hmiss⟩ :=
        Aeneas.Std.WP.spec_imp_exists (find_pos_spec entries k hs)
      rw [hfp]
      simp only [bind_tc_ok]
      dsimp only at hple hhit hmiss
      cases hit with
      | true =>
        show (do
            let entries1 ← replace_entry entries pos (k, v)
            let children ← clone_children (Children.Cons c cs)
            ok (InsertResult.Fit (Node.mk entries1 children) true)) ⦃ r => InsPost lo hi r ⦄
        obtain ⟨hplt, hkey⟩ := hhit rfl
        obtain ⟨out, hre, hrev⟩ :=
          Aeneas.Std.WP.spec_imp_exists (replace_entry_spec entries pos (k, v))
        obtain ⟨d, hcc, hccv⟩ :=
          Aeneas.Std.WP.spec_imp_exists (clone_children_spec (Children.Cons c cs))
        rw [hre, hcc]
        simp only [bind_tc_ok, Aeneas.Std.WP.spec_ok]
        subst hccv
        have hkv : (entries.val[pos.val]'hplt).1.val = (k, v).1.val := by
          rw [hkey]
        refine NodeInv.internal out c cs ?_ ?_ ?_ ?_
        · rw [hrev]; exact SortedE_set _ _ _ hs hplt hkv
        · rw [hrev]; exact InB_set _ _ _ _ _ hb hk
        · rw [hrev]; simpa using hlen
        · refine Aligned.congr_keys hal (by rw [hrev]; simp) ?_
          intro i h1 h2
          revert h2
          rw [hrev]
          intro h2
          rw [List.getElem_set]
          split
          · rename_i hpe
            subst hpe
            exact hkv.symm
          · rfl
      | false =>
        show (do
            let i ← children_len (Children.Cons c cs)
            if i = 0#usize then do
              let entries1 ← insert_entry_at entries pos (k, v)
              maybe_split entries1 Children.Nil false
            else do
              let n ← child_at (Children.Cons c cs) pos
              let ir ← insert_into_node n k v
              match ir with
              | InsertResult.Fit new_child replaced => do
                let children ← replace_child (Children.Cons c cs) pos new_child
                let entries1 ← alloc.vec.CloneVec.clone (BuiltinClone (Std.U64 × Std.U64)) entries
                ok (InsertResult.Fit (Node.mk entries1 children) replaced)
              | InsertResult.Split left median right replaced => do
                let entries1 ← insert_entry_at entries pos median
                let children ← replace_and_insert_child (Children.Cons c cs) pos left right
                maybe_split entries1 children replaced) ⦃ r => InsPost lo hi r ⦄
        obtain ⟨hlt', hgt'⟩ := hmiss rfl
        have hlen1 : (clist (Children.Cons c cs)).length = entries.val.length + 1 := by
          simpa [clist] using hal.length_eq
        obtain ⟨i, hcl, hclv⟩ := Aeneas.Std.WP.spec_imp_exists
          (children_len_spec (Children.Cons c cs) (by rw [hlen1]; scalar_tac))
        rw [hcl]
        simp only [bind_tc_ok]
        rw [if_neg (by rw [hlen1] at hclv; scalar_tac)]
        have hposlt : pos.val < (clist (Children.Cons c cs)).length := by
          rw [hlen1]; omega
        obtain ⟨n, hca, hcav⟩ := Aeneas.Std.WP.spec_imp_exists
          (child_at_spec (Children.Cons c cs) pos hposlt)
        rw [hca]
        simp only [bind_tc_ok]
        -- the child satisfies the invariant at its slot bounds
        obtain ⟨hpc, hcinv⟩ := hal.child pos.val hple
        have hcn : NodeInv (lbnd lo entries.val pos.val)
            (rbnd hi entries.val pos.val) n := by
          rw [hcav]
          have : (clist (Children.Cons c cs))[pos.val]'hposlt =
              (c :: clist cs)[pos.val]'(by simpa [clist] using hposlt) := by
            simp [clist]
          rw [this]
          exact hcinv
        have hkn : InB (lbnd lo entries.val pos.val)
            (rbnd hi entries.val pos.val) k.val :=
          InB_bnd_of_bracket entries.val pos.val k.val hple hk hlt' hgt'
        have hsz : Node.size n ≤ fuel := by
          have hmem : n ∈ clist (Children.Cons c cs) := by
            rw [hcav]; exact List.getElem_mem hposlt
          have h1 := size_mem_clist _ _ hmem
          have h2 : Node.size (Node.mk entries (Children.Cons c cs)) =
              1 + Children.size (Children.Cons c cs) := by
            simp [Node.size]
          omega
        obtain ⟨ir, hir, hirp⟩ :=
          Aeneas.Std.WP.spec_imp_exists (ih n hsz hcn hkn)
        rw [hir]
        simp only [bind_tc_ok]
        cases ir with
        | Fit new_child replaced =>
          show (do
              let children ← replace_child (Children.Cons c cs) pos new_child
              let entries1 ← alloc.vec.CloneVec.clone (BuiltinClone (Std.U64 × Std.U64)) entries
              ok (InsertResult.Fit (Node.mk entries1 children) replaced)) ⦃ r => InsPost lo hi r ⦄
          obtain ⟨ch, hrc, hrcv⟩ := Aeneas.Std.WP.spec_imp_exists
            (replace_child_spec (Children.Cons c cs) pos new_child)
          obtain ⟨ent, hcv, hcvv⟩ := Aeneas.Std.WP.spec_imp_exists
            (cloneVec_builtin_spec entries)
          rw [hrc, hcv]
          simp only [bind_tc_ok, Aeneas.Std.WP.spec_ok]
          rw [hcvv]
          have hchl : (clist ch).length = (clist (Children.Cons c cs)).length := by
            rw [hrcv]; simp
          have hchne : clist ch ≠ [] := by
            intro h
            rw [h] at hchl
            rw [hlen1] at hchl
            simp at hchl
          obtain ⟨c1, cs1, rfl⟩ := exists_cons_of_clist_ne_nil ch hchne
          refine NodeInv.internal entries c1 cs1 hs hb hlen ?_
          have hcs1 : c1 :: clist cs1 = (c :: clist cs).set pos.val new_child := by
            have h1 : clist (Children.Cons c1 cs1) = c1 :: clist cs1 := by simp [clist]
            have h2 : clist (Children.Cons c cs) = c :: clist cs := by simp [clist]
            rw [← h1, hrcv, h2]
          rw [hcs1]
          exact hal.set_child pos.val hple new_child hirp
        | Split l median r replaced =>
          show (do
              let entries1 ← insert_entry_at entries pos median
              let children ← replace_and_insert_child (Children.Cons c cs) pos l r
              maybe_split entries1 children replaced) ⦃ r => InsPost lo hi r ⦄
          obtain ⟨hnl, hnr, hnm⟩ := hirp
          obtain ⟨ent, hia, hiav⟩ := Aeneas.Std.WP.spec_imp_exists
            (insert_entry_at_spec entries pos median hple (by scalar_tac))
          obtain ⟨ch, hri, hriv⟩ := Aeneas.Std.WP.spec_imp_exists
            (replace_and_insert_child_spec (Children.Cons c cs) pos l r hposlt)
          rw [hia, hri]
          simp only [bind_tc_ok]
          obtain ⟨hmlt, hmgt⟩ := bracket_of_InB_bnd entries.val pos.val median.1.val
            hs hple hnm
          apply maybe_split_spec
          · rw [hiav]
            exact SortedE_insertAt _ _ _ hs hple hmlt hmgt
          · rw [hiav]
            exact InB_insertAt _ _ _ _ _ hb
              (InB_outer_of_bnd entries.val pos.val median.1.val hple hb hnm)
          · rw [hiav]
            simp only [List.length_append, List.length_take, List.length_cons,
              List.length_drop]
            omega
          · right
            have hchl : (clist ch).length =
                pos.val + 2 + ((clist (Children.Cons c cs)).length - (pos.val + 1)) := by
              rw [hriv]
              simp only [List.length_append, List.length_take, List.length_cons,
                List.length_drop]
              omega
            have hchne : clist ch ≠ [] := by
              intro h
              rw [h] at hchl
              simp at hchl
            obtain ⟨c1, cs1, rfl⟩ := exists_cons_of_clist_ne_nil ch hchne
            refine ⟨c1, cs1, rfl, ?_⟩
            have hcs1 : c1 :: clist cs1 =
                (c :: clist cs).take pos.val ++ l :: r :: (c :: clist cs).drop (pos.val + 1) := by
              have h1 : clist (Children.Cons c1 cs1) = c1 :: clist cs1 := by simp [clist]
              have h2 : clist (Children.Cons c cs) = c :: clist cs := by simp [clist]
              rw [← h1, hriv, h2]
            rw [hcs1, hiav]
            exact hal.insert_split pos.val hple median l r hnm hnl hnr

/-- Top-level: `BTree.insert` preserves the tree invariant (given room for
    the length counter). -/
theorem BTree.insert_inv (t : BTree) (k v : Std.U64)
    (hinv : BTreeInv t) (hcap : t.len.val < Std.Usize.max) :
    BTree.insert t k v ⦃ t' => BTreeInv t' ⦄ := by
  rw [BTree.insert]
  have hk : InB none none k.val := ⟨by simp, by simp⟩
  obtain ⟨ir, hir, hirp⟩ := Aeneas.Std.WP.spec_imp_exists
    (insert_into_node_inv (Node.size t.root) t.root k v (Nat.le_refl _) hinv hk)
  rw [hir]
  simp only [bind_tc_ok]
  cases ir with
  | Fit new_root replaced =>
    show (if replaced then ok { t with root := new_root }
          else do
            let new_len ← t.len + 1#usize
            ok ({ root := new_root, len := new_len } : BTree)) ⦃ t' => BTreeInv t' ⦄
    split
    · exact (Aeneas.Std.WP.spec_ok _).mpr hirp
    · obtain ⟨nl, hadd, -⟩ := Aeneas.Std.WP.spec_imp_exists
        (Usize.add_spec (x := t.len) (y := 1#usize) (by scalar_tac))
      rw [hadd]
      simp only [bind_tc_ok]
      exact (Aeneas.Std.WP.spec_ok _).mpr hirp
  | Split l median r replaced =>
    show (do
        let new_len ← if replaced then ok t.len else t.len + 1#usize
        let entries ← alloc.vec.Vec.push (alloc.vec.Vec.new (Std.U64 × Std.U64)) median
        ok ({ root := Node.mk entries
                (Children.Cons l (Children.Cons r Children.Nil)),
              len := new_len } : BTree)) ⦃ t' => BTreeInv t' ⦄
    obtain ⟨hnl, hnr, hnm⟩ := hirp
    have hlenok : (if replaced then ok t.len else t.len + 1#usize) ⦃ _ => True ⦄ := by
      split
      · exact (Aeneas.Std.WP.spec_ok _).mpr trivial
      · apply Aeneas.Std.WP.spec_mono (Usize.add_spec (by scalar_tac))
        intro x _
        trivial
    obtain ⟨nl, hnl2, -⟩ := Aeneas.Std.WP.spec_imp_exists hlenok
    rw [hnl2]
    simp only [bind_tc_ok]
    obtain ⟨ents, hpu, hpuv⟩ := Aeneas.Std.WP.spec_imp_exists
      (alloc.vec.Vec.push_spec (alloc.vec.Vec.new (Std.U64 × Std.U64)) median
        (by scalar_tac))
    rw [hpu]
    simp only [bind_tc_ok, Aeneas.Std.WP.spec_ok]
    have hev : ents.val = [median] := by simpa using hpuv
    show NodeInv none none (Node.mk ents
      (Children.Cons l (Children.Cons r Children.Nil)))
    refine NodeInv.internal ents l (Children.Cons r Children.Nil) ?_ ?_ ?_ ?_
    · rw [hev]
      simp [SortedE]
    · intro e _
      exact ⟨by simp, by simp⟩
    · rw [hev]; simp
    · have hcr : clist (Children.Cons r Children.Nil) = [r] := by simp [clist]
      rw [hcr, hev]
      exact Aligned.step median [] l [r] hnl (Aligned.last r hnr)

end btree_kernel
