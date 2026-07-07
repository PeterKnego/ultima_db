/- Get-after-insert correctness: after `insert k v`, `get k` returns `some v`.

   `GetPost k v r` characterizes where the binding `k ↦ v` lives in an
   insert result: in a `Fit` node directly; after a `Split`, either the
   promoted median IS `(k, v)`, or it is found in the half selected by
   comparing `k` with the median key — which is exactly how `get` navigates.
-/
import BtreeKernel
import BtreeInvariant
import BtreeInsertInv

open Aeneas Aeneas.Std Result

set_option maxRecDepth 4096

namespace btree_kernel

/-- Where the inserted binding lives in an insert result. -/
def GetPost (k v : Std.U64) : InsertResult → Prop
  | .Fit n _ => get_in_node n k = ok (some v)
  | .Split l m r _ =>
      (m.1.val = k.val → m.1 = k ∧ m.2 = v) ∧
      (k.val < m.1.val → get_in_node l k = ok (some v)) ∧
      (m.1.val < k.val → get_in_node r k = ok (some v))

/-- Combine two specs on the same computation. -/
theorem spec_and {α : Type} {x : Result α} {P Q : α → Prop}
    (hp : x ⦃ r => P r ⦄) (hq : x ⦃ r => Q r ⦄) : x ⦃ r => P r ∧ Q r ⦄ := by
  obtain ⟨a, ha, hpa⟩ := Aeneas.Std.WP.spec_imp_exists hp
  obtain ⟨b, hb, hqb⟩ := Aeneas.Std.WP.spec_imp_exists hq
  rw [ha] at hb
  cases hb
  rw [ha]
  exact (Aeneas.Std.WP.spec_ok _).mpr ⟨hpa, hqb⟩

/-! ## Navigation lemmas -/

theorem sorted_key_unique (l : List (Std.U64 × Std.U64)) (hs : SortedE l)
    {i j : Nat} (hi : i < l.length) (hj : j < l.length)
    (hk : (l[i]'hi).1.val = (l[j]'hj).1.val) : i = j := by
  have hs' := List.pairwise_iff_getElem.mp hs
  rcases Nat.lt_trichotomy i j with h | h | h
  · have := hs' i j hi hj h; omega
  · exact h
  · have := hs' j i hj hi h; omega

theorem bracket_unique (l : List (Std.U64 × Std.U64)) (k : Nat) {p1 p2 : Nat}
    (hp1 : p1 ≤ l.length) (hp2 : p2 ≤ l.length)
    (hlt1 : ∀ i, (h : i < l.length) → i < p1 → (l[i]'h).1.val < k)
    (hgt1 : ∀ i, (h : i < l.length) → p1 ≤ i → k < (l[i]'h).1.val)
    (hlt2 : ∀ i, (h : i < l.length) → i < p2 → (l[i]'h).1.val < k)
    (hgt2 : ∀ i, (h : i < l.length) → p2 ≤ i → k < (l[i]'h).1.val) : p1 = p2 := by
  rcases Nat.lt_trichotomy p1 p2 with h | h | h
  · have h1 := hgt1 p1 (by omega) (Nat.le_refl _)
    have h2 := hlt2 p1 (by omega) h
    omega
  · exact h
  · have h1 := hgt2 p2 (by omega) (Nat.le_refl _)
    have h2 := hlt1 p2 (by omega) h
    omega

theorem getElem_insertAt_self (l : List (Std.U64 × Std.U64)) (pos : Nat)
    (x : Std.U64 × Std.U64) (hpos : pos ≤ l.length) :
    (l.take pos ++ x :: l.drop pos)[pos]'(by simp; omega) = x := by
  rw [List.getElem_append_right (by simp)]
  simp [List.length_take, Nat.min_eq_left hpos]

theorem get_found (entries : alloc.vec.Vec (Std.U64 × Std.U64)) (ch : Children)
    (k : Std.U64) (hs : SortedE entries.val)
    (j : Nat) (hj : j < entries.val.length) (hkey : (entries.val[j]'hj).1 = k) :
    get_in_node (Node.mk entries ch) k = ok (some (entries.val[j]'hj).2) := by
  rw [get_in_node]
  simp only [Node.entries._simpLemma_, Node.children._simpLemma_]
  obtain ⟨⟨hit, pos⟩, hfp, hple, hhit, hmiss⟩ :=
    Aeneas.Std.WP.spec_imp_exists (find_pos_spec entries k hs)
  rw [hfp]
  simp only [bind_tc_ok]
  dsimp only at hple hhit hmiss
  cases hit with
  | false =>
    exfalso
    obtain ⟨hlt, hgt⟩ := hmiss rfl
    rcases Nat.lt_or_ge j pos.val with h | h
    · have := hlt j hj h
      rw [hkey] at this
      omega
    · have := hgt j hj h
      rw [hkey] at this
      omega
  | true =>
    obtain ⟨hplt, hpkey⟩ := hhit rfl
    have hpj : pos.val = j := by
      apply sorted_key_unique entries.val hs hplt hj
      rw [hpkey, hkey]
    show (do
        let (_, i) ←
          alloc.vec.Vec.index (core.slice.index.SliceIndexUsizeSlice
            (Std.U64 × Std.U64)) entries pos
        ok (some i) : Result (Option Std.U64)) = ok (some (entries.val[j]'hj).2)
    simp only [alloc.vec.Vec.index_slice_index]
    obtain ⟨e, hidx, hev⟩ := Aeneas.Std.WP.spec_imp_exists
      (alloc.vec.Vec.index_usize_spec entries pos (by scalar_tac))
    rw [hidx]
    simp only [bind_tc_ok]
    obtain ⟨e1, e2⟩ := e
    have he2 : e2 = (entries.val[j]'hj).2 := by
      have := congrArg Prod.snd hev
      simpa [hpj] using this
    simp [he2]

theorem get_descend (entries : alloc.vec.Vec (Std.U64 × Std.U64)) (c : Node)
    (cs : Children) (k : Std.U64) (hs : SortedE entries.val)
    (pos : Nat) (hpos : pos ≤ entries.val.length)
    (hlt : ∀ i, (h : i < entries.val.length) → i < pos →
      (entries.val[i]'h).1.val < k.val)
    (hgt : ∀ i, (h : i < entries.val.length) → pos ≤ i →
      k.val < (entries.val[i]'h).1.val)
    (hcl : (clist (Children.Cons c cs)).length = entries.val.length + 1)
    (hsmall : entries.val.length < Std.Usize.max) :
    get_in_node (Node.mk entries (Children.Cons c cs)) k =
      get_in_node ((c :: clist cs)[pos]'(by
        have := hcl; simp [clist] at this ⊢; omega)) k := by
  rw [get_in_node]
  simp only [Node.entries._simpLemma_, Node.children._simpLemma_]
  obtain ⟨⟨hit, pos'⟩, hfp, hple, hhit, hmiss⟩ :=
    Aeneas.Std.WP.spec_imp_exists (find_pos_spec entries k hs)
  rw [hfp]
  simp only [bind_tc_ok]
  dsimp only at hple hhit hmiss
  cases hit with
  | true =>
    exfalso
    obtain ⟨hplt, hpkey⟩ := hhit rfl
    have h1 := hgt pos'.val hplt
    have h2 := hlt pos'.val hplt
    rw [hpkey] at h1 h2
    rcases Nat.lt_or_ge pos'.val pos with h | h
    · have := h2 h; omega
    · have := h1 h; omega
  | false =>
    obtain ⟨hlt', hgt'⟩ := hmiss rfl
    have hpp : pos'.val = pos :=
      bracket_unique entries.val k.val hple hpos hlt' hgt' hlt hgt
    show (do
        let i ← children_len (Children.Cons c cs)
        if i = 0#usize
        then ok none
        else do
          let n ← child_at (Children.Cons c cs) pos'
          get_in_node n k : Result (Option Std.U64)) =
      get_in_node ((c :: clist cs)[pos]'(by
        have := hcl; simp [clist] at this ⊢; omega)) k
    obtain ⟨len, hcl2, hclv⟩ := Aeneas.Std.WP.spec_imp_exists
      (children_len_spec (Children.Cons c cs) (by rw [hcl]; scalar_tac))
    rw [hcl2]
    simp only [bind_tc_ok]
    rw [if_neg (by rw [hcl] at hclv; scalar_tac)]
    have hposlt : pos'.val < (clist (Children.Cons c cs)).length := by
      rw [hcl]; omega
    obtain ⟨n, hca, hcav⟩ := Aeneas.Std.WP.spec_imp_exists
      (child_at_spec (Children.Cons c cs) pos' hposlt)
    rw [hca]
    simp only [bind_tc_ok]
    rw [hcav]
    congr 1
    simp [clist, hpp]

/-! ## Split-level lemmas -/

/-- If `(k, v)` sits at index `j` of the entries, `maybe_split` produces a
    result in which `get` finds `v` at `k`. -/
theorem maybe_split_get {k v : Std.U64}
    (entries : alloc.vec.Vec (Std.U64 × Std.U64)) (children : Children)
    (replaced : Bool) (hs : SortedE entries.val)
    (j : Nat) (hj : j < entries.val.length)
    (hkv : entries.val[j]'hj = (k, v))
    (hlen : entries.val.length ≤ 64)
    (hch : (clist children).length ≤ Std.Usize.max) :
    maybe_split entries children replaced ⦃ r => GetPost k v r ⦄ := by
  have hkey : (entries.val[j]'hj).1 = k := by rw [hkv]
  have hval : (entries.val[j]'hj).2 = v := by rw [hkv]
  rw [maybe_split]
  step*
  · -- Fit
    show get_in_node (Node.mk entries children) k = ok (some v)
    rw [get_found entries children k hs j hj hkey, hval]
  · -- Split at the median
    have h64 : entries.val.length = 64 := by scalar_tac
    have hmid : mid.val = 32 := by scalar_tac
    have hmidlt : mid.val < entries.val.length := by scalar_tac
    have hmedian : median = entries.val[mid.val]'hmidlt := by rw [median_post]
    split
    all_goals {
      rename_i hi4
      step*
      refine ⟨?_, ?_, ?_⟩
      · -- median key = k → median is exactly (k, v)
        intro hmk
        have : mid.val = j := by
          apply sorted_key_unique entries.val hs hmidlt hj
          rw [hmedian] at hmk
          rw [← hkey] at hmk
          exact hmk
        simp [hmedian, this, hkv]
      · -- k < median key → k sits in the left half at index j
        intro hklt
        have hmedkey : median.1.val = (entries.val[mid.val]'hmidlt).1.val := by
          rw [hmedian]
        have hjkey : (entries.val[j]'hj).1.val = k.val := by rw [hkey]
        have hs' := List.pairwise_iff_getElem.mp hs
        have hjlt : j < mid.val := by
          rcases Nat.lt_trichotomy j mid.val with h | h | h
          · exact h
          · exfalso
            have hjm : (entries.val[j]'hj).1.val =
                (entries.val[mid.val]'hmidlt).1.val := by simp only [h]
            omega
          · exfalso
            have := hs' mid.val j hmidlt hj h
            omega
        have hjl : j < left_entries.val.length := by
          rw [left_entries_post]; simp only [List.length_take]; omega
        have hsl : SortedE left_entries.val := by
          rw [left_entries_post]; exact SortedE_take _ _ hs
        have hlj : (left_entries.val[j]'hjl) = (k, v) := by
          revert hjl
          rw [left_entries_post]
          intro hjl
          rw [List.getElem_take]
          exact hkv
        rw [get_found left_entries _ k hsl j hjl (by rw [hlj]),
          show (left_entries.val[j]'hjl).2 = v by rw [hlj]]
      · -- median key < k → k sits in the right half at index j - 33
        intro hkgt
        have hmedkey : median.1.val = (entries.val[mid.val]'hmidlt).1.val := by
          rw [hmedian]
        have hjkey : (entries.val[j]'hj).1.val = k.val := by rw [hkey]
        have hs' := List.pairwise_iff_getElem.mp hs
        have hjgt : mid.val < j := by
          rcases Nat.lt_trichotomy mid.val j with h | h | h
          · exact h
          · exfalso
            have hjm : (entries.val[j]'hj).1.val =
                (entries.val[mid.val]'hmidlt).1.val := by simp only [h]
            omega
          · exfalso
            have := hs' j mid.val hj hmidlt h
            omega
        have hrpost : right_entries.val = entries.val.drop (mid.val + 1) := by
          rw [right_entries_post]
          congr 1
        have hjd : j - (mid.val + 1) < right_entries.val.length := by
          rw [hrpost]; simp only [List.length_drop]; omega
        have hsr : SortedE right_entries.val := by
          rw [hrpost]; exact SortedE_drop _ _ hs
        have hidx : mid.val + 1 + (j - (mid.val + 1)) = j := by omega
        have hrj : (right_entries.val[j - (mid.val + 1)]'hjd) = (k, v) := by
          revert hjd
          rw [hrpost]
          intro hjd
          rw [List.getElem_drop]
          simp only [hidx]
          exact hkv
        rw [get_found right_entries _ k hsr _ hjd (by rw [hrj]),
          show (right_entries.val[j - (mid.val + 1)]'hjd).2 = v by rw [hrj]]
    }

/-- `get_descend` stated at `clist` level, for any nonempty children value. -/
theorem get_descend' (entries : alloc.vec.Vec (Std.U64 × Std.U64)) (ch : Children)
    (k : Std.U64) (hs : SortedE entries.val)
    (q : Nat) (hq : q ≤ entries.val.length)
    (hlt : ∀ i, (h : i < entries.val.length) → i < q →
      (entries.val[i]'h).1.val < k.val)
    (hgt : ∀ i, (h : i < entries.val.length) → q ≤ i →
      k.val < (entries.val[i]'h).1.val)
    (hcl : (clist ch).length = entries.val.length + 1)
    (hsmall : entries.val.length < Std.Usize.max) :
    get_in_node (Node.mk entries ch) k =
      get_in_node ((clist ch)[q]'(by rw [hcl]; omega)) k := by
  have hne : clist ch ≠ [] := by
    intro h
    rw [h] at hcl
    simp at hcl
  obtain ⟨c1, cs1, rfl⟩ := exists_cons_of_clist_ne_nil ch hne
  have hcl' : (clist (Children.Cons c1 cs1)).length = entries.val.length + 1 := hcl
  rw [get_descend entries c1 cs1 k hs q hq hlt hgt hcl' hsmall]
  simp [clist]

/-- If `k` is bracketed at slot `q` and the child there resolves `k ↦ v`,
    `maybe_split` produces a result in which `get` finds `v` at `k`. -/
theorem maybe_split_get_descend {k v : Std.U64}
    (entries : alloc.vec.Vec (Std.U64 × Std.U64)) (children : Children)
    (replaced : Bool) (hs : SortedE entries.val)
    (q : Nat) (hq : q ≤ entries.val.length)
    (hlt : ∀ i, (h : i < entries.val.length) → i < q →
      (entries.val[i]'h).1.val < k.val)
    (hgt : ∀ i, (h : i < entries.val.length) → q ≤ i →
      k.val < (entries.val[i]'h).1.val)
    (hcl : (clist children).length = entries.val.length + 1)
    (hlen : entries.val.length ≤ 64)
    (hsmall : entries.val.length < Std.Usize.max)
    (hgv : get_in_node ((clist children)[q]'(by rw [hcl]; omega)) k = ok (some v)) :
    maybe_split entries children replaced ⦃ r => GetPost k v r ⦄ := by
  rw [maybe_split]
  step*
  · -- Fit: descend through the unchanged node
    show get_in_node (Node.mk entries children) k = ok (some v)
    rw [get_descend' entries children k hs q hq hlt hgt hcl hsmall]
    exact hgv
  · -- Split at the median
    have h64 : entries.val.length = 64 := by scalar_tac
    have hmid : mid.val = 32 := by scalar_tac
    have hmidlt : mid.val < entries.val.length := by scalar_tac
    have hmedian : median = entries.val[mid.val]'hmidlt := by rw [median_post]
    have hi3 : i3.val = mid.val + 1 := by scalar_tac
    have hlpost : left_entries.val = entries.val.take mid.val := left_entries_post
    have hrpost : right_entries.val = entries.val.drop (mid.val + 1) := by
      rw [right_entries_post]
      congr 1
    split
    · -- children_len = 0: contradicts hcl
      exfalso
      rename_i hi40
      rw [hcl] at i4_post
      scalar_tac
    · rename_i hi40
      obtain ⟨lc, hlc, hlcv⟩ :=
        Aeneas.Std.WP.spec_imp_exists (children_prefix_spec children i3)
      obtain ⟨rc, hrc, hrcv⟩ :=
        Aeneas.Std.WP.spec_imp_exists (children_suffix_spec children i3)
      rw [hlc, hrc]
      simp only [bind_tc_ok, Aeneas.Std.WP.spec_ok]
      have hlcl : (clist lc).length = mid.val + 1 := by
        rw [hlcv, hi3]
        simp only [List.length_take]
        rw [hcl]
        omega
      have hrcl : (clist rc).length = entries.val.length - mid.val := by
        rw [hrcv, hi3]
        simp only [List.length_drop]
        rw [hcl]
        omega
      refine ⟨?_, ?_, ?_⟩
      · -- median key = k is impossible: k is bracketed strictly
        intro hmk
        exfalso
        rcases Nat.lt_or_ge mid.val q with h | h
        · have := hlt mid.val hmidlt h
          rw [hmedian] at hmk
          omega
        · have := hgt mid.val hmidlt h
          rw [hmedian] at hmk
          omega
      · -- k < median key → q ≤ mid; descend into the left half
        intro hklt
        have hqle : q ≤ mid.val := by
          rcases Nat.lt_or_ge mid.val q with h | h
          · exfalso
            have := hlt mid.val hmidlt h
            rw [hmedian] at hklt
            omega
          · exact h
        have hlt' : ∀ i, (h : i < left_entries.val.length) → i < q →
            (left_entries.val[i]'h).1.val < k.val := by
          intro i h hiq
          have hi : i < entries.val.length := by
            rw [hlpost] at h; simp at h; omega
          have : (left_entries.val[i]'h) = (entries.val[i]'hi) := by
            revert h; rw [hlpost]; intro h; rw [List.getElem_take]
          rw [this]
          exact hlt i hi hiq
        have hgt' : ∀ i, (h : i < left_entries.val.length) → q ≤ i →
            k.val < (left_entries.val[i]'h).1.val := by
          intro i h hqi
          have hi : i < entries.val.length := by
            rw [hlpost] at h; simp at h; omega
          have : (left_entries.val[i]'h) = (entries.val[i]'hi) := by
            revert h; rw [hlpost]; intro h; rw [List.getElem_take]
          rw [this]
          exact hgt i hi hqi
        have hqle' : q ≤ left_entries.val.length := by
          rw [hlpost]; simp; omega
        have hlcl' : (clist lc).length = left_entries.val.length + 1 := by
          rw [hlcl, hlpost]; simp; omega
        have hsmall' : left_entries.val.length < Std.Usize.max := by
          rw [hlpost]; simp; scalar_tac
        rw [get_descend' left_entries lc k
          (by rw [hlpost]; exact SortedE_take _ _ hs) q hqle' hlt' hgt' hlcl' hsmall']
        simp only [hlcv]
        rw [List.getElem_take]
        exact hgv
      · -- median key < k → q ≥ mid + 1; descend into the right half
        intro hkgt
        have hqge : mid.val + 1 ≤ q := by
          rcases Nat.lt_or_ge q (mid.val + 1) with h | h
          · exfalso
            have := hgt mid.val hmidlt (by omega)
            rw [hmedian] at hkgt
            omega
          · exact h
        have hlt' : ∀ i, (h : i < right_entries.val.length) → i < q - (mid.val + 1) →
            (right_entries.val[i]'h).1.val < k.val := by
          intro i h hiq
          have hi : mid.val + 1 + i < entries.val.length := by
            rw [hrpost] at h; simp at h; omega
          have : (right_entries.val[i]'h) = (entries.val[mid.val + 1 + i]'hi) := by
            revert h; rw [hrpost]; intro h; rw [List.getElem_drop]
          rw [this]
          exact hlt _ hi (by omega)
        have hgt' : ∀ i, (h : i < right_entries.val.length) → q - (mid.val + 1) ≤ i →
            k.val < (right_entries.val[i]'h).1.val := by
          intro i h hqi
          have hi : mid.val + 1 + i < entries.val.length := by
            rw [hrpost] at h; simp at h; omega
          have : (right_entries.val[i]'h) = (entries.val[mid.val + 1 + i]'hi) := by
            revert h; rw [hrpost]; intro h; rw [List.getElem_drop]
          rw [this]
          exact hgt _ hi (by omega)
        have hqle' : q - (mid.val + 1) ≤ right_entries.val.length := by
          rw [hrpost]; simp; omega
        have hrcl' : (clist rc).length = right_entries.val.length + 1 := by
          rw [hrcl, hrpost]; simp; omega
        have hsmall' : right_entries.val.length < Std.Usize.max := by
          rw [hrpost]; simp; scalar_tac
        rw [get_descend' right_entries rc k
          (by rw [hrpost]; exact SortedE_drop _ _ hs) (q - (mid.val + 1))
          hqle' hlt' hgt' hrcl' hsmall']
        simp only [hrcv]
        rw [List.getElem_drop]
        have hqq : i3.val + (q - (mid.val + 1)) = q := by omega
        simp only [hqq]
        exact hgv

theorem getElem_of_getElem? {α} (l : List α) (i : Nat) (h : i < l.length)
    (x : α) (hx : l[i]? = some x) : l[i]'h = x := by
  rw [List.getElem?_eq_getElem h] at hx
  exact Option.some.inj hx

/-! ## Main theorem -/

theorem insert_get_same (fuel : Nat) {lo hi : Option Nat}
    (node : Node) (k v : Std.U64)
    (hfuel : Node.size node ≤ fuel)
    (hinv : NodeInv lo hi node) (hk : InB lo hi k.val) :
    insert_into_node node k v ⦃ r => GetPost k v r ⦄ := by
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
            ok (InsertResult.Fit (Node.mk entries1 children) true)) ⦃ r => GetPost k v r ⦄
        obtain ⟨hplt, hkey⟩ := hhit rfl
        obtain ⟨out, hre, hrev⟩ :=
          Aeneas.Std.WP.spec_imp_exists (replace_entry_spec entries pos (k, v))
        obtain ⟨d, hcc, hccv⟩ :=
          Aeneas.Std.WP.spec_imp_exists (clone_children_spec Children.Nil)
        rw [hre, hcc]
        simp only [bind_tc_ok, Aeneas.Std.WP.spec_ok]
        show get_in_node (Node.mk out d) k = ok (some v)
        have hkv : (entries.val[pos.val]'hplt).1.val = (k, v).1.val := by rw [hkey]
        have hplt' : pos.val < out.val.length := by rw [hrev]; simpa using hplt
        have hokv : (out.val[pos.val]'hplt') = (k, v) := by
          revert hplt'
          rw [hrev]
          intro hplt'
          exact List.getElem_set_self ..
        rw [get_found out d k
          (by rw [hrev]; exact SortedE_set _ _ _ hs hplt hkv) pos.val hplt'
          (by rw [hokv]),
          show (out.val[pos.val]'hplt').2 = v by rw [hokv]]
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
                maybe_split entries1 children replaced) ⦃ r => GetPost k v r ⦄
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
        have hjl : pos.val < out.val.length := by
          rw [hiav]; simp; omega
        apply maybe_split_get out Children.Nil false
          (by rw [hiav]; exact SortedE_insertAt _ _ _ hs hple hlt' hgt')
          pos.val hjl
          (by revert hjl; rw [hiav]; intro hjl; exact getElem_insertAt_self _ _ _ hple)
          (by rw [hiav]; simp; omega)
          (by simp [clist])
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
            ok (InsertResult.Fit (Node.mk entries1 children) true)) ⦃ r => GetPost k v r ⦄
        obtain ⟨hplt, hkey⟩ := hhit rfl
        obtain ⟨out, hre, hrev⟩ :=
          Aeneas.Std.WP.spec_imp_exists (replace_entry_spec entries pos (k, v))
        obtain ⟨d, hcc, hccv⟩ :=
          Aeneas.Std.WP.spec_imp_exists (clone_children_spec (Children.Cons c cs))
        rw [hre, hcc]
        simp only [bind_tc_ok, Aeneas.Std.WP.spec_ok]
        show get_in_node (Node.mk out d) k = ok (some v)
        have hkv : (entries.val[pos.val]'hplt).1.val = (k, v).1.val := by rw [hkey]
        have hplt' : pos.val < out.val.length := by rw [hrev]; simpa using hplt
        have hokv : (out.val[pos.val]'hplt') = (k, v) := by
          revert hplt'
          rw [hrev]
          intro hplt'
          exact List.getElem_set_self ..
        rw [get_found out d k
          (by rw [hrev]; exact SortedE_set _ _ _ hs hplt hkv) pos.val hplt'
          (by rw [hokv]),
          show (out.val[pos.val]'hplt').2 = v by rw [hokv]]
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
                maybe_split entries1 children replaced) ⦃ r => GetPost k v r ⦄
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
        have hboth := spec_and (insert_into_node_inv fuel n k v hsz hcn hkn)
          (ih n hsz hcn hkn)
        obtain ⟨ir, hir, hirp, hirg⟩ := Aeneas.Std.WP.spec_imp_exists hboth
        rw [hir]
        simp only [bind_tc_ok]
        cases ir with
        | Fit new_child replaced =>
          show (do
              let children ← replace_child (Children.Cons c cs) pos new_child
              let entries1 ← alloc.vec.CloneVec.clone (BuiltinClone (Std.U64 × Std.U64)) entries
              ok (InsertResult.Fit (Node.mk entries1 children) replaced)) ⦃ r => GetPost k v r ⦄
          obtain ⟨ch, hrc, hrcv⟩ := Aeneas.Std.WP.spec_imp_exists
            (replace_child_spec (Children.Cons c cs) pos new_child)
          obtain ⟨ent, hcv, hcvv⟩ := Aeneas.Std.WP.spec_imp_exists
            (cloneVec_builtin_spec entries)
          rw [hrc, hcv]
          simp only [bind_tc_ok, Aeneas.Std.WP.spec_ok]
          rw [hcvv]
          show get_in_node (Node.mk entries ch) k = ok (some v)
          have hchl : (clist ch).length = entries.val.length + 1 := by
            rw [hrcv]
            simpa using hlen1
          rw [get_descend' entries ch k hs pos.val hple hlt' hgt' hchl (by scalar_tac)]
          simp only [hrcv]
          rw [List.getElem_set_self]
          exact hirg
        | Split l median r replaced =>
          show (do
              let entries1 ← insert_entry_at entries pos median
              let children ← replace_and_insert_child (Children.Cons c cs) pos l r
              maybe_split entries1 children replaced) ⦃ r => GetPost k v r ⦄
          obtain ⟨hgm, hgl, hgr⟩ := hirg
          obtain ⟨hnl, hnr, hnm⟩ := hirp
          obtain ⟨ent, hia, hiav⟩ := Aeneas.Std.WP.spec_imp_exists
            (insert_entry_at_spec entries pos median hple (by scalar_tac))
          obtain ⟨ch, hri, hriv⟩ := Aeneas.Std.WP.spec_imp_exists
            (replace_and_insert_child_spec (Children.Cons c cs) pos l r hposlt)
          rw [hia, hri]
          simp only [bind_tc_ok]
          obtain ⟨hmlt, hmgt⟩ := bracket_of_InB_bnd entries.val pos.val median.1.val
            hs hple hnm
          have hsent : SortedE ent.val := by
            rw [hiav]; exact SortedE_insertAt _ _ _ hs hple hmlt hmgt
          have hentl : ent.val.length = entries.val.length + 1 := by
            rw [hiav]; simp; omega
          have hchl : (clist ch).length = ent.val.length + 1 := by
            rw [hriv, hentl]
            simp only [List.length_append, List.length_take, List.length_cons,
              List.length_drop]
            rw [hlen1]
            omega
          have hchmax : (clist ch).length ≤ Std.Usize.max := by
            rw [hchl, hentl]
            scalar_tac
          rcases Nat.lt_trichotomy k.val median.1.val with hcmp | hcmp | hcmp
          · apply maybe_split_get_descend ent ch replaced hsent pos.val
              (by rw [hentl]; omega) ?_ ?_ hchl (by rw [hentl]; scalar_tac)
              (by rw [hentl]; scalar_tac) ?_
            · intro i h hipos
              have hi : i < entries.val.length := by omega
              have : (ent.val[i]'h) = (entries.val[i]'hi) := by
                revert h
                rw [hiav]
                intro h
                rw [List.getElem_append_left (by simp; omega)]
                rw [List.getElem_take]
              rw [this]
              exact hlt' i hi hipos
            · intro i h hposi
              rcases Nat.lt_trichotomy i pos.val with h2 | h2 | h2
              · omega
              · have : (ent.val[i]'h) = median := by
                  revert h
                  rw [hiav, ← h2]
                  intro h
                  exact getElem_insertAt_self _ _ _ (by omega)
                rw [this]
                exact hcmp
              · have hi : i - 1 < entries.val.length := by
                  rw [hentl] at h; omega
                have : (ent.val[i]'h) = (entries.val[i - 1]'hi) := by
                  apply getElem_of_getElem?
                  rw [hiav, List.getElem?_append_right
                    (by simp only [List.length_take]; omega)]
                  simp only [List.length_take]
                  rw [show i - min pos.val entries.val.length = (i - pos.val - 1) + 1
                      from by omega,
                    List.getElem?_cons_succ, List.getElem?_drop,
                    show pos.val + (i - pos.val - 1) = i - 1 from by omega]
                  exact List.getElem?_eq_getElem hi
                rw [this]
                exact hgt' (i - 1) hi (by omega)
            · have hchq : (clist ch)[pos.val]'(by rw [hchl, hentl]; omega) = l := by
                apply getElem_of_getElem?
                rw [hriv, List.getElem?_append_right
                  (by simp only [List.length_take]; omega)]
                simp only [List.length_take]
                rw [show pos.val - min pos.val (clist (Children.Cons c cs)).length = 0
                    from by rw [hlen1]; omega]
                rfl
              rw [hchq]
              exact hgl hcmp
          · obtain ⟨hm1, hm2⟩ := hgm hcmp.symm
            have hjl : pos.val < ent.val.length := by rw [hentl]; omega
            apply maybe_split_get ent ch replaced hsent pos.val hjl ?_
              (by rw [hentl]; omega) hchmax
            revert hjl
            rw [hiav]
            intro hjl
            rw [getElem_insertAt_self _ _ _ hple]
            rw [Prod.ext_iff]
            exact ⟨hm1, hm2⟩
          · apply maybe_split_get_descend ent ch replaced hsent (pos.val + 1)
              (by rw [hentl]; omega) ?_ ?_ hchl (by rw [hentl]; scalar_tac)
              (by rw [hentl]; scalar_tac) ?_
            · intro i h hipos
              rcases Nat.lt_trichotomy i pos.val with h2 | h2 | h2
              · have hi : i < entries.val.length := by omega
                have : (ent.val[i]'h) = (entries.val[i]'hi) := by
                  revert h
                  rw [hiav]
                  intro h
                  rw [List.getElem_append_left (by simp; omega)]
                  rw [List.getElem_take]
                rw [this]
                exact hlt' i hi h2
              · have : (ent.val[i]'h) = median := by
                  revert h
                  rw [hiav, ← h2]
                  intro h
                  exact getElem_insertAt_self _ _ _ (by omega)
                rw [this]
                exact hcmp
              · omega
            · intro i h hposi
              have hi : i - 1 < entries.val.length := by
                rw [hentl] at h; omega
              have : (ent.val[i]'h) = (entries.val[i - 1]'hi) := by
                apply getElem_of_getElem?
                rw [hiav, List.getElem?_append_right
                  (by simp only [List.length_take]; omega)]
                simp only [List.length_take]
                rw [show i - min pos.val entries.val.length = (i - pos.val - 1) + 1
                    from by omega,
                  List.getElem?_cons_succ, List.getElem?_drop,
                  show pos.val + (i - pos.val - 1) = i - 1 from by omega]
                exact List.getElem?_eq_getElem hi
              rw [this]
              exact hgt' (i - 1) hi (by omega)
            · have hchq : (clist ch)[pos.val + 1]'(by rw [hchl, hentl]; omega) = r := by
                apply getElem_of_getElem?
                rw [hriv, List.getElem?_append_right
                  (by simp only [List.length_take]; omega)]
                simp only [List.length_take]
                rw [show pos.val + 1 - min pos.val (clist (Children.Cons c cs)).length = 1
                    from by rw [hlen1]; omega]
                rfl
              rw [hchq]
              exact hgr hcmp

/-- Top-level: after `insert k v`, `get k` returns `some v`. -/
theorem BTree.insert_get (t : BTree) (k v : Std.U64)
    (hinv : BTreeInv t) (hcap : t.len.val < Std.Usize.max) :
    BTree.insert t k v ⦃ t' => t'.get k = ok (some v) ⦄ := by
  rw [BTree.insert]
  have hk : InB none none k.val := ⟨by simp, by simp⟩
  have hboth := spec_and
    (insert_into_node_inv (Node.size t.root) t.root k v (Nat.le_refl _) hinv hk)
    (insert_get_same (Node.size t.root) t.root k v (Nat.le_refl _) hinv hk)
  obtain ⟨ir, hir, hirp, hirg⟩ := Aeneas.Std.WP.spec_imp_exists hboth
  rw [hir]
  simp only [bind_tc_ok]
  cases ir with
  | Fit new_root replaced =>
    show (if replaced then ok { t with root := new_root }
          else do
            let new_len ← t.len + 1#usize
            ok ({ root := new_root, len := new_len } : BTree)) ⦃ t' =>
      t'.get k = ok (some v) ⦄
    split
    · exact (Aeneas.Std.WP.spec_ok _).mpr (by rw [BTree.get]; exact hirg)
    · obtain ⟨nl, hadd, -⟩ := Aeneas.Std.WP.spec_imp_exists
        (Usize.add_spec (x := t.len) (y := 1#usize) (by scalar_tac))
      rw [hadd]
      simp only [bind_tc_ok]
      exact (Aeneas.Std.WP.spec_ok _).mpr (by rw [BTree.get]; exact hirg)
  | Split l median r replaced =>
    show (do
        let new_len ← if replaced then ok t.len else t.len + 1#usize
        let entries ← alloc.vec.Vec.push (alloc.vec.Vec.new (Std.U64 × Std.U64)) median
        ok ({ root := Node.mk entries
                (Children.Cons l (Children.Cons r Children.Nil)),
              len := new_len } : BTree)) ⦃ t' => t'.get k = ok (some v) ⦄
    obtain ⟨hgm, hgl, hgr⟩ := hirg
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
    rw [BTree.get]
    show get_in_node (Node.mk ents
      (Children.Cons l (Children.Cons r Children.Nil))) k = ok (some v)
    have hse : SortedE ents.val := by rw [hev]; simp [SortedE]
    have hclr : (clist (Children.Cons l (Children.Cons r Children.Nil))).length =
        ents.val.length + 1 := by
      rw [hev]; simp [clist]
    rcases Nat.lt_trichotomy k.val median.1.val with hcmp | hcmp | hcmp
    · -- k < median: descend into l (slot 0)
      rw [get_descend' ents _ k hse 0 (by omega) (by omega)
        (fun i h _ => by
          revert h
          rw [hev]
          intro h
          have : i = 0 := by simp at h; omega
          subst this
          simpa using hcmp)
        hclr (by rw [hev]; scalar_tac)]
      simp only [clist, List.getElem_cons_zero]
      exact hgl hcmp
    · -- k = median key: it is at index 0
      obtain ⟨hm1, hm2⟩ := hgm hcmp.symm
      have h0 : 0 < ents.val.length := by rw [hev]; simp
      have hp0 : (ents.val[0]'h0) = median :=
        getElem_of_getElem? _ _ _ _ (by rw [hev]; rfl)
      rw [get_found ents _ k hse 0 h0 (by rw [hp0]; exact hm1),
        show (ents.val[0]'h0).2 = v by rw [hp0]; exact hm2]
    · -- median < k: descend into r (slot 1)
      rw [get_descend' ents _ k hse 1 (by rw [hev]; simp)
        (fun i h hi => by
          revert h
          rw [hev]
          intro h
          have : i = 0 := by simp at h; omega
          subst this
          simpa using hcmp)
        (fun i h hi => by
          revert h
          rw [hev]
          intro h
          simp at h
          omega)
        hclr (by rw [hev]; scalar_tac)]
      simp only [clist, List.getElem_cons_succ, List.getElem_cons_zero]
      exact hgr hcmp

end btree_kernel


