/- The frame property: `insert k v` does not change `get k'` for any k' ≠ k.

   `GetR k' res r` relates an insert result to a fixed lookup outcome `res`
   (which will be instantiated with `get_in_node <original node> k'`): a `Fit`
   node looks up to `res`; after a `Split`, either the median key IS k' and
   `res` is the median's (pre-existing) value, or the half selected by
   comparing k' with the median looks up to `res`.
-/
import BtreeKernel
import BtreeInvariant
import BtreeInsertInv
import BtreeInsertGet
import TransportLemmas

open Aeneas Aeneas.Std Result

set_option maxRecDepth 4096

namespace btree_kernel

/-- Insert-result lookup of `k'` resolves to `res`. -/
def GetR (k' : Std.U64) (res : Result (Option Std.U64)) : InsertResult → Prop
  | .Fit n _ => get_in_node n k' = res
  | .Split l m r _ =>
      (m.1.val = k'.val → res = ok (some m.2)) ∧
      (k'.val < m.1.val → get_in_node l k' = res) ∧
      (m.1.val < k'.val → get_in_node r k' = res)

/-- A leaf lookup of a key bracketed (hence absent) returns `none`. -/
theorem get_none_leaf (entries : alloc.vec.Vec (Std.U64 × Std.U64))
    (k' : Std.U64) (hs : SortedE entries.val)
    (q : Nat) (hq : q ≤ entries.val.length)
    (hlt : ∀ i, (h : i < entries.val.length) → i < q →
      (entries.val[i]'h).1.val < k'.val)
    (hgt : ∀ i, (h : i < entries.val.length) → q ≤ i →
      k'.val < (entries.val[i]'h).1.val) :
    get_in_node (Node.mk entries Children.Nil) k' = ok none := by
  rw [get_in_node]
  simp only [Node.entries._simpLemma_, Node.children._simpLemma_]
  obtain ⟨⟨hit, pos⟩, hfp, hple, hhit, hmiss⟩ :=
    Aeneas.Std.WP.spec_imp_exists (find_pos_spec entries k' hs)
  rw [hfp]
  simp only [bind_tc_ok]
  dsimp only at hple hhit hmiss
  cases hit with
  | true =>
    exfalso
    obtain ⟨hplt, hpkey⟩ := hhit rfl
    rcases Nat.lt_or_ge pos.val q with h | h
    · have := hlt pos.val hplt h
      rw [hpkey] at this
      omega
    · have := hgt pos.val hplt h
      rw [hpkey] at this
      omega
  | false =>
    show (do
        let i ← children_len Children.Nil
        if i = 0#usize
        then ok none
        else do
          let n ← child_at Children.Nil pos
          get_in_node n k' : Result (Option Std.U64)) = ok none
    obtain ⟨len, hcl, hclv⟩ := Aeneas.Std.WP.spec_imp_exists
      (children_len_spec Children.Nil (by simp [clist]))
    rw [hcl]
    simp only [bind_tc_ok]
    rw [if_pos (by simp [clist] at hclv; scalar_tac)]

/-- Frame version of `maybe_split_get_descend`: if `k` is bracketed at slot
    `q` and the child there looks up to `res`, every part of the split result
    looks up to `res`. -/
theorem maybe_split_frame_descend {k : Std.U64} {res : Result (Option Std.U64)}
    (entries : alloc.vec.Vec (Std.U64 × Std.U64)) (children : Children)
    (replaced : Bool) (hs : SortedE entries.val)
    (q : Nat) (hq : q ≤ entries.val.length)
    (hlt : ∀ i, (h : i < entries.val.length) → i < q →
      (entries.val[i]'h).1.val < k.val)
    (hgt : ∀ i, (h : i < entries.val.length) → q ≤ i →
      k.val < (entries.val[i]'h).1.val)
    (hcl : (clist children).length = entries.val.length + 1)
    (hlen : entries.val.length ≤ 128)
    (hsmall : entries.val.length < Std.Usize.max)
    (hgv : get_in_node ((clist children)[q]'(by rw [hcl]; omega)) k = res) :
    maybe_split entries children replaced ⦃ r => GetR k res r ⦄ := by
  rw [maybe_split]
  step*
  · -- Fit: descend through the unchanged node
    show get_in_node (Node.mk entries children) k = res
    rw [get_descend' entries children k hs q hq hlt hgt hcl hsmall]
    exact hgv
  · -- Split at the median
    have h64 : entries.val.length = 128 := by scalar_tac
    have hmid : mid.val = 64 := by scalar_tac
    have hmidlt : mid.val < entries.val.length := by scalar_tac
    have hmedian : median = entries.val[mid.val]'hmidlt := by rw [median_post]
    have hi3 : i3.val = mid.val + 1 := by scalar_tac
    have hlpost : left_entries.val = entries.val.take mid.val := left_entries_post
    have hrpost : right_entries.val = entries.val.drop (mid.val + 1) := by
      rw [right_entries_post]
      congr 1
    split
    · exfalso
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

/-- Leaf variant: a bracketed key looks up to `none` in every part of the
    split result. -/
theorem maybe_split_none {k : Std.U64}
    (entries : alloc.vec.Vec (Std.U64 × Std.U64)) (replaced : Bool)
    (hs : SortedE entries.val)
    (q : Nat) (hq : q ≤ entries.val.length)
    (hlt : ∀ i, (h : i < entries.val.length) → i < q →
      (entries.val[i]'h).1.val < k.val)
    (hgt : ∀ i, (h : i < entries.val.length) → q ≤ i →
      k.val < (entries.val[i]'h).1.val)
    (hlen : entries.val.length ≤ 128) :
    maybe_split entries Children.Nil replaced ⦃ r => GetR k (ok none) r ⦄ := by
  rw [maybe_split]
  step*
  · -- Fit
    show get_in_node (Node.mk entries Children.Nil) k = ok none
    exact get_none_leaf entries k hs q hq hlt hgt
  · -- side condition
    simp [clist]
  · -- Split
    have h64 : entries.val.length = 128 := by scalar_tac
    have hmid : mid.val = 64 := by scalar_tac
    have hmidlt : mid.val < entries.val.length := by scalar_tac
    have hmedian : median = entries.val[mid.val]'hmidlt := by rw [median_post]
    have hlpost : left_entries.val = entries.val.take mid.val := left_entries_post
    have hrpost : right_entries.val = entries.val.drop (mid.val + 1) := by
      rw [right_entries_post]
      congr 1
    split
    · step*
      refine ⟨?_, ?_, ?_⟩
      · intro hmk
        exfalso
        rcases Nat.lt_or_ge mid.val q with h | h
        · have := hlt mid.val hmidlt h
          rw [hmedian] at hmk
          omega
        · have := hgt mid.val hmidlt h
          rw [hmedian] at hmk
          omega
      · intro hklt
        have hqle : q ≤ mid.val := by
          rcases Nat.lt_or_ge mid.val q with h | h
          · exfalso
            have := hlt mid.val hmidlt h
            rw [hmedian] at hklt
            omega
          · exact h
        apply get_none_leaf left_entries k
          (by rw [hlpost]; exact SortedE_take _ _ hs) q
          (by rw [hlpost]; simp; omega)
        · intro i h hiq
          have hi : i < entries.val.length := by
            rw [hlpost] at h; simp at h; omega
          have : (left_entries.val[i]'h) = (entries.val[i]'hi) := by
            revert h; rw [hlpost]; intro h; rw [List.getElem_take]
          rw [this]
          exact hlt i hi hiq
        · intro i h hqi
          have hi : i < entries.val.length := by
            rw [hlpost] at h; simp at h; omega
          have : (left_entries.val[i]'h) = (entries.val[i]'hi) := by
            revert h; rw [hlpost]; intro h; rw [List.getElem_take]
          rw [this]
          exact hgt i hi hqi
      · intro hkgt
        have hqge : mid.val + 1 ≤ q := by
          rcases Nat.lt_or_ge q (mid.val + 1) with h | h
          · exfalso
            have := hgt mid.val hmidlt (by omega)
            rw [hmedian] at hkgt
            omega
          · exact h
        apply get_none_leaf right_entries k
          (by rw [hrpost]; exact SortedE_drop _ _ hs) (q - (mid.val + 1))
          (by rw [hrpost]; simp; omega)
        · intro i h hiq
          have hi : mid.val + 1 + i < entries.val.length := by
            rw [hrpost] at h; simp at h; omega
          have : (right_entries.val[i]'h) = (entries.val[mid.val + 1 + i]'hi) := by
            revert h; rw [hrpost]; intro h; rw [List.getElem_drop]
          rw [this]
          exact hlt _ hi (by omega)
        · intro i h hqi
          have hi : mid.val + 1 + i < entries.val.length := by
            rw [hrpost] at h; simp at h; omega
          have : (right_entries.val[i]'h) = (entries.val[mid.val + 1 + i]'hi) := by
            revert h; rw [hrpost]; intro h; rw [List.getElem_drop]
          rw [this]
          exact hgt _ hi (by omega)
    · exfalso
      rename_i hi40
      simp [clist] at i4_post
      scalar_tac

/-- `GetPost` (insert-then-get) implies `GetR` with the inserted value. -/
theorem GetR_of_GetPost {k w : Std.U64} {r : InsertResult}
    (h : GetPost k w r) : GetR k (ok (some w)) r := by
  cases r with
  | Fit n replaced => exact h
  | Split l m rn replaced =>
    obtain ⟨h1, h2, h3⟩ := h
    exact ⟨fun hmk => by rw [(h1 hmk).2], h2, h3⟩

/-- Brackets only read keys, so they transport along pointwise key equality. -/
theorem bracket_of_keys_eq {l l' : List (Std.U64 × Std.U64)} {q : Nat} {k : Nat}
    (hlen : l'.length = l.length)
    (hkeys : ∀ i, (h : i < l.length) → (h' : i < l'.length) →
      (l'[i]'h').1.val = (l[i]'h).1.val)
    (hlt : ∀ i, (h : i < l.length) → i < q → (l[i]'h).1.val < k)
    (hgt : ∀ i, (h : i < l.length) → q ≤ i → k < (l[i]'h).1.val) :
    (∀ i, (h : i < l'.length) → i < q → (l'[i]'h).1.val < k) ∧
    (∀ i, (h : i < l'.length) → q ≤ i → k < (l'[i]'h).1.val) := by
  constructor
  · intro i h hiq
    have hi : i < l.length := by omega
    rw [hkeys i hi h]
    exact hlt i hi hiq
  · intro i h hqi
    have hi : i < l.length := by omega
    rw [hkeys i hi h]
    exact hgt i hi hqi

/-! ### Indexing into the children surgery `take pos ++ l :: r :: drop (pos+1)` -/

theorem surgery_getElem_lo (A : List Node) (pos : Nat) (l r : Node)
    (hpos : pos < A.length) (j : Nat) (hj : j < A.length) (hjp : j < pos) :
    (A.take pos ++ l :: r :: A.drop (pos + 1))[j]'(by simp; omega) = A[j]'hj := by
  apply getElem_of_getElem?
  rw [List.getElem?_append_left (by simp; omega), List.getElem?_take_of_lt hjp]
  exact List.getElem?_eq_getElem hj

theorem surgery_getElem_l (A : List Node) (pos : Nat) (l r : Node)
    (hpos : pos < A.length) :
    (A.take pos ++ l :: r :: A.drop (pos + 1))[pos]'(by simp; omega) = l := by
  apply getElem_of_getElem?
  rw [List.getElem?_append_right (by simp only [List.length_take]; omega)]
  simp only [List.length_take]
  rw [show pos - min pos A.length = 0 from by omega]
  rfl

theorem surgery_getElem_r (A : List Node) (pos : Nat) (l r : Node)
    (hpos : pos < A.length) :
    (A.take pos ++ l :: r :: A.drop (pos + 1))[pos + 1]'(by simp; omega) = r := by
  apply getElem_of_getElem?
  rw [List.getElem?_append_right (by simp only [List.length_take]; omega)]
  simp only [List.length_take]
  rw [show pos + 1 - min pos A.length = 1 from by omega]
  rfl

theorem surgery_getElem_hi (A : List Node) (pos : Nat) (l r : Node)
    (hpos : pos < A.length) (j : Nat) (hj : j < A.length) (hjp : pos < j) :
    (A.take pos ++ l :: r :: A.drop (pos + 1))[j + 1]'(by simp; omega) = A[j]'hj := by
  apply getElem_of_getElem?
  rw [List.getElem?_append_right (by simp only [List.length_take]; omega)]
  simp only [List.length_take]
  rw [show j + 1 - min pos A.length = (j - pos - 1) + 1 + 1 from by omega,
    List.getElem?_cons_succ, List.getElem?_cons_succ, List.getElem?_drop,
    show pos + 1 + (j - pos - 1) = j from by omega]
  exact List.getElem?_eq_getElem hj

/-! ## Main frame theorem -/

theorem insert_frame (fuel : Nat) {lo hi : Option Nat}
    (node : Node) (k v k' : Std.U64)
    (hfuel : Node.size node ≤ fuel)
    (hinv : NodeInv lo hi node) (hk : InB lo hi k.val)
    (hne : k'.val ≠ k.val) :
    insert_into_node node k v ⦃ r => GetR k' (get_in_node node k') r ⦄ := by
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
      -- resolve the k' lookup on the ORIGINAL leaf once
      obtain ⟨⟨hit', pos'⟩, hfp', hple', hhit', hmiss'⟩ :=
        Aeneas.Std.WP.spec_imp_exists (find_pos_spec entries k' hs)
      dsimp only at hple' hhit' hmiss'
      cases hit with
      | true =>
        show (do
            let entries1 ← replace_entry entries pos (k, v)
            let children ← clone_children Children.Nil
            ok (InsertResult.Fit (Node.mk entries1 children) true)) ⦃ r =>
          GetR k' (get_in_node (Node.mk entries Children.Nil) k') r ⦄
        obtain ⟨hplt, hkey⟩ := hhit rfl
        obtain ⟨out, hre, hrev⟩ :=
          Aeneas.Std.WP.spec_imp_exists (replace_entry_spec entries pos (k, v))
        obtain ⟨d, hcc, hccv⟩ :=
          Aeneas.Std.WP.spec_imp_exists (clone_children_spec Children.Nil)
        rw [hre, hcc]
        simp only [bind_tc_ok, Aeneas.Std.WP.spec_ok]
        subst hccv
        show get_in_node (Node.mk out Children.Nil) k' =
          get_in_node (Node.mk entries Children.Nil) k'
        have hkv : (entries.val[pos.val]'hplt).1.val = (k, v).1.val := by rw [hkey]
        have hsout : SortedE out.val := by
          rw [hrev]; exact SortedE_set _ _ _ hs hplt hkv
        have hlenout : out.val.length = entries.val.length := by rw [hrev]; simp
        have hkeys : ∀ i, (h : i < entries.val.length) → (h' : i < out.val.length) →
            (out.val[i]'h').1.val = (entries.val[i]'h).1.val := by
          intro i h h'
          by_cases hip : i = pos.val
          · subst hip
            have h1 : (out.val[pos.val]'h') = (k, v) := by
              revert h'; rw [hrev]; intro h'; exact List.getElem_set_self ..
            rw [h1]
            have h2 : (entries.val[pos.val]'h).1 = k := hkey
            rw [h2]
          · have h1 : (out.val[i]'h') = (entries.val[i]'h) := by
              revert h'
              rw [hrev]
              intro h'
              rw [List.getElem_set_ne (by omega)]
            rw [h1]
        cases hit' with
        | true =>
          obtain ⟨hplt', hkey'⟩ := hhit' rfl
          have hne' : pos'.val ≠ pos.val := by
            intro heq
            apply hne
            have e1 : (entries.val[pos'.val]'hplt').1.val = k'.val := by rw [hkey']
            have e2 : (entries.val[pos.val]'hplt).1.val = k.val := by rw [hkey]
            have e3 : (entries.val[pos'.val]'hplt').1.val =
                (entries.val[pos.val]'hplt).1.val := by simp only [heq]
            omega
          have hplt'' : pos'.val < out.val.length := by omega
          have hout' : (out.val[pos'.val]'hplt'') = (entries.val[pos'.val]'hplt') := by
            revert hplt''
            rw [hrev]
            intro hplt''
            rw [List.getElem_set_ne (by omega)]
          rw [get_found out Children.Nil k' hsout pos'.val hplt''
              (by rw [hout']; exact hkey'),
            get_found entries Children.Nil k' hs pos'.val hplt' hkey']
          congr 1
          congr 1
          rw [hout']
        | false =>
          obtain ⟨hlt', hgt'⟩ := hmiss' rfl
          obtain ⟨hlt'', hgt''⟩ := bracket_of_keys_eq hlenout hkeys hlt' hgt'
          rw [get_none_leaf out k' hsout pos'.val (by omega) hlt'' hgt'',
            get_none_leaf entries k' hs pos'.val hple' hlt' hgt']
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
                maybe_split entries1 children replaced) ⦃ r =>
          GetR k' (get_in_node (Node.mk entries Children.Nil) k') r ⦄
        obtain ⟨hlt, hgt⟩ := hmiss rfl
        obtain ⟨i, hcl, hclv⟩ := Aeneas.Std.WP.spec_imp_exists
          (children_len_spec Children.Nil (by simp [clist]))
        rw [hcl]
        simp only [bind_tc_ok]
        rw [if_pos (by simp [clist] at hclv; scalar_tac)]
        obtain ⟨out, hia, hiav⟩ := Aeneas.Std.WP.spec_imp_exists
          (insert_entry_at_spec entries pos (k, v) hple (by scalar_tac))
        rw [hia]
        simp only [bind_tc_ok]
        have hsout : SortedE out.val := by
          rw [hiav]; exact SortedE_insertAt _ _ _ hs hple hlt hgt
        have hlenout : out.val.length = entries.val.length + 1 := by
          rw [hiav]; simp; omega
        cases hit' with
        | true =>
          -- k' present at pos' in the original: also present in out
          obtain ⟨hplt', hkey'⟩ := hhit' rfl
          rw [get_found entries Children.Nil k' hs pos'.val hplt' hkey']
          have hw : (entries.val[pos'.val]'hplt') =
              (k', (entries.val[pos'.val]'hplt').2) := by
            rw [Prod.ext_iff]
            exact ⟨hkey', rfl⟩
          rcases Nat.lt_or_ge pos'.val pos.val with hpp | hpp
          · -- k' stays at pos'
            have hj : pos'.val < out.val.length := by omega
            have hkv2 : (out.val[pos'.val]'hj) = (k', (entries.val[pos'.val]'hplt').2) := by
              revert hj
              rw [hiav]
              intro hj
              rw [insertAt_getElem_lo entries.val pos.val (k, v) hple pos'.val hplt' hpp]
              exact hw
            apply Aeneas.Std.WP.spec_mono (maybe_split_get out Children.Nil false
              hsout pos'.val hj hkv2 (by omega) (by simp [clist]))
            exact fun r hr => GetR_of_GetPost hr
          · -- k' shifts to pos' + 1
            have hj : pos'.val + 1 < out.val.length := by omega
            have hkv2 : (out.val[pos'.val + 1]'hj) = (k', (entries.val[pos'.val]'hplt').2) := by
              revert hj
              rw [hiav]
              intro hj
              rw [insertAt_getElem_hi entries.val pos.val (k, v) hple pos'.val hplt' hpp]
              exact hw
            apply Aeneas.Std.WP.spec_mono (maybe_split_get out Children.Nil false
              hsout (pos'.val + 1) hj hkv2 (by omega) (by simp [clist]))
            exact fun r hr => GetR_of_GetPost hr
        | false =>
          -- k' absent everywhere: both sides none
          obtain ⟨hlt', hgt'⟩ := hmiss' rfl
          rw [get_none_leaf entries k' hs pos'.val hple' hlt' hgt']
          rcases Nat.lt_trichotomy k'.val k.val with hcmp | hcmp | hcmp
          · -- k' < k: bracket position q' ≤ pos preserved
            have hqp : pos'.val ≤ pos.val :=
              bracket_lt_le entries.val hple' hple hcmp hlt' hgt
            obtain ⟨hlt2, hgt2⟩ := bracket_insertAt_lo entries.val pos.val (k, v)
              hple pos'.val hple' hqp k'.val hcmp hlt' hgt'
            have hlt3 : ∀ i, (h : i < out.val.length) → i < pos'.val →
                (out.val[i]'h).1.val < k'.val := by
              intro i h hi
              have h2 : i < (entries.val.take pos.val ++ (k, v) :: entries.val.drop pos.val).length := by
                rw [← hiav]; exact h
              have : (out.val[i]'h) =
                  ((entries.val.take pos.val ++ (k, v) :: entries.val.drop pos.val)[i]'h2) := by
                revert h; rw [hiav]; intro h; rfl
              rw [this]
              exact hlt2 i h2 hi
            have hgt3 : ∀ i, (h : i < out.val.length) → pos'.val ≤ i →
                k'.val < (out.val[i]'h).1.val := by
              intro i h hi
              have h2 : i < (entries.val.take pos.val ++ (k, v) :: entries.val.drop pos.val).length := by
                rw [← hiav]; exact h
              have : (out.val[i]'h) =
                  ((entries.val.take pos.val ++ (k, v) :: entries.val.drop pos.val)[i]'h2) := by
                revert h; rw [hiav]; intro h; rfl
              rw [this]
              exact hgt2 i h2 hi
            exact maybe_split_none out false hsout pos'.val (by omega) hlt3 hgt3
              (by omega)
          · exact absurd hcmp hne
          · -- k < k': bracket position shifts to q' + 1
            have hqp : pos.val ≤ pos'.val :=
              bracket_lt_le entries.val hple hple' hcmp hlt hgt'
            obtain ⟨hlt2, hgt2⟩ := bracket_insertAt_hi entries.val pos.val (k, v)
              hple pos'.val hple' hqp k'.val hcmp hlt' hgt'
            have hlt3 : ∀ i, (h : i < out.val.length) → i < pos'.val + 1 →
                (out.val[i]'h).1.val < k'.val := by
              intro i h hi
              have h2 : i < (entries.val.take pos.val ++ (k, v) :: entries.val.drop pos.val).length := by
                rw [← hiav]; exact h
              have : (out.val[i]'h) =
                  ((entries.val.take pos.val ++ (k, v) :: entries.val.drop pos.val)[i]'h2) := by
                revert h; rw [hiav]; intro h; rfl
              rw [this]
              exact hlt2 i h2 hi
            have hgt3 : ∀ i, (h : i < out.val.length) → pos'.val + 1 ≤ i →
                k'.val < (out.val[i]'h).1.val := by
              intro i h hi
              have h2 : i < (entries.val.take pos.val ++ (k, v) :: entries.val.drop pos.val).length := by
                rw [← hiav]; exact h
              have : (out.val[i]'h) =
                  ((entries.val.take pos.val ++ (k, v) :: entries.val.drop pos.val)[i]'h2) := by
                revert h; rw [hiav]; intro h; rfl
              rw [this]
              exact hgt2 i h2 hi
            exact maybe_split_none out false hsout (pos'.val + 1) (by omega)
              hlt3 hgt3 (by omega)
    | internal entries c cs hs hb hlen hal =>
      rw [insert_into_node]
      simp only [Node.entries._simpLemma_, Node.children._simpLemma_]
      obtain ⟨⟨hit, pos⟩, hfp, hple, hhit, hmiss⟩ :=
        Aeneas.Std.WP.spec_imp_exists (find_pos_spec entries k hs)
      rw [hfp]
      simp only [bind_tc_ok]
      dsimp only at hple hhit hmiss
      obtain ⟨⟨hit', pos'⟩, hfp', hple', hhit', hmiss'⟩ :=
        Aeneas.Std.WP.spec_imp_exists (find_pos_spec entries k' hs)
      dsimp only at hple' hhit' hmiss'
      have hlen1 : (clist (Children.Cons c cs)).length = entries.val.length + 1 := by
        simpa [clist] using hal.length_eq
      cases hit with
      | true =>
        show (do
            let entries1 ← replace_entry entries pos (k, v)
            let children ← clone_children (Children.Cons c cs)
            ok (InsertResult.Fit (Node.mk entries1 children) true)) ⦃ r =>
          GetR k' (get_in_node (Node.mk entries (Children.Cons c cs)) k') r ⦄
        obtain ⟨hplt, hkey⟩ := hhit rfl
        obtain ⟨out, hre, hrev⟩ :=
          Aeneas.Std.WP.spec_imp_exists (replace_entry_spec entries pos (k, v))
        obtain ⟨d, hcc, hccv⟩ :=
          Aeneas.Std.WP.spec_imp_exists (clone_children_spec (Children.Cons c cs))
        rw [hre, hcc]
        simp only [bind_tc_ok, Aeneas.Std.WP.spec_ok]
        subst hccv
        show get_in_node (Node.mk out (Children.Cons c cs)) k' =
          get_in_node (Node.mk entries (Children.Cons c cs)) k'
        have hkv : (entries.val[pos.val]'hplt).1.val = (k, v).1.val := by rw [hkey]
        have hsout : SortedE out.val := by
          rw [hrev]; exact SortedE_set _ _ _ hs hplt hkv
        have hlenout : out.val.length = entries.val.length := by rw [hrev]; simp
        have hkeys : ∀ i, (h : i < entries.val.length) → (h' : i < out.val.length) →
            (out.val[i]'h').1.val = (entries.val[i]'h).1.val := by
          intro i h h'
          by_cases hip : i = pos.val
          · subst hip
            have h1 : (out.val[pos.val]'h') = (k, v) := by
              revert h'; rw [hrev]; intro h'; exact List.getElem_set_self ..
            rw [h1]
            have h2 : (entries.val[pos.val]'h).1 = k := hkey
            rw [h2]
          · have h1 : (out.val[i]'h') = (entries.val[i]'h) := by
              revert h'
              rw [hrev]
              intro h'
              rw [List.getElem_set_ne (by omega)]
            rw [h1]
        cases hit' with
        | true =>
          obtain ⟨hplt', hkey'⟩ := hhit' rfl
          have hne' : pos'.val ≠ pos.val := by
            intro heq
            apply hne
            have e1 : (entries.val[pos'.val]'hplt').1.val = k'.val := by rw [hkey']
            have e2 : (entries.val[pos.val]'hplt).1.val = k.val := by rw [hkey]
            have e3 : (entries.val[pos'.val]'hplt').1.val =
                (entries.val[pos.val]'hplt).1.val := by simp only [heq]
            omega
          have hplt'' : pos'.val < out.val.length := by omega
          have hout' : (out.val[pos'.val]'hplt'') = (entries.val[pos'.val]'hplt') := by
            revert hplt''
            rw [hrev]
            intro hplt''
            rw [List.getElem_set_ne (by omega)]
          rw [get_found out (Children.Cons c cs) k' hsout pos'.val hplt''
              (by rw [hout']; exact hkey'),
            get_found entries (Children.Cons c cs) k' hs pos'.val hplt' hkey']
          congr 1
          congr 1
          rw [hout']
        | false =>
          obtain ⟨hlt', hgt'⟩ := hmiss' rfl
          obtain ⟨hlt'', hgt''⟩ := bracket_of_keys_eq hlenout hkeys hlt' hgt'
          rw [get_descend' out (Children.Cons c cs) k' hsout pos'.val (by omega)
              hlt'' hgt'' (by rw [hlenout]; exact hlen1) (by scalar_tac),
            get_descend' entries (Children.Cons c cs) k' hs pos'.val hple'
              hlt' hgt' hlen1 (by scalar_tac)]
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
                maybe_split entries1 children replaced) ⦃ r =>
          GetR k' (get_in_node (Node.mk entries (Children.Cons c cs)) k') r ⦄
        obtain ⟨hlt, hgt⟩ := hmiss rfl
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
          InB_bnd_of_bracket entries.val pos.val k.val hple hk hlt hgt
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
        obtain ⟨ir, hir, hirp, hirf⟩ := Aeneas.Std.WP.spec_imp_exists hboth
        rw [hir]
        simp only [bind_tc_ok]
        cases ir with
        | Fit new_child replaced =>
          show (do
              let children ← replace_child (Children.Cons c cs) pos new_child
              let entries1 ← alloc.vec.CloneVec.clone (BuiltinClone (Std.U64 × Std.U64)) entries
              ok (InsertResult.Fit (Node.mk entries1 children) replaced)) ⦃ r =>
            GetR k' (get_in_node (Node.mk entries (Children.Cons c cs)) k') r ⦄
          obtain ⟨ch, hrc, hrcv⟩ := Aeneas.Std.WP.spec_imp_exists
            (replace_child_spec (Children.Cons c cs) pos new_child)
          obtain ⟨ent2, hcv, hcvv⟩ := Aeneas.Std.WP.spec_imp_exists
            (cloneVec_builtin_spec entries)
          rw [hrc, hcv]
          simp only [bind_tc_ok, Aeneas.Std.WP.spec_ok]
          rw [hcvv]
          show get_in_node (Node.mk entries ch) k' =
            get_in_node (Node.mk entries (Children.Cons c cs)) k'
          have hchl : (clist ch).length = entries.val.length + 1 := by
            rw [hrcv]
            simpa using hlen1
          cases hit' with
          | true =>
            obtain ⟨hplt', hkey'⟩ := hhit' rfl
            rw [get_found entries ch k' hs pos'.val hplt' hkey',
              get_found entries (Children.Cons c cs) k' hs pos'.val hplt' hkey']
          | false =>
            obtain ⟨hlt', hgt'⟩ := hmiss' rfl
            rw [get_descend' entries ch k' hs pos'.val hple' hlt' hgt' hchl
                (by scalar_tac),
              get_descend' entries (Children.Cons c cs) k' hs pos'.val hple'
                hlt' hgt' hlen1 (by scalar_tac)]
            by_cases hpp : pos'.val = pos.val
            · have hpe : pos' = pos := UScalar.eq_of_val_eq hpp
              subst hpe
              have h1 : (clist ch)[pos'.val]'(by rw [hchl]; omega) = new_child := by
                simp only [hrcv]
                exact List.getElem_set_self ..
              have h2 : (clist (Children.Cons c cs))[pos'.val]'(by rw [hlen1]; omega)
                  = n := by rw [hcav]
              rw [h1, h2]
              exact hirf
            · have h1 : (clist ch)[pos'.val]'(by rw [hchl]; omega) =
                  (clist (Children.Cons c cs))[pos'.val]'(by rw [hlen1]; omega) := by
                simp only [hrcv]
                rw [List.getElem_set_ne (by omega)]
              rw [h1]
        | Split l median r replaced =>
          show (do
              let entries1 ← insert_entry_at entries pos median
              let children ← replace_and_insert_child (Children.Cons c cs) pos l r
              maybe_split entries1 children replaced) ⦃ r =>
            GetR k' (get_in_node (Node.mk entries (Children.Cons c cs)) k') r ⦄
          obtain ⟨hgm, hgl, hgr⟩ := hirf
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
          have hentmax : ent.val.length < Std.Usize.max := by
            rw [hentl]
            scalar_tac
          have horig : (c :: clist cs) = clist (Children.Cons c cs) := by
            simp [clist]
          cases hit' with
          | true =>
            -- k' present in this node's entries: it moves to pos' or pos'+1
            obtain ⟨hplt', hkey'⟩ := hhit' rfl
            rw [get_found entries (Children.Cons c cs) k' hs pos'.val hplt' hkey']
            have hw : (entries.val[pos'.val]'hplt') =
                (k', (entries.val[pos'.val]'hplt').2) := by
              rw [Prod.ext_iff]
              exact ⟨hkey', rfl⟩
            rcases Nat.lt_or_ge pos'.val pos.val with hpp | hpp
            · have hj : pos'.val < ent.val.length := by omega
              have hkv2 : (ent.val[pos'.val]'hj) =
                  (k', (entries.val[pos'.val]'hplt').2) := by
                revert hj
                rw [hiav]
                intro hj
                rw [insertAt_getElem_lo entries.val pos.val median hple pos'.val
                  hplt' hpp]
                exact hw
              apply Aeneas.Std.WP.spec_mono (maybe_split_get ent ch replaced
                hsent pos'.val hj hkv2 (by omega) hchmax)
              exact fun r hr => GetR_of_GetPost hr
            · have hj : pos'.val + 1 < ent.val.length := by omega
              have hkv2 : (ent.val[pos'.val + 1]'hj) =
                  (k', (entries.val[pos'.val]'hplt').2) := by
                revert hj
                rw [hiav]
                intro hj
                rw [insertAt_getElem_hi entries.val pos.val median hple pos'.val
                  hplt' hpp]
                exact hw
              apply Aeneas.Std.WP.spec_mono (maybe_split_get ent ch replaced
                hsent (pos'.val + 1) hj hkv2 (by omega) hchmax)
              exact fun r hr => GetR_of_GetPost hr
          | false =>
            -- k' absent from this node: descend on the original, then track slots
            obtain ⟨hlt', hgt'⟩ := hmiss' rfl
            rw [get_descend' entries (Children.Cons c cs) k' hs pos'.val hple'
              hlt' hgt' hlen1 (by scalar_tac)]
            rcases Nat.lt_trichotomy pos'.val pos.val with hpp | hpp | hpp
            · -- slot strictly left of the split point
              have hkm : k'.val < median.1.val := by
                have h1 := hgt' pos'.val (by omega) (Nat.le_refl _)
                have h2 := hmlt pos'.val (by omega) hpp
                omega
              obtain ⟨hlt2, hgt2⟩ := bracket_insertAt_lo entries.val pos.val median
                hple pos'.val hple' (by omega) k'.val hkm hlt' hgt'
              have hlt3 : ∀ i, (h : i < ent.val.length) → i < pos'.val →
                  (ent.val[i]'h).1.val < k'.val := by
                intro i h hiq
                simp only [hiav]
                exact hlt2 i (by rw [← hiav]; exact h) hiq
              have hgt3 : ∀ i, (h : i < ent.val.length) → pos'.val ≤ i →
                  k'.val < (ent.val[i]'h).1.val := by
                intro i h hqi
                simp only [hiav]
                exact hgt2 i (by rw [← hiav]; exact h) hqi
              have hgv : get_in_node ((clist ch)[pos'.val]'(by
                  rw [hchl, hentl]; omega)) k' =
                  get_in_node ((clist (Children.Cons c cs))[pos'.val]'(by
                    rw [hlen1]; omega)) k' := by
                congr 1
                simp only [hriv, ← horig]
                exact surgery_getElem_lo (c :: clist cs) pos.val l r
                  (by rw [horig, hlen1]; omega) pos'.val
                  (by rw [horig, hlen1]; omega) hpp
              exact maybe_split_frame_descend ent ch replaced hsent pos'.val
                (by omega) hlt3 hgt3 hchl (by omega) hentmax hgv
            · -- same slot as the inserted child: use the IH's three-way clause
              have hpe : pos' = pos := UScalar.eq_of_val_eq hpp
              subst hpe
              have h2 : (clist (Children.Cons c cs))[pos'.val]'(by rw [hlen1]; omega)
                  = n := by rw [hcav]
              rw [h2]
              rcases Nat.lt_trichotomy k'.val median.1.val with hcmp | hcmp | hcmp
              · -- k' < median: descend into l
                obtain ⟨hlt2, hgt2⟩ := bracket_insertAt_lo entries.val pos'.val median
                  hple pos'.val hple' (Nat.le_refl _) k'.val hcmp hlt' hgt'
                have hlt3 : ∀ i, (h : i < ent.val.length) → i < pos'.val →
                    (ent.val[i]'h).1.val < k'.val := by
                  intro i h hiq
                  simp only [hiav]
                  exact hlt2 i (by rw [← hiav]; exact h) hiq
                have hgt3 : ∀ i, (h : i < ent.val.length) → pos'.val ≤ i →
                    k'.val < (ent.val[i]'h).1.val := by
                  intro i h hqi
                  simp only [hiav]
                  exact hgt2 i (by rw [← hiav]; exact h) hqi
                have hgv : get_in_node ((clist ch)[pos'.val]'(by
                    rw [hchl, hentl]; omega)) k' = get_in_node n k' := by
                  have h1 : (clist ch)[pos'.val]'(by rw [hchl, hentl]; omega) = l := by
                    simp only [hriv, ← horig]
                    exact surgery_getElem_l (c :: clist cs) pos'.val l r
                      (by rw [horig, hlen1]; omega)
                  rw [h1]
                  exact hgl hcmp
                exact maybe_split_frame_descend ent ch replaced hsent pos'.val
                  (by omega) hlt3 hgt3 hchl (by omega) hentmax hgv
              · -- k' equals the promoted median key
                have hmk : median.1 = k' := UScalar.eq_of_val_eq hcmp.symm
                rw [hgm hcmp.symm]
                have hj : pos'.val < ent.val.length := by omega
                have hkv2 : (ent.val[pos'.val]'hj) = (k', median.2) := by
                  revert hj
                  rw [hiav]
                  intro hj
                  rw [getElem_insertAt_self _ _ _ hple]
                  rw [Prod.ext_iff]
                  exact ⟨hmk, rfl⟩
                apply Aeneas.Std.WP.spec_mono (maybe_split_get ent ch replaced
                  hsent pos'.val hj hkv2 (by omega) hchmax)
                exact fun r hr => GetR_of_GetPost hr
              · -- median < k': descend into r
                obtain ⟨hlt2, hgt2⟩ := bracket_insertAt_hi entries.val pos'.val median
                  hple pos'.val hple' (Nat.le_refl _) k'.val hcmp hlt' hgt'
                have hlt3 : ∀ i, (h : i < ent.val.length) → i < pos'.val + 1 →
                    (ent.val[i]'h).1.val < k'.val := by
                  intro i h hiq
                  simp only [hiav]
                  exact hlt2 i (by rw [← hiav]; exact h) hiq
                have hgt3 : ∀ i, (h : i < ent.val.length) → pos'.val + 1 ≤ i →
                    k'.val < (ent.val[i]'h).1.val := by
                  intro i h hqi
                  simp only [hiav]
                  exact hgt2 i (by rw [← hiav]; exact h) hqi
                have hgv : get_in_node ((clist ch)[pos'.val + 1]'(by
                    rw [hchl, hentl]; omega)) k' = get_in_node n k' := by
                  have h1 : (clist ch)[pos'.val + 1]'(by rw [hchl, hentl]; omega)
                      = r := by
                    simp only [hriv, ← horig]
                    exact surgery_getElem_r (c :: clist cs) pos'.val l r
                      (by rw [horig, hlen1]; omega)
                  rw [h1]
                  exact hgr hcmp
                exact maybe_split_frame_descend ent ch replaced hsent (pos'.val + 1)
                  (by omega) hlt3 hgt3 hchl (by omega) hentmax hgv
            · -- slot strictly right of the split point
              have hkm : median.1.val < k'.val := by
                have h1 := hmgt pos.val (by omega) (Nat.le_refl _)
                have h2 := hlt' pos.val (by omega) hpp
                omega
              obtain ⟨hlt2, hgt2⟩ := bracket_insertAt_hi entries.val pos.val median
                hple pos'.val hple' (by omega) k'.val hkm hlt' hgt'
              have hlt3 : ∀ i, (h : i < ent.val.length) → i < pos'.val + 1 →
                  (ent.val[i]'h).1.val < k'.val := by
                intro i h hiq
                simp only [hiav]
                exact hlt2 i (by rw [← hiav]; exact h) hiq
              have hgt3 : ∀ i, (h : i < ent.val.length) → pos'.val + 1 ≤ i →
                  k'.val < (ent.val[i]'h).1.val := by
                intro i h hqi
                simp only [hiav]
                exact hgt2 i (by rw [← hiav]; exact h) hqi
              have hgv : get_in_node ((clist ch)[pos'.val + 1]'(by
                  rw [hchl, hentl]; omega)) k' =
                  get_in_node ((clist (Children.Cons c cs))[pos'.val]'(by
                    rw [hlen1]; omega)) k' := by
                congr 1
                simp only [hriv, ← horig]
                exact surgery_getElem_hi (c :: clist cs) pos.val l r
                  (by rw [horig, hlen1]; omega) pos'.val
                  (by rw [horig, hlen1]; omega) hpp
              exact maybe_split_frame_descend ent ch replaced hsent (pos'.val + 1)
                (by omega) hlt3 hgt3 hchl (by omega) hentmax hgv

/-- Top-level frame property: `insert k v` leaves `get k'` unchanged for
    every other key. -/
theorem BTree.insert_frame (t : BTree) (k v k' : Std.U64)
    (hinv : BTreeInv t) (hcap : t.len.val < Std.Usize.max)
    (hne : k'.val ≠ k.val) :
    BTree.insert t k v ⦃ t' => t'.get k' = t.get k' ⦄ := by
  rw [BTree.insert]
  have hk : InB none none k.val := ⟨by simp, by simp⟩
  have hboth := spec_and
    (insert_into_node_inv (Node.size t.root) t.root k v (Nat.le_refl _) hinv hk)
    (btree_kernel.insert_frame (Node.size t.root) t.root k v k' (Nat.le_refl _)
      hinv hk hne)
  obtain ⟨ir, hir, hirp, hirf⟩ := Aeneas.Std.WP.spec_imp_exists hboth
  rw [hir]
  simp only [bind_tc_ok]
  cases ir with
  | Fit new_root replaced =>
    show (if replaced then ok { t with root := new_root }
          else do
            let new_len ← t.len + 1#usize
            ok ({ root := new_root, len := new_len } : BTree)) ⦃ t' =>
      t'.get k' = t.get k' ⦄
    split
    · exact (Aeneas.Std.WP.spec_ok _).mpr (by rw [BTree.get, BTree.get]; exact hirf)
    · obtain ⟨nl, hadd, -⟩ := Aeneas.Std.WP.spec_imp_exists
        (Usize.add_spec (x := t.len) (y := 1#usize) (by scalar_tac))
      rw [hadd]
      simp only [bind_tc_ok]
      exact (Aeneas.Std.WP.spec_ok _).mpr (by rw [BTree.get, BTree.get]; exact hirf)
  | Split l median r replaced =>
    show (do
        let new_len ← if replaced then ok t.len else t.len + 1#usize
        let entries ← alloc.vec.Vec.push (alloc.vec.Vec.new (Std.U64 × Std.U64)) median
        ok ({ root := Node.mk entries
                (Children.Cons l (Children.Cons r Children.Nil)),
              len := new_len } : BTree)) ⦃ t' => t'.get k' = t.get k' ⦄
    obtain ⟨hgm, hgl, hgr⟩ := hirf
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
    rw [BTree.get, BTree.get]
    show get_in_node (Node.mk ents
      (Children.Cons l (Children.Cons r Children.Nil))) k' =
      get_in_node t.root k'
    have hse : SortedE ents.val := by rw [hev]; simp [SortedE]
    have hclr : (clist (Children.Cons l (Children.Cons r Children.Nil))).length =
        ents.val.length + 1 := by
      rw [hev]; simp [clist]
    rcases Nat.lt_trichotomy k'.val median.1.val with hcmp | hcmp | hcmp
    · rw [get_descend' ents _ k' hse 0 (by omega) (by omega)
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
    · have h0 : 0 < ents.val.length := by rw [hev]; simp
      have hp0 : (ents.val[0]'h0) = median :=
        getElem_of_getElem? _ _ _ _ (by rw [hev]; rfl)
      rw [get_found ents _ k' hse 0 h0
          (by rw [hp0]; exact UScalar.eq_of_val_eq hcmp.symm),
        show (ents.val[0]'h0).2 = median.2 by rw [hp0]]
      exact (hgm hcmp.symm).symm
    · rw [get_descend' ents _ k' hse 1 (by rw [hev]; simp)
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


