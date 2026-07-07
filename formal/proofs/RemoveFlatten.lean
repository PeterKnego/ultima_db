/- In-order `flatten` foundation for the delete path.

   `flatten` produces the in-order key/value list of a B-tree node.  The delete
   rebalancers (rotate / merge / fix) are in-order-preserving, so `get` can be
   characterized by lookup in the flattened list (`get_flatten`), and delete's
   effect on `get` reduces to erasing the unique `key` entry from the flattened
   list (`delete_from_node_flatten`).  These are the reusable primitives shared
   by `remove_get` (this development) and `remove_frame` (the frame agent). -/
import BtreeKernel
import BtreeInvariant
import ListLemmas
import AlignedLemmas
import ChildrenSpecs
import FindPosSpec
import EntrySpecs
import RemoveSpecs
import BalancedInvariant
import BtreeInsertInv
import BtreeInsertGet
import RemoveRebalance
import RemoveInv

open Aeneas Aeneas.Std Result

namespace btree_kernel

set_option maxRecDepth 4096

/-! ## The in-order `flatten` -/

mutual
/-- In-order key/value list of a node. -/
def flatten : Node → List (Std.U64 × Std.U64)
  | .mk es cs => flattenC es.val cs

/-- Interleave the still-unconsumed entries `es` with the children `cs`:
    `flatten c₀ ++ e₀ :: flatten c₁ ++ e₁ :: … :: flatten cₙ`. -/
def flattenC : List (Std.U64 × Std.U64) → Children →
    List (Std.U64 × Std.U64)
  | es, .Nil => es
  | [], .Cons c rest => flatten c ++ flattenC [] rest
  | e :: es', .Cons c rest => flatten c ++ e :: flattenC es' rest
end

/-- The children as a plain `List Node`, interleaved with entries `es`.  Pure
    engine (recurses on the `List Node`; uses the total `flatten` on each). -/
def flattenFam : List (Std.U64 × Std.U64) → List Node →
    List (Std.U64 × Std.U64)
  | es, [] => es
  | [], c :: cs => flatten c ++ flattenFam [] cs
  | e :: es', c :: cs => flatten c ++ e :: flattenFam es' cs

@[simp] theorem flatten_mk (es : alloc.vec.Vec (Std.U64 × Std.U64))
    (cs : Children) : flatten (Node.mk es cs) = flattenC es.val cs := by
  rw [flatten]

/-- `flattenC` over the generated cons-list equals `flattenFam` over `clist`. -/
theorem flattenC_eq_fam (es : List (Std.U64 × Std.U64)) (cs : Children) :
    flattenC es cs = flattenFam es (clist cs) := by
  induction cs using children_ind generalizing es with
  | nil => cases es <;> simp [flattenC, flattenFam, clist]
  | cons c rest ih =>
    cases es with
    | nil => simp only [flattenC, clist, flattenFam]; rw [ih]
    | cons e es' => simp only [flattenC, clist, flattenFam]; rw [ih]

/-- `flatten` in terms of the family engine over `clist`. -/
theorem flatten_eq_fam (es : alloc.vec.Vec (Std.U64 × Std.U64)) (cs : Children) :
    flatten (Node.mk es cs) = flattenFam es.val (clist cs) := by
  rw [flatten_mk, flattenC_eq_fam]

/-! ## Pure `flattenFam` lemmas -/

@[simp] theorem flattenFam_nil_children (es : List (Std.U64 × Std.U64)) :
    flattenFam es [] = es := by cases es <;> rfl

theorem flattenFam_cons_cons (e : Std.U64 × Std.U64)
    (es : List (Std.U64 × Std.U64)) (c : Node) (cs : List Node) :
    flattenFam (e :: es) (c :: cs) = flatten c ++ e :: flattenFam es cs := rfl

theorem flattenFam_nil_cons (c : Node) (cs : List Node) :
    flattenFam [] (c :: cs) = flatten c ++ flattenFam [] cs := rfl

/-- Split an aligned family at an explicit separator `e`: when the first block
    `C1` has exactly `|E1|+1` children it is a complete sub-family, so the
    interleaving factors as `flattenFam E1 C1 ++ e :: flattenFam E2 C2`. -/
theorem flattenFam_split (E1 E2 : List (Std.U64 × Std.U64)) (e : Std.U64 × Std.U64)
    (C1 C2 : List Node) (hlen : C1.length = E1.length + 1) :
    flattenFam (E1 ++ e :: E2) (C1 ++ C2)
      = flattenFam E1 C1 ++ e :: flattenFam E2 C2 := by
  induction E1 generalizing C1 with
  | nil =>
    match C1, hlen with
    | [c0], _ =>
      simp [flattenFam]
  | cons a E1' ih =>
    match C1, hlen with
    | c0 :: C1', hlen =>
      have hlen' : C1'.length = E1'.length + 1 := by
        simpa using hlen
      simp only [List.cons_append, flattenFam_cons_cons]
      rw [ih C1' hlen']
      simp [List.append_assoc]

/-- End-split: when there is no trailing separator (all entries consumed by the
    first block), the second block contributes only children. -/
theorem flattenFam_split_nil (E1 : List (Std.U64 × Std.U64))
    (C1 C2 : List Node) (hlen : C1.length = E1.length + 1) :
    flattenFam E1 (C1 ++ C2) = flattenFam E1 C1 ++ flattenFam [] C2 := by
  induction E1 generalizing C1 with
  | nil =>
    match C1, hlen with
    | [c0], _ =>
      simp [flattenFam]
  | cons a E1' ih =>
    match C1, hlen with
    | c0 :: C1', hlen =>
      have hlen' : C1'.length = E1'.length + 1 := by simpa using hlen
      simp only [List.cons_append, flattenFam_cons_cons]
      rw [ih C1' hlen']
      simp [List.append_assoc]

/-! ## `flatten` of a `NodeInv` tree is strictly sorted and in-bounds -/

/-- Leaf flatten is exactly the entries. -/
@[simp] theorem flatten_leaf (es : alloc.vec.Vec (Std.U64 × Std.U64)) :
    flatten (Node.mk es Children.Nil) = es.val := by
  simp [flatten_mk, flattenC]

/-- Internal flatten unfolds to the family form. -/
theorem flatten_internal (es : alloc.vec.Vec (Std.U64 × Std.U64))
    (c : Node) (cs : Children) :
    flatten (Node.mk es (Children.Cons c cs))
      = flattenFam es.val (c :: clist cs) := by
  rw [flatten_eq_fam]; simp [clist]

private theorem flatten_props_fuel (fuel : Nat) : ∀ (n : Node),
    Node.size n ≤ fuel → ∀ (lo hi : Option Nat), NodeInv lo hi n →
    SortedE (flatten n) ∧ (∀ p ∈ flatten n, InB lo hi p.1.val) := by
  induction fuel with
  | zero =>
    intro n hsz; exfalso; have := Node.one_le_size n; omega
  | succ fuel ih =>
    intro n hsz lo hi hinv
    cases hinv with
    | leaf entries hs hb hlen =>
      rw [flatten_leaf]; exact ⟨hs, hb⟩
    | internal entries c cs hs hb hlen hal =>
      rw [flatten_internal]
      -- generalized helper over the aligned family
      have hsz' : ∀ x ∈ (c :: clist cs), Node.size x ≤ fuel := by
        intro x hx
        have h2 := size_mem_clist (Children.Cons c cs) x (by simpa [clist] using hx)
        have h3 : Node.size (Node.mk entries (Children.Cons c cs)) =
            1 + Children.size (Children.Cons c cs) := by simp [Node.size]
        simp only [Node.size] at hsz; omega
      have key : ∀ (es : List (Std.U64 × Std.U64)) (csl : List Node) (lo0 : Option Nat),
          (∀ x ∈ csl, Node.size x ≤ fuel) →
          SortedE es → (∀ e ∈ es, InB lo0 hi e.1.val) →
          Aligned lo0 hi es csl →
          SortedE (flattenFam es csl)
          ∧ (∀ p ∈ flattenFam es csl, InB lo0 hi p.1.val) := by
        intro es
        induction es with
        | nil =>
          intro csl lo0 hszc _ _ hA
          cases hA with
          | last c0 hc0 =>
            have := ih c0 (hszc c0 (by simp)) lo0 hi hc0
            simpa [flattenFam] using this
        | cons e es' ihe =>
          intro csl lo0 hszc hse hbe hA
          cases hA with
          | step _ _ c0 cs0 hc0 htail =>
            -- child c0 : keys in (lo0, e)
            have hchild := ih c0 (hszc c0 (by simp)) lo0 (some e.1.val) hc0
            -- tail family : keys in (some e, hi)
            have hse' : SortedE es' := (List.pairwise_cons.mp hse).2
            have he_lt : ∀ f ∈ es', e.1.val < f.1.val :=
              (List.pairwise_cons.mp hse).1
            have hbe' : ∀ f ∈ es', InB (some e.1.val) hi f.1.val := by
              intro f hf
              exact ⟨fun l hl => by
                  simp only [Option.mem_def, Option.some.injEq] at hl; subst hl
                  exact he_lt f hf,
                (hbe f (by simp [hf])).2⟩
            have htail' := ihe cs0 (some e.1.val)
              (fun x hx => hszc x (by simp [hx])) hse' hbe' htail
            obtain ⟨hcs, hcb⟩ := hchild
            obtain ⟨hts, htb⟩ := htail'
            have hbe_e := hbe e (by simp)
            rw [flattenFam_cons_cons]
            constructor
            · -- SortedE (flatten c0 ++ e :: flattenFam es' cs0)
              show List.Pairwise (fun a b => a.1.val < b.1.val)
                (flatten c0 ++ e :: flattenFam es' cs0)
              rw [List.pairwise_append]
              refine ⟨hcs, ?_, ?_⟩
              · rw [List.pairwise_cons]
                refine ⟨fun b hb => (htb b hb).1 e.1.val (by simp), hts⟩
              · intro a ha b hb
                have hae : a.1.val < e.1.val := (hcb a ha).2 e.1.val (by simp)
                simp only [List.mem_cons] at hb
                rcases hb with rfl | hb
                · exact hae
                · have := (htb b hb).1 e.1.val (by simp); omega
            · -- bounds InB lo0 hi for every key
              intro p hp
              rw [List.mem_append] at hp
              rcases hp with hp | hp
              · refine ⟨(hcb p hp).1, fun h hh => ?_⟩
                have hpe : p.1.val < e.1.val := (hcb p hp).2 e.1.val (by simp)
                have heh : e.1.val < h := hbe_e.2 h hh
                omega
              · simp only [List.mem_cons] at hp
                rcases hp with rfl | hp
                · exact hbe_e
                · refine ⟨fun l hl => ?_, (htb p hp).2⟩
                  have hle : l < e.1.val := hbe_e.1 l hl
                  have hep : e.1.val < p.1.val := (htb p hp).1 e.1.val (by simp)
                  omega
      exact key entries.val (c :: clist cs) lo hsz' hs hb hal

/-- `flatten` of a valid subtree is strictly sorted by key. -/
theorem flatten_sorted {lo hi : Option Nat} {n : Node} (hinv : NodeInv lo hi n) :
    SortedE (flatten n) :=
  (flatten_props_fuel (Node.size n) n (Nat.le_refl _) lo hi hinv).1

/-- Every key of `flatten` of a valid subtree lies strictly in `(lo, hi)`. -/
theorem flatten_bounds {lo hi : Option Nat} {n : Node} (hinv : NodeInv lo hi n) :
    ∀ p ∈ flatten n, InB lo hi p.1.val :=
  (flatten_props_fuel (Node.size n) n (Nat.le_refl _) lo hi hinv).2

/-- Family-level version: an aligned family flattens to a sorted, in-bounds
    list (the separators' bounds come from `hb`). -/
theorem flattenFam_props {lo hi : Option Nat} :
    ∀ (es : List (Std.U64 × Std.U64)) (cs : List Node),
      SortedE es → (∀ e ∈ es, InB lo hi e.1.val) → Aligned lo hi es cs →
      SortedE (flattenFam es cs)
      ∧ (∀ p ∈ flattenFam es cs, InB lo hi p.1.val) := by
  intro es
  induction es generalizing lo with
  | nil =>
    intro cs _ _ hA
    cases hA with
    | last c0 hc0 =>
      exact ⟨by simpa [flattenFam] using flatten_sorted hc0,
             by simpa [flattenFam] using flatten_bounds hc0⟩
  | cons e es' ihe =>
    intro cs hse hbe hA
    cases hA with
    | step _ _ c0 cs0 hc0 htail =>
      have hcs := flatten_sorted hc0
      have hcb := flatten_bounds hc0
      have hse' : SortedE es' := (List.pairwise_cons.mp hse).2
      have he_lt : ∀ f ∈ es', e.1.val < f.1.val := (List.pairwise_cons.mp hse).1
      have hbe' : ∀ f ∈ es', InB (some e.1.val) hi f.1.val := by
        intro f hf
        exact ⟨fun l hl => by
            simp only [Option.mem_def, Option.some.injEq] at hl; subst hl
            exact he_lt f hf, (hbe f (by simp [hf])).2⟩
      obtain ⟨hts, htb⟩ := ihe cs0 hse' hbe' htail
      have hbe_e := hbe e (by simp)
      rw [flattenFam_cons_cons]
      refine ⟨?_, ?_⟩
      · show List.Pairwise (fun a b => a.1.val < b.1.val)
          (flatten c0 ++ e :: flattenFam es' cs0)
        rw [List.pairwise_append]
        refine ⟨hcs, ?_, ?_⟩
        · rw [List.pairwise_cons]
          exact ⟨fun b hb => (htb b hb).1 e.1.val (by simp), hts⟩
        · intro a ha b hb
          have hae : a.1.val < e.1.val := (hcb a ha).2 e.1.val (by simp)
          simp only [List.mem_cons] at hb
          rcases hb with rfl | hb
          · exact hae
          · have := (htb b hb).1 e.1.val (by simp); omega
      · intro p hp
        rw [List.mem_append] at hp
        rcases hp with hp | hp
        · refine ⟨(hcb p hp).1, fun h hh => ?_⟩
          have hpe : p.1.val < e.1.val := (hcb p hp).2 e.1.val (by simp)
          have heh : e.1.val < h := hbe_e.2 h hh
          omega
        · simp only [List.mem_cons] at hp
          rcases hp with rfl | hp
          · exact hbe_e
          · refine ⟨fun l hl => ?_, (htb p hp).2⟩
            have hle : l < e.1.val := hbe_e.1 l hl
            have hep : e.1.val < p.1.val := (htb p hp).1 e.1.val (by simp)
            omega

/-! ## `lookupKey`: scan the flattened list for a key -/

/-- Left-to-right lookup of `k` in a key/value list. -/
def lookupKey (k : Std.U64) : List (Std.U64 × Std.U64) → Option Std.U64
  | [] => none
  | p :: rest => if p.1.val = k.val then some p.2 else lookupKey k rest

@[simp] theorem lookupKey_nil (k : Std.U64) : lookupKey k [] = none := rfl

theorem lookupKey_cons (k : Std.U64) (p : Std.U64 × Std.U64)
    (rest : List (Std.U64 × Std.U64)) :
    lookupKey k (p :: rest)
      = if p.1.val = k.val then some p.2 else lookupKey k rest := rfl

theorem lookupKey_append (k : Std.U64) (A B : List (Std.U64 × Std.U64)) :
    lookupKey k (A ++ B)
      = (lookupKey k A).orElse (fun _ => lookupKey k B) := by
  induction A with
  | nil => simp [lookupKey]
  | cons p rest ih =>
    simp only [List.cons_append, lookupKey_cons]
    split
    · simp [Option.orElse]
    · rw [ih]

/-- If no key in the list equals `k`, lookup fails. -/
theorem lookupKey_none_of_all_ne (k : Std.U64) (l : List (Std.U64 × Std.U64))
    (h : ∀ p ∈ l, p.1.val ≠ k.val) : lookupKey k l = none := by
  induction l with
  | nil => rfl
  | cons p rest ih =>
    rw [lookupKey_cons, if_neg (h p (by simp))]
    exact ih (fun q hq => h q (by simp [hq]))

/-- A prefix whose keys are all `< k` contributes nothing to the lookup. -/
theorem lookupKey_append_lt (k : Std.U64) (A B : List (Std.U64 × Std.U64))
    (h : ∀ p ∈ A, p.1.val < k.val) :
    lookupKey k (A ++ B) = lookupKey k B := by
  rw [lookupKey_append, lookupKey_none_of_all_ne k A (fun p hp => by
    have := h p hp; omega)]
  rfl

/-- A suffix whose keys are all `≠ k` contributes nothing to the lookup. -/
theorem lookupKey_append_ne (k : Std.U64) (A B : List (Std.U64 × Std.U64))
    (h : ∀ p ∈ B, p.1.val ≠ k.val) :
    lookupKey k (A ++ B) = lookupKey k A := by
  rw [lookupKey_append, lookupKey_none_of_all_ne k B h]
  cases lookupKey k A <;> rfl

/-! ## Two more pure `flattenFam` lemmas -/

/-- Equal-length split: no separator between the two blocks. -/
theorem flattenFam_split_eq (E1 E2 : List (Std.U64 × Std.U64))
    (C1 C2 : List Node) (hlen : C1.length = E1.length) :
    flattenFam (E1 ++ E2) (C1 ++ C2) = flattenFam E1 C1 ++ flattenFam E2 C2 := by
  induction E1 generalizing C1 with
  | nil =>
    match C1, hlen with
    | [], _ => simp [flattenFam]
  | cons a E1' ih =>
    match C1, hlen with
    | c :: C1', hlen =>
      have hlen' : C1'.length = E1'.length := by simpa using hlen
      simp only [List.cons_append, flattenFam_cons_cons]
      rw [ih C1' hlen']
      simp [List.append_assoc]

/-- Every key of a family is `< m` when every entry and every child key is. -/
theorem flattenFam_lt_of_bound (m : Nat) :
    ∀ (cs : List Node) (es : List (Std.U64 × Std.U64)),
      (∀ e ∈ es, e.1.val < m) →
      (∀ c ∈ cs, ∀ q ∈ flatten c, q.1.val < m) →
      ∀ q ∈ flattenFam es cs, q.1.val < m := by
  intro cs
  induction cs with
  | nil =>
    intro es he _ q hq
    simp only [flattenFam_nil_children] at hq
    exact he q hq
  | cons c cs' ih =>
    intro es he hc q hq
    have hc_c : ∀ q ∈ flatten c, q.1.val < m := hc c (by simp)
    have hc' : ∀ x ∈ cs', ∀ q ∈ flatten x, q.1.val < m :=
      fun x hx => hc x (by simp [hx])
    cases es with
    | nil =>
      rw [flattenFam_nil_cons, List.mem_append] at hq
      rcases hq with h | h
      · exact hc_c q h
      · exact ih [] (by simp) hc' q h
    | cons e es' =>
      rw [flattenFam_cons_cons, List.mem_append] at hq
      rcases hq with h | h
      · exact hc_c q h
      · simp only [List.mem_cons] at h
        rcases h with rfl | h
        · exact he q (by simp)
        · exact ih es' (fun f hf => he f (by simp [hf])) hc' q h

/-! ## `get` is lookup in the flattened list -/

/-- `get_in_node` returns exactly the `flatten`-list lookup of the key. -/
theorem get_flatten (fuel : Nat) {lo hi : Option Nat} (node : Node) (k : Std.U64)
    (hfuel : Node.size node ≤ fuel) (hinv : NodeInv lo hi node) :
    get_in_node node k = ok (lookupKey k (flatten node)) := by
  induction fuel generalizing lo hi node with
  | zero => exfalso; have := Node.one_le_size node; omega
  | succ fuel ih =>
    cases hinv with
    | leaf entries hs hb hlen =>
      obtain ⟨⟨hit, pos⟩, hfp, hple, hhit, hmiss⟩ :=
        WP.spec_imp_exists (find_pos_spec entries k hs)
      cases hit with
      | true =>
        obtain ⟨hplt, hpkey⟩ := hhit rfl
        rw [get_found entries Children.Nil k hs pos.val hplt hpkey, flatten_leaf]
        have hkeqv : (entries.val[pos.val]'hplt).1.val = k.val := by rw [hpkey]
        have hdec : entries.val = entries.val.take pos.val
            ++ (entries.val[pos.val]'hplt) :: entries.val.drop (pos.val + 1) := by
          conv_lhs => rw [← List.take_append_drop pos.val entries.val]
          congr 1
          exact List.drop_eq_getElem_cons hplt
        conv_rhs => rw [hdec]
        rw [lookupKey_append_lt k _ _ (fun q hq => by
              have := mem_take_lt_median entries.val pos.val hplt hs q hq
              omega),
          lookupKey_cons, if_pos hkeqv]
      | false =>
        obtain ⟨hlt', hgt'⟩ := hmiss rfl
        have hnone : lookupKey k (flatten (Node.mk entries Children.Nil)) = none := by
          rw [flatten_leaf]
          exact lookupKey_none_of_all_ne k entries.val (fun p hp => by
            obtain ⟨i, hi, rfl⟩ := List.mem_iff_getElem.mp hp
            rcases Nat.lt_or_ge i pos.val with h | h
            · have := hlt' i hi h; omega
            · have := hgt' i hi h; omega)
        rw [hnone, get_in_node]
        simp only [Node.entries._simpLemma_, Node.children._simpLemma_]
        rw [hfp]
        simp only [bind_tc_ok]
        rw [children_len]
        simp
    | internal entries c cs hs hb hlen hal =>
      obtain ⟨⟨hit, pos⟩, hfp, hple, hhit, hmiss⟩ :=
        WP.spec_imp_exists (find_pos_spec entries k hs)
      have hcl : (clist (Children.Cons c cs)).length = entries.val.length + 1 := by
        simpa [clist] using hal.length_eq
      have hCLlen : (c :: clist cs).length = entries.val.length + 1 := by
        simpa [clist] using hcl
      have hsmall : entries.val.length < Std.Usize.max :=
        Nat.lt_of_le_of_lt hlen (by scalar_tac)
      cases hit with
      | true =>
        obtain ⟨hplt, hpkey⟩ := hhit rfl
        rw [get_found entries (Children.Cons c cs) k hs pos.val hplt hpkey,
          flatten_internal]
        have hkeqv : (entries.val[pos.val]'hplt).1.val = k.val := by rw [hpkey]
        -- decompose the family at the separator entries[pos]
        have hE : entries.val = entries.val.take pos.val
            ++ (entries.val[pos.val]'hplt) :: entries.val.drop (pos.val + 1) := by
          conv_lhs => rw [← List.take_append_drop pos.val entries.val]
          congr 1
          exact List.drop_eq_getElem_cons hplt
        have hClen : ((c :: clist cs).take (pos.val + 1)).length
            = (entries.val.take pos.val).length + 1 := by
          rw [List.length_take, List.length_take, hCLlen]; omega
        have hsplit : flattenFam entries.val (c :: clist cs)
            = flattenFam (entries.val.take pos.val) ((c :: clist cs).take (pos.val + 1))
              ++ (entries.val[pos.val]'hplt)
                :: flattenFam (entries.val.drop (pos.val + 1))
                     ((c :: clist cs).drop (pos.val + 1)) := by
          conv_lhs => rw [hE, ← List.take_append_drop (pos.val + 1) (c :: clist cs)]
          exact flattenFam_split _ _ _ _ _ hClen
        rw [hsplit]
        have hAl := (Aligned.split hal pos.val hplt).1
        have hAprops := flattenFam_props (entries.val.take pos.val)
          ((c :: clist cs).take (pos.val + 1)) (SortedE_take _ _ hs)
          (InB.of_take entries.val pos.val hplt hs hb) hAl
        rw [lookupKey_append_lt k _ _ (fun q hq => by
              have := (hAprops.2 q hq).2 (entries.val[pos.val]'hplt).1.val (by simp)
              omega),
          lookupKey_cons, if_pos hkeqv]
      | false =>
        obtain ⟨hlt', hgt'⟩ := hmiss rfl
        rw [get_descend' entries (Children.Cons c cs) k hs pos.val hple hlt' hgt'
          (by simpa using hcl) hsmall]
        simp only [clist]
        obtain ⟨hclt, hcinv⟩ := hal.child pos.val hple
        have hchsz : Node.size ((c :: clist cs)[pos.val]'hclt) ≤ fuel := by
          have hmem : (c :: clist cs)[pos.val]'hclt ∈ clist (Children.Cons c cs) := by
            simp only [clist]; exact List.getElem_mem _
          have h1 := size_mem_clist _ _ hmem
          have h2 : Node.size (Node.mk entries (Children.Cons c cs))
              = 1 + Children.size (Children.Cons c cs) := by simp [Node.size]
          simp only [Node.size] at hfuel; omega
        rw [ih ((c :: clist cs)[pos.val]'hclt) hchsz hcinv, flatten_internal]
        congr 1
        -- lookupKey over the whole family = lookupKey over child pos
        set child := (c :: clist cs)[pos.val]'hclt with hchild_def
        have hCLdrop : (c :: clist cs).drop pos.val
            = child :: (c :: clist cs).drop (pos.val + 1) := by
          rw [hchild_def]; exact List.drop_eq_getElem_cons hclt
        have hposle : pos.val ≤ (c :: clist cs).length := le_of_lt hclt
        have hClen0 : ((c :: clist cs).take pos.val).length
            = (entries.val.take pos.val).length := by
          rw [List.length_take, List.length_take, hCLlen]; omega
        have hMsplit : flattenFam entries.val (c :: clist cs)
            = flattenFam (entries.val.take pos.val) ((c :: clist cs).take pos.val)
              ++ flattenFam (entries.val.drop pos.val)
                   ((c :: clist cs).drop pos.val) := by
          conv_lhs => rw [← List.take_append_drop pos.val entries.val,
            ← List.take_append_drop pos.val (c :: clist cs)]
          exact flattenFam_split_eq _ _ _ _ hClen0
        -- P is < k
        have hP_lt : ∀ q ∈ flattenFam (entries.val.take pos.val)
            ((c :: clist cs).take pos.val), q.1.val < k.val := by
          apply flattenFam_lt_of_bound
          · intro e he
            obtain ⟨i, hi, rfl⟩ := List.mem_iff_getElem.mp he
            have hilen : i < entries.val.length := by
              rw [List.length_take] at hi; omega
            have hipos : i < pos.val := by rw [List.length_take] at hi; omega
            rw [List.getElem_take]
            exact hlt' i hilen hipos
          · intro x hx q hq
            obtain ⟨i, hidx, rfl⟩ := List.mem_iff_getElem.mp hx
            have hipos : i < pos.val := by rw [List.length_take] at hidx; omega
            have hilen : i < entries.val.length := lt_of_lt_of_le hipos hple
            obtain ⟨hclti, hcinvi⟩ := hal.child i (le_of_lt hilen)
            have hxeq : ((c :: clist cs).take pos.val)[i]'hidx = (c :: clist cs)[i]'hclti := by
              rw [List.getElem_take]
            rw [hxeq] at hq
            have hqb := (flatten_bounds hcinvi q hq).2 (entries.val[i]'hilen).1.val
              (by rw [rbnd_eq_some hi entries.val i hilen]; simp)
            have := hlt' i hilen hipos
            omega
        -- M = flatten child ++ Suf, Suf keys ≠ k
        have hMdec : ∃ Suf, flattenFam (entries.val.drop pos.val)
            ((c :: clist cs).drop pos.val) = flatten child ++ Suf
            ∧ ∀ q ∈ Suf, q.1.val ≠ k.val := by
          rw [hCLdrop]
          rcases Nat.lt_or_eq_of_le hple with hposlt | hposeq
          · -- pos < len
            have hdrop : entries.val.drop pos.val
                = (entries.val[pos.val]'hposlt) :: entries.val.drop (pos.val + 1) :=
              List.drop_eq_getElem_cons hposlt
            rw [hdrop, flattenFam_cons_cons]
            refine ⟨(entries.val[pos.val]'hposlt)
              :: flattenFam (entries.val.drop (pos.val + 1))
                   ((c :: clist cs).drop (pos.val + 1)), rfl, ?_⟩
            have hAr := (Aligned.split hal pos.val hposlt).2
            have hRprops := flattenFam_props (entries.val.drop (pos.val + 1))
              ((c :: clist cs).drop (pos.val + 1)) (SortedE_drop _ _ hs)
              (InB.of_drop entries.val pos.val hposlt hs hb) hAr
            intro q hq
            simp only [List.mem_cons] at hq
            rcases hq with rfl | hq
            · have := hgt' pos.val hposlt (le_refl _); omega
            · have := (hRprops.2 q hq).1 (entries.val[pos.val]'hposlt).1.val (by simp)
              have := hgt' pos.val hposlt (le_refl _)
              omega
          · -- pos = len
            have hle1 : entries.val.length ≤ pos.val := Nat.le_of_eq hposeq.symm
            have hle2 : (c :: clist cs).length ≤ pos.val + 1 := by
              rw [hCLlen]; exact Nat.succ_le_succ hle1
            have hdrop : entries.val.drop pos.val = [] :=
              List.drop_eq_nil_of_le hle1
            have hcldrop : (c :: clist cs).drop (pos.val + 1) = [] :=
              List.drop_eq_nil_of_le hle2
            rw [hdrop, flattenFam_nil_cons, hcldrop]
            exact ⟨flattenFam [] [], by simp [flattenFam], by simp [flattenFam]⟩
        obtain ⟨Suf, hMeq, hSuf⟩ := hMdec
        rw [hMsplit, hMeq, lookupKey_append_lt k _ _ hP_lt,
          lookupKey_append_ne k _ _ hSuf]

/-! ## Rebalancers preserve `flatten` (in-order surgery) -/

/-- Swapping the head child by a merged node `m` (whose flatten is
    `flatten x ++ sep :: flatten b`) reassociates around the separator. -/
theorem flattenFam_swap_head (es : List (Std.U64 × Std.U64)) (m x b : Node)
    (rest : List Node) (sep : Std.U64 × Std.U64)
    (hm : flatten m = flatten x ++ sep :: flatten b) :
    flattenFam es (m :: rest) = flatten x ++ sep :: flattenFam es (b :: rest) := by
  cases es with
  | nil =>
    rw [flattenFam_nil_cons, flattenFam_nil_cons, hm]; simp [List.append_assoc]
  | cons e es' =>
    rw [flattenFam_cons_cons, flattenFam_cons_cons, hm]; simp [List.append_assoc]

/-- A node's children list is either empty (leaf) or has one more than the
    entries (internal / aligned). -/
theorem NodeInv.children_dichotomy {lo hi : Option Nat} {n : Node}
    (hinv : NodeInv lo hi n) :
    clist n.children = []
    ∨ (clist n.children).length = n.entries.val.length + 1 := by
  cases hinv with
  | leaf entries hs hb hlen => left; rfl
  | internal entries c cs hs hb hlen hal => right; simpa [clist] using hal.length_eq

/-- Flatten of a merged node equals the left flatten, the separator, then the
    right flatten. -/
theorem flatten_merge (left right : Node) (sep : Std.U64 × Std.U64)
    (me : alloc.vec.Vec (Std.U64 × Std.U64)) (mc : Children)
    (hme : me.val = left.entries.val ++ sep :: right.entries.val)
    (hmc : clist mc = clist left.children ++ clist right.children)
    (hcase : (clist left.children).length = left.entries.val.length + 1
             ∨ (clist left.children = [] ∧ clist right.children = [])) :
    flatten (Node.mk me mc) = flatten left ++ sep :: flatten right := by
  obtain ⟨lE, lC⟩ := left
  obtain ⟨rE, rC⟩ := right
  simp only [Node.entries._simpLemma_, Node.children._simpLemma_] at hme hmc hcase
  rw [flatten_eq_fam, hme, hmc, flatten_eq_fam, flatten_eq_fam]
  rcases hcase with hlen | ⟨hlnil, hrnil⟩
  · exact flattenFam_split _ _ _ _ _ (by rw [hlen])
  · rw [hlnil, hrnil]; simp [flattenFam]

/-- The leaf/internal case split needed by `flatten_merge`, discharged from
    the two siblings sharing a height. -/
theorem flatten_merge_hcase {h : Nat} {lo1 hi1 lo2 hi2 : Option Nat}
    {left right : Node} (hlinv : NodeInv lo1 hi1 left) (hrinv : NodeInv lo2 hi2 right)
    (hlh : HeightInv h left) (hrh : HeightInv h right) :
    (clist left.children).length = left.entries.val.length + 1
    ∨ (clist left.children = [] ∧ clist right.children = []) := by
  cases h with
  | zero =>
    right
    obtain ⟨lE, lC⟩ := left; obtain ⟨rE, rC⟩ := right
    simp only [Node.children._simpLemma_]
    rw [HeightInv.zero_nil hlh, HeightInv.zero_nil hrh]
    exact ⟨by simp [clist], by simp [clist]⟩
  | succ h0 =>
    left
    rcases hlinv.children_dichotomy with hnil | hlen
    · exact absurd hnil (by
        obtain ⟨lE, lC⟩ := left
        simp only [Node.children._simpLemma_] at *
        exact HeightInv.children_ne_nil hlh)
    · exact hlen

/-- In-order merge surgery: erasing separator `idx` and replacing children
    `idx, idx+1` by one `merged` node preserves the family flatten. -/
theorem flattenFam_merge_eq (E : List (Std.U64 × Std.U64)) (CL : List Node)
    (idx : Nat) (merged : Node)
    (hlen : CL.length = E.length + 1) (hidx : idx < E.length)
    (hmerged : flatten merged = flatten (CL[idx]'(by omega))
      ++ (E[idx]'hidx) :: flatten (CL[idx + 1]'(by omega))) :
    flattenFam (E.eraseIdx idx) (CL.take idx ++ merged :: CL.drop (idx + 2))
      = flattenFam E CL := by
  have hidxCL : idx < CL.length := by omega
  have hidx1CL : idx + 1 < CL.length := by omega
  have hEtake : (E.take idx).length = idx := by rw [List.length_take]; omega
  have hCLtake : (CL.take idx).length = idx := by rw [List.length_take]; omega
  have hErase : E.eraseIdx idx = E.take idx ++ E.drop (idx + 1) :=
    List.eraseIdx_eq_take_drop_succ E idx
  rw [hErase, flattenFam_split_eq _ _ _ _ (by rw [hCLtake, hEtake])]
  conv_rhs => rw [← List.take_append_drop idx E, ← List.take_append_drop idx CL,
    flattenFam_split_eq _ _ _ _ (by rw [hCLtake, hEtake])]
  congr 1
  have hCLdropidx : CL.drop idx
      = (CL[idx]'hidxCL) :: (CL[idx + 1]'hidx1CL) :: CL.drop (idx + 2) := by
    rw [List.drop_eq_getElem_cons hidxCL, List.drop_eq_getElem_cons hidx1CL]
  have hEdropidx : E.drop idx = (E[idx]'hidx) :: E.drop (idx + 1) :=
    List.drop_eq_getElem_cons hidx
  rw [hCLdropidx, hEdropidx, flattenFam_cons_cons]
  exact flattenFam_swap_head (E.drop (idx + 1)) merged (CL[idx]'hidxCL)
    (CL[idx + 1]'hidx1CL) (CL.drop (idx + 2)) (E[idx]'hidx) hmerged

/-- Swap two adjacent children (and the separator between them) for a
    segment-equal pair. -/
theorem flattenFam_swap2 (es : List (Std.U64 × Std.U64)) (x1 x2 y1 y2 : Node)
    (rest : List Node) (e e' : Std.U64 × Std.U64)
    (hseg : flatten x1 ++ e :: flatten x2 = flatten y1 ++ e' :: flatten y2) :
    flattenFam (e :: es) (x1 :: x2 :: rest)
      = flattenFam (e' :: es) (y1 :: y2 :: rest) := by
  have key : ∀ (T : List (Std.U64 × Std.U64)),
      flatten x1 ++ e :: (flatten x2 ++ T) = flatten y1 ++ e' :: (flatten y2 ++ T) := by
    intro T
    have h1 : flatten x1 ++ e :: (flatten x2 ++ T)
        = (flatten x1 ++ e :: flatten x2) ++ T := by simp [List.append_assoc]
    rw [h1, hseg]; simp [List.append_assoc]
  cases es with
  | nil =>
    simp only [flattenFam_cons_cons, flattenFam_nil_cons]
    exact key (flattenFam [] rest)
  | cons e2 es' =>
    simp only [flattenFam_cons_cons]
    exact key (e2 :: flattenFam es' rest)

/-- `set` as take/cons/drop. -/
theorem list_set_eq_take_cons_drop {α : Type} (l : List α) (j : Nat) (a : α)
    (h : j < l.length) : l.set j a = l.take j ++ a :: l.drop (j + 1) := by
  induction l generalizing j with
  | nil => simp at h
  | cons x t ih =>
    cases j with
    | zero => simp
    | succ k =>
      simp only [List.set_cons_succ, List.take_succ_cons, List.cons_append,
        List.drop_succ_cons]
      rw [ih k (by simp only [List.length_cons] at h; omega)]

/-- Two adjacent `set`s as take/cons/cons/drop. -/
theorem list_set_set_adjacent {α : Type} (l : List α) (j : Nat) (a b : α)
    (h : j + 1 < l.length) :
    (l.set j a).set (j + 1) b = l.take j ++ a :: b :: l.drop (j + 2) := by
  induction l generalizing j with
  | nil => simp at h
  | cons x t ih =>
    cases j with
    | zero =>
      match t, h with
      | y :: t', _ => simp
    | succ k =>
      simp only [List.set_cons_succ, List.take_succ_cons, List.cons_append]
      have ht : (x :: t).drop (k + 1 + 2) = t.drop (k + 2) := by
        simp [List.drop_succ_cons]
      rw [ht, ih k (by simp only [List.length_cons] at h; omega)]

/-- In-order rotate surgery: replacing separator `j` and the two adjacent
    children by a segment-equal triple preserves the family flatten. -/
theorem flattenFam_rotate_eq (E : List (Std.U64 × Std.U64)) (CL : List Node)
    (j : Nat) (s' : Std.U64 × Std.U64) (L' R' : Node)
    (hlen : CL.length = E.length + 1) (hj : j < E.length)
    (hseg : flatten (CL[j]'(by omega)) ++ (E[j]'hj) :: flatten (CL[j + 1]'(by omega))
      = flatten L' ++ s' :: flatten R') :
    flattenFam (E.set j s') ((CL.set j L').set (j + 1) R') = flattenFam E CL := by
  have hjCL : j < CL.length := by omega
  have hj1CL : j + 1 < CL.length := by omega
  have hEtake : (E.take j).length = j := by rw [List.length_take]; omega
  have hCLtake : (CL.take j).length = j := by rw [List.length_take]; omega
  rw [list_set_eq_take_cons_drop E j s' hj,
    list_set_set_adjacent CL j L' R' hj1CL]
  rw [flattenFam_split_eq _ _ _ _ (by rw [hCLtake, hEtake])]
  conv_rhs => rw [← List.take_append_drop j E, ← List.take_append_drop j CL,
    flattenFam_split_eq _ _ _ _ (by rw [hCLtake, hEtake])]
  congr 1
  have hCLdropj : CL.drop j
      = (CL[j]'hjCL) :: (CL[j + 1]'hj1CL) :: CL.drop (j + 2) := by
    rw [List.drop_eq_getElem_cons hjCL, List.drop_eq_getElem_cons hj1CL]
  have hEdropj : E.drop j = (E[j]'hj) :: E.drop (j + 1) :=
    List.drop_eq_getElem_cons hj
  rw [hCLdropj, hEdropj]
  exact (flattenFam_swap2 (E.drop (j + 1)) (CL[j]'hjCL) (CL[j + 1]'hj1CL) L' R'
    (CL.drop (j + 2)) (E[j]'hj) s' hseg).symm

/-- A `set` at `j` followed by erasing `j+1`, as take/cons/drop. -/
theorem list_set_eraseIdx_adjacent {α : Type} (l : List α) (j : Nat) (a : α)
    (h : j + 1 < l.length) :
    (l.set j a).eraseIdx (j + 1) = l.take j ++ a :: l.drop (j + 2) := by
  induction l generalizing j with
  | nil => simp at h
  | cons x t ih =>
    cases j with
    | zero =>
      match t, h with
      | y :: t', _ => simp
    | succ k =>
      simp only [List.set_cons_succ, List.eraseIdx_cons_succ,
        List.take_succ_cons, List.cons_append]
      have ht : (x :: t).drop (k + 1 + 2) = t.drop (k + 2) := by
        simp [List.drop_succ_cons]
      rw [ht, ih k (by simp only [List.length_cons] at h; omega)]

/-- `merge_with_right` preserves the family flatten. -/
theorem merge_with_right_flatten {lo hi : Option Nat} {h : Nat}
    (entries : alloc.vec.Vec (Std.U64 × Std.U64)) (children : Children)
    (idx : Std.Usize)
    (hinv : NodeInv lo hi (Node.mk entries children))
    (hh : HeightInv (h + 1) (Node.mk entries children))
    (hidx : idx.val < entries.val.length) :
    merge_with_right entries children idx ⦃ out =>
      flattenFam out.1.val (clist out.2)
        = flattenFam entries.val (clist children) ⦄ := by
  obtain ⟨hs, hb, hlen, hal, hall, hclen⟩ := parent_facts entries children hinv hh
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
  obtain ⟨hlt0, hlinv0⟩ := hal.child idx.val (by omega)
  obtain ⟨hrt0, hrinv0⟩ := hal.child (idx.val + 1) (by omega)
  rw [← hleftv] at hlinv0
  rw [← hrightv] at hrinv0
  have hlh : HeightInv h left := by rw [hleftv]; exact hall.getElem _ hchlt
  have hrh : HeightInv h right := by rw [hrightv]; exact hall.getElem _ hchlt1
  have hll : left.entries.val.length ≤ 63 := hlinv0.entries_len_le
  have hrl : right.entries.val.length ≤ 63 := hrinv0.entries_len_le
  obtain ⟨me, hmee, hmev⟩ := WP.spec_imp_exists
    (merge_entries_spec left.entries separator right.entries (by scalar_tac))
  obtain ⟨mc, hmce, hmcv⟩ := WP.spec_imp_exists
    (concat_children_spec left.children right.children)
  obtain ⟨new_entries, hnee, hnev⟩ := WP.spec_imp_exists
    (remove_entry_at_spec entries idx)
  obtain ⟨children1, hch1, hch1v⟩ := WP.spec_imp_exists
    (replace_child_spec children idx (Node.mk me mc))
  obtain ⟨children2, hch2, hch2v⟩ := WP.spec_imp_exists
    (remove_child_at_spec children1 i)
  simp only [hiv] at hch2v
  rw [hch1v] at hch2v
  simp only [alloc.vec.Vec.index_slice_index, hsep, hleft, hiadd, hright,
    hmee, hmce, hnee, hch1, hch2, bind_tc_ok, WP.spec_ok]
  have hch2v' : clist children2 = (clist children).take idx.val
      ++ Node.mk me mc :: (clist children).drop (idx.val + 2) := by
    rw [hch2v]
    exact list_set_eraseIdx_adjacent (clist children) idx.val _ (by omega)
  have hmerged : flatten (Node.mk me mc)
      = flatten ((clist children)[idx.val]'hchlt)
        ++ (entries.val[idx.val]'hidx)
          :: flatten ((clist children)[idx.val + 1]'hchlt1) := by
    rw [flatten_merge left right separator me mc hmev hmcv
      (flatten_merge_hcase hlinv0 hrinv0 hlh hrh), hleftv, hrightv, hsepv]
  show flattenFam new_entries.val (clist children2)
      = flattenFam entries.val (clist children)
  rw [hnev, hch2v']
  exact flattenFam_merge_eq entries.val (clist children) idx.val (Node.mk me mc)
    hclen hidx hmerged

/-- `merge_with_left` preserves the family flatten. -/
theorem merge_with_left_flatten {lo hi : Option Nat} {h : Nat}
    (entries : alloc.vec.Vec (Std.U64 × Std.U64)) (children : Children)
    (idx : Std.Usize)
    (hinv : NodeInv lo hi (Node.mk entries children))
    (hh : HeightInv (h + 1) (Node.mk entries children))
    (hidx0 : 0 < idx.val) (hidx : idx.val ≤ entries.val.length) :
    merge_with_left entries children idx ⦃ out =>
      flattenFam out.1.val (clist out.2)
        = flattenFam entries.val (clist children) ⦄ := by
  obtain ⟨hs, hb, hlen, hal, hall, hclen⟩ := parent_facts entries children hinv hh
  have hj : idx.val - 1 < entries.val.length := by omega
  have hj1 : idx.val - 1 + 1 = idx.val := by omega
  have hchlt : idx.val < (clist children).length := by omega
  have hchlt' : idx.val - 1 < (clist children).length := by omega
  rw [merge_with_left]
  obtain ⟨i, hisub, hivp⟩ := WP.spec_imp_exists
    (Usize.sub_spec (x := idx) (y := 1#usize) (by scalar_tac))
  have hiv : i.val = idx.val - 1 := by scalar_tac
  obtain ⟨separator, hsep, hsepv⟩ := WP.spec_imp_exists
    (alloc.vec.Vec.index_usize_spec entries i (by scalar_tac))
  simp only [hiv] at hsepv
  obtain ⟨left, hleft, hleftv⟩ := WP.spec_imp_exists
    (child_at_spec children i (by omega))
  simp only [hiv] at hleftv
  obtain ⟨right, hright, hrightv⟩ := WP.spec_imp_exists
    (child_at_spec children idx hchlt)
  obtain ⟨hlt0, hlinv0⟩ := hal.child (idx.val - 1) (by omega)
  obtain ⟨hrt0, hrinv0⟩ := hal.child idx.val hidx
  rw [← hleftv] at hlinv0
  rw [← hrightv] at hrinv0
  have hlh : HeightInv h left := by rw [hleftv]; exact hall.getElem _ hchlt'
  have hrh : HeightInv h right := by rw [hrightv]; exact hall.getElem _ hchlt
  have hll : left.entries.val.length ≤ 63 := hlinv0.entries_len_le
  have hrl : right.entries.val.length ≤ 63 := hrinv0.entries_len_le
  obtain ⟨me, hmee, hmev⟩ := WP.spec_imp_exists
    (merge_entries_spec left.entries separator right.entries (by scalar_tac))
  obtain ⟨mc, hmce, hmcv⟩ := WP.spec_imp_exists
    (concat_children_spec left.children right.children)
  obtain ⟨new_entries, hnee, hnev⟩ := WP.spec_imp_exists
    (remove_entry_at_spec entries i)
  simp only [hiv] at hnev
  obtain ⟨children1, hch1, hch1v⟩ := WP.spec_imp_exists
    (replace_child_spec children i (Node.mk me mc))
  simp only [hiv] at hch1v
  obtain ⟨children2, hch2, hch2v⟩ := WP.spec_imp_exists
    (remove_child_at_spec children1 idx)
  rw [hch1v] at hch2v
  simp only [alloc.vec.Vec.index_slice_index, hisub, hsep, hleft, hright,
    hmee, hmce, hnee, hch1, hch2, bind_tc_ok, WP.spec_ok]
  have hch2v' : clist children2 = (clist children).take (idx.val - 1)
      ++ Node.mk me mc :: (clist children).drop (idx.val - 1 + 2) := by
    rw [hch2v, ← hj1]
    exact list_set_eraseIdx_adjacent (clist children) (idx.val - 1) _ (by omega)
  have hrightv' : right = (clist children)[idx.val - 1 + 1]'(by omega) := by
    rw [hrightv]; congr 1; omega
  have hmerged : flatten (Node.mk me mc)
      = flatten ((clist children)[idx.val - 1]'hchlt')
        ++ (entries.val[idx.val - 1]'hj)
          :: flatten ((clist children)[idx.val - 1 + 1]'(by omega)) := by
    rw [flatten_merge left right separator me mc hmev hmcv
      (flatten_merge_hcase hlinv0 hrinv0 hlh hrh), hleftv, hrightv', hsepv]
  show flattenFam new_entries.val (clist children2)
      = flattenFam entries.val (clist children)
  rw [hnev, hch2v']
  exact flattenFam_merge_eq entries.val (clist children) (idx.val - 1) (Node.mk me mc)
    hclen hj hmerged

/-! ### Child-level splits used by rotate -/

/-- Split a node's flatten at its last entry: the prefix is the flatten of the
    node minus its last entry and last child; then the last entry; then the
    flatten of the dropped last child. -/
theorem flatten_split_last (n : Node)
    (hdich : clist n.children = []
      ∨ (clist n.children).length = n.entries.val.length + 1)
    (hll : 0 < n.entries.val.length) :
    flatten n = flattenFam (n.entries.val.take (n.entries.val.length - 1))
        ((clist n.children).take ((clist n.children).length - 1))
      ++ (n.entries.val[n.entries.val.length - 1]'(by omega))
        :: flattenFam [] ((clist n.children).drop ((clist n.children).length - 1)) := by
  obtain ⟨e, cs⟩ := n
  simp only [Node.entries._simpLemma_, Node.children._simpLemma_] at hdich hll ⊢
  have hlast : e.val = e.val.take (e.val.length - 1)
      ++ (e.val[e.val.length - 1]'(by omega)) :: e.val.drop (e.val.length - 1 + 1) := by
    conv_lhs => rw [← List.take_append_drop (e.val.length - 1) e.val]
    congr 1
    exact List.drop_eq_getElem_cons (by omega)
  have hdropnil : e.val.drop (e.val.length - 1 + 1) = [] :=
    List.drop_eq_nil_of_le (by omega)
  rw [flatten_eq_fam]
  rcases hdich with hnil | hlen
  · rw [hnil]
    simp only [List.take_nil, List.drop_nil, List.length_nil, flattenFam_nil_children]
    conv_lhs => rw [hlast, hdropnil]
  · have hCLtake : ((clist cs).take ((clist cs).length - 1)).length
        = (e.val.take (e.val.length - 1)).length + 1 := by
      rw [List.length_take, List.length_take, hlen]; omega
    conv_lhs => rw [hlast, ← List.take_append_drop ((clist cs).length - 1) (clist cs)]
    rw [flattenFam_split _ _ _ _ _ hCLtake, hdropnil]

/-- Prepending entry `x` at the front (with a moved child block `moved` before
    the original children). -/
theorem flatten_prepend_entry (right : Node) (x : Std.U64 × Std.U64)
    (nre : alloc.vec.Vec (Std.U64 × Std.U64)) (moved : List Node) (nrc : Children)
    (hnre : nre.val = x :: right.entries.val)
    (hnrc : clist nrc = moved ++ clist right.children)
    (hcase : moved.length = 1 ∨ (moved = [] ∧ clist right.children = [])) :
    flatten (Node.mk nre nrc) = flattenFam [] moved ++ x :: flatten right := by
  obtain ⟨re, rc⟩ := right
  simp only [Node.entries._simpLemma_, Node.children._simpLemma_] at hnre hnrc hcase ⊢
  rw [flatten_eq_fam, hnre, hnrc, flatten_eq_fam]
  rcases hcase with h1 | ⟨hm0, hrc0⟩
  · obtain ⟨c, rfl⟩ : ∃ c, moved = [c] := by
      match moved, h1 with | [c], _ => exact ⟨c, rfl⟩
    simp only [List.singleton_append, flattenFam_cons_cons, flattenFam_nil_cons,
      flattenFam_nil_children, List.append_nil]
  · rw [hm0, hrc0]; simp [flattenFam]

/-- Split a node's flatten at its first entry. -/
theorem flatten_split_first (n : Node)
    (hdich : clist n.children = []
      ∨ (clist n.children).length = n.entries.val.length + 1)
    (hll : 0 < n.entries.val.length) :
    flatten n = flattenFam [] ((clist n.children).take 1)
      ++ (n.entries.val[0]'hll)
        :: flattenFam (n.entries.val.drop 1) ((clist n.children).drop 1) := by
  obtain ⟨e, cs⟩ := n
  simp only [Node.entries._simpLemma_, Node.children._simpLemma_] at hdich hll ⊢
  have hfirst : e.val = (e.val[0]'hll) :: e.val.drop 1 := by
    have := List.drop_eq_getElem_cons (l := e.val) (i := 0) hll
    simpa using this
  rw [flatten_eq_fam]
  rcases hdich with hnil | hlen
  · rw [hnil]
    simp only [List.take_nil, List.drop_nil, flattenFam_nil_children, List.nil_append]
    exact hfirst
  · have hpos : 0 < (clist cs).length := by rw [hlen]; omega
    have hcs0 : clist cs = (clist cs)[0]'hpos :: (clist cs).drop 1 := by
      have := List.drop_eq_getElem_cons (l := clist cs) (i := 0) hpos
      simpa using this
    have htake1 : (clist cs).take 1 = [(clist cs)[0]'hpos] := by
      conv_lhs => rw [hcs0]
      simp
    rw [htake1]
    conv_lhs => rw [hfirst, hcs0]
    rw [flattenFam_cons_cons]
    simp only [flattenFam_nil_cons, flattenFam_nil_children, List.append_nil]

/-- Appending entry `x` at the back (with a moved child block `moved` after the
    original children). -/
theorem flatten_append_entry (left : Node) (x : Std.U64 × Std.U64)
    (nle : alloc.vec.Vec (Std.U64 × Std.U64)) (moved : List Node) (nlc : Children)
    (hnle : nle.val = left.entries.val ++ [x])
    (hnlc : clist nlc = clist left.children ++ moved)
    (hcase : (clist left.children).length = left.entries.val.length + 1
             ∨ (clist left.children = [] ∧ moved = [])) :
    flatten (Node.mk nle nlc) = flatten left ++ x :: flattenFam [] moved := by
  obtain ⟨le, lc⟩ := left
  simp only [Node.entries._simpLemma_, Node.children._simpLemma_] at hnle hnlc hcase ⊢
  rw [flatten_eq_fam, hnle, hnlc, flatten_eq_fam]
  rcases hcase with hlen | ⟨hlnil, hm0⟩
  · rw [flattenFam_split le.val [] x (clist lc) moved (by rw [hlen])]
  · rw [hlnil, hm0]; simp [flattenFam]

/-- `rotate_right` preserves the family flatten. -/
theorem rotate_right_flatten {lo hi : Option Nat} {h : Nat}
    (entries : alloc.vec.Vec (Std.U64 × Std.U64)) (children : Children)
    (idx : Std.Usize)
    (hinv : NodeInv lo hi (Node.mk entries children))
    (hh : HeightInv (h + 1) (Node.mk entries children))
    (hidx0 : 0 < idx.val) (hidx : idx.val ≤ entries.val.length)
    (hlsz : ∀ n, (clist children)[idx.val - 1]? = some n →
      0 < n.entries.val.length) :
    rotate_right entries children idx ⦃ out =>
      flattenFam out.1.val (clist out.2)
        = flattenFam entries.val (clist children) ⦄ := by
  obtain ⟨hs, hb, hlen, hal, hall, hclen⟩ := parent_facts entries children hinv hh
  have hj : idx.val - 1 < entries.val.length := by omega
  have hj1 : idx.val - 1 + 1 = idx.val := by omega
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
  obtain ⟨hlt0, hlinv0⟩ := hal.child (idx.val - 1) (by omega)
  obtain ⟨hrt0, hrinv0⟩ := hal.child idx.val hidx
  rw [← hleftv] at hlinv0
  rw [← hrightv] at hrinv0
  have hlh : HeightInv h left := by rw [hleftv]; exact hall.getElem _ hchlt'
  have hrh : HeightInv h right := by rw [hrightv]; exact hall.getElem _ hchlt
  have hll0 : 0 < left.entries.val.length := by
    apply hlsz left; rw [List.getElem?_eq_getElem hchlt', hleftv]
  have hlle := hlinv0.entries_len_le
  have hrle := hrinv0.entries_len_le
  have hlcl := hlinv0.children_len_le
  obtain ⟨i1, hi1, hi1p⟩ := WP.spec_imp_exists
    (Usize.sub_spec (x := alloc.vec.Vec.len left.entries) (y := 1#usize)
      (by have := alloc.vec.Vec.len_val left.entries; scalar_tac))
  have hi1v : i1.val = left.entries.val.length - 1 := by
    have := alloc.vec.Vec.len_val left.entries; scalar_tac
  obtain ⟨stolen, hstol, hstolv⟩ := WP.spec_imp_exists
    (alloc.vec.Vec.index_usize_spec left.entries i1 (by scalar_tac))
  obtain ⟨separator, hsep, hsepv⟩ := WP.spec_imp_exists
    (alloc.vec.Vec.index_usize_spec entries i (by scalar_tac))
  simp only [hiv] at hsepv
  obtain ⟨new_entries, hne', hnev⟩ := WP.spec_imp_exists
    (replace_entry_spec entries i stolen)
  simp only [hiv] at hnev
  obtain ⟨nle, hnle', hnlev⟩ := WP.spec_imp_exists
    (entries_prefix_spec left.entries i1 (by scalar_tac))
  simp only [hi1v] at hnlev
  obtain ⟨nre, hnre', hnrev⟩ := WP.spec_imp_exists
    (insert_entry_at_spec right.entries 0#usize separator (by scalar_tac)
      (by scalar_tac))
  have hnrev' : nre.val = separator :: right.entries.val := by simpa using hnrev
  obtain ⟨nlc, hnlc', hnlcv⟩ := WP.spec_imp_exists
    (drop_last_child_spec left.children (by scalar_tac))
  obtain ⟨moved, hmv', hmvv⟩ := WP.spec_imp_exists
    (last_child_singleton_spec left.children (by scalar_tac))
  obtain ⟨nrc, hnrc', hnrcv⟩ := WP.spec_imp_exists
    (concat_children_spec moved right.children)
  obtain ⟨children1, hch1, hch1v⟩ := WP.spec_imp_exists
    (replace_child_spec children i (Node.mk nle nlc))
  simp only [hiv] at hch1v
  obtain ⟨children2, hch2, hch2v⟩ := WP.spec_imp_exists
    (replace_child_spec children1 idx (Node.mk nre nrc))
  rw [hch1v] at hch2v
  simp only [alloc.vec.Vec.index_slice_index, hisub, hleft, hright, hi1, hstol,
    hsep, hne', hnle', hnre', hnlc', hmv', hnrc', hch1, hch2, bind_tc_ok,
    WP.spec_ok]
  -- concrete facts
  have hll1 : left.entries.val.length - 1 < left.entries.val.length := by omega
  have hstolen? : left.entries.val[left.entries.val.length - 1]? = some stolen := by
    rw [← hi1v, List.getElem?_eq_getElem (by scalar_tac), hstolv]
  have hstol_eq : left.entries.val[left.entries.val.length - 1]'hll1 = stolen := by
    rw [List.getElem?_eq_getElem hll1] at hstolen?; simpa using hstolen?
  have hflat_left : flatten left
      = flatten (Node.mk nle nlc) ++ stolen :: flattenFam [] (clist moved) := by
    rw [flatten_split_last left hlinv0.children_dichotomy hll0, hstol_eq,
      flatten_eq_fam, hnlev, hnlcv, hmvv]
  have hcase_moved : (clist moved).length = 1
      ∨ (clist moved = [] ∧ clist right.children = []) := by
    rw [hmvv]
    rcases flatten_merge_hcase hlinv0 hrinv0 hlh hrh with hlenlc | ⟨hnil, hrcnil⟩
    · left; rw [List.length_drop, hlenlc]; omega
    · right; exact ⟨by rw [hnil]; simp, hrcnil⟩
  have hflat_right : flatten (Node.mk nre nrc)
      = flattenFam [] (clist moved) ++ separator :: flatten right :=
    flatten_prepend_entry right separator nre (clist moved) nrc hnrev' hnrcv hcase_moved
  have hseg : flatten ((clist children)[idx.val - 1]'(by omega))
      ++ (entries.val[idx.val - 1]'hj)
        :: flatten ((clist children)[idx.val - 1 + 1]'(by omega))
      = flatten (Node.mk nle nlc) ++ stolen :: flatten (Node.mk nre nrc) := by
    have hL : (clist children)[idx.val - 1]'(by omega) = left := hleftv.symm
    have hR : (clist children)[idx.val - 1 + 1]'(by omega) = right := by
      rw [hrightv]; congr 1
    have hE : entries.val[idx.val - 1]'hj = separator := hsepv.symm
    rw [hL, hR, hE, hflat_left, hflat_right]; simp [List.append_assoc]
  have hnev' : new_entries.val = entries.val.set (idx.val - 1) stolen := hnev
  have hch2v' : clist children2
      = ((clist children).set (idx.val - 1) (Node.mk nle nlc)).set
          (idx.val - 1 + 1) (Node.mk nre nrc) := by
    rw [hch2v]; rw [show idx.val - 1 + 1 = idx.val from hj1]
  show flattenFam new_entries.val (clist children2)
      = flattenFam entries.val (clist children)
  rw [hnev', hch2v']
  exact flattenFam_rotate_eq entries.val (clist children) (idx.val - 1) stolen
    (Node.mk nle nlc) (Node.mk nre nrc) hclen hj hseg

/-- `rotate_left` preserves the family flatten. -/
theorem rotate_left_flatten {lo hi : Option Nat} {h : Nat}
    (entries : alloc.vec.Vec (Std.U64 × Std.U64)) (children : Children)
    (idx : Std.Usize)
    (hinv : NodeInv lo hi (Node.mk entries children))
    (hh : HeightInv (h + 1) (Node.mk entries children))
    (hidx : idx.val < entries.val.length)
    (hrsz : ∀ n, (clist children)[idx.val + 1]? = some n →
      0 < n.entries.val.length) :
    rotate_left entries children idx ⦃ out =>
      flattenFam out.1.val (clist out.2)
        = flattenFam entries.val (clist children) ⦄ := by
  obtain ⟨hs, hb, hlen, hal, hall, hclen⟩ := parent_facts entries children hinv hh
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
  obtain ⟨hlt0, hlinv0⟩ := hal.child idx.val (by omega)
  obtain ⟨hrt0, hrinv0⟩ := hal.child (idx.val + 1) (by omega)
  rw [← hleftv] at hlinv0
  rw [← hrightv] at hrinv0
  have hlh : HeightInv h left := by rw [hleftv]; exact hall.getElem _ hchlt
  have hrh : HeightInv h right := by rw [hrightv]; exact hall.getElem _ hchlt1
  have hrl0 : 0 < right.entries.val.length := by
    apply hrsz right; rw [List.getElem?_eq_getElem hchlt1, hrightv]
  have hlle := hlinv0.entries_len_le
  obtain ⟨stolen, hstol, hstolv⟩ := WP.spec_imp_exists
    (alloc.vec.Vec.index_usize_spec right.entries 0#usize (by scalar_tac))
  obtain ⟨separator, hsep, hsepv⟩ := WP.spec_imp_exists
    (alloc.vec.Vec.index_usize_spec entries idx (by scalar_tac))
  obtain ⟨new_entries, hnee, hnev⟩ := WP.spec_imp_exists
    (replace_entry_spec entries idx stolen)
  obtain ⟨nre, hnree, hnrev⟩ := WP.spec_imp_exists
    (entries_suffix_spec right.entries 1#usize)
  have hnrev' : nre.val = right.entries.val.drop 1 := by simpa using hnrev
  obtain ⟨nle, hnlee, hnlev⟩ := WP.spec_imp_exists
    (insert_entry_at_spec left.entries (alloc.vec.Vec.len left.entries) separator
      (by have := alloc.vec.Vec.len_val left.entries; scalar_tac) (by scalar_tac))
  have hnlev' : nle.val = left.entries.val ++ [separator] := by
    rw [hnlev]; simp [alloc.vec.Vec.len_val]
  obtain ⟨moved, hmve, hmvv⟩ := WP.spec_imp_exists
    (children_prefix_spec right.children 1#usize)
  have hmvv' : clist moved = (clist right.children).take 1 := by simpa using hmvv
  obtain ⟨nlc, hnlce, hnlcv⟩ := WP.spec_imp_exists
    (concat_children_spec left.children moved)
  obtain ⟨nrc, hnrce, hnrcv⟩ := WP.spec_imp_exists
    (children_suffix_spec right.children 1#usize)
  have hnrcv' : clist nrc = (clist right.children).drop 1 := by simpa using hnrcv
  obtain ⟨children1, hch1, hch1v⟩ := WP.spec_imp_exists
    (replace_child_spec children idx (Node.mk nle nlc))
  obtain ⟨children2, hch2, hch2v⟩ := WP.spec_imp_exists
    (replace_child_spec children1 i (Node.mk nre nrc))
  simp only [hiv] at hch2v
  rw [hch1v] at hch2v
  simp only [alloc.vec.Vec.index_slice_index, hleft, hiadd, hright, hstol,
    hsep, hnee, hnree, hnlee, hmve, hnlce, hnrce, hch1, hch2, bind_tc_ok,
    WP.spec_ok]
  have hstolen? : right.entries.val[0]? = some stolen := by
    rw [List.getElem?_eq_getElem hrl0, hstolv]; simp
  have hstol_eq : right.entries.val[0]'hrl0 = stolen := by
    rw [List.getElem?_eq_getElem hrl0] at hstolen?; simpa using hstolen?
  have hcase_l : (clist left.children).length = left.entries.val.length + 1
      ∨ (clist left.children = [] ∧ clist moved = []) := by
    rcases flatten_merge_hcase hlinv0 hrinv0 hlh hrh with hlenlc | ⟨hlnil, hrcnil⟩
    · left; exact hlenlc
    · right; exact ⟨hlnil, by rw [hmvv', hrcnil]; simp⟩
  have hflat_left : flatten (Node.mk nle nlc)
      = flatten left ++ separator :: flattenFam [] (clist moved) :=
    flatten_append_entry left separator nle (clist moved) nlc hnlev' hnlcv hcase_l
  have hflat_right : flatten right
      = flattenFam [] (clist moved) ++ stolen :: flatten (Node.mk nre nrc) := by
    rw [flatten_split_first right hrinv0.children_dichotomy hrl0, hstol_eq,
      hmvv', flatten_eq_fam, hnrev', hnrcv']
  have hseg : flatten ((clist children)[idx.val]'(by omega))
      ++ (entries.val[idx.val]'hidx)
        :: flatten ((clist children)[idx.val + 1]'(by omega))
      = flatten (Node.mk nle nlc) ++ stolen :: flatten (Node.mk nre nrc) := by
    have hL : (clist children)[idx.val]'(by omega) = left := hleftv.symm
    have hR : (clist children)[idx.val + 1]'(by omega) = right := hrightv.symm
    have hE : entries.val[idx.val]'hidx = separator := hsepv.symm
    rw [hL, hR, hE, hflat_left, hflat_right]; simp [List.append_assoc]
  have hnev' : new_entries.val = entries.val.set idx.val stolen := hnev
  have hch2v' : clist children2
      = ((clist children).set idx.val (Node.mk nle nlc)).set
          (idx.val + 1) (Node.mk nre nrc) := by rw [hch2v]
  show flattenFam new_entries.val (clist children2)
      = flattenFam entries.val (clist children)
  rw [hnev', hch2v']
  exact flattenFam_rotate_eq entries.val (clist children) idx.val stolen
    (Node.mk nle nlc) (Node.mk nre nrc) hclen hidx hseg

/-- `fix_underfull_child` preserves the family flatten. -/
theorem fix_underfull_child_flatten {lo hi : Option Nat} {h : Nat}
    (entries : alloc.vec.Vec (Std.U64 × Std.U64)) (children : Children)
    (idx : Std.Usize)
    (hinv : NodeInv lo hi (Node.mk entries children))
    (hh : HeightInv (h + 1) (Node.mk entries children))
    (hnz : 0 < entries.val.length)
    (hidx : idx.val ≤ entries.val.length) :
    fix_underfull_child entries children idx ⦃ out =>
      flattenFam out.1.val (clist out.2)
        = flattenFam entries.val (clist children) ⦄ := by
  obtain ⟨hs, hb, hlen, hal, hall, hclen⟩ := parent_facts entries children hinv hh
  have hchlt : idx.val < (clist children).length := by omega
  rw [fix_underfull_child]
  obtain ⟨n, hne, hnv⟩ := WP.spec_imp_exists
    (children_len_spec children (by scalar_tac))
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
    obtain ⟨i2, hmk, hmkv⟩ := WP.spec_imp_exists MIN_KEYS_spec
    simp only [hisub, hn1e, hmk, bind_tc_ok]
    split
    · rename_i hgt
      have hn1len : 0 < n1.entries.val.length := by
        have := alloc.vec.Vec.len_val n1.entries; scalar_tac
      have hlsz : ∀ m, (clist children)[idx.val - 1]? = some m → 0 < m.entries.val.length := by
        intro m hm
        rw [List.getElem?_eq_getElem (show idx.val - 1 < (clist children).length by omega)] at hm
        have hmn : m = n1 := by rw [hn1v]; exact (Option.some.inj hm).symm
        rw [hmn]; exact hn1len
      exact rotate_right_flatten entries children idx hinv hh hidx0 hidx hlsz
    · obtain ⟨i3, hi3a, hi3p⟩ := WP.spec_imp_exists
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
          have hn2len : 0 < n2.entries.val.length := by
            have := alloc.vec.Vec.len_val n2.entries; scalar_tac
          have hrsz : ∀ m, (clist children)[idx.val + 1]? = some m → 0 < m.entries.val.length := by
            intro m hm
            rw [List.getElem?_eq_getElem (show idx.val + 1 < (clist children).length by omega)] at hm
            have hmn : m = n2 := by rw [hn2v]; exact (Option.some.inj hm).symm
            rw [hmn]; exact hn2len
          exact rotate_left_flatten entries children idx hinv hh hidxlt hrsz
        · exact merge_with_left_flatten entries children idx hinv hh hidx0 hidx
      · exact merge_with_left_flatten entries children idx hinv hh hidx0 hidx
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
      obtain ⟨i2, hmk, hmkv⟩ := WP.spec_imp_exists MIN_KEYS_spec
      simp only [hn1e, hmk, bind_tc_ok]
      split
      · rename_i hgt
        have hn1len : 0 < n1.entries.val.length := by
          have := alloc.vec.Vec.len_val n1.entries; scalar_tac
        have hrsz : ∀ m, (clist children)[idx.val + 1]? = some m → 0 < m.entries.val.length := by
          intro m hm
          rw [List.getElem?_eq_getElem (show idx.val + 1 < (clist children).length by omega)] at hm
          have hmn : m = n1 := by rw [hn1v]; exact (Option.some.inj hm).symm
          rw [hmn]; exact hn1len
        exact rotate_left_flatten entries children idx hinv hh hidxlt hrsz
      · exact merge_with_right_flatten entries children idx hinv hh hidxlt
    · rename_i hge
      exfalso; scalar_tac

/-- `maybe_fix` preserves the family flatten (identity when not underfull). -/
theorem maybe_fix_flatten {lo hi : Option Nat} {h : Nat}
    (entries : alloc.vec.Vec (Std.U64 × Std.U64)) (children : Children)
    (idx : Std.Usize) (underfull : Bool)
    (hinv : NodeInv lo hi (Node.mk entries children))
    (hh : HeightInv (h + 1) (Node.mk entries children))
    (hnz : underfull = true → 0 < entries.val.length)
    (hidx : underfull = true → idx.val ≤ entries.val.length) :
    maybe_fix entries children idx underfull ⦃ out =>
      flattenFam out.1.val (clist out.2)
        = flattenFam entries.val (clist children) ⦄ := by
  rw [maybe_fix]
  split
  · rename_i hu
    exact fix_underfull_child_flatten entries children idx hinv hh (hnz hu) (hidx hu)
  · exact (WP.spec_ok _).mpr rfl

/-! ## `remove_leftmost` and `delete_from_node` flatten -/

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

private theorem index_usize_ok_inv {α : Type} {v : alloc.vec.Vec α}
    {i : Std.Usize} {x : α}
    (h : alloc.vec.Vec.index_usize v i = ok x) : v.val[i.val]? = some x := by
  unfold alloc.vec.Vec.index_usize at h
  split at h
  · exact absurd h (by simp)
  · rename_i y hy; cases h; simpa using hy

private theorem bounds_strengthen_lo {lo hi : Option Nat}
    (es : List (Std.U64 × Std.U64)) (hs : SortedE es)
    (hb : ∀ e ∈ es, InB lo hi e.1.val) (m : Nat)
    (hm : ∀ h0 : 0 < es.length, m < (es[0]'h0).1.val) :
    ∀ e ∈ es, InB (some m) hi e.1.val := by
  have hs' := List.pairwise_iff_getElem.mp hs
  intro e he
  obtain ⟨j, hj, rfl⟩ := List.mem_iff_getElem.mp he
  refine ⟨fun l hl => ?_, (hb _ (es.getElem_mem hj)).2⟩
  simp only [Option.mem_def, Option.some.injEq] at hl
  subst hl
  have h0 : 0 < es.length := by omega
  have hm0 := hm h0
  cases j with
  | zero => exact hm0
  | succ j' => have := hs' 0 (j' + 1) h0 hj (by omega); omega

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

/-- Pull the head child's flatten out of the family (used when the leftmost
    child is rebuilt). -/
theorem flattenFam_head_cons (es : List (Std.U64 × Std.U64)) (c nfc : Node)
    (rest : List Node) (entry : Std.U64 × Std.U64)
    (hc : flatten c = entry :: flatten nfc) :
    flattenFam es (c :: rest) = entry :: flattenFam es (nfc :: rest) := by
  cases es with
  | nil => rw [flattenFam_nil_cons, flattenFam_nil_cons, hc]; rfl
  | cons e es' => rw [flattenFam_cons_cons, flattenFam_cons_cons, hc]; rfl

/-- `remove_leftmost` peels the minimum entry off the front of the flattened
    list. Conditional on success (mirrors `remove_leftmost_spec`). -/
theorem remove_leftmost_flatten (fuel : Nat) {lo hi : Option Nat} {h : Nat}
    (node : Node) (e : Std.U64 × Std.U64) (n' : Node) (uf : Bool)
    (hfuel : Node.size node ≤ fuel)
    (hinv : NodeInv lo hi node) (hh : HeightInv h node)
    (hok : remove_leftmost node = ok (e, n', uf)) :
    flatten node = e :: flatten n' := by
  induction fuel generalizing lo hi h node e n' uf with
  | zero => exfalso; have := Node.one_le_size node; omega
  | succ fuel ih =>
    cases hinv with
    | leaf entries hs hb hlen =>
      cases hh with
      | leaf _ =>
        rw [remove_leftmost] at hok
        simp only [Node.entries._simpLemma_, Node.children._simpLemma_] at hok
        obtain ⟨i, hi', hok⟩ := bind_ok_inv hok
        have hilen : i.val = (clist Children.Nil).length :=
          spec_elim (children_len_spec Children.Nil (by simp [clist])) hi'
        simp only [clist, List.length_nil] at hilen
        rw [if_pos (by scalar_tac : i = 0#usize)] at hok
        simp only [alloc.vec.Vec.index_slice_index] at hok
        obtain ⟨first, hfirst, hok⟩ := bind_ok_inv hok
        have hfi := index_usize_ok_inv hfirst
        rw [(rfl : (0#usize).val = 0)] at hfi
        have hne : entries.val ≠ [] := by
          intro hnil; rw [hnil] at hfi; simp at hfi
        obtain ⟨a, t, hcons⟩ := List.exists_cons_of_ne_nil hne
        rw [hcons] at hfi
        simp at hfi
        obtain ⟨new_entries, hre, hok⟩ := bind_ok_inv hok
        have hrev : new_entries.val = entries.val.eraseIdx (0#usize).val :=
          spec_elim (remove_entry_at_spec entries 0#usize) hre
        obtain ⟨i2, hmk, hok⟩ := bind_ok_inv hok
        simp only [ok.injEq, Prod.mk.injEq] at hok
        obtain ⟨hfe, hnn, -⟩ := hok
        subst hnn
        have hnew : new_entries.val = t := by
          rw [hrev, hcons]; simp
        have hae : a = e := hfi.trans hfe
        rw [flatten_leaf, flatten_leaf, hcons, hnew, hae]
    | internal entries c cs hs hb hlen hal =>
      cases hh with
      | @internal h0 e2 c2 cs2 hch =>
        rw [remove_leftmost] at hok
        simp only [Node.entries._simpLemma_, Node.children._simpLemma_] at hok
        have hclen : (clist (Children.Cons c cs)).length = entries.val.length + 1 := by
          have := hal.length_eq; simpa [clist] using this
        obtain ⟨i, hi', hok⟩ := bind_ok_inv hok
        have hilen : i.val = (clist (Children.Cons c cs)).length :=
          spec_elim (children_len_spec _ (by rw [hclen]; scalar_tac)) hi'
        rw [hclen] at hilen
        rw [if_neg (by scalar_tac : ¬ (i = 0#usize))] at hok
        obtain ⟨n, hn, hok⟩ := bind_ok_inv hok
        have hn_eq : n = (clist (Children.Cons c cs))[(0#usize).val]'(by
            rw [hclen]; scalar_tac) :=
          spec_elim (child_at_spec _ 0#usize (by rw [hclen]; scalar_tac)) hn
        have hnc : n = c := by rw [hn_eq]; simp [clist]
        obtain ⟨hc0lt, hc0inv⟩ := hal.child 0 (Nat.zero_le _)
        simp only [lbnd, List.getElem_cons_zero, ↓reduceIte] at hc0inv
        have hc_h : HeightInv h0 c := (hch.allH) c (by simp [clist])
        have hsz : Node.size c ≤ fuel := by
          have hmem : c ∈ clist (Children.Cons c cs) := by simp [clist]
          have h1 := size_mem_clist _ _ hmem
          have h2 : Node.size (Node.mk entries (Children.Cons c cs)) =
              1 + Children.size (Children.Cons c cs) := by simp [Node.size]
          simp only [Node.size] at hfuel; omega
        obtain ⟨⟨entry, new_first_child, child_underfull⟩, hrl, hok⟩ := bind_ok_inv hok
        have hrl' : remove_leftmost c = ok (entry, new_first_child, child_underfull) := by
          rw [← hnc]; exact hrl
        have hIH : flatten c = entry :: flatten new_first_child :=
          ih c entry new_first_child child_underfull hsz hc0inv hc_h hrl'
        -- reuse the invariant spec to get NodeInv/HeightInv facts
        obtain ⟨hInB_e, hNI_nfc, hHI_nfc, huf_nfc⟩ :=
          remove_leftmost_spec (Node.size c) c entry new_first_child child_underfull
            (le_refl _) hc0inv hc_h hrl'
        obtain ⟨entries0, hcl0, hok⟩ := bind_ok_inv hok
        have he0 : entries0 = entries := spec_elim (cloneVec_builtin_spec entries) hcl0
        obtain ⟨children0, hrc0, hok⟩ := bind_ok_inv hok
        have hc0 : clist children0
            = (clist (Children.Cons c cs)).set (0#usize).val new_first_child :=
          spec_elim (replace_child_spec (Children.Cons c cs) 0#usize new_first_child) hrc0
        have hc0' : clist children0 = new_first_child :: clist cs := by
          rw [hc0]; simp [clist]
        obtain ⟨⟨entries1, children1⟩, hmf, hok⟩ := bind_ok_inv hok
        obtain ⟨i2, hmk, hok⟩ := bind_ok_inv hok
        simp only [ok.injEq, Prod.mk.injEq] at hok
        obtain ⟨he, hnn, -⟩ := hok
        subst he; subst hnn
        -- rebuild parent invariants at (some entry, hi) for maybe_fix_flatten
        have hal_new : Aligned (some entry.1.val) hi entries.val (new_first_child :: clist cs) :=
          Aligned.set_head hal new_first_child hNI_nfc
        have hb_new : ∀ f ∈ entries.val, InB (some entry.1.val) hi f.1.val := by
          apply bounds_strengthen_lo entries.val hs hb
          intro h0len
          have hrb := rbnd_eq_some hi entries.val 0 h0len
          exact hInB_e.2 _ (by rw [hrb]; simp)
        have hNI0 : NodeInv (some entry.1.val) hi (Node.mk entries0 children0) := by
          rw [he0]
          refine NodeInv.mk_node entries children0 hs hb_new hlen (Or.inr ?_)
          rw [hc0']; exact hal_new
        have hHI0 : HeightInv (h0 + 1) (Node.mk entries0 children0) := by
          rw [he0]
          refine HeightInv.mk_internal entries children0 ?_ ?_
          · rw [hc0']; simp
          · rw [hc0]; exact AllH.set _ hch.allH hHI_nfc
        have hmf_flat := maybe_fix_flatten entries0 children0 0#usize child_underfull
          hNI0 hHI0 (fun hu => maybe_fix_ok_pos (hu ▸ hmf))
          (fun _ => by scalar_tac)
        have hmf_eq : flattenFam entries1.val (clist children1)
            = flattenFam entries0.val (clist children0) := spec_elim hmf_flat hmf
        -- assemble
        rw [flatten_internal, flatten_eq_fam]
        rw [flattenFam_head_cons entries.val c new_first_child (clist cs) entry hIH]
        congr 1
        rw [hmf_eq, he0, hc0']

/-- Every key of a family is `≠ k` when every entry and every child key is. -/
theorem flattenFam_ne_of_bound (k : Nat) :
    ∀ (cs : List Node) (es : List (Std.U64 × Std.U64)),
      (∀ e ∈ es, e.1.val ≠ k) →
      (∀ c ∈ cs, ∀ q ∈ flatten c, q.1.val ≠ k) →
      ∀ q ∈ flattenFam es cs, q.1.val ≠ k := by
  intro cs
  induction cs with
  | nil =>
    intro es he _ q hq; simp only [flattenFam_nil_children] at hq; exact he q hq
  | cons c cs' ih =>
    intro es he hc q hq
    have hc_c : ∀ q ∈ flatten c, q.1.val ≠ k := hc c (by simp)
    have hc' : ∀ x ∈ cs', ∀ q ∈ flatten x, q.1.val ≠ k := fun x hx => hc x (by simp [hx])
    cases es with
    | nil =>
      rw [flattenFam_nil_cons, List.mem_append] at hq
      rcases hq with h | h
      · exact hc_c q h
      · exact ih [] (by simp) hc' q h
    | cons e es' =>
      rw [flattenFam_cons_cons, List.mem_append] at hq
      rcases hq with h | h
      · exact hc_c q h
      · simp only [List.mem_cons] at h
        rcases h with rfl | h
        · exact he q (by simp)
        · exact ih es' (fun f hf => he f (by simp [hf])) hc' q h

/-- `delete_from_node` removes `key` from the flattened list: no entry of the
    result carries `key`. Conditional on success (mirrors
    `delete_from_node_inv`). -/
theorem delete_from_node_notmem (fuel : Nat) {lo hi : Option Nat} {h : Nat}
    (node : Node) (key : Std.U64) (n' : Node) (uf : Bool)
    (hfuel : Node.size node ≤ fuel)
    (hinv : NodeInv lo hi node) (hh : HeightInv h node)
    (hok : delete_from_node node key = ok (DeleteResult.Removed n' uf)) :
    ∀ p ∈ flatten n', p.1.val ≠ key.val := by
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
          obtain ⟨new_entries, hre, hok⟩ := bind_ok_inv hok
          have hrev : new_entries.val = entries.val.eraseIdx pos.val :=
            spec_elim (remove_entry_at_spec entries pos) hre
          obtain ⟨i2, hmk, hok⟩ := bind_ok_inv hok
          simp only [ok.injEq, DeleteResult.Removed.injEq] at hok
          obtain ⟨hnn, -⟩ := hok
          subst hnn
          have hkey : (entries.val[pos.val]'hplt).1.val = key.val := by rw [hpkey]
          rw [flatten_leaf, hrev, List.eraseIdx_eq_take_drop_succ]
          intro p hp hcontra
          rw [List.mem_append] at hp
          rcases hp with hp | hp
          · obtain ⟨j, hj, rfl⟩ := List.mem_iff_getElem.mp hp
            rw [List.length_take] at hj
            have hjlt : j < entries.val.length := by omega
            have hjpos : j < pos.val := by omega
            rw [List.getElem_take] at hcontra
            have heq : j = pos.val :=
              sorted_key_unique entries.val hs hjlt hplt (by rw [hcontra, hkey])
            exact absurd heq (by omega)
          · obtain ⟨j, hj, rfl⟩ := List.mem_iff_getElem.mp hp
            rw [List.length_drop] at hj
            have hjlt : pos.val + 1 + j < entries.val.length := by omega
            rw [List.getElem_drop] at hcontra
            have heq : pos.val + 1 + j = pos.val :=
              sorted_key_unique entries.val hs hjlt hplt (by rw [hcontra, hkey])
            exact absurd heq (by omega)
        · rename_i hcond; simp at hok
    | internal entries c cs hs hb hlen hal =>
      cases hh with
      | @internal h0 e2 c2 cs2 hch =>
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
          -- succ > key
          have hsucc_gt : key.val < succ.1.val := by
            have hlb := hsucc_InB.1
            rw [lbnd_eq_some lo entries.val (pos.val + 1) (by omega) (by omega)] at hlb
            simp only [Nat.add_sub_cancel] at hlb
            have := hlb (entries.val[pos.val]'hplt).1.val (by simp)
            omega
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
          rw [flatten_eq_fam, hmf_eq, he0v, hc0v]
          -- all keys of the set-family ≠ key
          apply flattenFam_ne_of_bound key.val
          · -- entries.set pos succ ≠ key
            intro e he
            obtain ⟨j, hj, rfl⟩ := List.mem_iff_getElem.mp he
            have hjlt : j < entries.val.length := by rw [List.length_set] at hj; exact hj
            rw [List.getElem_set]
            split
            · omega
            · rename_i hne
              intro hcontra
              exact hne (sorted_key_unique entries.val hs hplt hjlt (by rw [hkey, hcontra]))
          · -- children (set i1 new_right) flatten ≠ key
            intro x hx q hq
            obtain ⟨j, hjlt, rfl⟩ := List.mem_iff_getElem.mp hx
            rw [List.getElem_set] at hq
            split at hq
            · -- new_right
              have hlb := (flatten_bounds hNI_nr q hq).1
              have := hlb succ.1.val (by simp)
              omega
            · rename_i hne
              have hne' : i1.val ≠ j := hne
              rw [List.length_set] at hjlt
              have hjle : j ≤ entries.val.length := by rw [hclen] at hjlt; omega
              obtain ⟨hcltj, hcinvj⟩ := hal.child j hjle
              have hqb := flatten_bounds hcinvj q hq
              have hpair := List.pairwise_iff_getElem.mp hs
              rcases Nat.lt_or_ge j i1.val with hjl | hjg
              · -- j ≤ pos : q < entries[j] ≤ key
                have hjle_pos : j ≤ pos.val := by rw [hi1v] at hjl; omega
                have hjlt2 : j < entries.val.length := by omega
                rw [rbnd_eq_some hi entries.val j hjlt2] at hqb
                have hq_lt := hqb.2 (entries.val[j]'hjlt2).1.val (by simp)
                have hle : (entries.val[j]'hjlt2).1.val ≤ (entries.val[pos.val]'hplt).1.val := by
                  rcases Nat.lt_or_eq_of_le hjle_pos with h | h
                  · exact le_of_lt (hpair j pos.val hjlt2 hplt h)
                  · subst h; exact le_refl _
                omega
              · -- j > pos+1 : q > entries[j-1] > key
                have hj2 : pos.val + 1 < j := by rw [hi1v] at hjg hne'; omega
                have hj1lt : j - 1 < entries.val.length := by omega
                rw [lbnd_eq_some lo entries.val j (by omega) hj1lt] at hqb
                have hq_gt := hqb.1 (entries.val[j-1]'hj1lt).1.val (by simp)
                have hgt_key : key.val < (entries.val[j-1]'hj1lt).1.val := by
                  rw [← hkey]; exact hpair pos.val (j-1) hplt hj1lt (by omega)
                omega
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
            have hIH : ∀ p ∈ flatten new_child, p.1.val ≠ key.val :=
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
                -- NodeInv of new_child at the slot bounds
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
            rw [flatten_eq_fam, hmf_eq, he0, hc0v]
            apply flattenFam_ne_of_bound key.val
            · intro e he
              obtain ⟨j, hj, rfl⟩ := List.mem_iff_getElem.mp he
              rcases Nat.lt_or_ge j pos.val with hjl | hjg
              · have := hlt' j hj hjl; omega
              · have := hgt' j hj hjg; omega
            · intro x hx q hq
              obtain ⟨j, hjlt, rfl⟩ := List.mem_iff_getElem.mp hx
              rw [List.getElem_set] at hq
              split at hq
              · exact hIH q hq
              · rename_i hne
                have hne' : pos.val ≠ j := hne
                rw [List.length_set] at hjlt
                have hjle : j ≤ entries.val.length := by rw [hclen] at hjlt; omega
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

end btree_kernel
