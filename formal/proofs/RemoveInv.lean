/- Main theorems: the delete path (`remove_leftmost`, `delete_from_node`,
   `BTree.remove`) preserves the B-tree sortedness invariant (`NodeInv` /
   `BTreeInv`, BtreeInvariant.lean) and the height-balance invariant
   (`HeightInv`, BalancedInvariant.lean).

   IMPORTANT DESIGN NOTE — conditional statements. `NodeInv ∧ HeightInv`
   still admits pathological 0-entry internal nodes (an internal node with 0
   entries and one child). `delete_from_node` / `remove_leftmost` can
   genuinely FAIL on such trees (`fix_underfull_child` on a 0-entry parent
   indexes into its empty entry vector). So the theorems here are stated
   conditionally on success (`… = ok …`), not as total WP triples: whenever
   the kernel returns `ok`, the result is well-formed. Totality would require
   the MIN_KEYS lower-bound invariant (future work).

   The `hnz` precondition of F2's `maybe_fix_inv` (a fixed parent has ≥ 1
   entry) is derived FROM the success of the call: every reachable branch of
   `fix_underfull_child` indexes `entries`, so `ok` implies nonempty
   (`fix_underfull_child_ok_pos`). -/
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
import RemoveRebalance

open Aeneas Aeneas.Std Result

namespace btree_kernel

set_option maxRecDepth 4096

/-! ## `Result` plumbing for conditional (`= ok`) reasoning -/

/-- Invert a monadic bind that succeeded: the first computation succeeded. -/
private theorem bind_ok_inv {α β : Type} {m : Result α} {k : α → Result β}
    {out : β} (h : (do let a ← m; k a) = ok out) :
    ∃ a, m = ok a ∧ k a = ok out := by
  cases m with
  | ok a => exact ⟨a, rfl, by rwa [bind_tc_ok] at h⟩
  | fail e => rw [bind_tc_fail] at h; exact absurd h (by simp)
  | div => rw [bind_tc_div] at h; exact absurd h (by simp)

/-- Eliminate a WP triple against a known `= ok` result. -/
private theorem spec_elim {α : Type} {m : Result α} {p : α → Prop}
    (hspec : WP.spec m p) {v : α} (heq : m = ok v) : p v := by
  obtain ⟨w, hw, hp⟩ := WP.spec_imp_exists hspec
  rw [heq] at hw
  cases hw
  exact hp

/-- A successful `Vec.index_usize` returns the element at the index. -/
private theorem index_usize_ok_inv {α : Type} {v : alloc.vec.Vec α}
    {i : Std.Usize} {x : α}
    (h : alloc.vec.Vec.index_usize v i = ok x) : v.val[i.val]? = some x := by
  unfold alloc.vec.Vec.index_usize at h
  split at h
  · exact absurd h (by simp)
  · rename_i y hy
    cases h
    simpa using hy

/-- A successful `Vec.index_usize` implies the index was in bounds. -/
private theorem index_usize_ok_lt {α : Type} {v : alloc.vec.Vec α}
    {i : Std.Usize} {x : α}
    (h : alloc.vec.Vec.index_usize v i = ok x) :
    ∃ hlt : i.val < v.val.length, x = v.val[i.val]'hlt := by
  have h' := index_usize_ok_inv h
  have hlt : i.val < v.val.length := by
    by_contra hge
    rw [List.getElem?_eq_none (by omega)] at h'
    cases h'
  refine ⟨hlt, ?_⟩
  rw [List.getElem?_eq_getElem hlt] at h'
  cases h'
  rfl

/-- `Vec.index_usize` on an empty vector always fails. -/
private theorem index_usize_nil {α : Type} (v : alloc.vec.Vec α)
    (i : Std.Usize) (h : v.val = []) :
    alloc.vec.Vec.index_usize v i = fail Error.arrayOutOfBounds := by
  unfold alloc.vec.Vec.index_usize
  have hnone : v[i.val]? = (none : Option α) := by
    simp [h]
  rw [hnone]

/-! ## The rebalancers fail on a 0-entry parent

Every reachable branch of `fix_underfull_child` indexes into the parent's
`entries` vector, so success implies the parent had at least one entry. -/

private theorem rotate_right_ne_ok
    (entries : alloc.vec.Vec (Std.U64 × Std.U64)) (children : Children)
    (idx : Std.Usize) (out : (alloc.vec.Vec (Std.U64 × Std.U64)) × Children)
    (hnil : entries.val = []) : rotate_right entries children idx ≠ ok out := by
  intro hok
  rw [rotate_right] at hok
  simp only [alloc.vec.Vec.index_slice_index] at hok
  obtain ⟨i, -, hok⟩ := bind_ok_inv hok
  obtain ⟨left, -, hok⟩ := bind_ok_inv hok
  obtain ⟨right, -, hok⟩ := bind_ok_inv hok
  obtain ⟨i1, -, hok⟩ := bind_ok_inv hok
  obtain ⟨stolen, -, hok⟩ := bind_ok_inv hok
  rw [index_usize_nil entries i hnil, bind_tc_fail] at hok
  exact absurd hok (by simp)

private theorem rotate_left_ne_ok
    (entries : alloc.vec.Vec (Std.U64 × Std.U64)) (children : Children)
    (idx : Std.Usize) (out : (alloc.vec.Vec (Std.U64 × Std.U64)) × Children)
    (hnil : entries.val = []) : rotate_left entries children idx ≠ ok out := by
  intro hok
  rw [rotate_left] at hok
  simp only [alloc.vec.Vec.index_slice_index] at hok
  obtain ⟨left, -, hok⟩ := bind_ok_inv hok
  obtain ⟨i, -, hok⟩ := bind_ok_inv hok
  obtain ⟨right, -, hok⟩ := bind_ok_inv hok
  obtain ⟨stolen, -, hok⟩ := bind_ok_inv hok
  rw [index_usize_nil entries idx hnil, bind_tc_fail] at hok
  exact absurd hok (by simp)

private theorem merge_with_right_ne_ok
    (entries : alloc.vec.Vec (Std.U64 × Std.U64)) (children : Children)
    (idx : Std.Usize) (out : (alloc.vec.Vec (Std.U64 × Std.U64)) × Children)
    (hnil : entries.val = []) :
    merge_with_right entries children idx ≠ ok out := by
  intro hok
  rw [merge_with_right] at hok
  simp only [alloc.vec.Vec.index_slice_index] at hok
  obtain ⟨sep, hsep, -⟩ := bind_ok_inv hok
  rw [index_usize_nil entries idx hnil] at hsep
  exact absurd hsep (by simp)

private theorem merge_with_left_ne_ok
    (entries : alloc.vec.Vec (Std.U64 × Std.U64)) (children : Children)
    (idx : Std.Usize) (out : (alloc.vec.Vec (Std.U64 × Std.U64)) × Children)
    (hnil : entries.val = []) :
    merge_with_left entries children idx ≠ ok out := by
  intro hok
  rw [merge_with_left] at hok
  simp only [alloc.vec.Vec.index_slice_index] at hok
  obtain ⟨i, -, hok⟩ := bind_ok_inv hok
  rw [index_usize_nil entries i hnil, bind_tc_fail] at hok
  exact absurd hok (by simp)

/-- If `fix_underfull_child` succeeds, the parent had at least one entry.
    (Every reachable branch indexes `entries`.) -/
theorem fix_underfull_child_ok_pos
    {entries : alloc.vec.Vec (Std.U64 × Std.U64)} {children : Children}
    {idx : Std.Usize} {out : (alloc.vec.Vec (Std.U64 × Std.U64)) × Children}
    (hok : fix_underfull_child entries children idx = ok out) :
    0 < entries.val.length := by
  by_contra hle
  have hnil : entries.val = [] :=
    List.eq_nil_of_length_eq_zero (by omega)
  rw [fix_underfull_child] at hok
  obtain ⟨n, -, hok⟩ := bind_ok_inv hok
  split at hok
  · -- idx > 0 (the inner `if idx > 0` collapses to merge_with_left)
    obtain ⟨i, -, hok⟩ := bind_ok_inv hok
    obtain ⟨n1, -, hok⟩ := bind_ok_inv hok
    obtain ⟨i2, -, hok⟩ := bind_ok_inv hok
    split at hok
    · exact rotate_right_ne_ok entries children idx out hnil hok
    · obtain ⟨i3, -, hok⟩ := bind_ok_inv hok
      split at hok
      · obtain ⟨n2, -, hok⟩ := bind_ok_inv hok
        simp only [gt_iff_lt] at hok  -- zeta-reduce the pure `let i4 := Vec.len …`
        split at hok
        · exact rotate_left_ne_ok entries children idx out hnil hok
        · exact merge_with_left_ne_ok entries children idx out hnil hok
      · exact merge_with_left_ne_ok entries children idx out hnil hok
  · -- idx = 0 (the inner `if idx > 0` collapses to merge_with_right)
    obtain ⟨i, -, hok⟩ := bind_ok_inv hok
    split at hok
    · obtain ⟨n1, -, hok⟩ := bind_ok_inv hok
      obtain ⟨i2, -, hok⟩ := bind_ok_inv hok
      split at hok
      · exact rotate_left_ne_ok entries children idx out hnil hok
      · exact merge_with_right_ne_ok entries children idx out hnil hok
    · exact merge_with_right_ne_ok entries children idx out hnil hok

/-- If `maybe_fix` succeeds with the underfull flag set, the parent had at
    least one entry. -/
theorem maybe_fix_ok_pos
    {entries : alloc.vec.Vec (Std.U64 × Std.U64)} {children : Children}
    {idx : Std.Usize} {out : (alloc.vec.Vec (Std.U64 × Std.U64)) × Children}
    (hok : maybe_fix entries children idx true = ok out) :
    0 < entries.val.length := by
  rw [maybe_fix] at hok
  rw [if_pos rfl] at hok
  exact fix_underfull_child_ok_pos hok

/-! ## Upper-bound relaxation for `NodeInv`

The delete-path internal-hit case replaces the separator key `es[pos]` by the
successor key extracted from child `pos + 1`. The (unchanged) child at `pos`
then needs its upper bound widened from `some es[pos]` to `some succ`. -/

private theorem relax_hi_fuel (fuel : Nat) : ∀ (n : Node),
    Node.size n ≤ fuel → ∀ (lo : Option Nat) (m : Nat) (hi' : Option Nat),
    NodeInv lo (some m) n → (∀ u ∈ hi', m ≤ u) → NodeInv lo hi' n := by
  induction fuel with
  | zero =>
    intro n hsz
    exfalso
    have := Node.one_le_size n
    omega
  | succ fuel ih =>
    intro n hsz lo m hi' hinv hm
    cases hinv with
    | leaf entries hs hb hlen =>
      refine NodeInv.leaf entries hs ?_ hlen
      intro e he
      obtain ⟨h1, h2⟩ := hb e he
      refine ⟨h1, fun u hu => ?_⟩
      have := h2 m (by simp)
      have := hm u hu
      omega
    | internal entries c cs hs hb hlen hal =>
      have hsz' : ∀ x ∈ (c :: clist cs), Node.size x ≤ fuel := by
        intro x hx
        have h2 := size_mem_clist (Children.Cons c cs) x
          (by simpa [clist] using hx)
        have h3 : Node.size (Node.mk entries (Children.Cons c cs)) =
            1 + Children.size (Children.Cons c cs) := by simp [Node.size]
        omega
      refine NodeInv.internal entries c cs hs ?_ hlen ?_
      · intro e he
        obtain ⟨h1, h2⟩ := hb e he
        refine ⟨h1, fun u hu => ?_⟩
        have := h2 m (by simp)
        have := hm u hu
        omega
      · have key : ∀ (es : List (Std.U64 × Std.U64)) (csl : List Node),
            (∀ x ∈ csl, Node.size x ≤ fuel) →
            ∀ lo0, Aligned lo0 (some m) es csl → Aligned lo0 hi' es csl := by
          intro es
          induction es with
          | nil =>
            intro csl hszc lo0 hA
            cases hA with
            | last c0 hc0 =>
              exact Aligned.last c0
                (ih c0 (hszc c0 (by simp)) lo0 m hi' hc0 hm)
          | cons e es ihe =>
            intro csl hszc lo0 hA
            cases hA with
            | step _ _ c0 cs0 hc0 htail =>
              refine Aligned.step e es c0 cs0 hc0 ?_
              exact ihe cs0 (fun x hx => hszc x (by simp [hx])) _ htail
        exact key entries.val (c :: clist cs) hsz' lo hal

/-- Widen the upper bound of a subtree invariant: keys `< m ≤ hi'`. -/
theorem NodeInv.relax_hi {lo : Option Nat} {m : Nat} {hi' : Option Nat}
    {n : Node} (hinv : NodeInv lo (some m) n) (hm : ∀ u ∈ hi', m ≤ u) :
    NodeInv lo hi' n :=
  relax_hi_fuel (Node.size n) n (Nat.le_refl _) lo m hi' hinv hm

/-! ## Small pure list lemmas -/

/-- `set (j+1)` in take/cons/cons/drop form, keeping element `j` explicit —
    the shape `Aligned.replace_pair` produces for the internal-hit case. -/
private theorem set_succ_eq_take_getElem_cons_drop {α : Type} (l : List α)
    (j : Nat) (a : α) (h : j + 1 < l.length) :
    l.set (j + 1) a =
      l.take j ++ (l[j]'(by omega)) :: a :: l.drop (j + 2) := by
  induction l generalizing j with
  | nil => simp at h
  | cons x t ih =>
    cases j with
    | zero =>
      match t, h with
      | y :: t', _ => simp
    | succ k =>
      simp only [List.set_cons_succ, List.take_succ_cons,
        List.getElem_cons_succ, List.cons_append]
      have ht : (x :: t).drop (k + 1 + 2) = t.drop (k + 2) := by
        simp [List.drop_succ_cons]
      rw [ht, ih k (by simp only [List.length_cons] at h; omega)]

/-- Strengthen the lower bound of an entry list when `m` sits below its
    first key. -/
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
  | succ j' =>
    have := hs' 0 (j' + 1) h0 hj (by omega)
    omega

/-! ## The three main theorems -/

/-- Balanced-tree bundle: some uniform height exists for the root. -/
def BTreeBalanced (t : BTree) : Prop := ∃ h, HeightInv h t.root

/-- `remove_leftmost` extracts the minimum key `e` of the subtree and returns
    the remaining subtree `n'` (bounded below by `e`), preserving height.
    Conditional on success. -/
theorem remove_leftmost_spec (fuel : Nat) {lo hi : Option Nat} {h : Nat}
    (node : Node) (e : Std.U64 × Std.U64) (n' : Node) (uf : Bool)
    (hfuel : Node.size node ≤ fuel)
    (hinv : NodeInv lo hi node) (hh : HeightInv h node)
    (hok : remove_leftmost node = ok (e, n', uf)) :
    InB lo hi e.1.val ∧ NodeInv (some e.1.val) hi n'
    ∧ HeightInv h n' ∧ (uf = true → n'.entries.val.length < 31) := by
  induction fuel generalizing lo hi h node e n' uf with
  | zero =>
    exfalso; have := Node.one_le_size node; omega
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
        obtain ⟨hlt0, -⟩ := index_usize_ok_lt hfirst
        have hne : entries.val ≠ [] := List.ne_nil_of_length_pos (by scalar_tac)
        obtain ⟨a, t, hcons⟩ := List.exists_cons_of_ne_nil hne
        have hfi := index_usize_ok_inv hfirst
        have h0v : (0#usize).val = 0 := rfl
        rw [h0v, hcons] at hfi
        simp only [List.getElem?_cons_zero, Option.some.injEq] at hfi
        obtain ⟨new_entries, hre, hok⟩ := bind_ok_inv hok
        have hrev : new_entries.val = entries.val.eraseIdx (0#usize).val :=
          spec_elim (remove_entry_at_spec entries 0#usize) hre
        obtain ⟨i2, hmk, hok⟩ := bind_ok_inv hok
        have hi2 : i2.val = 31 := spec_elim MIN_KEYS_spec hmk
        simp only [ok.injEq, Prod.mk.injEq] at hok
        obtain ⟨hfe, hnn, hufe⟩ := hok
        subst hnn; subst hufe
        -- e = first = a
        have hea : e.1.val = a.1.val := by rw [← hfe, ← hfi]
        have hs_cons : SortedE (a :: t) := hcons ▸ hs
        have hb_cons : ∀ f ∈ (a :: t), InB lo hi f.1.val := hcons ▸ hb
        have ht_eq : new_entries.val = t := by
          rw [hrev, hcons]; simp
        refine ⟨?_, ?_, ?_, ?_⟩
        · -- InB lo hi e
          rw [hea]; exact hb_cons a (by simp)
        · -- NodeInv (some e) hi (leaf new_entries)
          refine NodeInv.leaf new_entries ?_ ?_ ?_
          · rw [ht_eq]; exact (List.pairwise_cons.mp hs_cons).2
          · rw [ht_eq]; intro f hf
            refine InB.tighten_lo (hb_cons f (by simp [hf])) ?_
            rw [hea]; exact (List.pairwise_cons.mp hs_cons).1 f hf
          · rw [ht_eq]
            have : (a :: t).length ≤ 63 := hcons ▸ hlen
            simp only [List.length_cons] at this; omega
        · exact HeightInv.leaf new_entries
        · intro huf
          have := alloc.vec.Vec.len_val new_entries
          simp only [Node.entries._simpLemma_]; scalar_tac
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
        -- child 0
        obtain ⟨n, hn, hok⟩ := bind_ok_inv hok
        have hn_eq : n = (clist (Children.Cons c cs))[(0#usize).val]'(by
            rw [hclen]; scalar_tac) :=
          spec_elim (child_at_spec _ 0#usize (by rw [hclen]; scalar_tac)) hn
        have hnc : n = c := by rw [hn_eq]; simp [clist]
        -- child invariants at slot 0
        obtain ⟨hc0lt, hc0inv⟩ := hal.child 0 (Nat.zero_le _)
        simp only [lbnd, List.getElem_cons_zero, ↓reduceIte] at hc0inv
        have hc_h : HeightInv h0 c := (hch.allH) c (by simp [clist])
        have hsz : Node.size c ≤ fuel := by
          have hmem : c ∈ clist (Children.Cons c cs) := by simp [clist]
          have h1 := size_mem_clist _ _ hmem
          have h2 : Node.size (Node.mk entries (Children.Cons c cs)) =
              1 + Children.size (Children.Cons c cs) := by simp [Node.size]
          simp only [Node.size] at hfuel
          omega
        -- recursion
        obtain ⟨⟨entry, new_first_child, child_underfull⟩, hrl, hok⟩ := bind_ok_inv hok
        have hrl' : remove_leftmost c = ok (entry, new_first_child, child_underfull) := by
          rw [← hnc]; exact hrl
        obtain ⟨hInB_e, hNI_nfc, hHI_nfc, huf_nfc⟩ :=
          ih c entry new_first_child child_underfull hsz hc0inv hc_h hrl'
        -- clone + replace_child + maybe_fix
        obtain ⟨entries0, hcl0, hok⟩ := bind_ok_inv hok
        have he0 : entries0 = entries := spec_elim (cloneVec_builtin_spec entries) hcl0
        obtain ⟨children0, hrc0, hok⟩ := bind_ok_inv hok
        have hc0 : clist children0 = (clist (Children.Cons c cs)).set (0#usize).val new_first_child :=
          spec_elim (replace_child_spec (Children.Cons c cs) 0#usize new_first_child) hrc0
        have hc0' : clist children0 = new_first_child :: clist cs := by
          rw [hc0]; simp [clist]
        obtain ⟨⟨entries1, children1⟩, hmf, hok⟩ := bind_ok_inv hok
        obtain ⟨i2, hmk, hok⟩ := bind_ok_inv hok
        have hi2 : i2.val = 31 := spec_elim MIN_KEYS_spec hmk
        simp only [ok.injEq, Prod.mk.injEq] at hok
        obtain ⟨he, hnn, hufe⟩ := hok
        -- build parent invariants at (some entry, hi)
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
        have hmf_spec := maybe_fix_inv entries0 children0 0#usize child_underfull hNI0 hHI0
          (fun hu => maybe_fix_ok_pos (hu ▸ hmf))
          (fun _ => by scalar_tac)
          (fun hu m hm => by
            have h0v : (0#usize).val = 0 := rfl
            rw [h0v, hc0'] at hm
            simp only [List.getElem?_cons_zero, Option.some.injEq] at hm
            subst hm; exact huf_nfc hu)
        obtain ⟨hNI1, hHI1⟩ := spec_elim hmf_spec hmf
        subst he; subst hnn; subst hufe
        refine ⟨?_, hNI1, hHI1, ?_⟩
        · -- InB lo hi entry
          refine ⟨hInB_e.1, fun u hu => ?_⟩
          by_cases h0len : 0 < entries.val.length
          · have hrb := rbnd_eq_some hi entries.val 0 h0len
            have h1 := hInB_e.2 ((entries.val[0]'h0len).1.val) (by rw [hrb]; simp)
            have h2 := (hb _ (entries.val.getElem_mem h0len)).2 u hu
            omega
          · have hrb : rbnd hi entries.val 0 = hi := rbnd_eq_hi hi entries.val 0 (by omega)
            exact hInB_e.2 u (by rw [hrb]; exact hu)
        · intro huf
          have := alloc.vec.Vec.len_val entries1
          simp only [Node.entries._simpLemma_]; scalar_tac

/-- `delete_from_node` preserves both invariants whenever it returns
    `Removed`. Conditional on success. -/
theorem delete_from_node_inv (fuel : Nat) {lo hi : Option Nat} {h : Nat}
    (node : Node) (key : Std.U64) (n' : Node) (uf : Bool)
    (hfuel : Node.size node ≤ fuel)
    (hinv : NodeInv lo hi node) (hh : HeightInv h node)
    (hok : delete_from_node node key = ok (DeleteResult.Removed n' uf)) :
    NodeInv lo hi n' ∧ HeightInv h n' ∧ (uf = true → n'.entries.val.length < 31) := by
  induction fuel generalizing lo hi h node n' uf with
  | zero =>
    exfalso; have := Node.one_le_size node; omega
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
          obtain ⟨new_entries, hre, hok⟩ := bind_ok_inv hok
          have hrev : new_entries.val = entries.val.eraseIdx pos.val :=
            spec_elim (remove_entry_at_spec entries pos) hre
          obtain ⟨i2, hmk, hok⟩ := bind_ok_inv hok
          have hi2 : i2.val = 31 := spec_elim MIN_KEYS_spec hmk
          simp only [ok.injEq, DeleteResult.Removed.injEq] at hok
          obtain ⟨hnn, hufe⟩ := hok
          subst hnn; subst hufe
          refine ⟨?_, HeightInv.leaf new_entries, ?_⟩
          · refine NodeInv.leaf new_entries ?_ ?_ ?_
            · rw [hrev]; exact List.Pairwise.sublist (entries.val.eraseIdx_sublist pos.val) hs
            · rw [hrev]; intro f hf
              exact hb f ((entries.val.eraseIdx_sublist pos.val).mem hf)
            · rw [hrev]
              have := eraseIdx_length_le entries.val pos.val; omega
          · intro huf
            have := alloc.vec.Vec.len_val new_entries
            simp only [Node.entries._simpLemma_]; scalar_tac
        · rename_i hcond
          simp at hok
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
          obtain ⟨⟨entry, new_right, right_underfull⟩, hrl, hok⟩ := bind_ok_inv hok
          obtain ⟨hsucc_InB, hNI_nr, hHI_nr, huf_nr⟩ :=
            remove_leftmost_spec (Node.size n) n entry new_right right_underfull
              (le_refl _) hcn hn_h hrl
          rw [hi1v] at hsucc_InB hNI_nr
          obtain ⟨entries0, hre0, hok⟩ := bind_ok_inv hok
          have he0v : entries0.val = entries.val.set pos.val entry :=
            spec_elim (replace_entry_spec entries pos entry) hre0
          obtain ⟨children0, hrc0, hok⟩ := bind_ok_inv hok
          have hc0v : clist children0 = (clist (Children.Cons c cs)).set i1.val new_right :=
            spec_elim (replace_child_spec (Children.Cons c cs) i1 new_right) hrc0
          obtain ⟨⟨entries1, children1⟩, hmf, hok⟩ := bind_ok_inv hok
          obtain ⟨i3, hmk, hok⟩ := bind_ok_inv hok
          have hi3 : i3.val = 31 := spec_elim MIN_KEYS_spec hmk
          simp only [ok.injEq, DeleteResult.Removed.injEq] at hok
          obtain ⟨hnn, hufe⟩ := hok
          obtain ⟨hbk1, hbk2⟩ := bracket_of_InB_bnd entries.val (pos.val + 1) entry.1.val hs
            (by omega) hsucc_InB
          obtain ⟨hplt2, hlinv⟩ := hal.child pos.val (le_of_lt hplt)
          rw [rbnd_eq_some hi entries.val pos.val hplt] at hlinv
          have hl_relaxed : NodeInv (lbnd lo entries.val pos.val) (some entry.1.val)
              ((c :: clist cs)[pos.val]'hplt2) :=
            NodeInv.relax_hi hlinv (fun u hu => by
              simp only [Option.mem_def, Option.some.injEq] at hu; subst hu
              exact le_of_lt (hbk1 pos.val hplt (by omega)))
          have hsucclt : pos.val + 1 < (c :: clist cs).length := by
            rw [hal.length_eq]; omega
          have hNI0 : NodeInv lo hi (Node.mk entries0 children0) := by
            refine NodeInv.mk_node entries0 children0 ?_ ?_ ?_ (Or.inr ?_)
            · rw [he0v]
              exact SortedE_set_key entries.val pos.val entry hs hplt
                (fun i hi hip => hbk1 i hi (by omega))
                (fun i hi hip => hbk2 i hi (by omega))
            · rw [he0v]
              exact InB_set entries.val pos.val entry lo hi hb
                (InB_outer_of_bnd entries.val (pos.val + 1) entry.1.val (by omega) hb hsucc_InB)
            · rw [he0v]; simpa using hlen
            · rw [he0v, hc0v]
              simp only [clist]
              rw [hi1v, set_succ_eq_take_getElem_cons_drop (c :: clist cs) pos.val new_right hsucclt]
              exact Aligned.replace_pair hal pos.val hplt entry
                ((c :: clist cs)[pos.val]'hplt2) new_right hl_relaxed hNI_nr
          have hHI0 : HeightInv (h0 + 1) (Node.mk entries0 children0) := by
            refine HeightInv.mk_internal entries0 children0 ?_ ?_
            · rw [hc0v]; apply List.ne_nil_of_length_pos
              rw [List.length_set, hclen]; omega
            · rw [hc0v]; exact AllH.set _ hch.allH hHI_nr
          have hmf_spec := maybe_fix_inv entries0 children0 i1 right_underfull hNI0 hHI0
            (fun hu => maybe_fix_ok_pos (hu ▸ hmf))
            (fun _ => by rw [he0v, List.length_set]; omega)
            (fun hu m hm => by
              rw [hc0v, List.getElem?_set_self hi1lt] at hm
              simp only [Option.some.injEq] at hm; subst hm; exact huf_nr hu)
          obtain ⟨hNI1, hHI1⟩ := spec_elim hmf_spec hmf
          subst hnn; subst hufe
          refine ⟨hNI1, hHI1, ?_⟩
          intro huf
          have := alloc.vec.Vec.len_val entries1
          simp only [Node.entries._simpLemma_]; scalar_tac
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
          have hn_h : HeightInv h0 n := by
            rw [hn_eq]; exact hch.allH.getElem pos.val hposlt
          have hsz : Node.size n ≤ fuel := by
            have hmem : n ∈ clist (Children.Cons c cs) := by
              rw [hn_eq]; exact List.getElem_mem hposlt
            have h1 := size_mem_clist _ _ hmem
            simp only [Node.size] at hfuel; omega
          cases dr with
          | NotFound => simp at hok
          | Removed new_child underfull =>
            dsimp only at hok
            obtain ⟨hNI_nc, hHI_nc, huf_nc⟩ :=
              ih n new_child underfull hsz hcn hn_h hdr
            obtain ⟨entries0, hcl0, hok⟩ := bind_ok_inv hok
            have he0 : entries0 = entries := spec_elim (cloneVec_builtin_spec entries) hcl0
            obtain ⟨children0, hrc0, hok⟩ := bind_ok_inv hok
            have hc0v : clist children0 = (clist (Children.Cons c cs)).set pos.val new_child :=
              spec_elim (replace_child_spec (Children.Cons c cs) pos new_child) hrc0
            obtain ⟨⟨entries1, children1⟩, hmf, hok⟩ := bind_ok_inv hok
            obtain ⟨i2, hmk, hok⟩ := bind_ok_inv hok
            have hi2 : i2.val = 31 := spec_elim MIN_KEYS_spec hmk
            simp only [ok.injEq, DeleteResult.Removed.injEq] at hok
            obtain ⟨hnn, hufe⟩ := hok
            have hal' : Aligned lo hi entries.val (clist children0) := by
              rw [hc0v]; simp only [clist]
              exact hal.set_child pos.val hple new_child hNI_nc
            have hNI0 : NodeInv lo hi (Node.mk entries0 children0) := by
              rw [he0]
              exact NodeInv.mk_node entries children0 hs hb hlen (Or.inr hal')
            have hHI0 : HeightInv (h0 + 1) (Node.mk entries0 children0) := by
              rw [he0]
              refine HeightInv.mk_internal entries children0 ?_ ?_
              · rw [hc0v]; apply List.ne_nil_of_length_pos
                rw [List.length_set, hclen]; omega
              · rw [hc0v]; exact AllH.set _ hch.allH hHI_nc
            have hmf_spec := maybe_fix_inv entries0 children0 pos underfull hNI0 hHI0
              (fun hu => maybe_fix_ok_pos (hu ▸ hmf))
              (fun _ => by rw [he0]; exact hple)
              (fun hu m hm => by
                rw [hc0v, List.getElem?_set_self hposlt] at hm
                simp only [Option.some.injEq] at hm; subst hm; exact huf_nc hu)
            obtain ⟨hNI1, hHI1⟩ := spec_elim hmf_spec hmf
            subst hnn; subst hufe
            refine ⟨hNI1, hHI1, ?_⟩
            intro huf
            have := alloc.vec.Vec.len_val entries1
            simp only [Node.entries._simpLemma_]; scalar_tac

/-- Top-level: `BTree.remove` preserves the tree invariant and balance,
    handling the root-collapse case. Conditional on a successful delete. -/
theorem BTree.remove_inv (t : BTree) (key : Std.U64) (t' : BTree)
    (hinv : BTreeInv t) (hbal : BTreeBalanced t)
    (hok : BTree.remove t key = ok (some t')) :
    BTreeInv t' ∧ BTreeBalanced t' := by
  obtain ⟨h, hh⟩ := hbal
  rw [BTree.remove] at hok
  obtain ⟨dr, hdr, hok⟩ := bind_ok_inv hok
  cases dr with
  | NotFound => simp at hok
  | Removed new_root ufflag =>
    obtain ⟨nre, nrc⟩ := new_root
    dsimp only at hok
    obtain ⟨hNI_nr, hHI_nr, -⟩ :=
      delete_from_node_inv (Node.size t.root) t.root key (Node.mk nre nrc) ufflag
        (le_refl _) hinv hh hdr
    obtain ⟨actual_root, hact, hok⟩ := bind_ok_inv hok
    obtain ⟨i1sub, hsub, hok⟩ := bind_ok_inv hok
    simp only [ok.injEq, Option.some.injEq] at hok
    subst hok
    have hgoal : NodeInv none none actual_root ∧ ∃ h', HeightInv h' actual_root := by
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
            simp only [Node.children._simpLemma_] at this
            scalar_tac
          exact spec_elim (children_len_spec _ hle) hcl
        split at hact
        · rename_i hi1ne
          have hne0 : i1 ≠ 0#usize := by simpa using hi1ne
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
          refine ⟨by rw [hac_c0]; exact hc0inv, ?_⟩
          cases h with
          | zero =>
            have hcnil := HeightInv.zero_nil hHI_nr
            rw [hcnil] at hchne; simp [clist] at hchne
          | succ h0 =>
            have hall := HeightInv.children_allH hHI_nr
            exact ⟨h0, by rw [hac_c0]; exact hall c0 (by rw [hc0eq]; simp)⟩
        · rename_i hi1eq
          simp only [ok.injEq] at hact
          subst hact
          exact ⟨hNI_nr, h, hHI_nr⟩
      · rename_i hine
        simp only [ok.injEq] at hact
        subst hact
        exact ⟨hNI_nr, h, hHI_nr⟩
    exact ⟨hgoal.1, hgoal.2⟩

end btree_kernel
