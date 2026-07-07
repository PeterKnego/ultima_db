/- Functional correctness of `BTree.remove`, part 1: the removed key is gone.

   `BTree.remove_get`: after removing `key`, looking it up returns `none`.
   Built on the in-order `flatten` foundation (RemoveFlatten.lean):
   `get_flatten` characterises `get` as a lookup in the flattened list, and
   `delete_from_node_notmem` shows the deleted key no longer appears there.

   Verify axioms with `#print axioms BTree.remove_get`: only
   `propext`/`Classical.choice`/`Quot.sound` (the Aeneas *library* carries
   unrelated sorries — this file's theorems do not depend on them). -/
import RemoveFlatten
import RemoveInv

open Aeneas Aeneas.Std Result

namespace btree_kernel

set_option maxRecDepth 4096

private theorem rg_bind_ok_inv {α β : Type} {m : Result α} {k : α → Result β}
    {out : β} (h : (do let a ← m; k a) = ok out) :
    ∃ a, m = ok a ∧ k a = ok out := by
  cases m with
  | ok a => exact ⟨a, rfl, by rwa [bind_tc_ok] at h⟩
  | fail e => rw [bind_tc_fail] at h; exact absurd h (by simp)
  | div => rw [bind_tc_div] at h; exact absurd h (by simp)

private theorem rg_spec_elim {α : Type} {m : Result α} {p : α → Prop}
    (hspec : WP.spec m p) {v : α} (heq : m = ok v) : p v := by
  obtain ⟨w, hw, hp⟩ := WP.spec_imp_exists hspec
  rw [heq] at hw; cases hw; exact hp

/-- The first child's flattened list is a prefix of the parent's, so its
    entries all live in the parent's flattened list. -/
theorem flatten_child0_mem {es : alloc.vec.Vec (Std.U64 × Std.U64)}
    {cs : Children} {n : Node} (hne : clist cs ≠ [])
    (hn : n = (clist cs)[0]'(List.length_pos_iff.mpr hne))
    {p : Std.U64 × Std.U64} (hp : p ∈ flatten n) :
    p ∈ flatten (Node.mk es cs) := by
  rw [flatten_eq_fam]
  obtain ⟨c0, rest, hcs⟩ := List.exists_cons_of_ne_nil hne
  have hn0 : n = c0 := by rw [hn]; simp [hcs]
  rw [hcs]
  cases hev : es.val with
  | nil =>
    rw [flattenFam_nil_cons, List.mem_append]
    left; rw [← hn0]; exact hp
  | cons e es' =>
    rw [flattenFam_cons_cons, List.mem_append]
    left; rw [← hn0]; exact hp

/-- **Removing `key` deletes it.** After a successful `BTree.remove t key`
    producing `t'`, looking `key` up in `t'` returns `none`. -/
theorem BTree.remove_get (t t' : BTree) (key : Std.U64)
    (hinv : BTreeInv t) (hbal : BTreeBalanced t)
    (hok : BTree.remove t key = ok (some t')) :
    t'.get key = ok none := by
  obtain ⟨hinv', _⟩ := BTree.remove_inv t key t' hinv hbal hok
  rw [BTree.get, get_flatten (Node.size t'.root) t'.root key (le_refl _) hinv']
  -- suffices: key does not occur in the flattened tree
  suffices hnotin : ∀ p ∈ flatten t'.root, p.1.val ≠ key.val by
    rw [lookupKey_none_of_all_ne key (flatten t'.root) hnotin]
  obtain ⟨h, hh⟩ := hbal
  rw [BTree.remove] at hok
  obtain ⟨dr, hdr, hok⟩ := rg_bind_ok_inv hok
  cases dr with
  | NotFound => simp at hok
  | Removed new_root ufflag =>
    obtain ⟨nre, nrc⟩ := new_root
    dsimp only at hok
    have hnr : ∀ p ∈ flatten (Node.mk nre nrc), p.1.val ≠ key.val :=
      delete_from_node_notmem (Node.size t.root) t.root key (Node.mk nre nrc) ufflag
        (le_refl _) hinv hh hdr
    obtain ⟨hNI_nr, -, -⟩ :=
      delete_from_node_inv (Node.size t.root) t.root key (Node.mk nre nrc) ufflag
        (le_refl _) hinv hh hdr
    obtain ⟨actual_root, hact, hok⟩ := rg_bind_ok_inv hok
    obtain ⟨i1sub, hsub, hok⟩ := rg_bind_ok_inv hok
    simp only [ok.injEq, Option.some.injEq] at hok
    subst hok
    show ∀ p ∈ flatten actual_root, p.1.val ≠ key.val
    simp only [Node.entries._simpLemma_, Node.children._simpLemma_] at hact
    split at hact
    · -- new_root.entries empty
      obtain ⟨i1, hcl, hact⟩ := rg_bind_ok_inv hact
      have hi1len : i1.val = (clist nrc).length := by
        have hle : (clist nrc).length ≤ Std.Usize.max := by
          have := NodeInv.children_len_le hNI_nr
          simp only [Node.children._simpLemma_] at this; scalar_tac
        exact rg_spec_elim (children_len_spec _ hle) hcl
      split at hact
      · -- child count ≠ 0 : collapse to clone of child 0
        rename_i hi1ne
        have hne0 : i1 ≠ 0#usize := by simpa using hi1ne
        have hchne : clist nrc ≠ [] :=
          List.ne_nil_of_length_pos (by rw [← hi1len]; scalar_tac)
        obtain ⟨nchild, hca, hact⟩ := rg_bind_ok_inv hact
        have hclt : (0#usize).val < (clist nrc).length := List.length_pos_iff.mpr hchne
        have hnc_eq : nchild = (clist nrc)[0]'hclt :=
          rg_spec_elim (child_at_spec nrc 0#usize (by simpa using hclt)) hca
        have hact' : actual_root = nchild := rg_spec_elim (clone_node_spec nchild) hact
        intro p hp
        rw [hact'] at hp
        exact hnr p (flatten_child0_mem hchne hnc_eq hp)
      · -- keep new_root
        rename_i hi1eq; simp only [ok.injEq] at hact; subst hact; exact hnr
    · -- entries nonempty : keep new_root
      rename_i hine; simp only [ok.injEq] at hact; subst hact; exact hnr

#print axioms BTree.remove_get

end btree_kernel
