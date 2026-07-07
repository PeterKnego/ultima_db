import BtreeKernel
import BtreeInvariant

open Aeneas Aeneas.Std Result

namespace btree_kernel

theorem Node.one_le_size (n : Node) : 1 ≤ Node.size n := by
  cases n with
  | mk entries children => simp [Node.size]

theorem children_ind {motive : Children → Prop}
    (nil : motive Children.Nil)
    (cons : ∀ n rest, motive rest → motive (Children.Cons n rest)) :
    ∀ c, motive c
  | .Nil => nil
  | .Cons n rest => cons n rest (children_ind nil cons rest)
termination_by c => Children.size c
decreasing_by
  have := Node.one_le_size n
  simp only [Children.size]
  omega

theorem cloneVec_builtin_eq {T : Type} (v : alloc.vec.Vec T) :
    alloc.vec.CloneVec.clone (BuiltinClone T) v = ok v := by
  obtain ⟨v', hv, hvv⟩ := WP.spec_imp_exists
    (Slice.clone_spec (clone := Result.ok) (s := v) (fun _ _ => rfl))
  subst hvv
  simpa [alloc.vec.CloneVec.clone, BuiltinClone] using hv

mutual

theorem clone_node_spec (n : Node) : clone_node n ⦃ m => m = n ⦄ := by
  obtain ⟨entries, children⟩ := n
  obtain ⟨c1, hc, hcv⟩ := WP.spec_imp_exists (clone_children_spec children)
  rw [clone_node]
  simp only [Node.entries._simpLemma_, Node.children._simpLemma_,
    cloneVec_builtin_eq, bind_tc_ok, hc, WP.spec_ok]
  simp [hcv]
termination_by 2 * Node.size n
decreasing_by
  simp only [Node.size]
  omega

theorem clone_children_spec (c : Children) : clone_children c ⦃ d => d = c ⦄ :=
  match c with
  | Children.Nil => by rw [clone_children]; simp
  | Children.Cons n rest => by
    obtain ⟨n1, hn, hnv⟩ := WP.spec_imp_exists (clone_node_spec n)
    obtain ⟨c1, hc, hcv⟩ := WP.spec_imp_exists (clone_children_spec rest)
    rw [clone_children]
    simp only [hn, hc, bind_tc_ok, WP.spec_ok]
    simp [hnv, hcv]
termination_by 2 * Children.size c + 1
decreasing_by
  all_goals (have := Node.one_le_size n; simp only [Children.size]; omega)

end

theorem children_len_spec (c : Children) (hlen : (clist c).length ≤ Std.Usize.max) :
    children_len c ⦃ n => n.val = (clist c).length ⦄ := by
  induction c using children_ind with
  | nil => rw [children_len]; simp [clist]
  | cons n rest ih =>
    rw [children_len]
    have hlen' : (clist rest).length ≤ Std.Usize.max := by
      simp [clist] at hlen; omega
    obtain ⟨i, hi, hiv⟩ := WP.spec_imp_exists (ih hlen')
    have hadd : (1#usize).val + i.val ≤ Usize.max := by
      simp [clist] at hlen; scalar_tac
    obtain ⟨z, hz, hzv⟩ := WP.spec_imp_exists (Usize.add_spec hadd)
    simp only [hi, bind_tc_ok, hz, WP.spec_ok]
    simp only [clist, List.length_cons]
    scalar_tac

theorem child_at_spec' (c : Children) (i : Std.Usize) (h : i.val < (clist c).length) :
    child_at c i ⦃ n => (clist c)[i.val]? = some n ⦄ := by
  induction c using children_ind generalizing i with
  | nil => simp [clist] at h
  | cons n rest ih =>
    rw [child_at]
    split
    · rename_i hi0
      have hiv : i.val = 0 := by scalar_tac
      simp only [WP.spec_ok]
      simp [clist, hiv]
    · rename_i hi0
      have h1 : (1#usize).val ≤ i.val := by scalar_tac
      obtain ⟨i1, hi1, hi1v⟩ := WP.spec_imp_exists (Usize.sub_spec h1)
      have hlt : i1.val < (clist rest).length := by
        simp [clist] at h; scalar_tac
      obtain ⟨m, hm, hmv⟩ := WP.spec_imp_exists (ih i1 hlt)
      simp only [hi1, bind_tc_ok, hm, WP.spec_ok]
      have hiv : i.val = i1.val + 1 := by scalar_tac
      simp only [clist, hiv, List.getElem?_cons_succ]
      exact hmv

theorem child_at_spec (c : Children) (i : Std.Usize) (h : i.val < (clist c).length) :
    child_at c i ⦃ n => n = (clist c)[i.val]'h ⦄ := by
  obtain ⟨m, hm, hmv⟩ := WP.spec_imp_exists (child_at_spec' c i h)
  simp only [hm, WP.spec_ok]
  rw [List.getElem?_eq_getElem h] at hmv
  simpa using hmv.symm

theorem replace_child_spec (c : Children) (pos : Std.Usize) (x : Node) :
    replace_child c pos x ⦃ out => clist out = (clist c).set pos.val x ⦄ := by
  induction c using children_ind generalizing pos with
  | nil => rw [replace_child]; simp [clist]
  | cons n rest ih =>
    rw [replace_child]
    split
    · rename_i hp0
      obtain ⟨c1, hc, hcv⟩ := WP.spec_imp_exists (clone_children_spec rest)
      have hpv : pos.val = 0 := by scalar_tac
      simp only [hc, bind_tc_ok, WP.spec_ok]
      simp [clist, hpv, hcv]
    · rename_i hp0
      have h1 : (1#usize).val ≤ pos.val := by scalar_tac
      obtain ⟨n1, hn, hnv⟩ := WP.spec_imp_exists (clone_node_spec n)
      obtain ⟨i, hi, hiv⟩ := WP.spec_imp_exists (Usize.sub_spec h1)
      obtain ⟨c1, hc, hcv⟩ := WP.spec_imp_exists (ih i)
      simp only [hn, hi, hc, bind_tc_ok, WP.spec_ok]
      have hpv : pos.val = i.val + 1 := by scalar_tac
      simp [clist, hpv, hnv, hcv]

theorem replace_and_insert_child_spec (c : Children) (pos : Std.Usize) (l r : Node)
    (h : pos.val < (clist c).length) :
    replace_and_insert_child c pos l r ⦃ out =>
      clist out = (clist c).take pos.val ++ l :: r :: (clist c).drop (pos.val + 1) ⦄ := by
  induction c using children_ind generalizing pos with
  | nil => simp [clist] at h
  | cons n rest ih =>
    rw [replace_and_insert_child]
    split
    · rename_i hp0
      obtain ⟨c1, hc, hcv⟩ := WP.spec_imp_exists (clone_children_spec rest)
      have hpv : pos.val = 0 := by scalar_tac
      simp only [hc, bind_tc_ok, WP.spec_ok]
      simp [clist, hpv, hcv]
    · rename_i hp0
      have h1 : (1#usize).val ≤ pos.val := by scalar_tac
      obtain ⟨n1, hn, hnv⟩ := WP.spec_imp_exists (clone_node_spec n)
      obtain ⟨i, hi, hiv⟩ := WP.spec_imp_exists (Usize.sub_spec h1)
      have hlt : i.val < (clist rest).length := by
        simp [clist] at h; scalar_tac
      obtain ⟨c1, hc, hcv⟩ := WP.spec_imp_exists (ih i hlt)
      simp only [hn, hi, hc, bind_tc_ok, WP.spec_ok]
      have hpv : pos.val = i.val + 1 := by scalar_tac
      simp [clist, hpv, hnv, hcv, List.take_succ_cons, List.drop_succ_cons]

theorem children_prefix_spec (c : Children) (n : Std.Usize) :
    children_prefix c n ⦃ out => clist out = (clist c).take n.val ⦄ := by
  induction c using children_ind generalizing n with
  | nil =>
    rw [children_prefix]
    split <;> simp [clist]
  | cons x rest ih =>
    rw [children_prefix]
    split
    · rename_i hn0
      have hnv : n.val = 0 := by scalar_tac
      simp only [WP.spec_ok]
      simp [clist, hnv]
    · rename_i hn0
      have h1 : (1#usize).val ≤ n.val := by scalar_tac
      obtain ⟨x1, hx, hxv⟩ := WP.spec_imp_exists (clone_node_spec x)
      obtain ⟨i, hi, hiv⟩ := WP.spec_imp_exists (Usize.sub_spec h1)
      obtain ⟨c1, hc, hcv⟩ := WP.spec_imp_exists (ih i)
      simp only [hx, hi, hc, bind_tc_ok, WP.spec_ok]
      have hnv : n.val = i.val + 1 := by scalar_tac
      simp [clist, hnv, hxv, hcv, List.take_succ_cons]

theorem children_suffix_spec (c : Children) (n : Std.Usize) :
    children_suffix c n ⦃ out => clist out = (clist c).drop n.val ⦄ := by
  induction c using children_ind generalizing n with
  | nil =>
    rw [children_suffix]
    split
    · obtain ⟨d, hd, hdv⟩ := WP.spec_imp_exists (clone_children_spec Children.Nil)
      simp only [hd, WP.spec_ok]
      simp [clist, hdv]
    · simp [clist]
  | cons x rest ih =>
    rw [children_suffix]
    split
    · rename_i hn0
      obtain ⟨d, hd, hdv⟩ := WP.spec_imp_exists (clone_children_spec (Children.Cons x rest))
      have hnv : n.val = 0 := by scalar_tac
      simp only [hd, WP.spec_ok]
      simp [hnv, hdv]
    · rename_i hn0
      have h1 : (1#usize).val ≤ n.val := by scalar_tac
      obtain ⟨i, hi, hiv⟩ := WP.spec_imp_exists (Usize.sub_spec h1)
      obtain ⟨d, hd, hdv⟩ := WP.spec_imp_exists (ih i)
      simp only [hi, bind_tc_ok, hd, WP.spec_ok]
      have hnv : n.val = i.val + 1 := by scalar_tac
      simp [clist, hnv, hdv]

theorem size_mem_clist (c : Children) (n : Node) (h : n ∈ clist c) :
    Node.size n ≤ Children.size c := by
  induction c using children_ind with
  | nil => simp [clist] at h
  | cons m rest ih =>
    simp only [clist, List.mem_cons] at h
    rcases h with rfl | h
    · simp only [Children.size]
      omega
    · have := ih h
      simp only [Children.size]
      omega

end btree_kernel
