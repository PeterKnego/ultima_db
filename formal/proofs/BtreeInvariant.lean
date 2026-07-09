/- The B-tree sortedness invariant over the Aeneas-generated types.

   `NodeInv lo hi n` says every key in the subtree `n` lies strictly inside the
   open interval `(lo, hi)` (`none` = unbounded), each node's entries are
   strictly sorted, node arity is ≤ MAX_KEYS = 127, and — via
   `ChildrenAligned` — an internal node's children interleave its entries
   positionally: child i holds keys strictly between entry i-1 and entry i.
   `ChildrenAligned` also forces #children = #entries + 1.
-/
import BtreeKernel

open Aeneas Aeneas.Std Result

namespace btree_kernel

/-- Children as a plain list of nodes (model view of the generated cons-list). -/
def clist : Children → List Node
  | .Nil => []
  | .Cons n rest => n :: clist rest

/- Structural sizes, for well-founded induction over the mutual types. -/
mutual
def Node.size : Node → Nat
  | .mk _ c => 1 + Children.size c
def Children.size : Children → Nat
  | .Nil => 0
  | .Cons n rest => Node.size n + Children.size rest
end

/-- `n` lies strictly inside the open interval `(lo, hi)`; `none` = unbounded. -/
def InB (lo hi : Option Nat) (n : Nat) : Prop :=
  (∀ l ∈ lo, l < n) ∧ (∀ h ∈ hi, n < h)

/-- Entries strictly sorted by key. -/
def SortedE (l : List (Std.U64 × Std.U64)) : Prop :=
  List.Pairwise (fun a b => a.1.val < b.1.val) l

mutual
/-- The B-tree invariant for a subtree rooted at a node. -/
inductive NodeInv : Option Nat → Option Nat → Node → Prop where
  | leaf : ∀ {lo hi} (entries : alloc.vec.Vec (Std.U64 × Std.U64)),
      SortedE entries.val →
      (∀ e ∈ entries.val, InB lo hi e.1.val) →
      entries.val.length ≤ 127 →
      NodeInv lo hi (Node.mk entries Children.Nil)
  | internal : ∀ {lo hi} (entries : alloc.vec.Vec (Std.U64 × Std.U64))
      (c : Node) (cs : Children),
      SortedE entries.val →
      (∀ e ∈ entries.val, InB lo hi e.1.val) →
      entries.val.length ≤ 127 →
      Aligned lo hi entries.val (c :: clist cs) →
      NodeInv lo hi (Node.mk entries (Children.Cons c cs))

/-- `Aligned lo hi entries children`: children (as a plain list) interleave
    entries; child i is bounded by (entry i-1, entry i), with the outer bounds
    at the ends. Forces #children = #entries + 1. -/
inductive Aligned : Option Nat → Option Nat →
    List (Std.U64 × Std.U64) → List Node → Prop where
  | last : ∀ {lo hi} (c : Node),
      NodeInv lo hi c →
      Aligned lo hi [] [c]
  | step : ∀ {lo hi} (e : Std.U64 × Std.U64) (es : List (Std.U64 × Std.U64))
      (c : Node) (cs : List Node),
      NodeInv lo (some e.1.val) c →
      Aligned (some e.1.val) hi es cs →
      Aligned lo hi (e :: es) (c :: cs)
end

/-- Lower bound of the child slot at `pos` in an aligned family:
    the outer `lo` for the leftmost slot, else entry `pos - 1`. -/
def lbnd (lo : Option Nat) (es : List (Std.U64 × Std.U64)) (pos : Nat) : Option Nat :=
  if pos = 0 then lo
  else if h : pos - 1 < es.length then some ((es[pos - 1]'h).1.val) else none

/-- Upper bound of the child slot at `pos`: entry `pos`, or the outer `hi`
    for the rightmost slot. -/
def rbnd (hi : Option Nat) (es : List (Std.U64 × Std.U64)) (pos : Nat) : Option Nat :=
  if h : pos < es.length then some ((es[pos]'h).1.val) else hi

/-- Top-level invariant of a tree. -/
def BTreeInv (t : BTree) : Prop := NodeInv none none t.root

end btree_kernel
