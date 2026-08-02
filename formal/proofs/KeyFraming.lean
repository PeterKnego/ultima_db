/- The framing lemma, and order preservation for the variable-length and tuple
   key encodings (`formal/key_kernel/src/lib.rs`, a port of `src/primary_key.rs`),
   stated over the Aeneas translation in `KeyKernel.lean` (generated -- never edit).

   **This is the theorem the whole verification arc exists for.**

   `src/primary_key.rs` originally encoded a tuple by writing a *length prefix*
   ahead of each variable-length element.  That design gets the order wrong:
   `("aa", 0) < ("b", 0)` in value order, but the length-prefixed encoding
   compares `2` against `1` in the very first byte and puts `("b", 0)` first.
   The bug reached review before it was caught.  Replacing the length prefix with
   escape-and-terminate framing (`escape_and_terminate`: every `0x00` becomes
   `0x00 0xFF`, then `0x00 0x01` closes the element) fixes it, and
   `framing_no_confusion` below is the machine-checked reason *why*: comparing
   two framed elements is decided strictly **inside** the framed region, never by
   whatever bytes follow it.

   | theorem | content |
   |---|---|
   | `encode_mono_bytes`   | `encode_bytes` is the identity, hence trivially order preserving |
   | `escaped_byte_order`  | escaping preserves the order of a single differing byte |
   | `framed_prefix_lt_literal` | prefix case, the framed-shorter side wins against a literal byte `≥ 0x01` |
   | `framed_prefix_lt_escaped_zero` | prefix case, the framed-shorter side wins against an escaped `0x00 0xFF` |
   | `framing_no_confusion` | **the load-bearing lemma**: for `a ≠ b`, `lex_lt (frame a ++ sa) (frame b ++ sb) ↔ lex_lt a b`, for arbitrary suffixes |
   | `framing_no_confusion_kernel` | the same, over the translated `escape_and_terminate` rather than the list model |
   | `framed_element_lex`  | the composable corollary: a framed element followed by a suffix compares as the lexicographic product |
   | `encode_mono_pair`    | `(a,x) < (b,y) ↔ lex_lt (encode_pair_bytes_u64 a x) (encode_pair_bytes_u64 b y)` |
   | `encode_mono_triple`  | the same for `encode_triple_bytes_bytes_u64`, where **two** framed elements sit in sequence |

   The two prefix sub-lemmas are the entire correctness argument for choosing
   escape-and-terminate over a length prefix, so they are stated and proved
   separately rather than buried inside the induction.  When `a` is a proper
   prefix of `b`, `frame a` continues with the terminator `0x00 0x01` while
   `frame b` continues with the escaping of `b`'s next byte, which is either

   * a literal byte `≥ 0x01` -- all real zeros having been escaped -- so the
     first byte already decides, `0x00 < that` (`framed_prefix_lt_literal`); or
   * an escaped zero `0x00 0xFF`, so the first bytes tie at `0x00` and the
     second decides, `TERMINATOR = 0x01 < 0xFF = ESCAPE`
     (`framed_prefix_lt_escaped_zero`).

   Both branches put the shorter key first, which is exactly `lex_lt a b` when
   `a` is a proper prefix of `b`.  Note this is *why* `TERMINATOR` must be
   strictly below `ESCAPE`: swap the two constants and
   `framed_prefix_lt_escaped_zero` becomes false, and with it the whole ordering.

   As everywhere in this development, the statements are in the Aeneas `⦃ ⦄`
   form -- total correctness, so they also assert every `encode` call returns
   `ok`: no panic, no overflow, no out-of-bounds index.  The variable-length
   encoders carry the same `Vec`-capacity hypotheses Task 4 established
   (`2 * len + 2 ≤ Usize.max` per framed element, `+ 10` once the eight-byte
   fixed-width tail is appended); `Vec.push` genuinely fails without them.

   `bv_decide` / `native_decide` / `decide` over bitvectors are avoided here for
   the same reason as in `KeyRoundTrip.lean` and `KeyMonoFixed.lean`: in this
   toolchain they discharge by native evaluation and mint an extra axiom. -/
import KeyMonoFixed

open Aeneas Aeneas.Std Result

namespace key_kernel

/-! ## Generic facts about `lex_lt`

`lex_lt` is defined in `KeyMonoFixed.lean`; a reviewer checked it is
interchangeable with Lean core's `List.Lex (· < ·)`, so these are the standard
lexicographic facts, not properties of an approximation. -/

/-- Comparing under a common prefix is comparing the tails. -/
theorem lex_lt_append_left (p x y : List Std.U8) :
    lex_lt (p ++ x) (p ++ y) ↔ lex_lt x y := by
  induction p with
  | nil => simp
  | cons c t ih => simp [ih]

/-- `lex_lt` is asymmetric.  Used to turn the "shorter side wins" sub-lemmas into
    the corresponding negative statements without redoing the case analysis. -/
theorem lex_lt_asymm : ∀ x y : List Std.U8, lex_lt x y → ¬ lex_lt y x := by
  intro x
  induction x with
  | nil =>
    intro y _ hyx
    cases y with
    | nil => simp at hyx
    | cons d u => simp at hyx
  | cons c t ih =>
    intro y hxy hyx
    cases y with
    | nil => simp at hxy
    | cons d u =>
      simp only [lex_lt_cons_cons] at hxy hyx
      rcases hxy with h1 | ⟨he1, h1⟩ <;> rcases hyx with h2 | ⟨he2, h2⟩
      · omega
      · subst he2; omega
      · subst he1; omega
      · exact ih u h1 h2

/-- A byte is nonzero exactly when its value is positive. -/
theorem u8_pos_of_ne_zero {c : Std.U8} (hc : c ≠ 0#u8) : 0 < c.val := by
  rcases Nat.eq_zero_or_pos c.val with h | h
  · exact absurd (Std.UScalar.eq_of_val_eq (by simpa using h)) hc
  · exact h

/-! ## The framed form of an element

`escape_and_terminate b` produces `escList b ++ [0x00, TERMINATOR]`
(`escape_and_terminate_spec`, `KeyRoundTrip.lean`).  `frame` names that list so
the pure-list lemmas below can be stated without dragging the `Result` monad
around. -/

/-- The list model of `escape_and_terminate`: escape every `0x00` as
    `0x00 0xFF`, then close with `0x00 0x01`. -/
def frame (a : List Std.U8) : List Std.U8 := escList a ++ [0#u8, 1#u8]

theorem escList_cons (c : Std.U8) (t : List Std.U8) :
    escList (c :: t) = escOne c ++ escList t := rfl

theorem frame_cons (c : Std.U8) (t s : List Std.U8) :
    frame (c :: t) ++ s = escOne c ++ (frame t ++ s) := by
  simp [frame, escList_cons, List.append_assoc]

theorem frame_nil (s : List Std.U8) : frame [] ++ s = 0#u8 :: 1#u8 :: s := by
  simp [frame, escList]

/-! ## The two prefix sub-lemmas

These are the entire correctness argument for escape-and-terminate.  `a` has run
out, so its framed form continues with the terminator `0x00 0x01`; `b` has at
least one byte left, whose escaping is one of exactly two shapes. -/

/-- **Prefix case 1: the next byte of the longer element is a literal.**

    Every real `0x00` has been escaped, so a literal byte in the escaped stream
    is `≥ 0x01 > 0x00`.  The terminator's leading `0x00` therefore already
    decides the comparison in favour of the shorter (prefix) element -- the first
    byte settles it, with no length ever consulted. -/
theorem framed_prefix_lt_literal (c : Std.U8) (hc : c ≠ 0#u8) (sa rest : List Std.U8) :
    lex_lt (frame [] ++ sa) (escOne c ++ rest) := by
  rw [frame_nil, escOne, if_neg hc]
  simp only [List.cons_append, List.nil_append, lex_lt_cons_cons]
  exact Or.inl (by simpa using u8_pos_of_ne_zero hc)

/-- **Prefix case 2: the next byte of the longer element is an escaped zero.**

    The escaped zero is `0x00 0xFF`, so its first byte ties with the
    terminator's leading `0x00` and the *second* byte decides:
    `TERMINATOR = 0x01 < 0xFF = ESCAPE`.  Again the shorter (prefix) element
    sorts first.

    This is the case that pins the choice of constants: `TERMINATOR` must be
    strictly less than `ESCAPE`, or this lemma -- and the ordering -- fails. -/
theorem framed_prefix_lt_escaped_zero (sa rest : List Std.U8) :
    lex_lt (frame [] ++ sa) (escOne 0#u8 ++ rest) := by
  have hlt : (1#u8 : Std.U8).val < (255#u8 : Std.U8).val := by scalar_tac
  rw [frame_nil, escOne, if_pos rfl]
  simp only [List.cons_append, List.nil_append]
  exact Or.inr ⟨rfl, Or.inl hlt⟩

/-- The two cases combined: an element that has run out frames strictly below any
    element that has not, whatever suffixes follow either. -/
theorem framed_nil_lt_framed_cons (c : Std.U8) (t sa sb : List Std.U8) :
    lex_lt (frame [] ++ sa) (frame (c :: t) ++ sb) := by
  rw [frame_cons]
  by_cases hc : c = 0#u8
  · subst hc; exact framed_prefix_lt_escaped_zero _ _
  · exact framed_prefix_lt_literal c hc _ _

/-! ## Escaping preserves byte order -/

/-- When two elements first differ at a byte, the escaped forms differ there too,
    and in the same direction.  `0x00` escapes to `0x00 0xFF`, whose *first* byte
    is still `0x00`, so the escaping never reorders a differing byte. -/
theorem escaped_byte_order (c d : Std.U8) (hcd : c ≠ d) (X Y : List Std.U8) :
    lex_lt (escOne c ++ X) (escOne d ++ Y) ↔ c.val < d.val := by
  by_cases hc : c = 0#u8 <;> by_cases hd : d = 0#u8
  · exact absurd (hc.trans hd.symm) hcd
  · subst hc
    rw [escOne, if_pos rfl, escOne, if_neg hd]
    simp only [List.cons_append, List.nil_append, lex_lt_cons_cons]
    have hdpos : 0 < d.val := u8_pos_of_ne_zero hd
    simp only [Std.UScalar.val] at hdpos ⊢
    exact ⟨fun _ => by simpa using hdpos, fun _ => Or.inl (by simpa using hdpos)⟩
  · subst hd
    rw [escOne, if_neg hc, escOne, if_pos rfl]
    simp only [List.cons_append, List.nil_append, lex_lt_cons_cons]
    constructor
    · rintro (h | ⟨h, -⟩)
      · exact h
      · exact absurd h hc
    · intro h
      exact absurd h (by simp)
  · rw [escOne, if_neg hc, escOne, if_neg hd]
    simp only [List.cons_append, List.nil_append, lex_lt_cons_cons]
    constructor
    · rintro (h | ⟨h, -⟩)
      · exact h
      · exact absurd h hcd
    · exact Or.inl

/-! ## The framing lemma -/

/-- **`framing_no_confusion` — the theorem this arc exists for.**

    For two *distinct* elements `a` and `b`, comparing their framed forms
    followed by *arbitrary* suffixes is exactly comparing `a` and `b`.  The
    suffixes are universally quantified and appear nowhere in the conclusion:
    the comparison is decided strictly inside the framed region.

    That is precisely what the length-prefix design failed to do.  With a length
    prefix, `("aa", 0)` and `("b", 0)` are decided by the leading `2` vs `1`
    before a single content byte is read, inverting the value order.  Here, no
    such byte exists, and the two prefix sub-lemmas above show the framed region
    always resolves the comparison the way `lex_lt a b` does. -/
theorem framing_no_confusion : ∀ a b sa sb : List Std.U8, a ≠ b →
    (lex_lt (frame a ++ sa) (frame b ++ sb) ↔ lex_lt a b) := by
  intro a
  induction a with
  | nil =>
    intro b sa sb hne
    cases b with
    | nil => exact absurd rfl hne
    | cons d u =>
      simp only [lex_lt_nil_cons, iff_true]
      exact framed_nil_lt_framed_cons d u sa sb
  | cons c t ih =>
    intro b sa sb hne
    cases b with
    | nil =>
      simp only [lex_lt_nil_right, iff_false]
      exact lex_lt_asymm _ _ (framed_nil_lt_framed_cons c t sb sa)
    | cons d u =>
      rw [frame_cons, frame_cons]
      by_cases hcd : c = d
      · subst hcd
        have htu : t ≠ u := fun h => hne (by rw [h])
        rw [lex_lt_append_left, ih u sa sb htu]
        simp
      · rw [escaped_byte_order c d hcd]
        simp only [lex_lt_cons_cons]
        exact ⟨Or.inl, fun h => h.resolve_right (fun hr => absurd hr.1 hcd)⟩

/-- The composable corollary: a framed element followed by a suffix compares
    exactly as the lexicographic product of (element, suffix).  Both tuple
    monotonicity theorems below are immediate from this, applied once per framed
    element.

    The `a = b` half is plain prefix cancellation; the `a ≠ b` half is
    `framing_no_confusion`. -/
theorem framed_element_lex (a b sa sb : List Std.U8) :
    lex_lt (frame a ++ sa) (frame b ++ sb) ↔ (lex_lt a b ∨ (a = b ∧ lex_lt sa sb)) := by
  by_cases hab : a = b
  · subst hab
    rw [lex_lt_append_left]
    constructor
    · intro h; exact Or.inr ⟨rfl, h⟩
    · rintro (h | ⟨-, h⟩)
      · exact absurd h (lex_lt_irrefl a)
      · exact h
  · rw [framing_no_confusion a b sa sb hab]
    constructor
    · exact Or.inl
    · rintro (h | ⟨h, -⟩)
      · exact h
      · exact absurd h hab

/-! ## `encode_bytes`

Stated for completeness so the README table is honest about what is and is not
proved: `encode_bytes` is the identity, so it preserves order trivially.  It is
the `Vec<u8>` / `String` primary-key case (`String::encode` is
`self.as_bytes().to_vec()`, and `Ord for String` is bytewise over UTF-8). -/

theorem encode_bytes_spec (b : Slice Std.U8) :
    encode_bytes b ⦃ out => out.val = b.val ⦄ := by
  simp only [encode_bytes]
  apply WP.spec_mono (alloc.slice.Slice.to_vec_spec core.clone.CloneU8 b (by intros; rfl))
  intro out hout
  rw [← hout]

/-- `encode_bytes` is an order embedding -- trivially, since it is the identity. -/
theorem encode_mono_bytes (a b : Slice Std.U8) :
    encode_bytes a ⦃ ea =>
      encode_bytes b ⦃ eb => (lex_lt a.val b.val ↔ lex_lt ea.val eb.val) ⦄ ⦄ := by
  apply WP.spec_mono (encode_bytes_spec a)
  intro ea hea
  apply WP.spec_mono (encode_bytes_spec b)
  intro eb heb
  rw [hea, heb]

/-! ## `framing_no_confusion` over the kernel

The pure-list lemma above, transported to the actual translated
`escape_and_terminate` through `escape_and_terminate_spec`. -/

/-- `escape_and_terminate` produces exactly `frame`. -/
theorem escape_and_terminate_frame (bytes : Slice Std.U8)
    (hcap : 2 * bytes.val.length + 2 ≤ Std.Usize.max) :
    escape_and_terminate bytes ⦃ out => out.val = frame bytes.val ⦄ :=
  escape_and_terminate_spec bytes hcap

/-- **The framing lemma, over the kernel.**  Two distinct elements, escaped and
    terminated by the actual translated Rust, compare the way the elements do --
    for every pair of suffixes.  Being in `⦃ ⦄` form this also asserts both
    `escape_and_terminate` calls return `ok`. -/
theorem framing_no_confusion_kernel (a b : Slice Std.U8) (sa sb : List Std.U8)
    (hne : a.val ≠ b.val)
    (hca : 2 * a.val.length + 2 ≤ Std.Usize.max)
    (hcb : 2 * b.val.length + 2 ≤ Std.Usize.max) :
    escape_and_terminate a ⦃ fa =>
      escape_and_terminate b ⦃ fb =>
        (lex_lt (fa.val ++ sa) (fb.val ++ sb) ↔ lex_lt a.val b.val) ⦄ ⦄ := by
  apply WP.spec_mono (escape_and_terminate_frame a hca)
  intro fa hfa
  apply WP.spec_mono (escape_and_terminate_frame b hcb)
  intro fb hfb
  rw [hfa, hfb]
  exact framing_no_confusion a.val b.val sa sb hne

/-! ## The pair encoding -/

/-- `encode_pair_bytes_u64 a x` is the framed `a` followed by the eight
    big-endian bytes of `x`. -/
theorem encode_pair_bytes_u64_spec (a : Slice Std.U8) (x : Std.U64)
    (hcap : 2 * a.val.length + 10 ≤ Std.Usize.max) :
    encode_pair_bytes_u64 a x ⦃ out => out.val = frame a.val ++ u64bytes x ⦄ := by
  simp only [encode_pair_bytes_u64]
  apply WP.spec_bind (escape_and_terminate_frame a (by omega))
  intro framed hframed
  have hflen : framed.val.length ≤ 2 * a.val.length + 2 := by
    rw [hframed, frame]
    have := escList_length_le a.val
    simp only [List.length_append, List.length_cons, List.length_nil]
    omega
  apply WP.spec_bind (encode_u64_spec x)
  intro tail htail
  have htlen : tail.val.length = 8 := by rw [htail]; simp
  apply WP.spec_mono (encode_pair_bytes_u64_loop_spec framed tail 0#usize (by simp; omega))
  intro enc henc
  rw [henc, hframed, htail]
  simp

/-- **`encode_mono_pair`.**  The lexicographic product order on
    `(Vec<u8>, u64)` is carried exactly onto the bytewise order of the encoding.

    This is the concrete statement the original bug violated: with
    `a = "aa", x = 0` and `b = "b", y = 0` the left side holds (`"aa" < "b"`),
    so the right side must too -- and under the length-prefix encoding it did
    not. -/
theorem encode_mono_pair (a : Slice Std.U8) (x : Std.U64) (b : Slice Std.U8) (y : Std.U64)
    (hca : 2 * a.val.length + 10 ≤ Std.Usize.max)
    (hcb : 2 * b.val.length + 10 ≤ Std.Usize.max) :
    encode_pair_bytes_u64 a x ⦃ ea =>
      encode_pair_bytes_u64 b y ⦃ eb =>
        ((lex_lt a.val b.val ∨ (a.val = b.val ∧ x.val < y.val))
          ↔ lex_lt ea.val eb.val) ⦄ ⦄ := by
  apply WP.spec_mono (encode_pair_bytes_u64_spec a x hca)
  intro ea hea
  apply WP.spec_mono (encode_pair_bytes_u64_spec b y hcb)
  intro eb heb
  rw [hea, heb, framed_element_lex, ← u64bytes_lex]

/-! ## The triple encoding

The 2-tuple has a single framed element, so it never puts two framed regions in
sequence.  The 3-tuple does, and that is the shape a real composite primary key
takes.  It is proved by applying `framed_element_lex` twice -- once at each
boundary -- which is only sound because `framing_no_confusion` quantifies over
*arbitrary* suffixes: at the first boundary the suffix is itself a framed element
followed by eight raw bytes. -/

/-- The first copy loop of `encode_triple_bytes_bytes_u64` (appending the second
    framed element).  Same shape as `encode_pair_bytes_u64_loop`, but Aeneas
    emits a separate definition per syntactic loop. -/
theorem encode_triple_bytes_bytes_u64_loop0_spec (out0 mid : alloc.vec.Vec Std.U8)
    (i0 : Std.Usize)
    (hcap : out0.val.length + (mid.val.length - i0.val) ≤ Std.Usize.max) :
    encode_triple_bytes_bytes_u64_loop0 out0 mid i0
      ⦃ r => r.val = out0.val ++ mid.val.drop i0.val ⦄ := by
  simp only [encode_triple_bytes_bytes_u64_loop0]
  have h := loop.spec_decr_nat
      (fun (x : (alloc.vec.Vec Std.U8) × Std.Usize) => mid.val.length - x.2.val)
      (fun x => x.1.val ++ mid.val.drop x.2.val = out0.val ++ mid.val.drop i0.val)
      (fun r => r.val = out0.val ++ mid.val.drop i0.val)
      (fun (out1, i1) => encode_triple_bytes_bytes_u64_loop0.body mid out1 i1)
      (out0, i0)
      ?hbody ?hinv
  case hinv => simp
  case hbody =>
    rintro ⟨out, i⟩ hinv0
    have hinv : out.val ++ mid.val.drop i.val = out0.val ++ mid.val.drop i0.val := hinv0
    have hlen := congrArg List.length hinv
    simp at hlen
    simp only [encode_triple_bytes_bytes_u64_loop0.body, alloc.vec.Vec.index_slice_index]
    split
    · apply WP.spec_bind (alloc.vec.Vec.index_usize_spec mid i (by scalar_tac))
      intro x hx
      apply WP.spec_bind (alloc.vec.Vec.push_spec out x (by scalar_tac))
      intro out1 hout1
      apply WP.spec_bind (Usize.add_spec (x := i) (y := 1#usize) (by scalar_tac))
      intro i3 hi3
      have hi3' : i3.val = i.val + 1 := by scalar_tac
      simp only [WP.spec_ok]
      refine ⟨?_, by scalar_tac⟩
      show out1.val ++ mid.val.drop i3.val = out0.val ++ mid.val.drop i0.val
      rw [hout1, hx, hi3', List.append_assoc, List.singleton_append,
        ← List.drop_eq_getElem_cons (by scalar_tac)]
      exact hinv
    · simp only [WP.spec_ok]
      show out.val = out0.val ++ mid.val.drop i0.val
      have hd : mid.val.drop i.val = [] := by
        apply List.drop_eq_nil_of_le; scalar_tac
      rw [← hinv, hd, List.append_nil]
  exact h

/-- The second copy loop of `encode_triple_bytes_bytes_u64` (appending the raw
    fixed-width tail). -/
theorem encode_triple_bytes_bytes_u64_loop1_spec (out0 tail : alloc.vec.Vec Std.U8)
    (j0 : Std.Usize)
    (hcap : out0.val.length + (tail.val.length - j0.val) ≤ Std.Usize.max) :
    encode_triple_bytes_bytes_u64_loop1 out0 tail j0
      ⦃ r => r.val = out0.val ++ tail.val.drop j0.val ⦄ := by
  simp only [encode_triple_bytes_bytes_u64_loop1]
  have h := loop.spec_decr_nat
      (fun (x : (alloc.vec.Vec Std.U8) × Std.Usize) => tail.val.length - x.2.val)
      (fun x => x.1.val ++ tail.val.drop x.2.val = out0.val ++ tail.val.drop j0.val)
      (fun r => r.val = out0.val ++ tail.val.drop j0.val)
      (fun (out1, j1) => encode_triple_bytes_bytes_u64_loop1.body tail out1 j1)
      (out0, j0)
      ?hbody ?hinv
  case hinv => simp
  case hbody =>
    rintro ⟨out, j⟩ hinv0
    have hinv : out.val ++ tail.val.drop j.val = out0.val ++ tail.val.drop j0.val := hinv0
    have hlen := congrArg List.length hinv
    simp at hlen
    simp only [encode_triple_bytes_bytes_u64_loop1.body, alloc.vec.Vec.index_slice_index]
    split
    · apply WP.spec_bind (alloc.vec.Vec.index_usize_spec tail j (by scalar_tac))
      intro x hx
      apply WP.spec_bind (alloc.vec.Vec.push_spec out x (by scalar_tac))
      intro out1 hout1
      apply WP.spec_bind (Usize.add_spec (x := j) (y := 1#usize) (by scalar_tac))
      intro j3 hj3
      have hj3' : j3.val = j.val + 1 := by scalar_tac
      simp only [WP.spec_ok]
      refine ⟨?_, by scalar_tac⟩
      show out1.val ++ tail.val.drop j3.val = out0.val ++ tail.val.drop j0.val
      rw [hout1, hx, hj3', List.append_assoc, List.singleton_append,
        ← List.drop_eq_getElem_cons (by scalar_tac)]
      exact hinv
    · simp only [WP.spec_ok]
      show out.val = out0.val ++ tail.val.drop j0.val
      have hd : tail.val.drop j.val = [] := by
        apply List.drop_eq_nil_of_le; scalar_tac
      rw [← hinv, hd, List.append_nil]
  exact h

/-- `encode_triple_bytes_bytes_u64 a b c` is the framed `a`, then the framed `b`,
    then the eight big-endian bytes of `c`. -/
theorem encode_triple_bytes_bytes_u64_spec (a b : Slice Std.U8) (c : Std.U64)
    (hcap : 2 * a.val.length + 2 * b.val.length + 12 ≤ Std.Usize.max) :
    encode_triple_bytes_bytes_u64 a b c
      ⦃ out => out.val = frame a.val ++ (frame b.val ++ u64bytes c) ⦄ := by
  simp only [encode_triple_bytes_bytes_u64]
  apply WP.spec_bind (escape_and_terminate_frame a (by omega))
  intro fa hfa
  have hfalen : fa.val.length ≤ 2 * a.val.length + 2 := by
    rw [hfa, frame]
    have := escList_length_le a.val
    simp only [List.length_append, List.length_cons, List.length_nil]
    omega
  apply WP.spec_bind (escape_and_terminate_frame b (by omega))
  intro fb hfb
  have hfblen : fb.val.length ≤ 2 * b.val.length + 2 := by
    rw [hfb, frame]
    have := escList_length_le b.val
    simp only [List.length_append, List.length_cons, List.length_nil]
    omega
  apply WP.spec_bind (encode_triple_bytes_bytes_u64_loop0_spec fa fb 0#usize (by simp; omega))
  intro mid hmid
  have hmid' : mid.val = fa.val ++ fb.val := by simpa using hmid
  have hmidlen : mid.val.length ≤ 2 * a.val.length + 2 * b.val.length + 4 := by
    rw [hmid']; simp only [List.length_append]; omega
  apply WP.spec_bind (encode_u64_spec c)
  intro tail htail
  have htlen : tail.val.length = 8 := by rw [htail]; simp
  apply WP.spec_mono (encode_triple_bytes_bytes_u64_loop1_spec mid tail 0#usize (by simp; omega))
  intro enc henc
  rw [henc, hmid', hfa, hfb, htail]
  simp

/-- **`encode_mono_triple`.**  The lexicographic product order on
    `(Vec<u8>, Vec<u8>, u64)` is carried exactly onto the bytewise order of the
    encoding -- two framed elements in sequence, then a raw fixed-width tail.

    The second `framed_element_lex` application is the one the pair encoding
    never exercises: it fires under the hypothesis that the *first* elements are
    equal, so the comparison must be resolved by the second framed region without
    the `u64` tail interfering.  That is exactly `framing_no_confusion` with a
    non-empty suffix. -/
theorem encode_mono_triple (a b : Slice Std.U8) (x : Std.U64)
    (c d : Slice Std.U8) (y : Std.U64)
    (hcl : 2 * a.val.length + 2 * b.val.length + 12 ≤ Std.Usize.max)
    (hcr : 2 * c.val.length + 2 * d.val.length + 12 ≤ Std.Usize.max) :
    encode_triple_bytes_bytes_u64 a b x ⦃ el =>
      encode_triple_bytes_bytes_u64 c d y ⦃ er =>
        ((lex_lt a.val c.val ∨
            (a.val = c.val ∧ (lex_lt b.val d.val ∨ (b.val = d.val ∧ x.val < y.val))))
          ↔ lex_lt el.val er.val) ⦄ ⦄ := by
  apply WP.spec_mono (encode_triple_bytes_bytes_u64_spec a b x hcl)
  intro el hel
  apply WP.spec_mono (encode_triple_bytes_bytes_u64_spec c d y hcr)
  intro er her
  rw [hel, her, framed_element_lex, framed_element_lex, ← u64bytes_lex]

/-! ## Axiom audit -/

#print axioms encode_mono_bytes
#print axioms framed_prefix_lt_literal
#print axioms framed_prefix_lt_escaped_zero
#print axioms escaped_byte_order
#print axioms framing_no_confusion
#print axioms framing_no_confusion_kernel
#print axioms framed_element_lex
#print axioms encode_mono_pair
#print axioms encode_mono_triple

end key_kernel
