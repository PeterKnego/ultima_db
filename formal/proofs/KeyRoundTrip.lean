/- Round-trip theorems for the order-preserving key encoding
   (`formal/key_kernel/src/lib.rs`, a port of `src/primary_key.rs`), stated over
   the Aeneas translation in `KeyKernel.lean` (generated -- never edit).

   `decode (encode x) = x` for `u64`, `i64`, the escape/terminate framing, and
   the `(bytes, u64)` pair:

   | theorem | content |
   |---|---|
   | `decode_encode_u64`  | `decode_u64 (encode_u64 v) = some v` |
   | `decode_encode_i64`  | `decode_i64 (encode_i64 v) = some v` (through the sign flip) |
   | `escape_prefix_scan` | the scan stops at the first terminator and ignores any suffix |
   | `unescape_escape`    | `unescape_until_terminator (escape_and_terminate b) 0 = some (b, len)` |
   | `decode_encode_pair` | `decode_pair_bytes_u64 (encode_pair_bytes_u64 a v) = some (a, v)` |

   Every statement is in the Aeneas `⦃ ⦄` form, so it also asserts that `encode`
   returns `ok` -- no panic, no overflow, no out-of-bounds index.

   Two things the Rust does not make visible and that the statements must carry:

   * Inputs are `Slice Std.U8` but outputs are `alloc.vec.Vec Std.U8`; the two
     are bridged by `alloc.vec.Vec.deref`, and equality of a returned `Vec` with
     an input `Slice` is stated on the shared `.val : List U8`.
   * The variable-length encoders can *genuinely fail*: `Vec.push` fails once a
     vector reaches `Usize.max`, and escaping can double the length. The
     theorems about them therefore carry a capacity hypothesis
     (`2 * len + 2 ≤ Usize.max`, `2 * len + 10 ≤ Usize.max`), which is the
     weakest bound the reachable `push`es actually need. The fixed-width `u64`
     encoders need no such hypothesis. -/
import KeyKernel

open Aeneas Aeneas.Std Result

namespace key_kernel

/-! ## Pure models of the encodings -/

/-- `decode_u64` hands back a scalar built with the raw constructor; this is the
    eta rule that turns it back into the scalar it came from. -/
theorem UScalar_eta {ty : Std.UScalarTy} (x : Std.UScalar ty) :
    (Std.UScalar.mk x.bv) = x := rfl

/-- `simp` does not see through the `HXor` instance on its own. -/
theorem uscalar_xor_bv {ty : Std.UScalarTy} (x y : Std.UScalar ty) :
    (x ^^^ y).bv = x.bv ^^^ y.bv := rfl

/-- The `j`-th big-endian byte of `v`, as the translation computes it:
    `((v >>> (8*(7-j))) & 0xFF) as u8`. -/
def u64byteBv (v : Std.U64) (j : Nat) : BitVec 8 :=
  ((v.bv >>> (8 * (7 - j))) &&& 255#64).setWidth 8

def u64byte (v : Std.U64) (j : Nat) : Std.U8 := ⟨ u64byteBv v j ⟩

@[simp] theorem u64byte_bv (v : Std.U64) (j : Nat) : (u64byte v j).bv = u64byteBv v j := rfl

/-- The eight bytes `encode_u64` pushes, in order. -/
def u64bytes (v : Std.U64) : List Std.U8 := (List.range 8).map (u64byte v)

/-- The big-endian fold `decode_u64` performs: `acc := (acc <<< 8) ||| b`. -/
def beFold : List Std.U8 → BitVec 64 → BitVec 64
  | [], acc => acc
  | b :: l, acc => beFold l ((acc <<< 8) ||| b.bv.setWidth 64)

/-- The escaping half of `escape_and_terminate` (without the trailing
    `[0x00, TERMINATOR]`). -/
def escList : List Std.U8 → List Std.U8
  | [] => []
  | b :: l => (if b = 0#u8 then [0#u8, 255#u8] else [b]) ++ escList l

/-! ## `encode_u64` / `decode_u64` -/

@[simp] theorem u64bytes_length (v : Std.U64) : (u64bytes v).length = 8 := by
  simp [u64bytes]

theorem u64bytes_getElem (v : Std.U64) (j : Nat) (h : j < 8) :
    (u64bytes v)[j]'(by simp [h]) = u64byte v j := by
  simp [u64bytes]

/-- The encoding loop fills `out` with `u64bytes v`, one byte per iteration. -/
theorem encode_u64_loop_spec (v : Std.U64) :
    encode_u64_loop v (alloc.vec.Vec.new Std.U8) 0#usize ⦃ out => out.val = u64bytes v ⦄ := by
  simp only [encode_u64_loop]
  have h := loop.spec_decr_nat
      (fun (x : (alloc.vec.Vec Std.U8) × Std.Usize) => 8 - x.2.val)
      (fun x => x.1.val = (u64bytes v).take x.2.val ∧ x.2.val ≤ 8)
      (fun out => out.val = u64bytes v)
      (fun (out1, i1) => encode_u64_loop.body v out1 i1)
      (alloc.vec.Vec.new Std.U8, 0#usize)
      ?hbody ?hinv
  case hinv => simp
  case hbody =>
    rintro ⟨out, i⟩ hinv0
    have hout : out.val = (u64bytes v).take i.val := hinv0.1
    have hile : i.val ≤ 8 := hinv0.2
    have hlen := congrArg List.length hout
    simp at hlen
    simp only [encode_u64_loop.body]
    split
    · rename_i hlt
      have hi8 : i.val < 8 := by scalar_tac
      apply WP.spec_bind (Usize.sub_spec (x := 7#usize) (y := i) (by scalar_tac))
      intro i1 hi1
      have hi1' : i1.val = 7 - i.val := by scalar_tac
      apply WP.spec_bind (Usize.mul_spec (x := 8#usize) (y := i1) (by scalar_tac))
      intro shift hshift
      have hshift' : shift.val = 8 * (7 - i.val) := by scalar_tac
      apply WP.spec_bind (U64.ShiftRight_spec v shift (by scalar_tac))
      intro i2 hi2
      apply WP.spec_bind (UScalar.and_spec i2 255#u64)
      intro i3 hi3
      simp only [lift, bind_tc_ok]
      apply WP.spec_bind (alloc.vec.Vec.push_spec out (UScalar.cast .U8 i3) (by scalar_tac))
      intro out1 hout1
      apply WP.spec_bind (Usize.add_spec (x := i) (y := 1#usize) (by scalar_tac))
      intro i5 hi5
      have hi5' : i5.val = i.val + 1 := by scalar_tac
      simp only [WP.spec_ok]
      refine ⟨⟨?_, by scalar_tac⟩, by scalar_tac⟩
      show out1.val = (u64bytes v).take i5.val
      have hbyte : UScalar.cast .U8 i3 = u64byte v i.val := by
        apply U8.bv_eq_imp_eq
        simp only [UScalar.cast, u64byte, u64byteBv, hi3.2, hi2.2, hshift']
        rfl
      rw [hout1, hout, hbyte, hi5', List.take_add_one,
        List.getElem?_eq_getElem (by simp [hi8])]
      simp [u64bytes_getElem v i.val hi8]
    · rename_i hge
      have : i.val = 8 := by scalar_tac
      simp only [WP.spec_ok]
      show out.val = u64bytes v
      rw [hout, this]
      simp
  exact h

theorem encode_u64_spec (v : Std.U64) :
    encode_u64 v ⦃ out => out.val = u64bytes v ⦄ := by
  simp only [encode_u64]
  exact encode_u64_loop_spec v

/-- The decoding loop folds the remaining bytes into the accumulator. -/
theorem decode_u64_loop_spec (b : Slice Std.U8) (acc0 : Std.U64) (i0 : Std.Usize)
    (hlen : b.val.length = 8) (hi0 : i0.val ≤ 8) :
    decode_u64_loop b acc0 i0 ⦃ acc => acc.bv = beFold (b.val.drop i0.val) acc0.bv ⦄ := by
  simp only [decode_u64_loop]
  have h := loop.spec_decr_nat
      (fun (x : Std.U64 × Std.Usize) => 8 - x.2.val)
      (fun x => x.2.val ≤ 8 ∧
        beFold (b.val.drop x.2.val) x.1.bv = beFold (b.val.drop i0.val) acc0.bv)
      (fun acc => acc.bv = beFold (b.val.drop i0.val) acc0.bv)
      (fun (acc1, i1) => decode_u64_loop.body b acc1 i1)
      (acc0, i0)
      ?hbody ?hinv
  case hinv => exact ⟨hi0, rfl⟩
  case hbody =>
    rintro ⟨acc, i⟩ ⟨hile, hfold⟩
    simp only [decode_u64_loop.body]
    split
    · rename_i hlt
      have hi8 : i.val < 8 := by scalar_tac
      apply WP.spec_bind (U64.ShiftLeft_IScalar_spec acc 8#i32 (by scalar_tac) (by scalar_tac))
      intro i1 hi1
      apply WP.spec_bind (Slice.index_usize_spec b i (by simp [hlen]; scalar_tac))
      intro i2 hi2
      simp only [lift, bind_tc_ok]
      apply WP.spec_bind (Usize.add_spec (x := i) (y := 1#usize) (by scalar_tac))
      intro i4 hi4
      have hi4' : i4.val = i.val + 1 := by scalar_tac
      simp only [WP.spec_ok]
      refine ⟨⟨by scalar_tac, ?_⟩, by scalar_tac⟩
      have hdrop : b.val.drop i.val
          = b.val[i.val]'(by simp [hlen]; omega) :: b.val.drop (i.val + 1) :=
        List.drop_eq_getElem_cons (by simp [hlen]; omega)
      have hbv : (UScalar.or i1 (UScalar.cast .U64 i2)).bv
          = (acc.bv <<< 8) ||| (b.val[i.val]'(by simp [hlen]; omega)).bv.setWidth 64 := by
        simp only [UScalar.or, UScalar.cast, hi1.2, hi2]
        rfl
      rw [hi4', ← hfold, hdrop]
      simp only [beFold]
      congr 1
    · rename_i hge
      have hi : i.val = 8 := by scalar_tac
      simp only [WP.spec_ok]
      rw [← hfold, hi]
      have : b.val.drop 8 = [] := by simp [hlen]
      rw [this]
      simp [beFold]
  exact h

theorem decode_u64_eq (b : Slice Std.U8) (hlen : b.val.length = 8) :
    decode_u64 b = ok (some ⟨beFold b.val 0#64⟩) := by
  obtain ⟨acc, hacc, hpost⟩ :=
    WP.spec_imp_exists (decode_u64_loop_spec b 0#u64 0#usize hlen (by simp))
  simp only [decode_u64]
  have hlen8 : Slice.len b = 8#usize := by
    apply UScalar.eq_of_val_eq; simp [hlen]
  rw [hlen8]
  simp only [bne_self_eq_false, Bool.false_eq_true, reduceIte, hacc, bind_tc_ok]
  congr 1
  apply congrArg
  apply U64.bv_eq_imp_eq
  simpa using hpost

/-! ### The bit arithmetic behind the fixed-width round trip

`bv_decide` is deliberately not used here: in this Lean toolchain it discharges
its SAT certificate with native evaluation and therefore adds a
`…_native.bv_decide.ax…` axiom, which the project's axiom budget
(`propext`, `Classical.choice`, `Quot.sound`) does not allow. -/

theorem getLsbD_255 (i : Nat) : (255#64).getLsbD i = decide (i < 8) := by
  have e : (255#64 : BitVec 64) = BitVec.ofNat 64 (2 ^ 8 - 1) := rfl
  rw [e, BitVec.getLsbD_ofNat, Nat.testBit_two_pow_sub_one]
  by_cases h : i < 8
  · simp [h]
    omega
  · simp [h]

/-- Splitting off the low byte and shifting it back in is the identity. -/
theorem or_shift_step (y : BitVec 64) : ((y >>> 8) <<< 8) ||| (y &&& 255#64) = y := by
  apply BitVec.eq_of_getLsbD_eq
  intro i hi
  simp only [BitVec.getLsbD_or, BitVec.getLsbD_shiftLeft, BitVec.getLsbD_ushiftRight,
    BitVec.getLsbD_and, getLsbD_255]
  by_cases h : i < 8
  · simp [h]
  · rw [show 8 + (i - 8) = i by omega]
    simp [h, hi]

theorem ushiftRight_64 (y : BitVec 64) : y >>> 64 = 0#64 := by
  apply BitVec.eq_of_getLsbD_eq
  intro i hi
  simp

/-- The `& 0xFF` in the Rust is redundant once the value is truncated to `u8`. -/
theorem setWidth_and_255 (x : BitVec 64) :
    BitVec.setWidth 64 (BitVec.setWidth 8 (x &&& 255#64)) = x &&& 255#64 := by
  apply BitVec.eq_of_getLsbD_eq
  intro i hi
  simp only [BitVec.getLsbD_setWidth, BitVec.getLsbD_and, getLsbD_255]
  by_cases h : i < 8 <;> simp [h, hi]

/-- One decoding step: the accumulator so far, shifted up, or'd with the next
    byte, is the value shifted right by one byte less. -/
theorem beFold_step (x : BitVec 64) (m n : Nat) (h : m = n + 8) :
    ((x >>> m) <<< 8) ||| ((x >>> n) &&& 255#64) = x >>> n := by
  subst h
  have e : x >>> (n + 8) = (x >>> n) >>> 8 := by
    apply BitVec.eq_of_getLsbD_eq
    intro i hi
    simp [Nat.add_comm, Nat.add_left_comm]
  rw [e]
  exact or_shift_step _

/-- The arithmetic core: reassembling the eight big-endian bytes gives `v` back. -/
theorem u64bytes_eq (v : Std.U64) :
    u64bytes v = [u64byte v 0, u64byte v 1, u64byte v 2, u64byte v 3,
                  u64byte v 4, u64byte v 5, u64byte v 6, u64byte v 7] := by
  rfl

theorem beFold_u64bytes (v : Std.U64) : beFold (u64bytes v) 0#64 = v.bv := by
  rw [u64bytes_eq]
  simp only [beFold, u64byte_bv, u64byteBv, setWidth_and_255]
  rw [(ushiftRight_64 v.bv).symm]
  rw [beFold_step v.bv 64 56 (by omega), beFold_step v.bv 56 48 (by omega),
    beFold_step v.bv 48 40 (by omega), beFold_step v.bv 40 32 (by omega),
    beFold_step v.bv 32 24 (by omega), beFold_step v.bv 24 16 (by omega),
    beFold_step v.bv 16 8 (by omega), beFold_step v.bv 8 0 (by omega)]
  simp

/-! ## The four round-trip theorems -/

/-- `decode_u64 ∘ encode_u64 = some`. -/
theorem decode_encode_u64 (v : Std.U64) :
    encode_u64 v ⦃ out => decode_u64 (alloc.vec.Vec.deref out) = ok (some v) ⦄ := by
  apply WP.spec_mono (encode_u64_spec v)
  intro out hout
  have hlen : (alloc.vec.Vec.deref out).val.length = 8 := by
    simp only [alloc.vec.Vec.deref, hout]; simp
  rw [decode_u64_eq _ hlen]
  simp only [alloc.vec.Vec.deref, hout, beFold_u64bytes]

/-- `decode_i64 ∘ encode_i64 = some`, through the sign-bit flip. -/
theorem decode_encode_i64 (v : Std.I64) :
    encode_i64 v ⦃ out => decode_i64 (alloc.vec.Vec.deref out) = ok (some v) ⦄ := by
  obtain ⟨k, hk, hkbv⟩ := WP.spec_imp_exists
    (U64.ShiftLeft_IScalar_spec 1#u64 63#i32 (by scalar_tac) (by scalar_tac))
  -- xor with any constant is involutive, so the sign-bit flip undoes itself.
  have hflip : UScalar.hcast Std.IScalarTy.I64
      ((IScalar.hcast Std.UScalarTy.U64 v ^^^ k) ^^^ k) = v := by
    apply I64.bv_eq_imp_eq
    simp only [UScalar.hcast, IScalar.hcast, uscalar_xor_bv,
      Std.IScalarTy.numBits, Std.UScalarTy.numBits]
    simp [BitVec.xor_assoc]
  simp only [encode_i64, lift, bind_tc_ok, hk]
  apply WP.spec_mono (encode_u64_spec _)
  intro out hout
  have hlen : (alloc.vec.Vec.deref out).val.length = 8 := by
    simp only [alloc.vec.Vec.deref, hout]; simp
  simp only [decode_i64]
  rw [decode_u64_eq _ hlen]
  simp only [alloc.vec.Vec.deref, hout, beFold_u64bytes, UScalar_eta,
    bind_tc_ok, hk, lift, hflip]

/-! ## `escape_and_terminate` / `unescape_until_terminator` -/

unseal ESCAPE TERMINATOR in
theorem ESCAPE_eq : ESCAPE = 255#u8 := rfl

unseal ESCAPE TERMINATOR in
theorem TERMINATOR_eq : TERMINATOR = 1#u8 := rfl

/-- The escaped form of a single byte. -/
def escOne (b : Std.U8) : List Std.U8 := if b = 0#u8 then [0#u8, 255#u8] else [b]

theorem escList_append (l : List Std.U8) (x : Std.U8) :
    escList (l ++ [x]) = escList l ++ escOne x := by
  induction l with
  | nil => simp [escList, escOne]
  | cons c t ih => simp only [List.cons_append, escList, ih, List.append_assoc]

theorem escList_length_le (l : List Std.U8) : (escList l).length ≤ 2 * l.length := by
  induction l with
  | nil => simp [escList]
  | cons c t ih =>
    simp only [escList, List.length_append, List.length_cons]
    split <;> simp <;> omega

/-- Read an element of `l` through a known `drop`. -/
theorem drop_getElem {α : Type} {l r : List α} {n : Nat} (h : l.drop n = r)
    (k : Nat) (hk : k < r.length) (hb : n + k < l.length) :
    l[n + k]'hb = r[k]'hk := by
  subst h
  simp [List.getElem_drop]

theorem drop_succ_of_drop_eq {α : Type} {l : List α} {n : Nat} {x : α} {r : List α}
    (h : l.drop n = x :: r) : l.drop (n + 1) = r := by
  rw [← List.tail_drop, h]
  rfl

/-- The escaping loop fills `out` with `escList (bytes[0..i])`. -/
theorem escape_and_terminate_loop_spec (bytes : Slice Std.U8)
    (hcap : 2 * bytes.val.length + 2 ≤ Std.Usize.max) :
    escape_and_terminate_loop bytes (alloc.vec.Vec.new Std.U8) 0#usize
      ⦃ out => out.val = escList bytes.val ⦄ := by
  simp only [escape_and_terminate_loop]
  have h := loop.spec_decr_nat
      (fun (x : (alloc.vec.Vec Std.U8) × Std.Usize) => bytes.val.length - x.2.val)
      (fun x => x.1.val = escList (bytes.val.take x.2.val) ∧ x.2.val ≤ bytes.val.length)
      (fun out => out.val = escList bytes.val)
      (fun (out1, i1) => escape_and_terminate_loop.body bytes out1 i1)
      (alloc.vec.Vec.new Std.U8, 0#usize)
      ?hbody ?hinv
  case hinv => simp [escList]
  case hbody =>
    rintro ⟨out, i⟩ hinv0
    have hout : out.val = escList (bytes.val.take i.val) := hinv0.1
    have hile : i.val ≤ bytes.val.length := hinv0.2
    simp only [escape_and_terminate_loop.body, ESCAPE_eq]
    split
    · rename_i hlt
      have hilt : i.val < bytes.val.length := by scalar_tac
      have hlenout : out.val.length ≤ 2 * i.val := by
        rw [hout]
        have := escList_length_le (bytes.val.take i.val)
        simp only [List.length_take] at this
        omega
      apply WP.spec_bind (Slice.index_usize_spec bytes i (by scalar_tac))
      intro bb hbb
      have htake : bytes.val.take (i.val + 1) = bytes.val.take i.val ++ [bytes.val[i.val]'hilt] := by
        rw [List.take_add_one, List.getElem?_eq_getElem hilt]
        simp
      split
      · rename_i hb0
        have hb0' : bytes.val[i.val]'hilt = 0#u8 := by rw [← hbb]; exact hb0
        have hpush2 : (do
              let out2 ← alloc.vec.Vec.push out 0#u8
              alloc.vec.Vec.push out2 255#u8)
            ⦃ o => o.val = out.val ++ [0#u8, 255#u8] ⦄ := by
          apply WP.spec_bind (alloc.vec.Vec.push_spec out 0#u8 (by omega))
          intro out2 hout2
          apply WP.spec_mono (alloc.vec.Vec.push_spec out2 255#u8
            (by rw [hout2]; simp; omega))
          intro o ho
          rw [ho, hout2]; simp
        apply WP.spec_bind hpush2
        intro out1 hout1
        apply WP.spec_bind (Usize.add_spec (x := i) (y := 1#usize) (by scalar_tac))
        intro i2 hi2
        have hi2' : i2.val = i.val + 1 := by scalar_tac
        simp only [WP.spec_ok]
        refine ⟨⟨?_, by omega⟩, by omega⟩
        show out1.val = escList (bytes.val.take i2.val)
        rw [hi2', htake, escList_append, escOne, hb0', if_pos rfl, hout1, hout]
      · rename_i hb0
        have hb0' : ¬ (bytes.val[i.val]'hilt = 0#u8) := by rw [← hbb]; exact hb0
        apply WP.spec_bind (alloc.vec.Vec.push_spec out bb (by omega))
        intro out1 hout1
        apply WP.spec_bind (Usize.add_spec (x := i) (y := 1#usize) (by scalar_tac))
        intro i2 hi2
        have hi2' : i2.val = i.val + 1 := by scalar_tac
        simp only [WP.spec_ok]
        refine ⟨⟨?_, by omega⟩, by omega⟩
        show out1.val = escList (bytes.val.take i2.val)
        rw [hi2', htake, escList_append, escOne, if_neg hb0', hout1, hout, hbb]
    · rename_i hge
      have hi : i.val = bytes.val.length := by scalar_tac
      simp only [WP.spec_ok]
      show out.val = escList bytes.val
      rw [hout, hi]
      simp
  exact h

theorem escape_and_terminate_spec (bytes : Slice Std.U8)
    (hcap : 2 * bytes.val.length + 2 ≤ Std.Usize.max) :
    escape_and_terminate bytes ⦃ out => out.val = escList bytes.val ++ [0#u8, 1#u8] ⦄ := by
  simp only [escape_and_terminate, TERMINATOR_eq]
  apply WP.spec_bind (escape_and_terminate_loop_spec bytes hcap)
  intro out hout
  have hlen : out.val.length ≤ 2 * bytes.val.length := by
    rw [hout]; exact escList_length_le _
  apply WP.spec_bind (alloc.vec.Vec.push_spec out 0#u8 (by omega))
  intro out1 hout1
  apply WP.spec_mono (alloc.vec.Vec.push_spec out1 1#u8 (by rw [hout1]; simp; omega))
  intro out2 hout2
  rw [hout2, hout1, hout]
  simp

/-- The generalized scan lemma: from position `at1`, where the remaining bytes are
    `escList b` followed by the terminator and an arbitrary `suffix`, the loop
    reproduces `b` and stops just past the terminator. -/
theorem unescape_until_terminator_loop_spec (bytes : Slice Std.U8)
    (out0 : alloc.vec.Vec Std.U8) (at1 : Std.Usize) (b suffix : List Std.U8)
    (hdrop : bytes.val.drop at1.val = escList b ++ [0#u8, 1#u8] ++ suffix)
    (hcap : out0.val.length + b.length ≤ Std.Usize.max) :
    unescape_until_terminator_loop bytes out0 at1 false false ⦃ r =>
      r.1.val = out0.val ++ b ∧
      r.2.1.val = at1.val + (escList b).length + 2 ∧
      r.2.2 = true ⦄ := by
  simp only [unescape_until_terminator_loop]
  have h := loop.spec_decr_nat
      (fun (x : (alloc.vec.Vec Std.U8) × Std.Usize × Bool × Bool) =>
        bytes.val.length - x.2.1.val)
      (fun x => x.2.2.2 = false ∧
        ((x.2.2.1 = true ∧ x.1.val = out0.val ++ b ∧
            x.2.1.val = at1.val + (escList b).length + 2) ∨
         (x.2.2.1 = false ∧ ∃ b' : List Std.U8,
            x.1.val ++ b' = out0.val ++ b ∧
            bytes.val.drop x.2.1.val = escList b' ++ [0#u8, 1#u8] ++ suffix ∧
            x.2.1.val + (escList b').length = at1.val + (escList b).length ∧
            x.1.val.length + b'.length ≤ Std.Usize.max)))
      (fun r => r.1.val = out0.val ++ b ∧
        r.2.1.val = at1.val + (escList b).length + 2 ∧ r.2.2 = true)
      (fun (out1, i1, done2, bad1) =>
        unescape_until_terminator_loop.body bytes out1 i1 done2 bad1)
      (out0, at1, false, false)
      ?hbody ?hinv
  case hinv =>
    exact ⟨rfl, Or.inr ⟨rfl, b, by simp, hdrop, rfl, by simpa using hcap⟩⟩
  case hbody =>
    rintro ⟨out, i, dn, bd⟩ hinv0
    have hbd : bd = false := hinv0.1
    have hcase :
        (dn = true ∧ out.val = out0.val ++ b ∧
            i.val = at1.val + (escList b).length + 2) ∨
        (dn = false ∧ ∃ b' : List Std.U8,
            out.val ++ b' = out0.val ++ b ∧
            bytes.val.drop i.val = escList b' ++ [0#u8, 1#u8] ++ suffix ∧
            i.val + (escList b').length = at1.val + (escList b).length ∧
            out.val.length + b'.length ≤ Std.Usize.max) := hinv0.2
    subst hbd
    simp only [unescape_until_terminator_loop.body, ESCAPE_eq, TERMINATOR_eq]
    split
    · -- i < len
      rename_i hlt
      have hilt : i.val < bytes.val.length := by scalar_tac
      split
      · -- done1 = true: exit with the recorded answer
        rename_i hdn
        refine (WP.spec_ok _).mpr ?_
        dsimp only
        rcases hcase with ⟨_, ho, hi⟩ | ⟨hd, _⟩
        · exact ⟨ho, hi, rfl⟩
        · exact absurd hdn (by simp [hd])
      · rename_i hdn
        have hdnf : dn = false := by simpa using hdn
        split
        · -- bad = true: impossible, the invariant pins bad = false
          rename_i hbd2
          exact absurd hbd2 (by simp)
        · -- the scanning step
          rename_i hbd2
          obtain ⟨hd, b', hb', hdr', hpos', hcp⟩ := hcase.resolve_left (by simp [hdnf])
          have hlenL : bytes.val.length - i.val
              = (escList b').length + 2 + suffix.length := by
            have hh := congrArg List.length hdr'
            simp only [List.length_drop, List.length_append, List.length_cons,
              List.length_nil] at hh
            omega
          apply WP.spec_bind (Slice.index_usize_spec bytes i (by scalar_tac))
          intro bb hbb
          rcases b' with _ | ⟨c, t⟩
          · -- the terminator is right here
            simp only [escList, List.nil_append, List.cons_append, List.length_nil,
              Nat.add_zero] at hdr' hpos' hlenL
            have h0 : bytes.val[i.val]'hilt = 0#u8 := by
              have := drop_getElem hdr' 0 (by simp) (by omega)
              simpa using this
            have h1 : bytes.val[i.val + 1]'(by omega) = 1#u8 := by
              have := drop_getElem hdr' 1 (by simp) (by omega)
              simpa using this
            rw [hbb, h0]
            simp only [reduceIte]
            apply WP.spec_bind (Usize.add_spec (x := i) (y := 1#usize) (by scalar_tac))
            intro i2 hi2
            have hi2' : i2.val = i.val + 1 := by scalar_tac
            have hi2lt : i2.val < bytes.val.length := by omega
            rw [if_neg (by scalar_tac)]
            apply WP.spec_bind (Slice.index_usize_spec bytes i2 (by scalar_tac))
            intro next hnext
            have hnext1 : next = 1#u8 := by
              rw [hnext, ← h1]
              congr 1
            rw [hnext1]
            simp only [reduceIte]
            apply WP.spec_bind (Usize.add_spec (x := i) (y := 2#usize) (by scalar_tac))
            intro i4 hi4
            have hi4' : i4.val = i.val + 2 := by scalar_tac
            refine (WP.spec_ok _).mpr ?_
            dsimp only
            refine ⟨⟨rfl, Or.inl ⟨rfl, ?_, by simp only [hi4']; omega⟩⟩, by omega⟩
            simpa using hb'
          · -- one more source byte to unescape
            by_cases hc : c = 0#u8
            · subst hc
              simp only [escList, reduceIte, List.cons_append, List.nil_append,
                List.append_assoc, List.length_cons] at hdr' hpos' hlenL
              have h0 : bytes.val[i.val]'hilt = 0#u8 := by
                have := drop_getElem hdr' 0 (by simp) (by omega)
                simpa using this
              have h1 : bytes.val[i.val + 1]'(by omega) = 255#u8 := by
                have := drop_getElem hdr' 1 (by simp) (by omega)
                simpa using this
              rw [hbb, h0]
              simp only [reduceIte]
              apply WP.spec_bind (Usize.add_spec (x := i) (y := 1#usize) (by scalar_tac))
              intro i2 hi2
              have hi2' : i2.val = i.val + 1 := by scalar_tac
              have hi2lt : i2.val < bytes.val.length := by omega
              rw [if_neg (by scalar_tac)]
              apply WP.spec_bind (Slice.index_usize_spec bytes i2 (by scalar_tac))
              intro next hnext
              have hnext1 : next = 255#u8 := by
                rw [hnext, ← h1]
                congr 1
              rw [hnext1]
              simp only [reduceIte]
              apply WP.spec_bind (alloc.vec.Vec.push_spec out 0#u8 (by simp at hcp; omega))
              intro out1 hout1
              apply WP.spec_bind (Usize.add_spec (x := i) (y := 2#usize) (by scalar_tac))
              intro i4 hi4
              have hi4' : i4.val = i.val + 2 := by scalar_tac
              refine (WP.spec_ok _).mpr ?_
              dsimp only
              refine ⟨⟨rfl, Or.inr ⟨rfl, t, ?_, ?_, ?_, ?_⟩⟩, by omega⟩
              · rw [hout1]; simpa using hb'
              · rw [hi4', show i.val + 2 = i.val + 1 + 1 by omega]
                have h2 := drop_succ_of_drop_eq (drop_succ_of_drop_eq hdr')
                simpa using h2
              · simp only [hi4']; omega
              · rw [hout1]; simp at hcp ⊢; omega
            · simp only [escList, if_neg hc, List.cons_append, List.nil_append,
                List.append_assoc, List.length_cons] at hdr' hpos' hlenL
              have h0 : bytes.val[i.val]'hilt = c := by
                have := drop_getElem hdr' 0 (by simp) (by omega)
                simpa using this
              rw [hbb, h0]
              rw [if_neg hc]
              apply WP.spec_bind (alloc.vec.Vec.push_spec out c (by simp at hcp; omega))
              intro out1 hout1
              apply WP.spec_bind (Usize.add_spec (x := i) (y := 1#usize) (by scalar_tac))
              intro i2 hi2
              have hi2' : i2.val = i.val + 1 := by scalar_tac
              refine (WP.spec_ok _).mpr ?_
              dsimp only
              refine ⟨⟨rfl, Or.inr ⟨rfl, t, ?_, ?_, ?_, ?_⟩⟩, by omega⟩
              · rw [hout1]; simpa using hb'
              · rw [hi2']
                have h2 := drop_succ_of_drop_eq hdr'
                simpa using h2
              · simp only [hi2']; omega
              · rw [hout1]; simp at hcp ⊢; omega
    · -- i ≥ len: only reachable once the terminator has been consumed
      rename_i hge
      refine (WP.spec_ok _).mpr ?_
      dsimp only
      rcases hcase with ⟨hd, ho, hi⟩ | ⟨hd, b', hb', hdr', hpos', hcp⟩
      · exact ⟨ho, hi, hd⟩
      · exfalso
        have hh := congrArg List.length hdr'
        simp only [List.length_drop, List.length_append, List.length_cons,
          List.length_nil] at hh
        have : bytes.val.length ≤ i.val := by scalar_tac
        omega
  exact h

/-! ## The pair encoding -/

/-- The tail-append loop of `encode_pair_bytes_u64` (note: the loop takes
    `(out, tail, i)` but its `.body` takes `(tail, out, i)`). -/
theorem encode_pair_bytes_u64_loop_spec (out0 tail : alloc.vec.Vec Std.U8) (i0 : Std.Usize)
    (hcap : out0.val.length + (tail.val.length - i0.val) ≤ Std.Usize.max) :
    encode_pair_bytes_u64_loop out0 tail i0
      ⦃ r => r.val = out0.val ++ tail.val.drop i0.val ⦄ := by
  simp only [encode_pair_bytes_u64_loop]
  have h := loop.spec_decr_nat
      (fun (x : (alloc.vec.Vec Std.U8) × Std.Usize) => tail.val.length - x.2.val)
      (fun x => x.1.val ++ tail.val.drop x.2.val = out0.val ++ tail.val.drop i0.val)
      (fun r => r.val = out0.val ++ tail.val.drop i0.val)
      (fun (out1, i1) => encode_pair_bytes_u64_loop.body tail out1 i1)
      (out0, i0)
      ?hbody ?hinv
  case hinv => simp
  case hbody =>
    rintro ⟨out, i⟩ hinv0
    have hinv : out.val ++ tail.val.drop i.val = out0.val ++ tail.val.drop i0.val := hinv0
    have hlen := congrArg List.length hinv
    simp at hlen
    simp only [encode_pair_bytes_u64_loop.body, alloc.vec.Vec.index_slice_index]
    split
    · apply WP.spec_bind (alloc.vec.Vec.index_usize_spec tail i (by scalar_tac))
      intro x hx
      apply WP.spec_bind (alloc.vec.Vec.push_spec out x (by scalar_tac))
      intro out1 hout1
      apply WP.spec_bind (Usize.add_spec (x := i) (y := 1#usize) (by scalar_tac))
      intro i3 hi3
      have hi3' : i3.val = i.val + 1 := by scalar_tac
      simp only [WP.spec_ok]
      refine ⟨?_, by scalar_tac⟩
      show out1.val ++ tail.val.drop i3.val = out0.val ++ tail.val.drop i0.val
      rw [hout1, hx, hi3', List.append_assoc, List.singleton_append,
        ← List.drop_eq_getElem_cons (by scalar_tac)]
      exact hinv
    · simp only [WP.spec_ok]
      show out.val = out0.val ++ tail.val.drop i0.val
      have hd : tail.val.drop i.val = [] := by
        apply List.drop_eq_nil_of_le; scalar_tac
      rw [← hinv, hd, List.append_nil]
  exact h

/-- The tail-copying loop of `decode_pair_bytes_u64`. -/
theorem decode_pair_bytes_u64_loop_spec (bytes : Slice Std.U8)
    (tail0 : alloc.vec.Vec Std.U8) (i0 : Std.Usize)
    (hcap : tail0.val.length + (bytes.val.length - i0.val) ≤ Std.Usize.max) :
    decode_pair_bytes_u64_loop bytes tail0 i0
      ⦃ r => r.val = tail0.val ++ bytes.val.drop i0.val ⦄ := by
  simp only [decode_pair_bytes_u64_loop]
  have h := loop.spec_decr_nat
      (fun (x : (alloc.vec.Vec Std.U8) × Std.Usize) => bytes.val.length - x.2.val)
      (fun x => x.1.val ++ bytes.val.drop x.2.val = tail0.val ++ bytes.val.drop i0.val)
      (fun r => r.val = tail0.val ++ bytes.val.drop i0.val)
      (fun (tail1, i1) => decode_pair_bytes_u64_loop.body bytes tail1 i1)
      (tail0, i0)
      ?hbody ?hinv
  case hinv => simp
  case hbody =>
    rintro ⟨tl, i⟩ hinv0
    have hinv : tl.val ++ bytes.val.drop i.val = tail0.val ++ bytes.val.drop i0.val := hinv0
    have hlen := congrArg List.length hinv
    simp at hlen
    simp only [decode_pair_bytes_u64_loop.body]
    split
    · apply WP.spec_bind (Slice.index_usize_spec bytes i (by scalar_tac))
      intro x hx
      apply WP.spec_bind (alloc.vec.Vec.push_spec tl x (by scalar_tac))
      intro tl1 htl1
      apply WP.spec_bind (Usize.add_spec (x := i) (y := 1#usize) (by scalar_tac))
      intro i3 hi3
      have hi3' : i3.val = i.val + 1 := by scalar_tac
      simp only [WP.spec_ok]
      refine ⟨?_, by scalar_tac⟩
      show tl1.val ++ bytes.val.drop i3.val = tail0.val ++ bytes.val.drop i0.val
      rw [htl1, hx, hi3', List.append_assoc, List.singleton_append,
        ← List.drop_eq_getElem_cons (by scalar_tac)]
      exact hinv
    · simp only [WP.spec_ok]
      show tl.val = tail0.val ++ bytes.val.drop i0.val
      have hd : bytes.val.drop i.val = [] := by
        apply List.drop_eq_nil_of_le; scalar_tac
      rw [← hinv, hd, List.append_nil]
  exact h

/-- The suffix-generalized scan lemma the pair case reuses: the scan stops at the
    first terminator and ignores everything after it. -/
theorem escape_prefix_scan (b : Slice Std.U8) (suffix : List Std.U8)
    (bytes : Slice Std.U8) (framed : alloc.vec.Vec Std.U8)
    (hframed : framed.val = escList b.val ++ [0#u8, 1#u8])
    (hbytes : bytes.val = framed.val ++ suffix) :
    unescape_until_terminator bytes 0#usize
      = ok (some (⟨b.val, b.property⟩, alloc.vec.Vec.len framed)) := by
  have hdrop : bytes.val.drop (0#usize).val
      = escList b.val ++ [0#u8, 1#u8] ++ suffix := by
    simp [hbytes, hframed]
  obtain ⟨r, hr, hpost⟩ := WP.spec_imp_exists
    (unescape_until_terminator_loop_spec bytes (alloc.vec.Vec.new Std.U8) 0#usize
      b.val suffix hdrop (by simpa using b.property))
  obtain ⟨o, i, dn⟩ := r
  obtain ⟨h1, h2, h3⟩ := hpost
  have h1' : o.val = [] ++ b.val := h1
  have h2' : i.val = (0#usize).val + (escList b.val).length + 2 := h2
  have h3' : dn = true := h3
  have ho : o = (⟨b.val, b.property⟩ : alloc.vec.Vec Std.U8) := by
    simp only [alloc.vec.Vec, Subtype.ext_iff]
    simpa using h1'
  have hi : i = alloc.vec.Vec.len framed := by
    apply UScalar.eq_of_val_eq
    rw [h2']
    simp [hframed]
  simp only [unescape_until_terminator, hr, bind_tc_ok]
  simp only [h3', ho, hi]
  rfl

/-- `unescape_until_terminator` inverts `escape_and_terminate`, and reports the
    position just past the terminator (i.e. the whole framed length). -/
theorem unescape_escape (b : Slice Std.U8)
    (hcap : 2 * b.val.length + 2 ≤ Std.Usize.max) :
    escape_and_terminate b ⦃ framed =>
      unescape_until_terminator (alloc.vec.Vec.deref framed) 0#usize
        = ok (some (⟨b.val, b.property⟩, alloc.vec.Vec.len framed)) ⦄ := by
  apply WP.spec_mono (escape_and_terminate_spec b hcap)
  intro framed hframed
  exact escape_prefix_scan b [] (alloc.vec.Vec.deref framed) framed hframed
    (by simp [alloc.vec.Vec.deref])

/-- `decode_pair_bytes_u64 ∘ encode_pair_bytes_u64 = some`. -/
theorem decode_encode_pair (a : Slice Std.U8) (v : Std.U64)
    (hcap : 2 * a.val.length + 10 ≤ Std.Usize.max) :
    encode_pair_bytes_u64 a v ⦃ out =>
      decode_pair_bytes_u64 (alloc.vec.Vec.deref out)
        = ok (some (⟨a.val, a.property⟩, v)) ⦄ := by
  simp only [encode_pair_bytes_u64]
  apply WP.spec_bind (escape_and_terminate_spec a (by omega))
  intro framed hframed
  have hflen : framed.val.length ≤ 2 * a.val.length + 2 := by
    rw [hframed]
    have := escList_length_le a.val
    simp only [List.length_append, List.length_cons, List.length_nil]
    omega
  apply WP.spec_bind (encode_u64_spec v)
  intro tail htail
  have htlen : tail.val.length = 8 := by rw [htail]; simp
  apply WP.spec_mono (encode_pair_bytes_u64_loop_spec framed tail 0#usize (by simp; omega))
  intro enc henc
  have henc' : enc.val = framed.val ++ tail.val := by simpa using henc
  have hderef : (alloc.vec.Vec.deref enc).val = framed.val ++ tail.val := henc'
  have henclen : (alloc.vec.Vec.deref enc).val.length = framed.val.length + 8 := by
    rw [hderef]; simp [htlen]
  have hlenframed : (alloc.vec.Vec.len framed).val = framed.val.length := by simp
  -- the head scans back out, and the scan stops exactly at the fixed-width tail
  have hscan := escape_prefix_scan a tail.val (alloc.vec.Vec.deref enc) framed hframed hderef
  have hdropeq : (alloc.vec.Vec.deref enc).val.drop (alloc.vec.Vec.len framed).val
      = u64bytes v := by
    rw [hderef, hlenframed, List.drop_left, htail]
  -- the remaining bytes are exactly `encode_u64 v`
  obtain ⟨tl, htl, htlv⟩ := WP.spec_imp_exists
    (decode_pair_bytes_u64_loop_spec (alloc.vec.Vec.deref enc) (alloc.vec.Vec.new Std.U8)
      (alloc.vec.Vec.len framed) (by rw [hlenframed, henclen]; scalar_tac))
  have htlv' : tl.val = u64bytes v := by
    rw [htlv, hdropeq]; simp
  have htlderef : (alloc.vec.Vec.deref tl).val = u64bytes v := htlv'
  simp only [decode_pair_bytes_u64, hscan, bind_tc_ok]
  show (do
      let tl ← decode_pair_bytes_u64_loop (alloc.vec.Vec.deref enc)
        (alloc.vec.Vec.new Std.U8) (alloc.vec.Vec.len framed)
      let o1 ← decode_u64 (alloc.vec.Vec.deref tl)
      match o1 with
      | none => ok none
      | some w => ok (some ((⟨a.val, a.property⟩ : alloc.vec.Vec Std.U8), w)))
      = ok (some ((⟨a.val, a.property⟩ : alloc.vec.Vec Std.U8), v))
  rw [htl]
  simp only [bind_tc_ok]
  rw [decode_u64_eq _ (by rw [htlderef]; simp)]
  simp only [htlderef, beFold_u64bytes, UScalar_eta]
  rfl

/-! ## Axiom audit -/

#print axioms decode_encode_u64
#print axioms decode_encode_i64
#print axioms escape_prefix_scan
#print axioms unescape_escape
#print axioms decode_encode_pair

end key_kernel
