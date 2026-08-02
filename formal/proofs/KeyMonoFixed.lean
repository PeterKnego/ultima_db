/- Order preservation for the fixed-width key encodings
   (`formal/key_kernel/src/lib.rs`, a port of `src/primary_key.rs`), stated over
   the Aeneas translation in `KeyKernel.lean` (generated -- never edit).

   Task 4 (`KeyRoundTrip.lean`) proved `decode (encode x) = x`, which says the
   encoding loses no information.  It says *nothing* about order: the round trip
   would hold just as well for a byte-reversed or sign-unflipped encoding.  This
   file proves the property the B-tree actually depends on --

     `a < b  ↔  encode a  <lex  encode b`

   -- for `u64` and `i64`.

   | theorem | content |
   |---|---|
   | `encode_mono_u64`    | `a.val < b.val ↔ lex_lt (encode_u64 a) (encode_u64 b)` |
   | `sign_flip_order_iso`| `x ↦ (x as u64) ^ 2^63` maps the signed order onto the unsigned one |
   | `encode_mono_i64`    | `a.val < b.val ↔ lex_lt (encode_i64 a) (encode_i64 b)` |

   The statements are `↔`, not `→`, on purpose.  The two directions together give
   antisymmetry of the encoded order, hence injectivity of `encode` on the
   ordering -- which is exactly what `BTree::from_sorted` assumes when it takes
   a sorted, deduplicated key stream and never re-checks for collisions.

   Both theorems are in the Aeneas `⦃ ⦄` form, so they also assert that both
   `encode` calls return `ok`: no panic, no overflow.  The fixed-width encoders
   always push exactly eight bytes, so unlike the variable-length encoders of
   Task 4 they need no `Vec` capacity hypothesis.

   `lex_lt` is defined here rather than imported from Mathlib: the direct
   recursive definition is literally what a bytewise memcmp-style comparison
   does, and it avoids fighting Mathlib's `Decidable`/`WellFounded` instances
   on `List.Lex`.

   As in `KeyRoundTrip.lean`, `bv_decide` / `native_decide` / `decide` over
   bitvectors are deliberately avoided: in this toolchain `bv_decide` discharges
   its SAT certificate by native evaluation and mints a
   `…_native.bv_decide.ax…` axiom, which the project's axiom budget
   (`propext`, `Classical.choice`, `Quot.sound`) does not allow.  Every bit-level
   step below is a hand-written `getLsbD` / `toNat` argument. -/
import KeyRoundTrip

open Aeneas Aeneas.Std Result

namespace key_kernel

/-! ## The lexicographic byte order

The relation a bytewise comparator computes: the shorter list wins if it is a
prefix, otherwise the first differing byte decides.  Both encodings here are
exactly eight bytes long, so only the "first differing byte" clause is ever
exercised, but the definition is the general one. -/

def lex_lt : List Std.U8 → List Std.U8 → Prop
  | _, [] => False
  | [], _ :: _ => True
  | x :: xs, y :: ys => x.val < y.val ∨ (x = y ∧ lex_lt xs ys)

@[simp] theorem lex_lt_nil_right (l : List Std.U8) : lex_lt l [] = False := by
  cases l <;> rfl

@[simp] theorem lex_lt_nil_cons (y : Std.U8) (ys : List Std.U8) :
    lex_lt [] (y :: ys) = True := rfl

@[simp] theorem lex_lt_cons_cons (x y : Std.U8) (xs ys : List Std.U8) :
    lex_lt (x :: xs) (y :: ys) = (x.val < y.val ∨ (x = y ∧ lex_lt xs ys)) := rfl

/-- The same relation on the underlying `Nat` values.  All the arithmetic is done
    here; `lex_lt_iff_lexNat` transports it back to bytes. -/
def lexNat : List Nat → List Nat → Prop
  | _, [] => False
  | [], _ :: _ => True
  | x :: xs, y :: ys => x < y ∨ (x = y ∧ lexNat xs ys)

@[simp] theorem lexNat_nil_right (l : List Nat) : lexNat l [] = False := by
  cases l <;> rfl

@[simp] theorem lexNat_nil_cons (y : Nat) (ys : List Nat) :
    lexNat [] (y :: ys) = True := rfl

@[simp] theorem lexNat_cons_cons (x y : Nat) (xs ys : List Nat) :
    lexNat (x :: xs) (y :: ys) = (x < y ∨ (x = y ∧ lexNat xs ys)) := rfl

theorem u8_eq_iff_val (x y : Std.U8) : x = y ↔ x.val = y.val :=
  ⟨fun h => by rw [h], Std.UScalar.eq_of_val_eq⟩

theorem lex_lt_iff_lexNat (xs ys : List Std.U8) :
    lex_lt xs ys ↔ lexNat (xs.map (·.val)) (ys.map (·.val)) := by
  induction xs generalizing ys with
  | nil => cases ys <;> simp
  | cons x t ih =>
    cases ys with
    | nil => simp
    | cons y u => simp [ih, u8_eq_iff_val]

/-! ## The big-endian digit model

`topDigits v k` is the list of the top-`k` base-256 digits of `v`, most
significant first -- exactly what `encode_u64` pushes when `k = 8`. -/

def topDigits (v : Nat) : Nat → List Nat
  | 0 => []
  | k + 1 => v / 2 ^ (8 * k) % 256 :: topDigits v k

/-- The one arithmetic fact the induction needs: once two numbers agree above
    the low byte, both `<` and `=` are decided by the low byte alone. -/
theorem cmp_low_byte (A B : Nat) (h : A / 256 = B / 256) :
    (A < B ↔ A % 256 < B % 256) ∧ (A = B ↔ A % 256 = B % 256) := by
  omega

theorem div_lt_div_imp_lt {a b P : Nat} (h : a / P < b / P) : a < b := by
  by_contra hc
  exact Nat.not_lt.mpr (Nat.div_le_div_right (Nat.le_of_not_lt hc)) h

/-- The heart of the `u64` case: if `a` and `b` agree strictly above the top `k`
    bytes, then comparing them is the same as comparing their top-`k` digit
    lists lexicographically. -/
theorem topDigits_lex (k : Nat) :
    ∀ a b : Nat, a / 2 ^ (8 * k) = b / 2 ^ (8 * k) →
      (a < b ↔ lexNat (topDigits a k) (topDigits b k)) := by
  induction k with
  | zero =>
    intro a b h
    simp only [Nat.mul_zero, pow_zero, Nat.div_one] at h
    subst h
    simp [topDigits]
  | succ k ih =>
    intro a b h
    have hsplit : (2 : Nat) ^ (8 * (k + 1)) = 2 ^ (8 * k) * 256 := by
      rw [Nat.mul_succ, pow_add]
      norm_num
    have hdiv : ∀ x : Nat, x / 2 ^ (8 * (k + 1)) = x / 2 ^ (8 * k) / 256 := by
      intro x
      rw [Nat.div_div_eq_div_mul, ← hsplit]
    have hAB : a / 2 ^ (8 * k) / 256 = b / 2 ^ (8 * k) / 256 := by
      rw [← hdiv a, ← hdiv b]; exact h
    obtain ⟨hlt256, heq256⟩ := cmp_low_byte _ _ hAB
    obtain ⟨hgt256, -⟩ := cmp_low_byte _ _ hAB.symm
    simp only [topDigits, lexNat_cons_cons]
    rcases Nat.lt_trichotomy (a / 2 ^ (8 * k)) (b / 2 ^ (8 * k)) with hlt | heq | hgt
    · have hab : a < b := div_lt_div_imp_lt hlt
      exact ⟨fun _ => Or.inl (hlt256.mp hlt), fun _ => hab⟩
    · have hm : a / 2 ^ (8 * k) % 256 = b / 2 ^ (8 * k) % 256 := heq256.mp heq
      rw [ih a b heq]
      constructor
      · intro h1; exact Or.inr ⟨hm, h1⟩
      · rintro (h1 | ⟨-, h1⟩)
        · omega
        · exact h1
    · have hba : b < a := div_lt_div_imp_lt hgt
      have hm : b / 2 ^ (8 * k) % 256 < a / 2 ^ (8 * k) % 256 := hgt256.mp hgt
      constructor
      · intro h1; omega
      · rintro (h1 | ⟨h1, -⟩) <;> omega

/-! ## The encoded bytes are the big-endian digits -/

/-- Truncating to eight bits already drops everything the `& 0xFF` would. -/
theorem setWidth8_and_255 (x : BitVec 64) :
    BitVec.setWidth 8 (x &&& 255#64) = BitVec.setWidth 8 x := by
  apply BitVec.eq_of_getLsbD_eq
  intro i hi
  simp only [BitVec.getLsbD_setWidth, BitVec.getLsbD_and, getLsbD_255]
  by_cases h : (i : Nat) < 8 <;> simp [h]

theorem u64byte_val (v : Std.U64) (j : Nat) :
    (u64byte v j).val = v.val / 2 ^ (8 * (7 - j)) % 256 := by
  show (u64byteBv v j).toNat = v.bv.toNat / 2 ^ (8 * (7 - j)) % 256
  rw [u64byteBv, setWidth8_and_255, BitVec.toNat_setWidth, BitVec.toNat_ushiftRight,
    Nat.shiftRight_eq_div_pow]

theorem u64bytes_map_val (v : Std.U64) :
    (u64bytes v).map (·.val) = topDigits v.val 8 := by
  rw [u64bytes_eq]
  simp only [List.map_cons, List.map_nil, u64byte_val, topDigits]

/-- `encode_u64` is strictly monotone, as a bytewise comparison. -/
theorem u64bytes_lex (a b : Std.U64) :
    a.val < b.val ↔ lex_lt (u64bytes a) (u64bytes b) := by
  rw [lex_lt_iff_lexNat, u64bytes_map_val, u64bytes_map_val]
  have ha : a.val < 2 ^ 64 := a.bv.isLt
  have hb : b.val < 2 ^ 64 := b.bv.isLt
  refine topDigits_lex 8 _ _ ?_
  have h64 : (2 : Nat) ^ (8 * 8) = 2 ^ 64 := by norm_num
  rw [h64, Nat.div_eq_of_lt ha, Nat.div_eq_of_lt hb]

/-! ## The sign-bit flip -/

/-- The constant `1u64 << 63`, as a bit vector. -/
def signBit : BitVec 64 := BitVec.twoPow 64 63

theorem getLsbD_signBit_self : signBit.getLsbD 63 = true := by
  rw [signBit, BitVec.getLsbD_twoPow]; simp

theorem getLsbD_signBit_ne {i : Nat} (h : i ≠ 63) : signBit.getLsbD i = false := by
  rw [signBit, BitVec.getLsbD_twoPow]
  simp only [Bool.and_eq_false_imp, decide_eq_true_eq]
  intro _
  simp [Ne.symm h]

theorem toNat_signBit : signBit.toNat = 2 ^ 63 :=
  BitVec.toNat_twoPow_of_lt (by omega)

theorem msb_eq_getLsbD_63 (x : BitVec 64) : x.msb = x.getLsbD 63 :=
  BitVec.msb_eq_getLsbD_last x

/-- On a value whose top bit is clear, xoring in the sign bit is plain addition. -/
theorem xor_signBit_toNat_of_msb_false {x : BitVec 64} (h : x.msb = false) :
    (x ^^^ signBit).toNat = x.toNat + 2 ^ 63 := by
  have h63 : x.getLsbD 63 = false := by rw [← msb_eq_getLsbD_63]; exact h
  have hbits : ∀ i : Nat, (x.getLsbD i && signBit.getLsbD i) = false := by
    intro i
    by_cases hi : i = 63
    · subst hi; rw [h63]; simp
    · rw [getLsbD_signBit_ne hi]; simp
  have hand : x &&& signBit = 0#64 := by
    apply BitVec.eq_of_getLsbD_eq
    intro i hi
    rw [BitVec.getLsbD_and, BitVec.getLsbD_zero]
    exact hbits i
  have hxor : x ^^^ signBit = x + signBit := by
    rw [BitVec.add_eq_or_of_and_eq_zero _ _ hand]
    apply BitVec.eq_of_getLsbD_eq
    intro i hi
    have := hbits i
    simp only [BitVec.getLsbD_xor, BitVec.getLsbD_or]
    revert this
    cases x.getLsbD i <;> cases signBit.getLsbD i <;> simp
  rw [hxor, BitVec.toNat_add_of_and_eq_zero hand, toNat_signBit]

/-- Xoring in the sign bit shifts the *signed* value up by `2^63` into the
    unsigned range: `-2^63 ↦ 0`, `-1 ↦ 2^63 - 1`, `0 ↦ 2^63`, `2^63-1 ↦ 2^64-1`.
    This is the entire content of "negatives sort before positives". -/
theorem flip_toNat (x : BitVec 64) :
    ((x ^^^ signBit).toNat : Int) = x.toInt + 2 ^ 63 := by
  by_cases h : x.msb = true
  · have h63 : x.getLsbD 63 = true := by rw [← msb_eq_getLsbD_63]; exact h
    have hy : (x ^^^ signBit).msb = false := by
      rw [msb_eq_getLsbD_63, BitVec.getLsbD_xor, h63, getLsbD_signBit_self]
      simp
    have hyx : (x ^^^ signBit) ^^^ signBit = x := by
      rw [BitVec.xor_assoc, BitVec.xor_self, BitVec.xor_zero]
    have hstep := xor_signBit_toNat_of_msb_false hy
    rw [hyx] at hstep
    have hti : x.toInt = (x.toNat : Int) - 2 ^ 64 := by
      rw [BitVec.toInt_eq_msb_cond, if_pos h]
      norm_num
    omega
  · have hfalse : x.msb = false := by simpa using h
    have hstep := xor_signBit_toNat_of_msb_false hfalse
    have hti : x.toInt = (x.toNat : Int) := BitVec.toInt_eq_toNat_of_msb hfalse
    omega

/-- The `u64` the `i64` encoder actually feeds to `encode_u64`. -/
def flipI64 (v : Std.I64) : Std.U64 := ⟨v.bv ^^^ signBit⟩

/-- **The order isomorphism.**  The signed order on `i64` is carried exactly onto
    the unsigned order on the biased `u64`. -/
theorem sign_flip_order_iso (a b : Std.I64) :
    a.val < b.val ↔ (flipI64 a).val < (flipI64 b).val := by
  have ha : ((flipI64 a).val : Int) = a.val + 2 ^ 63 := flip_toNat a.bv
  have hb : ((flipI64 b).val : Int) = b.val + 2 ^ 63 := flip_toNat b.bv
  omega

/-- `encode_i64` is `encode_u64` of the biased value. -/
theorem encode_i64_eq (v : Std.I64) : encode_i64 v = encode_u64 (flipI64 v) := by
  obtain ⟨k, hk, hkpost⟩ := WP.spec_imp_exists
    (U64.ShiftLeft_IScalar_spec 1#u64 63#i32 (by scalar_tac) (by scalar_tac))
  have hkbv : k.bv = signBit := by
    rw [hkpost.2]
    rfl
  simp only [encode_i64, lift, bind_tc_ok, hk]
  congr 1
  apply U64.bv_eq_imp_eq
  show (Std.IScalar.hcast Std.UScalarTy.U64 v).bv ^^^ k.bv = v.bv ^^^ signBit
  have hc : (Std.IScalar.hcast Std.UScalarTy.U64 v).bv = v.bv := BitVec.signExtend_eq v.bv
  rw [hkbv, hc]

/-! ## The two monotonicity theorems -/

/-- `encode_u64` is an order embedding into the bytewise order. -/
theorem encode_mono_u64 (a b : Std.U64) :
    encode_u64 a ⦃ ea =>
      encode_u64 b ⦃ eb => (a.val < b.val ↔ lex_lt ea.val eb.val) ⦄ ⦄ := by
  apply WP.spec_mono (encode_u64_spec a)
  intro ea hea
  apply WP.spec_mono (encode_u64_spec b)
  intro eb heb
  rw [hea, heb]
  exact u64bytes_lex a b

/-- `encode_i64` is an order embedding into the bytewise order -- in particular
    every negative encodes below every non-negative. -/
theorem encode_mono_i64 (a b : Std.I64) :
    encode_i64 a ⦃ ea =>
      encode_i64 b ⦃ eb => (a.val < b.val ↔ lex_lt ea.val eb.val) ⦄ ⦄ := by
  simp only [encode_i64_eq]
  apply WP.spec_mono (encode_u64_spec (flipI64 a))
  intro ea hea
  apply WP.spec_mono (encode_u64_spec (flipI64 b))
  intro eb heb
  rw [hea, heb, ← u64bytes_lex]
  exact sign_flip_order_iso a b

/-! ## Injectivity, the corollary `from_sorted` cares about

This is why the two theorems above are stated as `↔` and not `→`: the two
directions give antisymmetry of the encoded order, and antisymmetry gives
injectivity of the encoding.  `BTree::from_sorted` consumes a sorted,
deduplicated key stream and never re-checks for collisions, so it needs
distinct keys to stay distinct after encoding. -/

theorem lex_lt_irrefl (l : List Std.U8) : ¬ lex_lt l l := by
  induction l with
  | nil => simp
  | cons x t ih => simp [ih]

theorem encode_inj_u64 (a b : Std.U64) :
    encode_u64 a ⦃ ea =>
      encode_u64 b ⦃ eb => (ea.val = eb.val → a = b) ⦄ ⦄ := by
  apply WP.spec_mono (encode_u64_spec a)
  intro ea hea
  apply WP.spec_mono (encode_u64_spec b)
  intro eb heb heq
  apply Std.UScalar.eq_of_val_eq
  by_contra hne
  rcases Nat.lt_trichotomy a.val b.val with h | h | h
  · have hl := (u64bytes_lex a b).mp h
    rw [← hea, ← heb, heq] at hl
    exact lex_lt_irrefl _ hl
  · exact hne h
  · have hl := (u64bytes_lex b a).mp h
    rw [← hea, ← heb, heq] at hl
    exact lex_lt_irrefl _ hl

theorem encode_inj_i64 (a b : Std.I64) :
    encode_i64 a ⦃ ea =>
      encode_i64 b ⦃ eb => (ea.val = eb.val → a = b) ⦄ ⦄ := by
  simp only [encode_i64_eq]
  apply WP.spec_mono (encode_u64_spec (flipI64 a))
  intro ea hea
  apply WP.spec_mono (encode_u64_spec (flipI64 b))
  intro eb heb heq
  apply Std.IScalar.eq_of_val_eq
  by_contra hne
  rcases lt_trichotomy a.val b.val with h | h | h
  · have hl := (u64bytes_lex _ _).mp ((sign_flip_order_iso a b).mp h)
    rw [← hea, ← heb, heq] at hl
    exact lex_lt_irrefl _ hl
  · exact hne h
  · have hl := (u64bytes_lex _ _).mp ((sign_flip_order_iso b a).mp h)
    rw [← hea, ← heb, heq] at hl
    exact lex_lt_irrefl _ hl

/-! ## Axiom audit -/

#print axioms encode_mono_u64
#print axioms sign_flip_order_iso
#print axioms encode_mono_i64
#print axioms encode_inj_u64
#print axioms encode_inj_i64

end key_kernel
