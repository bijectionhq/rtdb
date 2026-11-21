import Std

namespace RTDB.IidByteOrder

abbrev Byte := Fin 256

def unsignedWord : List Byte → Nat
  | [] => 0
  | first :: rest => first.val * 256 ^ rest.length + unsignedWord rest

theorem unsigned_word_fits_in_its_bytes (bytes : List Byte) :
    unsignedWord bytes < 256 ^ bytes.length := by
  induction bytes with
  | nil => simp [unsignedWord]
  | cons first rest bounded =>
      have digit : first.val + 1 ≤ 256 := first.isLt
      calc
        unsignedWord (first :: rest)
            < first.val * 256 ^ rest.length + 256 ^ rest.length :=
              Nat.add_lt_add_left bounded _
        _ = (first.val + 1) * 256 ^ rest.length := by rw [Nat.add_mul, Nat.one_mul]
        _ ≤ 256 * 256 ^ rest.length := Nat.mul_le_mul_right _ digit
        _ = 256 ^ (first :: rest).length := by rw [List.length_cons, Nat.pow_succ']

def precedes : List Byte → List Byte → Prop
  | [], _ :: _ => True
  | _, [] => False
  | first :: left, second :: right =>
      first.val < second.val ∨ (first.val = second.val ∧ precedes left right)

theorem first_different_byte_orders_equal_width_words (first second : Byte)
    (left right : List Byte) (same : left.length = right.length) (earlier : first.val < second.val) :
    unsignedWord (first :: left) < unsignedWord (second :: right) := by
  have bounded := unsigned_word_fits_in_its_bytes left
  have digit : first.val + 1 ≤ second.val := earlier
  calc
    unsignedWord (first :: left) < (first.val + 1) * 256 ^ left.length := by
      rw [unsignedWord, Nat.add_mul, Nat.one_mul]
      exact Nat.add_lt_add_left bounded _
    _ ≤ second.val * 256 ^ right.length := by
      rw [same]
      exact Nat.mul_le_mul_right _ digit
    _ ≤ unsignedWord (second :: right) := by
      simp [unsignedWord]

theorem unsigned_word_order_matches_lexicographic_bytes (left right : List Byte)
    (same : left.length = right.length) :
    unsignedWord left < unsignedWord right ↔ precedes left right := by
  induction left generalizing right with
  | nil =>
      cases right <;> simp_all [unsignedWord, precedes]
  | cons first left prior =>
      cases right with
      | nil => simp at same
      | cons second right =>
          simp only [List.length_cons, Nat.succ.injEq] at same
          by_cases equal : first.val = second.val
          · simpa [unsignedWord, precedes, equal, same] using prior right same
          · by_cases earlier : first.val < second.val
            · simp [precedes, earlier,
                first_different_byte_orders_equal_width_words first second left right same earlier]
            · have later : second.val < first.val := by omega
              have converse := first_different_byte_orders_equal_width_words second first right left same.symm later
              simp [precedes, earlier, equal, Nat.not_lt_of_ge (Nat.le_of_lt converse)]

theorem equal_width_words_are_injective (left right : List Byte)
    (same : left.length = right.length) (equal : unsignedWord left = unsignedWord right) :
    left = right := by
  induction left generalizing right with
  | nil =>
      cases right <;> simp_all
  | cons first left prior =>
      cases right with
      | nil => simp at same
      | cons second right =>
          simp only [List.length_cons, Nat.succ.injEq] at same
          have headSame : first.val = second.val := by
            by_cases headEqual : first.val = second.val
            · exact headEqual
            · by_cases headEarlier : first.val < second.val
              · exact False.elim ((Nat.ne_of_lt
                  (first_different_byte_orders_equal_width_words first second left right same headEarlier)) equal)
              · have headLater : second.val < first.val := by omega
                exact False.elim ((Nat.ne_of_lt
                  (first_different_byte_orders_equal_width_words second first right left same.symm headLater)) equal.symm)
          simp [unsignedWord, same, headSame] at equal
          rw [Fin.ext headSame, prior right same equal]

theorem prefix_then_suffix (left right leftTail rightTail : List Byte)
    (same : left.length = right.length) :
    precedes (left ++ leftTail) (right ++ rightTail) ↔
      precedes left right ∨ (left = right ∧ precedes leftTail rightTail) := by
  induction left generalizing right with
  | nil =>
      cases right <;> simp_all [precedes]
  | cons first left prior =>
      cases right with
      | nil => simp at same
      | cons second right =>
          simp only [List.length_cons, Nat.succ.injEq] at same
          simp only [List.cons_append, precedes, List.cons.injEq]
          rw [prior right same]
          have headEq : first.val = second.val ↔ first = second :=
            ⟨Fin.ext, fun h => congrArg Fin.val h⟩
          simp only [headEq]
          grind

theorem two_unsigned_words_match_iid_byte_order
    (leftHead rightHead leftTail rightTail : List Byte)
    (sameHead : leftHead.length = rightHead.length)
    (sameTail : leftTail.length = rightTail.length) :
    precedes (leftHead ++ leftTail) (rightHead ++ rightTail) ↔
      unsignedWord leftHead < unsignedWord rightHead ∨
        (unsignedWord leftHead = unsignedWord rightHead ∧ unsignedWord leftTail < unsignedWord rightTail) := by
  rw [prefix_then_suffix leftHead rightHead leftTail rightTail sameHead,
    ← unsigned_word_order_matches_lexicographic_bytes leftHead rightHead sameHead,
    ← unsigned_word_order_matches_lexicographic_bytes leftTail rightTail sameTail]
  exact or_congr Iff.rfl (and_congr
    ⟨fun equal => congrArg unsignedWord equal, equal_width_words_are_injective leftHead rightHead sameHead⟩ Iff.rfl)

private theorem compare_flipped_sign_bits
    (leftSign rightSign : Bool) (left right : BitVec width) :
    ((BitVec.cons leftSign left) ^^^ BitVec.cons true (0#width)).slt
      ((BitVec.cons rightSign right) ^^^ BitVec.cons true (0#width)) =
    (BitVec.cons leftSign left).ult (BitVec.cons rightSign right) := by
  have leftFits := left.isLt
  have rightFits := right.isLt
  simp only [BitVec.cons_xor_cons, BitVec.xor_zero, BitVec.slt_eq_ult, BitVec.msb_cons,
    BitVec.ult_eq_decide, BitVec.toNat_cons']
  cases leftSign <;> cases rightSign <;> simp [Nat.shiftLeft_eq, ← decide_not] <;> omega

theorem flipped_sign_bit_refines_unsigned_long_order (left right : BitVec 64) :
    BitVec.slt (left ^^^ 0x8000000000000000#64) (right ^^^ 0x8000000000000000#64) =
      BitVec.ult left right := by
  have mask : BitVec.cons true (0#63) = 0x8000000000000000#64 := by decide
  simpa only [BitVec.cons_msb_setWidth, mask] using
    compare_flipped_sign_bits left.msb right.msb (left.setWidth 63) (right.setWidth 63)

def word64 (bytes : List Byte) : BitVec 64 := BitVec.ofNat 64 (unsignedWord bytes)

theorem word64_preserves_eight_bytes (bytes : List Byte) (width : bytes.length = 8) :
    (word64 bytes).toNat = unsignedWord bytes := by
  have fits := unsigned_word_fits_in_its_bytes bytes
  rw [width] at fits
  exact Nat.mod_eq_of_lt fits

theorem same_word64_iff_same_eight_bytes (left right : List Byte)
    (leftWidth : left.length = 8) (rightWidth : right.length = 8) :
    word64 left = word64 right ↔ left = right := by
  constructor
  · intro eq
    have sameNat := congrArg BitVec.toNat eq
    rw [word64_preserves_eight_bytes left leftWidth, word64_preserves_eight_bytes right rightWidth] at sameNat
    exact equal_width_words_are_injective left right (leftWidth.trans rightWidth.symm) sameNat
  · rintro rfl
    rfl

def arrowWordBefore (left right : BitVec 64) : Prop :=
  (left ^^^ 0x8000000000000000#64).slt (right ^^^ 0x8000000000000000#64) = true

theorem arrow_word_comparison_matches_eight_bytes (left right : List Byte)
    (leftWidth : left.length = 8) (rightWidth : right.length = 8) :
    arrowWordBefore (word64 left) (word64 right) ↔ precedes left right := by
  simp only [arrowWordBefore, flipped_sign_bit_refines_unsigned_long_order, BitVec.ult_eq_decide,
    decide_eq_true_eq, word64_preserves_eight_bytes left leftWidth, word64_preserves_eight_bytes right rightWidth]
  exact unsigned_word_order_matches_lexicographic_bytes left right (leftWidth.trans rightWidth.symm)

def arrowRowBefore (leftHead rightHead leftTail rightTail : BitVec 64) (leftRow rightRow : Nat) : Prop :=
  arrowWordBefore leftHead rightHead ∨
    (leftHead = rightHead ∧
      (arrowWordBefore leftTail rightTail ∨ (leftTail = rightTail ∧ rightRow < leftRow)))

theorem arrow_leaf_comparison_matches_iid_then_descending_row
    (leftHead rightHead leftTail rightTail : List Byte) (leftRow rightRow : Nat)
    (lh : leftHead.length = 8) (rh : rightHead.length = 8)
    (lt : leftTail.length = 8) (rt : rightTail.length = 8) :
    arrowRowBefore (word64 leftHead) (word64 rightHead) (word64 leftTail) (word64 rightTail)
        leftRow rightRow ↔
      precedes (leftHead ++ leftTail) (rightHead ++ rightTail) ∨
        (leftHead ++ leftTail = rightHead ++ rightTail ∧ rightRow < leftRow) := by
  have sameBytes : leftHead ++ leftTail = rightHead ++ rightTail ↔
      leftHead = rightHead ∧ leftTail = rightTail := by
    constructor
    · intro eq
      have headSame := congrArg (List.take 8) eq
      have tailSame := congrArg (List.drop 8) eq
      simpa [List.take_append_of_le_length, List.drop_append_of_le_length, lh, rh] using
        And.intro headSame tailSame
    · rintro ⟨rfl, rfl⟩
      rfl
  simp only [arrowRowBefore, arrow_word_comparison_matches_eight_bytes _ _ lh rh,
    arrow_word_comparison_matches_eight_bytes _ _ lt rt, same_word64_iff_same_eight_bytes _ _ lh rh,
    same_word64_iff_same_eight_bytes _ _ lt rt, sameBytes,
    prefix_then_suffix leftHead rightHead leftTail rightTail (lh.trans rh.symm)]
  grind

def iidRowRank (iid : List Byte) (row : Nat) : Nat :=
  unsignedWord iid * 4294967296 + (4294967295 - row)

theorem iid_row_rank_agrees_with_the_key (left right : List Byte) (leftRow rightRow : Nat)
    (sameWidth : left.length = right.length)
    (leftFits : leftRow < 4294967296) (rightFits : rightRow < 4294967296) :
    iidRowRank left leftRow < iidRowRank right rightRow ↔
      precedes left right ∨ (left = right ∧ rightRow < leftRow) := by
  have sameWord : left = right ↔ unsignedWord left = unsignedWord right :=
    ⟨congrArg unsignedWord, equal_width_words_are_injective left right sameWidth⟩
  rw [← unsigned_word_order_matches_lexicographic_bytes left right sameWidth, sameWord]
  unfold iidRowRank
  omega

theorem equal_iid_row_ranks_identify_the_row (left right : List Byte) (leftRow rightRow : Nat)
    (leftFits : leftRow < 4294967296) (rightFits : rightRow < 4294967296)
    (same : iidRowRank left leftRow = iidRowRank right rightRow) :
    leftRow = rightRow := by
  unfold iidRowRank at same
  omega

theorem arrow_leaf_comparison_realizes_the_leaf_rank
    (leftHead rightHead leftTail rightTail : List Byte) (leftRow rightRow : Nat)
    (lh : leftHead.length = 8) (rh : rightHead.length = 8)
    (lt : leftTail.length = 8) (rt : rightTail.length = 8)
    (leftFits : leftRow < 4294967296) (rightFits : rightRow < 4294967296) :
    arrowRowBefore (word64 leftHead) (word64 rightHead) (word64 leftTail) (word64 rightTail)
        leftRow rightRow ↔
      iidRowRank (leftHead ++ leftTail) leftRow < iidRowRank (rightHead ++ rightTail) rightRow := by
  have same : (leftHead ++ leftTail).length = (rightHead ++ rightTail).length := by simp [lh, rh, lt, rt]
  rw [arrow_leaf_comparison_matches_iid_then_descending_row _ _ _ _ _ _ lh rh lt rt,
    iid_row_rank_agrees_with_the_key _ _ _ _ same leftFits rightFits]

#print axioms unsigned_word_fits_in_its_bytes
#print axioms unsigned_word_order_matches_lexicographic_bytes
#print axioms two_unsigned_words_match_iid_byte_order
#print axioms flipped_sign_bit_refines_unsigned_long_order
#print axioms arrow_leaf_comparison_matches_iid_then_descending_row
#print axioms equal_iid_row_ranks_identify_the_row
#print axioms arrow_leaf_comparison_realizes_the_leaf_rank

end RTDB.IidByteOrder
