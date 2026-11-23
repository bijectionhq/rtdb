import LeafRuns

namespace RTDB.TrieSiblingOrder

open IidByteOrder

def base4Digit (quotient : Nat) : Byte :=
  ⟨quotient % 4, Nat.lt_trans (Nat.mod_lt quotient (by decide)) (by decide)⟩

def base4Byte (byte : Byte) : List Byte :=
  [base4Digit (byte.val / 64), base4Digit (byte.val / 16),
   base4Digit (byte.val / 4), base4Digit byte.val]

def base4Iid (bytes : List Byte) : List Byte := bytes.flatMap base4Byte

theorem bucket_at_depth_uses_the_corresponding_byte_lane (bytes : List Byte) (depth : Nat) :
    (base4Iid bytes)[depth]? = (bytes[depth / 4]?).bind (fun byte => (base4Byte byte)[depth % 4]?) := by
  induction bytes generalizing depth with
  | nil => simp [base4Iid]
  | cons byte rest inductionHypothesis =>
      change (base4Byte byte ++ base4Iid rest)[depth]? =
        ((byte :: rest)[depth / 4]?).bind (fun byte => (base4Byte byte)[depth % 4]?)
      have size : (base4Byte byte).length = 4 := rfl
      by_cases first : depth < 4
      · have index : depth / 4 = 0 := by omega
        rw [List.getElem?_append_left (by simpa [size] using first), index]
        simp [Nat.mod_eq_of_lt first]
      · have index : depth / 4 = (depth - 4) / 4 + 1 := by omega
        have lane : (depth - 4) % 4 = depth % 4 := by omega
        rw [List.getElem?_append_right (by simpa [size] using Nat.le_of_not_gt first),
          size, inductionHypothesis, index, List.getElem?_cons_succ, lane]

private theorem reassemble (byte : Byte) :
    64 * (byte.val / 64 % 4) + 16 * (byte.val / 16 % 4) +
      4 * (byte.val / 4 % 4) + byte.val % 4 = byte.val := by
  have high := byte.isLt
  have first := Nat.div_add_mod byte.val 4
  have second := Nat.div_add_mod (byte.val / 4) 4
  have third := Nat.div_add_mod (byte.val / 16) 4
  rw [Nat.div_div_eq_div_mul] at second third
  change 4 * (byte.val / 16) + byte.val / 4 % 4 = byte.val / 4 at second
  change 4 * (byte.val / 64) + byte.val / 16 % 4 = byte.val / 16 at third
  omega

theorem byte_order_matches_base4_digits (left right : Byte) :
    left.val < right.val ↔ precedes (base4Byte left) (base4Byte right) := by
  have leftWord := reassemble left
  have rightWord := reassemble right
  simp [base4Byte, base4Digit, precedes]
  omega

theorem equal_base4_digits_identify_the_byte (left right : Byte)
    (same : base4Byte left = base4Byte right) : left = right := by
  apply Fin.ext
  by_cases below : left.val < right.val
  · have precedesSelf := (byte_order_matches_base4_digits left right).mp below
    rw [same] at precedesSelf
    have contradiction := (byte_order_matches_base4_digits right right).mpr precedesSelf
    omega
  · by_cases above : right.val < left.val
    · have precedesSelf := (byte_order_matches_base4_digits right left).mp above
      rw [same] at precedesSelf
      have contradiction := (byte_order_matches_base4_digits right right).mpr precedesSelf
      omega
    · omega

theorem iid_order_matches_base4_path_order (left right : List Byte) :
    precedes left right ↔ precedes (base4Iid left) (base4Iid right) := by
  induction left generalizing right with
  | nil =>
      cases right with
      | nil => simp [base4Iid, precedes]
      | cons head tail => simp [base4Iid, base4Byte, precedes]
  | cons first left previous =>
      cases right with
      | nil => simp [base4Iid, base4Byte, precedes]
      | cons second right =>
          have sameWidth : (base4Byte first).length = (base4Byte second).length := rfl
          have equality : base4Byte first = base4Byte second ↔ first.val = second.val := by
            constructor
            · intro digits
              exact congrArg Fin.val (equal_base4_digits_identify_the_byte _ _ digits)
            · intro value
              rw [Fin.ext value]
          change
            (first.val < second.val ∨ (first.val = second.val ∧ precedes left right)) ↔
              precedes (base4Byte first ++ base4Iid left) (base4Byte second ++ base4Iid right)
          rw [prefix_then_suffix _ _ _ _ sameWidth, ← byte_order_matches_base4_digits, equality, ← previous]

theorem separate_sibling_prefixes_order_their_rows (left right root : List Byte) (first second : Byte)
    (leftTail rightTail : List Byte)
    (leftPath : base4Iid left = root ++ first :: leftTail)
    (rightPath : base4Iid right = root ++ second :: rightTail)
    (earlier : first.val < second.val) : precedes left right := by
  apply (iid_order_matches_base4_path_order left right).mpr
  rw [leftPath, rightPath]
  apply (prefix_then_suffix root root _ _ rfl).mpr
  exact Or.inr ⟨rfl, Or.inl earlier⟩

theorem earlier_sibling_precedes_in_leaf_rank
    (left right root : List Byte) (first second : Byte) (leftTail rightTail : List Byte)
    (leftRow rightRow : Nat)
    (leftPath : base4Iid left = root ++ first :: leftTail)
    (rightPath : base4Iid right = root ++ second :: rightTail)
    (earlier : first.val < second.val) (sameWidth : left.length = right.length)
    (leftFits : leftRow < 4294967296) (rightFits : rightRow < 4294967296) :
    iidRowRank left leftRow < iidRowRank right rightRow :=
  (iid_row_rank_agrees_with_the_key left right leftRow rightRow sameWidth leftFits rightFits).mpr
    (Or.inl (separate_sibling_prefixes_order_their_rows left right root first second
      leftTail rightTail leftPath rightPath earlier))

abbrev Ordinal := Fin 4294967296

def leafRank (iid : Ordinal → List Byte) (row : Ordinal) := iidRowRank (iid row) row.val

theorem sibling_order_recovers_the_source_run
    (iid : Ordinal → List Byte) (root : List Byte) (buckets : List Byte)
    (rows : Byte → List Ordinal) (source : List Ordinal)
    (bucketOrder : buckets.Pairwise fun first second => first.val < second.val)
    (width : ∀ row, (iid row).length = 16)
    (matching : ∀ bucket ∈ buckets, ∀ row ∈ rows bucket,
      ∃ suffix, base4Iid (iid row) = root ++ bucket :: suffix)
    (within : ∀ bucket ∈ buckets, LeafRuns.ordered (leafRank iid) (rows bucket))
    (sourceOrder : LeafRuns.ordered (leafRank iid) source)
    (occurrences : ∀ row, (buckets.flatMap rows).count row = source.count row) :
    buckets.flatMap rows = source := by
  have visitedOrder : LeafRuns.ordered (leafRank iid) (buckets.flatMap rows) := by
    apply List.pairwise_flatMap.mpr
    refine ⟨within, ?_⟩
    apply List.pairwise_iff_forall_sublist.mpr
    intro first second inBuckets left leftIn right rightIn
    have members := inBuckets.subset
    obtain ⟨leftTail, leftPath⟩ := matching first (members (by simp)) left leftIn
    obtain ⟨rightTail, rightPath⟩ := matching second (members (by simp)) right rightIn
    have sameWidth : (iid left).length = (iid right).length := by
      rw [width, width]
    have inOrder : first.val < second.val :=
      List.pairwise_iff_forall_sublist.mp bucketOrder inBuckets
    have earlier := earlier_sibling_precedes_in_leaf_rank (iid left) (iid right) root
      first second leftTail rightTail left.val right.val leftPath rightPath
      inOrder sameWidth left.isLt right.isLt
    exact decide_eq_true (Nat.le_of_lt earlier)
  have injective : ∀ {left right}, leafRank iid left = leafRank iid right → left = right := by
    intro left right same
    exact Fin.ext (equal_iid_row_ranks_identify_the_row (iid left) (iid right) left.val right.val
      left.isLt right.isLt same)
  exact (LeafRuns.independently_sorted_occurrences_have_one_sequence (leafRank iid) injective
    ⟨visitedOrder, List.perm_iff_count.mpr occurrences⟩).trans
      (LeafRuns.independently_sorted_occurrences_have_one_sequence (leafRank iid) injective
        ⟨sourceOrder, List.Perm.refl source⟩).symm

#print axioms bucket_at_depth_uses_the_corresponding_byte_lane
#print axioms sibling_order_recovers_the_source_run
#print axioms earlier_sibling_precedes_in_leaf_rank
#print axioms byte_order_matches_base4_digits
#print axioms equal_base4_digits_identify_the_byte
#print axioms iid_order_matches_base4_path_order
#print axioms separate_sibling_prefixes_order_their_rows

end RTDB.TrieSiblingOrder
