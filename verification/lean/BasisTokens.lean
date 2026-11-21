import Std

namespace RTDB.BasisTokens

def covers : List Int → List Int → Prop
  | [], [] => True
  | available :: rest, requested :: tail => requested ≤ available ∧ covers rest tail
  | _, _ => False

def mergePositions (left right : List Int) : List Int := List.zipWith max left right

theorem merged_positions_cover_both_tokens (left right : List Int) (sameCount : left.length = right.length) :
    covers (mergePositions left right) left ∧ covers (mergePositions left right) right := by
  induction left generalizing right with
  | nil =>
    cases right with
    | nil => exact ⟨True.intro, True.intro⟩
    | cons first rest => simp at sameCount
  | cons first rest ih =>
    cases right with
    | nil => simp at sameCount
    | cons other tail =>
      have tails := ih tail (Nat.succ.inj sameCount)
      exact ⟨⟨Int.le_max_left _ _, tails.1⟩, ⟨Int.le_max_right _ _, tails.2⟩⟩

theorem a_common_upper_bound_covers_the_merge (candidate left right : List Int)
    (leftCovered : covers candidate left) (rightCovered : covers candidate right) :
    covers candidate (mergePositions left right) := by
  induction candidate generalizing left right with
  | nil =>
    cases left <;> cases right <;> simp [mergePositions, covers] at *
  | cons available rest ih =>
    cases left with
    | nil => simp [covers] at leftCovered
    | cons first tail =>
      cases right with
      | nil => simp [covers] at rightCovered
      | cons other suffix =>
        exact ⟨Int.max_le.mpr ⟨leftCovered.1, rightCovered.1⟩, ih tail suffix leftCovered.2 rightCovered.2⟩

theorem covering_the_merge_is_equivalent_to_covering_both_tokens (candidate left right : List Int)
    (sameCount : left.length = right.length) :
    covers candidate (mergePositions left right) ↔ covers candidate left ∧ covers candidate right := by
  constructor
  · intro coversMerge
    obtain ⟨coversLeft, coversRight⟩ := merged_positions_cover_both_tokens left right sameCount
    have trans (high middle low : List Int) (upper : covers high middle) (lower : covers middle low) :
        covers high low := by
      induction high generalizing middle low with
      | nil => cases middle <;> cases low <;> simp [covers] at *
      | cons h hs ih =>
        cases middle with
        | nil => simp [covers] at upper
        | cons m ms =>
          cases low with
          | nil => simp [covers] at lower
          | cons l ls => exact ⟨Int.le_trans lower.1 upper.1, ih ms ls upper.2 lower.2⟩
    exact ⟨trans _ _ _ coversMerge coversLeft, trans _ _ _ coversMerge coversRight⟩
  · rintro ⟨coversLeft, coversRight⟩
    exact a_common_upper_bound_covers_the_merge candidate left right coversLeft coversRight

structure WireTime where
  seconds : Int
  nanos : Nat
  deriving DecidableEq

def zeroTime : WireTime := ⟨0, 0⟩

def encodeTime (time : Option WireTime) : WireTime := time.getD zeroTime

def decodeTime (wire : WireTime) : Option WireTime :=
  if wire = zeroTime then none else some wire

theorem absent_and_explicit_unix_epoch_have_the_same_wire_time :
    encodeTime none = encodeTime (some zeroTime) ∧ decodeTime (encodeTime (some zeroTime)) = none := by
  decide

theorem every_non_default_time_round_trips_without_losing_precision (time : WireTime) (nonzero : time ≠ zeroTime) :
    decodeTime (encodeTime (some time)) = some time := by
  simp [encodeTime, decodeTime, nonzero]

theorem maps_preserve_positions_and_normalize_only_the_default_time (times : List (Option WireTime)) :
    (times.map encodeTime).map decodeTime =
      times.map (fun time => if time = some zeroTime then none else time) := by
  simp only [List.map_map]
  apply List.map_congr_left
  intro time _
  cases time with
  | none => simp [encodeTime, decodeTime]
  | some time =>
    by_cases zero : time = zeroTime <;> simp [encodeTime, decodeTime, zero]

#print axioms merged_positions_cover_both_tokens
#print axioms a_common_upper_bound_covers_the_merge
#print axioms covering_the_merge_is_equivalent_to_covering_both_tokens
#print axioms absent_and_explicit_unix_epoch_have_the_same_wire_time
#print axioms every_non_default_time_round_trips_without_losing_precision
#print axioms maps_preserve_positions_and_normalize_only_the_default_time

end RTDB.BasisTokens
