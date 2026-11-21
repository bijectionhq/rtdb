import Std

namespace RTDB.LimitOpen

inductive Stage
  | input
  | offset
  | fetch
deriving DecidableEq, Repr

abbrev Bound := Option (Except Stage Int)
abbrev Observation := List Stage × Except Stage (Nat × Option Nat)

def bound (stage : Stage) : Bound → Except Stage (Option Nat)
  | none => .ok none
  | some (.error issue) => .error issue
  | some (.ok integer) =>
    if 0 ≤ integer ∧ integer ≤ 9223372036854775807 then .ok (some integer.toNat)
    else .error stage

def openLimited (input : Except Stage Unit) (offset fetch : Bound) : Observation :=
  match input with
  | .error issue => ([.input], .error issue)
  | .ok _ =>
    match bound .offset offset with
    | .error issue => ([.input, .offset], .error issue)
    | .ok start =>
      match bound .fetch fetch with
      | .error issue => ([.input, .offset, .fetch], .error issue)
      | .ok count => ([.input, .offset, .fetch], .ok (start.getD 0, count))

theorem input_failure_precedes_every_bound (issue : Stage) (offset fetch : Bound) :
    openLimited (.error issue) offset fetch = ([.input], .error issue) := by
  rfl

theorem offset_refusal_precedes_fetch (offset fetch : Bound) (issue : Stage)
    (bad : bound .offset offset = .error issue) :
    openLimited (.ok ()) offset fetch = ([.input, .offset], .error issue) := by
  simp [openLimited, bad]

theorem fetch_refusal_follows_valid_offset (offset fetch : Bound) (start : Option Nat) (issue : Stage)
    (good : bound .offset offset = .ok start) (bad : bound .fetch fetch = .error issue) :
    openLimited (.ok ()) offset fetch = ([.input, .offset, .fetch], .error issue) := by
  simp [openLimited, good, bad]

theorem absent_bounds_open_without_skip_or_fetch :
    openLimited (.ok ()) none none = ([.input, .offset, .fetch], .ok (0, none)) := by
  rfl

theorem nonnegative_representable_integer_is_preserved (position : Stage) (integer : Int)
    (finite : 0 ≤ integer ∧ integer ≤ 9223372036854775807) :
    bound position (some (.ok integer)) = .ok (some integer.toNat) := by
  simp [bound, finite]

theorem minus_one_and_overflow_are_refused (stage : Stage) :
    bound stage (some (.ok (-1))) = .error stage ∧
      bound stage (some (.ok 9223372036854775808)) = .error stage := by
  simp [bound]

theorem bound_expression_preserves_its_own_error (position issue : Stage) :
    bound position (some (.error issue)) = .error issue := by
  rfl

def directBound : Bound → Option (Option Nat)
  | none => some none
  | some (.error _) => none
  | some (.ok integer) =>
    if 0 ≤ integer ∧ integer ≤ 9223372036854775807 then some (some integer.toNat) else none

theorem direct_bound_has_the_same_success (position : Stage) (source : Bound) (value : Option Nat)
    (direct : directBound source = some value) : bound position source = .ok value := by
  cases source with
  | none => simp [directBound] at direct; simp [bound, ← direct]
  | some source =>
    cases source with
    | error _ => simp [directBound] at direct
    | ok integer =>
      by_cases finite : 0 ≤ integer ∧ integer ≤ 9223372036854775807
      · simp [directBound, finite] at direct
        simp [bound, finite, ← direct]
      · simp [directBound, finite] at direct

theorem direct_bounds_can_skip_evaluating_the_counts (offset fetch : Bound) (start count : Option Nat)
    (readyOffset : directBound offset = some start) (readyFetch : directBound fetch = some count) :
    (openLimited (.ok ()) offset fetch).2 = .ok (start.getD 0, count) := by
  simp [openLimited, direct_bound_has_the_same_success .offset _ _ readyOffset,
    direct_bound_has_the_same_success .fetch _ _ readyFetch]

theorem count_sum_fits_the_unsigned_accumulator (offset fetch : Nat)
    (offsetFits : offset ≤ 9223372036854775807) (fetchFits : fetch ≤ 9223372036854775807) :
    offset + fetch < 18446744073709551616 := by
  omega

#print axioms input_failure_precedes_every_bound
#print axioms offset_refusal_precedes_fetch
#print axioms fetch_refusal_follows_valid_offset
#print axioms absent_bounds_open_without_skip_or_fetch
#print axioms nonnegative_representable_integer_is_preserved
#print axioms minus_one_and_overflow_are_refused
#print axioms bound_expression_preserves_its_own_error
#print axioms direct_bound_has_the_same_success
#print axioms direct_bounds_can_skip_evaluating_the_counts
#print axioms count_sum_fits_the_unsigned_accumulator

end RTDB.LimitOpen
