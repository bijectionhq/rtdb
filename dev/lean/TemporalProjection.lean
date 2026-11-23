import TemporalCeiling
import TemporalHistory

namespace RTDB.Temporal

open Ceiling

def observeRanges (point : Point) (systemFrom : Int) (value : Value) (ranges : List Range) : List Value :=
  ranges.filterMap fun range =>
    if contains range point.validTime ∧ systemFrom ≤ point.systemTime ∧ point.systemTime < range.systemTo
    then some value else none

theorem observeRanges_eq_ceilings (point : Point) (systemFrom : Int) (value : Value) (ranges : List Range) :
    observeRanges point systemFrom value ranges =
      (ceilingsAt point.validTime ranges).filterMap fun systemTo =>
        if systemFrom ≤ point.systemTime ∧ point.systemTime < systemTo then some value else none := by
  induction ranges with
  | nil => rfl
  | cons range rest ih =>
    simp only [observeRanges] at ih
    by_cases covered : contains range point.validTime
    · simp only [observeRanges, ceilingsAt, List.filterMap_cons, covered, ite_true, true_and, ih]
    · simp only [observeRanges, ceilingsAt, List.filterMap_cons, covered, ite_false, false_and, ih]

theorem observeRanges_clamp_valid (point : Point) (systemFrom : Int) (value : Value)
    (lower upper : Int) (ranges : List Range) (inQuery : lower ≤ point.validTime ∧ point.validTime < upper) :
    observeRanges point systemFrom value (clip lower upper ranges) = observeRanges point systemFrom value ranges := by
  simp [observeRanges_eq_ceilings, ceilingsAt_clip, inQuery]

def updateOf (event : Event Value) : Range := ⟨event.validFrom, event.validTo, event.systemFrom⟩

def rangeRows (point : Point) (minimum maximum : Int) (ranges : List Range) : List (Event Value) → List Value
  | [] => []
  | event :: older =>
    if event.iid ≠ point.iid then rangeRows point minimum maximum ranges older
    else
      let updated := overwrite minimum maximum ranges (updateOf event)
      match event.mutation with
      | .erase => []
      | .delete => rangeRows point minimum maximum updated older
      | .put value =>
        observeRanges point event.systemFrom value (clip event.validFrom event.validTo ranges) ++
          rangeRows point minimum maximum updated older

theorem rangeRows_eq_ceilingRows (point : Point) (minimum maximum ceiling : Int) (ranges : List Range)
    (history : List (Event Value)) (inDomain : minimum ≤ point.validTime ∧ point.validTime < maximum)
    (oneCeiling : ceilingsAt point.validTime ranges = [ceiling]) :
    rangeRows point minimum maximum ranges history = ceilingRows point ceiling history := by
  induction history generalizing ranges ceiling with
  | nil => rfl
  | cons event older ih =>
    by_cases entity : event.iid = point.iid
    · let next := overwrite minimum maximum ranges (updateOf event)
      have projection := ceilingsAt_overwrite minimum maximum point.validTime ranges (updateOf event) inDomain
      by_cases covers : event.validFrom ≤ point.validTime ∧ point.validTime < event.validTo
      · have nextCeiling : ceilingsAt point.validTime next = [event.systemFrom] := by
          simpa [next, updateOf, contains, covers] using projection
        have observed (value : Value) :
            observeRanges point event.systemFrom value (clip event.validFrom event.validTo ranges) =
              if event.systemFrom ≤ point.systemTime ∧ point.systemTime < ceiling then [value] else [] := by
          by_cases visible : event.systemFrom ≤ point.systemTime ∧ point.systemTime < ceiling <;>
            simp [observeRanges_eq_ceilings, ceilingsAt_clip, covers, oneCeiling, visible]
        cases op : event.mutation <;> simp [rangeRows, ceilingRows, entity, op, validAt, covers,
          ih event.systemFrom (overwrite minimum maximum ranges (updateOf event)) nextCeiling, observed]
      · have nextCeiling : ceilingsAt point.validTime next = [ceiling] := by
          simpa [next, updateOf, contains, covers, oneCeiling] using projection
        cases op : event.mutation <;> simp [rangeRows, ceilingRows, entity, op, validAt, covers,
          ih ceiling (overwrite minimum maximum ranges (updateOf event)) nextCeiling,
          observeRanges_eq_ceilings, ceilingsAt_clip]
    · simp [rangeRows, ceilingRows, entity, ih ceiling ranges oneCeiling]

def belowSystemUpper (upper : Int) (event : Event Value) : Bool :=
  match event.mutation with
  | .erase => true
  | _ => decide (event.systemFrom < upper)

theorem newest_belowSystemUpper (point : Point) (upper : Int) (history : List (Event Value))
    (inQuery : point.systemTime < upper) :
    newest point none (history.filter (belowSystemUpper upper)) = newest point none history := by
  apply newest_filter_eq
  intro event _ skipped
  cases op : event.mutation with
  | erase => simp [belowSystemUpper, op] at skipped
  | put value =>
    have outside : ¬ event.systemFrom < upper := by simpa [belowSystemUpper, op] using skipped
    exact future_mutation point event (by omega) (by simp [op])
  | delete =>
    have outside : ¬ event.systemFrom < upper := by simpa [belowSystemUpper, op] using skipped
    exact future_mutation point event (by omega) (by simp [op])

theorem observed_range_rows_eq_chronological_replay (point : Point) (minimum maximum initial queryUpper : Int)
    (history : List (Event Value)) (validDomain : minimum ≤ point.validTime ∧ point.validTime < maximum)
    (systemDomain : point.systemTime < initial) (inQuery : point.systemTime < queryUpper)
    (descending : DescendingPerEntity history.reverse) :
    rangeRows point minimum maximum [⟨minimum, maximum, initial⟩]
        (history.reverse.filter (belowSystemUpper queryUpper)) =
      (history.foldl (applyEvent point) none).toList := by
  have oneCeiling : ceilingsAt point.validTime [⟨minimum, maximum, initial⟩] = [initial] := by
    simp [ceilingsAt, contains, validDomain.1, validDomain.2]
  have filteredOrder : DescendingPerEntity (history.reverse.filter (belowSystemUpper queryUpper)) := by
    unfold DescendingPerEntity at *
    exact descending.filter (belowSystemUpper queryUpper)
  rw [rangeRows_eq_ceilingRows point minimum maximum initial _ _ validDomain oneCeiling,
    ceilingRows_eq_newest point initial _ systemDomain filteredOrder, newest_belowSystemUpper point _ _ inQuery,
    newest_reverse_eq_replay]

end RTDB.Temporal
