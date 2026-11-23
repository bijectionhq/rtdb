import Std

namespace RTDB.Temporal.Ceiling

structure Range where
  lower : Int
  upper : Int
  systemTo : Int

def contains (range : Range) (validTime : Int) : Prop :=
  range.lower ≤ validTime ∧ validTime < range.upper

instance (range : Range) (validTime : Int) : Decidable (contains range validTime) :=
  inferInstanceAs (Decidable (_ ∧ _))

def ceilingsAt (validTime : Int) : List Range → List Int
  | [] => []
  | range :: rest =>
    if contains range validTime then range.systemTo :: ceilingsAt validTime rest
    else ceilingsAt validTime rest

def clipRange (lower upper : Int) (range : Range) : Range :=
  ⟨max lower range.lower, min upper range.upper, range.systemTo⟩

def clip (lower upper : Int) (ranges : List Range) : List Range :=
  ranges.filterMap fun range =>
    let clipped := clipRange lower upper range
    if clipped.lower < clipped.upper then some clipped else none

theorem clipped_range_contains (lower upper : Int) (range : Range) (validTime : Int) :
    contains (clipRange lower upper range) validTime ↔
      lower ≤ validTime ∧ validTime < upper ∧ contains range validTime := by
  simp only [clipRange, contains, Int.max_le, Int.lt_min]
  omega

theorem ceilingsAt_clip (lower upper validTime : Int) (ranges : List Range) :
    ceilingsAt validTime (clip lower upper ranges) =
      if lower ≤ validTime ∧ validTime < upper then ceilingsAt validTime ranges else [] := by
  induction ranges with
  | nil => simp [clip, ceilingsAt]
  | cons range rest ih =>
    let clipped := clipRange lower upper range
    have contained : contains clipped validTime ↔
        lower ≤ validTime ∧ validTime < upper ∧ contains range validTime :=
      clipped_range_contains lower upper range validTime
    have sameCeiling : clipped.systemTo = range.systemTo := rfl
    have clipCons : clip lower upper (range :: rest) =
        if clipped.lower < clipped.upper then clipped :: clip lower upper rest
        else clip lower upper rest := by
      by_cases nonempty : (clipRange lower upper range).lower < (clipRange lower upper range).upper <;>
        simp [clip, clipped, nonempty]
    by_cases nonempty : clipped.lower < clipped.upper
    · by_cases within : lower ≤ validTime ∧ validTime < upper
      · have same : contains clipped validTime ↔ contains range validTime := by
          simpa [within.1, within.2] using contained
        simp [clipCons, nonempty, ceilingsAt, same, sameCeiling, ih, within]
      · have outside : ¬ contains clipped validTime := fun present => within
          ⟨(contained.mp present).1, (contained.mp present).2.1⟩
        simp [clipCons, nonempty, ceilingsAt, outside, ih, within]
    · by_cases within : lower ≤ validTime ∧ validTime < upper
      · have outside : ¬ contains range validTime := by
          intro present
          have conflicting : clipped.lower ≤ validTime ∧ validTime < clipped.upper :=
            contained.mpr ⟨within.1, within.2, present⟩
          omega
        simp [clipCons, nonempty, ceilingsAt, ih, within, outside]
      · simp [clipCons, nonempty, ih, within]

theorem ceilingsAt_append (validTime : Int) (first rest : List Range) :
    ceilingsAt validTime (first ++ rest) = ceilingsAt validTime first ++ ceilingsAt validTime rest := by
  induction first with
  | nil => rfl
  | cons range first ih =>
    by_cases present : contains range validTime <;> simp [ceilingsAt, present, ih]

def overwrite (minimum maximum : Int) (ranges : List Range) (update : Range) : List Range :=
  if update.lower < update.upper then
    clip minimum update.lower ranges ++ update :: clip update.upper maximum ranges
  else ranges

theorem ceilingsAt_overwrite (minimum maximum validTime : Int) (ranges : List Range) (update : Range)
    (inDomain : minimum ≤ validTime ∧ validTime < maximum) :
    ceilingsAt validTime (overwrite minimum maximum ranges update) =
      if contains update validTime then [update.systemTo] else ceilingsAt validTime ranges := by
  by_cases nonempty : update.lower < update.upper
  · simp only [overwrite, nonempty, ite_true, ceilingsAt_append, ceilingsAt_clip]
    by_cases below : validTime < update.lower
    · have outside : ¬ contains update validTime := by
        simp only [contains]
        omega
      have beforeUpper : ¬ update.upper ≤ validTime := by omega
      simp [ceilingsAt, inDomain.1, below, outside, ceilingsAt_clip, beforeUpper]
    · by_cases above : update.upper ≤ validTime
      · have outside : ¬ contains update validTime := by
          simp only [contains]
          omega
        simp [ceilingsAt, inDomain.1, below, outside, ceilingsAt_clip, above, inDomain.2]
      · have inside : contains update validTime := by
          simp only [contains]
          omega
        have beforeUpper : ¬ update.upper ≤ validTime := by omega
        simp [ceilingsAt, inDomain.1, below, inside, ceilingsAt_clip, beforeUpper]
  · have outside : ¬ contains update validTime := by
      simp only [contains]
      omega
    simp [overwrite, nonempty, outside]

def overwritePoint (validTime : Int) (prior : Int) (update : Range) : Int :=
  if contains update validTime then update.systemTo else prior

theorem ceilingsAt_overwrites (minimum maximum validTime initial : Int) (ranges updates : List Range)
    (inDomain : minimum ≤ validTime ∧ validTime < maximum)
    (oneCeiling : ceilingsAt validTime ranges = [initial]) :
    ceilingsAt validTime (updates.foldl (overwrite minimum maximum) ranges) =
      [updates.foldl (overwritePoint validTime) initial] := by
  induction updates generalizing ranges initial with
  | nil => exact oneCeiling
  | cons update updates ih =>
    have next : ceilingsAt validTime (overwrite minimum maximum ranges update) =
        [overwritePoint validTime initial update] := by
      rw [ceilingsAt_overwrite minimum maximum validTime _ update inDomain, oneCeiling]
      by_cases covered : contains update validTime <;> simp [overwritePoint, covered]
    exact ih _ _ next

theorem initial_and_overwrites_cover_domain_once (minimum maximum validTime initial : Int) (updates : List Range)
    (inDomain : minimum ≤ validTime ∧ validTime < maximum) :
    ceilingsAt validTime (updates.foldl (overwrite minimum maximum) [⟨minimum, maximum, initial⟩]) =
      [updates.foldl (overwritePoint validTime) initial] := by
  apply ceilingsAt_overwrites minimum maximum validTime initial _ updates inDomain
  simp [ceilingsAt, contains, inDomain.1, inDomain.2]

end RTDB.Temporal.Ceiling
