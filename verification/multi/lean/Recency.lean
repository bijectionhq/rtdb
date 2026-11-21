import Std

namespace RTDB.Recency

structure Range where
  validFrom : Int
  validTo : Int
  systemTo : Int

def score (range : Range) : Int := min range.validTo range.systemTo

def fullScan (accumulator : Int) (ranges : List Range) : Int :=
  ranges.foldl (fun current range => max current (score range)) accumulator

def earlyExit (accumulator : Int) : List Range → Int
  | [] => accumulator
  | range :: older =>
    let best := max accumulator (score range)
    if range.validFrom ≤ best then best
    else earlyExit best older

def Descending : List Range → Prop
  | [] => True
  | range :: older =>
    (∀ previous ∈ older, previous.validTo ≤ range.validFrom) ∧ Descending older

theorem fullScan_eq_initial (initial : Int) (ranges : List Range)
    (bounded : ∀ range ∈ ranges, score range ≤ initial) :
    fullScan initial ranges = initial := by
  induction ranges with
  | nil => rfl
  | cons range older inductionHypothesis =>
    have headBound := bounded range (List.mem_cons_self ..)
    have tailBound : ∀ previous ∈ older, score previous ≤ initial :=
      fun previous member => bounded previous (List.mem_cons_of_mem _ member)
    unfold fullScan at *
    rw [List.foldl_cons, Int.max_eq_left headBound]
    exact inductionHypothesis tailBound

theorem earlyExit_eq_fullScan (initial : Int) (ranges : List Range)
    (ordered : Descending ranges) :
    earlyExit initial ranges = fullScan initial ranges := by
  induction ranges generalizing initial with
  | nil => rfl
  | cons range older inductionHypothesis =>
    have ordering : (∀ previous ∈ older, previous.validTo ≤ range.validFrom) ∧
        Descending older := ordered
    let best := max initial (score range)
    change (if range.validFrom ≤ best then best else earlyExit best older) = fullScan best older
    by_cases stop : range.validFrom ≤ best
    · have restBound : ∀ previous ∈ older, score previous ≤ best :=
        fun previous member =>
          Int.le_trans (Int.min_le_left ..) (Int.le_trans (ordering.1 previous member) stop)
      simp [stop, fullScan_eq_initial best older restBound]
    · simpa [stop] using inductionHypothesis best ordering.2

end RTDB.Recency
