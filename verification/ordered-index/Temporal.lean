import Init

set_option autoImplicit false

namespace RTDB.OrderedIndex

structure Interval where
  lower : Int
  upper : Int
  deriving DecidableEq

def Interval.contains (interval : Interval) (point : Int) : Prop :=
  interval.lower ≤ point ∧ point < interval.upper

instance (interval : Interval) (point : Int) : Decidable (interval.contains point) :=
  inferInstanceAs (Decidable (interval.lower ≤ point ∧ point < interval.upper))

structure Span (K : Type) where
  valid : Interval
  key : K

inductive Operation (K : Type) where
  | put (key : K)
  | delete
  | erase

structure Event (K : Type) where
  system : Nat
  valid : Interval
  operation : Operation K

variable {K : Type}

/-- A single IID's table history, in commit order including operation order within a commit. -/
def step (systemAt : Nat) (validAt : Int) (before : Option K) (event : Event K) :
    Option K :=
  match event.operation with
  | .erase => none
  | .delete => if event.system ≤ systemAt ∧ event.valid.contains validAt then none else before
  | .put key => if event.system ≤ systemAt ∧ event.valid.contains validAt then some key else before

def tableAt (events : List (Event K)) (systemAt : Nat) (validAt : Int) : Option K :=
  events.foldl (step systemAt validAt) none

def seekSpan (validAt : Int) : List (Span K) → Option K
  | [] => none
  | span :: rest => if span.valid.contains validAt then some span.key else seekSpan validAt rest

/-- Joining adjacent spans with the same tuple preserves every point. -/
theorem coalesce_adjacent_preserves_points (left right : Interval) (key : K)
    (rest : List (Span K)) (point : Int)
    (touch : left.upper = right.lower)
    (leftOrdered : left.lower ≤ left.upper) (rightOrdered : right.lower ≤ right.upper) :
    seekSpan point (⟨left, key⟩ :: ⟨right, key⟩ :: rest) =
      seekSpan point (⟨⟨left.lower, right.upper⟩, key⟩ :: rest) := by
  let combined : Interval := ⟨left.lower, right.upper⟩
  have union : combined.contains point ↔ left.contains point ∨ right.contains point := by
    simp only [combined, Interval.contains]
    omega
  change _ = seekSpan point (⟨combined, key⟩ :: rest)
  by_cases insideLeft : left.contains point
  · simp [seekSpan, insideLeft, union.mpr (Or.inl insideLeft)]
  · by_cases insideRight : right.contains point
    · simp [seekSpan, insideLeft, insideRight, union.mpr (Or.inr insideRight)]
    · have outside : ¬combined.contains point := by
        simpa [insideLeft, insideRight] using union
      simp [seekSpan, insideLeft, insideRight, outside]

/-- Two disjoint fragments of a span after removing a cut. Empty fragments carry no points. -/
def subtract (cut : Interval) (span : Span K) : List (Span K) :=
  [⟨⟨span.valid.lower, min cut.lower span.valid.upper⟩, span.key⟩,
   ⟨⟨max cut.upper span.valid.lower, span.valid.upper⟩, span.key⟩]

def retract (cut : Interval) (before : List (Span K)) : List (Span K) :=
  before.flatMap (subtract cut)

def edits (systemAt : Nat) (before : List (Span K)) (event : Event K) : List (Span K) :=
  match event.operation with
  | .erase => []
  | .delete => if event.system ≤ systemAt then retract event.valid before else before
  | .put key => if event.system ≤ systemAt then
      ⟨event.valid, key⟩ :: retract event.valid before
    else before

theorem seekSpan_append (validAt : Int) (left right : List (Span K)) :
    seekSpan validAt (left ++ right) =
      (seekSpan validAt left).orElse (fun _ => seekSpan validAt right) := by
  induction left with
  | nil => rfl
  | cons span rest ih =>
    by_cases h : span.valid.contains validAt
    · simp [seekSpan, h]
    · simp [seekSpan, h, ih, Option.orElse]

private theorem selectFragments {A : Type} (left right cut old : Prop)
    [Decidable left] [Decidable right] [Decidable cut] [Decidable old] (key : A)
    (covers : (left ∨ right) ↔ old ∧ ¬cut) :
    (if left then some key else if right then some key else none) =
      (if cut then none else if old then some key else none) := by
  by_cases hl : left <;> by_cases hr : right <;> by_cases hc : cut <;> by_cases ho : old <;>
    simp_all

theorem subtract_hits (cut : Interval) (span : Span K) (validAt : Int) :
    seekSpan validAt (subtract cut span) =
      if cut.contains validAt then none else seekSpan validAt [span] := by
  apply selectFragments
  simp only [Interval.contains, Int.lt_min, Int.max_le]
  omega

theorem retract_hits (cut : Interval) (before : List (Span K)) (validAt : Int) :
    seekSpan validAt (retract cut before) =
      if cut.contains validAt then none else seekSpan validAt before := by
  induction before with
  | nil => simp [retract, seekSpan]
  | cons span rest ih =>
    by_cases h : cut.contains validAt
    · simp [retract, List.flatMap_cons, seekSpan_append, subtract_hits, h,
        show seekSpan validAt (List.flatMap (subtract cut) rest) = none by simpa [retract, h] using ih]
    · rw [retract, List.flatMap_cons, seekSpan_append, subtract_hits, ite_eq_right h]
      have hrest : seekSpan validAt (List.flatMap (subtract cut) rest) = seekSpan validAt rest := by
        simpa [retract, h] using ih
      rw [hrest]
      by_cases hs : span.valid.contains validAt <;> simp [h, hs, seekSpan]

theorem edits_refine_step (systemAt : Nat) (validAt : Int)
    (before : List (Span K)) (event : Event K) :
    seekSpan validAt (edits systemAt before event) =
      step systemAt validAt (seekSpan validAt before) event := by
  cases event with
  | mk system valid operation =>
    cases operation <;> by_cases hs : system ≤ systemAt <;>
      by_cases hv : valid.contains validAt <;>
      simp [edits, step, hs, hv, retract_hits, seekSpan]

theorem replay_refines_table_from (events : List (Event K)) (systemAt : Nat) (validAt : Int)
    (before : List (Span K)) :
    seekSpan validAt (events.foldl (edits systemAt) before) =
      events.foldl (step systemAt validAt) (seekSpan validAt before) := by
  induction events generalizing before with
  | nil => rfl
  | cons event rest ih =>
    simp only [List.foldl_cons]
    rw [ih, edits_refine_step]

/-- Projection can be maintained in commit order and queried at any valid point. -/
theorem replay_refines_table (events : List (Event K)) (systemAt : Nat) (validAt : Int) :
    seekSpan validAt (events.foldl (edits systemAt) []) = tableAt events systemAt validAt :=
  replay_refines_table_from events systemAt validAt []

/-- Any exact prefix checkpoint is an interchangeable starting point for tail replay. -/
theorem replay_checkpoint (front tail : List (Event K)) (systemAt : Nat) (validAt : Int) :
    seekSpan validAt (tail.foldl (edits systemAt) (front.foldl (edits systemAt) [])) =
      tableAt (front ++ tail) systemAt validAt := by
  rw [← List.foldl_append]
  exact replay_refines_table (front ++ tail) systemAt validAt

/-- An erasure in the captured tail removes every older fact, including at an earlier system time. -/
theorem erase_cuts_history (front tail : List (Event K)) (committed : Nat)
    (valid : Interval) (systemAt : Nat) (validAt : Int) :
    tableAt (front ++ ⟨committed, valid, .erase⟩ :: tail) systemAt validAt =
      tableAt tail systemAt validAt := by
  simp [tableAt, List.foldl_append, step]


def hitsSpans (validAt : Int) : List (Span K) → Nat
  | [] => 0
  | span :: rest =>
    (if span.valid.contains validAt then 1 else 0) + hitsSpans validAt rest

theorem hitsSpans_append (validAt : Int) (left right : List (Span K)) :
    hitsSpans validAt (left ++ right) = hitsSpans validAt left + hitsSpans validAt right := by
  induction left with
  | nil => simp [hitsSpans]
  | cons span rest ih => simp [hitsSpans, ih, Nat.add_assoc]

private theorem countFragments (left right cut old : Prop)
    [Decidable left] [Decidable right] [Decidable cut] [Decidable old]
    (covers : (left ∨ right) ↔ old ∧ ¬cut) (disjoint : ¬(left ∧ right)) :
    (if left then 1 else 0) + (if right then 1 else 0) =
      (if cut then 0 else if old then 1 else 0) := by
  by_cases hl : left <;> by_cases hr : right <;> by_cases hc : cut <;> by_cases ho : old <;>
    simp_all

theorem subtract_count (cut : Interval) (span : Span K) (validAt : Int)
    (ordered : cut.lower ≤ cut.upper) :
    hitsSpans validAt (subtract cut span) =
      if cut.contains validAt then 0 else hitsSpans validAt [span] := by
  simp only [subtract, hitsSpans, Nat.add_zero]
  apply countFragments <;> simp only [Interval.contains, Int.lt_min, Int.max_le] <;> omega

theorem retract_count (cut : Interval) (spans : List (Span K)) (validAt : Int)
    (ordered : cut.lower ≤ cut.upper) :
    hitsSpans validAt (retract cut spans) =
      if cut.contains validAt then 0 else hitsSpans validAt spans := by
  induction spans with
  | nil => simp [retract, hitsSpans]
  | cons span rest ih =>
    change hitsSpans validAt (subtract cut span ++ retract cut rest) = _
    rw [hitsSpans_append, subtract_count cut span validAt ordered, ih]
    have hc : hitsSpans validAt (span :: rest) =
        hitsSpans validAt [span] + hitsSpans validAt rest :=
      hitsSpans_append validAt [span] rest
    rw [hc]
    split <;> simp

theorem edits_keep_one (systemAt : Nat) (validAt : Int) (spans : List (Span K))
    (event : Event K) (ordered : event.valid.lower ≤ event.valid.upper)
    (unique : hitsSpans validAt spans ≤ 1) :
    hitsSpans validAt (edits systemAt spans event) ≤ 1 := by
  cases event with
  | mk system valid operation =>
    cases operation with
    | erase => simp [edits, hitsSpans]
    | delete =>
      by_cases hs : system ≤ systemAt <;> by_cases hv : valid.contains validAt <;>
        simp [edits, hs, hv, retract_count, ordered, unique]
    | put key =>
      by_cases hs : system ≤ systemAt
      · change hitsSpans validAt (if system ≤ systemAt then
            [⟨valid, key⟩] ++ retract valid spans else spans) ≤ 1
        rw [ite_eq_left hs, hitsSpans_append, retract_count valid spans validAt ordered]
        by_cases hv : valid.contains validAt <;> simp [hitsSpans, hv] at unique ⊢ <;>
          assumption <;> done
      · simpa [edits, hs] using unique

theorem replay_keeps_one (events : List (Event K)) (systemAt : Nat) (validAt : Int)
    (spans : List (Span K)) (initial : hitsSpans validAt spans ≤ 1)
    (ordered : ∀ event ∈ events, event.valid.lower ≤ event.valid.upper) :
    hitsSpans validAt (events.foldl (edits systemAt) spans) ≤ 1 := by
  induction events generalizing spans with
  | nil => exact initial
  | cons event rest ih =>
    apply ih (edits systemAt spans event)
    · exact edits_keep_one systemAt validAt spans event (ordered event (by simp)) initial
    · exact fun e he => ordered e (by simp [he])

end RTDB.OrderedIndex
