import Std

namespace RTDB.PrivateReplay

structure Marker where
  startsAt : Int
  expiresAt : Int
  original : Nat
deriving DecidableEq

structure State where
  marker : Option Marker
  effects : Nat
deriving DecidableEq

structure Attempt where
  submittedAt : Int
  outcome : Nat
  succeeds : Bool
  lookupFailed : Bool

inductive Reply where
  | refused
  | aborted
  | committed (outcome : Nat)
deriving DecidableEq

def admits (cutoff sample : Int) : Bool := decide (sample < cutoff)

def markerEnd (cutoff sample : Int) : Int := max (sample + 3600000000) cutoff

def overlaps (marker : Marker) (sample : Int) : Bool := decide (sample < marker.expiresAt)

def perform (cutoff : Int) (state : State) (attempt : Attempt) : State × Reply :=
  if attempt.succeeds then
    (⟨some ⟨attempt.submittedAt, markerEnd cutoff attempt.submittedAt, attempt.outcome⟩,
      state.effects + 1⟩, .committed attempt.outcome)
  else (state, .aborted)

def step (cutoff : Int) (state : State) (attempt : Attempt) : State × Reply :=
  if admits cutoff attempt.submittedAt then
    if attempt.lookupFailed then (state, .aborted)
    else
      match state.marker with
      | some marker =>
          if overlaps marker attempt.submittedAt then
            (state, .committed marker.original)
          else perform cutoff state attempt
      | none => perform cutoff state attempt
  else (state, .refused)

def Valid (cutoff : Int) (state : State) : Prop :=
  match state.marker with
  | none => state.effects = 0
  | some marker =>
      state.effects = 1 ∧ marker.startsAt < marker.expiresAt ∧ cutoff ≤ marker.expiresAt

theorem exact_cutoff_refuses (cutoff : Int) (state : State) (attempt : Attempt)
    (elapsed : cutoff ≤ attempt.submittedAt) :
    step cutoff state attempt = (state, .refused) := by
  simp [step, admits, Int.not_lt.mpr elapsed]

theorem installed_marker_covers_cutoff (cutoff sample : Int) :
    sample < markerEnd cutoff sample ∧ cutoff ≤ markerEnd cutoff sample := by
  unfold markerEnd
  omega

theorem admits_overlap_every_retained_marker (cutoff sample : Int) (marker : Marker)
    (admitted : admits cutoff sample = true) (covers : cutoff ≤ marker.expiresAt) :
    overlaps marker sample = true := by
  unfold admits at admitted
  simp only [decide_eq_true_eq] at admitted
  exact decide_eq_true (Int.lt_of_lt_of_le admitted covers)

theorem overlap_realizes_valid_time_range (marker : Marker) (sample : Int)
    (nonempty : marker.startsAt < marker.expiresAt) :
    overlaps marker sample = true ↔
      ∃ valid, marker.startsAt ≤ valid ∧ sample ≤ valid ∧ valid < marker.expiresAt := by
  simp only [overlaps, decide_eq_true_eq]
  constructor
  · intro admitted
    exact ⟨max marker.startsAt sample, by omega, by omega, by omega⟩
  · rintro ⟨valid, _, lower, upper⟩
    exact Int.lt_of_le_of_lt lower upper

theorem lookup_failure_preserves_marker_and_effects
    (cutoff : Int) (state : State) (attempt : Attempt)
    (admitted : admits cutoff attempt.submittedAt = true) (failed : attempt.lookupFailed = true) :
    step cutoff state attempt = (state, .aborted) := by
  simp [step, admitted, failed]

theorem duplicates_preserve_the_original_for_any_admitted_sample
    (cutoff : Int) (state : State) (attempt : Attempt) (marker : Marker)
    (valid : Valid cutoff state) (recorded : state.marker = some marker)
    (admitted : admits cutoff attempt.submittedAt = true) (lookedUp : attempt.lookupFailed = false) :
    step cutoff state attempt = (state, .committed marker.original) := by
  simp only [Valid, recorded] at valid
  simp [step, recorded, admitted, lookedUp, admits_overlap_every_retained_marker cutoff _ _ admitted valid.2.2]

theorem step_preserves_atomic_effect (cutoff : Int) (state : State) (attempt : Attempt)
    (valid : Valid cutoff state) : Valid cutoff (step cutoff state attempt).1 := by
  by_cases admitted : admits cutoff attempt.submittedAt = true
  · cases lookup : attempt.lookupFailed with
    | true =>
        rw [lookup_failure_preserves_marker_and_effects _ _ _ admitted lookup]
        exact valid
    | false =>
        cases recorded : state.marker with
        | some marker =>
            rw [duplicates_preserve_the_original_for_any_admitted_sample _ _ _ _ valid recorded admitted lookup]
            exact valid
        | none =>
            have effects : state.effects = 0 := by simpa [Valid, recorded] using valid
            have endBounds := installed_marker_covers_cutoff cutoff attempt.submittedAt
            cases success : attempt.succeeds <;>
              simp [step, perform, admitted, lookup, recorded, Valid, effects, success, endBounds]
  · simpa [step, admitted] using valid

def run (cutoff : Int) (state : State) (attempts : List Attempt) : State :=
  attempts.foldl (fun current next => (step cutoff current next).1) state

theorem ordered_resolution_has_at_most_one_effect (cutoff : Int) (attempts : List Attempt) :
    (run cutoff ⟨none, 0⟩ attempts).effects ≤ 1 := by
  have closed (state : State) (valid : Valid cutoff state) : Valid cutoff (run cutoff state attempts) := by
    induction attempts generalizing state with
    | nil => exact valid
    | cons attempt rest next => exact next _ (step_preserves_atomic_effect cutoff state attempt valid)
  let state := run cutoff ⟨none, 0⟩ attempts
  have valid : Valid cutoff state := closed ⟨none, 0⟩ rfl
  change state.effects ≤ 1
  cases recorded : state.marker <;> simp [Valid, recorded] at valid <;> omega

theorem retained_outcome_survives_subsequent_resolutions
    (cutoff : Int) (state : State) (marker : Marker) (attempts : List Attempt)
    (valid : Valid cutoff state) (recorded : state.marker = some marker) :
    run cutoff state attempts = state := by
  induction attempts with
  | nil => rfl
  | cons attempt rest tail =>
      have unchanged : (step cutoff state attempt).1 = state := by
        by_cases admitted : admits cutoff attempt.submittedAt = true
        · cases lookup : attempt.lookupFailed with
          | true => rw [lookup_failure_preserves_marker_and_effects _ _ _ admitted lookup]
          | false =>
              rw [duplicates_preserve_the_original_for_any_admitted_sample _ _ _ _ valid recorded admitted lookup]
        · simp [step, admitted]
      simpa only [run, List.foldl_cons, unchanged] using tail

#print axioms lookup_failure_preserves_marker_and_effects
#print axioms exact_cutoff_refuses
#print axioms installed_marker_covers_cutoff
#print axioms admits_overlap_every_retained_marker
#print axioms overlap_realizes_valid_time_range
#print axioms duplicates_preserve_the_original_for_any_admitted_sample
#print axioms step_preserves_atomic_effect
#print axioms ordered_resolution_has_at_most_one_effect
#print axioms retained_outcome_survives_subsequent_resolutions

end RTDB.PrivateReplay
