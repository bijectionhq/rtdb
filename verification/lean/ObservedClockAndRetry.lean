import Std

namespace RTDB.ObservedClockAndRetry

structure Commit where
  position : Nat
  observed : Int
  projected : Int

def resolve (previous : Commit) (position : Nat) (sample : Int) : Commit :=
  ⟨position, sample, max sample (previous.projected + 1)⟩

theorem ordered_projection_preserves_the_unsmoothed_observation
    (previous : Commit) (position : Nat) (sample : Int)
    (later : previous.position < position) :
    previous.position < (resolve previous position sample).position ∧
    previous.projected < (resolve previous position sample).projected ∧
    (resolve previous position sample).observed = sample := by
  simp [resolve, later]
  omega

inductive Terminal where
  | aborted
  | committed
  deriving DecidableEq

structure Receipt where
  digest : Nat
  outcome : Terminal
  deriving DecidableEq

structure State where
  clock : Int
  receipt : Option Receipt

inductive Reply where
  | expired
  | conflict
  | applied (outcome : Terminal)
  | existing (outcome : Terminal)
  deriving DecidableEq

def admit (cutoff sample : Int) (body : Receipt) (state : State) : State × Reply :=
  let observed := max state.clock sample
  let withClock := { state with clock := observed }
  if cutoff ≤ observed then (withClock, .expired)
  else match state.receipt with
    | none => ({ withClock with receipt := some body }, .applied body.outcome)
    | some original =>
      if original.digest = body.digest then (withClock, .existing original.outcome)
      else (withClock, .conflict)

theorem observing_expiry_is_irreversible
    (cutoff sample : Int) (body : Receipt) (state : State)
    (expired : cutoff ≤ state.clock) :
    (admit cutoff sample body state).2 = .expired ∧
    cutoff ≤ (admit cutoff sample body state).1.clock := by
  have bound : cutoff ≤ max state.clock sample := by omega
  simp [admit, bound]

theorem the_first_terminal_outcome_survives_all_other_attempts
    (cutoff sample : Int) (body original : Receipt) (state : State)
    (recorded : state.receipt = some original) :
    (admit cutoff sample body state).1.receipt = some original := by
  unfold admit
  simp only [recorded]
  split
  · rfl
  · split <;> rfl

theorem same_body_redeems_the_original_terminal_outcome
    (cutoff sample : Int) (body original : Receipt) (state : State)
    (recorded : state.receipt = some original)
    (before : max state.clock sample < cutoff) (same : original.digest = body.digest) :
    (admit cutoff sample body state).2 = .existing original.outcome := by
  simp [admit, recorded, same, Int.not_le.mpr before]

#print axioms ordered_projection_preserves_the_unsmoothed_observation
#print axioms observing_expiry_is_irreversible
#print axioms the_first_terminal_outcome_survives_all_other_attempts
#print axioms same_body_redeems_the_original_terminal_outcome

end RTDB.ObservedClockAndRetry
