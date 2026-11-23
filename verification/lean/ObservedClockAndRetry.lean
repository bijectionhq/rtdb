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

structure RetainedPayload where
  coordinate : Nat
  expiresAt : Int
  value : Nat

def retained (clock : Int) (row : RetainedPayload) : Bool :=
  decide (clock < row.expiresAt)

def retire (clock : Int) (rows : List RetainedPayload) : List RetainedPayload :=
  rows.filter (retained clock)

theorem retirement_selects_exactly_open_payloads
    (clock : Int) (rows : List RetainedPayload) (row : RetainedPayload) :
    row ∈ retire clock rows ↔ row ∈ rows ∧ clock < row.expiresAt := by
  simp [retire, retained]

theorem installed_retirement_survives_clock_regression
    (clock sample : Int) (rows : List RetainedPayload) (row : RetainedPayload)
    (expired : row.expiresAt ≤ clock) :
    row ∉ retire (max clock sample) rows := by
  rw [retirement_selects_exactly_open_payloads]
  omega

inductive OutcomeEvent where
  | put (attempt : Nat)
  | erase (attempt : Nat)
  deriving DecidableEq

def matchesAttempt (current : Nat) : OutcomeEvent → Bool
  | .put id => id == current
  | .erase id => id != current

def acceptsOutcome (versioned : Bool) (current : Nat) (events : List OutcomeEvent) : Bool :=
  (versioned || events.length == 1) &&
    events.count (.put current) == 1 &&
    events.all (matchesAttempt current)

theorem legacy_accepts_only_the_single_current_outcome
    (current : Nat) (events : List OutcomeEvent)
    (accepted : acceptsOutcome false current events = true) :
    events = [.put current] := by
  cases events with
  | nil => simp [acceptsOutcome] at accepted
  | cons event rest =>
    cases rest with
    | nil => cases event <;> simp_all [acceptsOutcome, matchesAttempt]
    | cons _ _ => simp [acceptsOutcome] at accepted

theorem accepted_outcome_cannot_erase_its_current_attempt
    (versioned : Bool) (current : Nat) (events : List OutcomeEvent)
    (accepted : acceptsOutcome versioned current events = true)
    (erased : .erase current ∈ events) : False := by
  simp only [acceptsOutcome, Bool.and_eq_true] at accepted
  have admitted := List.all_eq_true.mp accepted.2 (.erase current) erased
  simp [matchesAttempt] at admitted

#print axioms ordered_projection_preserves_the_unsmoothed_observation
#print axioms observing_expiry_is_irreversible
#print axioms the_first_terminal_outcome_survives_all_other_attempts
#print axioms same_body_redeems_the_original_terminal_outcome
#print axioms retirement_selects_exactly_open_payloads
#print axioms installed_retirement_survives_clock_regression
#print axioms legacy_accepts_only_the_single_current_outcome
#print axioms accepted_outcome_cannot_erase_its_current_attempt

end RTDB.ObservedClockAndRetry
