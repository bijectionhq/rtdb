import Std

namespace RTDB.ReaderReclamationMinimality

structure Run (Observation : Type) where
  observations : Nat → List Observation
  active : Nat → Prop

def Safe {Observation : Type} (admissible : Run Observation → Prop)
    (reap : List Observation → Bool) : Prop :=
  ∀ run, admissible run → ∀ time, reap (run.observations time) = true → ¬ run.active time

def EventuallyReaps {Observation : Type} (run : Run Observation)
    (reap : List Observation → Bool) : Prop :=
  ∃ time, reap (run.observations time) = true

theorem timing_only_cannot_guarantee_safety_and_progress {Observation : Type}
    (admissible : Run Observation → Prop) (reap : List Observation → Bool)
    (abandoned : Run Observation)
    (unbounded : ∀ time, ∃ stillReading, admissible stillReading ∧
      stillReading.observations time = abandoned.observations time ∧ stillReading.active time)
    (safe : Safe admissible reap) : ¬ EventuallyReaps abandoned reap := by
  rintro ⟨time, deleted⟩
  obtain ⟨stillReading, possible, samePrefix, active⟩ := unbounded time
  exact safe stillReading possible time (samePrefix.symm ▸ deleted) active

def Timed (limit : Nat) (run : Run Nat) : Prop :=
  ∀ time, run.observations time = [time] ∧ (limit ≤ time → ¬ run.active time)

def reapAfter (limit : Nat) (observations : List Nat) : Bool :=
  match observations with
  | [time] => decide (limit ≤ time)
  | _ => false

theorem a_valid_bound_makes_timed_reclamation_safe (limit : Nat) :
    Safe (Timed limit) (reapAfter limit) := by
  intro run valid time deleted
  obtain ⟨observed, finished⟩ := valid time
  rw [observed] at deleted
  exact finished (by simpa [reapAfter] using deleted)

theorem a_valid_bound_allows_a_finite_reclamation (limit : Nat) (run : Run Nat)
    (valid : Timed limit run) : EventuallyReaps run (reapAfter limit) := by
  refine ⟨limit, ?_⟩
  rw [(valid limit).1]
  simp [reapAfter]

structure DrainObservation where
  drained : Bool

def DrainSound (run : Run DrainObservation) : Prop :=
  ∀ time, run.observations time = [⟨true⟩] → ¬ run.active time

def reapOnDrain (observations : List DrainObservation) : Bool :=
  match observations with
  | [result] => result.drained
  | _ => false

theorem confirmed_drain_makes_reclamation_safe : Safe DrainSound reapOnDrain := by
  intro run sound time deleted
  have observed : run.observations time = [⟨true⟩] := by
    cases h : run.observations time with
    | nil => simp [h, reapOnDrain] at deleted
    | cons result rest =>
      cases rest with
      | nil => cases result <;> simpa [h, reapOnDrain] using deleted
      | cons _ _ => simp [h, reapOnDrain] at deleted
  exact sound time observed

theorem confirmed_drain_allows_a_finite_reclamation (run : Run DrainObservation) (time : Nat)
    (confirmed : run.observations time = [⟨true⟩]) : EventuallyReaps run reapOnDrain := by
  exact ⟨time, by simp [confirmed, reapOnDrain]⟩

#print axioms timing_only_cannot_guarantee_safety_and_progress
#print axioms a_valid_bound_makes_timed_reclamation_safe
#print axioms a_valid_bound_allows_a_finite_reclamation
#print axioms confirmed_drain_makes_reclamation_safe
#print axioms confirmed_drain_allows_a_finite_reclamation

end RTDB.ReaderReclamationMinimality
