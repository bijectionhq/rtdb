import Std

namespace RTDB.ReplicaTrimAdmission

structure State (Reader : Type) where
  checkpoint : Nat
  floor : Nat
  authorized : Nat
  held : Reader → Option Nat

variable {Reader : Type} [DecidableEq Reader]

def initial : State Reader := ⟨0, 0, 0, fun _ => none⟩

def setHold (state : State Reader) (reader : Reader) (first : Option Nat) : State Reader :=
  { state with held := fun current => if current = reader then first else state.held current }

def mayAcquire (state : State Reader) (first : Nat) : Prop :=
  state.authorized ≤ first

def mayAuthorize (state : State Reader) (first : Nat) : Prop :=
  ∀ reader lowest, state.held reader = some lowest → first ≤ lowest

def Safe (state : State Reader) : Prop :=
  state.floor ≤ state.authorized ∧ state.authorized ≤ state.checkpoint ∧
  ∀ reader lowest, state.held reader = some lowest → state.authorized ≤ lowest

inductive Step : State Reader → State Reader → Prop where
  | publish (state : State Reader) (next : Nat) :
      state.checkpoint ≤ next → Step state { state with checkpoint := next }
  | acquire (state : State Reader) (reader : Reader) (first : Nat) :
      state.held reader = none → first ≤ state.checkpoint → mayAcquire state first →
      Step state (setHold state reader (some first))
  | release (state : State Reader) (reader : Reader) :
      Step state (setHold state reader none)
  | authorize (state : State Reader) (first : Nat) :
      state.authorized ≤ first → first ≤ state.checkpoint → mayAuthorize state first →
      Step state { state with authorized := first }
  | complete (state : State Reader) (first : Nat) :
      first ≤ state.authorized → Step state { state with floor := max state.floor first }

inductive Reachable : State Reader → Prop where
  | start : Reachable initial
  | next {state updated : State Reader} :
      Reachable state → Step state updated → Reachable updated

variable {state updated : State Reader}

theorem step_preserves_admission (transition : Step state updated) (safe : Safe state) :
    Safe updated := by
  rcases safe with ⟨floor, checkpoint, holds⟩
  cases transition with
  | publish next forward =>
    exact ⟨floor, Nat.le_trans checkpoint forward, holds⟩
  | acquire reader first _ _ admission =>
    refine ⟨floor, checkpoint, ?_⟩
    intro current lowest held
    by_cases same : current = reader
    · subst current
      have isFirst : first = lowest := by simpa [setHold] using held
      subst lowest
      exact admission
    · exact holds current lowest (by simpa [setHold, same] using held)
  | release reader =>
    refine ⟨floor, checkpoint, ?_⟩
    intro current lowest held
    by_cases same : current = reader
    · simp [setHold, same] at held
    · exact holds current lowest (by simpa [setHold, same] using held)
  | authorize first forward covered permitted =>
    exact ⟨Nat.le_trans floor forward, covered, permitted⟩
  | complete first limit =>
    exact ⟨Nat.max_le.mpr ⟨floor, limit⟩, checkpoint, holds⟩

theorem reachable_admission_is_safe (reach : Reachable state) : Safe state := by
  induction reach with
  | start => simp [Safe, initial]
  | next _ transition safe => exact step_preserves_admission transition safe

theorem active_recovery_survives_physical_trim (reach : Reachable state)
    (held : state.held reader = some first) : state.floor ≤ first := by
  have safe := reachable_admission_is_safe reach
  exact Nat.le_trans safe.1 (safe.2.2 reader first held)

#print axioms step_preserves_admission
#print axioms reachable_admission_is_safe
#print axioms active_recovery_survives_physical_trim

end RTDB.ReplicaTrimAdmission
