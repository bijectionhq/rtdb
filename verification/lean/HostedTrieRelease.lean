import Std

namespace RTDB.HostedTrieRelease

structure State where
  remaining : List Nat
  released : List Nat
  pending : Bool
  installed : Bool
  receipt : Bool
  floor : Bool
  interrupted : Bool

def staged (files : List Nat) : State := ⟨files, [], true, false, false, false, false⟩

def mayQueue (state : State) : Prop :=
  state.pending = true ∧ state.receipt = true ∧ state.floor = true ∧ state.interrupted = false

def mayDiscardPlan (state : State) : Prop :=
  state.pending = true ∧ state.remaining = []

inductive Step : State → State → Prop
  | install (state : State) (pending : state.pending = true) :
      Step state { state with installed := true }
  | record (state : State) (installed : state.installed = true) :
      Step state { state with receipt := true }
  | raiseFloor (state : State) (receipt : state.receipt = true) :
      Step state { state with floor := true }
  | interrupt (state : State) : Step state { state with interrupted := true }
  | reopen (state : State) : Step state { state with interrupted := false }
  | queue (state : State) (file : Nat) (rest : List Nat) (next : state.remaining = file :: rest)
      (ready : mayQueue state) :
      Step state { state with remaining := rest, released := file :: state.released }
  | forget (state : State) (ready : mayDiscardPlan state) :
      Step state { state with pending := false }

inductive History (files : List Nat) : State → Prop
  | stage : History files (staged files)
  | extend (prior : History files before) (step : Step before after) : History files after

def authorized (state : State) : Prop :=
  state.released ≠ [] → state.receipt = true ∧ state.floor = true

def recoverable (state : State) : Prop :=
  state.remaining ≠ [] → state.pending = true

def accountsFor (files : List Nat) (state : State) : Prop :=
  state.released.reverse ++ state.remaining = files

theorem step_preserves_release_authority (step : Step before after)
    (prior : authorized before) : authorized after := by
  cases step <;> simp_all [authorized, mayQueue]

theorem step_retains_an_unfinished_plan (step : Step before after)
    (prior : recoverable before) : recoverable after := by
  cases step <;> simp_all [recoverable, mayQueue, mayDiscardPlan]

theorem step_accounts_for_every_file (step : Step before after)
    (prior : accountsFor files before) : accountsFor files after := by
  cases step with
  | queue file rest next ready =>
    simpa [accountsFor, List.append_assoc, ← next] using prior
  | install | record | raiseFloor | interrupt | reopen | forget => exact prior

theorem every_history_preserves_release_authority_and_plan (history : History files state) :
    authorized state ∧ recoverable state ∧ accountsFor files state := by
  induction history with
  | stage => simp [staged, authorized, recoverable, accountsFor]
  | extend prior step law =>
    exact ⟨step_preserves_release_authority step law.1,
      step_retains_an_unfinished_plan step law.2.1, step_accounts_for_every_file step law.2.2⟩

#print axioms step_preserves_release_authority
#print axioms step_retains_an_unfinished_plan
#print axioms step_accounts_for_every_file
#print axioms every_history_preserves_release_authority_and_plan

end RTDB.HostedTrieRelease
