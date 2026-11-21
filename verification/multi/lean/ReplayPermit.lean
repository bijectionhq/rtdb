import Std

namespace RTDB.Multi.ReplayPermit

structure State where
  markerExpiry : Option Nat
  mutations : Nat

def submit (cutoff observed expiresAt : Nat) (state : State) : State :=
  if observed < cutoff then
    match state.markerExpiry with
    | some expiry => if observed < expiry then state else ⟨some (max cutoff expiresAt), state.mutations + 1⟩
    | none => ⟨some (max cutoff expiresAt), state.mutations + 1⟩
  else state

def Safe (cutoff : Nat) (state : State) : Prop :=
  match state.markerExpiry with
  | none => state.mutations = 0
  | some expiry => cutoff ≤ expiry ∧ state.mutations = 1

theorem submission_preserves_safety (cutoff observed expiresAt : Nat) (state : State)
    (safe : Safe cutoff state) : Safe cutoff (submit cutoff observed expiresAt state) := by
  by_cases admitted : observed < cutoff
  · cases marker : state.markerExpiry with
    | none =>
      have empty : state.mutations = 0 := by simpa [Safe, marker] using safe
      simp [submit, admitted, marker, Safe, empty]
      exact Nat.le_max_left _ _
    | some expiry =>
      have covers : cutoff ≤ expiry ∧ state.mutations = 1 := by simpa [Safe, marker] using safe
      have active : observed < expiry := Nat.lt_of_lt_of_le admitted covers.1
      simpa [submit, admitted, marker, active]
  · simpa [submit, admitted]

def attempt (cutoff : Nat) (state : State) (clock : Nat × Nat) : State :=
  submit cutoff clock.1 clock.2 state

theorem arbitrary_writer_clocks_preserve_safety (cutoff : Nat) (state : State)
    (safe : Safe cutoff state) (clocks : List (Nat × Nat)) :
    Safe cutoff (clocks.foldl (attempt cutoff) state) := by
  induction clocks generalizing state with
  | nil => exact safe
  | cons clock clocks ih =>
    exact ih _ (submission_preserves_safety cutoff clock.1 clock.2 state safe)

theorem at_most_one_mutation (cutoff : Nat) (clocks : List (Nat × Nat)) :
    (clocks.foldl (attempt cutoff) ⟨none, 0⟩).mutations ≤ 1 := by
  let final := clocks.foldl (attempt cutoff) ⟨none, 0⟩
  change final.mutations ≤ 1
  have safe := arbitrary_writer_clocks_preserve_safety cutoff ⟨none, 0⟩ rfl clocks
  change Safe cutoff final at safe
  cases marker : final.markerExpiry <;> simp_all [Safe]

#print axioms arbitrary_writer_clocks_preserve_safety
#print axioms at_most_one_mutation

inductive Resolver
  | guarded (observed expiresAt : Nat)
  | legacy

def resolve (cutoff : Nat) (state : State) (resolver : Resolver) : State :=
  match resolver with
  | .guarded observed expiresAt => submit cutoff observed expiresAt state
  | .legacy => state

theorem mixed_resolvers_preserve_safety (cutoff : Nat) (state : State)
    (safe : Safe cutoff state) (resolvers : List Resolver) :
    Safe cutoff (resolvers.foldl (resolve cutoff) state) := by
  induction resolvers generalizing state with
  | nil => exact safe
  | cons resolver resolvers ih =>
    apply ih
    cases resolver with
    | guarded observed expiresAt => exact submission_preserves_safety cutoff observed expiresAt state safe
    | legacy => exact safe

#print axioms mixed_resolvers_preserve_safety

end RTDB.Multi.ReplayPermit
