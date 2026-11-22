import Lean

namespace SharedReferenceRevision

structure State (Id : Type) where
  revision : Nat
  referrers : List Id

inductive Event (Id : Type) where
  | shared (child : Id)
  | independent
  | refused

def step {Id : Type} (state : State Id) : Event Id → State Id
  | .shared child => ⟨state.revision + 1, child :: state.referrers⟩
  | .independent => state
  | .refused => state

def run {Id : Type} (state : State Id) : List (Event Id) → State Id
  | [] => state
  | event :: events => run (step state event) events

def Current (snapshot : Nat) {Id : Type} (state : State Id) : Prop :=
  snapshot = state.revision

theorem shared_publication_advances_with_its_reference {Id : Type} (state : State Id) (child : Id) :
    (step state (.shared child)).revision = state.revision + 1 ∧
    (step state (.shared child)).referrers = child :: state.referrers := by
  exact ⟨rfl, rfl⟩

theorem independent_and_refused_publications_preserve_the_cut {Id : Type} (state : State Id) :
    step state .independent = state ∧ step state .refused = state := by
  exact ⟨rfl, rfl⟩

theorem revisions_never_go_back {Id : Type} (initial : State Id) (events : List (Event Id)) :
    initial.revision ≤ (run initial events).revision := by
  induction events generalizing initial with
  | nil => simp [run]
  | cons event rest ih =>
    have later := ih (step initial event)
    cases event <;> simp [run, step] at * <;> omega

theorem an_unchanged_cut_preserves_the_reference_inventory {Id : Type} (initial : State Id)
    (events : List (Event Id)) (current : Current initial.revision (run initial events)) :
    (run initial events).referrers = initial.referrers := by
  induction events generalizing initial with
  | nil => rfl
  | cons event rest ih =>
    cases event with
    | shared child =>
      have later := revisions_never_go_back (step initial (.shared child)) rest
      simp [run, Current] at current
      change initial.revision + 1 ≤ (run (step initial (.shared child)) rest).revision at later
      omega
    | independent => exact ih initial current
    | refused => exact ih initial current

#print axioms shared_publication_advances_with_its_reference
#print axioms independent_and_refused_publications_preserve_the_cut
#print axioms revisions_never_go_back
#print axioms an_unchanged_cut_preserves_the_reference_inventory

end SharedReferenceRevision
