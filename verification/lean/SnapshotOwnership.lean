import Std

namespace RTDB.SnapshotOwnership

structure State where
  published : Bool
  readers : Nat
  references : Nat
  released : Bool

def publisherReferences (s : State) : Nat := if s.published then 1 else 0

def Valid (s : State) : Prop :=
  s.references = publisherReferences s + s.readers ∧ (s.released = true ↔ s.references = 0)

def publishedState : State := ⟨true, 0, 1, false⟩

def transactionState : State := ⟨false, 1, 1, false⟩

def openReader (s : State) : State :=
  {s with readers := s.readers + 1, references := s.references + 1}

def releaseReader (s : State) : State :=
  { s with
    readers := s.readers - 1
    references := s.references - 1
    released := s.released || s.references == 1 }

def retire (s : State) : State :=
  { s with
    published := false
    references := s.references - 1
    released := s.released || s.references == 1 }

inductive Step : State → State → Prop where
  | acquire {s : State} : s.published = true → Step s (openReader s)
  | finish {s : State} : s.readers > 0 → Step s (releaseReader s)
  | retire {s : State} : s.published = true → Step s (retire s)

theorem published_state_valid : Valid publishedState := by
  simp [Valid, publishedState, publisherReferences]

theorem transaction_state_valid : Valid transactionState := by
  simp [Valid, transactionState, publisherReferences]

theorem any_step_preserves_reference_ownership {before after : State}
    (valid : Valid before) (step : Step before after) : Valid after := by
  cases step with
  | acquire published =>
    cases h : before.released <;>
      simp_all [Valid, publisherReferences, openReader] <;> omega
  | finish present =>
    cases h : before.released <;> cases pub : before.published <;>
      simp_all [Valid, publisherReferences, releaseReader] <;> omega
  | retire published =>
    cases h : before.released <;>
      simp_all [Valid, publisherReferences, retire] <;> omega

inductive History : State → State → Prop where
  | unchanged {s : State} : History s s
  | next {before middle after : State} : History before middle → Step middle after → History before after

theorem reachable_state_valid {before after : State} (valid : Valid before)
    (steps : History before after) : Valid after := by
  induction steps with
  | unchanged => exact valid
  | next _ step prior => exact any_step_preserves_reference_ownership prior step

theorem retained_readers_prevent_final_release {before after : State} (valid : Valid before)
    (steps : History before after) (readers : after.readers > 0) : after.released = false := by
  obtain ⟨references, released⟩ := reachable_state_valid valid steps
  cases h : after.released <;> simp_all

theorem published_owner_keeps_a_live_reference (s : State) (valid : Valid s)
    (published : s.published = true) : s.references > 0 := by
  have total : s.references = 1 + s.readers := by
    simpa [publisherReferences, published] using valid.1
  omega

theorem released_resources_admit_no_further_operations {before after : State}
    (valid : Valid before) (released : before.released = true) (step : Step before after) : False := by
  have zero := valid.2.mp released
  have total := valid.1
  cases step <;> simp_all [publisherReferences] <;> omega

theorem after_retirement_last_reader_releases_resources (s : State)
    (valid : Valid s) (retired : s.published = false) (last : s.readers = 1) :
    (releaseReader s).released = true := by
  have total : s.references = 1 := by simpa [publisherReferences, retired, last] using valid.1
  simp [releaseReader, total]

#print axioms published_state_valid
#print axioms transaction_state_valid
#print axioms any_step_preserves_reference_ownership
#print axioms retained_readers_prevent_final_release
#print axioms published_owner_keeps_a_live_reference
#print axioms released_resources_admit_no_further_operations
#print axioms after_retirement_last_reader_releases_resources

end RTDB.SnapshotOwnership
