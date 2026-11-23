import Std

namespace RTDB.CheckpointSourceHandoff

structure State (Root Publication Owner : Type) where
  current : Root
  present : Publication → Prop
  authorized : Publication → Prop
  holds : Owner → Option Root
  reading : Owner → Prop

variable {Root Publication Owner : Type}
variable [DecidableEq Owner]
variable {needs : Root → Publication → Prop}

def Covered (needs : Root → Publication → Prop) (state : State Root Publication Owner) (root : Root) : Prop :=
  ∀ publication, needs root publication → state.present publication ∧ ¬ state.authorized publication

def Unheld (needs : Root → Publication → Prop) (state : State Root Publication Owner)
    (publication : Publication) : Prop :=
  ∀ owner root, state.holds owner = some root → ¬ needs root publication

def Safe (needs : Root → Publication → Prop) (state : State Root Publication Owner) : Prop :=
  Covered needs state state.current ∧
  (∀ owner root, state.holds owner = some root → Covered needs state root) ∧
  ∀ owner, state.reading owner → ∃ root, state.holds owner = some root

def setHold (state : State Root Publication Owner) (owner : Owner) (root : Option Root) :
    State Root Publication Owner :=
  { state with holds := fun other => if other = owner then root else state.holds other }

inductive Step (needs : Root → Publication → Prop) :
    State Root Publication Owner → State Root Publication Owner → Prop where
  | adopt (state) (root : Root) :
      Covered needs state root → Step needs state { state with current := root }
  | admit (state) (owner : Owner) (root : Root) :
      state.holds owner = none →
      Covered needs state root →
      Step needs state (setHold state owner (some root))
  | beginRead (state) (owner : Owner) (root : Root) :
      state.holds owner = some root →
      Step needs state { state with reading := fun other => other = owner ∨ state.reading other }
  | endRead (state) (owner : Owner) :
      Step needs state { state with reading := fun other => other ≠ owner ∧ state.reading other }
  | release (state) (owner : Owner) :
      ¬ state.reading owner → Step needs state (setHold state owner none)
  | authorize (state) (publication : Publication) :
      ¬ needs state.current publication →
      Unheld needs state publication →
      Step needs state { state with
        authorized := fun other => other = publication ∨ state.authorized other }
  | remove (state) (publication : Publication) :
      state.authorized publication →
      Step needs state { state with present := fun other => state.present other ∧ other ≠ publication }

variable {state updated : State Root Publication Owner}

theorem exchanges_preserve_exact_dependencies (transition : Step needs state updated)
    (safe : Safe needs state) : Safe needs updated := by
  obtain ⟨current, holds, reading⟩ := safe
  cases transition with
  | adopt _ covered => exact ⟨covered, holds, reading⟩
  | admit owner root free covered =>
    refine ⟨current, ?_, ?_⟩
    · intro other used held
      by_cases same : other = owner
      · subst other
        have theRoot : root = used := by simpa [setHold] using held
        exact theRoot ▸ covered
      · exact holds other used (by simpa [setHold, same] using held)
    · intro other active
      by_cases same : other = owner
      · exact ⟨root, by simp [setHold, same]⟩
      · obtain ⟨used, held⟩ := reading other active
        exact ⟨used, by simpa [setHold, same] using held⟩
  | beginRead owner root held =>
    refine ⟨current, holds, ?_⟩
    intro other h
    rcases h with same | already
    · exact ⟨root, same ▸ held⟩
    · exact reading other already
  | endRead owner =>
    exact ⟨current, holds, fun other active => reading other active.2⟩
  | release owner drained =>
    refine ⟨current, ?_, ?_⟩
    · intro other used held
      by_cases same : other = owner
      · simp [setHold, same] at held
      · exact holds other used (by simpa [setHold, same] using held)
    · intro other active
      have different : other ≠ owner := by
        intro same
        exact drained (same ▸ active)
      obtain ⟨used, held⟩ := reading other active
      exact ⟨used, by simpa [setHold, different] using held⟩
  | authorize publication outsideCurrent outsideHolds =>
    have keep (root : Root) (cover : Covered needs state root) (unused : ¬ needs root publication)
        (other : Publication) (required : needs root other) :
        state.present other ∧ ¬ (other = publication ∨ state.authorized other) := by
      obtain ⟨present, unauthorized⟩ := cover other required
      exact ⟨present, fun allowed => allowed.elim (fun same => unused (same ▸ required)) unauthorized⟩
    exact ⟨keep state.current current outsideCurrent,
      fun owner root held => keep root (holds owner root held) (outsideHolds owner root held), reading⟩
  | remove publication allowed =>
    have keep (root : Root) (cover : Covered needs state root)
        (other : Publication) (required : needs root other) :
        (state.present other ∧ other ≠ publication) ∧ ¬ state.authorized other := by
      obtain ⟨present, unauthorized⟩ := cover other required
      exact ⟨⟨present, fun same => unauthorized (same.symm ▸ allowed)⟩, unauthorized⟩
    exact ⟨keep state.current current, fun owner root held => keep root (holds owner root held), reading⟩

inductive Schedule (needs : Root → Publication → Prop) :
    State Root Publication Owner → State Root Publication Owner → Prop where
  | start (initial : State Root Publication Owner) : Schedule needs initial initial
  | next {initial middle final : State Root Publication Owner} :
      Schedule needs initial middle → Step needs middle final → Schedule needs initial final

theorem finite_schedules_preserve_exact_dependencies
    (history : Schedule needs state updated) (safe : Safe needs state) : Safe needs updated := by
  induction history with
  | start => exact safe
  | next _ step ih => exact exchanges_preserve_exact_dependencies step ih

theorem an_outstanding_read_keeps_its_source
    (history : Schedule needs state updated) (safe : Safe needs state) (owner : Owner)
    (inFlight : updated.reading owner) :
    ∃ root, updated.holds owner = some root ∧ Covered needs updated root := by
  have result := finite_schedules_preserve_exact_dependencies history safe
  obtain ⟨root, held⟩ := result.2.2 owner inFlight
  exact ⟨root, held, result.2.1 owner root held⟩

theorem an_unrelated_publication_can_be_authorized
    (publication : Publication) (outsideCurrent : ¬ needs state.current publication)
    (outsideHolds : Unheld needs state publication) :
    ∃ next, Step needs state next ∧ next.authorized publication := by
  exact ⟨_, .authorize state publication outsideCurrent outsideHolds, Or.inl rfl⟩

#print axioms exchanges_preserve_exact_dependencies
#print axioms finite_schedules_preserve_exact_dependencies
#print axioms an_outstanding_read_keeps_its_source
#print axioms an_unrelated_publication_can_be_authorized

end RTDB.CheckpointSourceHandoff
