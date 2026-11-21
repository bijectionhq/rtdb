import Std

namespace RTDB.GenerationRetention

abbrev Generation := Nat
abbrev Object := Nat
abbrev Dependencies := Generation → Object → Prop

structure State where
  current : Generation
  readers : List Generation
  inflight : List Generation
  present : Object → Prop
  pendingDeletion : Object → Prop

def entitled (state : State) (generation : Generation) : Prop :=
  generation = state.current ∨ generation ∈ state.readers ∨ generation ∈ state.inflight

def safe (dependencies : Dependencies) (state : State) : Prop :=
  ∀ generation object, entitled state generation → dependencies generation object →
    state.present object ∧ ¬ state.pendingDeletion object

def closeReader (state : State) (generation : Generation) : State :=
  { state with readers := state.readers.erase generation }

def finishRead (state : State) (generation : Generation) : State :=
  { state with inflight := state.inflight.erase generation }

inductive Step (dependencies : Dependencies) : State → State → Prop where
  | publish (state : State) (generation : Generation) :
      state.current < generation →
      (∀ object, dependencies generation object → state.present object ∧ ¬ state.pendingDeletion object) →
      Step dependencies state { state with current := generation }
  | acquire (state : State) :
      Step dependencies state { state with readers := state.current :: state.readers }
  | start (state : State) (generation : Generation) :
      generation ∈ state.readers →
      Step dependencies state { state with inflight := generation :: state.inflight }
  | close (state : State) (generation : Generation) :
      generation ∈ state.readers →
      Step dependencies state (closeReader state generation)
  | finish (state : State) (generation : Generation) :
      generation ∈ state.inflight →
      Step dependencies state (finishRead state generation)
  | stage (state : State) (object : Object) :
      (¬ state.pendingDeletion object) →
      Step dependencies state { state with present := fun other => other = object ∨ state.present other }
  | authorize (state : State) (object : Object) :
      state.present object →
      (¬ state.pendingDeletion object) →
      (∀ generation, entitled state generation → ¬ dependencies generation object) →
      Step dependencies state { state with pendingDeletion := fun other => other = object ∨ state.pendingDeletion other }
  | deleted (state : State) (object : Object) :
      state.pendingDeletion object →
      Step dependencies state { state with
        present := fun other => state.present other ∧ other ≠ object
        pendingDeletion := fun other => state.pendingDeletion other ∧ other ≠ object }

theorem step_preserves_entitlement (transition : Step dependencies before after)
    (prior : safe dependencies before) : safe dependencies after := by
  intro generation object visible required
  cases transition with
  | publish new _ ready =>
    rcases visible with atHead | held | read
    · subst generation
      exact ready object required
    · exact prior generation object (Or.inr (Or.inl held)) required
    · exact prior generation object (Or.inr (Or.inr read)) required
  | acquire =>
    rcases visible with atHead | held | read
    · exact prior generation object (Or.inl atHead) required
    · rcases List.mem_cons.mp held with justOpened | earlier
      · exact prior generation object (Or.inl justOpened) required
      · exact prior generation object (Or.inr (Or.inl earlier)) required
    · exact prior generation object (Or.inr (Or.inr read)) required
  | start opened admitted =>
    rcases visible with atHead | held | read
    · exact prior generation object (Or.inl atHead) required
    · exact prior generation object (Or.inr (Or.inl held)) required
    · rcases List.mem_cons.mp read with justStarted | earlier
      · subst generation
        exact prior opened object (Or.inr (Or.inl admitted)) required
      · exact prior generation object (Or.inr (Or.inr earlier)) required
  | close closed _ =>
    rcases visible with atHead | held | read
    · exact prior generation object (Or.inl atHead) required
    · exact prior generation object (Or.inr (Or.inl (List.mem_of_mem_erase held))) required
    · have retained : generation ∈ before.inflight := by
        first
        | exact read
        | simp [closeReader] at read
      exact prior generation object (Or.inr (Or.inr retained)) required
  | finish completed _ =>
    rcases visible with atHead | held | read
    · exact prior generation object (Or.inl atHead) required
    · exact prior generation object (Or.inr (Or.inl held)) required
    · have retained : generation ∈ before.inflight := by
        first
        | exact List.mem_of_mem_erase read
        | exact (List.mem_filter.mp read).1
      exact prior generation object (Or.inr (Or.inr retained)) required
  | stage stocked _ =>
    obtain ⟨present, available⟩ := prior generation object visible required
    exact ⟨Or.inr present, available⟩
  | authorize collected _ _ unreferenced =>
    obtain ⟨present, available⟩ := prior generation object visible required
    refine ⟨present, ?_⟩
    rintro (candidate | pending)
    · subst object
      exact unreferenced generation visible required
    · exact available pending
  | deleted collected alreadyPending =>
    obtain ⟨present, available⟩ := prior generation object visible required
    have distinct : object ≠ collected := by
      rintro rfl
      exact available alreadyPending
    exact ⟨⟨present, distinct⟩, fun closed => available closed.1⟩

inductive Reachable (dependencies : Dependencies) (initial : State) : State → Prop where
  | start : Reachable dependencies initial initial
  | extend : Reachable dependencies initial before → Step dependencies before after → Reachable dependencies initial after

theorem every_reachable_state_retains_reader_dependencies
    (history : Reachable dependencies initial after) (init : safe dependencies initial) :
    safe dependencies after := by
  induction history with
  | start => exact init
  | extend _ transition prior => exact step_preserves_entitlement transition prior

theorem closing_a_reader_preserves_its_inflight_operations (state : State) (generation : Generation) :
    (closeReader state generation).readers = state.readers.erase generation ∧
      (closeReader state generation).inflight = state.inflight := by
  exact ⟨rfl, rfl⟩

theorem completing_one_read_preserves_all_other_references (state : State) (generation : Generation)
    (started : generation ∈ state.inflight) :
    (finishRead state generation).inflight.count generation + 1 = state.inflight.count generation ∧
      (∀ other, other ≠ generation →
        (finishRead state generation).inflight.count other = state.inflight.count other) := by
  constructor
  · have positive := List.count_pos_iff.mpr started
    simp [finishRead]
    omega
  · intro other distinct
    exact List.count_erase_of_ne distinct

#print axioms step_preserves_entitlement
#print axioms every_reachable_state_retains_reader_dependencies
#print axioms closing_a_reader_preserves_its_inflight_operations
#print axioms completing_one_read_preserves_all_other_references

theorem an_unreferenced_object_can_be_collected (state : State) (object : Object)
    (present : state.present object) (available : ¬ state.pendingDeletion object)
    (outside : ∀ generation, entitled state generation → ¬ dependencies generation object) :
    ∃ pending final, Step dependencies state pending ∧ Step dependencies pending final ∧ ¬ final.present object := by
  refine ⟨_, _, Step.authorize state object present available outside, Step.deleted _ object (Or.inl rfl), ?_⟩
  exact fun retained => retained.2 rfl

#print axioms an_unreferenced_object_can_be_collected

end RTDB.GenerationRetention
