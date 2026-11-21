import Std

namespace RTDB.Temporal

inductive Mutation (Value : Type) where
  | put : Value → Mutation Value
  | delete : Mutation Value
  | erase : Mutation Value

structure Event (Value : Type) where
  iid : Nat
  validFrom : Int
  validTo : Int
  systemFrom : Int
  mutation : Mutation Value

structure Point where
  iid : Nat
  validTime : Int
  systemTime : Int

def decision (point : Point) (event : Event Value) : Option (Option Value) :=
  if event.iid ≠ point.iid then none
  else
    match event.mutation with
    | .erase => some none
    | .put value =>
      if event.validFrom ≤ point.validTime ∧ point.validTime < event.validTo ∧
          event.systemFrom ≤ point.systemTime
      then some (some value)
      else none
    | .delete =>
      if event.validFrom ≤ point.validTime ∧ point.validTime < event.validTo ∧
          event.systemFrom ≤ point.systemTime
      then some none
      else none

def applyEvent (point : Point) (value : Option Value) (event : Event Value) : Option Value :=
  (decision point event).getD value

def newest (point : Point) (initial : Option Value) : List (Event Value) → Option Value
  | [] => initial
  | event :: older =>
    match decision point event with
    | none => newest point initial older
    | some value => value

theorem newest_append (point : Point) (initial : Option Value)
    (recent older : List (Event Value)) :
    newest point initial (recent ++ older) = newest point (newest point initial older) recent := by
  induction recent with
  | nil => rfl
  | cons event recent inductionHypothesis =>
    simp only [List.cons_append, newest]
    cases decision point event <;> simp [inductionHypothesis]

theorem newest_reverse_eq_replay (point : Point) (history : List (Event Value))
    (initial : Option Value) :
    newest point initial history.reverse = history.foldl (applyEvent point) initial := by
  induction history generalizing initial with
  | nil => rfl
  | cons event older inductionHypothesis =>
    rw [List.reverse_cons, newest_append]
    cases result : decision point event <;>
      simpa [newest, applyEvent, Option.getD, result] using
        (inductionHypothesis (newest point initial [event]))

theorem newest_filter_eq (point : Point) (initial : Option Value)
    (history : List (Event Value)) (keep : Event Value → Bool)
    (sound : ∀ event ∈ history, keep event = false → decision point event = none) :
    newest point initial (history.filter keep) = newest point initial history := by
  induction history with
  | nil => rfl
  | cons event older inductionHypothesis =>
    have olderSound : ∀ entry ∈ older, keep entry = false → decision point entry = none :=
      fun entry present omitted => sound entry (List.mem_cons_of_mem _ present) omitted
    cases selected : keep event with
    | false =>
      have irrelevant := sound event (List.mem_cons_self ..) selected
      simp [selected, newest, irrelevant, inductionHypothesis olderSound]
    | true =>
      simp [selected, newest, inductionHypothesis olderSound]

theorem newest_for_iid (point : Point) (initial : Option Value) (history : List (Event Value)) :
    newest point initial (history.filter fun event => decide (event.iid = point.iid)) =
      newest point initial history := by
  apply newest_filter_eq
  intro event _ omitted
  have different : event.iid ≠ point.iid := by simpa using omitted
  simp [decision, different]

theorem erase_in_captured_history (point : Point) (initial : Option Value)
    (event : Event Value) (older : List (Event Value))
    (entity : event.iid = point.iid) (op : event.mutation = .erase) :
    newest point initial (event :: older) = none := by
  simp [newest, decision, entity, op]

theorem exclusive_valid_end (point : Point) (event : Event Value)
    (outside : event.validTo ≤ point.validTime) (op : event.mutation ≠ .erase) :
    decision point event = none := by
  have upper : ¬ point.validTime < event.validTo := by omega
  cases mutation : event.mutation with
  | erase => exact (op mutation).elim
  | put value => simp [decision, mutation, upper]
  | delete => simp [decision, mutation, upper]

theorem future_mutation (point : Point) (event : Event Value)
    (future : point.systemTime < event.systemFrom) (op : event.mutation ≠ .erase) :
    decision point event = none := by
  have lower : ¬ event.systemFrom ≤ point.systemTime := by omega
  cases mutation : event.mutation with
  | erase => exact (op mutation).elim
  | put value => simp [decision, mutation, lower]
  | delete => simp [decision, mutation, lower]

theorem ordinary_mutation_outside_valid_range (point : Point) (initial : Option Value)
    (event : Event Value) (older : List (Event Value)) (op : event.mutation ≠ .erase)
    (outside : point.validTime < event.validFrom ∨ event.validTo ≤ point.validTime) :
    newest point initial (event :: older) = newest point initial older := by
  rcases outside with below | above
  · have lower : ¬ event.validFrom ≤ point.validTime := by omega
    cases mutation : event.mutation with
    | erase => exact (op mutation).elim
    | put value => simp [newest, decision, mutation, lower]
    | delete => simp [newest, decision, mutation, lower]
  · simp [newest, exclusive_valid_end point event above op]

theorem delete_at_covered_point (point : Point) (initial : Option Value)
    (event : Event Value) (older : List (Event Value)) (entity : event.iid = point.iid)
    (op : event.mutation = .delete) (withinStart : event.validFrom ≤ point.validTime)
    (beforeEnd : point.validTime < event.validTo) (committed : event.systemFrom ≤ point.systemTime) :
    newest point initial (event :: older) = none := by
  simp [newest, decision, entity, op, withinStart, beforeEnd, committed]

theorem later_events_do_not_reveal_erased_history (point : Point) (initial : Option Value)
    (event : Event Value) (recent older : List (Event Value))
    (entity : event.iid = point.iid) (op : event.mutation = .erase) :
    newest point initial (recent ++ event :: older) = newest point none recent := by
  rw [newest_append, erase_in_captured_history point initial event older entity op]

theorem newest_constant_between_event_boundaries (history : List (Event Value))
    (corner sample : Point) (initial : Option Value) (entity : corner.iid = sample.iid)
    (validOrder : corner.validTime ≤ sample.validTime)
    (systemOrder : corner.systemTime ≤ sample.systemTime)
    (noCrossing : ∀ event ∈ history,
      ¬ (corner.validTime < event.validFrom ∧ event.validFrom ≤ sample.validTime) ∧
      ¬ (corner.validTime < event.validTo ∧ event.validTo ≤ sample.validTime) ∧
      ¬ (corner.systemTime < event.systemFrom ∧ event.systemFrom ≤ sample.systemTime)) :
    newest corner initial history = newest sample initial history := by
  induction history with
  | nil => rfl
  | cons event older ih =>
    obtain ⟨noStart, noEnd, noCommit⟩ := noCrossing event (List.mem_cons_self ..)
    have validStart : (event.validFrom ≤ corner.validTime) ↔ (event.validFrom ≤ sample.validTime) := by omega
    have validEnd : (corner.validTime < event.validTo) ↔ (sample.validTime < event.validTo) := by omega
    have systemStart : (event.systemFrom ≤ corner.systemTime) ↔ (event.systemFrom ≤ sample.systemTime) := by omega
    have sameDecision : decision corner event = decision sample event := by
      cases op : event.mutation <;> simp [decision, op, entity, validStart, validEnd, systemStart]
    have sameOlder := ih (fun entry member => noCrossing entry (List.mem_cons_of_mem _ member))
    simp [newest, sameDecision, sameOlder]

theorem replay_segments (point : Point) (initial : Option Value)
    (older recent : List (Event Value)) :
    newest point initial (recent.reverse ++ older.reverse) =
      recent.foldl (applyEvent point) (older.foldl (applyEvent point) initial) := by
  rw [newest_append, newest_reverse_eq_replay, newest_reverse_eq_replay]

end RTDB.Temporal
