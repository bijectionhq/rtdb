import Std

namespace RTDB

structure Span where
  lower : Int
  upper : Int
  deriving DecidableEq, Repr

def Span.contains (span : Span) (valid : Int) : Bool :=
  decide (span.lower ≤ valid ∧ valid < span.upper)

inductive Fact where
  | put (key value : Nat) (valid : Span)
  | delete (key : Nat) (valid : Span)
  | erase (key : Nat)
  deriving DecidableEq, Repr

structure Event where
  commit : Nat
  fact : Fact
  deriving DecidableEq, Repr

abbrev Batch := List Fact
abbrev History := List Batch

def atCommit (commit : Nat) (batch : Batch) : List Event :=
  batch.reverse.map (fun fact => ⟨commit, fact⟩)

-- The newest commit and the last command in each batch have priority.
def events : History → List Event
  | [] => []
  | batch :: older => atCommit (older.length + 1) batch ++ events older

def admit (history : History) (horizon : Nat) : List Event :=
  (events history).filter (fun event => decide (event.commit ≤ horizon))

def affected (key : Nat) (valid : Int) : Fact → Bool
  | .put other _ span => key == other && span.contains valid
  | .delete other span => key == other && span.contains valid
  | .erase _ => false

def valueOf : Fact → Option Nat
  | .put _ value _ => some value
  | _ => none

def redact (key : Nat) : List Event → List Event
  | [] => []
  | event :: tail =>
      match event.fact with
      | .erase other => if key = other then [] else redact key tail
      | _ => event :: redact key tail

def point (view : List Event) (system key : Nat) (valid : Int) : Option Nat :=
  ((redact key view).find? (fun event =>
    decide (event.commit ≤ system) && affected key valid event.fact)).bind
    (fun event => valueOf event.fact)

-- A single pass over the ordered view gives an alternate implementation.
def scan (system key : Nat) (valid : Int) : List Event → Option Nat
  | [] => none
  | event :: tail =>
      match event.fact with
      | .erase other => if key = other then none else scan system key valid tail
      | .put other value span =>
          if event.commit ≤ system ∧ key = other ∧ span.contains valid = true
          then some value else scan system key valid tail
      | .delete other span =>
          if event.commit ≤ system ∧ key = other ∧ span.contains valid = true
          then none else scan system key valid tail

theorem scan_refines_point (view : List Event) (system key : Nat) (valid : Int) :
    scan system key valid view = point view system key valid := by
  induction view with
  | nil => rfl
  | cons event tail ih =>
    rcases event with ⟨commit, fact⟩
    cases fact with
    | put other value span =>
      by_cases before : commit ≤ system <;> by_cases same : key = other <;>
        cases inside : span.contains valid <;>
        simp_all [scan, point, redact, affected, valueOf]
      all_goals omega
    | delete other span =>
      by_cases before : commit ≤ system <;> by_cases same : key = other <;>
        cases inside : span.contains valid <;>
        simp_all [scan, point, redact, affected, valueOf]
      all_goals omega
    | erase other =>
      by_cases same : key = other <;> simp [scan, point, redact, same, ih]

theorem erase_masks_older_events (older : List Event) (eraseAt system key : Nat)
    (valid : Int) :
    point (⟨eraseAt, .erase key⟩ :: older) system key valid = none := by
  simp [point, redact]

theorem erase_does_not_mask_other_keys (older : List Event)
    (eraseAt system key other : Nat) (valid : Int) (different : key ≠ other) :
    point (⟨eraseAt, .erase other⟩ :: older) system key valid =
      point older system key valid := by
  simp [point, redact, different]

theorem erased_history_cannot_change_fresh_answer (newer olderLeft olderRight : List Event)
    (eraseAt system key : Nat) (valid : Int) :
    point (newer ++ ⟨eraseAt, .erase key⟩ :: olderLeft) system key valid =
    point (newer ++ ⟨eraseAt, .erase key⟩ :: olderRight) system key valid := by
  simp only [← scan_refines_point]
  induction newer with
  | nil => simp [scan]
  | cons event tail ih =>
    rcases event with ⟨commit, fact⟩
    cases fact <;> simp [scan, ih]

theorem half_open (span : Span) : span.contains span.upper = false := by
  simp [Span.contains]

theorem admissible_prefix (history : History) (batch : Batch)
    (horizon : Nat) (earlier : horizon ≤ history.length) :
    admit (batch :: history) horizon = admit history horizon := by
  have tooNew : ¬history.length + 1 ≤ horizon := by omega
  simp [admit, events, atCommit, List.filter_map, tooNew]

theorem pinned_answers_survive_publication (history : History) (batch : Batch)
    (horizon system key : Nat) (valid : Int) (earlier : horizon ≤ history.length) :
    point (admit (batch :: history) horizon) system key valid =
      point (admit history horizon) system key valid := by
  rw [admissible_prefix history batch horizon earlier]

def privateView (history : History) (resolved : Batch) : List Event :=
  atCommit (history.length + 1) resolved ++ events history

theorem resolution_sees_last_tentative_put (history : History) (prior : Batch)
    (key value : Nat) (span : Span) (valid : Int)
    (inside : span.contains valid = true) :
    scan (history.length + 1) key valid (privateView history (prior ++ [.put key value span])) =
      some value := by
  simp [privateView, atCommit, List.reverse_append, scan, inside]

def supportedPoint (candidates : Nat → Bool) (view : List Event)
    (system key : Nat) (valid : Int) : Option Nat :=
  if candidates key then point view system key valid else none

theorem candidate_superset_refines_scan (candidates : Nat → Bool) (view : List Event)
    (system key : Nat) (valid : Int)
    (covers : ∀ value, point view system key valid = some value → candidates key = true) :
    supportedPoint candidates view system key valid = point view system key valid := by
  cases answer : point view system key valid with
  | none => simp [supportedPoint, answer]
  | some value => simp [supportedPoint, answer, covers value answer]

-- Ranks are abstract scores after fixing the corpus and ordering.
-- The strict frontier comparison keeps the final SQL tie-break available.
theorem cutoff_keeps_possible_ties (actual frontier kth : Int)
    (isBound : actual ≤ frontier) (canTie : kth ≤ actual) :
    ¬ frontier < kth := by omega

theorem compaction_equivalence_composes (old middle replacement : List Event)
    (first : ∀ s k v, point old s k v = point middle s k v)
    (second : ∀ s k v, point middle s k v = point replacement s k v) :
    ∀ s k v, point old s k v = point replacement s k v :=
  fun system key valid => (first system key valid).trans (second system key valid)

def Fact.key : Fact → Nat
  | .put key _ _ | .delete key _ | .erase key => key

def forKey (view : List Event) (key : Nat) : List Event :=
  view.filter (fun event => event.fact.key == key)

theorem key_partition_preserves_answer
    (view : List Event) (system key : Nat) (valid : Int) :
    scan system key valid (forKey view key) = scan system key valid view := by
  induction view with
  | nil => rfl
  | cons event tail ih =>
    simp only [forKey, Fact.key] at ih
    rcases event with ⟨commit, fact⟩
    cases fact with
    | put other value span =>
      by_cases same : other = key
      · subst other
        simp [forKey, Fact.key, scan, ih]
      · have different : key ≠ other := Ne.symm same
        simp [forKey, Fact.key, scan, ih, same, different]
    | delete other span =>
      by_cases same : other = key
      · subst other
        simp [forKey, Fact.key, scan, ih]
      · have different : key ≠ other := Ne.symm same
        simp [forKey, Fact.key, scan, ih, same, different]
    | erase other =>
      by_cases same : other = key
      · subst other
        simp [forKey, Fact.key, scan]
      · have different : key ≠ other := Ne.symm same
        simp [forKey, Fact.key, scan, ih, same, different]

def Fact.validSpan : Fact → Option Span
  | .put _ _ span | .delete _ span => some span
  | .erase _ => none

theorem contains_between_endpoints (span : Span) (left right : Int)
    (ordered : left ≤ right)
    (lower : ¬ (left < span.lower ∧ span.lower ≤ right))
    (upper : ¬ (left < span.upper ∧ span.upper ≤ right)) :
    span.contains left = span.contains right := by
  unfold Span.contains
  congr 1
  apply propext
  omega

theorem scan_constant_between_valid_endpoints (view : List Event)
    (system key : Nat) (left right : Int) (ordered : left ≤ right)
    (noEndpoint : ∀ event ∈ view, ∀ span, event.fact.validSpan = some span →
      ¬ (left < span.lower ∧ span.lower ≤ right) ∧
      ¬ (left < span.upper ∧ span.upper ≤ right)) :
    scan system key left view = scan system key right view := by
  induction view with
  | nil => rfl
  | cons event tail ih =>
    have tailAgreement := ih (fun e member => noEndpoint e (List.mem_cons_of_mem event member))
    rcases event with ⟨commit, fact⟩
    cases fact with
    | put other value span =>
      have ends := noEndpoint ⟨commit, .put other value span⟩ (by simp) span rfl
      have same := contains_between_endpoints span left right ordered ends.1 ends.2
      simp [scan, same, tailAgreement]
    | delete other span =>
      have ends := noEndpoint ⟨commit, .delete other span⟩ (by simp) span rfl
      have same := contains_between_endpoints span left right ordered ends.1 ends.2
      simp [scan, same, tailAgreement]
    | erase other => simp [scan, tailAgreement]

theorem scan_constant_between_system_commits (view : List Event)
    (left right key : Nat) (valid : Int) (ordered : left ≤ right)
    (noCommit : ∀ event ∈ view, ¬ (left < event.commit ∧ event.commit ≤ right)) :
    scan left key valid view = scan right key valid view := by
  induction view with
  | nil => rfl
  | cons event tail ih =>
    have tailAgreement := ih (fun e member => noCommit e (List.mem_cons_of_mem event member))
    have eventUnchanged : (event.commit ≤ left) ↔ (event.commit ≤ right) := by
      have missed := noCommit event (List.mem_cons_self)
      omega
    rcases event with ⟨commit, fact⟩
    cases fact <;> simp [scan, eventUnchanged, tailAgreement]

def whole : Span := ⟨0, 10⟩
def original : Batch := [.put 0 20 whole]
def correction : Batch := [.erase 0, .put 0 35 ⟨2, 4⟩]

-- The old admission, fresh historical read and fresh current read differ.
example : (point (admit [correction, original] 1) 1 0 3,
    point (admit [correction, original] 2) 1 0 3,
    point (admit [correction, original] 2) 2 0 3) =
    (some 20, none, some 35) := by decide

-- Reversing the same-commit commands changes the public result.
example : point (admit [correction.reverse, original] 2) 2 0 3 = none := by decide

#print axioms scan_refines_point
#print axioms erased_history_cannot_change_fresh_answer
#print axioms half_open
#print axioms pinned_answers_survive_publication
#print axioms resolution_sees_last_tentative_put
#print axioms candidate_superset_refines_scan
#print axioms cutoff_keeps_possible_ties
#print axioms key_partition_preserves_answer
#print axioms scan_constant_between_valid_endpoints
#print axioms scan_constant_between_system_commits

end RTDB
