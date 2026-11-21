import History

namespace RTDB.CompactionIntervals

/-- The candidate portions of an older write on either side of a newer interval. -/
def subtract (span blocker : Span) : List Span :=
  [⟨span.lower, min span.upper blocker.lower⟩, ⟨max span.lower blocker.upper, span.upper⟩].filter
    (fun piece => decide (piece.lower < piece.upper))

theorem subtraction_preserves_exactly_uncovered_points (span blocker : Span) (valid : Int) :
    (∃ piece ∈ subtract span blocker, piece.contains valid) ↔
      span.contains valid ∧ blocker.contains valid = false := by
  simp only [subtract, List.mem_filter, List.mem_cons, List.not_mem_nil, or_false,
    decide_eq_true_eq]
  constructor
  · rintro ⟨piece, ⟨which, _⟩, inside⟩
    rcases which with rfl | rfl <;> simp [Span.contains] at * <;> omega
  · rintro ⟨inside, outside⟩
    simp [Span.contains] at inside outside
    by_cases before : valid < blocker.lower
    · refine ⟨_, ⟨Or.inl rfl, ?_⟩, ?_⟩ <;> simp [Span.contains] <;> omega
    · refine ⟨_, ⟨Or.inr rfl, ?_⟩, ?_⟩ <;> simp [Span.contains] <;> omega

theorem opposing_fragments_are_ordered (span blocker : Span) (ordered : blocker.lower ≤ blocker.upper) :
    (subtract span blocker).Pairwise (fun left right => left.upper ≤ right.lower) := by
  apply List.Pairwise.filter
  simp
  omega

def clip (span : Span) : List Span → List Span
  | [] => [span]
  | blocker :: newer => (subtract span blocker).flatMap (fun piece => clip piece newer)

theorem clipping_preserves_exactly_uncovered_points (span : Span) (blockers : List Span) (valid : Int) :
    (∃ piece ∈ clip span blockers, piece.contains valid) ↔
      span.contains valid ∧ (∀ blocker ∈ blockers, blocker.contains valid = false) := by
  induction blockers generalizing span with
  | nil => simp [clip]
  | cons first rest ih =>
    rw [List.forall_mem_cons]
    constructor
    · rintro ⟨piece, member, inside⟩
      obtain ⟨prior, present, survives⟩ := List.mem_flatMap.mp member
      obtain ⟨held, unobscured⟩ := (ih prior).mp ⟨piece, survives, inside⟩
      obtain ⟨original, before⟩ := (subtraction_preserves_exactly_uncovered_points span first valid).mp ⟨prior, present, held⟩
      exact ⟨original, before, unobscured⟩
    · rintro ⟨original, before, unobscured⟩
      obtain ⟨prior, present, held⟩ := (subtraction_preserves_exactly_uncovered_points span first valid).mpr ⟨original, before⟩
      obtain ⟨piece, survives, inside⟩ := (ih prior).mpr ⟨held, unobscured⟩
      exact ⟨piece, List.mem_flatMap.mpr ⟨prior, present, survives⟩, inside⟩

theorem subtraction_keeps_endpoints_within_input (span blocker piece : Span)
    (member : piece ∈ subtract span blocker) :
    span.lower ≤ piece.lower ∧ piece.upper ≤ span.upper := by
  simp only [subtract, List.mem_filter, List.mem_cons, List.not_mem_nil, or_false,
    decide_eq_true_eq] at member
  rcases member.1 with rfl | rfl <;> simp <;> omega

theorem clipping_keeps_endpoints_within_input (span piece : Span) (blockers : List Span)
    (member : piece ∈ clip span blockers) :
    span.lower ≤ piece.lower ∧ piece.upper ≤ span.upper := by
  induction blockers generalizing span with
  | nil =>
    have same : piece = span := by simpa [clip] using member
    simp [same]
  | cons first rest ih =>
    obtain ⟨prior, present, survives⟩ := List.mem_flatMap.mp member
    obtain ⟨before, after⟩ := subtraction_keeps_endpoints_within_input span first prior present
    obtain ⟨lower, upper⟩ := ih prior survives
    exact ⟨Int.le_trans before lower, Int.le_trans upper after⟩

theorem clipping_produces_ordered_disjoint_fragments (span : Span) (blockers : List Span)
    (wellFormed : ∀ blocker ∈ blockers, blocker.lower ≤ blocker.upper) :
    (clip span blockers).Pairwise (fun left right => left.upper ≤ right.lower) := by
  induction blockers generalizing span with
  | nil => simp [clip]
  | cons first rest ih =>
    simp only [clip, List.pairwise_flatMap]
    refine ⟨fun piece _ => ih piece (fun other held => wellFormed other (List.mem_cons_of_mem _ held)), ?_⟩
    apply (opposing_fragments_are_ordered span first (wellFormed first (by simp))).imp
    intro left right separate x fromLeft y fromRight
    have upper := (clipping_keeps_endpoints_within_input left x rest fromLeft).2
    have lower := (clipping_keeps_endpoints_within_input right y rest fromRight).1
    exact Int.le_trans upper (Int.le_trans separate lower)

#print axioms subtraction_preserves_exactly_uncovered_points
#print axioms opposing_fragments_are_ordered
#print axioms clipping_preserves_exactly_uncovered_points

#print axioms clipping_produces_ordered_disjoint_fragments

structure Command where
  span : Span
  value : Option Nat
  deriving DecidableEq, Repr

def winningCommand (valid : Int) (commands : List Command) : Option (Option Nat) :=
  (commands.find? (fun command => command.span.contains valid)).map Command.value

def clipCommand (command : Command) (blockers : List Span) : List Command :=
  (clip command.span blockers).map fun piece => { command with span := piece }

theorem clipped_command_preserves_payload_when_visible
    (command : Command) (blockers : List Span) (valid : Int) :
    winningCommand valid (clipCommand command blockers) =
      if command.span.contains valid ∧ (∀ blocker ∈ blockers, blocker.contains valid = false)
      then some command.value else none := by
  unfold winningCommand
  cases found : (clipCommand command blockers).find? (fun item => item.span.contains valid) with
  | none =>
    have absent : ¬ (command.span.contains valid ∧ (∀ blocker ∈ blockers, blocker.contains valid = false)) := by
      intro visible
      obtain ⟨piece, inPieces, covers⟩ := (clipping_preserves_exactly_uncovered_points command.span blockers valid).mpr visible
      have inCommands : ({ command with span := piece } : Command) ∈ clipCommand command blockers :=
        List.mem_map.mpr ⟨piece, inPieces, rfl⟩
      have absent := (List.find?_eq_none.mp found) _ inCommands
      simp [covers] at absent
    simp [absent]
  | some matched =>
    obtain ⟨piece, present, equal⟩ := List.mem_map.mp (List.mem_of_find?_eq_some found)
    obtain rfl := equal
    have covers : (fun item : Command => item.span.contains valid)
        { command with span := piece } = true :=
      List.find?_some (p := fun item : Command => item.span.contains valid) found
    have visible := (clipping_preserves_exactly_uncovered_points command.span blockers valid).mp ⟨piece, present, covers⟩
    rw [ite_eq_left visible]
    rfl

def normalize (blockers : List Span) : List Command → List Command
  | [] => []
  | command :: older => clipCommand command blockers ++ normalize (command.span :: blockers) older

theorem normalization_preserves_visible_winners (commands : List Command)
    (blockers : List Span) (valid : Int) :
    winningCommand valid (normalize blockers commands) =
      if ∀ blocker ∈ blockers, blocker.contains valid = false then winningCommand valid commands else none := by
  induction commands generalizing blockers with
  | nil => simp [normalize, winningCommand]
  | cons command older ih =>
    have append (left right : List Command) :
        winningCommand valid (left ++ right) =
          (winningCommand valid left).orElse (fun () => winningCommand valid right) := by
      simp only [winningCommand, List.find?_append]
      cases left.find? (fun item => item.span.contains valid) <;> rfl
    rw [normalize, append, clipped_command_preserves_payload_when_visible, ih]
    simp only [List.forall_mem_cons]
    by_cases unseen : ∀ blocker ∈ blockers, blocker.contains valid = false
    · cases covers : command.span.contains valid <;> simp [covers, winningCommand]
    · simp [unseen]

#print axioms normalization_preserves_visible_winners

theorem clipped_command_within_uncovered_span (command fragment : Command)
    (blockers : List Span) (valid : Int) (held : fragment ∈ clipCommand command blockers)
    (covers : fragment.span.contains valid) :
    command.span.contains valid ∧ (∀ blocker ∈ blockers, blocker.contains valid = false) := by
  obtain ⟨piece, present, equal⟩ := List.mem_map.mp held
  subst fragment
  exact (clipping_preserves_exactly_uncovered_points command.span blockers valid).mp ⟨piece, present, covers⟩

theorem normalized_commands_respect_prior_blockers (commands : List Command) (fragment : Command)
    (blockers : List Span) (valid : Int) (held : fragment ∈ normalize blockers commands)
    (covers : fragment.span.contains valid) :
    ∀ blocker ∈ blockers, blocker.contains valid = false := by
  induction commands generalizing blockers with
  | nil => simp [normalize] at held
  | cons command older ih =>
    rcases List.mem_append.mp held with atHead | atTail
    · exact (clipped_command_within_uncovered_span command fragment blockers valid atHead covers).2
    · intro blocker present
      exact ih (command.span :: blockers) atTail blocker (List.mem_cons_of_mem _ present)

def disjoint (left right : Command) : Prop :=
  ∀ valid, left.span.contains valid → right.span.contains valid = false

theorem normalization_makes_all_output_fragments_disjoint (commands : List Command)
    (blockers : List Span)
    (validCommands : ∀ command ∈ commands, command.span.lower ≤ command.span.upper)
    (validBlockers : ∀ blocker ∈ blockers, blocker.lower ≤ blocker.upper) :
    (normalize blockers commands).Pairwise disjoint := by
  induction commands generalizing blockers with
  | nil => simp [normalize]
  | cons command older ih =>
    simp only [normalize, List.pairwise_append]
    refine ⟨?_, ?_, ?_⟩
    · simp only [clipCommand, List.pairwise_map]
      apply (clipping_produces_ordered_disjoint_fragments command.span blockers validBlockers).imp
      intro left right separated instant inside
      simp [Span.contains] at inside ⊢
      omega
    · apply ih (command.span :: blockers) (fun item held => validCommands item (List.mem_cons_of_mem _ held))
      intro span present
      rcases List.mem_cons.mp present with same | rest
      · subst span
        exact validCommands command (by simp)
      · exact validBlockers span rest
    · intro head headMember tail tailMember instant inside
      cases covers : tail.span.contains instant
      · rfl
      · have blocked := normalized_commands_respect_prior_blockers older tail
          (command.span :: blockers) instant tailMember covers command.span (by simp)
        have visible := (clipped_command_within_uncovered_span command head blockers instant headMember inside).1
        simp [blocked] at visible

#print axioms normalization_makes_all_output_fragments_disjoint

end RTDB.CompactionIntervals

namespace RTDB.CompactionHistory

open CompactionIntervals

def encode (key commit : Nat) (command : Command) : Event :=
  ⟨commit, match command.value with
    | some value => .put key value command.span
    | none => .delete key command.span⟩

def readCohort (commands : List Command) (key commit : Nat)
    (system queried : Nat) (valid : Int) (later : List Event) : Option Nat :=
  if key = queried ∧ commit ≤ system then
    (winningCommand valid commands).getD (scan system queried valid later)
  else scan system queried valid later

theorem read_cohort_matches_scan (commands : List Command) (key commit system queried : Nat)
    (valid : Int) (later : List Event) :
    scan system queried valid (commands.map (encode key commit) ++ later) =
      readCohort commands key commit system queried valid later := by
  induction commands with
  | nil => simp [readCohort, winningCommand]
  | cons command rest ih =>
    cases payload : command.value <;> cases covers : command.span.contains valid
    all_goals
      by_cases keyMatches : queried = key
      · subst queried
        by_cases visible : commit ≤ system
        · simp [encode, payload, scan, readCohort, winningCommand, covers, visible] at ih ⊢
          all_goals try exact ih
        · simp [encode, payload, scan, readCohort, covers, visible] at ih ⊢
          all_goals try exact ih
      · have other : key ≠ queried := Ne.symm keyMatches
        simp [encode, payload, scan, readCohort, covers, keyMatches, other] at ih ⊢
        all_goals try exact ih

theorem replacing_a_cohort_preserves_its_answer (commands : List Command)
    (key commit system queried : Nat) (valid : Int) (later : List Event) :
    scan system queried valid (((normalize [] commands).map (encode key commit)) ++ later) =
      scan system queried valid (commands.map (encode key commit) ++ later) := by
  rw [read_cohort_matches_scan, read_cohort_matches_scan]
  unfold readCohort
  simp [normalization_preserves_visible_winners]

inductive Piece where
  | cohort (key commit : Nat) (commands : List Command)
  | event (event : Event)

def Piece.original : Piece → List Event
  | .cohort key commit commands => commands.map (encode key commit)
  | .event passthrough => [passthrough]

def Piece.compacted : Piece → List Event
  | .cohort key commit commands => (normalize [] commands).map (encode key commit)
  | .event passthrough => [passthrough]

theorem equal_suffix_answers_survive_an_event (event : Event) (left right : List Event)
    (system queried : Nat) (valid : Int)
    (answer : scan system queried valid left = scan system queried valid right) :
    scan system queried valid (event :: left) = scan system queried valid (event :: right) := by
  cases event.fact <;> simp [scan, answer]

theorem equal_suffix_answers_survive_a_prefix (earlier left right : List Event)
    (system queried : Nat) (valid : Int)
    (answer : scan system queried valid left = scan system queried valid right) :
    scan system queried valid (earlier ++ left) = scan system queried valid (earlier ++ right) := by
  induction earlier with
  | nil => exact answer
  | cons event rest ih => exact equal_suffix_answers_survive_an_event event _ _ _ _ _ ih

theorem normalization_preserves_arbitrary_surrounding_history
    (pieces : List Piece) (system queried : Nat) (valid : Int) :
    point (pieces.flatMap Piece.compacted) system queried valid =
      point (pieces.flatMap Piece.original) system queried valid := by
  simp only [← scan_refines_point]
  induction pieces with
  | nil => rfl
  | cons piece tail ih =>
    simp only [List.flatMap_cons]
    cases piece with
    | event passthrough =>
      exact equal_suffix_answers_survive_an_event passthrough _ _ _ _ _ ih
    | cohort key commit commands =>
      unfold Piece.compacted Piece.original
      exact (replacing_a_cohort_preserves_its_answer commands key commit system queried valid
        (tail.flatMap Piece.compacted)).trans
        (equal_suffix_answers_survive_a_prefix _ _ _ _ _ _ ih)

#print axioms normalization_preserves_arbitrary_surrounding_history

end RTDB.CompactionHistory
