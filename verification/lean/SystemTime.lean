import Std

namespace RTDB.SystemTime

inductive Request where
  | automatic (clock : Int)
  | explicit (clock requested : Int)
  | catalog (clock : Int)

def automatic (maximum : Int) (previous : Option Int) (clock : Int) : Option Int :=
  match previous with
  | some floor =>
    if clock ≤ floor then
      if floor < maximum then some (floor + 1) else none
    else some clock
  | none => some clock

-- As in XTDB, a supplied time must be strictly later than its predecessor's; it may lead the clock.
def admitsSupplied (previous : Option Int) (requested : Int) : Bool :=
  match previous with
  | some floor => decide (floor < requested)
  | none => true

def chosen (maximum : Int) (previous : Option Int) : Request → Option Int
  | .automatic clock => automatic maximum previous clock
  | .catalog clock => automatic maximum previous clock
  | .explicit clock requested =>
    (automatic maximum previous clock).map fun fallback =>
      if admitsSupplied previous requested then requested else fallback

def trace (maximum : Int) (previous : Option Int) : List Request → List Int
  | [] => []
  | request :: rest =>
    match chosen maximum previous request with
    | none => []
    | some time => time :: trace maximum (some time) rest

theorem automatic_advances (selected : automatic maximum (some previous) clock = some time) :
    previous < time := by
  by_cases stalled : clock ≤ previous
  · by_cases room : previous < maximum
    · simp [automatic, stalled, room] at selected
      omega
    · simp [automatic, stalled, room] at selected
  · simp [automatic, stalled] at selected
    omega

theorem chosen_time_advances (selected : chosen maximum (some previous) request = some time) :
    previous < time := by
  cases request with
  | automatic => exact automatic_advances selected
  | catalog => exact automatic_advances selected
  | explicit clock requested =>
    cases fallback : automatic maximum (some previous) clock with
    | none => simp [chosen, fallback] at selected
    | some next =>
      by_cases admitted : admitsSupplied (some previous) requested = true
      · simp [chosen, fallback, admitted] at selected
        simp [admitsSupplied] at admitted
        omega
      · simp [chosen, fallback, admitted] at selected
        exact selected ▸ automatic_advances fallback

theorem an_admitted_time_follows_its_predecessor
    (admitted : admitsSupplied (some previous) requested = true) :
    previous < requested := by
  simp [admitsSupplied] at admitted
  omega

theorem staged_times_increase (maximum previous : Int) (requests : List Request) :
    (previous :: trace maximum (some previous) requests).Pairwise (· < ·) := by
  induction requests generalizing previous with
  | nil => simp [trace]
  | cons request rest ih =>
    cases selected : chosen maximum (some previous) request with
    | none => simp [trace, selected]
    | some time =>
      simp only [trace, selected, List.pairwise_cons]
      have next := chosen_time_advances selected
      obtain ⟨afterTime, orderedRest⟩ := List.pairwise_cons.mp (ih time)
      refine ⟨fun later held => ?_, afterTime, orderedRest⟩
      rcases List.mem_cons.mp held with same | appended
      · simpa [same] using next
      · exact Int.lt_trans next (afterTime later appended)

theorem a_fresh_trace_is_ordered (maximum : Int) (requests : List Request) :
    (trace maximum none requests).Pairwise (· < ·) := by
  cases requests with
  | nil => simp [trace]
  | cons request rest =>
    cases selected : chosen maximum none request with
    | none => simp [trace, selected]
    | some time => simpa [trace, selected] using staged_times_increase maximum time rest

theorem completed_prefix_can_seed_replay (maximum : Int) (previous : Option Int)
    (before after : List Request)
    (completed : (trace maximum previous before).length = before.length) :
    trace maximum previous (before ++ after) =
      trace maximum previous before ++
        trace maximum ((trace maximum previous before).getLast?.or previous) after := by
  induction before generalizing previous with
  | nil => simp [trace]
  | cons request rest ih =>
    cases selected : chosen maximum previous request with
    | none => simp [trace, selected] at completed
    | some time =>
      have completeRest : (trace maximum (some time) rest).length = rest.length := by
        simpa [trace, selected] using completed
      have cursor : ((time :: trace maximum (some time) rest).getLast?.or previous) =
          ((trace maximum (some time) rest).getLast?.or (some time)) := by
        cases trace maximum (some time) rest with
        | nil => rfl
        | cons head tail =>
          rw [List.getLast?_cons_cons, List.getLast?_eq_some_getLast (by simp : head :: tail ≠ [])] <;> rfl
      simp only [List.cons_append, trace, selected, List.cons.injEq, true_and, cursor]
      exact ih (some time) completeRest

theorem catalog_and_explicit_refusals_use_the_same_clock :
    trace 100 none [.explicit 10 5, .catalog 2, .explicit 20 6, .explicit 20 7, .explicit 20 30,
      .explicit 30 25, .automatic 8] = [5, 6, 20, 21, 30, 31, 32] := by
  decide

#print axioms chosen_time_advances
#print axioms an_admitted_time_follows_its_predecessor
#print axioms staged_times_increase
#print axioms a_fresh_trace_is_ordered
#print axioms completed_prefix_can_seed_replay
#print axioms catalog_and_explicit_refusals_use_the_same_clock

end RTDB.SystemTime
