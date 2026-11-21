import Std

namespace RTDB.CheckpointDiscovery

def newer (attempted : Int) (listed : Option Int) : Option Int :=
  match listed with
  | none => none
  | some index => if attempted < index then some index else none

def retries (attempted : Int) : List (Option Int) → List Int
  | [] => []
  | listing :: rest =>
    match newer attempted listing with
    | none => []
    | some index => index :: retries index rest

theorem attempts_strictly_increase (attempted : Int) (listings : List (Option Int)) :
    (attempted :: retries attempted listings).Pairwise (· < ·) := by
  induction listings generalizing attempted with
  | nil => simp [retries]
  | cons listing rest ih =>
    cases listing with
    | none => simp [retries, newer]
    | some index =>
      by_cases advance : attempted < index
      · have ordered := ih index
        simp only [retries, newer, advance, if_true, List.pairwise_cons] at ordered ⊢
        exact ⟨fun later held => by
          rcases List.mem_cons.1 held with same | subsequent
          · simpa [same] using advance
          · exact Int.lt_trans advance (ordered.1 later subsequent),
          ordered⟩
      · simp [retries, newer, advance]

theorem retries_fit_in_the_remaining_indices (attempted maximum : Int) (listings : List (Option Int))
    (inRange : attempted ≤ maximum)
    (advertisedInRange : ∀ index, some index ∈ listings → index ≤ maximum) :
    (retries attempted listings).length ≤ maximum - attempted := by
  induction listings generalizing attempted with
  | nil => simp only [retries, List.length_nil, Int.natCast_zero]; omega
  | cons listing rest ih =>
    cases listing with
    | none => simp [retries, newer]; omega
    | some index =>
      by_cases advance : attempted < index
      · have inside := advertisedInRange index List.mem_cons_self
        have stayed := ih index inside (fun next held => advertisedInRange next (List.mem_cons_of_mem _ held))
        simp only [retries, newer, advance, if_true, List.length_cons, Int.natCast_succ]
        omega
      · simp [retries, newer, advance]; omega

theorem same_index_cannot_be_repeated :
    retries 4 [some 4, some 5] = [] := by decide

#print axioms attempts_strictly_increase
#print axioms retries_fit_in_the_remaining_indices
#print axioms same_index_cannot_be_repeated

end RTDB.CheckpointDiscovery
