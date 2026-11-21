import Std

namespace RTDB.CompactionMerge

def heads (pages : List (List α)) : List α := pages.filterMap List.head?

def ordered (key : α → Nat) (pages : List (List α)) : Prop :=
  ∀ page ∈ pages, page.Pairwise (fun left right => key left ≤ key right)

inductive Take : List (List α) → α → List (List α) → Prop where
  | here : Take ((row :: rest) :: pages) row (rest :: pages)
  | later : Take pages row advanced → Take (page :: pages) row (page :: advanced)

def eligible (key : α → Nat) (pages : List (List α)) (row : α) : Prop :=
  ∀ candidate ∈ heads pages, key row ≤ key candidate

def canFinish (pages : List (List α)) : Prop := pages.flatten.length ≤ 0

inductive Drain (key : α → Nat) : List (List α) → List α → Prop where
  | empty : canFinish pages → Drain key pages []
  | emit : Take pages row advanced → eligible key pages row →
      Drain key advanced output → Drain key pages (row :: output)

theorem take_accounts_for_one_occurrence (step : Take pages row advanced) :
    (row :: advanced.flatten).Perm pages.flatten := by
  induction step with
  | here => simp [List.flatten_cons]
  | @later _ _ _ page _ ih =>
    rw [List.flatten_cons, List.flatten_cons]
    exact List.perm_middle.symm.trans ((List.Perm.refl page).append ih)

theorem take_keeps_pages_ordered (step : Take pages row advanced)
    (sorted : ordered key pages) : ordered key advanced := by
  induction step with
  | @here row rest pages =>
    intro page held
    rcases List.mem_cons.mp held with same | later
    · subst page
      exact (List.pairwise_cons.mp (sorted _ List.mem_cons_self)).2
    · exact sorted page (List.mem_cons_of_mem _ later)
  | @later pages row advanced first _ ih =>
    intro page held
    rcases List.mem_cons.mp held with same | later
    · subst page
      exact sorted _ List.mem_cons_self
    · exact ih (fun stream h => sorted stream (List.mem_cons_of_mem _ h)) page later

theorem any_eligible_head_lower_bounds_the_input (least : eligible key pages row)
    (sorted : ordered key pages) (held : later ∈ pages.flatten) : key row ≤ key later := by
  obtain ⟨page, present, inPage⟩ := List.mem_flatten.mp held
  cases page with
  | nil => simp at inPage
  | cons first rest =>
    have atHead : first ∈ heads pages := List.mem_filterMap.mpr ⟨_, present, rfl⟩
    have bound : key row ≤ key first := by
      have compared := least first atHead
      omega
    rcases List.mem_cons.mp inPage with same | afterHead
    · simpa [same] using bound
    · exact Nat.le_trans bound ((List.pairwise_cons.mp (sorted _ present)).1 _ afterHead)

theorem drain_preserves_all_occurrences (merged : Drain key pages output) :
    output.Perm pages.flatten := by
  induction merged with
  | @empty pages exhausted =>
    have none : pages.flatten = [] := List.length_eq_zero_iff.mp (Nat.eq_zero_of_le_zero exhausted)
    exact none ▸ List.Perm.nil
  | emit step _ _ ih => exact (ih.cons _).trans (take_accounts_for_one_occurrence step)

theorem drain_emits_sorted_keys (merged : Drain key pages output)
    (sorted : ordered key pages) : output.Pairwise (fun left right => key left ≤ key right) := by
  induction merged with
  | empty => exact List.Pairwise.nil
  | emit step least tail ih =>
    apply List.pairwise_cons.mpr
    refine ⟨fun later held => ?_, ih (take_keeps_pages_ordered step sorted)⟩
    have inRest := (drain_preserves_all_occurrences tail).mem_iff.mp held
    have inInput := (take_accounts_for_one_occurrence step).mem_iff.mp (List.mem_cons_of_mem _ inRest)
    exact any_eligible_head_lower_bounds_the_input least sorted inInput

theorem drain_agrees_with_flatten_and_sort (merged : Drain key pages output)
    (sorted : ordered key pages) :
    output.map key = (pages.flatten.map key).mergeSort (fun left right => decide (left ≤ right)) := by
  apply List.Perm.eq_of_pairwise (le := (· ≤ ·))
  · exact fun _ _ _ _ le ge => Nat.le_antisymm le ge
  · exact List.pairwise_map.mpr (drain_emits_sorted_keys merged sorted)
  · exact (List.pairwise_mergeSort (le := fun (left right : Nat) => decide (left ≤ right))
      (fun _ _ _ ab bc => decide_eq_true (Nat.le_trans (of_decide_eq_true ab) (of_decide_eq_true bc)))
      (fun a b => by simpa only [Bool.or_eq_true, decide_eq_true_eq] using Nat.le_total a b)
      (pages.flatten.map key)).imp of_decide_eq_true
  · exact ((drain_preserves_all_occurrences merged).map key).trans (List.mergeSort_perm _ _).symm

theorem front_can_be_taken (held : row ∈ heads pages) : ∃ advanced, Take pages row advanced := by
  induction pages with
  | nil => simp [heads] at held
  | cons page pages ih =>
    cases page with
    | nil =>
      obtain ⟨advanced, step⟩ := ih (by simpa [heads] using held)
      exact ⟨[] :: advanced, step.later⟩
    | cons first rest =>
      simp only [heads, List.filterMap_cons, List.head?_cons, List.mem_cons] at held
      rcases held with same | later
      · subst first
        exact ⟨rest :: pages, Take.here⟩
      · obtain ⟨advanced, step⟩ := ih later
        exact ⟨(first :: rest) :: advanced, step.later⟩

theorem a_nonempty_input_has_a_head (nonempty : pages.flatten ≠ []) : heads pages ≠ [] := by
  obtain ⟨row, held⟩ := List.exists_mem_of_ne_nil _ nonempty
  obtain ⟨page, present, inPage⟩ := List.mem_flatten.mp held
  cases page with
  | nil => simp at inPage
  | cons first rest => exact List.ne_nil_of_mem (List.mem_filterMap.mpr ⟨_, present, rfl⟩ : first ∈ heads pages)

theorem every_batch_can_drain (key : α → Nat) (pages : List (List α)) :
    ∃ output, Drain key pages output := by
  by_cases exhausted : pages.flatten = []
  · exact ⟨[], Drain.empty (by simp [canFinish, exhausted])⟩
  · have nonempty := a_nonempty_input_has_a_head exhausted
    let row := (heads pages).minOn key nonempty
    have held : row ∈ heads pages := List.minOn_mem
    obtain ⟨advanced, step⟩ := front_can_be_taken held
    have shorter : advanced.flatten.length < pages.flatten.length := by
      have removed := (take_accounts_for_one_occurrence step).length_eq
      simp only [List.length_cons] at removed
      omega
    obtain ⟨output, rest⟩ := every_batch_can_drain key advanced
    have least : eligible key pages row := by
      intro candidate member
      have compared : key row ≤ key candidate := List.apply_minOn_le_of_mem member
      omega
    exact ⟨row :: output, Drain.emit step least rest⟩
termination_by pages.flatten.length
decreasing_by simpa using shorter

theorem a_tie_allows_either_payload_order :
    let key := fun _ : Nat => 0
    Drain key [[1], [2]] [1, 2] ∧ Drain key [[1], [2]] [2, 1] := by
  constructor
  · apply Drain.emit Take.here _ (Drain.emit (Take.later Take.here) _ (Drain.empty _)) <;>
      simp [eligible, canFinish]
  · apply Drain.emit (Take.later Take.here) _ (Drain.emit Take.here _ (Drain.empty _)) <;>
      simp [eligible, canFinish]

#print axioms drain_preserves_all_occurrences
#print axioms drain_emits_sorted_keys
#print axioms drain_agrees_with_flatten_and_sort
#print axioms every_batch_can_drain
#print axioms a_tie_allows_either_payload_order

end RTDB.CompactionMerge
