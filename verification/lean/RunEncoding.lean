import Std

namespace RTDB.RunEncoding

structure Run (Value : Type) where
  width : Nat
  value : Value

def encode [DecidableEq Value] : List Value → List (Run Value)
  | [] => []
  | value :: later =>
    match encode later with
    | [] => [⟨1, value⟩]
    | first :: rest =>
      if value = first.value then ⟨first.width + 1, value⟩ :: rest
      else ⟨1, value⟩ :: first :: rest

def expand : List (Run Value) → List Value
  | [] => []
  | run :: rest => List.replicate run.width run.value ++ expand rest

theorem expand_encode [DecidableEq Value] (values : List Value) : expand (encode values) = values := by
  induction values with
  | nil => rfl
  | cons value later ih =>
    cases encoded : encode later with
    | nil =>
      have empty : later = [] := by simpa [encoded, expand] using ih.symm
      simp [encode, expand, empty]
    | cons first rest =>
      rw [encoded] at ih
      rw [encode, encoded]
      by_cases equal : value = first.value
      · simpa [equal, expand, List.replicate_succ] using congrArg (List.cons first.value) ih
      · simpa [equal, expand] using congrArg (List.cons value) ih

def PositiveWidths (runs : List (Run Value)) : Prop := ∀ run ∈ runs, 0 < run.width

theorem encoded_widths_are_positive [DecidableEq Value] (values : List Value) :
    PositiveWidths (encode values) := by
  induction values with
  | nil => simp [encode, PositiveWidths]
  | cons value later ih =>
    cases encoded : encode later with
    | nil => simp [encode, encoded, PositiveWidths]
    | cons first rest =>
      rw [encoded] at ih
      rw [encode, encoded]
      intro run member
      by_cases equal : value = first.value
      · simp only [equal, ite_true, List.mem_cons] at member
        rcases member with isFirst | inRest
        · subst run
          exact Nat.zero_lt_succ first.width
        · exact ih run (List.mem_cons_of_mem _ inRest)
      · simp only [equal, ite_false, List.mem_cons] at member
        rcases member with isFirst | isSecond | inRest
        · subst run
          exact Nat.zero_lt_succ 0
        · subst run
          exact ih first (List.mem_cons_self ..)
        · exact ih run (List.mem_cons_of_mem _ inRest)

def physicalEnds (prior : Nat) : List (Run Value) → List (Nat × Value)
  | [] => []
  | run :: rest =>
    let stop := prior + run.width
    (stop, run.value) :: physicalEnds stop rest

theorem end_after_base (base : Nat) (runs : List (Run Value)) (positive : PositiveWidths runs) :
    ∀ pair ∈ physicalEnds base runs, base < pair.1 := by
  induction runs generalizing base with
  | nil => simp [physicalEnds]
  | cons run rest ih =>
    have width := positive run (List.mem_cons_self ..)
    intro pair present
    simp only [physicalEnds, List.mem_cons] at present
    rcases present with head | tail
    · subst pair
      simp [Nat.lt_add_of_pos_right width]
    · exact Nat.lt_trans (Nat.lt_add_of_pos_right width) (ih _
        (fun entry member => positive entry (List.mem_cons_of_mem _ member)) pair tail)

theorem positive_runs_have_strict_ends (base : Nat) (runs : List (Run Value)) (positive : PositiveWidths runs) :
    (physicalEnds base runs).Pairwise (fun first second => first.1 < second.1) := by
  induction runs generalizing base with
  | nil => simp [physicalEnds]
  | cons run rest ih =>
    have restPositive : PositiveWidths rest :=
      fun entry member => positive entry (List.mem_cons_of_mem _ member)
    apply List.pairwise_cons.mpr
    exact ⟨end_after_base _ rest restPositive, ih _ restPositive⟩

def firstEndAfter (logicalIndex : Nat) : List (Nat × Value) → Option Value
  | [] => none
  | (stop, value) :: rest => if logicalIndex < stop then some value else firstEndAfter logicalIndex rest

theorem end_lookup_matches_expansion (base index : Nat) (runs : List (Run Value)) (inside : base ≤ index) :
    firstEndAfter index (physicalEnds base runs) = (expand runs)[index - base]? := by
  induction runs generalizing base with
  | nil => rfl
  | cons run rest ih =>
    by_cases inHead : index < base + run.width
    · have inWidth : index - base < run.width := by omega
      have inRepeat : index - base < (List.replicate run.width run.value).length := by simpa using inWidth
      rw [expand, List.getElem?_append_left inRepeat]
      simp [physicalEnds, firstEndAfter, inHead, inWidth]
    · have afterHead : base + run.width ≤ index := by omega
      have indexAfterRepeat : (List.replicate run.width run.value).length ≤ index - base := by simp; omega
      have remainder : index - (base + run.width) = index - base - run.width := by omega
      rw [expand, List.getElem?_append_right indexAfterRepeat]
      simp [physicalEnds, firstEndAfter, inHead, ih (base + run.width) afterHead, remainder]

def readSlice (start row : Nat) (runs : List (Nat × Value)) : Option Value :=
  firstEndAfter (start + row) runs

theorem encoded_slice_read_matches_plain [DecidableEq Value] (values : List Value) (start row : Nat) :
    readSlice start row (physicalEnds 0 (encode values)) = (values.drop start)[row]? := by
  rw [readSlice, end_lookup_matches_expansion 0 (start + row) _ (Nat.zero_le _), expand_encode]
  simp [List.getElem?_drop]

#print axioms expand_encode
#print axioms encoded_widths_are_positive
#print axioms positive_runs_have_strict_ends
#print axioms end_lookup_matches_expansion
#print axioms encoded_slice_read_matches_plain

end RTDB.RunEncoding
