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

inductive SearchResult where
  | found (index : Nat)
  | absent (index : Nat)

def SearchConforms (runs : List (Nat × Value)) (logical : Nat) : SearchResult → Prop
  | .found j => ∃ value, runs[j]? = some (logical, value)
  | .absent cut =>
    (∀ j stop value, j < cut → runs[j]? = some (stop, value) → stop < logical) ∧
    (∀ j stop value, cut ≤ j → runs[j]? = some (stop, value) → logical < stop)

def arrowPhysical : SearchResult → Nat
  | .found j => j + 1
  | .absent j => j

theorem first_end_after_at_position (runs : List (Nat × Value)) (logical i stop : Nat) (value : Value)
    (atPosition : runs[i]? = some (stop, value))
    (covers : logical < stop)
    (before : ∀ j prior earlier, j < i → runs[j]? = some (prior, earlier) → prior ≤ logical) :
    firstEndAfter logical runs = some value := by
  induction i generalizing runs with
  | zero =>
      cases runs with
      | nil => simp at atPosition
      | cons head tail =>
          simp at atPosition
          subst head
          simp [firstEndAfter, covers]
  | succ i ih =>
      cases runs with
      | nil => simp at atPosition
      | cons head tail =>
          rcases head with ⟨endHead, head⟩
          have pastHead := before 0 endHead head (by omega) (by simp)
          simp only [List.getElem?_cons_succ] at atPosition
          simp only [firstEndAfter, ite_eq_right (Nat.not_lt.mpr pastHead)]
          apply ih tail atPosition
          intro j prior earlier beforePosition entry
          exact before (j+1) prior earlier (by omega) (by simpa using entry)

theorem positive_order_means_later_end_is_greater (runs : List (Nat × Value))
    (ordered : runs.Pairwise (fun x y => x.1 < y.1))
    (j k stop rest : Nat) (a b : Value) (first : runs[j]? = some (stop, a))
    (second : runs[k]? = some (rest, b)) (lower : j < k) : stop < rest := by
  obtain ⟨atFirst, getFirst⟩ := List.getElem?_eq_some_iff.mp first
  obtain ⟨atSecond, getSecond⟩ := List.getElem?_eq_some_iff.mp second
  have greater := List.pairwise_iff_getElem.mp ordered j k atFirst atSecond lower
  rw [getFirst, getSecond] at greater
  exact greater

theorem arrow_physical_index_is_first_exclusive_end (runs : List (Nat × Value)) (logical : Nat)
    (result : SearchResult)
    (ordered : runs.Pairwise (fun x y => x.1 < y.1))
    (inside : ∃ (j stop : Nat) (value : Value), runs[j]? = some (stop, value) ∧ logical < stop)
    (search : SearchConforms runs logical result) :
    ∃ (stop : Nat) (value : Value),
      runs[arrowPhysical result]? = some (stop, value) ∧ logical < stop ∧
      ∀ j prior earlier, j < arrowPhysical result → runs[j]? = some (prior, earlier) → prior ≤ logical := by
  obtain ⟨insideAt, insideStop, insideValue, atCover, cover⟩ := inside
  obtain ⟨coverWithin, _⟩ := List.getElem?_eq_some_iff.mp atCover
  cases result with
  | found j =>
      obtain ⟨matchValue, matchAt⟩ := search
      obtain ⟨matchWithin, _⟩ := List.getElem?_eq_some_iff.mp matchAt
      have afterMatch : j < insideAt := by
        have unequal : insideAt ≠ j := by
          intro eq
          rw [eq, matchAt] at atCover
          simp at atCover
          omega
        by_cases greater : j < insideAt
        · exact greater
        · have earlier := positive_order_means_later_end_is_greater runs ordered
            insideAt j insideStop logical insideValue matchValue atCover matchAt (by omega)
          omega
      have nextWithin : j + 1 < runs.length := by omega
      let nextPair := runs[j+1]'nextWithin
      have nextAt : runs[j+1]? = some nextPair := List.getElem?_eq_getElem nextWithin
      have after := positive_order_means_later_end_is_greater runs ordered
        j (j+1) logical nextPair.1 matchValue nextPair.2 matchAt nextAt (by omega)
      refine ⟨nextPair.1, nextPair.2, nextAt, after, ?_⟩
      intro k prior value tooEarly lookup
      change k < j + 1 at tooEarly
      by_cases same : k = j
      · rw [same, matchAt] at lookup
        simp at lookup
        omega
      · exact Nat.le_of_lt (positive_order_means_later_end_is_greater runs ordered
          k j prior logical value matchValue lookup matchAt (by omega))
  | absent j =>
      obtain ⟨before, after⟩ := search
      have beforeCover : j ≤ insideAt := by
        by_cases smaller : insideAt < j
        · have prior := before insideAt insideStop insideValue smaller atCover
          omega
        · omega
      have hasRun : j < runs.length := by omega
      let firstPair := runs[j]'hasRun
      have firstAt : runs[j]? = some firstPair := List.getElem?_eq_getElem hasRun
      have firstCovers := after j firstPair.1 firstPair.2 (Nat.le_refl _) firstAt
      exact ⟨firstPair.1, firstPair.2, firstAt, firstCovers,
        fun k prior value earlier lookup => Nat.le_of_lt (before k prior value earlier lookup)⟩

theorem arrow_run_search_matches_the_first_exclusive_end (runs : List (Nat × Value)) (logical : Nat)
    (result : SearchResult)
    (ordered : runs.Pairwise (fun x y => x.1 < y.1))
    (inside : ∃ (j stop : Nat) (value : Value), runs[j]? = some (stop, value) ∧ logical < stop)
    (search : SearchConforms runs logical result) :
    (runs[arrowPhysical result]?).map Prod.snd = firstEndAfter logical runs := by
  obtain ⟨stop, value, entry, covers, before⟩ :=
    arrow_physical_index_is_first_exclusive_end runs logical result ordered inside search
  rw [entry, Option.map_some]
  exact (first_end_after_at_position runs logical _ stop value entry covers before).symm

theorem a_returned_value_has_a_covering_run (runs : List (Nat × Value))
    (logical : Nat) (value : Value) (read : firstEndAfter logical runs = some value) :
    ∃ (j stop : Nat), runs[j]? = some (stop, value) ∧ logical < stop := by
  induction runs with
  | nil => simp [firstEndAfter] at read
  | cons first later ih =>
      obtain ⟨stop, head⟩ := first
      by_cases covers : logical < stop
      · simp [firstEndAfter, covers] at read
        subst head
        exact ⟨0, stop, by simp, covers⟩
      · simp [firstEndAfter, covers] at read
        obtain ⟨found, finalEnd, entry, coversLast⟩ := ih read
        exact ⟨found+1, finalEnd, by simpa using entry, coversLast⟩

theorem arrow_search_for_a_slice_matches_plain [DecidableEq Value] (values : List Value)
    (start row : Nat) (result : SearchResult)
    (inside : start + row < values.length)
    (search : SearchConforms (physicalEnds 0 (encode values)) (start + row) result) :
    ((physicalEnds 0 (encode values))[arrowPhysical result]?).map Prod.snd = (values.drop start)[row]? := by
  have reads := end_lookup_matches_expansion 0 (start + row) (encode values) (Nat.zero_le _)
  rw [expand_encode, Nat.sub_zero, List.getElem?_eq_getElem inside] at reads
  obtain ⟨j, stop, entry, covers⟩ := a_returned_value_has_a_covering_run _ _ _ reads
  rw [arrow_run_search_matches_the_first_exclusive_end (physicalEnds 0 (encode values))
    (start + row) result (positive_runs_have_strict_ends _ _ (encoded_widths_are_positive values))
    ⟨j, stop, values[start + row], entry, covers⟩ search]
  exact encoded_slice_read_matches_plain values start row

theorem signed_index_cast_preserves_sliced_coordinate (width start row : Nat)
    (signedBound : 2 * (start + row) < 2 ^ width) :
    (BitVec.ofNat width (start + row)).toInt = Int.ofNat (start + row) := by
  have unsignedBound : start + row < 2 ^ width := by omega
  simp [BitVec.toInt, Nat.mod_eq_of_lt unsignedBound, signedBound]

#print axioms expand_encode
#print axioms encoded_widths_are_positive
#print axioms positive_runs_have_strict_ends
#print axioms end_lookup_matches_expansion
#print axioms encoded_slice_read_matches_plain

#print axioms arrow_physical_index_is_first_exclusive_end
#print axioms arrow_run_search_matches_the_first_exclusive_end
#print axioms arrow_search_for_a_slice_matches_plain
#print axioms signed_index_cast_preserves_sliced_coordinate

end RTDB.RunEncoding
