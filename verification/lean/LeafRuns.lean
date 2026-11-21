import Std
import IidByteOrder

namespace RTDB.LeafRuns

structure Leaf (Row : Type) where
  data : List Row
  pending : List Row
  cached : Option (List Row)

def rowLE (rank : Row → Nat) (left right : Row) : Bool :=
  decide (rank left ≤ rank right)

def ordered (rank : Row → Nat) (rows : List Row) : Prop :=
  rows.Pairwise fun left right => rowLE rank left right = true

def canonical (rank : Row → Nat) (rows : List Row) : List Row :=
  rows.mergeSort (rowLE rank)

def mergeRuns (rank : Row → Nat) (left right : List Row) : List Row :=
  left.merge right (rowLE rank)

def represents (rank : Row → Nat) (actual history : List Row) : Prop :=
  ordered rank actual ∧ actual.Perm history

def Valid (rank : Row → Nat) (leaf : Leaf Row) : Prop :=
  ordered rank leaf.data ∧
    ∀ cache, leaf.cached = some cache → represents rank cache (leaf.data ++ leaf.pending)

def rowsOf (rank : Row → Nat) (leaf : Leaf Row) : List Row :=
  if leaf.pending.isEmpty then leaf.data
  else leaf.cached.getD (mergeRuns rank leaf.data (canonical rank leaf.pending))

private theorem rowLE_trans (rank : Row → Nat) (a b c : Row) :
    rowLE rank a b → rowLE rank b c → rowLE rank a c := by
  simp [rowLE]
  omega

private theorem rowLE_total (rank : Row → Nat) (a b : Row) :
    rowLE rank a b || rowLE rank b a := by
  simp [rowLE]
  omega

theorem canonical_represents (rank : Row → Nat) (history : List Row) :
    represents rank (canonical rank history) history :=
  ⟨List.pairwise_mergeSort (rowLE_trans rank) (rowLE_total rank) history, List.mergeSort_perm _ _⟩

theorem merge_represents (rank : Row → Nat) {left right leftHistory rightHistory : List Row}
    (l : represents rank left leftHistory) (r : represents rank right rightHistory) :
    represents rank (mergeRuns rank left right) (leftHistory ++ rightHistory) :=
  ⟨List.pairwise_merge (rowLE_trans rank) (rowLE_total rank) _ _ l.1 r.1,
    (List.merge_perm_append (le := rowLE rank)).trans (l.2.append r.2)⟩

theorem visible_rows_represent_compacted_and_pending_rows (rank : Row → Nat)
    (leaf : Leaf Row) (valid : Valid rank leaf) :
    represents rank (rowsOf rank leaf) (leaf.data ++ leaf.pending) := by
  cases pending : leaf.pending with
  | nil =>
      simpa [rowsOf, pending, represents] using valid.1
  | cons first rest =>
      cases cache : leaf.cached with
      | none =>
          have merged := merge_represents rank ⟨valid.1, List.Perm.refl leaf.data⟩
            (canonical_represents rank leaf.pending)
          simpa [rowsOf, pending, cache] using merged
      | some kept =>
          simpa [rowsOf, pending, cache] using valid.2 kept cache

theorem independently_sorted_occurrences_have_one_sequence (rank : Row → Nat)
    (injective : ∀ {a b}, rank a = rank b → a = b)
    {actual history : List Row} (result : represents rank actual history) :
    actual = canonical rank history := by
  have expected := canonical_represents rank history
  apply List.Perm.eq_of_pairwise (le := fun a b => rowLE rank a b = true)
  · intro a b _ _ forward backward
    simp only [rowLE, decide_eq_true_eq] at forward backward
    exact injective (Nat.le_antisymm forward backward)
  · exact result.1
  · exact expected.1
  · exact result.2.trans expected.2.symm

def settled (rows : List Row) : Leaf Row := ⟨rows, [], none⟩

def appendPending (leaf : Leaf Row) (added : List Row) : Leaf Row :=
  ⟨leaf.data, leaf.pending ++ added, none⟩

def memoize (rank : Row → Nat) (leaf : Leaf Row) : Leaf Row :=
  {leaf with cached := some (rowsOf rank leaf)}

def compact (rank : Row → Nat) (leaf : Leaf Row) : Leaf Row :=
  settled (rowsOf rank leaf)

def bulk (rank : Row → Nat) (leaf : Leaf Row) (batch : List Row) : Leaf Row :=
  settled (mergeRuns rank leaf.data (mergeRuns rank (canonical rank leaf.pending) (canonical rank batch)))

def child (rank : Row → Nat) (leaf : Leaf Row) (select : Row → Bool) : Leaf Row :=
  settled ((rowsOf rank leaf).filter select)

theorem settled_valid (rank : Row → Nat) {rows : List Row} (sorted : ordered rank rows) :
    Valid rank (settled rows) := by
  simp [Valid, settled, sorted]

theorem pending_append_preserves_order (rank : Row → Nat) {leaf : Leaf Row}
    (valid : Valid rank leaf) (added : List Row) :
    Valid rank (appendPending leaf added) := by
  simp [Valid, appendPending, valid.1]

theorem memoized_result_describes_the_same_rows (rank : Row → Nat) {leaf : Leaf Row}
    (valid : Valid rank leaf) :
    Valid rank (memoize rank leaf) := by
  refine ⟨valid.1, ?_⟩
  intro cache stored
  simp [memoize] at stored
  subst cache
  exact visible_rows_represent_compacted_and_pending_rows rank leaf valid

theorem bulk_represents_all_old_and_new_occurrences (rank : Row → Nat) {leaf : Leaf Row}
    (valid : Valid rank leaf) (batch : List Row) :
    represents rank (bulk rank leaf batch).data ((leaf.data ++ leaf.pending) ++ batch) := by
  have merged := merge_represents rank ⟨valid.1, List.Perm.refl leaf.data⟩
    (merge_represents rank (canonical_represents rank leaf.pending) (canonical_represents rank batch))
  simpa [bulk, settled, List.append_assoc] using merged

theorem child_represents_the_matching_occurrences (rank : Row → Nat) {leaf : Leaf Row}
    (valid : Valid rank leaf) (select : Row → Bool) :
    represents rank (child rank leaf select).data ((leaf.data ++ leaf.pending).filter select) := by
  have visible := visible_rows_represent_compacted_and_pending_rows rank leaf valid
  exact ⟨List.Pairwise.filter _ visible.1, visible.2.filter _⟩

inductive Edit (Row : Type) where
  | append (added : List Row)
  | cache
  | flush
  | overflow (batch : List Row)
  | split (select : Row → Bool)

def inputRows (leaf : Leaf Row) : List Row := leaf.data ++ leaf.pending

def applyEdit (rank : Row → Nat) (leaf : Leaf Row) : Edit Row → Leaf Row
  | .append added => appendPending leaf added
  | .cache => memoize rank leaf
  | .flush => compact rank leaf
  | .overflow batch => bulk rank leaf batch
  | .split select => child rank leaf select

def expectedEdit (history : List Row) : Edit Row → List Row
  | .append added => history ++ added
  | .cache => history
  | .flush => history
  | .overflow batch => history ++ batch
  | .split select => history.filter select

theorem edit_refines_history (rank : Row → Nat) {leaf : Leaf Row}
    (valid : Valid rank leaf) (edit : Edit Row) :
    Valid rank (applyEdit rank leaf edit) ∧
      (inputRows (applyEdit rank leaf edit)).Perm (expectedEdit (inputRows leaf) edit) := by
  cases edit with
  | append added =>
    refine ⟨pending_append_preserves_order rank valid added, ?_⟩
    simp [applyEdit, expectedEdit, inputRows, appendPending, List.append_assoc]
  | cache =>
    exact ⟨memoized_result_describes_the_same_rows rank valid, List.Perm.refl _⟩
  | flush =>
    have result := visible_rows_represent_compacted_and_pending_rows rank leaf valid
    exact ⟨settled_valid rank result.1, by simpa [applyEdit, compact, settled, inputRows, expectedEdit] using result.2⟩
  | overflow batch =>
    have result := bulk_represents_all_old_and_new_occurrences rank valid batch
    refine ⟨settled_valid rank result.1, ?_⟩
    simpa [applyEdit, bulk, settled, inputRows, expectedEdit] using result.2
  | split select =>
    have result := child_represents_the_matching_occurrences rank valid select
    refine ⟨settled_valid rank result.1, ?_⟩
    simpa [applyEdit, child, settled, inputRows, expectedEdit] using result.2

theorem expected_edit_respects_multiplicities {left right : List Row}
    (same : left.Perm right) (edit : Edit Row) :
    (expectedEdit left edit).Perm (expectedEdit right edit) := by
  cases edit with
  | append added => exact same.append_right added
  | cache | flush => exact same
  | overflow batch => exact same.append_right batch
  | split select => exact same.filter select

theorem sequence_of_edits_refines_history (rank : Row → Nat)
    (leaf : Leaf Row) (history : List Row) (edits : List (Edit Row))
    (valid : Valid rank leaf) (same : (inputRows leaf).Perm history) :
    Valid rank (edits.foldl (applyEdit rank) leaf) ∧
      (inputRows (edits.foldl (applyEdit rank) leaf)).Perm (edits.foldl expectedEdit history) := by
  induction edits generalizing leaf history with
  | nil => exact ⟨valid, same⟩
  | cons edit rest prior =>
    have after := edit_refines_history rank valid edit
    exact prior (applyEdit rank leaf edit) (expectedEdit history edit) after.1
      (after.2.trans (expected_edit_respects_multiplicities same edit))

theorem after_any_leaf_history_read_orders_every_occurrence (rank : Row → Nat)
    (injective : ∀ {a b}, rank a = rank b → a = b)
    (leaf : Leaf Row) (edits : List (Edit Row)) (valid : Valid rank leaf) :
    rowsOf rank (edits.foldl (applyEdit rank) leaf) =
      canonical rank (edits.foldl expectedEdit (inputRows leaf)) := by
  have after := sequence_of_edits_refines_history rank leaf (inputRows leaf) edits valid (List.Perm.refl _)
  have result := visible_rows_represent_compacted_and_pending_rows rank _ after.1
  exact independently_sorted_occurrences_have_one_sequence rank injective ⟨result.1, result.2.trans after.2⟩

def adjacentOrdered (rank : Row → Nat) : List Row → Prop
  | [] => True
  | first :: tail =>
      (∀ second, tail.head? = some second → rowLE rank first second = true) ∧ adjacentOrdered rank tail

theorem adjacent_validation_implies_leaf_order (rank : Row → Nat) (rows : List Row)
    (checked : adjacentOrdered rank rows) : ordered rank rows := by
  induction rows with
  | nil => exact .nil
  | cons first tail prior =>
      obtain ⟨checkedHead, checkedTail⟩ := checked
      have orderedTail := prior checkedTail
      apply List.Pairwise.cons _ orderedTail
      intro other present
      cases tail with
      | nil => simp at present
      | cons next rest =>
          have pair := checkedHead next rfl
          rcases List.mem_cons.mp present with same | later
          · subst other
            exact pair
          · exact rowLE_trans rank _ _ _ pair (List.rel_of_pairwise_cons orderedTail later)

theorem stable_iid_leaf_history_returns_the_exact_rank_sequence
    (iidAt : Fin 4294967296 → List IidByteOrder.Byte) (leaf : Leaf (Fin 4294967296))
    (edits : List (Edit (Fin 4294967296)))
    (valid : Valid (fun row => IidByteOrder.iidRowRank (iidAt row) row.val) leaf) :
    let rank := fun (row : Fin 4294967296) => IidByteOrder.iidRowRank (iidAt row) row.val
    rowsOf rank (edits.foldl (applyEdit rank) leaf) =
      canonical rank (edits.foldl expectedEdit (inputRows leaf)) := by
  let rank := fun (row : Fin 4294967296) => IidByteOrder.iidRowRank (iidAt row) row.val
  apply after_any_leaf_history_read_orders_every_occurrence rank _ leaf edits valid
  intro left right equal
  apply Fin.ext
  exact IidByteOrder.equal_iid_row_ranks_identify_the_row _ _ _ _ left.isLt right.isLt equal

#print axioms visible_rows_represent_compacted_and_pending_rows
#print axioms bulk_represents_all_old_and_new_occurrences
#print axioms child_represents_the_matching_occurrences
#print axioms edit_refines_history
#print axioms after_any_leaf_history_read_orders_every_occurrence
#print axioms adjacent_validation_implies_leaf_order
#print axioms stable_iid_leaf_history_returns_the_exact_rank_sequence

end RTDB.LeafRuns
