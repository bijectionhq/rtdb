import Std

namespace RTDB.PartitionedTopK

structure Row where
  partition : Nat
  score : Int
  id : Nat
  deriving DecidableEq

def aheadInPartition (candidate row : Row) : Bool :=
  decide (candidate.partition = row.partition ∧ row.score < candidate.score)

def keeps (rows : List Row) (limit : Nat) (row : Row) : Bool :=
  decide (rows.countP (aheadInPartition · row) < limit)

def descendingScores (rows : List Row) : Prop :=
  rows.Pairwise (fun first later => later.score ≤ first.score)

theorem prefix_row_survives_partition_pruning (prefixRows suffix : List Row) (row : Row) (limit : Nat)
    (ordered : descendingScores (prefixRows ++ suffix))
    (fits : prefixRows.length ≤ limit) (requested : row ∈ prefixRows) :
    keeps (prefixRows ++ suffix) limit row = true := by
  obtain ⟨before, after, rfl⟩ := List.append_of_mem requested
  simp only [List.append_assoc, List.cons_append] at ordered ⊢
  have later : ∀ candidate ∈ row :: (after ++ suffix), candidate.score ≤ row.score := by
    intro candidate member
    have sortedTail := (List.pairwise_append.mp ordered).2.1
    rcases List.mem_cons.mp member with same | tail
    · simp [same]
    · exact (List.pairwise_cons.mp sortedTail).1 candidate tail
  have tailCount : (row :: (after ++ suffix)).countP (aheadInPartition · row) = 0 := by
    apply List.countP_eq_zero.mpr
    intro candidate member
    simp [aheadInPartition, Int.not_lt.mpr (later candidate member)]
  have priorCount := List.countP_le_length (p := (aheadInPartition · row)) (l := before)
  have limitBound : before.length + 1 ≤ limit := by
    simp at fits
    omega
  simp only [keeps, decide_eq_true_eq, List.countP_append, tailCount, Nat.add_zero]
  omega

-- The ordered list already includes the reference query's tie-breaking order.
-- We keep its relative order, including rows with equal scores in one or more partitions.
theorem global_top_k_survives_partition_pruning (rows : List Row) (limit : Nat)
    (ordered : descendingScores rows) :
    (rows.filter (keeps rows limit)).take limit = rows.take limit := by
  let wanted := rows.take limit
  let remaining := rows.drop limit
  have original : wanted ++ remaining = rows := List.take_append_drop limit rows
  have kept : wanted.filter (keeps rows limit) = wanted := by
    apply List.filter_eq_self.mpr
    intro row member
    have sorted : descendingScores (wanted ++ remaining) := original ▸ ordered
    have fits : wanted.length ≤ limit := by simp [wanted, Nat.min_le_left]
    rw [← original]
    exact prefix_row_survives_partition_pruning wanted remaining row limit sorted fits member
  calc
    (rows.filter (keeps rows limit)).take limit =
        ((wanted ++ remaining).filter (keeps rows limit)).take limit :=
      congrArg (fun values => (values.filter (keeps rows limit)).take limit) original.symm
    _ = wanted := by
      rw [List.filter_append, kept]
      by_cases enough : limit ≤ rows.length
      · have exactLength : wanted.length = limit := by simp [wanted, Nat.min_eq_left enough]
        rw [← exactLength]
        simp
      · have nothingLeft : remaining = [] := List.drop_eq_nil_iff.mpr (by omega)
        simp [nothingLeft, wanted, List.take_take]

def prunedPage (rows : List Row) (offset fetch : Nat) : List Row :=
  ((rows.filter (keeps rows (offset + fetch))).take (offset + fetch)).drop offset

theorem ordered_page_survives_partition_pruning (rows : List Row) (offset fetch : Nat)
    (ordered : descendingScores rows) :
    prunedPage rows offset fetch = (rows.take (offset + fetch)).drop offset := by
  unfold prunedPage
  rw [global_top_k_survives_partition_pruning rows (offset + fetch) ordered]

theorem tied_rows_remain_eligible :
    keeps [⟨0, 1, 0⟩, ⟨0, 1, 1⟩] 1 ⟨0, 1, 1⟩ = true := by decide

theorem an_offset_keeps_the_following_row :
    prunedPage [⟨0, 2, 0⟩, ⟨0, 1, 1⟩] 1 1 = [⟨0, 1, 1⟩] := by decide

#print axioms prefix_row_survives_partition_pruning
#print axioms global_top_k_survives_partition_pruning
#print axioms ordered_page_survives_partition_pruning
#print axioms tied_rows_remain_eligible
#print axioms an_offset_keeps_the_following_row

end RTDB.PartitionedTopK
