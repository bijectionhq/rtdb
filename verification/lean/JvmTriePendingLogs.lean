import Std

namespace RTDB.JvmTriePendingLogs

structure LogRef where
  buffer : Nat
  count : Nat

def read (buffers : List (List Row)) (ref : LogRef) : List Row :=
  (buffers[ref.buffer]?.getD []).take ref.count

def fork (buffers : List (List Row)) (parent : LogRef) (added : List Row) :
    List (List Row) × LogRef :=
  let log := read buffers parent ++ added
  (buffers ++ [log], ⟨buffers.length, log.length⟩)

theorem child_reads_its_parent_prefix_and_added_rows (buffers : List (List Row))
    (parent : LogRef) (added : List Row) :
    read (fork buffers parent added).1 (fork buffers parent added).2 =
      read buffers parent ++ added := by
  let log := read buffers parent ++ added
  change read (buffers ++ [log]) ⟨buffers.length, log.length⟩ = log
  simp [read]

theorem fork_preserves_every_retained_log (buffers : List (List Row))
    (parent earlier : LogRef) (added : List Row) (valid : earlier.buffer < buffers.length) :
    read (fork buffers parent added).1 earlier = read buffers earlier := by
  simp [fork, read, List.getElem?_append_left valid]

inductive History : List (List Row) → List (List Row) → Prop where
  | unchanged (buffers : List (List Row)) : History buffers buffers
  | next {before middle : List (List Row)} :
      History before middle → (parent : LogRef) → (added : List Row) →
      History before (fork middle parent added).1

theorem fork_history_extends_the_allocated_buffers {before after : List (List Row)}
    (history : History before after) : before <+: after := by
  induction history with
  | unchanged => exact ⟨[], by simp⟩
  | next _ parent added prior =>
      exact prior.trans (show _ <+: (fork _ parent added).1 from ⟨_, rfl⟩)

theorem fork_history_preserves_every_retained_log {before after : List (List Row)}
    (history : History before after) (retained : LogRef)
    (valid : retained.buffer < before.length) :
    read after retained = read before retained := by
  obtain ⟨newBuffers, complete⟩ := fork_history_extends_the_allocated_buffers history
  simp [← complete, read, List.getElem?_append_left valid]

def resizedCopy (padding : Row) (source : List Row) (size : Nat) : List Row :=
  source.take size ++ List.replicate (size - source.length) padding

theorem resized_copy_has_requested_length (padding : Row) (source : List Row) (size : Nat) :
    (resizedCopy padding source size).length = size := by
  simp [resizedCopy]
  omega

theorem resized_copy_keeps_visible_prefix (padding : Row) (source : List Row)
    (count size : Nat) (valid : count ≤ source.length) (fits : count ≤ size) :
    (resizedCopy padding source size).take count = source.take count := by
  have within : count ≤ (source.take size).length := by
    rw [List.length_take]
    omega
  rw [resizedCopy, List.take_append_of_le_length within, List.take_take, Nat.min_eq_left fits]

def copyBatch (destination : List Row) (start : Nat) (rows : List Row) : List Row :=
  destination.take start ++ rows ++ destination.drop (start + rows.length)

theorem bounded_batch_matches_visible_append (padding : Row) (source added : List Row)
    (count : Nat) (valid : count ≤ source.length) :
    copyBatch (resizedCopy padding source (count + added.length)) count added =
      source.take count ++ added := by
  have prefixEq := resized_copy_keeps_visible_prefix padding source count (count + added.length)
    valid (Nat.le_add_right _ _)
  have atEnd : (resizedCopy padding source (count + added.length)).length ≤ count + added.length := by
    simp [resized_copy_has_requested_length]
  simp only [copyBatch, prefixEq, List.drop_eq_nil_of_le atEnd, List.append_nil]

theorem single_store_matches_visible_append (padding row : Row) (source : List Row)
    (count : Nat) (valid : count ≤ source.length) :
    (resizedCopy padding source (count + 1)).set count row = source.take count ++ [row] := by
  have within : count < (resizedCopy padding source (count + 1)).length := by
    simp [resized_copy_has_requested_length]
  rw [List.set_eq_take_append_cons_drop, ite_eq_left within]
  simpa [copyBatch] using bounded_batch_matches_visible_append padding source [row] count valid

def sharedAppend (buffers : List (List Row)) (parent : LogRef) (row : Row) :
    List (List Row) × LogRef :=
  let old := buffers[parent.buffer]?.getD []
  let updated := old.set parent.count row
  (buffers.set parent.buffer updated, ⟨parent.buffer, parent.count + 1⟩)

theorem an_in_place_sibling_overwrites_a_retained_log :
    let base : List (List Nat) := [[0]]
    let root : LogRef := ⟨0, 0⟩
    let first := sharedAppend base root 7
    let second := sharedAppend first.1 root 9
    read first.1 first.2 = [7] ∧ read second.1 first.2 = [9] := by
  decide

#print axioms child_reads_its_parent_prefix_and_added_rows
#print axioms fork_preserves_every_retained_log
#print axioms fork_history_preserves_every_retained_log
#print axioms bounded_batch_matches_visible_append
#print axioms single_store_matches_visible_append
#print axioms an_in_place_sibling_overwrites_a_retained_log

end RTDB.JvmTriePendingLogs
