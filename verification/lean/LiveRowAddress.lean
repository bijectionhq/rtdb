import Std

namespace RTDB.LiveRowAddress

def chunkStart (chunks : List (List Row)) (index : Nat) : Nat :=
  (chunks.take index).flatten.length

theorem next_chunk_starts_at_exclusive_end (chunks : List (List Row))
    (index : Nat) (present : index < chunks.length) :
    chunkStart chunks (index + 1) = chunkStart chunks index + chunks[index].length := by
  simp only [chunkStart, List.take_succ_eq_append_getElem present, List.flatten_append,
    List.flatten_cons, List.flatten_nil, List.append_nil, List.length_append]

theorem last_start_locates_a_row (chunks : List (List Row)) (row index offset : Nat)
    (inTable : row < chunks.flatten.length) (present : index < chunks.length)
    (relative : chunkStart chunks index + offset = row)
    (last : ∀ next, index < next → next < chunks.length → row < chunkStart chunks next) :
    offset < chunks[index].length := by
  have ending := next_chunk_starts_at_exclusive_end chunks index present
  by_cases successor : index + 1 < chunks.length
  · have beforeNext := last (index + 1) (by omega) successor
    omega
  · have atEnd : index + 1 = chunks.length := by omega
    rw [atEnd] at ending
    have terminal : chunkStart chunks chunks.length = chunks.flatten.length := by
      simp [chunkStart]
    omega

theorem chunk_read_matches_flat_history (chunks : List (List Row)) (index offset : Nat)
    (present : index < chunks.length) (inChunk : offset < chunks[index].length) :
    chunks.flatten[chunkStart chunks index + offset]? = chunks[index][offset]? := by
  induction index generalizing chunks with
  | zero =>
      cases chunks with
      | nil => simp at present
      | cons first rest =>
          simpa [chunkStart, List.flatten_cons] using
            (List.getElem?_append_left (l₂ := rest.flatten) (by simpa using inChunk))
  | succ index smaller =>
      cases chunks with
      | nil => simp at present
      | cons first rest =>
          have restHasChunk : index < rest.length := by simpa using present
          have cut : chunkStart (first :: rest) (index + 1) + offset =
              first.length + (chunkStart rest index + offset) := by
            simp [chunkStart, List.flatten_cons, Nat.add_assoc]
          rw [cut, List.flatten_cons, List.getElem?_append_right (by omega),
            Nat.add_sub_cancel_left]
          exact smaller rest restHasChunk (by simpa using inChunk)

theorem last_start_reads_the_requested_row (chunks : List (List Row)) (row index offset : Nat)
    (inTable : row < chunks.flatten.length) (present : index < chunks.length)
    (relative : chunkStart chunks index + offset = row)
    (last : ∀ next, index < next → next < chunks.length → row < chunkStart chunks next) :
    ∃ value, chunks[index][offset]? = some value ∧ chunks.flatten[row]? = some value := by
  have inChunk := last_start_locates_a_row chunks row index offset inTable present relative last
  have same := chunk_read_matches_flat_history chunks index offset present inChunk
  rw [relative] at same
  exact ⟨chunks[index][offset], by simp [inChunk], same.trans (by simp [inChunk])⟩

inductive Edit : List (List Row) → List (List Row) → Prop where
  | append (chunks : List (List Row)) (rows : List Row) : Edit chunks (chunks ++ [rows])
  | combine (prior : List (List Row)) (left right : List Row) (suffix : List (List Row)) :
      Edit (prior ++ left :: right :: suffix) (prior ++ (left ++ right) :: suffix)

theorem edit_preserves_existing_rows {before after : List (List Row)}
    (edit : Edit before after) : before.flatten <+: after.flatten := by
  cases edit with
  | append chunks rows =>
      exact ⟨rows, by simp⟩
  | combine prior left right suffix =>
      exact ⟨[], by simp [List.flatten_cons, List.append_assoc]⟩

inductive History : List (List Row) → List (List Row) → Prop where
  | unchanged (chunks : List (List Row)) : History chunks chunks
  | next : History before middle → Edit middle after → History before after

theorem edits_preserve_the_captured_prefix {before after : List (List Row)}
    (history : History before after) : before.flatten <+: after.flatten := by
  induction history with
  | unchanged => exact ⟨[], by simp⟩
  | next _ edit prior => exact prior.trans (edit_preserves_existing_rows edit)

theorem retained_ordinal_reads_the_same_row {before after : List (List Row)}
    (history : History before after) (row : Nat) (inSnapshot : row < before.flatten.length) :
    after.flatten[row]? = before.flatten[row]? := by
  obtain ⟨suffix, complete⟩ := edits_preserve_the_captured_prefix history
  rw [← complete]
  exact List.getElem?_append_left inSnapshot

structure ChunkAddress (chunks : List (List Row)) (row : Nat) where
  index : Nat
  offset : Nat
  present : index < chunks.length
  relative : chunkStart chunks index + offset = row
  last : ∀ next, index < next → next < chunks.length → row < chunkStart chunks next

theorem retained_addresses_read_the_same_row {before after : List (List Row)}
    (history : History before after) (row : Nat) (inSnapshot : row < before.flatten.length)
    (oldAddress : ChunkAddress before row) (newAddress : ChunkAddress after row) :
    (before[oldAddress.index]'(oldAddress.present))[oldAddress.offset]? =
      (after[newAddress.index]'(newAddress.present))[newAddress.offset]? := by
  have inLater : row < after.flatten.length := by
    obtain ⟨suffix, complete⟩ := edits_preserve_the_captured_prefix history
    rw [← complete, List.length_append]
    omega
  obtain ⟨_, oldRead, oldFlat⟩ := last_start_reads_the_requested_row before row
    oldAddress.index oldAddress.offset inSnapshot oldAddress.present oldAddress.relative oldAddress.last
  obtain ⟨_, newRead, newFlat⟩ := last_start_reads_the_requested_row after row
    newAddress.index newAddress.offset inLater newAddress.present newAddress.relative newAddress.last
  rw [oldRead, newRead, ← oldFlat, ← newFlat]
  exact (retained_ordinal_reads_the_same_row history row inSnapshot).symm

#print axioms next_chunk_starts_at_exclusive_end
#print axioms last_start_reads_the_requested_row
#print axioms edit_preserves_existing_rows
#print axioms retained_ordinal_reads_the_same_row
#print axioms retained_addresses_read_the_same_row

end RTDB.LiveRowAddress
