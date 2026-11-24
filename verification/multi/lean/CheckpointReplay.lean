import Std

namespace RTDB.Recovery

theorem replay_suffix_from_checkpoint
    (applyEvent : State → Event → State) (initial : State) (history : List Event)
    (checkpoint basis : Nat) (checkpointBeforeBasis : checkpoint ≤ basis) :
    ((history.take basis).drop checkpoint).foldl applyEvent
        ((history.take checkpoint).foldl applyEvent initial) =
      (history.take basis).foldl applyEvent initial := by
  rw [← List.foldl_append]
  have atCheckpoint : (history.take basis).take checkpoint = history.take checkpoint := by
    rw [List.take_take, Nat.min_eq_left checkpointBeforeBasis]
  rw [← atCheckpoint, List.take_append_drop]

/-- A renewed hold preserves the current cut. Failed delivery and cancellation
also preserve it. Successful delivery consumes one original journal record. -/
inductive CursorReplay (history : List Event) : Nat → Nat → List Event → Prop where
  | stop (cut : Nat) : CursorReplay history cut cut []
  | retry : CursorReplay history start through delivered → CursorReplay history start through delivered
  | deliver (original : history[start]? = some event)
      (rest : CursorReplay history (start + 1) through delivered) :
      CursorReplay history start through (event :: delivered)

theorem cursor_replay_covers_exact_segment
    (replay : CursorReplay history start through delivered) :
    start ≤ through ∧ delivered.length = through - start ∧
      delivered ++ history.drop through = history.drop start := by
  induction replay with
  | stop => simp
  | retry _ ih => exact ih
  | @deliver position event _ _ original _ ih =>
    obtain ⟨present, same⟩ := List.getElem?_eq_some_iff.mp original
    obtain ⟨ordered, count, suffix⟩ := ih
    refine ⟨by omega, by simp only [List.length_cons]; omega, ?_⟩
    rw [List.cons_append, suffix, List.drop_eq_getElem_cons present, same]

theorem certified_checkpoint_with_retried_delivery
    (applyEvent : State → Event → State) (initial : State)
    (replay : CursorReplay history checkpoint basis delivered) :
    delivered.foldl applyEvent ((history.take checkpoint).foldl applyEvent initial) =
      (history.take basis).foldl applyEvent initial := by
  obtain ⟨ordered, count, suffix⟩ := cursor_replay_covers_exact_segment replay
  have exactSegment : delivered = (history.take basis).drop checkpoint := by
    have selected := congrArg (List.take (basis - checkpoint)) suffix
    rw [List.take_drop, Nat.add_sub_of_le ordered] at selected
    rw [← count, List.take_append_of_le_length (Nat.le_refl _), List.take_length] at selected
    exact selected
  rw [exactSegment]
  exact replay_suffix_from_checkpoint applyEvent initial history checkpoint basis ordered

def snapshotAt (applyEvent : State → Event → State) (initial : State)
    (checkpointHistory : List Event) (basis : Nat) : Option State :=
  if basis ≤ checkpointHistory.length then
    some ((checkpointHistory.take basis).foldl applyEvent initial)
  else
    none

theorem snapshot_reply_is_at_the_requested_basis
    (applyEvent : State → Event → State) (initial : State)
    (checkpointHistory : List Event) (basis : Nat) (result : State)
    (reply : snapshotAt applyEvent initial checkpointHistory basis = some result) :
    basis ≤ checkpointHistory.length ∧
      result = (checkpointHistory.take basis).foldl applyEvent initial := by
  unfold snapshotAt at reply
  split at reply <;> simp_all

#print axioms replay_suffix_from_checkpoint
#print axioms cursor_replay_covers_exact_segment
#print axioms certified_checkpoint_with_retried_delivery
#print axioms snapshot_reply_is_at_the_requested_basis

end RTDB.Recovery

namespace RTDB.Recovery

inductive ReplayEvent (Tx Notice : Type) where
  | transaction (tx : Tx)
  | compaction (notice : Notice)

def replayRestored (cutoff : Nat) :
    List (Nat × ReplayEvent Tx Notice) → List (Nat × ReplayEvent Tx Notice)
  | [] => []
  | (position, event) :: rest =>
    let later := replayRestored cutoff rest
    match event with
    | .transaction _ => (position, event) :: later
    | .compaction _ => if position > cutoff then (position, event) :: later else later

def logicalApply (applyTx : State → Tx → State) (state : State)
    (entry : Nat × ReplayEvent Tx Notice) : State :=
  match entry.2 with
  | .transaction tx => applyTx state tx
  | .compaction _ => state

theorem restored_replay_preserves_transaction_history
    (applyTx : State → Tx → State) (initial : State)
    (cutoff : Nat) (suffix : List (Nat × ReplayEvent Tx Notice)) :
    (replayRestored cutoff suffix).foldl (logicalApply applyTx) initial =
      suffix.foldl (logicalApply applyTx) initial := by
  induction suffix generalizing initial with
  | nil => rfl
  | cons entry rest ih =>
    rcases entry with ⟨position, event⟩
    cases event with
    | transaction tx =>
      simp [replayRestored, logicalApply, ih]
    | compaction notice =>
      by_cases permitted : position > cutoff
      · simp [replayRestored, logicalApply, permitted, ih]
      · simp [replayRestored, logicalApply, permitted, ih]

theorem replay_keeps_a_new_compaction (cutoff position : Nat) (notice : Notice)
    (newer : cutoff < position) :
    replayRestored cutoff [(position, ReplayEvent.compaction (Tx := Tx) notice)] =
      [(position, ReplayEvent.compaction (Tx := Tx) notice)] := by
  simp [replayRestored, newer]

#print axioms restored_replay_preserves_transaction_history
#print axioms replay_keeps_a_new_compaction

end RTDB.Recovery
