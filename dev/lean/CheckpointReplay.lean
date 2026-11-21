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
