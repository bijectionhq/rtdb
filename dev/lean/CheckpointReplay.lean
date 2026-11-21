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
