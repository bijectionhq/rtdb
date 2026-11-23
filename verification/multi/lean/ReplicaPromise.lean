import Quorum

namespace RTDB.Recovery

def replayPromises (initial : Nat) (events : List Nat) : Nat := events.foldl max initial

private theorem replay_never_lowers (initial : Nat) (events : List Nat) :
    initial ≤ replayPromises initial events := by
  induction events generalizing initial with
  | nil => exact Nat.le_refl _
  | cons next rest ih =>
    exact Nat.le_trans (Nat.le_max_left _ _) (ih (max initial next))

theorem a_seal_promise_outlives_later_events (initial sealEpoch : Nat) (laterEvents : List Nat) :
    sealEpoch ≤ replayPromises initial (sealEpoch :: laterEvents) :=
  Nat.le_trans (Nat.le_max_right _ _) (replay_never_lowers (max initial sealEpoch) laterEvents)

def writerAdmitted (promised : Nat) (synced : Option Nat) (dropped : Bool) (writer : Nat) : Bool :=
  decide (dropped = false ∧ promised = writer ∧ synced = some writer)

theorem a_stale_writer_cannot_append_after_a_seal (initial sealEpoch writer : Nat)
    (laterEvents : List Nat) (synced : Option Nat) (dropped : Bool) (stale : writer < sealEpoch) :
    writerAdmitted (replayPromises initial (sealEpoch :: laterEvents)) synced dropped writer = false := by
  have durable := a_seal_promise_outlives_later_events initial sealEpoch laterEvents
  simp [writerAdmitted, Nat.ne_of_gt (Nat.lt_of_lt_of_le stale durable)]

theorem a_seal_majority_excludes_a_later_stale_writer (replicas : List Replica)
    (sealed : Replica → Bool) (promised : Replica → Nat) (laterEvents : Replica → List Nat)
    (synced : Replica → Option Nat) (dropped : Replica → Bool) (sealEpoch writer : Nat)
    (stale : writer < sealEpoch) (sealMajority : HasMajority replicas sealed) :
    ¬HasMajority replicas (fun replica => writerAdmitted
      (replayPromises (promised replica) (if sealed replica then sealEpoch :: laterEvents replica else laterEvents replica))
      (synced replica) (dropped replica) writer) := by
  apply a_seal_quorum_excludes_a_later_stale_append_quorum replicas sealed _ sealMajority
  intro replica _ sealedHere
  simp only [sealedHere, ite_true]
  exact a_stale_writer_cannot_append_after_a_seal
    (promised replica) sealEpoch writer (laterEvents replica) (synced replica) (dropped replica) stale

#print axioms a_seal_promise_outlives_later_events
#print axioms a_stale_writer_cannot_append_after_a_seal
#print axioms a_seal_majority_excludes_a_later_stale_writer

end RTDB.Recovery
