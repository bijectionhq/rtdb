import Std

namespace RTDB.Recovery

structure JournalControl where
  promised : Nat
  retired : Bool
  deriving DecidableEq

inductive JournalEvent where
  | promise (epoch : Nat)
  | drop (epoch : Nat)
  | other

def applyControl (state : JournalControl) (event : JournalEvent) : JournalControl :=
  match event with
  | .promise epoch => { state with promised := max state.promised epoch }
  | .drop epoch => { promised := max state.promised epoch, retired := true }
  | .other => state

def replayControl (initial : JournalControl) (events : List JournalEvent) : JournalControl :=
  events.foldl applyControl initial

structure JournalSnapshot where
  promisedEpoch : Nat
  streamRetired : Bool

def writeSnapshot (state : JournalControl) : JournalSnapshot :=
  ⟨state.promised, state.retired⟩

def readSnapshot (snapshot : JournalSnapshot) : JournalControl :=
  ⟨snapshot.promisedEpoch, snapshot.streamRetired⟩

theorem dropping_a_stream_sets_retirement_and_retains_the_epoch_bound (state : JournalControl)
    (deletionEpoch : Nat) :
    (applyControl state (.drop deletionEpoch)).retired = true ∧
      state.promised ≤ (applyControl state (.drop deletionEpoch)).promised ∧
      deletionEpoch ≤ (applyControl state (.drop deletionEpoch)).promised := by
  exact ⟨rfl, Nat.le_max_left _ _, Nat.le_max_right _ _⟩

theorem replay_cannot_reopen_a_retired_journal (initial : JournalControl) (events : List JournalEvent)
    (retired : initial.retired = true) : (replayControl initial events).retired = true := by
  induction events generalizing initial with
  | nil => exact retired
  | cons event events ih =>
    change (replayControl (applyControl initial event) events).retired = true
    apply ih
    cases event <;> simp [applyControl, retired]

theorem compacting_and_replaying_preserves_the_control_state (initial : JournalControl)
    (before after : List JournalEvent) :
    replayControl (readSnapshot (writeSnapshot (replayControl initial before))) after =
      replayControl initial (before ++ after) := by
  have roundTrip (state : JournalControl) : readSnapshot (writeSnapshot state) = state := by
    cases state
    rfl
  simp [roundTrip, replayControl, List.foldl_append]

def allRetired (addressed : List Replica) (settled : Replica → Bool) : Bool :=
  addressed.all settled

theorem retirement_completion_covers_every_addressed_replica (addressed : List Replica)
    (settled : Replica → Bool) (complete : allRetired addressed settled = true) :
    ∀ replica ∈ addressed, settled replica = true :=
  List.all_eq_true.mp complete

theorem a_majority_does_not_establish_retirement_of_all_replicas :
    let replicas : List Nat := [0, 1, 2]
    let settled := fun replica => decide (replica < 2)
    replicas.Nodup ∧ replicas.countP settled > 1 ∧ allRetired replicas settled = false := by
  decide

#print axioms dropping_a_stream_sets_retirement_and_retains_the_epoch_bound
#print axioms replay_cannot_reopen_a_retired_journal
#print axioms compacting_and_replaying_preserves_the_control_state
#print axioms retirement_completion_covers_every_addressed_replica
#print axioms a_majority_does_not_establish_retirement_of_all_replicas

end RTDB.Recovery
