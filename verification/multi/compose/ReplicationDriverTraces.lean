import ReplicationDriver

set_option warningAsError true

namespace RTDB.Recovery.ReplicationDriver

def execute (s : State) (actions : List Action) : Except Reject State :=
  match actions with
  | [] => .ok s
  | action :: rest => do
    let effect ← step s action
    execute effect.state rest

theorem execute_sound (ran : execute s actions = .ok t) : Run s actions t := by
  induction actions generalizing s with
  | nil =>
    injection ran with same
    subst t
    exact .nil s
  | cons action rest ih =>
    cases first : step s action with
    | error => simp [execute, first, bind, Except.bind] at ran
    | ok effect =>
      exact .cons first (ih (by simpa [execute, first, bind, Except.bind] using ran))

private def payload : List UInt8 := List.replicate 32 42
private def identity : NativeRecordIdentity := ⟨1, some 0, some payload⟩

private def flight : List Action :=
  [.worker .emptyRead, .worker (.observeGrant 0), .worker (.promote 1), .worker (.enqueue 0 17),
    .dispatch 0 1 0 payload false]

private def chooseFirst : Action := .choose 0 0 ⟨1, 1⟩ 8 9 10 30 31 2 .ordinary
private def exact : List Action := flight ++ [chooseFirst, .worker (.readBack identity true)]
private def applied : List Action := exact ++ [.worker .applyBegin, .worker (.applyFinished 17)]
private def ack : Action := .worker (.acknowledge 0 17)

theorem writer_replacement_mid_append_is_reachable :
    (execute {} (flight ++ [.replace 11, .worker (.providerFault 0), .worker .termLoss])).map
      (fun s => (s.journal.writer, s.journal.head.next, s.worker.channels 0, s.worker.owner.isNone)) =
      .ok (1, 0, .spent, true) := rfl

theorem readback_mismatch_is_reachable :
    (execute {} (flight ++ [chooseFirst,
      .worker (.readBack {identity with fingerprint := some (List.replicate 32 43)} true)])).map
      (fun s => (s.journal.head.next, s.worker.cursor, s.worker.halted, s.worker.channels 0)) =
      .ok (1, 0, true, .spent) := rfl

theorem durability_replies_can_reorder :
    (execute {} (flight ++ [
      .worker (.appendReturned 0), .worker (.enqueue 1 18),
      .dispatch 1 1 1 payload false, .worker (.appendReturned 1),
      chooseFirst, .choose 1 1 ⟨1, 1⟩ 12 13 14 30 31 2 .ordinary,
      .worker (.durable 1 2), .worker (.durable 0 1)])).map
      (fun s => (s.journal.head.next, s.worker.returned 0, s.worker.returned 1)) =
      .ok (2, false, false) := rfl

theorem a_chosen_append_can_replay_after_lost_reply :
    (execute {} (flight ++ [chooseFirst, .worker .crash, .replace 11, .worker (.replay identity),
      .worker .applyBegin, .worker (.applyFinished 17), .worker .advance, .worker .emptyRead,
      .worker (.observeGrant 1), .worker (.promote 2)])).map
      (fun s => (s.worker.cursor, s.worker.owner.map (·.term), s.worker.channels 0)) =
      .ok (1, some 2, .spent) := rfl

theorem successful_readback_routes_before_provider_reply :
    (execute {} (applied ++ [ack, .worker .advance])).map
      (fun s => (s.worker.cursor, s.worker.enqueueing, s.worker.channels 0)) =
      .ok (1, some 0, .spent) := rfl

theorem choice_can_outlive_the_retry_receipt_at_acknowledgement :
    (execute {} (exact ++ [.expire 30, .worker .applyBegin, .worker (.applyFinished 17), ack])).map
      (fun s => (s.journal.head.next, (s.journal.receipts ⟨0, 0, 30⟩).isNone, s.worker.channels 0)) =
      .ok (1, true, .spent) := rfl

theorem acknowledge_before_apply_rejected :
    (execute {} (exact ++ [ack])).map (fun _ => ()) = .error .acknowledgeAfterApply := rfl

theorem reuse_superseded_writer_rejected :
    (execute {} (flight ++ [.replace 11, chooseFirst])).map (fun _ => ()) = .error .staleWriter := rfl

theorem route_twice_rejected :
    (execute {} (applied ++ [ack, ack])).map (fun _ => ()) = .error .routeOnce := rfl

#print axioms execute_sound
#print axioms writer_replacement_mid_append_is_reachable
#print axioms readback_mismatch_is_reachable
#print axioms durability_replies_can_reorder
#print axioms a_chosen_append_can_replay_after_lost_reply
#print axioms successful_readback_routes_before_provider_reply
#print axioms choice_can_outlive_the_retry_receipt_at_acknowledgement
#print axioms acknowledge_before_apply_rejected
#print axioms reuse_superseded_writer_rejected
#print axioms route_twice_rejected

end RTDB.Recovery.ReplicationDriver
