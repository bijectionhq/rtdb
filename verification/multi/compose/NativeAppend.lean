import Logkeeper.Protocol

set_option warningAsError true

namespace RTDB.Recovery.NativeAppend

open Logkeeper

structure ChosenRecord where
  attempt : Protocol.Attempt
  requestFingerprint : Nat
  outcomeFingerprint : Nat
  through : Protocol.DataCut
  expiry : Protocol.DecisionCut
  chargeClass : Protocol.ChargeClass
  chargedBytes : Nat

def nativeAppend (record : ChosenRecord) : Protocol.Append :=
  ⟨⟨record.attempt, record.requestFingerprint, record.outcomeFingerprint, record.through, record.expiry⟩,
    ⟨record.chargeClass, record.chargedBytes⟩⟩

inductive Body (Tx : Type) where
  | resolvedTx (tx : Tx)
  | control
  | oversized (fingerprint : Option Nat)
  deriving DecidableEq

structure Frame (Tx : Type) where
  term : Int
  termSequence : Option Int
  body : Option (Body Tx)
  deriving DecidableEq

def decodeFrame (term : Option Int) (sequence : Option Int) (body : Option (Body Tx)) : Frame Tx :=
  ⟨term.getD 0, sequence, body⟩

def decodedEvent (frame : Frame Tx) : Option Tx :=
  match frame.body with
  | some (.resolvedTx tx) => some tx
  | _ => none

def decodeAppend (frameFor : Protocol.Append → Option (Frame Tx)) (append : Protocol.Append) : Option Tx :=
  (frameFor append).bind decodedEvent

/-- Binds the exact chosen journal record to its opened and resolved native frame. -/
structure Binding (frameFor : Protocol.Append → Option (Frame Tx)) (append : Protocol.Append) (tx : Tx) where
  record : ChosenRecord
  chosen : nativeAppend record = append
  frame : Frame Tx
  opened : frameFor (nativeAppend record) = some frame
  resolved : frame.body = some (.resolvedTx tx)

theorem decodedAcknowledgement (binding : Binding frameFor append tx) :
    decodeAppend frameFor append = some tx := by
  rw [← binding.chosen, decodeAppend, binding.opened]
  simp [decodedEvent, binding.resolved]

theorem decode_frame_preserves_resolved_event (term : Option Int) (sequence : Option Int) (tx : Tx) :
    decodedEvent (decodeFrame term sequence (some (.resolvedTx tx))) = some tx := rfl

#print axioms decodedAcknowledgement
#print axioms decode_frame_preserves_resolved_event

end RTDB.Recovery.NativeAppend
