import Logkeeper.Protocol

set_option warningAsError true

namespace RTDB.Recovery.NativeAppend

open Logkeeper

structure ChosenRecord where
  resultPosition : Nat
  attempt : Protocol.Attempt
  requestFingerprint : Nat
  outcomeFingerprint : Nat
  throughNext : Nat
  throughDigest : Nat
  expiryNext : Nat
  expiryDigest : Nat
  chargeClass : Protocol.ChargeClass
  byteStart : Nat
  byteEnd : Nat

def chosenResultAt (resultPosition decisionPosition : Nat) : Bool :=
  resultPosition == decisionPosition

def matchesPendingAppend (resultClass pendingClass : Protocol.ChargeClass)
    (lsn expectedNext writerNext : Nat) : Bool :=
  resultClass == pendingClass && lsn == expectedNext && lsn == writerNext

def nativeAppend (record : ChosenRecord) : Protocol.Append :=
  ⟨⟨record.attempt, record.requestFingerprint, record.outcomeFingerprint,
    ⟨record.attempt.placement, record.throughNext, record.throughDigest⟩,
    ⟨record.attempt.placement, record.expiryNext, record.expiryDigest⟩⟩,
    ⟨record.chargeClass, record.byteEnd - record.byteStart⟩⟩

inductive Body (Tx : Type) where
  | resolvedTx (tx : Tx)
  | stagedTx (tx : Tx)
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
  | some (.resolvedTx tx) | some (.stagedTx tx) => some tx
  | _ => none

structure JournalKey where
  placement : Protocol.Placement
  generation : Nat
  deriving DecidableEq

structure ChosenPayload where
  generation : Nat
  ciphertext : Nat
  deriving DecidableEq

def recordedKey (append : Protocol.Append) (payload : ChosenPayload) : JournalKey :=
  ⟨append.receipt.attempt.placement, payload.generation⟩

structure DecodeSource (Tx : Type) where
  chosenPayload : Protocol.Append → Option ChosenPayload
  openPayload : JournalKey → Nat → Option Nat
  decode : Nat → Option (Frame Tx)
  detachedBytes : Protocol.Append → Option Nat
  sha256 : Nat → Nat

def resolveDetached (source : DecodeSource Tx) (proxy : Frame Tx) (digest : Option Nat)
    (bytes : Nat) : Option (Frame Tx) := do
  if digest.any (· != source.sha256 bytes) then none else
    let payload ← source.decode bytes
    if payload.term != proxy.term || payload.termSequence != proxy.termSequence then none else
      match payload.body with
      | some (.resolvedTx _) | some .control => some payload
      | _ => none

def DecodeSource.openedFrame (source : DecodeSource Tx) (append : Protocol.Append) :
    Option (Frame Tx) := do
  let payload ← source.chosenPayload append
  let bytes ← source.openPayload (recordedKey append payload) payload.ciphertext
  source.decode bytes

def DecodeSource.frameFor (source : DecodeSource Tx) (append : Protocol.Append) :
    Option (Frame Tx) := do
  let frame ← source.openedFrame append
  match frame.body with
  | some (.oversized digest) =>
    let bytes ← source.detachedBytes append
    resolveDetached source frame digest bytes
  | _ => some frame

def decodeAppend (source : DecodeSource Tx) (append : Protocol.Append) : Option Tx :=
  (source.frameFor append).bind decodedEvent

inductive Delivery (source : DecodeSource Tx) (append : Protocol.Append) (plaintext : Nat) : Tx → Prop
  | inline : source.decode plaintext = some (decodeFrame term sequence (some (.resolvedTx tx))) →
      Delivery source append plaintext tx
  | staged : source.decode plaintext = some (decodeFrame term sequence (some (.stagedTx tx))) →
      Delivery source append plaintext tx
  | detached : source.decode plaintext = some (decodeFrame term sequence (some (.oversized digest))) →
      source.detachedBytes append = some bytes →
      digest.any (· != source.sha256 bytes) = false →
      source.decode bytes = some (decodeFrame term sequence (some (.resolvedTx tx))) →
      Delivery source append plaintext tx

structure Binding (source : DecodeSource Tx) (append : Protocol.Append) (tx : Tx) where
  payload : ChosenPayload
  selected : source.chosenPayload append = some payload
  plaintext : Nat
  decrypted : source.openPayload (recordedKey append payload) payload.ciphertext = some plaintext
  delivery : Delivery source append plaintext tx

theorem openedFrame_of_chosen_payload (binding : Binding source append tx) :
    source.openedFrame append = source.decode binding.plaintext := by
  simp [DecodeSource.openedFrame, binding.selected, binding.decrypted]

theorem resolveDetached_preserves_body (source : DecodeSource Tx)
    (decoded : source.decode bytes = some (decodeFrame term sequence (some (.resolvedTx tx))))
    (digestMatches : digest.any (· != source.sha256 bytes) = false) :
    resolveDetached source (decodeFrame term sequence (some (.oversized digest))) digest bytes =
      some (decodeFrame term sequence (some (.resolvedTx tx))) := by
  simp [resolveDetached, digestMatches, decoded, decodeFrame]

theorem decodedAcknowledgement (binding : Binding source append tx) :
    decodeAppend source append = some tx := by
  unfold decodeAppend DecodeSource.frameFor
  rw [openedFrame_of_chosen_payload binding]
  cases binding.delivery with
  | inline frame | staged frame => simp [frame, decodeFrame, decodedEvent]
  | detached proxy fetched digest inner =>
    simp [proxy, fetched, resolveDetached, digest, inner, decodeFrame, decodedEvent]

theorem decode_frame_preserves_resolved_event (term : Option Int) (sequence : Option Int) (tx : Tx) :
    decodedEvent (decodeFrame term sequence (some (.resolvedTx tx))) = some tx := rfl

#print axioms openedFrame_of_chosen_payload
#print axioms resolveDetached_preserves_body
#print axioms decodedAcknowledgement
#print axioms decode_frame_preserves_resolved_event

end RTDB.Recovery.NativeAppend
