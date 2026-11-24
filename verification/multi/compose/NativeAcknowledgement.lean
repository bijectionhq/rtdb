set_option warningAsError true

namespace RTDB.Recovery

structure NativeRecordIdentity where
  term : Int
  sequence : Option Int
  fingerprint : Option (List UInt8)
  deriving DecidableEq

def matchingPresentInteger (observed : Option Int) (expected : Int) : Bool :=
  decide (observed = some expected)

def ownReadbackTerm (leader : Option Int) (observed : Int) : Bool :=
  matchingPresentInteger leader observed

def matchingSequence (appended observed : Option Int) : Bool :=
  match appended with
  | none => false
  | some expected => matchingPresentInteger observed expected

def readbackMatchesTransaction (resolved : Bool) (appended observed : Option (List UInt8)) : Bool :=
  resolved && (match appended, observed with
    | some expected, some actual => decide (expected = actual)
    | _, _ => false)

def ExactReadback (appended observed : NativeRecordIdentity) (resolved : Bool) : Bool :=
  ownReadbackTerm (some appended.term) observed.term &&
    matchingSequence appended.sequence observed.sequence &&
    readbackMatchesTransaction resolved appended.fingerprint observed.fingerprint

inductive TransactionEvent (Result Error : Type) where
  | enqueued (staged : Result)
  | readbackFailed (error : Error)
  | applyFinished (outcome : Except Error Result)

def transactionCompletion : TransactionEvent Result Error → Option (Except Error Result)
  | .enqueued _ => none
  | .readbackFailed error => some (.error error)
  | .applyFinished outcome => some outcome

inductive NativeEvent (Result Error : Type) where
  | enqueued (staged : Result)
  | readBack (observed : NativeRecordIdentity) (resolved : Bool) (mismatch : Error)
      (applied : Option (Except Error Result))

def nativeAcknowledgement (appended : NativeRecordIdentity) :
    NativeEvent Result Error → Option (Except Error Result)
  | .enqueued staged => transactionCompletion (.enqueued staged)
  | .readBack observed resolved mismatch applied =>
    if ownReadbackTerm (some appended.term) observed.term &&
        matchingSequence appended.sequence observed.sequence then
      if readbackMatchesTransaction resolved appended.fingerprint observed.fingerprint then
        applied.bind fun outcome => transactionCompletion (.applyFinished outcome)
      else
        transactionCompletion (.readbackFailed mismatch)
    else
      none

theorem enqueued_does_not_acknowledge (appended : NativeRecordIdentity) (staged : Result) :
    nativeAcknowledgement (Error := Error) appended (.enqueued staged) = none := rfl

theorem same_key_mismatch_fails_pending_write
    (sameTerm : ownReadbackTerm (some appended.term) observed.term = true)
    (sameSequence : matchingSequence appended.sequence observed.sequence = true)
    (mismatched : readbackMatchesTransaction resolved appended.fingerprint observed.fingerprint = false)
    (applied : Option (Except Error Result)) (mismatch : Error) :
    nativeAcknowledgement appended (.readBack observed resolved mismatch applied) = some (.error mismatch) := by
  simp [nativeAcknowledgement, sameTerm, sameSequence, mismatched, transactionCompletion]

theorem native_acknowledgement_refines_applied_record
    (applied : Option (Except Error Result)) (mismatch : Error) (result : Result) :
    nativeAcknowledgement appended (.readBack observed resolved mismatch applied) = some (.ok result) ↔
      ExactReadback appended observed resolved = true ∧ applied = some (.ok result) := by
  by_cases term : ownReadbackTerm (some appended.term) observed.term = true <;>
    by_cases seq : matchingSequence appended.sequence observed.sequence = true <;>
    by_cases payload : readbackMatchesTransaction resolved appended.fingerprint observed.fingerprint = true <;>
    cases applied <;> simp_all [nativeAcknowledgement, ExactReadback, transactionCompletion]

private def appended : NativeRecordIdentity :=
  ⟨1, some 2, some (List.replicate 32 0)⟩

private def otherPayload : NativeRecordIdentity :=
  { appended with fingerprint := some (1 :: List.replicate 31 0) }

theorem exact_readback_and_application_acknowledge_the_applied_result :
    nativeAcknowledgement appended (.readBack appended true false (some (.ok 42))) = some (.ok 42) := rfl

theorem enqueue_only_acknowledgement_is_invalid :
    let staged := 17
    nativeAcknowledgement (Error := Bool) appended (.enqueued staged) ≠ some (.ok staged) := by
  simp [nativeAcknowledgement, transactionCompletion]

theorem fingerprint_blind_acknowledgement_is_invalid :
    nativeAcknowledgement appended (.readBack otherPayload true false (some (.ok 42))) = some (.error false) ∧
      ExactReadback appended otherPayload true = false := ⟨rfl, rfl⟩

theorem foreign_term_acknowledgement_is_invalid :
    nativeAcknowledgement appended (.readBack { appended with term := 9 } true false (some (.ok 42))) = none := by decide

theorem wrong_sequence_acknowledgement_is_invalid :
    nativeAcknowledgement appended (.readBack { appended with sequence := some 3 } true false (some (.ok 42))) = none := by decide

theorem staged_result_substitution_is_invalid :
    nativeAcknowledgement appended (.readBack appended true false (some (.ok 42))) ≠ some (.ok 17) := by
  intro substituted
  cases substituted

#print axioms enqueued_does_not_acknowledge
#print axioms same_key_mismatch_fails_pending_write
#print axioms native_acknowledgement_refines_applied_record

end RTDB.Recovery
