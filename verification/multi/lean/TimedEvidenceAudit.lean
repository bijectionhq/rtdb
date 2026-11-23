import Std

/-!
Clock evidence and original voter lookup for a timed guarded replica record.

The sealed acceptance stage is inside the deployment seal. The journal frame
carries the cutoff and receiver clock certificate digest outside it. The writer
derives the frame from the sealed stage, and the exact-record audit reports the
clock evidence only when the frame repeats it.

A checkpoint transfer reseals the payload under the target scope. Archive
capture first records the original stream's digest of the exact appended entry,
so the audit can still address the original voters. The retained chain is
modelled as an injective digest of the entry's fields.
-/

namespace RTDB.TimedEvidence

/-- A physically bounded acceptance's exclusive journal cutoff and certificate digest. -/
structure ClockEvidence where
  cutoff : Nat
  certificate : Nat
  deriving DecidableEq

/-- The writer's guarded append takes its frame coordinates from the sealed stage. -/
def writerFrame (sealed : Option ClockEvidence) : Option ClockEvidence := sealed

/-- The audit reports the sealed evidence, and refuses a frame that differs from it. -/
def auditClockEvidence (sealed framed : Option ClockEvidence) : Option (Option ClockEvidence) :=
  if framed = sealed then some sealed else none

theorem reported_evidence_is_the_sealed_decision (sealed framed : Option ClockEvidence)
    (report : Option ClockEvidence) (audited : auditClockEvidence sealed framed = some report) :
    report = sealed ∧ framed = sealed := by
  unfold auditClockEvidence at audited
  split at audited
  · rename_i same
    exact ⟨(Option.some.inj audited).symm, same⟩
  · cases audited

theorem a_substituted_frame_is_refused (sealed framed : Option ClockEvidence)
    (differs : framed ≠ sealed) : auditClockEvidence sealed framed = none := by
  simp [auditClockEvidence, differs]

theorem every_writer_record_is_auditable (sealed : Option ClockEvidence) :
    auditClockEvidence sealed (writerFrame sealed) = some sealed := by
  simp [auditClockEvidence, writerFrame]

/-- The single-entry retained chain over the original stream, as an injective digest. -/
structure Link where
  stream : Nat
  lsn : Nat
  payload : Nat
  evidence : ClockEvidence
  carried : Bool
  deriving DecidableEq

/-- A retained replica entry. `carried` is the original stream's digest, empty on the original append. -/
structure Entry where
  stream : Nat
  lsn : Nat
  payload : Nat
  evidence : ClockEvidence
  carried : Option Link
  deriving DecidableEq

def link (entry : Entry) : Link :=
  ⟨entry.stream, entry.lsn, entry.payload, entry.evidence, entry.carried.isSome⟩

/-- The digest the original voters hold for this record. -/
def originalDigest (entry : Entry) : Link :=
  match entry.carried with
  | some original => original
  | none => link entry

/-- Archive capture records the original digest before the payload is opened. -/
def retain (entry : Entry) : Entry := { entry with carried := some (originalDigest entry) }

/-- Checkpoint transfer reseals the payload under the target scope. -/
def reseal (payload : Nat) (entry : Entry) : Entry := { entry with payload := payload }

/-- A logkeeper admits an original timed append only without a carried digest. -/
def admitsOriginal (entry : Entry) : Bool := entry.carried.isNone

theorem an_admitted_original_holds_its_own_link (entry : Entry)
    (admitted : admitsOriginal entry = true) : originalDigest entry = link entry := by
  cases carried : entry.carried with
  | none => simp [originalDigest, carried]
  | some _ => simp [admitsOriginal, carried] at admitted

theorem a_resealed_copy_still_names_its_original_voters (original : Entry) (payload : Nat)
    (admitted : admitsOriginal original = true) :
    originalDigest (reseal payload (retain original)) = link original := by
  have empty : original.carried = none := by simpa [admitsOriginal] using admitted
  simp [reseal, retain, originalDigest, empty]

theorem successive_transfers_keep_the_first_original (original : Entry) (first second : Nat)
    (admitted : admitsOriginal original = true) :
    originalDigest (reseal second (retain (reseal first (retain original)))) = link original := by
  have empty : original.carried = none := by simpa [admitsOriginal] using admitted
  simp [reseal, retain, originalDigest, empty]

theorem resealing_without_capture_loses_the_original_voters (original : Entry) (payload : Nat)
    (admitted : admitsOriginal original = true) (changed : payload ≠ original.payload) :
    originalDigest (reseal payload original) ≠ link original := by
  have empty : original.carried = none := by simpa [admitsOriginal] using admitted
  simp [reseal, originalDigest, link, empty, changed]

/-- A voter answers only for its own held original entry. -/
def voterAnswers (held : Entry) (requested : Link) : Bool :=
  admitsOriginal held && decide (link held = requested)

theorem an_answering_voter_holds_the_exact_original (held original : Entry) (payload : Nat)
    (admitted : admitsOriginal original = true)
    (answers : voterAnswers held (originalDigest (reseal payload (retain original))) = true) :
    held.payload = original.payload ∧ held.lsn = original.lsn ∧ held.evidence = original.evidence := by
  have target := a_resealed_copy_still_names_its_original_voters original payload admitted
  simp only [voterAnswers, Bool.and_eq_true, decide_eq_true_eq, target] at answers
  have same := answers.2
  simp only [link, Link.mk.injEq] at same
  exact ⟨same.2.2.1, same.2.1, same.2.2.2.1⟩

theorem timed_evidence_program :
    auditClockEvidence (some ⟨150, 7⟩) (some ⟨150, 7⟩) = some (some ⟨150, 7⟩) ∧
    auditClockEvidence (some ⟨150, 7⟩) (some ⟨150, 8⟩) = none ∧
    auditClockEvidence none (some ⟨150, 7⟩) = none ∧
    auditClockEvidence none none = some none := by decide

#print axioms reported_evidence_is_the_sealed_decision
#print axioms a_substituted_frame_is_refused
#print axioms every_writer_record_is_auditable
#print axioms an_admitted_original_holds_its_own_link
#print axioms a_resealed_copy_still_names_its_original_voters
#print axioms successive_transfers_keep_the_first_original
#print axioms resealing_without_capture_loses_the_original_voters
#print axioms an_answering_voter_holds_the_exact_original
#print axioms timed_evidence_program

end RTDB.TimedEvidence
