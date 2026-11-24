import Logkeeper.Protocol
import OrderedJournal
import InstalledRead
import NativeAppend

set_option warningAsError true

namespace RTDB.Recovery.Composition

open Logkeeper

abbrev Ledger (Voter : Type) := PrefixAgreement.Ledger Voter Protocol.Decision

abbrev projectCut := InstalledRead.projectCut

abbrev projectAttempt := InstalledRead.projectAttempt

def projectReceipt (receipt : Protocol.AppendReceipt) : OrderedJournal.Receipt :=
  ⟨receipt.requestDigest, projectCut receipt.cut⟩

structure RecordedAppend (ledger : Ledger Voter) (state : OrderedJournal.State)
    (position : Nat) where
  certifiedPrefix : Protocol.Seal ledger
  record : Protocol.Append
  selected : certifiedPrefix.decisions[position]? = some (.append record)
  placement : record.receipt.cut.placement = record.receipt.attempt.placement
  expiryPlacement : record.receipt.expiresAt.placement = record.receipt.attempt.placement
  decoded : state.receipts (projectAttempt record.receipt) = some (projectReceipt record.receipt)

structure ChosenAppend (ledger : Ledger Voter) (position : Nat) where
  certifiedPrefix : Protocol.Seal ledger
  nativeRecord : NativeAppend.ChosenRecord
  resultAtDecision : NativeAppend.chosenResultAt nativeRecord.resultPosition (position + 1) = true
  atResult : certifiedPrefix.decisions[nativeRecord.resultPosition - 1]? = some (.append (NativeAppend.nativeAppend nativeRecord))

def ChosenAppend.record (initial : ChosenAppend ledger position) : Protocol.Append :=
  NativeAppend.nativeAppend initial.nativeRecord

theorem ChosenAppend.selected (initial : ChosenAppend ledger position) :
    initial.certifiedPrefix.decisions[position]? = some (.append initial.record) := by
  have same : initial.nativeRecord.resultPosition = position + 1 := by
    simpa [NativeAppend.chosenResultAt] using initial.resultAtDecision
  have index : initial.nativeRecord.resultPosition - 1 = position := by omega
  exact (congrArg (fun i => initial.certifiedPrefix.decisions[i]?) index.symm).trans initial.atResult

def ChosenAppend.recordedState (initial : ChosenAppend ledger position) (previous : OrderedJournal.State) :
    OrderedJournal.State :=
  OrderedJournal.recordAppend previous (projectAttempt initial.record.receipt) (projectReceipt initial.record.receipt)

def chosen_result_projection (initial : ChosenAppend ledger position) (previous : OrderedJournal.State) :
    RecordedAppend ledger (initial.recordedState previous) position :=
  ⟨initial.certifiedPrefix, initial.record, initial.selected, rfl, rfl,
    by simp [ChosenAppend.recordedState, OrderedJournal.recordAppend, OrderedJournal.update]⟩

theorem recorded_append_reissues_while_live
    (initial : RecordedAppend ledger before position)
    (path : OrderedJournal.Trace readers activations before after)
    (live : after.observed < initial.record.receipt.expiresAt.next) :
    OrderedJournal.Reissue after (projectAttempt initial.record.receipt)
      initial.record.receipt.requestDigest = some (projectReceipt initial.record.receipt) :=
  OrderedJournal.repeated_payload_returns_exact_receipt
    (OrderedJournal.live_receipt_survives_recovery initial.decoded path live)

/-- Different choices about the same ordered position give the same outcome.
The RTDB recovery trace preserves its projection throughout its lifetime. -/
theorem recovery_reissues_exact_chosen_append
    {ledger : Ledger Voter} {before after : OrderedJournal.State} {position : Nat}
    (voters : Protocol.Evidence ledger) (initial : RecordedAppend ledger before position)
    (recovered : Protocol.Seal ledger)
    (selected : recovered.decisions[position]? = some (.append result))
    (path : OrderedJournal.Trace readers activations before after)
    (live : after.observed < result.receipt.expiresAt.next) :
    initial.record.receipt = result.receipt ∧
      OrderedJournal.Reissue after (projectAttempt result.receipt) result.receipt.requestDigest =
        some (projectReceipt result.receipt) := by
  have agrees := Protocol.same_chosen_append voters initial.certifiedPrefix recovered initial.selected selected
  refine ⟨congrArg Protocol.Append.receipt agrees, ?_⟩
  rw [← agrees] at live ⊢
  exact recorded_append_reissues_while_live initial path live

theorem recovery_reissues_append_after_cohort_repair
    {oldLedger : Ledger OldVoter} {newLedger : Ledger NewVoter}
    {before after : OrderedJournal.State} {position : Nat}
    (oldVoters : Protocol.Evidence oldLedger) (newVoters : Protocol.Evidence newLedger)
    (transfer : Protocol.Transfer oldLedger newLedger)
    (initial : RecordedAppend oldLedger before position) (recovered : Protocol.Seal newLedger)
    (beforeSeal : initial.certifiedPrefix.decisions.length ≤ transfer.finalSource.decisions.length)
    (afterInstall : transfer.installedBase.decisions.length ≤ recovered.decisions.length)
    (selected : recovered.decisions[position]? = some (.append result))
    (path : OrderedJournal.Trace readers activations before after)
    (live : after.observed < result.receipt.expiresAt.next) :
    initial.record.receipt = result.receipt ∧
      OrderedJournal.Reissue after (projectAttempt result.receipt) result.receipt.requestDigest =
        some (projectReceipt result.receipt) := by
  have agrees := Protocol.same_append_after_transfer oldVoters newVoters transfer initial.certifiedPrefix
    recovered beforeSeal afterInstall initial.selected selected
  refine ⟨congrArg Protocol.Append.receipt agrees, ?_⟩
  rw [← agrees] at live ⊢
  exact recorded_append_reissues_while_live initial path live

structure ReadProjection (Meaning : Type) (history : List Protocol.Decision)
    (state : OrderedJournal.State) where
  seed : Protocol.DataCut
  ordered : InstalledRead.OrderedDataHistory seed history
  barrier : InstalledRead.FreshBarrier seed history
  admissionPath : ∃ activations, OrderedJournal.Trace barrier.readerSlots activations barrier.admitted state
  actual : OrderedJournal.Cut
  admitted : OrderedJournal.CanServe state barrier.hold actual
  stateMeaning : Protocol.DataCut → Meaning
  published : InstalledRead.PublishedCatalog Meaning
  publicationMeaning : published.meaning = stateMeaning published.cut
  capture : InstalledRead.PublishedCatalog Meaning
  captured : InstalledRead.captureAt barrier.cut published = some capture

def ReadProjection.cut (read : ReadProjection Meaning history state) : Protocol.DataCut := read.barrier.cut

def ReadProjection.hold (read : ReadProjection Meaning history state) : OrderedJournal.Hold := read.barrier.hold

def ReadProjection.result (read : ReadProjection Meaning history state) : Meaning := read.capture.meaning

theorem ReadProjection.journalAgreesAtReadCut (read : ReadProjection Meaning history state) :
    state.chosen read.cut.next = some (projectCut read.cut) := by
  obtain ⟨_, path⟩ := read.admissionPath
  have atHead := read.barrier.projected.headAtAdmission.2
  exact OrderedJournal.trace_preserves_chosen_cut (.cons read.barrier.step path) atHead (Nat.le_refl _)

theorem ReadProjection.sameInstallation (read : ReadProjection Meaning history state)
    {position : Nat} {record : Protocol.Append} (selected : history[position]? = some (.append record))
    (placement : record.receipt.cut.placement = record.receipt.attempt.placement) :
    read.cut.placement = record.receipt.attempt.placement :=
  (InstalledRead.chosen_append_placement read.ordered selected).symm.trans placement

theorem ReadProjection.usesSelectedCut (read : ReadProjection Meaning history state) :
    read.result = read.stateMeaning read.cut := by
  obtain ⟨same, exactCut⟩ := InstalledRead.capture_at_selects_exact_catalog read.captured
  rw [same] at exactCut
  rw [ReadProjection.result, same, read.publicationMeaning, exactCut, ReadProjection.cut]

theorem prepared_checkpoint_and_trim_need_no_writer
    (s : OrderedJournal.State) (base target : OrderedJournal.Checkpoint)
    (through : OrderedJournal.Cut) (quorum : s.quorum = true)
    (ready : s.preparing = some ⟨base, target, true⟩)
    (agrees : s.chosen target.cut.next = some target.cut)
    (forward : s.published.cut.next < target.cut.next) (within : target.cut.next ≤ s.head.next)
    (exactTrim : s.chosen through.next = some through) (advancing : s.floor ≤ through.next)
    (covered : through.next ≤ target.cut.next)
    (unheld : ∀ h ∈ s.holdBook, h.first = h.through.next ∨ through.next ≤ h.first) :
    ∃ t, OrderedJournal.Trace readers activations s t ∧
      t.writer = s.writer ∧ t.published = target ∧ t.floor = through.next := by
  let published := {s with published := target, preparing := none}
  let trimmed := {published with floor := through.next}
  have publishStep : OrderedJournal.Step readers activations s published :=
    OrderedJournal.Step.publish s s.custodian base target rfl quorum ready agrees forward within
  have trimStep : OrderedJournal.Step readers activations published trimmed :=
    OrderedJournal.Step.trim published s.custodian target through rfl quorum rfl exactTrim
      advancing covered unheld
  exact ⟨trimmed, .cons publishStep (.cons trimStep (.nil trimmed)), rfl, rfl, rfl⟩

#print axioms chosen_result_projection
#print axioms ReadProjection.journalAgreesAtReadCut
#print axioms ReadProjection.sameInstallation
#print axioms recorded_append_reissues_while_live
#print axioms recovery_reissues_exact_chosen_append
#print axioms recovery_reissues_append_after_cohort_repair
#print axioms ReadProjection.usesSelectedCut
#print axioms prepared_checkpoint_and_trim_need_no_writer

end RTDB.Recovery.Composition
