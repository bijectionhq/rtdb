import Logkeeper.Protocol
import OrderedJournal

set_option warningAsError true

namespace RTDB.Recovery.Composition

open Logkeeper

abbrev Ledger (Voter : Type) := PrefixAgreement.Ledger Voter Protocol.Decision

def projectCut (cut : Protocol.DataCut) : OrderedJournal.Cut :=
  ⟨cut.next, cut.digest⟩

/-- Project one shared decision into the RTDB receipt table. Placement and
request identity are explicit inputs to the worker's interpretation. -/
def projectAttempt (receipt : Protocol.AppendReceipt) : OrderedJournal.Attempt :=
  ⟨receipt.attempt.issuer, receipt.attempt.nonce, receipt.expiresAt.next⟩

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

/-- All the native semantics live in this function. An applied position can
justify readiness while fidelity still requires a value at the exact cut. -/
structure ReadProjection (Meaning : Type) (state : OrderedJournal.State) where
  placement : Protocol.Placement
  cut : Protocol.DataCut
  belongs : cut.placement = placement
  hold : OrderedJournal.Hold
  bound : hold.through = projectCut cut
  actual : OrderedJournal.Cut
  admitted : OrderedJournal.CanServe state hold actual
  stateMeaning : Protocol.DataCut → Meaning
  result : Meaning
  usesSelectedCut : result = stateMeaning cut

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

#print axioms recorded_append_reissues_while_live
#print axioms recovery_reissues_exact_chosen_append
#print axioms recovery_reissues_append_after_cohort_repair
#print axioms prepared_checkpoint_and_trim_need_no_writer

end RTDB.Recovery.Composition
