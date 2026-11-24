import ExactReclamation
import RecoveryComposition
import NativeAppend
import ReplayCertificate
import CheckpointReplay
import Logkeeper.ObjectWithdrawal
import Logkeeper.SelectionFence
import MaintenanceClassification
import DisposalLease
import ReaderRetention

set_option warningAsError true

namespace RTDB.Recovery.X1

open Logkeeper ReplayClosure RecoveryCertificate Composition

structure ChosenHistory where
  Voter : Type
  ledger : Ledger Voter
  certificate : Protocol.Seal ledger

def ChosenHistory.decisions (history : ChosenHistory) : List Protocol.Decision :=
  history.certificate.decisions

inductive Continuation (first : ChosenHistory) : ChosenHistory → Prop
  | start : Continuation first first
  | extend {ledger : Ledger Voter} {before after : Protocol.Seal ledger} :
      Continuation first ⟨Voter, ledger, before⟩ → Protocol.Evidence ledger →
      before.decisions.length ≤ after.decisions.length →
      Continuation first ⟨Voter, ledger, after⟩
  | repair {source : Ledger OldVoter} {destination : Ledger NewVoter}
      {before : Protocol.Seal source} {after : Protocol.Seal destination} :
      Continuation first ⟨OldVoter, source, before⟩ →
      Protocol.Evidence source → Protocol.Evidence destination →
      (transfer : Protocol.Transfer source destination) →
      before.decisions.length ≤ transfer.finalSource.decisions.length →
      transfer.installedBase.decisions.length ≤ after.decisions.length →
      Continuation first ⟨NewVoter, destination, after⟩

theorem continuation_preserves_chosen_prefix (path : Continuation first last) :
    first.decisions <+: last.decisions := by
  induction path with
  | start => exact List.prefix_rfl
  | @extend _ _ before after _ evidence advancing prior =>
    apply prior.trans
    rcases Protocol.compatible evidence before after with forward | backward
    · exact forward
    · exact List.prefix_of_prefix_length_le List.prefix_rfl backward advancing
  | repair _ oldVoters newVoters transfer beforeSeal afterInstall prior =>
    exact prior.trans (Protocol.transfer_extends_chosen_history oldVoters newVoters
      transfer _ _ beforeSeal afterInstall)

theorem selected_decision_survives_extension
    {before after : List Protocol.Decision} {position : Nat}
    (extendsHistory : before <+: after) (selected : before[position]? = some decision) :
    after[position]? = some decision := by
  obtain ⟨suffix, rfl⟩ := extendsHistory
  rw [List.getElem?_append_left (List.getElem?_eq_some_iff.mp selected).1, selected]

theorem recovered_history_keeps_committed_fence
    {ledger : Ledger Voter} {recovered : ChosenHistory}
    (rules : PrefixAgreement.BallotRules ledger) (durable : SelectionFence.DurableReports ledger)
    (chosen : PrefixAgreement.Chosen ledger fenceBallot fence)
    (sealLater : fenceBallot < sealBallot) (sealing : ledger.active sealBallot)
    (repairs : SelectionFence.InstalledAfter Voter (PrefixAgreement.seed ledger sealBallot) installed)
    (selected : SelectionFence.CohortRecovery Voter installed recovered.decisions) :
    fence <+: recovered.decisions :=
  SelectionFence.a_repaired_ranked_majority_keeps_the_original_fence
    rules durable chosen sealLater sealing repairs selected

theorem selectable_image_keeps_mirrored_fence
    {ack image : SelectionFence.Image Protocol.Decision}
    (mediaTrace : SelectionFence.Reachable (SelectionFence.mirrored ack) media)
    (eligible : media slot = some image) :
    image.Dominates ack :=
  SelectionFence.any_surviving_slot_keeps_the_acknowledged_rank mediaTrace slot image eligible

theorem retained_journal_entry_survives_one_az
    {Zone : Type} [DecidableEq Zone] {journal : JournalRefinement.Journal Voter Payload}
    (complete : JournalRefinement.CompleteCut journal) (zoneOf : Voter → Zone) (lost : Zone)
    (roster : journal.members = [a, b, c])
    (ab : zoneOf a ≠ zoneOf b) (ac : zoneOf a ≠ zoneOf c) (bc : zoneOf b ≠ zoneOf c)
    (retained : journal.logical.firstRetained ≤ lsn)
    (chosenEntry : journal.logical.history[lsn]? = some entry) :
    ∃ voter ∈ journal.members, zoneOf voter ≠ lost ∧ journal.copies voter lsn entry := by
  have majority : JournalRefinement.Majority journal.members (fun voter => decide (zoneOf voter ≠ lost)) :=
    roster ▸ JournalRefinement.distinct_zone_configuration_has_majority_after_loss zoneOf lost a b c ab ac bc
  obtain ⟨voter, member, up, copy⟩ := JournalRefinement.any_recovery_quorum_can_read_exact_entry
    complete _ majority retained chosenEntry
  exact ⟨voter, member, of_decide_eq_true up, copy⟩

def erases (committedTargets : Nat → Tx → Bool) (tx : Tx) : Protocol.Decision → Bool
  | .append _ => false
  | .control digest => committedTargets digest tx

def applyDecision (decodeTx : Protocol.Append → Option Tx) (committedTargets : Nat → Tx → Bool)
    (events : List Tx) : Protocol.Decision → List Tx
  | .append append =>
    match decodeTx append with
    | some tx => events ++ [tx]
    | none => events
  | .control digest => events.filter fun tx => !(committedTargets digest tx)

theorem survivor_of_chosen_append
    (decodeTx : Protocol.Append → Option Tx) (committedTargets : Nat → Tx → Bool)
    {history : List Protocol.Decision} (selected : history[position]? = some (.append append))
    (decoded : decodeTx append = some tx)
    (survives : (history.drop (position + 1)).any (erases committedTargets tx) = false)
    (initial : List Tx) :
    tx ∈ history.foldl (applyDecision decodeTx committedTargets) initial := by
  have retained : ∀ (tail : List Protocol.Decision) (events : List Tx), tx ∈ events →
      tail.any (erases committedTargets tx) = false →
      tx ∈ tail.foldl (applyDecision decodeTx committedTargets) events := by
    intro tail
    induction tail with
    | nil => exact fun _ member _ => member
    | cons decision rest ih =>
      intro events member survives
      simp only [List.any_cons, Bool.or_eq_false_iff] at survives
      apply ih _ _ survives.2
      cases decision with
      | append record =>
        cases decoding : decodeTx record <;> simp [applyDecision, decoding, member]
      | control digest =>
        exact List.mem_filter.mpr ⟨member, by simpa [erases] using survives.1⟩
  induction history generalizing position initial with
  | nil => simp at selected
  | cons decision tail ih =>
    cases position with
    | zero =>
      simp at selected
      subst decision
      exact retained tail _ (by simp [applyDecision, decoded]) (by simpa using survives)
    | succ position =>
      exact ih (position := position) (by simpa using selected) (by simpa using survives) _

def ExactCoverage (zoneOf : PhysicalVersion → Zone) (inventory : Key → PhysicalVersion → Prop)
    (grants : Grant Key → Prop) (scope : Scope) (objects : PhysicalVersion → ExactReclamation.OrderedState)
    (lost : Zone) (key : Key) : Prop :=
  ∃ physical, inventory key physical ∧ zoneOf physical ≠ lost ∧
    ¬ ObjectWithdrawal.fenced (objects physical) () ∧ ∃ grant, grants grant ∧ Binds scope key physical grant

theorem replay_reads_have_unwithdrawn_survivors
    {zoneOf : PhysicalVersion → Zone}
    (ready : ZoneReady Certificate.dependencies zoneOf inventory grants scope selected (fun _ => True))
    (withdrawalTrace : ∀ physical, ObjectWithdrawal.Trace ExactReclamation.initialOrdered (objects physical))
    (registered : ∀ key physical grant, inventory key physical → grants grant →
      Binds scope key physical grant → ObjectWithdrawal.referenced (objects physical) ())
    (lost : Zone) :
    (∀ key, ReplayRead selected key → ExactCoverage zoneOf inventory grants scope objects lost key) ∧
    (∀ state, Reaches selected state → ∀ key ∈ state.demands,
      ExactCoverage zoneOf inventory grants scope objects lost key) := by
  have available : ∀ key, (∃ physical, inventory key physical ∧ zoneOf physical ≠ lost ∧
      ∃ grant, grants grant ∧ Binds scope key physical grant) →
      ExactCoverage zoneOf inventory grants scope objects lost key := by
    rintro key ⟨physical, present, survives, grant, held, binds⟩
    have reference := registered key physical grant present held binds
    have safe := ObjectWithdrawal.trace_preserves_separation ExactReclamation.initial_ordered_is_safe
      (withdrawalTrace physical)
    exact ⟨physical, present, survives, fun fenced => (safe () fenced).2 reference, grant, held, binds⟩
  constructor
  · intro key reads
    exact available key (every_application_read_has_exact_held_coverage Certificate.dependencies
      _ inventory grants scope selected _ (zone_placement_refines_the_provider_contract
        Certificate.dependencies zoneOf inventory grants scope selected _ ready) key reads lost trivial)
  · intro state reached key demanded
    exact available key (Certificate.one_az_placement_covers_every_reachable_replay_demand
      zoneOf inventory grants scope selected _ ready reached key demanded lost trivial)

structure ReplaySemantics (Tx : Type) where
  source : NativeAppend.DecodeSource Tx
  committedTargets : Nat → Tx → Bool
  authenticatesPlan : Scope → OrderedJournal.Checkpoint → Protocol.DataCut → Selection → Prop
  checkpointEvents : ReplayClosure.Checkpoint → List Tx
  replayEvents : Selection → List Tx → List Tx

def ReplaySemantics.decodeTx (semantics : ReplaySemantics Tx) : Protocol.Append → Option Tx :=
  NativeAppend.decodeAppend semantics.source

inductive TypedReplay (semantics : ReplaySemantics Tx) :
    List Protocol.Decision → List ReplayClosure.Record → List Tx → List Tx → Prop
  | done : TypedReplay semantics [] [] events events
  | transaction : record.body = .resolvedTx → semantics.decodeTx append = some tx →
      TypedReplay semantics history records (events ++ [tx]) result →
      TypedReplay semantics (.append append :: history) (record :: records) events result
  | maintenance : record.body ≠ .resolvedTx → semantics.decodeTx append = none →
      TypedReplay semantics history records events result →
      TypedReplay semantics (.append append :: history) (record :: records) events result
  | control : TypedReplay semantics history records
      (events.filter fun tx => !(semantics.committedTargets digest tx)) result →
      TypedReplay semantics (.control digest :: history) records events result

theorem TypedReplay.fidelity (replayed : TypedReplay semantics history records events result) :
    result = history.foldl (applyDecision semantics.decodeTx semantics.committedTargets) events := by
  induction replayed with
  | done => rfl
  | transaction _ decoded _ rest => simpa [applyDecision, decoded] using rest
  | maintenance _ decoded _ rest => simpa [applyDecision, decoded] using rest
  | control _ rest => exact rest

inductive CheckpointReplay (semantics : ReplaySemantics Tx) :
    Option ReplayClosure.Checkpoint → List Protocol.Decision → Prop
  | genesis : CheckpointReplay semantics none []
  | restored : TypedReplay semantics history records [] (semantics.checkpointEvents base) →
      CheckpointReplay semantics (some base) history

theorem CheckpointReplay.fidelity (replayed : CheckpointReplay semantics base history) :
    (base.map semantics.checkpointEvents).getD [] =
      history.foldl (applyDecision semantics.decodeTx semantics.committedTargets) [] := by
  cases replayed with
  | genesis => rfl
  | restored applied => exact applied.fidelity

/-- The decision count is a logical coordinate and can advance on control commands without advancing the data LSN. -/
structure RecoveryProjection (semantics : ReplaySemantics Tx) (history : List Protocol.Decision)
    (selected : Selection) (scope : Scope) (read : ReadProjection (List Tx) history state) where
  checkpointDecisionCount : Nat
  checkpointInHistory : checkpointDecisionCount ≤ history.length
  exactScopeCut : scope.orderedCut = read.cut.next
  selectedRoot : (selected.checkpoint.map (·.root)).getD 0 = read.hold.base.root
  authenticPlan : semantics.authenticatesPlan scope read.hold.base read.cut selected
  nativeReadKeys : List Key
  typedReads : ∀ key ∈ nativeReadKeys, (∃ base, selected.checkpoint = some base ∧ BaseRead base key) ∨
    ∃ replay, Reaches selected replay ∧ key ∈ replay.demands
  checkpointAtCut : read.hold.base.cut =
    projectCut (InstalledRead.dataCut read.seed (history.take checkpointDecisionCount))
  checkpointReplay : CheckpointReplay semantics selected.checkpoint (history.take checkpointDecisionCount)
  suffixReplay : TypedReplay semantics (history.drop checkpointDecisionCount) selected.suffix
    ((selected.checkpoint.map semantics.checkpointEvents).getD [])
    (semantics.replayEvents selected ((selected.checkpoint.map semantics.checkpointEvents).getD []))
  readUsesReplay : read.stateMeaning read.cut =
    semantics.replayEvents selected (((selected.checkpoint.map semantics.checkpointEvents).getD []))

theorem RecoveryProjection.checkpointFidelity
    {semantics : ReplaySemantics Tx} {read : ReadProjection (List Tx) history state}
    (projection : RecoveryProjection semantics history selected scope read) :
    (selected.checkpoint.map semantics.checkpointEvents).getD [] =
      (history.take projection.checkpointDecisionCount).foldl
        (applyDecision semantics.decodeTx semantics.committedTargets) [] :=
  projection.checkpointReplay.fidelity

theorem RecoveryProjection.suffixFidelity
    {semantics : ReplaySemantics Tx} {read : ReadProjection (List Tx) history state}
    (projection : RecoveryProjection semantics history selected scope read) :
    semantics.replayEvents selected ((selected.checkpoint.map semantics.checkpointEvents).getD []) =
      (history.drop projection.checkpointDecisionCount).foldl
        (applyDecision semantics.decodeTx semantics.committedTargets)
        ((selected.checkpoint.map semantics.checkpointEvents).getD []) :=
  projection.suffixReplay.fidelity

theorem recovery_read_is_exact_history
    {semantics : ReplaySemantics Tx} {read : ReadProjection (List Tx) history state}
    (projection : RecoveryProjection semantics history selected scope read) :
    read.result = history.foldl (applyDecision semantics.decodeTx semantics.committedTargets) [] := by
  rw [read.usesSelectedCut, projection.readUsesReplay, projection.suffixFidelity,
    projection.checkpointFidelity]
  simpa using replay_suffix_from_checkpoint
    (applyDecision semantics.decodeTx semantics.committedTargets) [] history
    projection.checkpointDecisionCount history.length projection.checkpointInHistory

theorem recovered_query_observes_exact_history
    {semantics : ReplaySemantics Tx} {read : ReadProjection (List Tx) history state}
    (projection : RecoveryProjection semantics history selected scope read)
    (query : List Tx → Result) (current : InstalledRead.PublishedCatalog (List Tx)) :
    query (InstalledRead.querySnapshot (some read.capture) current).meaning =
      query (history.foldl (applyDecision semantics.decodeTx semantics.committedTargets) []) :=
  (InstalledRead.queryUsesRead read.capture current query).trans
    (congrArg query (recovery_read_is_exact_history projection))

theorem acknowledged_transaction_survives_recovery
    {ledger : Ledger Voter} {zoneOf : PhysicalVersion → Zone} {semantics : ReplaySemantics Tx}
    (initial : ChosenAppend ledger position) (recovered : ChosenHistory)
    (chosenPath : Continuation ⟨Voter, ledger, initial.certifiedPrefix⟩ recovered)
    (journalPath : OrderedJournal.Trace readers activations (initial.recordedState previous) after)
    (journalInvariant : OrderedJournal.Invariant (initial.recordedState previous))
    (read : ReadProjection (List Tx) recovered.decisions after)
    (projection : RecoveryProjection semantics recovered.decisions selected scope read)
    (nativeAcknowledgement : NativeAppend.Binding semantics.source initial.record tx)
    (ready : ZoneReady Certificate.dependencies zoneOf inventory grants scope selected (fun _ => True))
    (withdrawalTrace : ∀ physical, ObjectWithdrawal.Trace ExactReclamation.initialOrdered (objects physical))
    (registered : ∀ key physical grant, inventory key physical → grants grant →
      Binds scope key physical grant → ObjectWithdrawal.referenced (objects physical) ())
    (lost : Zone) :
    recovered.decisions[position]? = some (.append initial.record) ∧
    (after.observed < initial.record.receipt.expiresAt.next →
      OrderedJournal.Reissue after (projectAttempt initial.record.receipt)
        initial.record.receipt.requestDigest = some (projectReceipt initial.record.receipt)) ∧
    (read.hold.first = read.hold.through.next ∨ after.floor ≤ read.hold.first) ∧
    read.hold.through.next ≤ read.actual.next ∧
    (read.actual.next = read.cut.next → read.actual = projectCut read.cut) ∧
    (read.cut = InstalledRead.dataCut read.seed recovered.decisions ∧
      read.cut.placement = initial.record.receipt.attempt.placement ∧
      initial.record.receipt.cut.next ≤ read.cut.next) ∧
    ((recovered.decisions.drop (position + 1)).any
      (erases semantics.committedTargets tx) = false → tx ∈ read.result) ∧
    (∀ key ∈ projection.nativeReadKeys, ExactCoverage zoneOf inventory grants scope objects lost key) := by
  have selectedAppend := selected_decision_survives_extension
    (continuation_preserves_chosen_prefix chosenPath) initial.selected
  have invariant := OrderedJournal.protected_through_trace journalInvariant journalPath
  refine ⟨selectedAppend, recorded_append_reissues_while_live (chosen_result_projection initial previous) journalPath,
    invariant.2.2.2 _ read.admitted.1, OrderedJournal.read_cannot_serve_an_unapplied_barrier read.admitted,
    fun same => OrderedJournal.read_cannot_serve_same_position_with_alien_digest
      read.admitted read.journalAgreesAtReadCut same,
    ⟨InstalledRead.exactReadCut read.barrier read.ordered, read.sameInstallation selectedAppend rfl,
      InstalledRead.readCoversAcknowledgement read.barrier read.ordered selectedAppend⟩, ?_, ?_⟩
  · intro survives
    rw [recovery_read_is_exact_history projection]
    exact survivor_of_chosen_append semantics.decodeTx semantics.committedTargets
      selectedAppend (NativeAppend.decodedAcknowledgement nativeAcknowledgement) survives []
  · obtain ⟨baseAvailable, tailAvailable⟩ :=
      replay_reads_have_unwithdrawn_survivors ready withdrawalTrace registered lost
    intro key readKey
    rcases projection.typedReads key readKey with base | ⟨replay, reached, demanded⟩
    · exact baseAvailable key (.inl base)
    · exact tailAvailable replay reached key demanded

theorem maintenance_reserve_survives_replacement {receiverBudget : Nat} (lane : MaintenanceBound.SettlementLane)
    (reached : MaintenanceBound.SettlementReaches receiverBudget lane) :
    lane.used ≤ MaintenanceBound.funded_records receiverBudget ∧
      ∀ body, MaintenanceBound.maintenance_append_allowed lane.checkpoint.phase lane.slot body →
        lane.used + MaintenanceBound.record_charge body ≤ MaintenanceBound.funded_records receiverBudget :=
  ⟨Nat.le_trans (Nat.le_add_right _ _) (MaintenanceBound.settlement_lane_stays_funded reached),
    MaintenanceBound.reachable_maintenance_append_fits reached⟩

theorem disposed_worker_cannot_admit_read (leaseEnd drift now workerClock : Nat)
    (bounded : now ≤ workerClock + drift) (dispose : DisposalLease.MayDispose leaseEnd drift now) :
    ¬ DisposalLease.Admits leaseEnd workerClock :=
  DisposalLease.no_read_is_admitted_once_disposal_is_allowed leaseEnd drift now workerClock bounded dispose

theorem leased_read_finishes_before_reclamation
    (admitted retired started readerClock skewBound ioBound duration lifetime : Int)
    (admittedBeforeRetirement : admitted ≤ retired)
    (clockBound : started ≤ readerClock + skewBound)
    (admission : ReaderRetention.leaseAdmits readerClock skewBound ioBound (admitted + lifetime))
    (completionBound : duration ≤ ioBound) :
    started + duration < retired + lifetime :=
  ReaderRetention.lease_read_finishes_before_reclamation admitted retired started readerClock
    skewBound ioBound duration lifetime admittedBeforeRetirement clockBound admission completionBound

#print axioms TypedReplay.fidelity
#print axioms CheckpointReplay.fidelity
#print axioms RecoveryProjection.checkpointFidelity
#print axioms RecoveryProjection.suffixFidelity
#print axioms continuation_preserves_chosen_prefix
#print axioms selected_decision_survives_extension
#print axioms recovered_history_keeps_committed_fence
#print axioms selectable_image_keeps_mirrored_fence
#print axioms retained_journal_entry_survives_one_az
#print axioms survivor_of_chosen_append
#print axioms replay_reads_have_unwithdrawn_survivors
#print axioms recovery_read_is_exact_history
#print axioms recovered_query_observes_exact_history
#print axioms acknowledged_transaction_survives_recovery
#print axioms maintenance_reserve_survives_replacement
#print axioms disposed_worker_cannot_admit_read
#print axioms leased_read_finishes_before_reclamation

end RTDB.Recovery.X1
