import Logkeeper.Protocol
import OrderedJournal

set_option warningAsError true

namespace RTDB.Recovery.InstalledRead

open Logkeeper

def projectCut (cut : Protocol.DataCut) : OrderedJournal.Cut := ⟨cut.next, cut.digest⟩

def projectAttempt (receipt : Protocol.AppendReceipt) : OrderedJournal.Attempt :=
  ⟨receipt.attempt.issuer, receipt.attempt.nonce, receipt.expiresAt.next⟩

def advanceDataCut (head : Protocol.DataCut) : Protocol.Decision → Protocol.DataCut
  | .append record => record.receipt.cut
  | .control _ => head

def dataCut (seed : Protocol.DataCut) (history : List Protocol.Decision) : Protocol.DataCut :=
  history.foldl advanceDataCut seed

inductive OrderedDataHistory : Protocol.DataCut → List Protocol.Decision → Prop
  | empty : OrderedDataHistory head []
  | control : OrderedDataHistory head tail → OrderedDataHistory head (.control digest :: tail)
  | append : record.receipt.cut.next = head.next + 1 →
      record.receipt.cut.placement = head.placement →
      OrderedDataHistory record.receipt.cut tail →
      OrderedDataHistory head (.append record :: tail)

theorem dataCut_advances (ordered : OrderedDataHistory seed history) :
    seed.next ≤ (dataCut seed history).next ∧ (dataCut seed history).placement = seed.placement := by
  induction ordered with
  | empty => exact ⟨Nat.le_refl _, rfl⟩
  | control _ ih => exact ih
  | append next placement _ ih =>
    exact ⟨Nat.le_trans (Nat.le_of_lt (by omega)) ih.1, ih.2.trans placement⟩

theorem chosen_append_is_below_data_cut {history : List Protocol.Decision}
    {position : Nat} {acknowledged : Protocol.Append} (ordered : OrderedDataHistory seed history)
    (selected : history[position]? = some (.append acknowledged)) :
    acknowledged.receipt.cut.next ≤ (dataCut seed history).next := by
  induction ordered generalizing position with
  | empty => simp at selected
  | control _ ih =>
    cases position with
    | zero => simp at selected
    | succ position => exact ih (by simpa using selected)
  | append _ _ ordered ih =>
    cases position with
    | zero =>
      simp only [List.getElem?_cons_zero, Option.some.injEq, Protocol.Decision.append.injEq] at selected
      subst acknowledged
      exact (dataCut_advances ordered).1
    | succ position => exact ih (by simpa using selected)

theorem chosen_append_placement {history : List Protocol.Decision}
    {position : Nat} {acknowledged : Protocol.Append} (ordered : OrderedDataHistory seed history)
    (selected : history[position]? = some (.append acknowledged)) :
    acknowledged.receipt.cut.placement = seed.placement := by
  induction ordered generalizing position with
  | empty => simp at selected
  | control _ ih =>
    cases position with
    | zero => simp at selected
    | succ position => exact ih (by simpa using selected)
  | append _ placement _ ih =>
    cases position with
    | zero =>
      simp only [List.getElem?_cons_zero, Option.some.injEq, Protocol.Decision.append.injEq] at selected
      subst acknowledged
      exact placement
    | succ position => exact (ih (by simpa using selected)).trans placement

inductive PrefixProjection (seed : Protocol.DataCut) :
    List Protocol.Decision → OrderedJournal.State → Prop
  | genesis : projectCut seed = OrderedJournal.seedCut → PrefixProjection seed [] OrderedJournal.seed
  | append : PrefixProjection seed history state →
      OrderedJournal.Step readers activations state (OrderedJournal.recordAppend state (projectAttempt record.receipt)
        ⟨record.receipt.requestDigest, projectCut record.receipt.cut⟩) →
      PrefixProjection seed (history ++ [.append record]) (OrderedJournal.recordAppend state (projectAttempt record.receipt)
        ⟨record.receipt.requestDigest, projectCut record.receipt.cut⟩)
  | control : PrefixProjection seed history state → OrderedJournal.Step readers activations state advanced →
      advanced.head = state.head → PrefixProjection seed (history ++ [.control digest]) advanced

theorem PrefixProjection.headAtAdmission (projected : PrefixProjection seed history state) :
    state.head = projectCut (dataCut seed history) ∧ state.chosen state.head.next = some state.head := by
  induction projected with
  | genesis origin =>
    exact ⟨origin.symm, rfl⟩
  | append _ _ _ =>
    simp [OrderedJournal.recordAppend, OrderedJournal.update, dataCut, List.foldl_append, advanceDataCut]
  | control _ step same prior =>
    refine ⟨?_, ?_⟩
    · simpa [dataCut, List.foldl_append, advanceDataCut, same] using prior.1
    · rw [same]
      exact OrderedJournal.trace_preserves_chosen_cut (.cons step (.nil _)) prior.2 (Nat.le_refl _)

structure Invocation where
  nonce : Nat
  expiresAt : Nat
  deriving DecidableEq

abbrev barrierMatchesInvocation (actual requested : Invocation) : Prop := actual = requested

abbrev readReleaseSettled (position : Nat) (read : Invocation) : Prop := read.expiresAt ≤ position

abbrev leaseAdmits (now deadline : Nat) : Prop := now < deadline

def barrierOffset (next : Nat) : Int := (next : Int) - 1

theorem barrier_offset_preserves_exclusive_cut (next : Nat) :
    barrierOffset next + 1 = next := by simp [barrierOffset]

/-- The modeled state sampled by the chosen Barrier decision under D01. -/
structure FreshBarrier (seed : Protocol.DataCut) (history : List Protocol.Decision) where
  before : OrderedJournal.State
  enrollment : Nat
  requested : Invocation
  claimed : Invocation
  consumed : List Invocation
  fresh : requested ∉ consumed
  invocationMatches : barrierMatchesInvocation claimed requested
  now : Nat
  deadline : Nat
  leased : leaseAdmits now deadline
  projected : PrefixProjection seed history before
  cohort : Nat
  current : OrderedJournal.Current before cohort enrollment
  quorum : before.quorum = true
  readerSlots : Nat
  room : OrderedJournal.ReaderCount before < readerSlots

def FreshBarrier.hold (admission : FreshBarrier seed history) : OrderedJournal.Hold :=
  OrderedJournal.hold admission.before admission.enrollment admission.claimed.nonce false

def FreshBarrier.cut (admission : FreshBarrier seed history) : Protocol.DataCut :=
  ⟨seed.placement, admission.hold.through.next, admission.hold.through.digest⟩

def FreshBarrier.admitted (admission : FreshBarrier seed history) : OrderedJournal.State :=
  {admission.before with holdBook := admission.hold :: admission.before.holdBook}

theorem FreshBarrier.step (admission : FreshBarrier seed history) :
    OrderedJournal.Step admission.readerSlots activations admission.before admission.admitted :=
  .barrier _ admission.cohort admission.enrollment admission.claimed.nonce admission.current
    admission.quorum admission.room

theorem exactReadCut (admission : FreshBarrier seed history)
    (ordered : OrderedDataHistory seed history) :
    admission.cut = dataCut seed history := by
  have placement := (dataCut_advances ordered).2
  cases h : dataCut seed history with
  | mk scope next digest =>
    simp only [h] at placement
    simp [FreshBarrier.cut, FreshBarrier.hold, OrderedJournal.hold, admission.projected.headAtAdmission.1,
      h, projectCut, placement]

theorem readCoversAcknowledgement {history : List Protocol.Decision}
    {position : Nat} {acknowledged : Protocol.Append} (admission : FreshBarrier seed history)
    (ordered : OrderedDataHistory seed history)
    (selected : history[position]? = some (.append acknowledged)) :
    acknowledged.receipt.cut.next ≤ admission.cut.next := by
  rw [exactReadCut admission ordered]
  exact chosen_append_is_below_data_cut ordered selected

structure PublishedCatalog (Meaning : Type) where
  cut : Protocol.DataCut
  meaning : Meaning

def emptyCatalogMatchesCut (catalogCut : Option Protocol.DataCut) (txBasis : Option Tx) : Bool :=
  catalogCut.isNone && txBasis.isNone

def captureAt (requested : Protocol.DataCut) (published : PublishedCatalog Meaning) :
    Option (PublishedCatalog Meaning) :=
  if published.cut = requested then some published else none

theorem capture_at_selects_exact_catalog {published snapshot : PublishedCatalog Meaning}
    (captured : captureAt cut published = some snapshot) :
    snapshot = published ∧ snapshot.cut = cut := by
  unfold captureAt at captured
  split at captured
  · simp only [Option.some.injEq] at captured
    subst snapshot
    exact ⟨rfl, by assumption⟩
  · simp at captured

def querySnapshot (captured : Option (PublishedCatalog Meaning))
    (current : PublishedCatalog Meaning) : PublishedCatalog Meaning :=
  captured.getD current

theorem queryUsesRead (snapshot current : PublishedCatalog Meaning) (query : Meaning → Answer) :
    query (querySnapshot (some snapshot) current).meaning = query snapshot.meaning := rfl

structure SnapshotScope where
  root : Nat
  fromLSN : Nat
  through : Nat
  deriving DecidableEq

inductive OwnedReadCommand
  | barrier
  | protectCheckpoint (scope : SnapshotScope)
  | releaseRead (id : Invocation)
  | release (hold : OrderedJournal.Hold)
  | inspectRoute

def protectSnapshot (scope : SnapshotScope) : OwnedReadCommand :=
  .protectCheckpoint scope

def releaseOnClose (hold : OrderedJournal.Hold) : OwnedReadCommand :=
  .release hold

def releaseAfterLostBarrier (read : Invocation) : OwnedReadCommand :=
  .releaseRead read

theorem protected_snapshot_names_its_exact_scope (scope : SnapshotScope) :
    protectSnapshot scope = .protectCheckpoint scope := rfl

theorem close_releases_the_original_hold (hold : OrderedJournal.Hold) :
    releaseOnClose hold = .release hold := rfl

theorem a_lost_barrier_names_its_original_invocation (read : Invocation) :
    releaseAfterLostBarrier read = .releaseRead read := rfl

theorem barrier_alone_does_not_protect_the_snapshot (scope : SnapshotScope) :
    OwnedReadCommand.barrier ≠ .protectCheckpoint scope := by
  intro h
  contradiction

theorem route_inspection_does_not_release_the_hold (hold : OrderedJournal.Hold) :
    OwnedReadCommand.inspectRoute ≠ .release hold := by
  intro h
  contradiction

#print axioms dataCut_advances
#print axioms chosen_append_placement
#print axioms PrefixProjection.headAtAdmission
#print axioms exactReadCut
#print axioms readCoversAcknowledgement
#print axioms capture_at_selects_exact_catalog
#print axioms queryUsesRead
#print axioms protected_snapshot_names_its_exact_scope
#print axioms close_releases_the_original_hold
#print axioms a_lost_barrier_names_its_original_invocation
#print axioms barrier_alone_does_not_protect_the_snapshot
#print axioms route_inspection_does_not_release_the_hold

end RTDB.Recovery.InstalledRead
