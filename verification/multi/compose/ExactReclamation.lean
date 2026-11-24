import Logkeeper.ObjectWithdrawal
import DisposalLease

set_option warningAsError true

namespace ExactReclamation

open Logkeeper

def MatchingObjectFence {Name Version : Type} (fencedName name : Name)
    (fencedVersion version : Version) : Prop :=
  fencedName = name ∧ fencedVersion = version

def ReleaseIsCovered (through : Int) (trimFloor selectedCut : Nat) : Prop :=
  0 ≤ through ∧ through < (trimFloor : Int) ∧ trimFloor ≤ selectedCut

def ErasureRequiresCustodyJoin (erasureFloor : Nat) : Prop := erasureFloor ≠ 0

def ExactVersionRemoval (listedAsCurrent : Option Bool) (otherData : Bool) : Prop :=
  listedAsCurrent = some false ∨ (listedAsCurrent = some true ∧ otherData = false)

def TerminalTargetMatches (sameAddress sameCondition : Bool) : Prop :=
  sameAddress = true ∧ sameCondition = true

def RetainsServingSlot (sameOrigin sameAttempt : Bool) : Prop :=
  sameOrigin = true ∧ sameAttempt = true

def RetainsPublicationScope (deleted disposed regionsEmpty : Bool) : Prop :=
  deleted = false ∨ disposed = false ∨ regionsEmpty = false

def ProviderDrainCompleted (admissionClosed liveReadsSeen : Bool) : Prop :=
  admissionClosed = true ∧ liveReadsSeen = false

def MayDelete (fenced admissionClosed liveReadsSeen exactMatch : Bool) : Prop :=
  fenced = true ∧ ProviderDrainCompleted admissionClosed liveReadsSeen ∧ exactMatch = true

def StableAbsence (listed actual : Bool) : Prop :=
  listed = false ∧ (actual = true → listed = true)

inductive ReceiptAction where
  | reconcile | retire | defer
  deriving DecidableEq

def HostedReceiptAction (exactAbsent currentCompatible : Bool) : ReceiptAction :=
  if exactAbsent then .reconcile else if currentCompatible then .retire else .defer

theorem StableAbsence.excludes_target {listed actual : Bool} (absence : StableAbsence listed actual) :
    actual = false := by
  cases actual <;> simp_all [StableAbsence]

inductive ReaderPhase where
  | idle | registered | admitted | running
  deriving DecidableEq

abbrev OrderedState := ObjectWithdrawal.State Unit Nat

def initialOrdered : OrderedState := ⟨fun _ _ _ => False, fun _ => none, False, 0, 0⟩

theorem initial_ordered_is_safe : ObjectWithdrawal.Safe initialOrdered := by
  simp [ObjectWithdrawal.Safe, ObjectWithdrawal.fenced, initialOrdered]

def withdrawnOrdered : OrderedState := { initialOrdered with fence := fun _ => some 7 }

theorem reaches_ordered_fence : ObjectWithdrawal.Step initialOrdered withdrawnOrdered :=
  .withdraw initialOrdered () 7 ⟨id, fun ⟨_, _, held⟩ => held⟩
    (by simp [ObjectWithdrawal.fenced, initialOrdered])

structure State where
  ordered : OrderedState
  reader : ReaderPhase := .idle
  admissionClosed : Bool := false
  leaseExpired : Bool := false
  exactPresent : Bool := true
  replacementPresent : Bool := false
  lostReply : Bool := false
  settled : Bool := false

def liveReadsSeen (s : State) : Bool := s.reader != .idle

def noAdmittedRead (s : State) : Prop := s.reader = .idle ∨ s.reader = .registered

def State.hasOrderedFence (s : State) : Bool := decide (s.ordered.fence () = some 7)

theorem ordered_fence_persists {before after : OrderedState}
    (step : ObjectWithdrawal.Step before after)
    (fenced : decide (before.fence () = some 7) = true) :
    decide (after.fence () = some 7) = true :=
  decide_eq_true (ObjectWithdrawal.order_preserves_fence step (of_decide_eq_true fenced))

def Safe (s : State) : Prop :=
  ObjectWithdrawal.Safe s.ordered ∧
  (s.exactPresent = false → s.hasOrderedFence = true ∧ s.admissionClosed = true ∧ noAdmittedRead s) ∧
  (s.settled = true → s.exactPresent = false)

inductive Step : State → State → Prop where
  | order (s : State) (ordered : OrderedState)
      (step : ObjectWithdrawal.Step s.ordered ordered) :
      Step s { s with ordered := ordered }
  | register (s : State) (idle : s.reader = .idle) : Step s { s with reader := .registered }
  | admit (s : State) (registered : s.reader = .registered) (opened : s.admissionClosed = false)
      (leased : s.leaseExpired = false) : Step s { s with reader := .admitted }
  | start (s : State) (admitted : s.reader = .admitted) : Step s { s with reader := .running }
  | releaseUnadmitted (s : State) (registered : s.reader = .registered) :
      Step s { s with reader := .idle }
  | complete (s : State) (running : s.reader = .running) : Step s { s with reader := .idle }
  | expire (s : State) : Step s { s with leaseExpired := true }
  | close (s : State) (fenced : s.hasOrderedFence = true) : Step s { s with admissionClosed := true }
  | installReplacement (s : State) : Step s { s with replacementPresent := true }
  | remove (s : State) (ready : MayDelete s.hasOrderedFence s.admissionClosed (liveReadsSeen s) s.exactPresent)
      (lost : Bool) : Step s { s with exactPresent := false, lostReply := lost }
  | reconcile (s : State) (listed : Bool) (absence : StableAbsence listed s.exactPresent) :
      Step s { s with settled := true }

inductive Trace : State → State → Prop where
  | empty (s : State) : Trace s s
  | next {start here after : State} (earlier : Trace start here) (step : Step here after) :
      Trace start after

theorem step_preserves_safety {before after : State} (safe : Safe before)
    (step : Step before after) : Safe after := by
  rcases safe with ⟨ordered, removed, settled⟩
  cases step with
  | order _ step =>
      exact ⟨ObjectWithdrawal.order_preserves_separation ordered step,
        fun absent => ⟨ordered_fence_persists step (removed absent).1, (removed absent).2⟩, settled⟩
  | register => exact ⟨ordered, fun absent => ⟨(removed absent).1, (removed absent).2.1, Or.inr rfl⟩, settled⟩
  | admit _ opened _ =>
      exact ⟨ordered, fun absent => Bool.noConfusion (opened.symm.trans (removed absent).2.1), settled⟩
  | start admitted =>
      refine ⟨ordered, fun absent => ?_, settled⟩
      rcases (removed absent).2.2 with idle | registered <;> simp_all
  | releaseUnadmitted | complete =>
      exact ⟨ordered, fun absent => ⟨(removed absent).1, (removed absent).2.1, Or.inl rfl⟩, settled⟩
  | expire => exact ⟨ordered, removed, settled⟩
  | close => exact ⟨ordered, fun absent => ⟨(removed absent).1, rfl, (removed absent).2.2⟩, settled⟩
  | installReplacement => exact ⟨ordered, removed, settled⟩
  | remove ready _ =>
      have idle := ready.2.1.2
      simp [liveReadsSeen] at idle
      exact ⟨ordered, fun _ => ⟨ready.1, ready.2.1.1, Or.inl idle⟩, fun _ => rfl⟩
  | reconcile _ absence => exact ⟨ordered, removed, fun _ => absence.excludes_target⟩

theorem trace_preserves_safety {start finish : State} (safe : Safe start)
    (trace : Trace start finish) : Safe finish := by
  induction trace with
  | empty => exact safe
  | next _ step preceding => exact step_preserves_safety preceding step

theorem trace_preserves_a_present_replacement {start finish : State}
    (present : start.replacementPresent = true) (trace : Trace start finish) :
    finish.replacementPresent = true := by
  induction trace with
  | empty => exact present
  | next _ step preceding => cases step <;> first | rfl | exact preceding

def initial : State := { ordered := initialOrdered }
def registered : State := { initial with reader := .registered }
def admitted : State := { registered with reader := .admitted }
def reading : State := { admitted with reader := .running }
def expired : State := { reading with leaseExpired := true }
def fenced : State := { expired with ordered := withdrawnOrdered }
def blocked : State := { fenced with admissionClosed := true }
def drained : State := { blocked with reader := .idle }
def replaced : State := { drained with replacementPresent := true }
def replyLost : State := { replaced with exactPresent := false, lostReply := true }
def settledAfterLoss : State := { replyLost with settled := true }

theorem initial_is_safe : Safe initial := by
  exact ⟨initial_ordered_is_safe, by simp [initial], by simp [initial]⟩

theorem reaches_closed_census_with_expired_detached_read : Trace initial blocked :=
  .next (.next (.next (.next (.next (.next (.empty initial)
    (.register initial rfl)) (.admit registered rfl rfl rfl)) (.start admitted rfl)) (.expire reading))
    (.order expired withdrawnOrdered reaches_ordered_fence)) (.close fenced rfl)

theorem closed_census_refuses_deletion_with_a_live_detached_read :
    Trace initial blocked ∧ DisposalLease.MayDispose 10 1 11 ∧
      ¬ MayDelete blocked.hasOrderedFence blocked.admissionClosed (liveReadsSeen blocked) blocked.exactPresent := by
  refine ⟨reaches_closed_census_with_expired_detached_read, (by decide : 10 + 1 ≤ 11), ?_⟩
  intro ready
  exact Bool.noConfusion ready.2.1.2

theorem reaches_replacement : Trace initial replaced :=
  .next (.next reaches_closed_census_with_expired_detached_read (.complete blocked rfl))
    (.installReplacement drained)

theorem lost_reply_reconciles_with_replacement_present :
    Trace initial settledAfterLoss ∧ HostedReceiptAction true false = .reconcile ∧
      settledAfterLoss.exactPresent = false ∧ settledAfterLoss.replacementPresent = true ∧
      settledAfterLoss.lostReply = true :=
  ⟨.next (.next reaches_replacement (.remove replaced ⟨rfl, ⟨rfl, rfl⟩, rfl⟩ true))
    (.reconcile replyLost false ⟨rfl, by simp [replyLost]⟩), rfl, rfl, rfl, rfl⟩

theorem visible_replacement_without_exact_absence_defers : HostedReceiptAction false false = .defer := rfl

def ReconcileFromIncompleteListing (s : State) (listed : Bool) (t : State) : Prop :=
  listed = false ∧ t = { s with settled := true }

theorem incomplete_listing_can_settle_a_present_target :
    ∃ bad, Trace initial replaced ∧ ReconcileFromIncompleteListing replaced false bad ∧ ¬ Safe bad :=
  ⟨{ replaced with settled := true }, reaches_replacement, ⟨rfl, rfl⟩,
    fun safe => Bool.noConfusion (safe.2.2 rfl)⟩

def RetryDeletesCurrent (s t : State) : Prop :=
  s.lostReply = true ∧ t = { s with replacementPresent := false }

theorem path_only_retry_can_remove_a_replacement :
    ∃ bad, Trace initial replyLost ∧ RetryDeletesCurrent replyLost bad ∧
      replyLost.replacementPresent = true ∧ bad.replacementPresent = false :=
  ⟨{ replyLost with replacementPresent := false },
    .next reaches_replacement (.remove replaced ⟨rfl, ⟨rfl, rfl⟩, rfl⟩ true), ⟨rfl, rfl⟩, rfl, rfl⟩

def DeleteAfterLeaseExpiry (s t : State) : Prop :=
  s.hasOrderedFence = true ∧ s.leaseExpired = true ∧ t = { s with exactPresent := false }

theorem replacing_completion_with_expiry_deletes_during_a_read :
    ∃ bad, Trace initial blocked ∧ DeleteAfterLeaseExpiry blocked bad ∧ ¬ Safe bad :=
  ⟨{ blocked with exactPresent := false }, reaches_closed_census_with_expired_detached_read,
    ⟨rfl, rfl, rfl⟩, fun safe => by rcases (safe.2.1 rfl).2.2 with idle | registered <;> contradiction⟩

def ReconcileVisibleReplacement (s t : State) : Prop :=
  s.replacementPresent = true ∧ t = { s with settled := true }

theorem treating_a_visible_replacement_as_absence_settles_a_live_target :
    ∃ bad, Trace initial replaced ∧ ReconcileVisibleReplacement replaced bad ∧ ¬ Safe bad :=
  ⟨{ replaced with settled := true }, reaches_replacement, ⟨rfl, rfl⟩,
    fun safe => Bool.noConfusion (safe.2.2 rfl)⟩

#print axioms trace_preserves_safety
#print axioms trace_preserves_a_present_replacement
#print axioms closed_census_refuses_deletion_with_a_live_detached_read
#print axioms lost_reply_reconciles_with_replacement_present
#print axioms visible_replacement_without_exact_absence_defers
#print axioms incomplete_listing_can_settle_a_present_target
#print axioms path_only_retry_can_remove_a_replacement
#print axioms replacing_completion_with_expiry_deletes_during_a_read
#print axioms treating_a_visible_replacement_as_absence_settles_a_live_target

end ExactReclamation
