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

def MayDelete (fenced readersDrained exactMatch : Bool) : Prop :=
  fenced = true ∧ readersDrained = true ∧ exactMatch = true

def StableAbsence (listed actual : Bool) : Prop :=
  listed = false ∧ (actual = true → listed = true)

def HostedHeadAllowsRetry (observedMatch : Option Bool) : Prop :=
  observedMatch = some true ∨ observedMatch = none

theorem StableAbsence.excludes_target {listed actual : Bool} (absence : StableAbsence listed actual) :
    actual = false := by
  cases actual <;> simp_all [StableAbsence]

structure State where
  ordered : ObjectWithdrawal.State
  readersDrained : Bool := false
  exactPresent : Bool := true
  replacementPresent : Bool := false
  lostReply : Bool := false
  settled : Bool := false

def Safe (s : State) : Prop :=
  ObjectWithdrawal.Safe s.ordered ∧
  (s.exactPresent = false → s.ordered.fenced = true ∧ s.readersDrained = true) ∧
  (s.settled = true → s.exactPresent = false)

theorem ordered_fence_persists {before after : ObjectWithdrawal.State}
    (step : ObjectWithdrawal.Step before after) (fenced : before.fenced = true) :
    after.fenced = true := by
  cases step <;> first | rfl | exact fenced

inductive Step : State → State → Prop where
  | order (s : State) (ordered : ObjectWithdrawal.State)
      (step : ObjectWithdrawal.Step s.ordered ordered) :
      Step s { s with ordered := ordered }
  | drain (s : State) (fenced : s.ordered.fenced = true) :
      Step s { s with readersDrained := true }
  | installReplacement (s : State) : Step s { s with replacementPresent := true }
  | remove (s : State) (ready : MayDelete s.ordered.fenced s.readersDrained s.exactPresent)
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
  | drain => exact ⟨ordered, fun absent => ⟨(removed absent).1, rfl⟩, settled⟩
  | installReplacement => exact ⟨ordered, removed, settled⟩
  | remove ready _ => exact ⟨ordered, fun _ => ⟨ready.1, ready.2.1⟩, fun _ => rfl⟩
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

def initial : State := { ordered := ObjectWithdrawal.empty }

def fenced : State := { initial with ordered := ObjectWithdrawal.withdrawn }

def drained : State := { fenced with readersDrained := true }

def replaced : State := { drained with replacementPresent := true }

def replyLost : State := { replaced with exactPresent := false, lostReply := true }

def settledAfterLoss : State := { replyLost with settled := true }

theorem initial_is_safe : Safe initial := by
  simp [Safe, initial, ObjectWithdrawal.Safe, ObjectWithdrawal.empty]

theorem reaches_replacement : Trace initial replaced :=
  .next (.next (.next (.empty initial) (.order initial ObjectWithdrawal.withdrawn
    ObjectWithdrawal.reachable_fence)) (.drain fenced rfl)) (.installReplacement drained)

theorem lost_reply_reconciles_with_replacement_present :
    Trace initial settledAfterLoss ∧ settledAfterLoss.exactPresent = false ∧
      settledAfterLoss.replacementPresent = true ∧ settledAfterLoss.lostReply = true :=
  ⟨.next (.next reaches_replacement (.remove replaced ⟨rfl, rfl, rfl⟩ true))
    (.reconcile replyLost false ⟨rfl, by simp [replyLost]⟩), rfl, rfl, rfl⟩

theorem hosted_retry_reaches_settlement_from_a_missing_head :
    ∃ before after, Trace initial before ∧ before.lostReply = true ∧ before.replacementPresent = false ∧
      HostedHeadAllowsRetry none ∧ Step before after ∧ after.settled = true := by
  let before := { drained with exactPresent := false, lostReply := true }
  let after := { before with settled := true }
  refine ⟨before, after, ?_, rfl, rfl, Or.inr rfl, .reconcile before false ⟨rfl, ?_⟩, rfl⟩
  · exact .next (.next (.next (.empty initial) (.order initial ObjectWithdrawal.withdrawn
      ObjectWithdrawal.reachable_fence)) (.drain fenced rfl)) (.remove drained ⟨rfl, rfl, rfl⟩ true)
  · simp [before]

theorem hosted_retry_skips_a_visible_mismatch : ¬ HostedHeadAllowsRetry (some false) := by
  simp [HostedHeadAllowsRetry]

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
    .next reaches_replacement (.remove replaced ⟨rfl, rfl, rfl⟩ true), ⟨rfl, rfl⟩, rfl, rfl⟩

theorem lease_expiry_alone_allows_an_outstanding_provider_read :
    ∃ start leaseEnd drift disposedAt completedAt : Nat,
      start < leaseEnd ∧ DisposalLease.MayDispose leaseEnd drift disposedAt ∧ disposedAt < completedAt :=
  ⟨9, 10, 1, 11, 12, by decide, (by decide : 10 + 1 ≤ 11), by decide⟩

#print axioms trace_preserves_safety
#print axioms trace_preserves_a_present_replacement
#print axioms lost_reply_reconciles_with_replacement_present
#print axioms hosted_retry_reaches_settlement_from_a_missing_head
#print axioms hosted_retry_skips_a_visible_mismatch
#print axioms incomplete_listing_can_settle_a_present_target
#print axioms path_only_retry_can_remove_a_replacement
#print axioms lease_expiry_alone_allows_an_outstanding_provider_read

end ExactReclamation
