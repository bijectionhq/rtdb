import ErasureCommit
import Logkeeper.SelectionFence

set_option warningAsError true

namespace InstalledErasureCommit

open Logkeeper.SelectionFence

structure Attempt where
  identity : Nat
  checkpoint : Nat
  successorGeneration : Nat
deriving DecidableEq

structure Reservation where
  attempt : Attempt
  due : Nat
  retiring : Nat
  draining : Nat

structure Commitment where
  attempt : Attempt
  orderedSwitch : Nat
  committedAt : Nat
  enforcedDeadline : Option Nat
  retiring : Nat
  draining : Nat

def AtCleanCheckpoint (checkpoint generation : Nat) (attempt : Attempt) : Prop :=
  checkpoint = attempt.checkpoint ∧ generation = attempt.successorGeneration ∧ 1 < generation

def FixedDeadlineAdmits (requested reserved : Nat) : Prop :=
  0 < reserved ∧ reserved ≤ requested

def SameAttempt (attempt : Attempt) (orderedSwitch : Nat) (commit : Commitment) : Prop :=
  commit.attempt = attempt ∧ commit.orderedSwitch = orderedSwitch

def SameReservation (reservation : Reservation) (commit : Commitment) : Prop :=
  commit.enforcedDeadline = some reservation.due ∧
    commit.retiring = reservation.retiring ∧ commit.draining = reservation.draining

def FixedCommitAdmits (requested : Nat) (commit : Commitment) : Prop :=
  ∃ due, commit.enforcedDeadline = some due ∧ commit.committedAt < due ∧ due ≤ requested

def CompletionStatus (enforcedDeadline : Option Nat) (destroyedAt : Nat) : Option Bool :=
  enforcedDeadline.map fun due => decide (destroyedAt ≤ due)

def NamespaceGrantsDrained (expiresAt : List Int) (now : Int) : Prop :=
  ∀ expiry ∈ expiresAt, expiry ≤ now

def OrderedHoldsDrained (outstanding : Nat) : Prop := outstanding = 0

def WithdrawalFollowsJoin (withdrawnAt latestJoin : Nat) : Prop := latestJoin ≤ withdrawnAt

def DestructionFollowsDrain (committedAt closedAt destroyedAt : Nat) : Prop :=
  committedAt ≤ closedAt ∧ closedAt ≤ destroyedAt

def ObservedDuringDrain (closedAt observedAt destroyedAt : Nat) : Prop :=
  closedAt ≤ observedAt ∧ observedAt ≤ destroyedAt

theorem authenticated_rotation_covers_recovery
    {roster : List Voter} {fence : Fence Prefix}
    {rows : List (VoterEvidence Voter Digest)}
    {read : Voter → Bool → Option (SlotRead Prefix Digest)}
    (evidence : RotationEvidence roster fence rows read)
    (retired checkpoint generation : Nat) (attempt : Attempt) (exact : AtCleanCheckpoint checkpoint generation attempt)
    (next : attempt.successorGeneration = retired + 1) (selectable : List ErasureCommit.Image)
    (opens : ∀ image ∈ selectable, ∃ row ∈ rows, ∃ slot : Bool,
      ∀ observed, read row.voter slot = some observed → AtFence fence observed →
        generation ≤ image.root)
    (fenced : ErasureCommit.Fenced selectable) :
    roster.length < 2 * rows.length ∧
      ∀ image ∈ selectable, ErasureCommit.Readable retired image := by
  obtain ⟨majority, _, witnesses⟩ := rotation_evidence_names_a_majority_with_both_exact_reads evidence
  refine ⟨majority, ErasureCommit.every_selectable_recovery_avoids_the_revoked_generation retired selectable ?_ fenced⟩
  intro image listed
  obtain ⟨row, member, slot, inherits⟩ := opens image listed
  obtain ⟨observed, found, _, promised, accepted, digest⟩ := (witnesses row member).2 slot
  have clean := inherits observed found ⟨promised, accepted, digest⟩
  have sameGeneration := exact.2.1
  omega

theorem a_fixed_commit_keeps_the_reserved_attempt
    (requested orderedSwitch : Nat) (reservation : Reservation) (commit : Commitment)
    (admission : FixedDeadlineAdmits requested reservation.due)
    (attempt : SameAttempt reservation.attempt orderedSwitch commit)
    (reserved : SameReservation reservation commit)
    (armed : FixedCommitAdmits requested commit) :
    commit.attempt = reservation.attempt ∧ commit.orderedSwitch = orderedSwitch ∧
      commit.retiring = reservation.retiring ∧ commit.draining = reservation.draining ∧
      commit.committedAt < reservation.due ∧ reservation.due ≤ requested := by
  obtain ⟨due, deadline, later, _⟩ := armed
  have exactDeadline : due = reservation.due := Option.some.inj (deadline.symm.trans reserved.1)
  exact ⟨attempt.1, attempt.2, reserved.2.1, reserved.2.2, exactDeadline ▸ later, admission.2⟩

theorem fixed_completion_reports_late (due destroyedAt : Nat) (late : due < destroyedAt) :
    CompletionStatus (some due) destroyedAt = some false := by
  simp [CompletionStatus, Nat.not_le.mpr late]

theorem an_operational_target_claims_no_deadline (destroyedAt : Nat) :
    CompletionStatus none destroyedAt = none := rfl

theorem serving_retirement_waits_for_all_grants_and_original_hold
    (expiresAt : List Int) (fenceObserved horizon now : Int)
    (originalHoldActive : Prop) (outstanding : Nat)
    (bound : ∀ expiry ∈ expiresAt, expiry ≤ fenceObserved + horizon)
    (elapsed : fenceObserved + horizon ≤ now)
    (countsOriginalHold : originalHoldActive → 0 < outstanding)
    (drained : OrderedHoldsDrained outstanding) :
    NamespaceGrantsDrained expiresAt now ∧ ¬ originalHoldActive := by
  refine ⟨fun expiry listed => Int.le_trans (bound expiry listed) elapsed, ?_⟩
  intro active
  have counted := countsOriginalHold active
  unfold OrderedHoldsDrained at drained
  omega

theorem destruction_follows_every_observed_join (roster : List Nat) (observed : Nat → Nat)
    (committedAt closedAt destroyedAt : Nat) (ordered : DestructionFollowsDrain committedAt closedAt destroyedAt)
    (joins : ∀ duty ∈ roster, ObservedDuringDrain closedAt (observed duty) destroyedAt) :
    ∀ duty ∈ roster, committedAt ≤ observed duty ∧ observed duty ≤ destroyedAt := by
  intro duty member
  exact ⟨Nat.le_trans ordered.1 (joins duty member).1, (joins duty member).2⟩

private def sampleAttempt : Attempt := ⟨7, 30, 2⟩
private def sampleReservation : Reservation := ⟨sampleAttempt, 50, 3, 4⟩
private def sampleCommit : Commitment := ⟨sampleAttempt, 5, 40, some 50, 3, 4⟩

theorem exact_fixed_commit_and_completion_are_reachable :
    FixedDeadlineAdmits 55 sampleReservation.due ∧ SameAttempt sampleReservation.attempt 5 sampleCommit ∧
    SameReservation sampleReservation sampleCommit ∧ FixedCommitAdmits 55 sampleCommit ∧
    CompletionStatus sampleCommit.enforcedDeadline 50 = some true := by
  exact ⟨⟨by decide, by decide⟩, ⟨rfl, rfl⟩, ⟨rfl, rfl, rfl⟩, ⟨50, rfl, by decide, by decide⟩, rfl⟩

theorem changed_attempt_control :
    ¬ SameAttempt { sampleAttempt with identity := 8 } 5 sampleCommit := by
  intro mixed
  have := congrArg Attempt.identity mixed.1
  contradiction

theorem unjoined_duty_and_live_hold_controls :
    ¬ ObservedDuringDrain 40 51 50 ∧ ¬ OrderedHoldsDrained 1 ∧ ¬ WithdrawalFollowsJoin 50 51 := by
  unfold ObservedDuringDrain OrderedHoldsDrained WithdrawalFollowsJoin
  omega

theorem a_missing_slot_forbids_mirrors_fence
    {fence : Fence Prefix} {row : VoterEvidence Voter Digest}
    {read : Bool → Option (SlotRead Prefix Digest)} (slot : Bool) (missing : read slot = none) :
    ¬ MirrorsFence fence row read := by
  intro mirrors
  obtain ⟨observed, found, _⟩ := mirrors.2 slot
  rw [missing] at found
  contradiction

theorem grant_still_live_at_fence_control :
    ¬ NamespaceGrantsDrained [41, 49] 40 := by
  intro drained
  have live := drained 49 (by simp)
  omega

theorem stale_generation_control : ¬ AtCleanCheckpoint 30 1 sampleAttempt := by
  intro atFence
  have := atFence.2.1
  contradiction

#print axioms authenticated_rotation_covers_recovery
#print axioms a_fixed_commit_keeps_the_reserved_attempt
#print axioms fixed_completion_reports_late
#print axioms an_operational_target_claims_no_deadline
#print axioms serving_retirement_waits_for_all_grants_and_original_hold
#print axioms destruction_follows_every_observed_join
#print axioms exact_fixed_commit_and_completion_are_reachable
#print axioms changed_attempt_control
#print axioms unjoined_duty_and_live_hold_controls
#print axioms a_missing_slot_forbids_mirrors_fence
#print axioms grant_still_live_at_fence_control
#print axioms stale_generation_control

end InstalledErasureCommit
