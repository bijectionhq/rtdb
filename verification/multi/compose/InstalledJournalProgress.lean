import MaintenanceClassification
import DisposalLease

set_option warningAsError true

namespace InstalledJournalProgress

open MaintenanceBound

theorem funded_maintenance_admits_next_record {receiverBudget : Nat} (lane : SettlementLane)
    (reached : SettlementReaches receiverBudget lane) (body : ReplicaKind)
    (allowed : maintenance_append_allowed lane.checkpoint.phase lane.slot body)
    (charging : record_charge body = 1)
    (retainedCharge nextCharge maxRecordCharge fundedBytes : Nat)
    (retainedBound : retainedCharge ≤ lane.used * maxRecordCharge)
    (nextBound : nextCharge ≤ maxRecordCharge)
    (funded : funded_records receiverBudget * maxRecordCharge ≤ fundedBytes) :
    retainedCharge + nextCharge ≤ fundedBytes := by
  have room := reachable_maintenance_append_fits reached body allowed
  rw [charging] at room
  calc
    retainedCharge + nextCharge ≤ lane.used * maxRecordCharge + maxRecordCharge :=
      Nat.add_le_add retainedBound nextBound
    _ = (lane.used + 1) * maxRecordCharge := by rw [Nat.add_mul, Nat.one_mul]
    _ ≤ funded_records receiverBudget * maxRecordCharge := Nat.mul_le_mul_right _ room
    _ ≤ fundedBytes := funded

def ReachesTime (clock : Nat → Nat) (start threshold : Nat) : Prop :=
  ∃ n ≥ start, threshold ≤ clock n

def BoundedLag (realTime journalTime : Nat → Nat) (start clockError : Nat) : Prop :=
  ∀ n ≥ start, realTime n ≤ journalTime n + clockError

theorem advancing_bounded_clock_reaches_disposal_time (leaseEnd drift clockError : Nat)
    (realTime journalTime : Nat → Nat) (withdrawnAt : Nat)
    (advances : ReachesTime realTime withdrawnAt (leaseEnd + drift + clockError))
    (boundedLag : BoundedLag realTime journalTime withdrawnAt clockError) :
    ∃ n ≥ withdrawnAt, DisposalLease.MayDispose leaseEnd drift (journalTime n) := by
  obtain ⟨n, after, elapsed⟩ := advances
  have bounded := boundedLag n after
  exact ⟨n, after, by unfold DisposalLease.MayDispose; omega⟩

def WeaklyFair (enabled chosen : Nat → Prop) : Prop :=
  ∀ start, (∀ n ≥ start, enabled n) → ∃ n ≥ start, chosen n

theorem withdrawn_holds_are_eventually_disposed (leaseEnd drift clockError : Nat)
    (realTime journalTime : Nat → Nat) (withdrawnAt : Nat)
    (holds custodyAvailable disposeChosen : Nat → Prop)
    (advances : ∃ start ≥ withdrawnAt, ∀ n ≥ start, leaseEnd + drift + clockError ≤ realTime n)
    (boundedLag : BoundedLag realTime journalTime withdrawnAt clockError)
    (liveCustody : ∀ n ≥ withdrawnAt, custodyAvailable n)
    (fairDisposal : WeaklyFair
      (fun n => holds n ∧ custodyAvailable n ∧ DisposalLease.MayDispose leaseEnd drift (journalTime n))
      disposeChosen)
    (releases : ∀ n, disposeChosen n → ¬ holds (n + 1)) :
    ∃ n ≥ withdrawnAt, ¬ holds n := by
  classical
  by_cases released : ∃ n ≥ withdrawnAt, ¬ holds n
  · exact released
  · obtain ⟨start, afterWithdrawal, elapsed⟩ := advances
    obtain ⟨n, afterStart, chosen⟩ := fairDisposal start (by
      intro n afterStart
      have after := Nat.le_trans afterWithdrawal afterStart
      have held : holds n := Classical.byContradiction (fun missing => released ⟨n, after, missing⟩)
      have lag := boundedLag n after
      have realElapsed := elapsed n afterStart
      exact ⟨held, liveCustody n after, by unfold DisposalLease.MayDispose; omega⟩)
    exact ⟨n + 1, by omega, releases n chosen⟩

private def awaitingUpload : SettlementLane := ⟨⟨2, .boundary, 4⟩, .vacant, 0, 2⟩

private theorem awaitingUpload_reachable : SettlementReaches 2 awaitingUpload :=
  .step (.step second_inherited_slot (.fence _ rfl)) (.boundary _ rfl rfl)

theorem funded_lane_admission_controls :
    SettlementReaches 2 awaitingUpload ∧
    maintenance_append_allowed awaitingUpload.checkpoint.phase awaitingUpload.slot .blockUploaded ∧
    (4 ≤ awaitingUpload.used * 1 ∧ 1 ≤ 1 ∧ ¬ (funded_records 2 * 1 ≤ 4) ∧ ¬ (4 + 1 ≤ 4)) ∧
    (4 ≤ awaitingUpload.used * 1 ∧ ¬ (2 ≤ 1) ∧ funded_records 2 * 1 ≤ 5 ∧ ¬ (4 + 2 ≤ 5)) ∧
    (¬ (5 ≤ awaitingUpload.used * 1) ∧ 1 ≤ 1 ∧ funded_records 2 * 1 ≤ 5 ∧ ¬ (5 + 1 ≤ 5)) :=
  ⟨awaitingUpload_reachable, rfl, by decide, by decide, by decide⟩

private def frozen (_ : Nat) : Nat := 0

theorem advancing_real_time_with_frozen_journal_stalls_disposal :
    ReachesTime id 0 4 ∧ ¬ BoundedLag id frozen 0 1 ∧
    ∀ n, ¬ DisposalLease.MayDispose 2 1 (frozen n) := by
  refine ⟨⟨4, by decide, by decide⟩, ?_, ?_⟩
  · intro bounded
    have lag := bounded 2 (by decide)
    change 2 ≤ 1 at lag
    omega
  · simp [frozen, DisposalLease.MayDispose]

theorem synchronized_clocks_without_elapsed_time_stall_disposal :
    ¬ ReachesTime frozen 0 4 ∧ BoundedLag frozen frozen 0 1 ∧
    ∀ n, ¬ DisposalLease.MayDispose 2 1 (frozen n) := by
  simp [ReachesTime, BoundedLag, frozen, DisposalLease.MayDispose]

#print axioms funded_maintenance_admits_next_record
#print axioms advancing_bounded_clock_reaches_disposal_time
#print axioms withdrawn_holds_are_eventually_disposed
#print axioms funded_lane_admission_controls
#print axioms advancing_real_time_with_frozen_journal_stalls_disposal
#print axioms synchronized_clocks_without_elapsed_time_stall_disposal

end InstalledJournalProgress
