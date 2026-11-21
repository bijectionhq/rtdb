import Std

namespace RTDB.Recovery

structure EpochRun where
  start : Nat
  epoch : Nat

def epochAt (lsn : Nat) : List EpochRun → Option Nat
  | [] => none
  | run :: older => if run.start ≤ lsn then some run.epoch else epochAt lsn older

theorem epochAt_step (newestFirst : List EpochRun) (lsn : Nat)
    (noNewRun : ∀ run ∈ newestFirst, run.start ≠ lsn + 1) :
    epochAt (lsn + 1) newestFirst = epochAt lsn newestFirst := by
  induction newestFirst with
  | nil => rfl
  | cons run older ih =>
    have unchanged : (run.start ≤ lsn + 1) ↔ (run.start ≤ lsn) := by
      have notAtNext := noNewRun run (List.mem_cons_self ..)
      omega
    have olderUnchanged := ih (fun run member => noNewRun run (List.mem_cons_of_mem _ member))
    simp [epochAt, unchanged, olderUnchanged]

def IsBoundary (server history : List EpochRun) (start lsn : Nat) : Prop :=
  lsn = start ∨ ∃ run ∈ server ++ history, run.start = lsn

theorem matching_boundaries_imply_matching_prefix (server history : List EpochRun)
    (start cutoff : Nat)
    (matching : ∀ lsn, start ≤ lsn → lsn < cutoff → IsBoundary server history start lsn →
      epochAt lsn server = epochAt lsn history) :
    ∀ lsn, start ≤ lsn → lsn < cutoff → epochAt lsn server = epochAt lsn history := by
  have byOffset : ∀ offset, start + offset < cutoff →
      epochAt (start + offset) server = epochAt (start + offset) history := by
    intro offset
    induction offset with
    | zero =>
      intro below
      exact matching start (by omega) (by omega) (Or.inl rfl)
    | succ offset ih =>
      intro below
      let next := start + offset + 1
      change next < cutoff at below
      change epochAt next server = epochAt next history
      by_cases boundary : IsBoundary server history start next
      · exact matching next (by dsimp [next]; omega) below boundary
      · have serverStep := epochAt_step server (start + offset) (fun run member atNext =>
          boundary (Or.inr ⟨run, List.mem_append_left _ member, atNext⟩))
        have historyStep := epochAt_step history (start + offset) (fun run member atNext =>
          boundary (Or.inr ⟨run, List.mem_append_right _ member, atNext⟩))
        exact serverStep.trans ((ih (by dsimp [next] at below; omega)).trans historyStep.symm)
  intro lsn inRange below
  have point : start + (lsn - start) = lsn := by omega
  simpa only [point] using byOffset (lsn - start) (by omega)

theorem first_differing_boundary_is_first_difference (server history : List EpochRun)
    (start boundary : Nat)
    (earlierMatch : ∀ lsn, start ≤ lsn → lsn < boundary → IsBoundary server history start lsn →
      epochAt lsn server = epochAt lsn history)
    (differs : epochAt boundary server ≠ epochAt boundary history) :
    (∀ lsn, start ≤ lsn → lsn < boundary → epochAt lsn server = epochAt lsn history) ∧
      epochAt boundary server ≠ epochAt boundary history :=
  ⟨matching_boundaries_imply_matching_prefix server history start boundary earlierMatch, differs⟩

theorem first_difference_occurs_at_a_boundary (server history : List EpochRun)
    (start lsn : Nat) (inRange : start ≤ lsn)
    (earlierMatch : ∀ earlier, start ≤ earlier → earlier < lsn →
      epochAt earlier server = epochAt earlier history)
    (differs : epochAt lsn server ≠ epochAt lsn history) : IsBoundary server history start lsn := by
  apply Classical.byContradiction
  intro notBoundary
  have pastStart : lsn ≠ start := fun atStart => notBoundary (Or.inl atStart)
  have previous : lsn - 1 + 1 = lsn := by omega
  have serverStep := epochAt_step server (lsn - 1) (fun run member atNext =>
      notBoundary (Or.inr ⟨run, List.mem_append_left _ member, atNext.trans previous⟩))
  have historyStep := epochAt_step history (lsn - 1) (fun run member atNext =>
      notBoundary (Or.inr ⟨run, List.mem_append_right _ member, atNext.trans previous⟩))
  rw [previous] at serverStep historyStep
  exact differs (serverStep.trans ((earlierMatch (lsn - 1) (by omega) (by omega)).trans historyStep.symm))

end RTDB.Recovery
