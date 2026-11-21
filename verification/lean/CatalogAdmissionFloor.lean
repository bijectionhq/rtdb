import Std

namespace RTDB.CatalogAdmissionFloor

abbrev Cut := Option Nat

def permits : Cut → Cut → Prop
  | none, _ => True
  | some _, none => False
  | some floor, some captured => floor ≤ captured

structure Ledger where
  floor : Cut
  readers : List Cut

def consistent (ledger : Ledger) : Prop :=
  ∀ cut ∈ ledger.readers, permits ledger.floor cut

def coversPreviousFloor (installedFloor next : Cut) : Prop :=
  permits installedFloor next

inductive Step : Ledger → Ledger → Prop where
  | register (state : Ledger) (cut : Cut) (allowed : permits state.floor cut) :
      Step state { state with readers := cut :: state.readers }
  | release (state : Ledger) (remaining : List Cut)
      (subset : ∀ cut ∈ remaining, cut ∈ state.readers) :
      Step state { state with readers := remaining }
  | advance (state : Ledger) (floor : Nat)
      (covered : coversPreviousFloor state.floor (some floor))
      (allAllow : ∀ cut ∈ state.readers, permits (some floor) cut) :
      Step state { state with floor := some floor }

theorem a_step_preserves_held_readers {state next : Ledger}
    (safe : consistent state) (step : Step state next) : consistent next := by
  cases step with
  | register cut allowed =>
      intro captured inReaders
      simp only [List.mem_cons] at inReaders
      rcases inReaders with rfl | earlier
      · exact allowed
      · exact safe _ earlier
  | release remaining subset =>
      intro captured inReaders
      exact safe _ (subset _ inReaders)
  | advance floor covered allAllow =>
      exact allAllow

theorem a_step_never_lowers_the_floor {state next : Ledger}
    (step : Step state next) : coversPreviousFloor state.floor next.floor := by
  cases step with
  | register cut allowed =>
      cases state.floor <;> simp [coversPreviousFloor, permits]
  | release remaining subset =>
      cases state.floor <;> simp [coversPreviousFloor, permits]
  | advance floor covered allAllow =>
      exact covered

inductive Reachable : Ledger → Ledger → Prop where
  | same (state) : Reachable state state
  | next (start previous state) :
      Reachable start previous → Step previous state → Reachable start state

theorem any_history_preserves_held_readers {initial state : Ledger}
    (safe : consistent initial) (history : Reachable initial state) : consistent state := by
  induction history <;> first | exact safe | solve_by_elim [a_step_preserves_held_readers]

theorem no_reader_is_required_to_advance (current proposed : Nat) (later : current ≤ proposed) :
    Step ⟨some current, []⟩ ⟨some proposed, []⟩ := by
  exact .advance _ _ (by first | exact later | trivial) (by simp)

#print axioms a_step_preserves_held_readers
#print axioms a_step_never_lowers_the_floor
#print axioms any_history_preserves_held_readers
#print axioms no_reader_is_required_to_advance

end RTDB.CatalogAdmissionFloor
