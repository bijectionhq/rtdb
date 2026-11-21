import Std

namespace ReaderRetention

def leaseAdmits (readerClock skewBound ioBound expiry : Int) : Prop :=
  readerClock + skewBound + ioBound < expiry

theorem lease_read_finishes_before_reclamation
    (admitted retired started readerClock skewBound ioBound duration lifetime : Int)
    (admittedBeforeRetirement : admitted ≤ retired)
    (clockBound : started ≤ readerClock + skewBound)
    (admission : leaseAdmits readerClock skewBound ioBound (admitted + lifetime))
    (completionBound : duration ≤ ioBound) :
    started + duration < retired + lifetime := by
  unfold leaseAdmits at admission
  omega

structure PinState where
  opened : Bool
  active : Nat
  pinned : Bool
  present : Bool

def PinState.safe (s : PinState) : Prop :=
  (s.opened = true ∨ 0 < s.active) → s.pinned = true ∧ s.present = true

def initial : PinState := ⟨false, 0, false, true⟩

inductive PinStep : PinState → PinState → Prop where
  | acquire (s : PinState) (present : s.present = true) :
      PinStep s { s with opened := true, pinned := true }
  | start (s : PinState) (opened : s.opened = true) :
      PinStep s { s with active := s.active + 1 }
  | close (s : PinState) :
      PinStep s { s with opened := false, pinned := decide (0 < s.active) }
  | finish (s : PinState) (hasRead : 0 < s.active) :
      PinStep s { s with active := s.active - 1, pinned := s.opened || decide (1 < s.active) }
  | reclaim (s : PinState) (unpinned : s.pinned = false) :
      PinStep s { s with present := false }

theorem pin_step_preserves_entitlement {before after : PinState}
    (step : PinStep before after) (safe : before.safe) : after.safe := by
  cases step with
  | acquire hasObject =>
    exact fun _ => ⟨rfl, hasObject⟩
  | start opened =>
    exact fun _ => safe (Or.inl opened)
  | close =>
    intro entitled
    have active : 0 < before.active := by
      simpa using entitled
    exact ⟨decide_eq_true active, (safe (Or.inr active)).2⟩
  | finish hasRead =>
    intro entitled
    have pin : (before.opened || decide (1 < before.active)) = true := by
      rcases entitled with opened | active
      · change before.opened = true at opened
        simp [opened]
      · have remaining : 1 < before.active := by
          change 0 < before.active - 1 at active
          omega
        simp [remaining]
    exact ⟨pin, (safe (Or.inr hasRead)).2⟩
  | reclaim unpinned =>
    intro entitled
    have held := (safe entitled).1
    simp_all

inductive Reachable : PinState → Prop where
  | init : Reachable initial
  | next {before after} : Reachable before → PinStep before after → Reachable after

theorem reachable_pin_retains_active_reads {s : PinState} (reachable : Reachable s) : s.safe := by
  induction reachable with
  | init => simp [initial, PinState.safe]
  | next _ step safe => exact pin_step_preserves_entitlement step safe

#print axioms lease_read_finishes_before_reclamation
#print axioms pin_step_preserves_entitlement
#print axioms reachable_pin_retains_active_reads

end ReaderRetention
