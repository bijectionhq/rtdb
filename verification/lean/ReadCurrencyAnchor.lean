import Std

namespace RTDB.ReadCurrencyAnchor

structure Observation where
  sampledAt : Nat
  required : Nat
  tip : Nat

structure State where
  installed : Nat
  pending : Option Observation
  anchor : Option Observation

def keepGreaterTip (current : Option Observation) (new : Observation) : Option Observation :=
  match current with
  | none => some new
  | some old => if old.tip < new.tip then some new else some old

def keepLaterTime (current : Option Observation) (new : Observation) : Option Observation :=
  match current with
  | none => some new
  | some old => if new.sampledAt < old.sampledAt then some old else some new

def Safe (state : State) : Prop :=
  (∀ obs, state.pending = some obs → obs.required ≤ obs.tip) ∧
  (∀ obs, state.anchor = some obs → obs.required ≤ obs.tip ∧ obs.tip ≤ state.installed)

def anchorTime (anchor : Option Observation) : Nat :=
  match anchor with
  | none => 0
  | some observation => observation.sampledAt

inductive Step : State → State → Prop where
  | install (state : State) (next : Nat) :
      state.installed ≤ next → Step state { state with installed := next }
  | record (state : State) (obs : Observation) :
      obs.required ≤ obs.tip →
      Step state { state with pending := keepGreaterTip state.pending obs }
  | confirm (state : State) (obs : Observation) :
      state.pending = some obs → obs.tip ≤ state.installed →
      Step state { state with anchor := keepLaterTime state.anchor obs, pending := none }

def initial : State := ⟨0, none, none⟩

inductive Reachable : State → Prop where
  | start : Reachable initial
  | next {previous current} : Reachable previous → Step previous current → Reachable current

inductive History : State → State → Prop where
  | empty (state) : History state state
  | next {initial before after} : History initial before → Step before after → History initial after

private theorem keepGreaterTip_selects_current_or_new (current : Option Observation) (new : Observation) :
    keepGreaterTip current new = current ∨ keepGreaterTip current new = some new := by
  cases current with
  | none => exact Or.inr rfl
  | some old =>
    by_cases next : old.tip < new.tip
    · exact Or.inr (by simp [keepGreaterTip, next])
    · exact Or.inl (by simp [keepGreaterTip, next])

private theorem keepLaterTime_selects_current_or_new (current : Option Observation) (new : Observation) :
    keepLaterTime current new = current ∨ keepLaterTime current new = some new := by
  cases current with
  | none => exact Or.inr rfl
  | some old =>
    by_cases earlier : new.sampledAt < old.sampledAt <;> simp [keepLaterTime, earlier]

theorem currency_step_preserves_coverage {before after : State} (prior : Safe before)
    (step : Step before after) : Safe after := by
  rcases prior with ⟨pending, anchor⟩
  cases step with
  | install next advances =>
    refine ⟨pending, ?_⟩
    intro obs observed
    obtain ⟨covered, installed⟩ := anchor obs observed
    exact ⟨covered, Nat.le_trans installed advances⟩
  | record new justified =>
    refine ⟨?_, anchor⟩
    intro obs selected
    change keepGreaterTip before.pending new = some obs at selected
    rcases keepGreaterTip_selects_current_or_new before.pending new with old | same
    · exact pending obs (old.symm.trans selected)
    · have equal : new = obs := Option.some.inj (same.symm.trans selected)
      exact equal ▸ justified
  | confirm new observed reached =>
    refine ⟨by simp, ?_⟩
    intro obs selected
    change keepLaterTime before.anchor new = some obs at selected
    rcases keepLaterTime_selects_current_or_new before.anchor new with old | same
    · exact anchor obs (old.symm.trans selected)
    · have equal : new = obs := Option.some.inj (same.symm.trans selected)
      exact equal ▸ ⟨pending new observed, reached⟩

theorem every_confirmed_anchor_covers_its_required_prefix {state : State} (history : Reachable state) :
    Safe state := by
  induction history with
  | start => simp [Safe, initial]
  | next _ transition prior => exact currency_step_preserves_coverage prior transition

theorem step_does_not_rewind_the_anchor {before after : State} (step : Step before after) :
    anchorTime before.anchor ≤ anchorTime after.anchor := by
  cases step with
  | install _ _ => exact Nat.le_refl _
  | record _ _ => exact Nat.le_refl _
  | confirm new _ _ =>
    change anchorTime before.anchor ≤ anchorTime (keepLaterTime before.anchor new)
    cases h : before.anchor with
    | none => simp [keepLaterTime, anchorTime]
    | some old =>
      by_cases earlier : new.sampledAt < old.sampledAt
      · simp [keepLaterTime, anchorTime, earlier]
      · simpa [keepLaterTime, anchorTime, h, earlier] using Nat.le_of_not_gt earlier

theorem a_read_anchor_never_rewinds_over_a_history {before after : State} (history : History before after) :
    anchorTime before.anchor ≤ anchorTime after.anchor := by
  induction history with
  | empty => exact Nat.le_refl _
  | next _ step prior => exact Nat.le_trans prior (step_does_not_rewind_the_anchor step)

#print axioms currency_step_preserves_coverage
#print axioms every_confirmed_anchor_covers_its_required_prefix
#print axioms step_does_not_rewind_the_anchor
#print axioms a_read_anchor_never_rewinds_over_a_history

end RTDB.ReadCurrencyAnchor
