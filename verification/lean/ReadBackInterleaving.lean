import Std

namespace RTDB.ReadBackInterleaving

inductive Delivered where
  | original
  | substituted
  deriving DecidableEq

structure Flow where
  stamped : Bool
  exposed : Bool
  receiptSeen : Bool
  active : Bool
  held : Option Delivered
  imported : Option Delivered
  deriving DecidableEq

def initial : Flow := ⟨false, false, false, true, none, none⟩

def safe (flow : Flow) : Prop :=
  (flow.exposed = true → flow.stamped = true) ∧
  (flow.held = none ∨ flow.held = some .original) ∧
  (flow.imported = none ∨ flow.imported = some .original)

inductive Event where
  | stamp
  | expose
  | finishAppend
  | receive (frame : Delivered) (resolverBorrowed : Bool)
  | endTerm

def step (flow : Flow) : Event → Flow
  | .stamp => if flow.active then { flow with stamped := true } else flow
  | .expose => if flow.stamped then { flow with exposed := true } else flow
  | .finishAppend => { flow with receiptSeen := true }
  | .receive frame resolverBorrowed =>
      if flow.active && flow.exposed then
        if frame = .original then
          if resolverBorrowed then { flow with held := some frame }
          else { flow with imported := some frame }
        else flow
      else flow
  | .endTerm => { flow with active := false, imported := flow.held.or flow.imported, held := none }

theorem transition_preserves_safe (before : Flow) (safeBefore : safe before) (event : Event) :
    safe (step before event) := by
  cases event with
  | stamp =>
      by_cases active : before.active = true <;> simp_all [step, safe]
  | expose =>
      by_cases stamp : before.stamped = true <;> simp_all [step, safe]
  | finishAppend => exact safeBefore
  | receive frame borrowed =>
      cases active : before.active <;> cases exposed : before.exposed <;>
        cases receipt : before.receiptSeen <;> cases frame <;> cases borrowed <;> simp_all [step, safe]
  | endTerm =>
      simp only [safe, step] at *
      rcases safeBefore with ⟨visible, held, imported⟩
      refine ⟨visible, Or.inl trivial, ?_⟩
      rcases held with empty | original <;> simp_all

theorem every_schedule_safe (events : List Event) : safe (events.foldl step initial) := by
  have go (remaining : List Event) (flow : Flow) (invariant : safe flow) :
      safe (remaining.foldl step flow) := by
    induction remaining generalizing flow with
    | nil => exact invariant
    | cons action tail induction =>
        exact induction (step flow action) (transition_preserves_safe flow invariant action)
  exact go events initial (by simp [safe, initial])

theorem a_visible_frame_can_apply_before_its_receipt :
    let early := [Event.stamp, .expose, .receive .original false].foldl step initial
    early.imported = some .original ∧ early.receiptSeen = false := by
  exact ⟨rfl, rfl⟩

theorem an_ended_term_step_preserves_import (before : Flow) (ended : before.active = false)
    (drained : before.held = none) (event : Event) :
    let after := step before event
    after.active = false ∧ after.held = none ∧ after.imported = before.imported := by
  cases event with
  | expose => by_cases stamped : before.stamped = true <;> simp [step, ended, drained, stamped]
  | stamp | finishAppend | receive | endTerm => simp [step, ended, drained]

theorem an_ended_term_preserves_import_under_any_schedule (events : List Event) (before : Flow)
    (ended : before.active = false) (drained : before.held = none) :
    (events.foldl step before).imported = before.imported := by
  induction events generalizing before with
  | nil => rfl
  | cons event tail ih =>
      obtain ⟨inactive, empty, same⟩ := an_ended_term_step_preserves_import before ended drained event
      exact (ih (step before event) inactive empty).trans same

#print axioms transition_preserves_safe
#print axioms every_schedule_safe
#print axioms a_visible_frame_can_apply_before_its_receipt
#print axioms an_ended_term_step_preserves_import
#print axioms an_ended_term_preserves_import_under_any_schedule

end RTDB.ReadBackInterleaving
