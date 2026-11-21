import Std

namespace RTDB.ReceiptedCheckpointStartup

inductive Phase where
  | startup
  | cursor
  | finished
  deriving DecidableEq

structure Recovery where
  phase : Phase
  rootPresent : Bool
  firstRequired : Nat
  firstRetained : Nat

def covered (state : Recovery) : Prop :=
  (state.phase = .startup → state.rootPresent = true) ∧
  (state.phase ≠ .finished → state.firstRetained ≤ state.firstRequired)

inductive Step : Recovery → Recovery → Prop where
  | trim (state) (first : Nat) (keeps : state.phase ≠ .finished → first ≤ state.firstRequired) :
      Step state { state with firstRetained := first }
  | collect (state) (released : state.phase ≠ .startup) :
      Step state { state with rootPresent := false }
  | openCursor (state) (startup : state.phase = .startup) :
      Step state { state with phase := .cursor }
  | finish (state) (cursor : state.phase = .cursor) :
      Step state { state with phase := .finished }

inductive Schedule : Recovery → Recovery → Prop where
  | same (state) : Schedule state state
  | next {start middle endState} : Schedule start middle → Step middle endState → Schedule start endState

theorem each_effect_preserves_recovery {before after : Recovery}
    (step : Step before after) (safe : covered before) : covered after := by
  cases step with
  | trim _ keeps => exact ⟨safe.1, keeps⟩
  | collect released => exact ⟨fun active => False.elim (released active), safe.2⟩
  | openCursor startup =>
      refine ⟨by simp, fun _ => safe.2 ?_⟩
      simp [startup]
  | finish _ => simp [covered]

theorem every_schedule_preserves_recovery {start finish : Recovery}
    (schedule : Schedule start finish) (safe : covered start) : covered finish := by
  induction schedule with
  | same => exact safe
  | next _ step preserved => exact each_effect_preserves_recovery step preserved

#print axioms each_effect_preserves_recovery
#print axioms every_schedule_preserves_recovery

end RTDB.ReceiptedCheckpointStartup
