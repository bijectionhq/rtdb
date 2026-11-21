import Std

namespace RTDB.QueuedReadBack

structure Entry where
  sequence : Nat
  staged : Nat
  deriving DecidableEq

structure Flow where
  pending : List Entry
  awaitingDurability : List Entry
  unread : List Entry
  held : List Entry
  published : List Entry
  replayCandidates : List Entry
  leading : Bool

def ordered (flow : Flow) : Prop :=
  flow.pending = flow.held ++ flow.unread ++ flow.awaitingDurability

def initial : Flow := ⟨[], [], [], [], [], [], true⟩

inductive Step : Flow → Flow → Prop
  | stage (flow : Flow) (entry : Entry) (leading : flow.leading = true) :
      Step flow { flow with pending := flow.pending ++ [entry], awaitingDurability := flow.awaitingDurability ++ [entry] }
  | becameDurable (flow : Flow) (entry : Entry) (tail : List Entry)
      (next : flow.awaitingDurability = entry :: tail) :
      Step flow { flow with awaitingDurability := tail, unread := flow.unread ++ [entry] }
  | delivered (flow : Flow) (entry : Entry) (tail : List Entry)
      (next : flow.unread = entry :: tail) :
      Step flow { flow with unread := tail, held := flow.held ++ [entry] }
  | publish (flow : Flow) (entry : Entry) (pendingRest heldRest : List Entry)
      (receipt : flow.held = entry :: heldRest) (head : flow.pending = entry :: pendingRest) :
      Step flow { flow with pending := pendingRest, held := heldRest, published := flow.published ++ [entry] }
  | resign (flow : Flow) (leading : flow.leading = true) :
      Step flow { flow with
        pending := [], awaitingDurability := [], unread := [], held := [], leading := false,
        replayCandidates := flow.held ++ flow.unread }

theorem step_preserves_order {before after : Flow} (beforeOrder : ordered before)
    (step : Step before after) : ordered after := by
  unfold ordered at beforeOrder
  cases step with
  | stage => simp [ordered, beforeOrder, List.append_assoc]
  | becameDurable _ _ next => simp [ordered, beforeOrder, next, List.append_assoc]
  | delivered _ _ next => simp [ordered, beforeOrder, next, List.append_assoc]
  | publish _ _ _ receipt head =>
      simpa [ordered, receipt, head] using beforeOrder
  | resign => rfl

inductive Reachable : Flow → Prop
  | start : Reachable initial
  | advance {before after} : Reachable before → Step before after → Reachable after

theorem reachable_flow_is_ordered {flow : Flow} (trace : Reachable flow) : ordered flow := by
  induction trace with
  | start => rfl
  | advance _ step prior => exact step_preserves_order prior step

theorem read_head_is_pending_head {flow : Flow} (trace : Reachable flow)
    (entry : Entry) (heldRest : List Entry) (receipt : flow.held = entry :: heldRest) :
    flow.pending = entry :: (heldRest ++ flow.unread ++ flow.awaitingDurability) := by
  have orderedFlow := reachable_flow_is_ordered trace
  simpa [ordered, receipt] using orderedFlow

theorem successful_publication_consumes_the_first_held_entry {before after : Flow}
    (step : Step before after) (changed : after.published ≠ before.published) :
    ∃ entry rest, before.held = entry :: rest ∧ after.published = before.published ++ [entry] := by
  cases step with
  | publish entry _ rest receipt _ => exact ⟨entry, rest, receipt, rfl⟩
  | stage | becameDurable | delivered | resign => exact (changed rfl).elim

theorem resignation_preserves_accepted_and_unread_durable_entries (before : Flow)
    (leading : before.leading = true) :
    ∃ after, Step before after ∧ after.published = before.published ∧ after.pending = [] ∧
      after.replayCandidates = before.held ++ before.unread ∧ after.leading = false :=
  ⟨_, Step.resign before leading, rfl, rfl, rfl, rfl⟩

#print axioms step_preserves_order
#print axioms reachable_flow_is_ordered
#print axioms read_head_is_pending_head
#print axioms successful_publication_consumes_the_first_held_entry
#print axioms resignation_preserves_accepted_and_unread_durable_entries

end RTDB.QueuedReadBack
