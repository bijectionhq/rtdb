import Std

set_option warningAsError true

namespace SurvivorCheckpoint

abbrev Position := Int × Nat

def keepInheritedEvent (selectedOrigin : Bool) : Prop := selectedOrigin = false

def beforeMarker (position marker : Position) : Prop :=
  position.1 < marker.1 ∨ (position.1 = marker.1 ∧ position.2 < marker.2)

def keepLocalEvent (position : Position) (marker : Option Position) : Prop :=
  match marker with
  | none => True
  | some before => ¬ beforeMarker position before

def writeSurvivorTrie (rows : Nat) : Prop := 0 < rows

def freshSurvivorAllocation {Allocation : Type} (candidate previous : Allocation) : Prop :=
  candidate ≠ previous

def declaresWrittenMember {Address : Type} (observed expected : Address)
    (observedBytes expectedBytes : Nat) : Prop :=
  observed = expected ∧ observedBytes = expectedBytes

def survivorEvents {Head Event : Type} (events : Head → Event → Prop) (exactHead : Head)
    (selectedOrigin : Event → Bool) (position : Event → Position) (marker : Event → Option Position) :
    Event → Prop :=
  fun event => events exactHead event ∧
    keepInheritedEvent (selectedOrigin event) ∧ keepLocalEvent (position event) (marker event)

def survivorMembers {Allocation Path : Type} (allocation : Allocation) (produced : Path → Prop) :
    Allocation × Path → Prop :=
  fun member => member.1 = allocation ∧ produced member.2

theorem selects_exactly_the_surviving_events {Head Event : Type}
    (events : Head → Event → Prop) (head : Head) (selectedOrigin : Event → Bool)
    (position : Event → Position) (marker : Event → Option Position) (event : Event) :
    survivorEvents events head selectedOrigin position marker event ↔
      events head event ∧ selectedOrigin event = false ∧
        (∀ before, marker event = some before → ¬ beforeMarker (position event) before) := by
  cases h : marker event <;> simp [survivorEvents, keepInheritedEvent, keepLocalEvent, h]

theorem keeps_the_marker_and_later_events (tx : Int) (markerRow eventRow : Nat)
    (later : markerRow ≤ eventRow) : keepLocalEvent (tx, eventRow) (some (tx, markerRow)) := by
  simp [keepLocalEvent, beforeMarker, Nat.not_lt.mpr later]

theorem writes_a_trie_exactly_for_nonempty_survivors (rows : Nat) :
    writeSurvivorTrie rows ↔ rows ≠ 0 := by
  exact Nat.pos_iff_ne_zero

theorem declares_exactly_written_members {Allocation Path : Type}
    (candidate previous : Allocation) (produced : Path → Prop) (member : Allocation × Path)
    (fresh : freshSurvivorAllocation candidate previous) :
    (survivorMembers candidate produced member ↔ member.1 = candidate ∧ produced member.2) ∧
      (survivorMembers candidate produced member → member.1 ≠ previous) := by
  refine ⟨Iff.rfl, ?_⟩
  intro declared
  rw [declared.1]
  exact fresh

theorem rejects_a_same_extent_old_member {Allocation Path : Type}
    (candidate previous : Allocation) (path : Path) (size : Nat)
    (fresh : freshSurvivorAllocation candidate previous) :
    ¬ declaresWrittenMember (previous, path) (candidate, path) size size := by
  intro declared
  exact fresh (congrArg Prod.fst declared.1).symm

theorem a_selected_head_can_keep_one_event_and_omit_another :
    let events := fun (head event : Nat) => event ≤ head
    let selected := fun (event : Nat) => decide (event = 0)
    let position := fun (event : Nat) => ((event : Int), (0 : Nat))
    let markers := fun (_ : Nat) => (none : Option Position)
    survivorEvents events 1 selected position markers 1 ∧
      ¬ survivorEvents events 1 selected position markers 0 ∧
      ¬ survivorEvents events 1 selected position markers 2 := by
  simp [survivorEvents, keepInheritedEvent, keepLocalEvent]

#print axioms selects_exactly_the_surviving_events
#print axioms keeps_the_marker_and_later_events
#print axioms writes_a_trie_exactly_for_nonempty_survivors
#print axioms declares_exactly_written_members
#print axioms rejects_a_same_extent_old_member
#print axioms a_selected_head_can_keep_one_event_and_omit_another

end SurvivorCheckpoint
