import Lean

namespace MonotoneReferenceManifest

variable {Name Owner : Type}

abbrev References (Name Owner : Type) := Name → Owner → Prop

def withdraw (current requested : References Name Owner) : References Name Owner :=
  fun name owner => current name owner ∧ ¬ requested name owner

theorem a_withdrawal_cannot_add_a_reference (current requested : References Name Owner)
    (name : Name) (owner : Owner) (retained : withdraw current requested name owner) :
    current name owner := by
  simp only [withdraw] at retained
  first | exact retained.1 | exact retained

theorem a_requested_current_edge_is_removed (current requested : References Name Owner)
    (name : Name) (owner : Owner) (releasing : requested name owner) :
    ¬ withdraw current requested name owner := by
  simp [withdraw, releasing]

theorem another_owner_is_preserved (current requested : References Name Owner)
    (name : Name) (owner : Owner) (untargeted : ¬ requested name owner) :
    withdraw current requested name owner ↔ current name owner := by
  simp [withdraw, untargeted]

theorem withdrawals_compose_in_either_order (current first second : References Name Owner)
    (name : Name) (owner : Owner) :
    withdraw (withdraw current first) second name owner ↔
      withdraw (withdraw current second) first name owner := by
  simp [withdraw, and_assoc, and_comm, and_left_comm]

theorem a_retried_withdrawal_is_idempotent (current requested : References Name Owner)
    (name : Name) (owner : Owner) :
    withdraw (withdraw current requested) requested name owner ↔ withdraw current requested name owner := by
  simp [withdraw, and_assoc]

def mayPrune (slot confirmed : Nat) : Prop := slot < confirmed

theorem a_confirmed_head_is_never_pruned (slot : Nat) : ¬ mayPrune slot slot := by
  simp [mayPrune]

theorem a_confirmed_cleanup_cannot_remove_a_future_head (slot confirmed current : Nat)
    (approved : mayPrune slot confirmed) (monotone : confirmed ≤ current) : slot < current := by
  exact Nat.lt_of_lt_of_le approved monotone

#print axioms a_withdrawal_cannot_add_a_reference
#print axioms a_requested_current_edge_is_removed
#print axioms another_owner_is_preserved
#print axioms withdrawals_compose_in_either_order
#print axioms a_retried_withdrawal_is_idempotent
#print axioms a_confirmed_head_is_never_pruned
#print axioms a_confirmed_cleanup_cannot_remove_a_future_head

end MonotoneReferenceManifest
