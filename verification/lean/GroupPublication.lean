import Std

namespace RTDB.GroupPublication

def binds {Identity : Type} [DecidableEq Identity] (observed : Identity) (records : List (Option Identity)) : Bool :=
  !records.isEmpty && records.all (fun publication => decide (publication = some observed))

theorem a_binding_covers_each_held_record {Identity : Type} [DecidableEq Identity]
    (observed : Identity) (records : List (Option Identity)) :
    binds observed records = true ↔ records ≠ [] ∧ ∀ publication ∈ records, publication = some observed := by
  simp [binds, List.all_eq_true]

theorem a_shared_publication_can_bind_all_agreeing_members :
    binds (17 : Nat) [some 17, some 17, some 17, some 17] = true := by decide

theorem an_unwitnessed_group_cannot_bind :
    binds (17 : Nat) [] = false := by decide

theorem a_legacy_member_cannot_bind_the_group :
    binds (17 : Nat) [some 17, none, some 17] = false := by decide

theorem several_versions_cannot_bind_the_same_group :
    binds (17 : Nat) [some 17, some 18, some 17] = false := by decide

#print axioms a_binding_covers_each_held_record
#print axioms a_shared_publication_can_bind_all_agreeing_members
#print axioms an_unwitnessed_group_cannot_bind
#print axioms a_legacy_member_cannot_bind_the_group
#print axioms several_versions_cannot_bind_the_same_group

end RTDB.GroupPublication
