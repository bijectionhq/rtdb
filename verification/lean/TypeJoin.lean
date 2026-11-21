import Std

namespace RTDB.Types

structure ScalarFamily (Kind : Type) where
  accepts : Kind → Prop
  allowsNull : Bool

def joinScalars (first second : ScalarFamily Kind) : ScalarFamily Kind :=
  ⟨fun kind => first.accepts kind ∨ second.accepts kind, first.allowsNull || second.allowsNull⟩

def nothing : ScalarFamily Kind := ⟨fun _ => False, false⟩
def null : ScalarFamily Kind := ⟨fun _ => False, true⟩

theorem scalar_join_preserves_membership (first second : ScalarFamily Kind) (kind : Kind) :
    (joinScalars first second).accepts kind ↔ first.accepts kind ∨ second.accepts kind := by
  rfl

theorem scalar_join_assoc (first second third : ScalarFamily Kind) :
    joinScalars (joinScalars first second) third = joinScalars first (joinScalars second third) := by
  simp [joinScalars, or_assoc, Bool.or_assoc]

theorem scalar_join_comm (first second : ScalarFamily Kind) : joinScalars first second = joinScalars second first := by
  simp [joinScalars, or_comm, Bool.or_comm]

theorem scalar_join_idem (value : ScalarFamily Kind) : joinScalars value value = value := by
  cases value
  simp [joinScalars]

theorem bottom_contributes_no_scalar_legs (value : ScalarFamily Kind) : joinScalars nothing value = value := by
  cases value
  simp [joinScalars, nothing]

theorem null_contributes_only_null (value : ScalarFamily Kind) :
    joinScalars null value = ⟨value.accepts, true⟩ := by
  simp [joinScalars, null]

def joinFields (first second : Option (ScalarFamily Kind)) : Option (ScalarFamily Kind) :=
  match first, second with
  | none, none => none
  | some value, none | none, some value => some (joinScalars null value)
  | some left, some right => some (joinScalars left right)

theorem field_join_assoc (first second third : Option (ScalarFamily Kind)) :
    joinFields (joinFields first second) third = joinFields first (joinFields second third) := by
  cases first <;> cases second <;> cases third <;>
    simp [joinFields, joinScalars, null, or_assoc, Bool.or_assoc]

theorem field_join_comm (first second : Option (ScalarFamily Kind)) : joinFields first second = joinFields second first := by
  cases first <;> cases second <;> simp [joinFields, scalar_join_comm]

theorem field_join_idem (value : Option (ScalarFamily Kind)) : joinFields value value = value := by
  cases value <;> simp [joinFields, scalar_join_idem]

theorem field_absence_contributes_null_if_the_key_exists (value : ScalarFamily Kind) :
    joinFields (some value) none = some ⟨value.accepts, true⟩ := by
  simp [joinFields, null_contributes_only_null]

def FlatStruct (Name Kind : Type) := Name → Option (ScalarFamily Kind)

def joinStructs (first second : FlatStruct Name Kind) : FlatStruct Name Kind :=
  fun name => joinFields (first name) (second name)

theorem struct_join_assoc (first second third : FlatStruct Name Kind) :
    joinStructs (joinStructs first second) third = joinStructs first (joinStructs second third) := by
  funext name
  exact field_join_assoc (first name) (second name) (third name)

theorem struct_join_comm (first second : FlatStruct Name Kind) : joinStructs first second = joinStructs second first := by
  funext name
  exact field_join_comm (first name) (second name)

theorem struct_join_idem (value : FlatStruct Name Kind) : joinStructs value value = value := by
  funext name
  exact field_join_idem (value name)

#print axioms scalar_join_preserves_membership
#print axioms scalar_join_assoc
#print axioms scalar_join_comm
#print axioms scalar_join_idem
#print axioms bottom_contributes_no_scalar_legs
#print axioms null_contributes_only_null
#print axioms field_absence_contributes_null_if_the_key_exists
#print axioms struct_join_assoc
#print axioms struct_join_comm
#print axioms struct_join_idem

end RTDB.Types
