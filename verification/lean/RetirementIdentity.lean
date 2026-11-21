import Std

namespace RTDB.RetirementIdentity

structure ObjectVersion where
  providerNamespace : Nat
  physicalKey : Nat
  version : Nat
  deriving DecidableEq, Repr

structure Publication where
  logicalKey : Nat
  contentDigest : Nat
  identity : ObjectVersion
  deriving DecidableEq, Repr

def matchesSelection (selected observed : Publication) : Prop :=
  selected.identity = observed.identity

def deleteExact (identity : ObjectVersion) (available : List Publication) : List Publication :=
  available.filter fun publication => decide (publication.identity ≠ identity)

theorem accepted_observation_identifies_selected (selected observed : Publication)
    (accepted : matchesSelection selected observed) :
    observed.identity = selected.identity := by
  exact accepted.symm

theorem exact_delete_retains_any_other_publication (selected observed retained : Publication)
    (available : List Publication) (accepted : matchesSelection selected observed)
    (other : retained.identity ≠ selected.identity) (present : retained ∈ available) :
    retained ∈ deleteExact observed.identity available := by
  rw [accepted_observation_identifies_selected selected observed accepted]
  simp [deleteExact, present, other]

theorem exact_delete_concludes_retirement (selected observed : Publication)
    (available : List Publication) (accepted : matchesSelection selected observed) :
    selected ∉ deleteExact observed.identity available := by
  rw [accepted_observation_identifies_selected selected observed accepted]
  simp [deleteExact]

def retired : Publication := ⟨7, 92, ⟨1, 7, 2⟩⟩
def replacement : Publication := ⟨7, 92, ⟨1, 7, 3⟩⟩
def anotherResidence : Publication := ⟨7, 92, ⟨2, 7, 2⟩⟩

theorem a_replacement_can_share_the_key_and_digest :
    retired.logicalKey = replacement.logicalKey ∧
    retired.contentDigest = replacement.contentDigest ∧
    ¬ matchesSelection retired replacement := by
  unfold matchesSelection
  decide

theorem another_namespace_can_share_the_key_and_version :
    retired.identity.physicalKey = anotherResidence.identity.physicalKey ∧
    retired.identity.version = anotherResidence.identity.version ∧
    ¬ matchesSelection retired anotherResidence := by
  unfold matchesSelection
  decide

structure QualifiedName where
  logicalKey : Nat
  epoch : Nat
  inputDigest : Nat
  deriving DecidableEq

def qualifiedName (logicalKey epoch inputDigest : Nat) : QualifiedName :=
  ⟨logicalKey, epoch, inputDigest⟩

theorem reset_disjoins_qualified_names (k old new digest : Nat) (advanced : old ≠ new) :
    qualifiedName k old digest ≠ qualifiedName k new digest := by
  intro same
  exact advanced (congrArg QualifiedName.epoch same)

theorem different_inputs_disjoin_qualified_names (k epoch old new : Nat) (changed : old ≠ new) :
    qualifiedName k epoch old ≠ qualifiedName k epoch new := by
  intro same
  exact changed (congrArg QualifiedName.inputDigest same)

#print axioms accepted_observation_identifies_selected
#print axioms exact_delete_retains_any_other_publication
#print axioms exact_delete_concludes_retirement
#print axioms a_replacement_can_share_the_key_and_digest
#print axioms another_namespace_can_share_the_key_and_version
#print axioms reset_disjoins_qualified_names
#print axioms different_inputs_disjoin_qualified_names

end RTDB.RetirementIdentity
