universe u v

namespace ExactRetirementRetry

variable {Key : Type u} {Version : Type v}

abbrev Inventory (Key : Type u) (Version : Type v) := Key → Version → Prop

def erase (inventory : Inventory Key Version) (key : Key) (version : Version) :
    Inventory Key Version :=
  fun listedKey listedVersion =>
    inventory listedKey listedVersion ∧ ¬ (listedKey = key ∧ listedVersion = version)

def CompleteFor (listed actual : Inventory Key Version) (key : Key) (version : Version) : Prop :=
  actual key version → listed key version

theorem an_exact_removal_excludes_its_target (actual : Inventory Key Version) (key : Key)
    (version : Version) : ¬ erase actual key version key version := by
  simp [erase]

theorem an_exact_removal_preserves_other_versions (actual : Inventory Key Version)
    (key otherKey : Key) (version otherVersion : Version)
    (different : otherKey ≠ key ∨ otherVersion ≠ version) :
    erase actual key version otherKey otherVersion ↔ actual otherKey otherVersion := by
  have distinct : ¬ (otherKey = key ∧ otherVersion = version) := by
    intro same
    exact different.elim (fun h => h same.1) (fun h => h same.2)
  simp [erase, distinct]

theorem a_repeated_exact_removal_is_idempotent (actual : Inventory Key Version)
    (key otherKey : Key) (version otherVersion : Version) :
    erase (erase actual key version) key version otherKey otherVersion ↔
      erase actual key version otherKey otherVersion := by
  constructor
  · intro removed
    exact removed.1
  · intro removed
    exact ⟨removed, removed.2⟩

theorem a_complete_negative_observation_establishes_absence (listed actual : Inventory Key Version)
    (key : Key) (version : Version) (complete : CompleteFor listed actual key version)
    (unlisted : ¬ listed key version) : ¬ actual key version := by
  intro present
  exact unlisted (complete present)

#print axioms an_exact_removal_excludes_its_target
#print axioms an_exact_removal_preserves_other_versions
#print axioms a_repeated_exact_removal_is_idempotent
#print axioms a_complete_negative_observation_establishes_absence

end ExactRetirementRetry
