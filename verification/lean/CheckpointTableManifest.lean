import Std

namespace RTDB.CheckpointTableManifest

abbrev Table := Nat
abbrev Bytes := List UInt8
abbrev DigestFunction := Bytes → Nat

def covers (registry : Table → Prop) (manifest : Table → Option Nat) : Prop :=
  ∀ table, registry table ↔ (manifest table).isSome

def bind (registry : Table → Prop) [DecidablePred registry]
    (digest : DigestFunction) (written : Table → Bytes) (table : Table) : Option Nat :=
  if registry table then some (digest (written table)) else none

def accepts (registry : Table → Prop) (manifest : Table → Option Nat)
    (digest : DigestFunction) (observed : Table → Bytes) : Prop :=
  ∀ table, registry table → manifest table = some (digest (observed table))

theorem published_manifest_covers_exactly_the_registered_tables
    (registry : Table → Prop) [DecidablePred registry] (digest : DigestFunction) (written : Table → Bytes) :
    covers registry (bind registry digest written) := by
  intro table
  by_cases member : registry table <;> simp [bind, member]

theorem the_written_metadata_is_accepted
    (registry : Table → Prop) [DecidablePred registry] (digest : DigestFunction) (written : Table → Bytes) :
    accepts registry (bind registry digest written) digest written := by
  intro table member
  simp [bind, member]

theorem acceptance_preserves_each_bound_table (registry : Table → Prop) [DecidablePred registry]
    (digest : DigestFunction) (written observed : Table → Bytes)
    (openRoot : accepts registry (bind registry digest written) digest observed)
    (distinct : ∀ t, registry t → digest (written t) = digest (observed t) → written t = observed t)
    (table : Table) (member : registry table) : written table = observed table := by
  have matched : digest (written table) = digest (observed table) := by
    simpa [bind, member] using openRoot table member
  exact distinct table member matched

theorem accepted_metadata_preserves_derived_dependencies (registry : Table → Prop) [DecidablePred registry]
    (digest : DigestFunction) (written observed : Table → Bytes)
    (openRoot : accepts registry (bind registry digest written) digest observed)
    (distinct : ∀ t, registry t → digest (written t) = digest (observed t) → written t = observed t)
    (table : Table) (member : registry table) (dependencies : Bytes → List Nat) :
    dependencies (written table) = dependencies (observed table) := by
  exact congrArg dependencies (acceptance_preserves_each_bound_table registry digest written observed openRoot distinct table member)

#print axioms published_manifest_covers_exactly_the_registered_tables
#print axioms the_written_metadata_is_accepted
#print axioms acceptance_preserves_each_bound_table
#print axioms accepted_metadata_preserves_derived_dependencies

end RTDB.CheckpointTableManifest
