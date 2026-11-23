import Std

namespace RTDB.ReplicaHoldClose

structure Registry (Reader : Type) where
  revision : Nat
  holds : Reader → Option Nat

variable {Reader : Type} [DecidableEq Reader]

def close (registry : Registry Reader) (reader : Reader) : Registry Reader :=
  { revision := registry.revision + 1
    holds := fun current => if current = reader then none else registry.holds current }

def admitsRevision (registry : Registry Reader) (requestRevision : Nat) : Prop :=
  registry.revision = requestRevision

theorem close_consumes_only_its_hold (registry : Registry Reader) (reader : Reader) :
    (close registry reader).holds reader = none ∧
    ∀ current, current ≠ reader → (close registry reader).holds current = registry.holds current := by
  constructor
  · simp [close]
  · intro current distinct
    simp [close, distinct]

theorem close_fences_prior_admissions (registry after : Registry Reader) (reader : Reader)
    (future : (close registry reader).revision ≤ after.revision)
    (requestRevision : Nat) (issued : requestRevision ≤ registry.revision) :
    ¬ admitsRevision after requestRevision := by
  intro accepted
  unfold admitsRevision at accepted
  simp [close] at future
  omega

theorem a_missing_hold_still_fences_its_ambiguous_admission
    (registry : Registry Reader) (reader : Reader)
    (missing : registry.holds reader = none) :
    registry.holds reader = (close registry reader).holds reader ∧
    ¬ admitsRevision (close registry reader) registry.revision := by
  exact ⟨missing.trans (close_consumes_only_its_hold registry reader).1.symm,
    close_fences_prior_admissions registry (close registry reader) reader (Nat.le_refl _)
      registry.revision (Nat.le_refl _)⟩

#print axioms close_consumes_only_its_hold
#print axioms close_fences_prior_admissions
#print axioms a_missing_hold_still_fences_its_ambiguous_admission

end RTDB.ReplicaHoldClose
