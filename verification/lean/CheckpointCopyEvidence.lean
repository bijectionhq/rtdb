import Std

namespace RTDB.CheckpointCopyEvidence

structure Planned (Physical Digest : Type) where
  version : Option Physical
  digest : Option Digest

structure Body (Physical Digest : Type) where
  version : Option Physical
  digest : Digest

variable {Physical Digest : Type}

def Agrees (planned : Planned Physical Digest) (body : Body Physical Digest) : Prop :=
  planned.version = body.version ∧
  ∀ digest, planned.digest = some digest → body.digest = digest

inductive Copy : List (Planned Physical Digest) → List (Body Physical Digest) → Prop where
  | empty : Copy [] []
  | member : Agrees planned body → Copy tail bodies → Copy (planned :: tail) (body :: bodies)

theorem copy_covers_the_inventory {plan : List (Planned Physical Digest)}
    {bodies : List (Body Physical Digest)} (copied : Copy plan bodies) :
    bodies.length = plan.length := by
  induction copied with
  | empty => rfl
  | member _ _ rest => simp [rest]

theorem copied_members_match_the_plan {plan : List (Planned Physical Digest)}
    {bodies : List (Body Physical Digest)} (copied : Copy plan bodies) :
    ∀ pair ∈ plan.zip bodies, pair.1.version = pair.2.version ∧
      ∀ digest, pair.1.digest = some digest → pair.2.digest = digest := by
  induction copied with
  | empty => simp
  | member agrees _ rest =>
    intro pair listed
    simp only [List.zip_cons_cons, List.mem_cons] at listed
    rcases listed with same | tail
    · subst pair
      exact ⟨agrees.1, agrees.2⟩
    · exact rest pair tail

def charge (remaining : Nat) : List Nat → Option Nat
  | [] => some remaining
  | cost :: tail => if cost ≤ remaining then charge (remaining - cost) tail else none

theorem successful_plan_conserves_budget {costs : List Nat} {remaining left : Nat}
    (funded : charge remaining costs = some left) :
    left + costs.sum = remaining := by
  induction costs generalizing remaining with
  | nil =>
    simp [charge] at funded ⊢
    exact funded.symm
  | cons cost tail rest =>
    by_cases fits : cost ≤ remaining
    · have fundedTail : charge (remaining - cost) tail = some left := by
        simpa [charge, fits] using funded
      have conservation := rest fundedTail
      simp only [List.sum_cons]
      omega
    · simp [charge, fits] at funded

theorem a_funded_inventory_uses_its_exact_cost {costs : List Nat} {remaining : Nat}
    (funded : costs.sum ≤ remaining) :
    charge remaining costs = some (remaining - costs.sum) := by
  induction costs generalizing remaining with
  | nil => simp [charge]
  | cons cost tail rest =>
    simp only [List.sum_cons] at funded ⊢
    have fits : cost ≤ remaining := by omega
    have tailFits : tail.sum ≤ remaining - cost := by omega
    have debit : remaining - cost - tail.sum = remaining - (cost + tail.sum) := by omega
    simp [charge, fits, rest tailFits, debit]

theorem a_funded_framed_copy_fits_the_source_preflight {payload legacyOverhead header frameOverhead frameCount remaining : Nat}
    (headerCoversLegacy : legacyOverhead ≤ header)
    (funded : payload + header + frameCount * frameOverhead ≤ remaining) :
    payload + legacyOverhead ≤ remaining := by
  omega

#print axioms copy_covers_the_inventory
#print axioms copied_members_match_the_plan
#print axioms successful_plan_conserves_budget
#print axioms a_funded_inventory_uses_its_exact_cost
#print axioms a_funded_framed_copy_fits_the_source_preflight

end RTDB.CheckpointCopyEvidence
