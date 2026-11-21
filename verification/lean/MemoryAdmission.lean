import Std

namespace RTDB.MemoryAdmission

structure Limits where
  child : Nat
  parent : Nat
  wordMaximum : Nat

structure Usage where
  child : Nat
  grant : Nat
  parent : Nat

def roundedUp (quantity quantum : Nat) : Nat :=
  let blocks := quantity / quantum
  if quantity % quantum = 0 then blocks * quantum else (blocks + 1) * quantum

theorem rounded_up_covers_request (quantity quantum : Nat) (positive : 0 < quantum) :
    quantity ≤ roundedUp quantity quantum := by
  have decomposed := Nat.mod_add_div quantity quantum
  have remainder := Nat.mod_lt quantity positive
  by_cases exactMultiple : quantity % quantum = 0
  · simp [roundedUp, exactMultiple]
    rw [Nat.mul_comm]
    omega
  · simp [roundedUp, exactMultiple, Nat.add_mul]
    rw [Nat.mul_comm (quantity / quantum) quantum]
    omega

theorem rounding_slack_is_below_one_quantum (quantity quantum : Nat) (positive : 0 < quantum) :
    roundedUp quantity quantum < quantity + quantum := by
  have decomposed := Nat.mod_add_div quantity quantum
  by_cases exactMultiple : quantity % quantum = 0
  · simp [roundedUp, exactMultiple]
    rw [Nat.mul_comm (quantity / quantum) quantum]
    omega
  · simp [roundedUp, exactMultiple, Nat.add_mul]
    rw [Nat.mul_comm (quantity / quantum) quantum]
    omega

def grantFor (used granted quantum : Nat) : Nat :=
  if used ≤ granted then granted else granted + roundedUp (used - granted) quantum

theorem grant_covers_usage (used granted quantum : Nat) (positive : 0 < quantum) :
    used ≤ grantFor used granted quantum := by
  by_cases covered : used ≤ granted
  · simp [grantFor, covered]
  · have additional := rounded_up_covers_request (used - granted) quantum positive
    simp only [grantFor, covered, ite_false]
    omega

theorem a_new_grant_never_shrinks (used granted quantum : Nat) :
    granted ≤ grantFor used granted quantum := by
  unfold grantFor
  split <;> omega

def rollBack (provisional : Usage) (request : Nat) : Usage :=
  { provisional with child := provisional.child - request }

def attempt (limits : Limits) (prior : Usage) (request quantum : Nat) : Bool × Usage :=
  if prior.child + request ≤ limits.child then
    let provisional := { prior with child := prior.child + request }
    let nextGrant := grantFor provisional.child prior.grant quantum
    let nextParent := prior.parent + (nextGrant - prior.grant)
    if nextGrant ≤ limits.wordMaximum ∧ nextParent ≤ limits.parent then
      (true, ⟨provisional.child, nextGrant, nextParent⟩)
    else (false, rollBack provisional request)
  else (false, prior)

theorem a_refused_admission_restores_both_counters (limits : Limits) (prior : Usage) (request quantum : Nat)
    (refused : (attempt limits prior request quantum).1 = false) :
    (attempt limits prior request quantum).2 = prior := by
  by_cases childFits : prior.child + request ≤ limits.child
  · by_cases grantFits :
      grantFor (prior.child + request) prior.grant quantum ≤ limits.wordMaximum ∧
      prior.parent + (grantFor (prior.child + request) prior.grant quantum - prior.grant) ≤ limits.parent
    · simp [attempt, childFits, grantFits] at refused
    · cases prior
      simp [attempt, childFits, grantFits, rollBack]
  · simp [attempt, childFits]

theorem admitted_charge_stays_within_its_limits (limits : Limits) (prior : Usage) (request quantum : Nat)
    (positive : 0 < quantum) (admitted : (attempt limits prior request quantum).1 = true) :
    let updated := (attempt limits prior request quantum).2
    updated.child = prior.child + request ∧
    updated.child ≤ updated.grant ∧ updated.child ≤ limits.child ∧
    updated.grant ≤ limits.wordMaximum ∧ updated.parent ≤ limits.parent ∧
    updated.parent = prior.parent + (updated.grant - prior.grant) := by
  by_cases childFits : prior.child + request ≤ limits.child
  · by_cases grantFits :
      grantFor (prior.child + request) prior.grant quantum ≤ limits.wordMaximum ∧
      prior.parent + (grantFor (prior.child + request) prior.grant quantum - prior.grant) ≤ limits.parent
    · simp [attempt, childFits, grantFits, grant_covers_usage _ _ _ positive]
    · simp [attempt, childFits, grantFits] at admitted
  · simp [attempt, childFits] at admitted

#print axioms rounded_up_covers_request
#print axioms rounding_slack_is_below_one_quantum
#print axioms grant_covers_usage
#print axioms a_new_grant_never_shrinks
#print axioms a_refused_admission_restores_both_counters
#print axioms admitted_charge_stays_within_its_limits

end RTDB.MemoryAdmission
