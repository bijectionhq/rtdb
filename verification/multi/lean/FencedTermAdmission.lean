set_option warningAsError true

namespace FencedTermAdmission

def replica_read_is_empty (recordCount : Nat) : Bool := recordCount == 0

def may_promote (recognizedPrefix mayLead alreadyLeading holdsGrant : Bool) : Bool :=
  recognizedPrefix && mayLead && !alreadyLeading && holdsGrant

def should_start_claim (fenced mayLead alreadyLeading claimPending : Bool) : Bool :=
  !fenced && mayLead && !alreadyLeading && !claimPending

def next_term (highest : Int) : Option Int :=
  if highest < 9223372036854775807 then some (highest + 1) else none

def initial_term_sequence (fenced : Bool) : Int := if fenced then 0 else 1

def fenced_promotion (recordCount : Nat) (recognizedPrefix mayLead alreadyLeading holdsGrant : Bool)
    (highest : Int) : Option (Int × Int) :=
  if replica_read_is_empty recordCount && may_promote recognizedPrefix mayLead alreadyLeading holdsGrant then
    (next_term highest).map fun term => (term, initial_term_sequence true)
  else none

theorem fenced_promotion_after_empty_read {recordCount : Nat} {recognizedPrefix mayLead alreadyLeading holdsGrant : Bool}
    {highest term sequence : Int}
    (promoted : fenced_promotion recordCount recognizedPrefix mayLead alreadyLeading holdsGrant highest = some (term, sequence)) :
    recordCount = 0 ∧ recognizedPrefix = true ∧ mayLead = true ∧ alreadyLeading = false ∧ holdsGrant = true ∧
      highest < 9223372036854775807 ∧ term = highest + 1 ∧ sequence = 0 := by
  simp only [fenced_promotion, replica_read_is_empty, may_promote, Bool.and_eq_true,
    Bool.not_eq_true'] at promoted
  split at promoted
  · rename_i eligible
    unfold next_term at promoted
    split at promoted
    · rename_i capacity
      simp [initial_term_sequence] at promoted
      exact ⟨beq_iff_eq.mp eligible.1, eligible.2.1.1.1, eligible.2.1.1.2, eligible.2.1.2, eligible.2.2,
        capacity, promoted.1.symm, promoted.2.symm⟩
    · contradiction
  · contradiction

theorem fenced_driver_never_starts_a_claim (mayLead alreadyLeading claimPending : Bool) :
    should_start_claim true mayLead alreadyLeading claimPending = false := rfl

def fences_superseded (highest arriving : Int) : Bool := decide (arriving < highest)

def highest_after_admission (highest arriving : Int) : Int := max highest arriving

theorem a_superseded_record_is_fenced (highest arriving : Int) (stale : arriving < highest) :
    fences_superseded highest arriving = true ∧ highest_after_admission highest arriving = highest := by
  constructor
  · exact decide_eq_true stale
  · unfold highest_after_admission
    omega

inductive Reaches (boundary : Int) : Int → Prop where
  | start : Reaches boundary boundary
  | admit {highest : Int} (reached : Reaches boundary highest) (arriving : Int) :
      Reaches boundary (highest_after_admission highest arriving)

theorem a_superseded_term_stays_fenced {boundary highest : Int} (reached : Reaches boundary highest)
    {arriving : Int} (stale : arriving < boundary) : fences_superseded highest arriving = true := by
  have monotone : boundary ≤ highest := by
    induction reached with
    | start => exact Int.le_refl _
    | admit _ _ ih => exact Int.le_trans ih (Int.le_max_left _ _)
  exact (a_superseded_record_is_fenced highest arriving (Int.lt_of_lt_of_le stale monotone)).1

theorem promotion_and_fencing_are_reachable :
    fenced_promotion 0 true true false true 0 = some (1, 0) ∧
    Reaches 0 1 ∧ fences_superseded 1 0 = true :=
  ⟨rfl, .admit .start 1, rfl⟩

#print axioms fenced_promotion_after_empty_read
#print axioms fenced_driver_never_starts_a_claim
#print axioms a_superseded_record_is_fenced
#print axioms a_superseded_term_stays_fenced
#print axioms promotion_and_fencing_are_reachable

end FencedTermAdmission
