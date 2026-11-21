import Std

namespace RTDB.ResidenceReaderRetention

structure Sweep where
  head : Nat
  held : Nat → Prop

variable (used : Nat → Prop) (retirement : Nat) (sweep : Sweep)

def Certified : Prop := ∀ generation, retirement ≤ generation → ¬ used generation

def Covered (generation : Nat) : Prop := sweep.held generation ∨ sweep.head ≤ generation

def FloorAllows : Prop :=
  retirement ≤ sweep.head ∧ ∀ generation, sweep.held generation → retirement ≤ generation

def ExactAllows : Prop :=
  retirement ≤ sweep.head ∧ ∀ generation, sweep.held generation → ¬ used generation

theorem floor_preserves_dependencies (certified : Certified used retirement)
    (mayDelete : FloorAllows retirement sweep)
    (generation : Nat) (read : Covered sweep generation) : ¬ used generation := by
  have atOrAfterHead : retirement ≤ sweep.head := mayDelete.1
  have respectsHolds : ∀ cut, sweep.held cut → retirement ≤ cut := mayDelete.2
  rcases read with held | later
  · exact certified generation (respectsHolds generation held)
  · exact certified generation (Nat.le_trans atOrAfterHead later)

theorem exact_preserves_dependencies (certified : Certified used retirement)
    (mayDelete : ExactAllows used retirement sweep)
    (generation : Nat) (read : Covered sweep generation) : ¬ used generation := by
  have atOrAfterHead : retirement ≤ sweep.head := mayDelete.1
  have respectsHolds : ∀ cut, sweep.held cut → ¬ used cut := mayDelete.2
  rcases read with held | later
  · exact respectsHolds generation held
  · exact certified generation (Nat.le_trans atOrAfterHead later)

theorem exact_allows_every_floor_deletion (certified : Certified used retirement)
    (mayDelete : FloorAllows retirement sweep) : ExactAllows used retirement sweep := by
  have retiredByHead : retirement ≤ sweep.head := mayDelete.1
  refine ⟨retiredByHead, ?_⟩
  intro generation held
  exact certified generation (mayDelete.2 generation held)

theorem exact_can_collect_with_an_unrelated_earlier_hold :
    ∃ (used : Nat → Prop) (retirement : Nat) (sweep : Sweep),
        Certified used retirement ∧ (∃ g, used g) ∧
        ExactAllows used retirement sweep ∧ ¬ FloorAllows retirement sweep := by
  refine ⟨(· = 1), 2, ⟨2, (· = 0)⟩, ?_, ⟨1, rfl⟩, ?_, ?_⟩
  · intro generation retired used
    omega
  · refine ⟨Nat.le_refl 2, ?_⟩
    intro generation held used
    change generation = 0 at held
    omega
  · intro mayDelete
    have late := mayDelete.2 0 rfl
    omega

#print axioms floor_preserves_dependencies
#print axioms exact_preserves_dependencies
#print axioms exact_allows_every_floor_deletion
#print axioms exact_can_collect_with_an_unrelated_earlier_hold

end RTDB.ResidenceReaderRetention
