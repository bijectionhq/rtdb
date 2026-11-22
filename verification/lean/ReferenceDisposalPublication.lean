import Std

namespace RTDB.ReferenceDisposalPublication

structure State (Residence : Type) where
  generation : Nat
  owner : Option Residence

variable {Residence : Type}

def closedFor (s : State Residence) (residence : Residence) : State Residence :=
  ⟨s.generation + 1, some residence⟩

def MayPublish (s : State Residence) (preparedAt : Nat) : Prop :=
  s.owner = none ∧ preparedAt = s.generation

def MaySettle (s : State Residence) (generation : Nat) (residence : Residence) : Prop :=
  s.generation = generation ∧ s.owner = some residence

inductive Step : State Residence → State Residence → Prop where
  | close (s) (residence) : s.owner = none → Step s (closedFor s residence)
  | settle (s) (generation) (residence) :
      MaySettle s generation residence → Step s { s with owner := none }
  | publish (s) (preparedAt) : MayPublish s preparedAt → Step s s
  | restart (s) : Step s s

inductive History : State Residence → State Residence → Prop where
  | empty (s) : History s s
  | append {s mid dst} : History s mid → Step mid dst → History s dst

theorem history_preserves_generation {s dst : State Residence} (h : History s dst) :
    s.generation ≤ dst.generation := by
  induction h with
  | empty => exact Nat.le_refl _
  | append _ step previous =>
    cases step with
    | close _ _ =>
      exact Nat.le_trans previous (by simp [closedFor])
    | settle _ _ _ => exact previous
    | publish _ => exact previous
    | restart => exact previous

theorem a_preparation_cannot_cross_retirement {s dst : State Residence} (residence : Residence)
    (h : History (closedFor s residence) dst) :
    ¬ MayPublish dst s.generation := by
  have advanced : s.generation < dst.generation :=
    Nat.lt_of_lt_of_le (show s.generation < (closedFor s residence).generation by
      exact Nat.lt_succ_self _) (history_preserves_generation h)
  exact fun admitted => (Nat.ne_of_lt advanced) admitted.2

theorem settlement_names_the_current_residence {s : State Residence} {generation : Nat}
    {residence : Residence} (allowed : MaySettle s generation residence) : s.owner = some residence :=
  allowed.2

#print axioms history_preserves_generation
#print axioms a_preparation_cannot_cross_retirement
#print axioms settlement_names_the_current_residence

end RTDB.ReferenceDisposalPublication
