import MaintenanceBound

set_option warningAsError true

namespace MaintenanceBound

inductive ReplicaKind where
  | blockBoundary | blockUploaded | noOp | ordinary
  deriving DecidableEq

def is_maintenance : ReplicaKind → Bool
  | .blockBoundary | .blockUploaded => true
  | .noOp | .ordinary => false

def record_charge (body : ReplicaKind) : Nat := if is_maintenance body then 1 else 0

def maintenance_append_allowed (phase : Phase) : ReplicaKind → Prop
  | .blockBoundary => phase = .idle
  | .blockUploaded => phase = .boundary
  | .noOp | .ordinary => True

theorem classifier_refines_three_record_bound {lane : Lane} (shape : Shape lane)
    (body : ReplicaKind) (allowed : maintenance_append_allowed lane.phase body) :
    lane.used + record_charge body ≤ 3 := by
  cases body <;> rcases shape with ⟨phase, used⟩ | ⟨phase, used⟩ | ⟨phase, used⟩ <;>
    simp_all [maintenance_append_allowed, record_charge, is_maintenance]

theorem three_records_are_reachable :
    Reaches ⟨3, .uploaded, 1⟩ :=
  .step (.step .start (.boundary _ rfl)) (.upload _ rfl)

theorem classifier_charges_checkpoint_progress :
    record_charge .blockBoundary = 1 ∧ record_charge .blockUploaded = 1 := ⟨rfl, rfl⟩

#print axioms classifier_refines_three_record_bound
#print axioms three_records_are_reachable
#print axioms classifier_charges_checkpoint_progress

end MaintenanceBound
