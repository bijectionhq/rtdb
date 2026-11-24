import MaintenanceBound

set_option warningAsError true

namespace MaintenanceBound

inductive ReplicaKind where
  | blockBoundary | blockUploaded | noOp | resolvedTx (fencedTerminal : Bool) | ordinary
  deriving DecidableEq

def is_maintenance : ReplicaKind → Bool
  | .blockBoundary | .blockUploaded => true
  | .resolvedTx fencedTerminal => fencedTerminal
  | .noOp | .ordinary => false

def record_charge (body : ReplicaKind) : Nat := if is_maintenance body then 1 else 0

inductive ReceiverSlot where
  | vacant | current | inherited
  deriving DecidableEq

def ReceiverSlot.outstanding : ReceiverSlot → Nat
  | .vacant => 0
  | .current | .inherited => 1

def ReceiverSlot.afterRegrant : ReceiverSlot → ReceiverSlot
  | .vacant => .vacant
  | .current | .inherited => .inherited

def maintenance_append_allowed (phase : Phase) (slot : ReceiverSlot) : ReplicaKind → Prop
  | .blockBoundary => phase = .idle ∧ slot = .vacant
  | .blockUploaded => phase = .boundary
  | .resolvedTx true => phase = .idle ∧ slot = .inherited
  | .resolvedTx false | .noOp | .ordinary => True

def funded_records (receiverBudget : Nat) : Nat := 3 + receiverBudget

theorem classifier_refines_maintenance_bound {checkpoint : Lane} (shape : Shape checkpoint)
    (settled receiverBudget : Nat) (slot : ReceiverSlot)
    (receiverRoom : settled + slot.outstanding ≤ receiverBudget)
    (body : ReplicaKind) (allowed : maintenance_append_allowed checkpoint.phase slot body) :
    checkpoint.used + settled + record_charge body ≤ funded_records receiverBudget := by
  cases body with
  | resolvedTx fencedTerminal =>
    cases fencedTerminal <;> cases slot <;>
      rcases shape with ⟨phase, used⟩ | ⟨phase, used⟩ | ⟨phase, used⟩ <;>
      simp_all [maintenance_append_allowed, record_charge, is_maintenance, ReceiverSlot.outstanding, funded_records] <;>
      omega
  | blockBoundary | blockUploaded | noOp | ordinary =>
    rcases shape with ⟨phase, used⟩ | ⟨phase, used⟩ | ⟨phase, used⟩ <;>
      simp_all [maintenance_append_allowed, record_charge, is_maintenance, funded_records] <;>
      omega

theorem classifier_charges_checkpoint_progress :
    record_charge .blockBoundary = 1 ∧ record_charge .blockUploaded = 1 := ⟨rfl, rfl⟩

theorem classifier_charges_only_fenced_terminals :
    record_charge (.resolvedTx true) = 1 ∧ record_charge (.resolvedTx false) = 0 := ⟨rfl, rfl⟩

structure SettlementLane where
  checkpoint : Lane
  slot : ReceiverSlot
  available : Nat
  settled : Nat

def SettlementLane.used (lane : SettlementLane) : Nat := lane.checkpoint.used + lane.settled

inductive SettlementStep (receiverBudget : Nat) : SettlementLane → SettlementLane → Prop where
  | regrant (lane : SettlementLane) :
      SettlementStep receiverBudget lane
        { lane with checkpoint.grant := lane.checkpoint.grant + 1, slot := lane.slot.afterRegrant }
  | reserve (lane : SettlementLane) :
      lane.checkpoint.phase = .idle → lane.slot = .vacant → 0 < lane.available →
      SettlementStep receiverBudget lane { lane with slot := .current, available := lane.available - 1 }
  | complete (lane : SettlementLane) :
      lane.slot = .current →
      SettlementStep receiverBudget lane { lane with slot := .vacant, available := lane.available + 1 }
  | fence (lane : SettlementLane) :
      lane.slot = .inherited →
      SettlementStep receiverBudget lane { lane with slot := .vacant, settled := lane.settled + 1 }
  | boundary (lane : SettlementLane) :
      lane.checkpoint.phase = .idle → lane.slot = .vacant →
      SettlementStep receiverBudget lane
        { lane with checkpoint.used := lane.checkpoint.used + 1, checkpoint.phase := .boundary }
  | upload (lane : SettlementLane) :
      lane.checkpoint.phase = .boundary →
      SettlementStep receiverBudget lane
        { lane with checkpoint.used := lane.checkpoint.used + 1, checkpoint.phase := .uploaded }
  | publishAndTrim (lane : SettlementLane) :
      lane.checkpoint.phase = .uploaded → lane.slot = .vacant →
      SettlementStep receiverBudget lane ⟨⟨1, .idle, lane.checkpoint.grant⟩, .vacant, receiverBudget, 0⟩

inductive SettlementReaches (receiverBudget : Nat) : SettlementLane → Prop where
  | start : SettlementReaches receiverBudget ⟨⟨1, .idle, 1⟩, .vacant, receiverBudget, 0⟩
  | step {lane next : SettlementLane} :
      SettlementReaches receiverBudget lane → SettlementStep receiverBudget lane next →
      SettlementReaches receiverBudget next

theorem settlement_projects_to_checkpoint_lane {receiverBudget : Nat} {lane : SettlementLane}
    (reached : SettlementReaches receiverBudget lane) : Reaches lane.checkpoint := by
  induction reached with
  | start => exact .start
  | step _ step reached =>
    cases step with
    | reserve | complete | fence => exact reached
    | regrant => exact .step reached (.regrant _)
    | boundary idle _ => exact .step reached (.boundary _ idle)
    | upload pending => exact .step reached (.upload _ pending)
    | publishAndTrim uploaded _ => exact .step reached (.publishAndTrim _ uploaded)

theorem settlement_preserves_receiver_budget {receiverBudget : Nat} {lane : SettlementLane}
    (reached : SettlementReaches receiverBudget lane) :
    lane.settled + lane.slot.outstanding + lane.available = receiverBudget := by
  induction reached with
  | start => simp [ReceiverSlot.outstanding]
  | @step previous _ _ step budget =>
    cases step with
    | regrant =>
      cases slot : previous.slot <;>
        simpa [slot, ReceiverSlot.afterRegrant, ReceiverSlot.outstanding] using budget
    | reserve _ vacant credit => simp [ReceiverSlot.outstanding, vacant] at budget ⊢; omega
    | complete current => simp [ReceiverSlot.outstanding, current] at budget ⊢; omega
    | fence inherited =>
      cases slot : previous.slot <;> simp_all [ReceiverSlot.outstanding] <;> omega
    | boundary | upload => exact budget
    | publishAndTrim => simp [ReceiverSlot.outstanding]

theorem settlement_lane_stays_funded {receiverBudget : Nat} {lane : SettlementLane}
    (reached : SettlementReaches receiverBudget lane) :
    lane.used + lane.slot.outstanding ≤ funded_records receiverBudget := by
  have checkpoint := grant_fencing_bounds_maintenance_by_three (settlement_projects_to_checkpoint_lane reached)
  have receiver := settlement_preserves_receiver_budget reached
  unfold SettlementLane.used funded_records
  omega

theorem reachable_maintenance_append_fits {receiverBudget : Nat} {lane : SettlementLane}
    (reached : SettlementReaches receiverBudget lane) (body : ReplicaKind)
    (allowed : maintenance_append_allowed lane.checkpoint.phase lane.slot body) :
    lane.used + record_charge body ≤ funded_records receiverBudget := by
  have shape := every_reachable_lane_has_its_phase_shape (settlement_projects_to_checkpoint_lane reached)
  have receiver := settlement_preserves_receiver_budget reached
  exact classifier_refines_maintenance_bound shape lane.settled receiverBudget lane.slot (by omega) body allowed

theorem second_inherited_slot :
    SettlementReaches 2 ⟨⟨1, .idle, 4⟩, .inherited, 0, 1⟩ := by
  have first : SettlementReaches 2 ⟨⟨1, .idle, 1⟩, .current, 1, 0⟩ :=
    .step .start (.reserve _ rfl rfl (by decide))
  have firstInherited : SettlementReaches 2 ⟨⟨1, .idle, 2⟩, .inherited, 1, 0⟩ :=
    .step first (.regrant _)
  have firstFenced : SettlementReaches 2 ⟨⟨1, .idle, 2⟩, .vacant, 1, 1⟩ :=
    .step firstInherited (.fence _ (by decide))
  have second : SettlementReaches 2 ⟨⟨1, .idle, 2⟩, .current, 0, 1⟩ :=
    .step firstFenced (.reserve _ rfl rfl (by decide))
  exact .step (.step second (.regrant _)) (.regrant _)

theorem five_records_are_reachable :
    SettlementReaches 2 ⟨⟨3, .uploaded, 4⟩, .vacant, 0, 2⟩ :=
  .step (.step (.step second_inherited_slot (.fence _ (by decide))) (.boundary _ rfl rfl)) (.upload _ rfl)

#print axioms classifier_refines_maintenance_bound
#print axioms classifier_charges_checkpoint_progress
#print axioms classifier_charges_only_fenced_terminals
#print axioms settlement_projects_to_checkpoint_lane
#print axioms settlement_preserves_receiver_budget
#print axioms settlement_lane_stays_funded
#print axioms reachable_maintenance_append_fits
#print axioms second_inherited_slot
#print axioms five_records_are_reachable

end MaintenanceBound
