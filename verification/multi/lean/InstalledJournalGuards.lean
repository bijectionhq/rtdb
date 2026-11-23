namespace RTDB.InstalledJournalGuards

def memoryAllowsInsert (reserved replacingStage recordExists : Bool) : Bool :=
  !(reserved || (!replacingStage && recordExists))

def memoryAllowsReservation (recordExists : Bool) : Bool := !recordExists

def directoryRouteAllowed (installed : Bool) : Bool := !installed

def directoryMoveParkAllowed (installed moving : Bool) : Bool := !(installed && moving)

def pressureCandidate (directoryHolding isProtected resident : Bool) : Bool :=
  directoryHolding && !isProtected && resident

def reservationFields : List String := ["id", "installed_journal"]

def dynamoAllowsWrite (fields : List String) (current expected : Option Nat) : Prop :=
  match expected with
  | none => "id" ∉ fields
  | some version => "version" ∈ fields ∧ current = some version

theorem reserved_identity_refuses_directory_insert (replacingStage recordExists : Bool) :
    memoryAllowsInsert true replacingStage recordExists = false := rfl

theorem directory_record_refuses_reservation :
    memoryAllowsReservation true = false := rfl

theorem dynamo_reservation_refuses_directory_write (current expected : Option Nat) :
    ¬ dynamoAllowsWrite reservationFields current expected := by
  cases expected <;> simp [dynamoAllowsWrite, reservationFields]

theorem installed_name_refuses_directory_route : directoryRouteAllowed true = false := rfl

theorem installed_name_refuses_directory_move_park :
    directoryMoveParkAllowed true true = false := rfl

theorem journal_holding_is_never_a_pressure_candidate (isProtected resident : Bool) :
    pressureCandidate false isProtected resident = false := rfl

theorem directory_effect_guards_exclude_installed
    (replacingStage recordExists moving isProtected resident : Bool)
    (current expected : Option Nat) (isMove : moving = true) :
    memoryAllowsInsert true replacingStage recordExists = false ∧
    (¬ dynamoAllowsWrite reservationFields current expected) ∧
    directoryRouteAllowed true = false ∧
    directoryMoveParkAllowed true moving = false ∧
    pressureCandidate false isProtected resident = false := by
  subst moving
  exact ⟨reserved_identity_refuses_directory_insert replacingStage recordExists,
    dynamo_reservation_refuses_directory_write current expected,
    installed_name_refuses_directory_route, installed_name_refuses_directory_move_park,
    journal_holding_is_never_a_pressure_candidate isProtected resident⟩

theorem directory_controls_have_counterexamples :
    memoryAllowsInsert false true true = true ∧
    memoryAllowsReservation false = true ∧
    dynamoAllowsWrite [] none none ∧
    dynamoAllowsWrite ["id", "version"] (some 1) (some 1) ∧
    directoryRouteAllowed false = true ∧
    directoryMoveParkAllowed false true = true ∧
    pressureCandidate true false true = true := by
  simp [memoryAllowsInsert, memoryAllowsReservation, dynamoAllowsWrite,
    directoryRouteAllowed, directoryMoveParkAllowed, pressureCandidate]

structure Cut where
  nextLsn : Nat
  history : Nat
  deriving DecidableEq

def checkpointNextLsn (boundary : Int) : Option Nat :=
  let next := boundary + 1
  if 0 ≤ next ∧ next ≤ 9223372036854775807 then some next.toNat else none

def trimTarget (floor selected : Cut) : Option Cut :=
  if floor.nextLsn < selected.nextLsn then some selected else none

inductive RetentionAction where
  | disposeExpired
  | retainSelected
  | publish
  deriving DecidableEq

def retentionActions : List RetentionAction := [.disposeExpired, .retainSelected, .publish]

def DisposalBeforeTrim (actions : List RetentionAction) : Prop :=
  ∃ rest, actions = .disposeExpired :: rest ∧ .retainSelected ∈ rest ∧ .publish ∈ rest

theorem retention_attempts_disposal_before_trim : DisposalBeforeTrim retentionActions := by
  exact ⟨[.retainSelected, .publish], rfl, by simp, by simp⟩

theorem checkpoint_next_lsn_is_boundary_successor {boundary : Int} {next : Nat}
    (accepted : checkpointNextLsn boundary = some next) : (next : Int) = boundary + 1 := by
  simp only [checkpointNextLsn] at accepted
  split at accepted
  next h =>
    simp only [Option.some.injEq] at accepted
    rw [← accepted, Int.toNat_of_nonneg h.1]
  next => contradiction

theorem trim_uses_selected_cut {floor selected through : Cut}
    (planned : trimTarget floor selected = some through) :
    through = selected ∧ floor.nextLsn < through.nextLsn := by
  simp [trimTarget] at planned
  obtain ⟨later, rfl⟩ := planned
  exact ⟨rfl, later⟩

theorem native_trim_stops_at_boundary_successor {floor selected through : Cut} {boundary : Int}
    (selectedAtBoundary : (selected.nextLsn : Int) = boundary + 1)
    (planned : trimTarget floor selected = some through) :
    (through.nextLsn : Int) ≤ boundary + 1 := by
  rw [(trim_uses_selected_cut planned).1, selectedAtBoundary]
  exact Int.le_refl _

theorem early_trim_control : ¬ DisposalBeforeTrim [.retainSelected, .disposeExpired, .publish] := by
  simp [DisposalBeforeTrim]

theorem wrong_trim_cut_control :
    let floor : Cut := ⟨0, 10⟩
    let selected : Cut := ⟨42, 20⟩
    checkpointNextLsn 41 = some selected.nextLsn ∧
    trimTarget floor selected ≠ some ⟨43, 20⟩ ∧
    trimTarget floor selected ≠ some ⟨42, 10⟩ := by decide

structure Placement where
  deployment : Nat
  incarnation : Nat
  group : Nat
  generation : Nat
  deriving DecidableEq

structure Enrollment where
  worker : Nat
  serial : Nat
  deriving DecidableEq

structure Grant where
  enrollment : Enrollment
  epoch : Nat
  keyGeneration : Nat
  deriving DecidableEq

structure Invocation where
  nonce : Nat
  expiresAt : Nat
  deriving DecidableEq

inductive Principal where
  | administrator
  | worker (enrollment : Enrollment)
  | custodian
  | routeReader
  deriving DecidableEq

def exactScope (installed observed : Placement) : Prop := observed = installed

def currentGrant (principal : Principal) (current : Option Grant) (used : Grant) : Prop :=
  principal = .worker used.enrollment ∧ current = some used

def barrierReader (principal : Principal) (roster : Option Enrollment) : Prop :=
  match principal with
  | .worker reader => roster = some reader
  | _ => False

theorem barrier_names_the_enrolled_reader (reader : Enrollment) :
    barrierReader (.worker reader) (some reader) := by rfl

theorem barrier_refuses_a_superseded_enrollment :
    ¬ barrierReader (.worker ⟨1, 1⟩) (some ⟨1, 2⟩) := by
  simp [barrierReader]

def exactReadIdentity (invocation returned : Invocation) : Prop := returned = invocation

def appendIdentity (principal : Principal) (current : Option Grant) (used : Grant)
    (head expectedNext : Nat) : Prop := currentGrant principal current used ∧ expectedNext = head

def disposalDue (eligibleAt : Option Nat) (localTime routeTime : Nat) : Prop :=
  ∃ eligible, eligibleAt = some eligible ∧ (eligible ≤ localTime ∨ eligible ≤ routeTime)

def expiryDisposalAdmits (principal : Principal) (serial issuedSerial : Nat)
    (stillEnrolled : Bool) (eligibleAt : Option Nat) (decisionTime : Nat) : Prop :=
  (principal = .administrator ∨ principal = .custodian) ∧
  0 < serial ∧ serial ≤ issuedSerial ∧ stillEnrolled = false ∧
  ∃ eligible, eligibleAt = some eligible ∧ eligible ≤ decisionTime

def referencePresence (explicit fenced : Bool) : Bool := explicit || !fenced

theorem opaque_append_is_refused_after_a_fence : referencePresence false true = false := rfl

def completeReferencesWhenFenced (required : List Nat) (provided : Option (List Nat))
    (fenced : List Nat) : Prop :=
  match provided with
  | none => fenced = []
  | some names =>
      (∀ name ∈ required, name ∈ names) ∧
      names.length ≤ 16384 ∧ names.Pairwise (· < ·) ∧
      (∀ name ∈ names, name ≠ 0 ∧ name ∉ fenced)

def appendReferences (supplied : Option (List Nat)) : Option (List Nat) := supplied

theorem append_preserves_reference_list (supplied : Option (List Nat)) :
    appendReferences supplied = supplied := by rfl

theorem a_reference_list_stays_explicit_across_append_construction :
    appendReferences (some [2]) = some [2] := by rfl

theorem writer_request_admissible_under_unchanged_grant (g : Grant) :
    currentGrant (.worker g.enrollment) (some g) g := by
  exact ⟨rfl, rfl⟩

theorem append_request_names_observed_head (g : Grant) (head : Nat) :
    appendIdentity (.worker g.enrollment) (some g) g head head :=
  ⟨writer_request_admissible_under_unchanged_grant g, rfl⟩

theorem release_names_the_barrier_invocation (invocation : Invocation) :
    exactReadIdentity invocation invocation := rfl

theorem eligible_withdrawn_enrollment_admits_disposal
    {serial issuedSerial eligible decisionTime : Nat}
    (issued : 0 < serial ∧ serial ≤ issuedSerial) (elapsed : eligible ≤ decisionTime) :
    expiryDisposalAdmits .custodian serial issuedSerial false (some eligible) decisionTime :=
  ⟨Or.inr rfl, issued.1, issued.2, rfl, eligible, rfl, elapsed⟩

theorem identity_controls :
    ¬ exactScope ⟨1, 1, 1, 1⟩ ⟨1, 1, 2, 1⟩ ∧
    ¬ currentGrant (.worker ⟨1, 1⟩) (some ⟨⟨1, 1⟩, 2, 2⟩) ⟨⟨1, 1⟩, 2, 1⟩ ∧
    ¬ exactReadIdentity ⟨1, 10⟩ ⟨1, 11⟩ ∧
    ¬ appendIdentity (.worker ⟨1, 1⟩) (some ⟨⟨1, 1⟩, 2, 1⟩) ⟨⟨1, 1⟩, 2, 1⟩ 7 8 := by
  simp [exactScope, currentGrant, exactReadIdentity, appendIdentity]

theorem ahead_local_clock_control :
    disposalDue (some 9) 9 8 ∧ ¬ expiryDisposalAdmits .custodian 2 2 false (some 9) 8 := by
  simp [disposalDue, expiryDisposalAdmits]

theorem explicit_references_cover_required_unfenced_objects
    {required names fenced : List Nat} (admitted : completeReferencesWhenFenced required (some names) fenced)
    {name : Nat} (dependency : name ∈ required) : name ∈ names ∧ name ∉ fenced := by
  have declared := admitted.1 name dependency
  exact ⟨declared, (admitted.2.2.2 name declared).2⟩

theorem opaque_append_control :
    completeReferencesWhenFenced [2] none [] ∧
    ¬ completeReferencesWhenFenced [2] none [3] := by
  simp [completeReferencesWhenFenced]

theorem explicit_append_reference_controls :
    completeReferencesWhenFenced [2] (some [2]) [3] ∧
    ¬ completeReferencesWhenFenced [2] (some [3]) [] ∧
    ¬ completeReferencesWhenFenced [2] (some [2]) [2] := by
  simp [completeReferencesWhenFenced]

#print axioms reserved_identity_refuses_directory_insert
#print axioms dynamo_reservation_refuses_directory_write
#print axioms directory_effect_guards_exclude_installed
#print axioms directory_controls_have_counterexamples
#print axioms retention_attempts_disposal_before_trim
#print axioms checkpoint_next_lsn_is_boundary_successor
#print axioms trim_uses_selected_cut
#print axioms native_trim_stops_at_boundary_successor
#print axioms early_trim_control
#print axioms wrong_trim_cut_control
#print axioms barrier_names_the_enrolled_reader
#print axioms barrier_refuses_a_superseded_enrollment
#print axioms append_request_names_observed_head
#print axioms append_preserves_reference_list
#print axioms a_reference_list_stays_explicit_across_append_construction
#print axioms eligible_withdrawn_enrollment_admits_disposal
#print axioms identity_controls
#print axioms ahead_local_clock_control
#print axioms explicit_references_cover_required_unfenced_objects
#print axioms opaque_append_is_refused_after_a_fence
#print axioms opaque_append_control
#print axioms explicit_append_reference_controls

end RTDB.InstalledJournalGuards
