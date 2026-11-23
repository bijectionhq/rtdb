import OrderedJournal

namespace RTDB.Recovery.HeldWorldsOrder

open OrderedJournal (Cut)

/-- The source transaction ID and the assigned system-from revision are separate coordinates. -/
structure Key where
  source : Int
  revision : Int

inductive Action where
  | decideHosted (staging : Bool)
  | decideHeldWorlds (rootKind : Nat)
  | switchPreparedProposal (hasClockSlot : Bool)
  | publishHeldWorldRetention
  | publishWorldCut
  | decideWorldCut

structure Submission where
  action : Action
  expected : Int
  contexts : List Nat
  fingerprint : ByteArray

inductive Outcome where
  | rejected
  | stagingRejected
  | primary
  | constraintRefusal

structure Marker where
  context : Nat
  revision : Int
  fingerprint : ByteArray

structure Publication where
  key : Key
  refusal : Bool
  markers : List Marker

def submittedMarkers (request : Submission) (key : Key) : List Marker :=
  request.contexts.map (fun ctx => ⟨ctx, key.revision, request.fingerprint⟩)

def Action.admits : Action → Bool
  | .decideHeldWorlds 6 => false
  | .switchPreparedProposal hasClockSlot => hasClockSlot
  | _ => true

def Action.permitsRefusal : Action → Bool
  | .decideHosted _ | .decideWorldCut | .switchPreparedProposal _ => true
  | .decideHeldWorlds kind => kind == 4 || kind == 5 || kind == 7
  | .publishHeldWorldRetention | .publishWorldCut => false

/--
The input is the placed action after the Flight decoder and action-specific preconditions.
The original check belongs to the primary. A constraint refusal carries that check forward
and starts from the same ordered predecessor. The outcome includes the staging cutoff verdict.
-/
def resolve (authority : Option Int) (request : Submission) (key : Key)
    (outcome : Outcome) : Option Publication :=
  if request.action.admits = true ∧ authority = some request.expected then
    match outcome with
    | .primary => some ⟨key, false, submittedMarkers request key⟩
    | .constraintRefusal =>
      if request.action.permitsRefusal then some ⟨key, true, submittedMarkers request key⟩ else none
    | .rejected | .stagingRejected => none
  else none

private theorem admitted_shape_before_publication
    (accepted : resolve authority request key outcome = some publication) :
    request.action.admits = true ∧ authority = some request.expected := by
  by_cases admitted : request.action.admits = true ∧ authority = some request.expected
  · exact admitted
  · simp [resolve, admitted] at accepted

theorem placed_action_checks_ordered_predecessor
    (accepted : resolve authority request key outcome = some publication) :
    authority = some request.expected := by
  exact (admitted_shape_before_publication accepted).2

theorem staged_or_ordinary_rejection_has_no_publication (authority : Option Int)
    (request : Submission) (key : Key) :
    resolve authority request key .rejected = none ∧
    resolve authority request key .stagingRejected = none := by
  simp [resolve]

theorem accepted_markers_are_exact
    (accepted : resolve authority request key outcome = some publication) :
    publication.key = key ∧
    publication.markers = submittedMarkers request key := by
  have matched := admitted_shape_before_publication accepted
  cases outcome with
  | rejected | stagingRejected => simp [resolve, matched] at accepted
  | primary =>
    simp [resolve, matched] at accepted
    subst publication
    exact ⟨rfl, rfl⟩
  | constraintRefusal =>
    cases supports : request.action.permitsRefusal <;> simp [resolve, matched, supports] at accepted
    subst publication
    exact ⟨rfl, rfl⟩

theorem current_writer_can_publish (request : Submission) (key : Key)
    (supported : request.action.admits = true) :
    resolve (some request.expected) request key .primary =
      some ⟨key, false, submittedMarkers request key⟩ := by
  simp [resolve, supported]

theorem kind_six_can_settle_under_its_original_check (request : Submission) (key : Key)
    (landing : request.action = .switchPreparedProposal true) :
    resolve (some request.expected) request key .constraintRefusal =
      some ⟨key, true, submittedMarkers request key⟩ := by
  simp [resolve, landing, Action.admits, Action.permitsRefusal]

theorem legacy_kind_six_and_clockless_switch_cannot_publish (request : Submission)
    (legacy : request.action = .decideHeldWorlds 6 ∨ request.action = .switchPreparedProposal false) :
    resolve authority request key outcome = none := by
  rcases legacy with legacy | legacy <;> simp [resolve, legacy, Action.admits]

theorem every_live_context_has_its_submitted_fingerprint
    (accepted : resolve authority request key outcome = some publication) (context : Nat) :
    (⟨context, key.revision, request.fingerprint⟩ : Marker) ∈ publication.markers ↔
    context ∈ request.contexts := by
  rw [(accepted_markers_are_exact accepted).2]
  constructor
  · intro present
    obtain ⟨original, member, eq⟩ := List.mem_map.mp present
    have same : original = context := congrArg Marker.context eq
    simpa [same] using member
  · intro member
    exact List.mem_map.mpr ⟨context, member, rfl⟩

/--
The authority projection uses the same chosen data history as the existing OrderedJournal.
The history's chosen cut is checked independently for each input, including after recovery.
-/
structure Predecessor where
  head : Key
  authority : Option Key

def atChosen (state : OrderedJournal.State) (project : Cut → Predecessor)
    (cut : Cut) : Option Predecessor :=
  if state.chosen cut.next = some cut then some (project cut) else none

theorem reopen_uses_one_chosen_history (project : Cut → Predecessor)
    (recovered : atChosen state project recoverCut = some recoveredPrefix)
    (resolving : atChosen state project resolveCut = some resolverPrefix)
    (samePosition : recoverCut.next = resolveCut.next) :
    recoverCut = resolveCut ∧ recoveredPrefix = resolverPrefix := by
  have chosen_recovery : state.chosen recoverCut.next = some recoverCut := by
    by_cases chosen : state.chosen recoverCut.next = some recoverCut
    · exact chosen
    · simp [atChosen, chosen] at recovered
  have chosen_resolver : state.chosen resolveCut.next = some resolveCut := by
    by_cases chosen : state.chosen resolveCut.next = some resolveCut
    · exact chosen
    · simp [atChosen, chosen] at resolving
  rw [samePosition, chosen_resolver] at chosen_recovery
  have sameCut : recoverCut = resolveCut := (Option.some.inj chosen_recovery).symm
  rw [sameCut, resolving] at recovered
  exact ⟨sameCut, (Option.some.inj recovered).symm⟩

private theorem step_preserves_chosen_cut
    (chosen : state.chosen cut.next = some cut) (within : cut.next ≤ state.head.next)
    (step : OrderedJournal.Step readers activations state next) :
    next.chosen cut.next = some cut ∧ cut.next ≤ next.head.next := by
  cases step <;> try exact ⟨chosen, within⟩
  case append cohort writer attempt receipt auth quorum issuer fresh advances =>
    have distinct : cut.next ≠ receipt.cut.next := by omega
    constructor
    · simpa [OrderedJournal.update, distinct] using chosen
    · change cut.next ≤ receipt.cut.next
      omega

theorem chosen_recovery_basis_survives_resolution_on_its_history (project : Cut → Predecessor)
    (recovered : atChosen state project cut = some prefixAtCut)
    (within : cut.next ≤ state.head.next)
    (path : OrderedJournal.Trace readers activations state next) :
    atChosen next project cut = some prefixAtCut := by
  have original : state.chosen cut.next = some cut := by
    by_cases chosen : state.chosen cut.next = some cut
    · exact chosen
    · simp [atChosen, chosen] at recovered
  have kept : next.chosen cut.next = some cut ∧ cut.next ≤ next.head.next := by
    induction path with
    | nil => exact ⟨original, within⟩
    | cons step _ ih =>
      have kept := step_preserves_chosen_cut original within step
      exact ih (by simpa [atChosen, original, kept.1] using recovered) kept.2 kept.1
  simpa [atChosen, original, kept.1] using recovered

theorem reopened_claim_is_compared_at_a_later_order {predecessor : Predecessor}
    {claim key : Key}
    (live : predecessor.authority = some claim)
    (claimSource : claim.source ≤ predecessor.head.source)
    (claimRevision : claim.revision ≤ predecessor.head.revision)
    (nextSource : predecessor.head.source < key.source)
    (nextRevision : predecessor.head.revision < key.revision)
    (accepted : resolve (predecessor.authority.map Key.revision) request key outcome = some publication) :
    request.expected = claim.revision ∧
    claim.source < publication.key.source ∧ claim.revision < publication.key.revision := by
  have compared := placed_action_checks_ordered_predecessor accepted
  have keyExact := (accepted_markers_are_exact accepted).1
  rw [live] at compared
  rw [keyExact]
  exact ⟨(Option.some.inj compared).symm, Int.lt_of_le_of_lt claimSource nextSource,
    Int.lt_of_le_of_lt claimRevision nextRevision⟩

theorem reopened_action_uses_the_recovered_installation_history
    (project : Cut → Predecessor) {predecessor : Predecessor} {claim key : Key}
    (recovered : atChosen state project recoverCut = some recoveredPrefix)
    (within : recoverCut.next ≤ state.head.next)
    (path : OrderedJournal.Trace readers activations state next)
    (resolving : atChosen next project next.head = some predecessor)
    (live : predecessor.authority = some claim)
    (claimSource : claim.source ≤ predecessor.head.source)
    (claimRevision : claim.revision ≤ predecessor.head.revision)
    (nextSource : predecessor.head.source < key.source)
    (nextRevision : predecessor.head.revision < key.revision)
    (accepted : resolve (predecessor.authority.map Key.revision) request key outcome = some publication) :
    atChosen next project recoverCut = some recoveredPrefix ∧
    predecessor = project next.head ∧ request.expected = claim.revision ∧
    claim.source < publication.key.source ∧ claim.revision < publication.key.revision := by
  have kept := chosen_recovery_basis_survives_resolution_on_its_history project recovered within path
  have same : predecessor = project next.head := by
    by_cases chosen : next.chosen next.head.next = some next.head
    · simpa [atChosen, chosen, eq_comm] using resolving
    · simp [atChosen, chosen] at resolving
  exact ⟨kept, same, reopened_claim_is_compared_at_a_later_order live claimSource claimRevision
    nextSource nextRevision accepted⟩

#print axioms placed_action_checks_ordered_predecessor
#print axioms staged_or_ordinary_rejection_has_no_publication
#print axioms accepted_markers_are_exact
#print axioms current_writer_can_publish
#print axioms kind_six_can_settle_under_its_original_check
#print axioms legacy_kind_six_and_clockless_switch_cannot_publish
#print axioms every_live_context_has_its_submitted_fingerprint
#print axioms reopen_uses_one_chosen_history
#print axioms chosen_recovery_basis_survives_resolution_on_its_history
#print axioms reopened_claim_is_compared_at_a_later_order
#print axioms reopened_action_uses_the_recovered_installation_history

end RTDB.Recovery.HeldWorldsOrder
