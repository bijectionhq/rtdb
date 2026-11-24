import Std

set_option warningAsError true

namespace ForkCustody

inductive Kind where
  | dependent
  | export
  deriving DecidableEq

inductive Domain where
  | source
  | independent
  deriving DecidableEq

def publishDestination (kind : Kind) (reencrypted : Bool) : Option Domain :=
  match kind with
  | .dependent => some .source
  | .export => if reencrypted then some .independent else none

def mayCheckForkOutcome (preparationHeld : Bool) : Prop := preparationHeld = false

def settledForkOutcome (hasStage deleted : Bool) : Option Bool :=
  if !hasStage then some true else if deleted then some false else none

def mayWithdraw (held : Bool) : Prop := held = false

structure State where
  kind : Kind
  started : Bool := false
  preparation : Bool := false
  hold : Bool := false
  reencrypted : Bool := false
  destination : Option Domain := none
  disposed : Bool := false
  withdrawn : Bool := false

inductive Step : State → State → Prop where
  | admit (s : State) (unused : s.started = false) (openSource : s.withdrawn = false) :
      Step s { s with started := true, preparation := true, hold := true }
  | reencrypt (s : State) (held : s.hold = true) (preparing : s.preparation = true) :
      Step s { s with reencrypted := true }
  | publish (s : State) (domain : Domain) (preparing : s.preparation = true)
      (held : s.hold = true) (allowed : publishDestination s.kind s.reencrypted = some domain) :
      Step s { s with preparation := false, destination := some domain }
  | fenceRejected (s : State) (preparing : s.preparation = true) :
      Step s { s with preparation := false }
  | disposeRejected (s : State) (started : s.started = true)
      (fenced : s.preparation = false) (unpublished : s.destination = none) :
      Step s { s with disposed := true }
  | release (s : State) (outcome : Bool) (held : s.hold = true) (cleared : mayCheckForkOutcome s.preparation)
      (settled : settledForkOutcome s.destination.isNone s.disposed = some outcome) :
      Step s { s with hold := false }
  | withdraw (s : State) (released : mayWithdraw s.hold) :
      Step s { s with withdrawn := true }

inductive Trace : State → State → Prop where
  | empty (s : State) : Trace s s
  | next {start here after : State} (earlier : Trace start here) (step : Step here after) :
      Trace start after

def initial (kind : Kind) : State := { kind }

def Safe (s : State) : Prop :=
  (s.started = true → s.destination = none → s.disposed = false → s.hold = true) ∧
  (s.withdrawn = true → s.hold = false) ∧
  (∀ domain, s.destination = some domain →
    (s.kind = .dependent → domain = .source) ∧
    (s.kind = .export → domain = .independent ∧ s.reencrypted = true))

theorem release_requires_a_settled_outcome (hasStage deleted : Bool) :
    (settledForkOutcome hasStage deleted = some true ↔ hasStage = false) ∧
    (settledForkOutcome hasStage deleted = some false ↔ hasStage = true ∧ deleted = true) ∧
    (settledForkOutcome hasStage deleted = none ↔ hasStage = true ∧ deleted = false) := by
  cases hasStage <;> cases deleted <;> decide

theorem retained_preparation_cannot_release : ¬ mayCheckForkOutcome true := by
  simp [mayCheckForkOutcome]

theorem step_preserves_custody {before after : State} (safe : Safe before)
    (step : Step before after) : Safe after := by
  rcases safe with ⟨held, withdrawn, published⟩
  cases step with
  | release outcome _ _ settled =>
      refine ⟨?_, fun _ => rfl, published⟩
      intro _ unpublished notDisposed
      change before.destination = none at unpublished
      change before.disposed = false at notDisposed
      simp [settledForkOutcome, unpublished, notDisposed] at settled
  | publish domain _ _ allowed =>
      refine ⟨by simp, withdrawn, ?_⟩
      intro other same
      cases Option.some.inj same
      constructor
      · intro kind
        change before.kind = .dependent at kind
        simpa [publishDestination, kind, eq_comm] using allowed
      · intro kind
        change before.kind = .export at kind
        change domain = .independent ∧ before.reencrypted = true
        cases copied : before.reencrypted <;> simp [publishDestination, kind, copied, eq_comm] at allowed ⊢
        exact allowed
  | reencrypt =>
      exact ⟨held, withdrawn, fun domain exactDomain =>
        ⟨(published domain exactDomain).1, fun kind => ⟨((published domain exactDomain).2 kind).1, rfl⟩⟩⟩
  | admit _ openSource => exact ⟨fun _ _ _ => rfl, by simp [openSource], published⟩
  | fenceRejected => exact ⟨held, withdrawn, published⟩
  | disposeRejected => exact ⟨by simp, withdrawn, published⟩
  | withdraw released => exact ⟨held, fun _ => released, published⟩

theorem trace_preserves_custody {kind finish} (trace : Trace (initial kind) finish) : Safe finish := by
  induction trace with
  | empty => simp [initial, Safe]
  | next _ step prior => exact step_preserves_custody prior step

theorem dependent_publication_is_in_the_source_domain :
    publishDestination .dependent false = some .source := by rfl

theorem export_refuses_before_reencryption : publishDestination .export false = none := by rfl

theorem export_reaches_publication_release_and_withdrawal :
    ∃ final, Trace (initial .export) final ∧ final.destination = some .independent ∧
      final.reencrypted = true ∧ final.hold = false ∧ final.withdrawn = true := by
  let held := { initial .export with started := true, preparation := true, hold := true }
  let copied := { held with reencrypted := true }
  let published := { copied with preparation := false, destination := some .independent }
  let released := { published with hold := false }
  refine ⟨{ released with withdrawn := true }, ?_, rfl, rfl, rfl, rfl⟩
  exact .next (.next (.next (.next (.next (.empty _) (.admit _ rfl rfl))
    (.reencrypt held rfl rfl)) (.publish copied .independent rfl rfl rfl))
    (.release published true rfl rfl rfl)) (.withdraw released (by simp [mayWithdraw, released]))

theorem dependent_fork_reaches_publication_with_its_hold :
    ∃ published, Trace (initial .dependent) published ∧ published.destination = some .source ∧
      published.hold = true := by
  let held := { initial .dependent with started := true, preparation := true, hold := true }
  exact ⟨{ held with preparation := false, destination := some .source },
    .next (.next (.empty _) (.admit _ rfl rfl)) (.publish held .source rfl rfl rfl), rfl, rfl⟩

theorem a_rejected_fork_can_release_after_disposal :
    ∃ final, Trace (initial .dependent) final ∧ final.destination = none ∧
      final.disposed = true ∧ final.hold = false := by
  let held := { initial .dependent with started := true, preparation := true, hold := true }
  let fenced := { held with preparation := false }
  let disposed := { fenced with disposed := true }
  refine ⟨{ disposed with hold := false }, ?_, rfl, rfl, rfl⟩
  exact .next (.next (.next (.next (.empty _) (.admit _ rfl rfl)) (.fenceRejected held rfl))
    (.disposeRejected fenced rfl rfl rfl)) (.release disposed false rfl rfl rfl)

#print axioms release_requires_a_settled_outcome
#print axioms retained_preparation_cannot_release
#print axioms step_preserves_custody
#print axioms trace_preserves_custody
#print axioms dependent_publication_is_in_the_source_domain
#print axioms export_refuses_before_reencryption
#print axioms export_reaches_publication_release_and_withdrawal
#print axioms dependent_fork_reaches_publication_with_its_hold
#print axioms a_rejected_fork_can_release_after_disposal

end ForkCustody
