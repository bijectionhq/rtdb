import RetirementIdentity

namespace RTDB.RetirementCertificate

open RTDB.RetirementIdentity (ObjectVersion Publication deleteExact)

inductive HoldKind
  | reader | recoveryRoot | fork
  deriving DecidableEq

structure Hold where
  owner : Nat
  kind : HoldKind
  generation : Nat
  deriving DecidableEq

structure Ledger where
  head : Nat
  floor : Nat
  revision : Nat
  holds : List Hold
  proposal : Option Nat
  certified : Option ObjectVersion

def initial (head : Nat) : Ledger := ⟨head, 0, 0, [], none, none⟩

def allHoldsAfter (retirement : Nat) (state : Ledger) : Prop :=
  ∀ hold ∈ state.holds, retirement ≤ hold.generation

def ready (retirement : Nat) (state : Ledger) : Prop :=
  retirement ≤ state.head ∧ allHoldsAfter retirement state

inductive Step (retirement : Nat) (selected : ObjectVersion) : Ledger → Ledger → Prop
  | admit (state : Ledger) (hold : Hold) (aboveFloor : state.floor ≤ hold.generation)
      (belowHead : hold.generation ≤ state.head) :
      Step retirement selected state
        { state with holds := hold :: state.holds, revision := state.revision + 1 }
  | release (state : Ledger) (hold : Hold) :
      Step retirement selected state
        { state with holds := state.holds.filter (· != hold), revision := state.revision + 1 }
  | publishHead (state : Ledger) (head : Nat) (advance : state.head ≤ head) :
      Step retirement selected state
        { state with head, revision := state.revision + 1 }
  | observe (state : Ledger) (canPlan : ready retirement state) :
      Step retirement selected state { state with proposal := some state.revision }
  | certify (state : Ledger) (observed : ObjectVersion)
      (samePublication : selected = observed) (fresh : state.proposal = some state.revision) :
      Step retirement selected state
        { state with floor := max state.floor retirement, revision := state.revision + 1, certified := some observed }

inductive Reached (retirement : Nat) (selected : ObjectVersion) (startHead : Nat) : Ledger → Prop
  | start : Reached retirement selected startHead (initial startHead)
  | next {before after : Ledger} (history : Reached retirement selected startHead before)
      (step : Step retirement selected before after) : Reached retirement selected startHead after

def ProposalSound (retirement : Nat) (state : Ledger) : Prop :=
  ∀ revision, state.proposal = some revision →
    revision ≤ state.revision ∧ (revision = state.revision → ready retirement state)

def CertificateSound (retirement : Nat) (selected : ObjectVersion) (state : Ledger) : Prop :=
  ∀ identity, state.certified = some identity →
    identity = selected ∧ retirement ≤ state.floor ∧ allHoldsAfter retirement state

private theorem retain_stale_proposal {retirement : Nat} {before after : Ledger}
    (sound : ProposalSound retirement before)
    (sameProposal : after.proposal = before.proposal)
    (newRevision : after.revision = before.revision + 1) : ProposalSound retirement after := by
  intro revision planned
  rw [sameProposal] at planned
  have earlier := (sound revision planned).1
  rw [newRevision]
  exact ⟨by omega, by omega⟩

theorem certificate_preserved_by_step {retirement : Nat} {selected : ObjectVersion}
    {before after : Ledger} (step : Step retirement selected before after)
    (plan : ProposalSound retirement before) (sound : CertificateSound retirement selected before) :
    CertificateSound retirement selected after := by
  cases step with
  | admit hold aboveFloor belowHead =>
    intro identity cert
    obtain ⟨selectedIdentity, floor, holds⟩ := sound identity cert
    refine ⟨selectedIdentity, floor, ?_⟩
    intro retained member
    rcases List.mem_cons.mp member with same | older
    · subst retained
      exact Nat.le_trans floor aboveFloor
    · exact holds retained older
  | release hold =>
    intro identity cert
    obtain ⟨selectedIdentity, floor, holds⟩ := sound identity cert
    exact ⟨selectedIdentity, floor, fun retained member => holds retained ((List.mem_filter.mp member).1)⟩
  | publishHead head advance => exact sound
  | observe canPlan => exact sound
  | certify observed samePublication fresh =>
    intro identity cert
    have sameIdentity : observed = identity := Option.some.inj cert
    have readyNow := (plan before.revision fresh).2 rfl
    exact ⟨sameIdentity.symm.trans samePublication.symm, Nat.le_max_right .., readyNow.2⟩

theorem proposal_preserved_by_step {retirement : Nat} {selected : ObjectVersion}
    {before after : Ledger} (step : Step retirement selected before after)
    (sound : ProposalSound retirement before) : ProposalSound retirement after := by
  cases step with
  | admit hold aboveFloor belowHead => exact retain_stale_proposal sound rfl rfl
  | release hold => exact retain_stale_proposal sound rfl rfl
  | publishHead head advance => exact retain_stale_proposal sound rfl rfl
  | certify observed samePublication fresh => exact retain_stale_proposal sound rfl rfl
  | observe canPlan =>
    intro revision planned
    have sameRevision : before.revision = revision := Option.some.inj planned
    subst revision
    exact ⟨Nat.le_refl _, fun _ => canPlan⟩

theorem every_issued_certificate_is_sound {retirement : Nat} {selected : ObjectVersion}
    {startHead : Nat} {state : Ledger} (history : Reached retirement selected startHead state) :
    ProposalSound retirement state ∧ CertificateSound retirement selected state := by
  induction history with
  | start => simp [initial, ProposalSound, CertificateSound]
  | next _ step previous =>
    exact ⟨proposal_preserved_by_step step previous.1, certificate_preserved_by_step step previous.1 previous.2⟩

theorem certified_delete_preserves_held_publications {retirement : Nat} {selected target : ObjectVersion}
    {startHead : Nat} {state : Ledger} (history : Reached retirement selected startHead state)
    (certified : state.certified = some target)
    (depends : Hold → Publication → Prop)
    (retired : ∀ hold publication, retirement ≤ hold.generation → publication.identity = selected →
      ¬ depends hold publication)
    (hold : Hold) (held : hold ∈ state.holds) (publication : Publication) (uses : depends hold publication)
    (inventory : List Publication) (present : publication ∈ inventory) :
    publication ∈ deleteExact target inventory := by
  obtain ⟨matched, _, covered⟩ := (every_issued_certificate_is_sound history).2 target certified
  have different : publication.identity ≠ target := fun same => retired hold publication
    (covered hold held) (same.trans matched) uses
  simp [deleteExact, present, different]

theorem an_installed_floor_refuses_every_older_hold {retirement : Nat} {selected : ObjectVersion}
    {startHead : Nat} {state : Ledger} (history : Reached retirement selected startHead state)
    (certified : state.certified = some selected) (hold : Hold)
    (old : hold.generation < retirement) : ¬ state.floor ≤ hold.generation := by
  have floor := ((every_issued_certificate_is_sound history).2 selected certified).2.1
  omega


theorem current_unheld_retirement_can_be_certified (retirement : Nat) (selected : ObjectVersion) :
    ∃ state, Reached retirement selected retirement state ∧ state.certified = some selected := by
  let start := initial retirement
  let proposed : Ledger := { start with proposal := some start.revision }
  let certified : Ledger :=
    { proposed with floor := max proposed.floor retirement, revision := proposed.revision + 1,
                    certified := some selected }
  have observe : Step retirement selected start proposed := by
    apply Step.observe
    simp [start, initial, ready] <;> intro hold member <;> cases member
  have issue : Step retirement selected proposed certified :=
    Step.certify proposed selected rfl rfl
  exact ⟨certified, .next (.next .start observe) issue, rfl⟩

#print axioms certificate_preserved_by_step
#print axioms proposal_preserved_by_step
#print axioms every_issued_certificate_is_sound
#print axioms certified_delete_preserves_held_publications
#print axioms an_installed_floor_refuses_every_older_hold
#print axioms current_unheld_retirement_can_be_certified

end RTDB.RetirementCertificate
